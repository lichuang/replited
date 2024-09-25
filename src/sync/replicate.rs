use std::sync::Arc;

use log::debug;
use log::error;
use log::info;
use parking_lot::RwLock;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use super::ShadowWalReader;
use crate::base::compress_buffer;
use crate::base::decompressed_data;
use crate::base::Generation;
use crate::config::StorageConfig;
use crate::database::DatabaseInfo;
use crate::database::DbCommand;
use crate::database::WalGenerationPos;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::align_frame;
use crate::sqlite::WALFrame;
use crate::sqlite::WALHeader;
use crate::storage::SnapshotInfo;
use crate::storage::StorageClient;
use crate::storage::WalSegmentInfo;

#[derive(Clone, Debug)]
pub enum SyncCommand {
    DbChanged(WalGenerationPos),
    Snapshot((WalGenerationPos, Vec<u8>)),
}

#[derive(Debug, Clone, PartialEq)]
enum SyncState {
    WaitDbChanged,
    WaitSnapshot,
}

#[derive(Debug, Clone)]
pub struct Replicate {
    db: String,
    index: usize,
    client: StorageClient,
    db_notifier: Sender<DbCommand>,
    position: Arc<RwLock<WalGenerationPos>>,
    state: SyncState,
    info: DatabaseInfo,
}

impl Replicate {
    pub fn new(
        config: StorageConfig,
        db: String,
        index: usize,
        db_notifier: Sender<DbCommand>,
        info: DatabaseInfo,
    ) -> Result<Self> {
        Ok(Self {
            db: db.clone(),
            index,
            position: Arc::new(RwLock::new(WalGenerationPos::default())),
            db_notifier,
            client: StorageClient::try_create(db, config)?,
            state: SyncState::WaitDbChanged,
            info,
        })
    }

    pub fn start(s: Replicate, rx: Receiver<SyncCommand>) -> Result<JoinHandle<()>> {
        let s = s.clone();
        let handle = tokio::spawn(async move {
            let _ = Replicate::main(s, rx).await;
        });

        Ok(handle)
    }

    pub async fn main(s: Replicate, rx: Receiver<SyncCommand>) -> Result<()> {
        let mut rx = rx;
        let mut s = s;
        loop {
            select! {
                cmd = rx.recv() => if let Some(cmd) = cmd {
                    s.command(cmd).await?
                }
            }
        }
    }

    // returns the last snapshot in a generation.
    async fn max_snapshot(&self, generation: &str) -> Result<SnapshotInfo> {
        let snapshots = self.client.snapshots(generation).await?;
        if snapshots.is_empty() {
            return Err(Error::NoSnapshotError(generation));
        }
        let mut max_index = 0;
        let mut max_snapshot_index = 0;
        for (i, snapshot) in snapshots.iter().enumerate() {
            if snapshot.index > max_snapshot_index {
                max_snapshot_index = snapshot.index;
                max_index = i;
            }
        }

        Ok(snapshots[max_index].clone())
    }

    // returns the highest WAL segment in a generation.
    async fn max_wal_segment(&self, generation: &str) -> Result<WalSegmentInfo> {
        let wal_segments = self.client.wal_segments(generation).await?;
        if wal_segments.is_empty() {
            return Err(Error::NoWalsegmentError(generation));
        }
        let mut max_index = 0;
        let mut max_wg_index = 0;
        for (i, wg) in wal_segments.iter().enumerate() {
            if wg.index > max_wg_index {
                max_wg_index = wg.index;
                max_index = i;
            }
        }

        Ok(wal_segments[max_index].clone())
    }

    async fn calculate_generation_position(&self, generation: &str) -> Result<WalGenerationPos> {
        // Fetch last snapshot. Return error if no snapshots exist.
        let snapshot = self.max_snapshot(generation).await?;
        let generation = Generation::try_create(generation)?;

        // Determine last WAL segment available.
        let segment = self.max_wal_segment(generation.as_str()).await;
        let segment = match segment {
            Err(e) => {
                if e.code() == Error::NO_WALSEGMENT_ERROR {
                    // Use snapshot if none exist.
                    return Ok(WalGenerationPos {
                        generation,
                        index: snapshot.index,
                        offset: 0,
                    });
                } else {
                    return Err(e);
                }
            }
            Ok(segment) => segment,
        };

        let compressed_data = self.client.read_wal_segment(&segment).await?;
        let decompressed_data = decompressed_data(compressed_data)?;

        Ok(WalGenerationPos {
            generation: segment.generation.clone(),
            index: segment.index,
            offset: segment.offset + decompressed_data.len() as u64,
        })
    }

    async fn sync_wal(&mut self) -> Result<()> {
        let mut reader = ShadowWalReader::try_create(self.position(), &self.info)?;

        // Obtain initial position from shadow reader.
        // It may have moved to the next index if previous position was at the end.
        let init_pos = reader.position();
        let mut data = Vec::new();

        debug!("db {} write wal segment position {:?}", self.db, init_pos,);

        // Copy header if at offset zero.
        let mut salt1 = 0;
        let mut salt2 = 0;
        if init_pos.offset == 0 {
            let wal_header = WALHeader::read_from(&mut reader)?;
            salt1 = wal_header.salt1;
            salt2 = wal_header.salt2;
            data.extend_from_slice(&wal_header.data);
        }

        // Copy frames.
        loop {
            if reader.left == 0 {
                break;
            }

            let pos = reader.position();
            debug_assert_eq!(pos.offset, align_frame(self.info.page_size, pos.offset));

            let wal_frame = WALFrame::read(&mut reader, self.info.page_size)?;

            if (salt1 != 0 && salt1 != wal_frame.salt1) || (salt2 != 0 && salt2 != wal_frame.salt2)
            {
                return Err(Error::SqliteInvalidWalFrameError(format!(
                    "db {} Invalid WAL frame at offset {}",
                    self.db, pos.offset
                )));
            }
            salt1 = wal_frame.salt1;
            salt2 = wal_frame.salt2;

            data.extend_from_slice(&wal_frame.data);
        }
        let compressed_data = compress_buffer(&data)?;

        self.client
            .write_wal_segment(&init_pos, compressed_data)
            .await?;

        // update position
        let mut position = self.position.write();
        *position = reader.position();
        Ok(())
    }

    pub fn position(&self) -> WalGenerationPos {
        let position = self.position.read();
        position.clone()
    }

    fn reset_position(&self) {
        let mut position = self.position.write();
        *position = WalGenerationPos::default();
    }

    async fn command(&mut self, cmd: SyncCommand) -> Result<()> {
        match cmd {
            SyncCommand::DbChanged(pos) => {
                if let Err(e) = self.sync(pos).await {
                    error!("sync db error: {:?}", e);
                    // Clear last position if if an error occurs during sync.
                    self.reset_position();
                }
            }
            SyncCommand::Snapshot((pos, compressed_data)) => {
                if let Err(e) = self.sync_snapshot(pos, compressed_data).await {
                    error!("sync db snapshot error: {:?}", e);
                }
            }
        }
        Ok(())
    }

    async fn sync_snapshot(
        &mut self,
        pos: WalGenerationPos,
        compressed_data: Vec<u8>,
    ) -> Result<()> {
        debug_assert_eq!(self.state, SyncState::WaitSnapshot);
        if pos.offset == 0 {
            return Ok(());
        }

        let _ = self.client.write_snapshot(&pos, compressed_data).await?;

        // change state from WaitSnapshot to WaitDbChanged
        self.state = SyncState::WaitDbChanged;
        self.sync(pos).await
    }

    async fn sync(&mut self, pos: WalGenerationPos) -> Result<()> {
        info!("replica sync pos: {:?}\n", pos);

        if self.state == SyncState::WaitSnapshot {
            return Ok(());
        }

        if pos.offset == 0 {
            return Ok(());
        }

        // Create a new snapshot and update the current replica position if
        // the generation on the database has changed.
        let generation = pos.generation.clone();
        let position = {
            let position = self.position.read();
            position.clone()
        };
        if generation != position.generation {
            let snapshots = self.client.snapshots(generation.as_str()).await?;
            if snapshots.is_empty() {
                // Create snapshot if no snapshots exist for generation.
                self.db_notifier
                    .send(DbCommand::Snapshot(self.index))
                    .await?;
                self.state = SyncState::WaitSnapshot;
                return Ok(());
            }

            let pos = self
                .calculate_generation_position(generation.as_str())
                .await?;
            *self.position.write() = pos;
        }

        // Read all WAL files since the last position.
        loop {
            if let Err(e) = self.sync_wal().await {
                if e.code() == Error::UNEXPECTED_EOF_ERROR {
                    break;
                }
            }
        }
        Ok(())
    }
}
