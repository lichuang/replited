use std::sync::Arc;

use log::info;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use super::sync_client::SnapshotInfo;
use super::sync_client::SyncClient;
use super::sync_client::WalSegmentInfo;
use super::ShadowWalReader;
use crate::base::compress_buffer;
use crate::base::decompressed_data;
use crate::config::StorageConfig;
use crate::database::DatabaseInfo;
use crate::database::DbCommand;
use crate::database::WalGenerationPos;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::align_frame;
use crate::sqlite::WALFrame;
use crate::sqlite::WALHeader;

#[derive(Clone, Debug)]
pub enum SyncCommand {
    DbChanged(WalGenerationPos),
    Snapshot((WalGenerationPos, Vec<u8>)),
}

#[derive(Debug, PartialEq)]
enum SyncState {
    WaitDbChanged,
    WaitSnapshot,
}

pub struct Sync {
    index: usize,
    client: SyncClient,
    db_notifier: Sender<DbCommand>,
    position: WalGenerationPos,
    state: SyncState,
    info: DatabaseInfo,
}

impl Sync {
    pub fn new(
        config: StorageConfig,
        db: String,
        index: usize,
        db_notifier: Sender<DbCommand>,
        info: DatabaseInfo,
    ) -> Result<Arc<RwLock<Self>>> {
        Ok(Arc::new(RwLock::new(Self {
            index,
            position: WalGenerationPos::default(),
            db_notifier,
            client: SyncClient::new(db, config)?,
            state: SyncState::WaitDbChanged,
            info,
        })))
    }

    pub fn start(s: Arc<RwLock<Sync>>, rx: Receiver<SyncCommand>) -> Result<JoinHandle<()>> {
        let handle = tokio::spawn(async move {
            let _ = Sync::main(s.clone(), rx).await;
        });

        Ok(handle)
    }

    pub async fn main(s: Arc<RwLock<Sync>>, rx: Receiver<SyncCommand>) -> Result<()> {
        let mut rx = rx;
        let mut s = s.write().await;
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
        let snapshots = self.client.snapshots(&generation).await?;
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
        let wal_segments = self.client.wal_segments(&generation).await?;
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

        // Determine last WAL segment available.
        let segment = self.max_wal_segment(generation).await;
        let segment = match segment {
            Err(e) => {
                if e.code() == Error::NO_WALSEGMENT_ERROR {
                    // Use snapshot if none exist.
                    return Ok(WalGenerationPos {
                        generation: generation.to_string(),
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
        let mut reader = ShadowWalReader::try_create(self.position.clone(), &self.info)?;

        // Obtain initial position from shadow reader.
        // It may have moved to the next index if previous position was at the end.
        let init_pos = reader.pos.clone();
        let mut data = Vec::new();

        // Copy header if at offset zero.
        let mut salt = 0;
        if init_pos.offset == 0 {
            let wal_header = WALHeader::read_from(&mut reader.file)?;
            salt = wal_header.salt;
            data.extend_from_slice(&wal_header.data);
        }

        // Copy frames.
        loop {
            if reader.left == 0 {
                break;
            }

            let pos = reader.pos();
            debug_assert_eq!(pos.offset, align_frame(self.info.page_size, pos.offset));

            let wal_frame =
                WALFrame::read_without_checksum(&mut reader.file, self.info.page_size, salt)?;
            salt = wal_frame.salt;

            data.extend_from_slice(&wal_frame.data);
            reader.advance(wal_frame.data.len())?;
        }
        let compressed_data = compress_buffer(&data)?;

        let _ = self
            .client
            .write_wal_segment(&init_pos, compressed_data)
            .await?;

        // update position
        self.position = reader.pos();
        Ok(())
    }

    pub fn position(&self) -> WalGenerationPos {
        self.position.clone()
    }

    async fn command(&mut self, cmd: SyncCommand) -> Result<()> {
        match cmd {
            SyncCommand::DbChanged(pos) => self.sync(pos).await?,
            SyncCommand::Snapshot((pos, compressed_data)) => {
                self.sync_snapshot(pos, compressed_data).await?;
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
        if generation != self.position.generation {
            let snapshots = self.client.snapshots(&generation).await?;
            if snapshots.len() == 0 {
                // Create snapshot if no snapshots exist for generation.
                self.db_notifier
                    .send(DbCommand::Snapshot(self.index))
                    .await?;
                self.state = SyncState::WaitSnapshot;
                return Ok(());
            }

            let pos = self.calculate_generation_position(&generation).await?;
            self.position = pos;
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
