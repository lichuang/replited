use std::borrow::BorrowMut;
use std::sync::Arc;

use log::debug;
use log::info;
use opendal::Operator;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use super::init_operator;
use super::sync_client::SnapshotInfo;
use super::sync_client::SyncClient;
use crate::config::StorageConfig;
use crate::config::StorageParams;
use crate::database::DbCommand;
use crate::database::WalGenerationPos;
use crate::error::Result;

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
}

impl Sync {
    pub fn new(
        config: StorageConfig,
        db: String,
        index: usize,
        db_notifier: Sender<DbCommand>,
    ) -> Result<Self> {
        Ok(Self {
            index,
            position: WalGenerationPos::default(),
            db_notifier,
            client: SyncClient::new(db, config)?,
            state: SyncState::WaitDbChanged,
        })
    }

    pub fn start(s: Sync, rx: Receiver<SyncCommand>) -> Result<JoinHandle<()>> {
        let mut s = s;
        let handle = tokio::spawn(async move {
            let _ = Sync::main(&mut s, rx).await;
        });

        Ok(handle)
    }

    pub async fn main(s: &mut Sync, rx: Receiver<SyncCommand>) -> Result<()> {
        let mut rx = rx;
        loop {
            select! {
                cmd = rx.recv() => if let Some(cmd) = cmd {
                    s.command(cmd).await?
                }
            }
        }
        Ok(())
    }

    async fn command(&mut self, cmd: SyncCommand) -> Result<()> {
        match cmd {
            SyncCommand::DbChanged(pos) => self.sync(pos).await?,
            SyncCommand::Snapshot((pos, compressed_data)) => {
                self.sync_snapshot(pos, compressed_data).await?;
            }
            _ => unreachable!(),
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

        let _ = self
            .client
            .save_snapshot(&pos.generation, pos.wal_index, compressed_data)
            .await?;

        Ok(())
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
        }

        Ok(())
    }
}
