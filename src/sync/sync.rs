use std::borrow::BorrowMut;
use std::sync::Arc;

use log::debug;
use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;

use super::new_sync_client;
use super::sync_client::SyncClient;
use crate::config::StorageConfig;
use crate::error::Result;

#[derive(Clone, Debug)]
pub enum SyncCommand {
    DbChanged,
}

pub struct Sync {
    client: Arc<dyn SyncClient>,
}

impl Sync {
    pub fn new(config: StorageConfig) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            client: new_sync_client(config)?,
        }))
    }

    pub fn start(s: Arc<Sync>, rx: Receiver<SyncCommand>) -> Result<JoinHandle<()>> {
        let handle = tokio::spawn(async move {
            let _ = Sync::main(s, rx).await;
        });

        Ok(handle)
    }

    pub async fn main(s: Arc<Sync>, rx: Receiver<SyncCommand>) -> Result<()> {
        let mut rx = rx;
        loop {
            select! {
                cmd = rx.recv() => match cmd {
                        Err(e) => return Err(e.into()),
                        Ok(cmd) => s.as_ref().command(cmd).await?
                    }
            }
        }
        Ok(())
    }

    async fn command(&self, cmd: SyncCommand) -> Result<()> {
        Ok(())
    }
}
