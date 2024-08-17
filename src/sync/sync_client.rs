use std::sync::Arc;

use async_trait::async_trait;

use super::fs::FsSyncClient;
use crate::config::StorageConfig;
use crate::config::StorageParams;
use crate::error::Result;

#[async_trait]
pub trait SyncClient: Send + Sync {
    async fn run(&mut self) -> Result<()>;
}

pub fn new_sync_client(config: StorageConfig) -> Result<Arc<dyn SyncClient>> {
    match config.params {
        StorageParams::Fs(fs) => Ok(FsSyncClient::new(fs)),
        _ => unreachable!(),
    }
}
