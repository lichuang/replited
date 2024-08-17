use std::sync::Arc;

use async_trait::async_trait;

use crate::config::StorageFsConfig;
use crate::error::Result;
use crate::sync::sync_client::SyncClient;

pub struct FsSyncClient {
    root: String,
}

#[async_trait]
impl SyncClient for FsSyncClient {
    async fn run(&mut self) -> Result<()> {
        Ok(())
    }
}

impl FsSyncClient {
    pub fn new(config: StorageFsConfig) -> Arc<dyn SyncClient> {
        Arc::new(FsSyncClient { root: config.root })
    }
}
