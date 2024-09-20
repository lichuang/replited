use std::time::Duration;

use log::error;
use tokio::select;
use tokio::time::sleep;

use crate::config::DatabaseConfig;
use crate::config::StorageConfig;
use crate::error::Error;
use crate::error::Result;

pub struct RestoreClient {
    db: String,
    config: StorageConfig,
}

impl RestoreClient {
    pub fn try_create(db: String, config: StorageConfig) -> Result<Self> {
        Ok(Self { db, config })
    }

    pub async fn sync(&self) -> Result<()> {
        // Ensure output path does not already exist.

        Ok(())
    }
}

pub async fn run_restore(config: DatabaseConfig) -> Result<()> {
    let restore = RestoreClient::try_create(config.db.clone(), config.replicate[0].clone())?;
    loop {
        select! {
            _ = sleep(Duration::from_secs(1)) => {
                if let Err(e) = restore.sync().await {
                    error!("restore db {} error: {:?}", restore.db, e);
                }
            }
        }
    }
}
