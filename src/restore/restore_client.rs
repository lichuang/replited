use std::fs;

use log::error;

use crate::config::RestoreDbConfig;
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

    pub async fn run(&self, overwrite: bool) -> Result<()> {
        // Ensure output path does not already exist.
        if !overwrite && fs::exists(&self.db)? {
            error!("db {} already exists but cannot overwrite", self.db);
            return Err(Error::OverwriteDbError("cannot overwrite exist db"));
        }

        Ok(())
    }
}

pub async fn run_restore(config: &RestoreDbConfig, overwrite: bool) -> Result<()> {
    let restore = RestoreClient::try_create(config.db.clone(), config.replicate.clone())?;

    restore.run(overwrite).await?;

    Ok(())
}
