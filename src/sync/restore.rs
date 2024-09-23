use std::fs;

use log::error;

use crate::config::DbConfig;
use crate::config::RestoreOptions;
use crate::config::StorageConfig;
use crate::error::Error;
use crate::error::Result;

struct Restore {
    db: String,
    config: Vec<StorageConfig>,
    options: RestoreOptions,
}

struct RestoreInfo {
    config: StorageConfig,
    generation: String,
}

impl Restore {
    pub fn try_create(
        db: String,
        config: Vec<StorageConfig>,
        options: RestoreOptions,
    ) -> Result<Self> {
        Ok(Self {
            db,
            config,
            options,
        })
    }

    // pub async fn decide_restore_info(&self) -> Result<RestoreInfo> {
    //    for config in &self.config {}
    //}

    pub async fn run(&self) -> Result<()> {
        // Ensure output path does not already exist.
        if !self.options.overwrite && fs::exists(&self.db)? {
            error!("db {} already exists but cannot overwrite", self.db);
            return Err(Error::OverwriteDbError("cannot overwrite exist db"));
        }

        Ok(())
    }
}

pub async fn run_restore(config: &DbConfig, options: &RestoreOptions) -> Result<()> {
    let restore =
        Restore::try_create(config.db.clone(), config.replicate.clone(), options.clone())?;

    restore.run().await?;

    Ok(())
}
