use std::fs;

use log::error;
use opendal::Operator;

use crate::config::DbConfig;
use crate::config::RestoreOptions;
use crate::config::StorageConfig;
use crate::error::Error;
use crate::error::Result;
use crate::storage::StorageClient;

struct Restore {
    db: String,
    config: Vec<StorageConfig>,
    options: RestoreOptions,
}

struct RestoreInfo {
    config: StorageConfig,
    generation: String,
    operator: Operator,
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

    pub async fn decide_restore_info(&self) -> Result<()> {
        for config in &self.config {
            let client = StorageClient::try_create(self.db.clone(), config.clone())?;
            let snapshot_info = client.latest_snapshot(&self.options.generation).await?;
        }

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        // Ensure output path does not already exist.
        if !self.options.overwrite && fs::exists(&self.options.output)? {
            println!("db {} already exists but cannot overwrite", self.db);
            return Err(Error::OverwriteDbError("cannot overwrite exist db"));
        }

        self.decide_restore_info().await?;

        Ok(())
    }
}

pub async fn run_restore(config: &DbConfig, options: &RestoreOptions) -> Result<()> {
    let restore =
        Restore::try_create(config.db.clone(), config.replicate.clone(), options.clone())?;

    restore.run().await?;

    Ok(())
}
