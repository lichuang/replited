use std::fs;
use std::fs::OpenOptions;
use std::io::Write;

use log::debug;
use opendal::Operator;

use crate::base::decompressed_data;
use crate::base::parent_dir;
use crate::config::DbConfig;
use crate::config::RestoreOptions;
use crate::config::StorageConfig;
use crate::error::Error;
use crate::error::Result;
use crate::storage::SnapshotInfo;
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

    pub async fn decide_restore_info(&self) -> Result<Option<(SnapshotInfo, StorageClient)>> {
        let mut latest_snapshot: Option<(SnapshotInfo, StorageClient)> = None;

        for config in &self.config {
            let client = StorageClient::try_create(self.db.clone(), config.clone())?;
            let snapshot_info = match client.latest_snapshot().await? {
                Some(snapshot_into) => snapshot_into,
                None => continue,
            };
            println!("snapshot_info: {:?}", snapshot_info);
            match &latest_snapshot {
                Some(ls) => {
                    if snapshot_info.generation > ls.0.generation {
                        latest_snapshot = Some((snapshot_info, client));
                    }
                }
                None => {
                    latest_snapshot = Some((snapshot_info, client));
                }
            }
        }

        Ok(latest_snapshot)
    }

    async fn restore_snapshot(
        &self,
        client: &StorageClient,
        snapshot: &SnapshotInfo,
    ) -> Result<()> {
        // create a temp file to write snapshot
        let temp_output = format!("{}.tmp", self.options.output);

        let dir = parent_dir(&temp_output).unwrap();
        fs::create_dir_all(&dir)?;

        let compressed_data = client.read_snapshot(snapshot).await?;
        let decompressed_data = decompressed_data(compressed_data)?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_output)?;

        file.write(&decompressed_data)?;

        // rename the temp file to output file
        fs::rename(&temp_output, &self.options.output)?;

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        // Ensure output path does not already exist.
        if !self.options.overwrite && fs::exists(&self.options.output)? {
            println!("db {} already exists but cannot overwrite", self.db);
            return Err(Error::OverwriteDbError("cannot overwrite exist db"));
        }

        let (latest_snapshot, client) = match self.decide_restore_info().await? {
            Some(latest_snapshot) => latest_snapshot,
            None => {
                debug!("cannot find snapshot");
                return Ok(());
            }
        };

        // restore snapshot
        self.restore_snapshot(&client, &latest_snapshot).await?;
        println!("after restore snapshot");

        // apply wal frames
        Ok(())
    }
}

pub async fn run_restore(config: &DbConfig, options: &RestoreOptions) -> Result<()> {
    let restore =
        Restore::try_create(config.db.clone(), config.replicate.clone(), options.clone())?;

    restore.run().await?;

    Ok(())
}
