use std::fs;
use std::fs::OpenOptions;
use std::io::Write;

use log::debug;
use log::error;
use rusqlite::Connection;

use crate::base::decompressed_data;
use crate::base::parent_dir;
use crate::config::DbConfig;
use crate::config::RestoreOptions;
use crate::config::StorageConfig;
use crate::error::Error;
use crate::error::Result;
use crate::storage::RestoreInfo;
use crate::storage::SnapshotInfo;
use crate::storage::StorageClient;
use crate::storage::WalSegmentInfo;

struct Restore {
    db: String,
    config: Vec<StorageConfig>,
    options: RestoreOptions,
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

    pub async fn decide_restore_info(&self) -> Result<Option<(RestoreInfo, StorageClient)>> {
        let mut latest_restore_info: Option<(RestoreInfo, StorageClient)> = None;

        for config in &self.config {
            let client = StorageClient::try_create(self.db.clone(), config.clone())?;
            let restore_info = match client.restore_info().await? {
                Some(snapshot_into) => snapshot_into,
                None => continue,
            };
            match &latest_restore_info {
                Some(ls) => {
                    if restore_info.snapshot.generation > ls.0.snapshot.generation {
                        latest_restore_info = Some((restore_info, client));
                    }
                }
                None => {
                    latest_restore_info = Some((restore_info, client));
                }
            }
        }

        Ok(latest_restore_info)
    }

    async fn restore_snapshot(
        &self,
        client: &StorageClient,
        snapshot: &SnapshotInfo,
        path: &str,
    ) -> Result<()> {
        let compressed_data = client.read_snapshot(snapshot).await?;
        let decompressed_data = decompressed_data(compressed_data)?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        file.write_all(&decompressed_data)?;

        Ok(())
    }

    async fn apply_wal_frames(
        &self,
        client: &StorageClient,
        wal_segments: &[WalSegmentInfo],
        db_path: &str,
    ) -> Result<()> {
        let connection = Connection::open(db_path)?;
        let wal_file_name = format!("{}-wal", db_path);
        let sql = "PRAGMA wal_checkpoin(TRUNCATE)".to_string();

        for wal_segment in wal_segments {
            let compressed_data = client.read_wal_segment(wal_segment).await?;
            let decompressed_data = decompressed_data(compressed_data)?;

            let mut wal_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&wal_file_name)?;

            wal_file.write_all(&decompressed_data)?;
            if let Err(e) = connection.execute_batch(&sql) {
                error!(
                    "truncation checkpoint failed during restore {}:{}",
                    wal_segment.index, wal_segment.offset
                );
                return Err(e.into());
            }
        }

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        // Ensure output path does not already exist.
        if !self.options.overwrite && fs::exists(&self.options.output)? {
            println!("db {} already exists but cannot overwrite", self.db);
            return Err(Error::OverwriteDbError("cannot overwrite exist db"));
        }

        let (latest_restore_info, client) = match self.decide_restore_info().await? {
            Some(latest_restore_info) => latest_restore_info,
            None => {
                debug!("cannot find snapshot");
                return Ok(());
            }
        };

        // create a temp file to write snapshot
        let temp_output = format!("{}.tmp", self.options.output);

        let dir = parent_dir(&temp_output).unwrap();
        fs::create_dir_all(&dir)?;

        // restore snapshot
        self.restore_snapshot(&client, &latest_restore_info.snapshot, &temp_output)
            .await?;

        // apply wal frames
        self.apply_wal_frames(&client, &latest_restore_info.wal_segments, &temp_output)
            .await?;

        // rename the temp file to output file
        fs::rename(&temp_output, &self.options.output)?;

        Ok(())
    }
}

pub async fn run_restore(config: &DbConfig, options: &RestoreOptions) -> Result<()> {
    let restore =
        Restore::try_create(config.db.clone(), config.replicate.clone(), options.clone())?;

    restore.run().await?;

    Ok(())
}
