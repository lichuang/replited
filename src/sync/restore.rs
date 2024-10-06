use std::fs;
use std::fs::OpenOptions;
use std::io::Write;

use log::debug;
use log::error;
use rusqlite::Connection;
use tempfile::NamedTempFile;

use crate::base::decompressed_data;
use crate::base::parent_dir;
use crate::config::DbConfig;
use crate::config::RestoreOptions;
use crate::config::StorageConfig;
use crate::error::Error;
use crate::error::Result;
use crate::storage::RestoreInfo;
use crate::storage::RestoreWalSegments;
use crate::storage::SnapshotInfo;
use crate::storage::StorageClient;
use crate::storage::WalSegmentInfo;

static WAL_CHECKPOINT_TRUNCATE: &str = "PRAGMA wal_checkpoint(TRUNCATE);";

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
        snapshot: &SnapshotInfo,
        wal_segments: &RestoreWalSegments,
        db_path: &str,
    ) -> Result<()> {
        debug!(
            "restore db {} apply wal segments: {:?}",
            self.db, wal_segments
        );
        let wal_file_name = format!("{}-wal", db_path);

        for (index, offsets) in wal_segments {
            let mut wal_decompressed_data = Vec::new();
            for offset in offsets {
                let wal_segment = WalSegmentInfo {
                    generation: snapshot.generation.clone(),
                    index: *index,
                    offset: *offset,
                    size: 0,
                };

                let compressed_data = client.read_wal_segment(&wal_segment).await?;
                let data = decompressed_data(compressed_data)?;
                wal_decompressed_data.extend_from_slice(&data);
            }

            // prepare db wal before open db connection
            let mut wal_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&wal_file_name)?;

            wal_file.write_all(&wal_decompressed_data)?;
            wal_file.flush()?;

            let connection = Connection::open(db_path)?;

            if let Err(e) = connection.query_row(WAL_CHECKPOINT_TRUNCATE, [], |_row| Ok(())) {
                error!(
                    "truncation checkpoint failed during restore {}:{:?}",
                    index, offsets
                );
                return Err(e.into());
            }

            // connection.close().unwrap();
        }

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        // Ensure output path does not already exist.
        if fs::exists(&self.options.output)? {
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
        let temp_file = NamedTempFile::new()?;
        let temp_file_name = temp_file.path().to_str().unwrap().to_string();

        let dir = parent_dir(&temp_file_name).unwrap();
        fs::create_dir_all(&dir)?;

        // restore snapshot
        self.restore_snapshot(&client, &latest_restore_info.snapshot, &temp_file_name)
            .await?;

        // apply wal frames
        self.apply_wal_frames(
            &client,
            &latest_restore_info.snapshot,
            &latest_restore_info.wal_segments,
            &temp_file_name,
        )
        .await?;

        // rename the temp file to output file
        fs::rename(&temp_file_name, &self.options.output)?;

        println!(
            "restore db {} to {} success",
            self.options.db, self.options.output
        );

        Ok(())
    }
}

pub async fn run_restore(config: &DbConfig, options: &RestoreOptions) -> Result<()> {
    let restore =
        Restore::try_create(config.db.clone(), config.replicate.clone(), options.clone())?;

    restore.run().await?;

    Ok(())
}
