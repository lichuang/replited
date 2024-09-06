use std::path::Path;

use log::info;
use opendal::Metakey;
use opendal::Operator;

use super::init_operator;
use crate::base::parse_snapshot_path;
use crate::base::parse_wal_path;
use crate::base::parse_wal_segment_path;
use crate::base::snapshot_file;
use crate::base::snapshots_dir;
use crate::base::walsegments_dir;
use crate::config::StorageConfig;
use crate::config::StorageParams;
use crate::error::Result;

pub struct SyncClient {
    operator: Operator,
    root: String,
    db: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub generation: String,
    pub index: u64,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct WalSegmentInfo {
    pub generation: String,
    pub index: u64,
    pub offset: u64,
    pub size: u64,
}

impl SyncClient {
    pub fn new(db: String, config: StorageConfig) -> Result<Self> {
        Ok(Self {
            root: config.params.root(),
            operator: init_operator(&config.params)?,
            db,
        })
    }

    pub async fn save_snapshot(
        &self,
        generation: &str,
        index: u64,
        compressed_data: Vec<u8>,
    ) -> Result<SnapshotInfo> {
        let snapshot_file = snapshot_file(&self.db, generation, index);

        let snapshot_info = SnapshotInfo {
            generation: generation.to_string(),
            index,
            size: compressed_data.len() as u64,
        };
        self.operator.write(&snapshot_file, compressed_data).await?;

        Ok(snapshot_info)
    }

    pub async fn snapshots(&self, generation: &str) -> Result<Vec<SnapshotInfo>> {
        let snapshots_dir = snapshots_dir(&self.db, generation);
        let entries = self
            .operator
            .list_with(&snapshots_dir)
            .metakey(Metakey::ContentLength)
            .await?;

        let mut snapshots = vec![];
        for entry in entries {
            let index = parse_snapshot_path(entry.name())?;
            snapshots.push(SnapshotInfo {
                generation: generation.to_string(),
                index,
                size: entry.metadata().content_length(),
            })
        }

        Ok(snapshots)
    }

    pub async fn wal_segments(&self, generation: &str) -> Result<Vec<WalSegmentInfo>> {
        let walsegments_dir = walsegments_dir(&self.db, generation);
        let entries = self
            .operator
            .list_with(&walsegments_dir)
            .metakey(Metakey::ContentLength)
            .await?;

        let mut wal_segments = vec![];
        for entry in entries {
            let (index, offset) = parse_wal_segment_path(entry.name())?;
            wal_segments.push(WalSegmentInfo {
                generation: generation.to_string(),
                index,
                offset,
                size: entry.metadata().content_length(),
            })
        }

        Ok(wal_segments)
    }
}
