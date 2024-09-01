use log::info;
use opendal::Operator;

use super::init_operator;
use crate::base::parse_snapshot_path;
use crate::base::snapshots_dir;
use crate::config::StorageConfig;
use crate::config::StorageParams;
use crate::error::Result;

pub struct SyncClient {
    operator: Operator,
    root: String,
}

pub struct SnapshotInfo {
    generation: String,
    index: u64,
    size: u64,
}

impl SyncClient {
    pub fn new(config: StorageConfig) -> Result<Self> {
        Ok(Self {
            root: config.params.root(),
            operator: init_operator(&config.params)?,
        })
    }

    pub async fn snapshots(&self, generation: &str) -> Result<Vec<SnapshotInfo>> {
        let snapshots_dir = snapshots_dir(&self.root, generation);
        let entries = self.operator.list(&snapshots_dir).await?;

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
}
