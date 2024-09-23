use chrono::DateTime;
use chrono::Utc;
use opendal::Metakey;
use opendal::Operator;

use super::init_operator;
use crate::base::parse_snapshot_path;
use crate::base::parse_wal_segment_path;
use crate::base::snapshot_file;
use crate::base::snapshots_dir;
use crate::base::walsegment_file;
use crate::base::walsegments_dir;
use crate::config::StorageConfig;
use crate::database::WalGenerationPos;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct StorageClient {
    operator: Operator,
    root: String,
    db: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub generation: String,
    pub index: u64,
    pub size: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WalSegmentInfo {
    pub generation: String,
    pub index: u64,
    pub offset: u64,
    pub size: u64,
}

impl StorageClient {
    pub fn new(db: String, config: StorageConfig) -> Result<Self> {
        Ok(Self {
            root: config.params.root(),
            operator: init_operator(&config.params)?,
            db,
        })
    }

    pub async fn write_wal_segment(
        &self,
        pos: &WalGenerationPos,
        compressed_data: Vec<u8>,
    ) -> Result<()> {
        let file = walsegment_file(&self.db, &pos.generation, pos.index, pos.offset);

        self.operator.write(&file, compressed_data).await?;

        Ok(())
    }

    pub async fn write_snapshot(
        &self,
        pos: &WalGenerationPos,
        compressed_data: Vec<u8>,
    ) -> Result<SnapshotInfo> {
        let snapshot_file = snapshot_file(&self.db, &pos.generation, pos.index);
        let snapshot_info = SnapshotInfo {
            generation: pos.generation.to_string(),
            index: pos.index,
            size: compressed_data.len() as u64,
            created_at: Utc::now(),
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
            .metakey(Metakey::LastModified)
            .await?;

        let mut snapshots = vec![];
        for entry in entries {
            let metadata = entry.metadata();
            let index = parse_snapshot_path(entry.name())?;
            snapshots.push(SnapshotInfo {
                generation: generation.to_string(),
                index,
                size: metadata.content_length(),
                created_at: metadata.last_modified().unwrap(),
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

    pub async fn read_wal_segment(&self, info: &WalSegmentInfo) -> Result<Vec<u8>> {
        let generation = &info.generation;
        let index = info.index;
        let offset = info.offset;

        let wal_segment_file = walsegment_file(&self.db, generation, index, offset);
        let bytes = self.operator.read(&wal_segment_file).await?.to_vec();
        Ok(bytes)
    }
}
