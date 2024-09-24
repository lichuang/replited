use chrono::DateTime;
use chrono::Utc;
use opendal::Metakey;
use opendal::Operator;

use super::init_operator;
use crate::base::parse_snapshot_path;
use crate::base::parse_wal_segment_path;
use crate::base::path_base;
use crate::base::remote_generations_dir;
use crate::base::snapshot_file;
use crate::base::snapshots_dir;
use crate::base::walsegment_file;
use crate::base::walsegments_dir;
use crate::base::Generation;
use crate::config::StorageConfig;
use crate::database::WalGenerationPos;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct StorageClient {
    operator: Operator,
    root: String,
    db_path: String,
    db_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotInfo {
    pub generation: Generation,
    pub index: u64,
    pub size: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WalSegmentInfo {
    pub generation: Generation,
    pub index: u64,
    pub offset: u64,
    pub size: u64,
}

impl StorageClient {
    pub fn try_create(db_path: String, config: StorageConfig) -> Result<Self> {
        Ok(Self {
            root: config.params.root(),
            operator: init_operator(&config.params)?,
            db_name: path_base(&db_path)?,
            db_path,
        })
    }

    pub async fn write_wal_segment(
        &self,
        pos: &WalGenerationPos,
        compressed_data: Vec<u8>,
    ) -> Result<()> {
        let file = walsegment_file(
            &self.db_path,
            pos.generation.as_str(),
            pos.index,
            pos.offset,
        );

        self.operator.write(&file, compressed_data).await?;

        Ok(())
    }

    pub async fn write_snapshot(
        &self,
        pos: &WalGenerationPos,
        compressed_data: Vec<u8>,
    ) -> Result<SnapshotInfo> {
        let snapshot_file = snapshot_file(&self.db_path, pos.generation.as_str(), pos.index);
        let snapshot_info = SnapshotInfo {
            generation: pos.generation.clone(),
            index: pos.index,
            size: compressed_data.len() as u64,
            created_at: Utc::now(),
        };
        self.operator.write(&snapshot_file, compressed_data).await?;

        Ok(snapshot_info)
    }

    pub async fn snapshots(&self, generation: &str) -> Result<Vec<SnapshotInfo>> {
        let generation = Generation::try_create(generation)?;
        let snapshots_dir = snapshots_dir(&self.db_path, generation.as_str());
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
                generation: generation.clone(),
                index,
                size: metadata.content_length(),
                created_at: metadata.last_modified().unwrap(),
            })
        }

        Ok(snapshots)
    }

    pub async fn wal_segments(&self, generation: &str) -> Result<Vec<WalSegmentInfo>> {
        let generation = Generation::try_create(generation)?;
        let walsegments_dir = walsegments_dir(&self.db_path, generation.as_str());
        let entries = self
            .operator
            .list_with(&walsegments_dir)
            .metakey(Metakey::ContentLength)
            .await?;

        let mut wal_segments = vec![];
        for entry in entries {
            let (index, offset) = parse_wal_segment_path(entry.name())?;
            wal_segments.push(WalSegmentInfo {
                generation: generation.clone(),
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

        let wal_segment_file = walsegment_file(&self.db_path, generation.as_str(), index, offset);
        let bytes = self.operator.read(&wal_segment_file).await?.to_vec();
        Ok(bytes)
    }

    pub async fn latest_snapshot(&self, generation: &str) -> Result<SnapshotInfo> {
        let dir = remote_generations_dir(&self.db_name);
        let entries = self.operator.list(&dir).await?;
        for entry in entries {
            let metadata = entry.metadata();
            if !metadata.is_dir() {
                continue;
            }
            let entry_generation = path_base(entry.name())?;
            println!("entry: {}", entry_generation);
            if !generation.is_empty() && entry_generation != generation {
                continue;
            }
        }

        Ok(SnapshotInfo::default())
    }
}
