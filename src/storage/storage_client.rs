use std::collections::BTreeMap;

use chrono::DateTime;
use chrono::Utc;
use log::error;
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
use crate::error::Error;
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

// restore wal_segments formats: vector<index, vector<offsets in order>>
pub type RestoreWalSegments = Vec<(u64, Vec<u64>)>;

#[derive(Debug)]
pub struct RestoreInfo {
    pub snapshot: SnapshotInfo,

    pub wal_segments: RestoreWalSegments,
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
        let snapshot_file = snapshot_file(&self.db_name, pos.generation.as_str(), pos.index);
        let snapshot_info = SnapshotInfo {
            generation: pos.generation.clone(),
            index: pos.index,
            size: compressed_data.len() as u64,
            created_at: Utc::now(),
        };
        self.operator.write(&snapshot_file, compressed_data).await?;

        Ok(snapshot_info)
    }

    pub async fn read_snapshot(&self, info: &SnapshotInfo) -> Result<Vec<u8>> {
        let snapshot_file = snapshot_file(&self.db_name, info.generation.as_str(), info.index);

        let data = self.operator.read(&snapshot_file).await?;

        Ok(data.to_vec())
    }

    pub async fn snapshots(&self, generation: &str) -> Result<Vec<SnapshotInfo>> {
        let generation = Generation::try_create(generation)?;
        let snapshots_dir = snapshots_dir(&self.db_name, generation.as_str());
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

    async fn max_snapshot(&self, generation: &str) -> Result<Option<SnapshotInfo>> {
        let generation = Generation::try_create(generation)?;
        let snapshots_dir = snapshots_dir(&self.db_name, generation.as_str());
        let entries = self
            .operator
            .list_with(&snapshots_dir)
            .metakey(Metakey::ContentLength)
            .metakey(Metakey::LastModified)
            .await?;

        let mut snapshot = None;
        let mut max_index = None;
        for entry in entries {
            let metadata = entry.metadata();
            let index = parse_snapshot_path(entry.name())?;
            let mut update = false;
            match max_index {
                Some(mi) => {
                    if index > mi {
                        update = true;
                    }
                }
                None => {
                    update = true;
                }
            }

            if !update {
                continue;
            }

            max_index = Some(index);
            snapshot = Some(SnapshotInfo {
                generation: generation.clone(),
                index,
                size: metadata.content_length(),
                created_at: metadata.last_modified().unwrap(),
            });
        }

        Ok(snapshot)
    }

    pub async fn wal_segments(&self, generation: &str) -> Result<Vec<WalSegmentInfo>> {
        let generation = Generation::try_create(generation)?;
        let walsegments_dir = walsegments_dir(&self.db_name, generation.as_str());
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

        let wal_segment_file = walsegment_file(&self.db_name, generation.as_str(), index, offset);
        let bytes = self.operator.read(&wal_segment_file).await?.to_vec();
        Ok(bytes)
    }

    async fn restore_wal_segments_of(&self, snapshot: &SnapshotInfo) -> Result<RestoreWalSegments> {
        let mut wal_segments = self.wal_segments(snapshot.generation.as_str()).await?;

        // sort wal segments first by index, then offset
        wal_segments.sort_by(|a, b| {
            let ordering = a.index.partial_cmp(&b.index).unwrap();
            if ordering.is_eq() {
                a.offset.partial_cmp(&b.offset).unwrap()
            } else {
                ordering
            }
        });

        let mut restore_wal_segments: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

        for wal_segment in wal_segments {
            if wal_segment.index < snapshot.index {
                continue;
            }

            match restore_wal_segments.get_mut(&wal_segment.index) {
                Some(offsets) => {
                    if *offsets.last().unwrap() >= wal_segment.offset {
                        let msg = format!(
                            "wal segment out of order, generation: {:?}, index: {}, offset: {}",
                            snapshot.generation.as_str(),
                            wal_segment.index,
                            wal_segment.offset
                        );
                        error!("{}", msg);
                        return Err(Error::InvalidWalSegmentError(msg));
                    }
                    offsets.push(wal_segment.offset);
                }
                None => {
                    if wal_segment.offset != 0 {
                        let msg = format!(
                            "missing initial wal segment, generation: {:?}, index: {}, offset: {}",
                            snapshot.generation.as_str(),
                            wal_segment.index,
                            wal_segment.offset
                        );
                        error!("{}", msg);
                        return Err(Error::InvalidWalSegmentError(msg));
                    }
                    restore_wal_segments.insert(wal_segment.index, vec![wal_segment.offset]);
                }
            }
        }

        Ok(restore_wal_segments.into_iter().collect())
    }

    pub async fn restore_info(&self) -> Result<Option<RestoreInfo>> {
        let dir = remote_generations_dir(&self.db_name);
        let entries = self.operator.list(&dir).await?;

        let mut entry_with_generation = Vec::with_capacity(entries.len());
        for entry in entries {
            let metadata = entry.metadata();
            if !metadata.is_dir() {
                continue;
            }
            let generation = path_base(entry.name())?;

            let generation = match Generation::try_create(&generation) {
                Ok(generation) => generation,
                Err(e) => {
                    error!(
                        "dir {} is not valid generation dir, err: {:?}",
                        generation, e
                    );
                    continue;
                }
            };

            entry_with_generation.push((entry, generation));
        }

        // sort the entries in reverse order
        entry_with_generation.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        for (_entry, generation) in entry_with_generation {
            let snapshot = match self.max_snapshot(generation.as_str()).await? {
                Some(snapshot) => snapshot,
                // if generation has no snapshot, ignore and skip to the next generation
                None => {
                    error!("dir {:?} has no snapshots", generation);
                    continue;
                }
            };

            // return only if wal segments in this snapshot is valid.
            if let Ok(wal_segments) = self.restore_wal_segments_of(&snapshot).await {
                return Ok(Some(RestoreInfo {
                    snapshot,
                    wal_segments,
                }));
            }
        }

        Ok(None)
    }
}
