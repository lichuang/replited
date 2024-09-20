use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use log::debug;
use log::error;
use log::info;
use parking_lot::Mutex;
use rusqlite::Connection;
use rusqlite::DropBehavior;
use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::task::block_in_place;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use uuid::timestamp;
use uuid::NoContext;
use uuid::Uuid;

use crate::base::compress_file;
use crate::base::generation_dir;
use crate::base::generation_file_path;
use crate::base::generations_dir;
use crate::base::parent_dir;
use crate::base::parse_wal_path;
use crate::base::path_base;
use crate::base::shadow_wal_dir;
use crate::base::shadow_wal_file;
use crate::config::ReplicateDbConfig;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::align_frame;
use crate::sqlite::checksum;
use crate::sqlite::read_last_checksum;
use crate::sqlite::WALFrame;
use crate::sqlite::WALHeader;
use crate::sqlite::CHECKPOINT_MODE_PASSIVE;
use crate::sqlite::CHECKPOINT_MODE_RESTART;
use crate::sqlite::CHECKPOINT_MODE_TRUNCATE;
use crate::sqlite::WAL_FRAME_HEADER_SIZE;
use crate::sqlite::WAL_HEADER_SIZE;
use crate::sync::Sync;
use crate::sync::SyncCommand;

const GENERATION_LEN: usize = 32;

// MaxIndex is the maximum possible WAL index.
// If this index is reached then a new generation will be started.
const MAX_WAL_INDEX: u64 = 0x7FFFFFFF;

// Default DB settings.
const DEFAULT_MONITOR_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub enum DbCommand {
    Snapshot(usize),
}

#[derive(Debug, Clone)]
pub struct DatabaseInfo {
    // Path to the database metadata.
    pub meta_dir: String,

    pub page_size: u64,
}

pub struct Database {
    config: ReplicateDbConfig,

    // Path to the database metadata.
    meta_dir: String,

    // full wal file name of db file
    wal_file: String,
    page_size: u64,

    connection: Connection,

    // database connection for transaction, None if there if no tranction
    tx_connection: Option<Connection>,

    // for sync
    sync_notifiers: Vec<Sender<SyncCommand>>,
    // sync: Vec<Sync>,
    sync_handle: Vec<JoinHandle<()>>,
    syncs: Vec<Arc<RwLock<Sync>>>,

    // checkpoint mutex
    checkpoint_mutex: Mutex<()>,
}

#[derive(Debug, Clone, Default)]
pub struct WalGenerationPos {
    pub generation: String,
    pub index: u64,
    pub offset: u64,
}

impl WalGenerationPos {
    pub fn is_empty(&self) -> bool {
        self.offset == 0
    }
}

#[derive(Debug, Default)]
struct SyncInfo {
    pub generation: String,
    pub db_mod_time: Option<SystemTime>,
    pub index: u64,
    pub wal_size: u64,
    pub shadow_wal_file: String,
    pub shadow_wal_size: u64,
    pub reason: Option<String>,
    pub restart: bool,
}

impl Database {
    fn init_params(connection: &Connection) -> Result<()> {
        // busy timeout
        connection.busy_timeout(Duration::from_secs(1))?;
        // PRAGMA journal_mode = wal;
        connection.pragma_update_and_check(None, "journal_mode", &"WAL", |param| {
            println!("journal_mode param: {:?}\n", param);
            Ok(())
        })?;
        // PRAGMA wal_autocheckpoint = 0;
        connection.pragma_update_and_check(None, "wal_autocheckpoint", &"0", |param| {
            println!("wal_autocheckpoint param: {:?}\n", param);
            Ok(())
        })?;

        Ok(())
    }

    fn create_internal_tables(connection: &Connection) -> Result<()> {
        connection.execute(
            "CREATE TABLE IF NOT EXISTS _replited_seq (id INTEGER PRIMARY KEY, seq INTEGER);",
            (),
        )?;

        connection.execute(
            "CREATE TABLE IF NOT EXISTS _replited_lock (id INTEGER);",
            (),
        )?;
        Ok(())
    }

    // acquire_read_lock begins a read transaction on the database to prevent checkpointing.
    fn acquire_read_lock(&mut self) -> Result<()> {
        if self.tx_connection.is_none() {
            let tx_connection = Connection::open(&self.config.db)?;
            // Execute read query to obtain read lock.
            tx_connection.execute_batch("BEGIN;SELECT COUNT(1) FROM _replited_seq;")?;
            self.tx_connection = Some(tx_connection);
        }

        Ok(())
    }

    fn release_read_lock(&mut self) -> Result<()> {
        // Rollback & clear read transaction.
        if let Some(tx) = &self.tx_connection {
            tx.execute_batch("ROLLBACK;")?;
            self.tx_connection = None;
        }

        Ok(())
    }

    // init replited directory
    fn init_directory(config: &ReplicateDbConfig) -> Result<String> {
        let file_path = PathBuf::from(&config.db);
        let db_name = file_path.file_name().unwrap().to_str().unwrap();
        let dir_path = file_path.parent().unwrap_or_else(|| Path::new("."));
        let meta_dir = format!("{}/.{}-replited/", dir_path.to_str().unwrap(), db_name,);
        fs::create_dir_all(&meta_dir)?;

        Ok(meta_dir)
    }

    fn try_create(config: ReplicateDbConfig) -> Result<(Self, Receiver<DbCommand>)> {
        info!("start database with config: {:?}\n", config);
        let connection = Connection::open(&config.db)?;

        Database::init_params(&connection)?;

        Database::create_internal_tables(&connection)?;

        let page_size = connection.pragma_query_value(None, "page_size", |row| row.get(0))?;
        let wal_file = format!("{}-wal", config.db);

        // init path
        let meta_dir = Database::init_directory(&config)?;

        // init replicate
        let (db_notifier, db_receiver) = mpsc::channel(16);
        let mut sync_handle = Vec::with_capacity(config.replicate.len());
        let mut sync_notifiers = Vec::with_capacity(config.replicate.len());
        let mut syncs = Vec::with_capacity(config.replicate.len());
        let info = DatabaseInfo {
            meta_dir: meta_dir.clone(),
            page_size: page_size,
        };
        let db = Path::new(&config.db)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        for (index, replicate) in config.replicate.iter().enumerate() {
            let (sync_notifier, sync_receiver) = mpsc::channel(16);
            let s = Sync::new(
                replicate.clone(),
                db.clone(),
                index,
                db_notifier.clone(),
                info.clone(),
            )?;
            syncs.push(s.clone());
            let h = Sync::start(s, sync_receiver)?;
            sync_handle.push(h);
            sync_notifiers.push(sync_notifier);
        }

        let mut db = Self {
            config: config.clone(),
            connection,
            meta_dir,
            wal_file,
            page_size,
            tx_connection: None,
            sync_notifiers,
            sync_handle,
            syncs,
            checkpoint_mutex: Mutex::new(()),
        };

        db.acquire_read_lock()?;

        Ok((db, db_receiver))
    }

    fn calc_wal_size(&self, n: u64) -> u64 {
        WAL_HEADER_SIZE + (WAL_FRAME_HEADER_SIZE + self.page_size) * n
    }

    // copy pending data from wal to shadow wal
    async fn sync(&mut self) -> Result<()> {
        debug!("sync database: {}", self.config.db);

        // make sure wal file has at least one frame in it
        self.ensure_wal_exists()?;

        // Verify our last sync matches the current state of the WAL.
        // This ensures that we have an existing generation & that the last sync
        // position of the real WAL hasn't been overwritten by another process.
        let mut info = self.verify()?;
        debug!("db {} sync info: {:?}", self.config.db, info);

        // Track if anything in the shadow WAL changes and then notify at the end.
        let mut changed =
            info.wal_size != info.shadow_wal_size || info.restart || info.reason.is_some();

        if let Some(reason) = &info.reason {
            // Start new generation & notify user via log message.
            info.generation = self.create_generation().await?;
            info!(
                "db {} sync new generation: {}, reason: {}",
                self.config.db, info.generation, reason
            );

            // Clear shadow wal info.
            info.shadow_wal_file = shadow_wal_file(&self.meta_dir, &info.generation, 0);
            info.shadow_wal_size = WAL_HEADER_SIZE;
            info.restart = false;
            info.reason = None;
        }

        // Synchronize real WAL with current shadow WAL.
        let (orig_wal_size, new_wal_size) = self.sync_wal(&info)?;

        // If WAL size is great than max threshold, force checkpoint.
        // If WAL size is greater than min threshold, attempt checkpoint.
        let mut checkpoint = false;
        let mut checkmode = CHECKPOINT_MODE_PASSIVE;
        if self.config.truncate_page_number > 0
            && orig_wal_size >= self.calc_wal_size(self.config.truncate_page_number)
        {
            checkpoint = true;
            checkmode = CHECKPOINT_MODE_TRUNCATE;
        } else if self.config.max_checkpoint_page_number > 0
            && new_wal_size >= self.calc_wal_size(self.config.max_checkpoint_page_number)
        {
            checkpoint = true;
            checkmode = CHECKPOINT_MODE_RESTART;
        } else if new_wal_size >= self.calc_wal_size(self.config.min_checkpoint_page_number) {
            checkpoint = true;
        } else if self.config.checkpoint_interval_secs > 0 {
            if let Some(db_mod_time) = &info.db_mod_time {
                let now = SystemTime::now();

                if let Ok(duration) = now.duration_since(db_mod_time.clone()) {
                    if duration.as_secs() > self.config.checkpoint_interval_secs
                        && new_wal_size > self.calc_wal_size(1)
                    {
                        checkpoint = true;
                    }
                }
            }
        }

        if checkpoint {
            changed = true;

            self.do_checkpoint(&info.generation, checkmode)?;
        }

        // Clean up any old files.
        self.clean()?;

        // notify the database has been changed
        if changed {
            let generation_pos = self.wal_generation_position()?;
            self.sync_notifiers[0]
                .send(SyncCommand::DbChanged(generation_pos))
                .await?;
        }

        debug!("sync db {} ok", self.config.db);
        Ok(())
    }

    pub fn wal_generation_position(&self) -> Result<WalGenerationPos> {
        let generation = self.current_generation()?;

        let (index, _total_size) = self.current_shadow_index(&generation)?;

        let shadow_wal_file = self.shadow_wal_file(&generation, index);

        let file_metadata = fs::metadata(&shadow_wal_file)?;

        Ok(WalGenerationPos {
            generation,
            index,
            offset: align_frame(self.page_size, file_metadata.size()),
        })
    }

    // CurrentShadowWALPath returns the path to the last shadow WAL in a generation.
    fn current_shadow_wal_file(&self, generation: &str) -> Result<String> {
        let (index, _total_size) = self.current_shadow_index(&generation)?;

        Ok(self.shadow_wal_file(&generation, index))
    }

    // returns the path of a single shadow WAL file.
    fn shadow_wal_file(&self, generation: &str, index: u64) -> String {
        shadow_wal_file(&self.meta_dir, generation, index)
    }

    // create_generation initiates a new generation by establishing the generation
    // directory, capturing snapshots for each replica, and refreshing the current
    // generation name.
    async fn create_generation(&mut self) -> Result<String> {
        // Generate random generation using UUID V7
        let timestamp = timestamp::Timestamp::now(NoContext);
        let generation = Uuid::new_v7(timestamp).as_simple().to_string();
        fs::write(generation_file_path(&self.meta_dir), generation.clone())?;

        // create new directory.
        let dir = generation_dir(&self.meta_dir, &generation);
        fs::create_dir_all(&dir)?;

        // init first(index 0) shadow wal file
        self.init_shadow_wal_file(&self.shadow_wal_file(&generation, 0))?;

        // Remove old generations.
        self.clean()?;

        Ok(generation)
    }

    // copies pending bytes from the real WAL to the shadow WAL.
    fn sync_wal(&self, info: &SyncInfo) -> Result<(u64, u64)> {
        let (orig_size, new_size) = self.copy_to_shadow_wal(&info.shadow_wal_file)?;

        if !info.restart {
            return Ok((orig_size, new_size));
        }

        // Parse index of current shadow WAL file.
        let index = parse_wal_path(&info.shadow_wal_file)?;

        // Start a new shadow WAL file with next index.
        let new_shadow_wal_file = shadow_wal_file(&self.meta_dir, &info.generation, index + 1);
        let new_size = self.init_shadow_wal_file(&new_shadow_wal_file)?;

        Ok((orig_size, new_size))
    }

    // remove old generations files and wal files
    fn clean(&mut self) -> Result<()> {
        self.clean_generations()?;

        block_in_place(move || {
            Handle::current().block_on(async {
                if let Err(e) = self.clean_wal().await {
                    error!("db {} clean wal error: {:?}", self.config.db, e);
                }
            })
        });

        Ok(())
    }

    fn clean_generations(&self) -> Result<()> {
        let generation = self.current_generation()?;
        let genetations_dir = generations_dir(&self.meta_dir);

        if !fs::exists(&genetations_dir)? {
            return Ok(());
        }
        for entry in fs::read_dir(&genetations_dir)? {
            let entry = entry?;
            let file_name = entry.file_name().as_os_str().to_str().unwrap().to_string();
            let base = path_base(&file_name)?;
            if base == generation {
                continue;
            }
            let path = Path::new(&genetations_dir)
                .join(&file_name)
                .as_path()
                .to_str()
                .unwrap()
                .to_string();
            fs::remove_dir_all(&path)?;
        }
        Ok(())
    }

    // removes WAL files that have been replicated.
    async fn clean_wal(&self) -> Result<()> {
        let generation = self.current_generation()?;

        let mut min = None;
        for sync in &self.syncs {
            // let sync = sync.read().await;
            let sync = sync.read().await;
            let mut position = sync.position();
            if position.generation != generation {
                position = WalGenerationPos::default();
            }
            match min {
                None => min = Some(position.index),
                Some(m) => {
                    if position.index < m {
                        min = Some(position.index);
                    }
                }
            }
        }

        // Skip if our lowest index is too small.
        let mut min = match min {
            None => return Ok(()),
            Some(min) => min,
        };

        if min <= 0 {
            return Ok(());
        }
        // Keep an extra WAL file.
        min -= 1;

        // Remove all WAL files for the generation before the lowest index.
        let dir = shadow_wal_dir(&self.meta_dir, &generation);
        if !fs::exists(&dir)? {
            return Ok(());
        }
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let file_name = entry.file_name().as_os_str().to_str().unwrap().to_string();
            let index = parse_wal_path(&file_name)?;
            if index >= min {
                continue;
            }
            let path = Path::new(&dir)
                .join(&file_name)
                .as_path()
                .to_str()
                .unwrap()
                .to_string();
            fs::remove_dir_all(&path)?;
        }

        Ok(())
    }

    fn init_shadow_wal_file(&self, shadow_wal: &String) -> Result<u64> {
        debug!("init_shadow_wal_file {}", shadow_wal);

        // read wal file header
        let wal_header = WALHeader::read(&self.wal_file)?;
        println!("after read");
        if wal_header.page_size != self.page_size {
            return Err(Error::SqliteInvalidWalHeaderError("Invalid page size"));
        }

        // create new shadow wal file
        let db_file_metadata = fs::metadata(&self.config.db)?;
        let mode = db_file_metadata.mode();
        let dir = parent_dir(&shadow_wal);
        if let Some(dir) = dir {
            fs::create_dir_all(&dir)?;
        } else {
            debug!("db {} cannot find parent dir of shadow wal", self.config.db);
        }
        let mut shadow_wal_file = fs::File::create(&shadow_wal)?;
        let mut permissions = shadow_wal_file.metadata()?.permissions();
        permissions.set_mode(mode);
        shadow_wal_file.set_permissions(permissions)?;
        std::os::unix::fs::chown(
            &shadow_wal,
            Some(db_file_metadata.uid()),
            Some(db_file_metadata.gid()),
        )?;
        // write wal file header into shadow wal file
        shadow_wal_file.write(&wal_header.data)?;
        shadow_wal_file.flush()?;
        debug!("create shadow wal file {}", shadow_wal);

        // copy wal file frame into shadow wal file
        let (_, new_size) = self.copy_to_shadow_wal(&shadow_wal)?;

        Ok(new_size)
    }

    // return original wal file size and new wal size
    fn copy_to_shadow_wal(&self, shadow_wal: &String) -> Result<(u64, u64)> {
        let wal_file_metadata = fs::metadata(&self.wal_file)?;
        let orig_wal_size = align_frame(self.page_size, wal_file_metadata.size());

        let shadow_wal_file_metadata = fs::metadata(shadow_wal)?;
        let orig_shadow_wal_size = align_frame(self.page_size, shadow_wal_file_metadata.size());
        debug!(
            "copy_to_shadow_wal orig_wal_size: {},  orig_shadow_wal_size: {}",
            orig_wal_size, orig_shadow_wal_size
        );

        // read shadow wal header
        let wal_header = WALHeader::read(shadow_wal)?;

        // copy wal frames into temp shadow wal file
        let temp_shadow_wal_file = format!("{}.tmp", shadow_wal);
        if let Err(e) = fs::remove_file(&temp_shadow_wal_file) {
            // ignore file not exists case
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(e.into());
            }
        }
        // create a temp file to copy wal frames, truncate if exists
        let mut temp_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_shadow_wal_file)?;

        // seek on real db wal
        let mut wal_file = File::open(&self.wal_file)?;
        wal_file.seek(SeekFrom::Start(orig_shadow_wal_size))?;
        // read last checksum of shadow wal file
        let (ck1, ck2) = read_last_checksum(&shadow_wal, self.page_size)?;
        let mut offset = orig_shadow_wal_size;
        let mut last_commit_size = orig_shadow_wal_size;
        // Read through WAL from last position to find the page of the last
        // committed transaction.
        loop {
            let wal_frame = WALFrame::read(&mut wal_file, self.page_size);
            let wal_frame = match wal_frame {
                Ok(wal_frame) => wal_frame,
                Err(e) => {
                    if e.code() == Error::UNEXPECTED_EOF_ERROR {
                        debug!("copy_to_shadow_wal end of file at offset: {}", offset);
                        break;
                    } else {
                        error!("copy_to_shadow_wal error: {}", e);
                        return Err(e);
                    }
                }
            };
            if wal_frame.salt1 != wal_header.salt1 || wal_frame.salt2 != wal_header.salt2 {
                debug!(
                    "db {} copy shadow wal frame salt mismatch at offset {}",
                    self.config.db, offset
                );
                break;
            }

            // frame header
            let (ck1, ck2) = checksum(&wal_frame.data[0..8], ck1, ck2, wal_header.is_big_endian);
            // frame data
            let (ck1, ck2) = checksum(&wal_frame.data[24..], ck1, ck2, wal_header.is_big_endian);
            if ck1 != wal_frame.checksum1 || ck2 != wal_frame.checksum2 {
                debug!(
                    "db {} copy shadow wal checksum mismatch at offset {}",
                    self.config.db, offset
                );
                break;
            }

            // Write page to temporary WAL file.
            temp_file.write(&wal_frame.data)?;

            offset += wal_frame.data.len() as u64;
            if wal_frame.db_size != 0 {
                last_commit_size = offset;
            }
        }

        // If no WAL writes found, exit.
        if last_commit_size == orig_shadow_wal_size {
            return Ok((orig_wal_size, last_commit_size));
        }

        // copy frames from temp file to shadow wal file
        temp_file.flush()?;
        temp_file.seek(SeekFrom::Start(0))?;

        let mut buffer = Vec::new();
        temp_file.read_to_end(&mut buffer)?;
        fs::remove_file(&temp_shadow_wal_file)?;

        // append wal frames to end of shadow wal file
        let mut shadow_wal_file = OpenOptions::new().append(true).open(shadow_wal)?;
        shadow_wal_file.write_all(&buffer).unwrap();

        Ok((orig_wal_size, last_commit_size))
    }

    // make sure wal file has at least one frame in it
    fn ensure_wal_exists(&self) -> Result<()> {
        let stat = fs::metadata(&self.wal_file)?;
        if !stat.is_file() {
            return Err(Error::SqliteWalError(format!(
                "wal {} is not a file",
                self.wal_file,
            )));
        }

        if stat.len() >= WAL_HEADER_SIZE {
            return Ok(());
        }

        // create transaction that updates the internal table.
        self.connection.execute(
            "INSERT INTO _replited_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1;",
            (),
        )?;

        Ok(())
    }

    // current_shadow_index returns the current WAL index & total size.
    fn current_shadow_index(&self, generation: &str) -> Result<(u64, u64)> {
        let wal_dir = shadow_wal_dir(&self.meta_dir, generation);
        if !fs::exists(&wal_dir)? {
            return Ok((0, 0));
        }

        let entries = fs::read_dir(&wal_dir)?;
        let mut total_size = 0;
        let mut index = 0;
        for entry in entries {
            if let Ok(entry) = entry {
                let file_type = entry.file_type()?;
                let file_name = entry.file_name().into_string().unwrap();
                if !fs::exists(&file_name)? {
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                let metadata = entry.metadata()?;
                total_size += metadata.size();
                match parse_wal_path(&file_name) {
                    Err(e) => {
                        debug!("invald wal file {:?}", file_name);
                        continue;
                    }
                    Ok(i) => {
                        if i > index {
                            index = i;
                        }
                    }
                }
            }
        }

        Ok((index, total_size))
    }

    // current_generation returns the name of the generation saved to the "generation"
    // file in the meta data directory.
    // Returns empty string if none exists.
    fn current_generation(&self) -> Result<String> {
        let generation_file = generation_file_path(&self.meta_dir);
        if !fs::exists(&generation_file)? {
            return Ok("".to_string());
        }
        let generation = fs::read_to_string(&generation_file)?;
        if generation.len() != GENERATION_LEN {
            return Ok("".to_string());
        }

        Ok(generation)
    }

    // verify ensures the current shadow WAL state matches where it left off from
    // the real WAL. Returns generation & WAL sync information. If info.reason is
    // not blank, verification failed and a new generation should be started.
    fn verify(&mut self) -> Result<SyncInfo> {
        let mut info = SyncInfo::default();

        // get existing generation
        let generation = self.current_generation()?;
        if generation.is_empty() {
            info.reason = Some("no generation exists".to_string());
            return Ok(info);
        }
        info.generation = generation;

        let db_file = fs::metadata(&self.config.db)?;
        if let Ok(db_mod_time) = db_file.modified() {
            info.db_mod_time = Some(db_mod_time);
        } else {
            info.db_mod_time = None;
        }

        // total bytes of real WAL.
        let wal_file = fs::metadata(&self.wal_file)?;
        let wal_size = align_frame(self.page_size, wal_file.len());
        info.wal_size = wal_size;

        // get current shadow wal index
        let (index, _total_size) = self.current_shadow_index(&info.generation)?;
        if index > MAX_WAL_INDEX {
            info.reason = Some("max index exceeded".to_string());
            return Ok(info);
        }
        info.shadow_wal_file = self.shadow_wal_file(&info.generation, index);

        // Determine shadow WAL current size.
        if !fs::exists(&info.shadow_wal_file)? {
            info.reason = Some("no shadow wal".to_string());
            return Ok(info);
        }
        let shadow_wal_file = fs::metadata(&info.shadow_wal_file)?;
        info.shadow_wal_size = align_frame(self.page_size, shadow_wal_file.len());
        if info.shadow_wal_size < WAL_HEADER_SIZE {
            info.reason = Some("short shadow wal".to_string());
            return Ok(info);
        }

        if info.shadow_wal_size > info.wal_size {
            info.reason = Some("wal truncated by another process".to_string());
            return Ok(info);
        }

        // Compare WAL headers. Start a new shadow WAL if they are mismatched.
        let wal_header = WALHeader::read(&self.wal_file)?;
        let shadow_wal_header = WALHeader::read(&info.shadow_wal_file)?;
        if wal_header != shadow_wal_header {
            info.restart = true;
        }

        if info.shadow_wal_size == WAL_HEADER_SIZE && info.restart {
            info.reason = Some("wal header only, mismatched".to_string());
            return Ok(info);
        }

        // Verify last page synced still matches.
        if info.shadow_wal_size > WAL_HEADER_SIZE {
            let offset = info.shadow_wal_size - self.page_size - WAL_FRAME_HEADER_SIZE;

            let mut wal_file = OpenOptions::new().read(true).open(&self.wal_file)?;
            wal_file.seek(SeekFrom::Start(offset))?;
            let wal_last_frame = WALFrame::read(&mut wal_file, self.page_size)?;

            let mut shadow_wal_file = OpenOptions::new().read(true).open(&info.shadow_wal_file)?;
            shadow_wal_file.seek(SeekFrom::Start(offset))?;
            let shadow_wal_last_frame = WALFrame::read(&mut shadow_wal_file, self.page_size)?;
            if wal_last_frame != shadow_wal_last_frame {
                info.reason = Some("wal overwritten by another process".to_string());
                return Ok(info);
            }
        }

        Ok(info)
    }

    fn checkpoint(&mut self, mode: &str) -> Result<()> {
        let generation = self.current_generation()?;

        self.do_checkpoint(&generation, mode)
    }

    // checkpoint performs a checkpoint on the WAL file and initializes a
    // new shadow WAL file.
    fn do_checkpoint(&mut self, generation: &str, mode: &str) -> Result<()> {
        // Try getting a checkpoint lock, will fail during snapshots.
        if self.checkpoint_mutex.try_lock().is_none() {
            return Ok(());
        }

        let shadow_wal_file = self.current_shadow_wal_file(generation)?;

        // Read WAL header before checkpoint to check if it has been restarted.
        let wal_header1 = WALHeader::read(&self.wal_file)?;

        // Copy shadow WAL before checkpoint to copy as much as possible.
        self.copy_to_shadow_wal(&shadow_wal_file)?;

        // Execute checkpoint and immediately issue a write to the WAL to ensure
        // a new page is written.
        self.exec_checkpoint(mode)?;

        self.connection.execute(
            "INSERT INTO _replited_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1;",
            (),
        )?;

        // If WAL hasn't been restarted, exit.
        let wal_header2 = WALHeader::read(&self.wal_file)?;
        if wal_header1 == wal_header2 {
            return Ok(());
        }

        // Start a transaction. This will be promoted immediately after.
        let mut connection = Connection::open(&self.config.db)?;
        let mut tx = connection.transaction()?;
        tx.set_drop_behavior(DropBehavior::Rollback);

        // Insert into the lock table to promote to a write tx. The lock table
        // insert will never actually occur because our tx will be rolled back,
        // however, it will ensure our tx grabs the write lock. Unfortunately,
        // we can't call "BEGIN IMMEDIATE" as we are already in a transaction.
        tx.execute("INSERT INTO _replited_lock (id) VALUES (1);", ())?;

        // Copy the end of the previous WAL before starting a new shadow WAL.
        self.copy_to_shadow_wal(&shadow_wal_file)?;

        // Parse index of current shadow WAL file.
        let index = parse_wal_path(&shadow_wal_file)?;

        // Start a new shadow WAL file with next index.
        let new_shadow_wal_file = self.shadow_wal_file(generation, index + 1);
        self.init_shadow_wal_file(&new_shadow_wal_file)?;

        Ok(())
    }

    fn exec_checkpoint(&mut self, mode: &str) -> Result<()> {
        // Ensure the read lock has been removed before issuing a checkpoint.
        // We defer the re-acquire to ensure it occurs even on an early return.
        self.release_read_lock()?;

        // A non-forced checkpoint is issued as "PASSIVE". This will only checkpoint
        // if there are not pending transactions. A forced checkpoint ("RESTART")
        // will wait for pending transactions to end & block new transactions before
        // forcing the checkpoint and restarting the WAL.
        //
        // See: https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
        let sql = format!("PRAGMA wal_checkpoint({})", mode);

        let ret = self.connection.execute_batch(&sql);

        // Reacquire the read lock immediately after the checkpoint.
        self.acquire_read_lock()?;

        let _ = ret?;

        Ok(())
    }

    pub async fn handle_db_command(&mut self, cmd: DbCommand) -> Result<()> {
        match cmd {
            DbCommand::Snapshot(i) => self.handle_db_snapshot_command(i).await?,
        }
        Ok(())
    }

    fn snapshot(&mut self) -> Result<Vec<u8>> {
        // Issue a passive checkpoint to flush any pages to disk before snapshotting.
        self.checkpoint(CHECKPOINT_MODE_PASSIVE)?;

        // Prevent internal checkpoints during snapshot.
        let _ = self.checkpoint_mutex.lock();

        // Acquire a read lock on the database during snapshot to prevent external checkpoints.
        self.acquire_read_lock()?;

        // Obtain current position.
        let pos = self.wal_generation_position()?;
        if pos.is_empty() {}

        // compress db file
        let compressed_data = compress_file(&self.config.db)?;

        Ok(compressed_data.to_owned())
    }

    async fn handle_db_snapshot_command(&mut self, index: usize) -> Result<()> {
        let compressed_data = self.snapshot()?;
        let generation_pos = self.wal_generation_position()?;
        self.sync_notifiers[index]
            .send(SyncCommand::Snapshot((generation_pos, compressed_data)))
            .await?;
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.release_read_lock();
    }
}

pub async fn run_database(config: ReplicateDbConfig) -> Result<()> {
    let (mut database, mut db_receiver) = Database::try_create(config)?;
    loop {
        select! {
            cmd = db_receiver.recv() => {
                match cmd {
                    Some(cmd) => if let Err(e) = database.handle_db_command(cmd).await {
                        error!("handle_db_command of db {} error: {:?}", database.config.db, e);
                    },
                    None => {}
                }
            }
            _ = sleep(DEFAULT_MONITOR_INTERVAL) => {
                if let Err(e) = database.sync().await {
                    error!("sync db {} error: {:?}", database.config.db, e);
                }
            }
        }
    }
}
