use std::fs;
use std::fs::File;
use std::fs::FileType;
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

use log::debug;
use log::error;
use log::info;
use lz4::EncoderBuilder;
use parking_lot::Mutex;
use rusqlite::Connection;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use uuid::timestamp;
use uuid::NoContext;
use uuid::Uuid;

use crate::base::format_integer_with_leading_zeros;
use crate::base::format_wal_path;
use crate::base::generation_file_path;
use crate::base::generations_dir;
use crate::base::parse_wal_path;
use crate::config::DatabaseConfig;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::align_frame;
use crate::sqlite::read_last_checksum;
use crate::sqlite::WALFrame;
use crate::sqlite::WALHeader;
use crate::sqlite::CHECKPOINT_MODE_PASSIVE;
use crate::sqlite::WALFRAME_HEADER_SIZE;
use crate::sync::Sync;
use crate::sync::SyncCommand;

const GENERATION_LEN: usize = 32;

// MaxIndex is the maximum possible WAL index.
// If this index is reached then a new generation will be started.
const MAX_WAL_INDEX: u64 = 0x7FFFFFFF;

#[derive(Clone, Debug)]
pub enum DbCommand {
    Snapshot(usize),
}

pub struct Database {
    config: DatabaseConfig,

    // Path to the database metadata.
    meta_dir: String,

    // shadow wal directory, empty when there is no shadow wal file
    shadow_wal_dir: String,

    // full wal file name of db file
    wal_file: String,
    page_size: u32,

    connection: Connection,

    // database connection for transaction, None if there if no tranction
    tx_connection: Option<Connection>,

    // for sync
    sync_notifiers: Vec<Sender<SyncCommand>>,
    // sync: Vec<Sync>,
    sync_handle: Vec<JoinHandle<()>>,

    // checkpoint mutex
    checkpoint_mutex: Mutex<()>,
}

#[derive(Debug, Clone)]
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

impl Default for WalGenerationPos {
    fn default() -> Self {
        Self {
            generation: "".to_string(),
            index: 0,
            offset: 0,
        }
    }
}

#[derive(Debug)]
struct SyncInfo {
    pub generation: String,
    pub index: u64,
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
            "CREATE TABLE IF NOT EXISTS _litesync_seq (id INTEGER PRIMARY KEY, seq INTEGER);",
            (),
        )?;

        connection.execute(
            "CREATE TABLE IF NOT EXISTS _litesync_lock (id INTEGER);",
            (),
        )?;
        Ok(())
    }

    // acquire_read_lock begins a read transaction on the database to prevent checkpointing.
    fn acquire_read_lock(&mut self) -> Result<()> {
        if self.tx_connection.is_none() {
            let tx_connection = Connection::open(&self.config.path)?;
            // Execute read query to obtain read lock.
            tx_connection.execute_batch("BEGIN;SELECT COUNT(1) FROM _litesync_seq;")?;
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

    // init litesync directory
    fn init_directory(config: &DatabaseConfig) -> Result<String> {
        let file_path = PathBuf::from(&config.path);
        let db_name = file_path.file_name().unwrap().to_str().unwrap();
        let dir_path = file_path.parent().unwrap_or_else(|| Path::new("."));
        let meta_dir = format!("{}/.{}-litesync/", dir_path.to_str().unwrap(), db_name,);
        fs::create_dir_all(&meta_dir)?;

        Ok(meta_dir)
    }

    fn try_create(config: DatabaseConfig) -> Result<(Self, Receiver<DbCommand>)> {
        info!("start database with config: {:?}\n", config);
        let connection = Connection::open(&config.path)?;

        Database::init_params(&connection)?;

        Database::create_internal_tables(&connection)?;

        let page_size = connection.pragma_query_value(None, "page_size", |row| row.get(0))?;
        let wal_file = format!("{}-wal", config.path);

        // init path
        let meta_dir = Database::init_directory(&config)?;

        // init replicate
        let (db_notifier, db_receiver) = mpsc::channel(16);
        let mut sync_handle = Vec::with_capacity(config.replicate.len());
        let mut sync_notifiers = Vec::with_capacity(config.replicate.len());
        let db = Path::new(&config.path)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        for (index, replicate) in config.replicate.iter().enumerate() {
            let (sync_notifier, sync_receiver) = mpsc::channel(16);
            let s = Sync::new(replicate.clone(), db.clone(), index, db_notifier.clone())?;
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
            shadow_wal_dir: "".to_string(),
            tx_connection: None,
            sync_notifiers,
            sync_handle,
            checkpoint_mutex: Mutex::new(()),
        };

        db.acquire_read_lock()?;

        Ok((db, db_receiver))
    }

    async fn sync(&mut self) -> Result<()> {
        debug!("sync database: {}", self.config.path);

        // make sure wal file has at least one frame in it
        if let Err(e) = self.ensure_wal_exists() {
            error!("ensure_wal_exists error: {:?}", e);
            return Ok(());
        }

        // Verify our last sync matches the current state of the WAL.
        // This ensures that we have an existing generation & that the last sync
        // position of the real WAL hasn't been overwritten by another process.
        let ret = self.verify();
        if let Err(e) = ret {
            if e.code() == Error::STORAGE_NOT_FOUND {
                debug!("generation not exists, try to create new generation...");
                self.create_generation()?;
            } else {
                error!("verify error: {:?}", e);
                return Err(e);
            }
        }

        // notify the database has been changed
        let generation_pos = self.wal_generation_position()?;
        self.sync_notifiers[0]
            .send(SyncCommand::DbChanged(generation_pos))
            .await?;

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

    // returns the path of a single shadow WAL file.
    fn shadow_wal_file(&self, generation: &str, index: u64) -> String {
        Path::new(&self.shadow_wal_dir(generation))
            .join(format_wal_path(index))
            .as_path()
            .to_str()
            .unwrap()
            .to_string()
    }

    // returns the path of the shadow wal directory
    fn shadow_wal_dir(&self, generation: &str) -> String {
        Path::new(&generations_dir(&self.meta_dir, generation))
            .join("wal")
            .as_path()
            .to_str()
            .unwrap()
            .to_string()
    }

    fn create_generation(&mut self) -> Result<()> {
        // Generate random generation using UUID V7
        let timestamp = timestamp::Timestamp::now(NoContext);
        let generation = Uuid::new_v7(timestamp).as_simple().to_string();
        fs::write(generation_file_path(&self.meta_dir), generation.clone())?;

        // Generate new directory.
        let wal_dir_path = self.shadow_wal_dir(&generation);
        debug!("create new wal dir {:?}", wal_dir_path);
        fs::create_dir_all(&wal_dir_path)?;

        self.shadow_wal_dir = wal_dir_path;
        // init first(index 0) shadow wal file
        self.init_shadow_wal_file(self.shadow_wal_file(&generation, 0))?;
        Ok(())
    }

    fn init_shadow_wal_file(&self, shadow_wal: String) -> Result<()> {
        debug!("init_shadow_wal_file {}", shadow_wal);

        // read wal file header
        let wal_header = WALHeader::read(&self.wal_file)?;
        if wal_header.page_size != self.page_size {
            return Err(Error::SqliteWalHeaderError("Invalid page size"));
        }

        // create new shadow wal file
        let db_file_metadata = fs::metadata(&self.config.path)?;
        let mode = db_file_metadata.mode();
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
        self.copy_to_shadow_wal(shadow_wal)?;
        Ok(())
    }

    // return original wal file size and new wal size
    fn copy_to_shadow_wal(&self, shadow_wal: String) -> Result<(u64, u64)> {
        let wal_file_metadata = fs::metadata(&self.wal_file)?;
        let orig_wal_size = align_frame(self.page_size, wal_file_metadata.size());

        let shadow_wal_file_metadata = fs::metadata(&shadow_wal)?;
        let orig_shadow_wal_size = align_frame(self.page_size, shadow_wal_file_metadata.size());
        debug!(
            "copy_to_shadow_wal orig_wal_size: {},  orig_shadow_wal_size: {}",
            orig_wal_size, orig_shadow_wal_size
        );

        // read shadow wal header
        let wal_header = WALHeader::read(&shadow_wal)?;

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

        let mut shadow_wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&shadow_wal)?;
        wal_file.seek(SeekFrom::Start(orig_shadow_wal_size))?;
        // read last checksum of shadow wal file
        let (ck1, ck2) = read_last_checksum(&mut shadow_wal_file, self.page_size)?;
        let mut offset = orig_shadow_wal_size;
        let mut last_commit_size = orig_shadow_wal_size;
        // Read through WAL from last position to find the page of the last
        // committed transaction.
        loop {
            let wal_frame = WALFrame::read(&mut wal_file, ck1, ck2, self.page_size, &wal_header);
            let wal_frame = match wal_frame {
                Ok(wal_frame) => wal_frame,
                Err(e) => {
                    if e.code() == Error::UNEXPECTED_E_O_F_ERROR {
                        debug!("copy_to_shadow_wal end of file at offset: {}", offset);
                        break;
                    } else {
                        error!("copy_to_shadow_wal error: {}", e);
                        return Err(e);
                    }
                }
            };

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
        let mut shadow_wal_file = OpenOptions::new().append(true).open(&shadow_wal)?;
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

        if stat.len() >= WALFRAME_HEADER_SIZE as u64 {
            return Ok(());
        }

        // create transaction that updates the internal table.
        self.connection.execute(
            "INSERT INTO _litesync_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1;",
            (),
        )?;

        Ok(())
    }

    // current_shadow_index returns the current WAL index & total size.
    fn current_shadow_index(&self, generation: &str) -> Result<(u64, u64)> {
        let wal_dir_path = self.shadow_wal_dir(generation);
        let entries = fs::read_dir(&wal_dir_path)?;

        let mut total_size = 0;
        let mut index = 0;
        for entry in entries {
            if let Ok(entry) = entry {
                let file_type = entry.file_type()?;
                if !file_type.is_file() {
                    continue;
                }
                let metadata = entry.metadata()?;
                total_size += metadata.size();
                let file_name = entry.file_name().into_string().unwrap();
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
        if total_size == 0 {
            return Err(Error::StorageNotFound("no shadow wal file"));
        }
        Ok((index, total_size))
    }

    // current_generation returns the name of the generation saved to the "generation"
    // file in the meta data directory.
    // Returns error if none exists.
    fn current_generation(&self) -> Result<String> {
        let generation_file = generation_file_path(&self.meta_dir);
        debug!("read generation_file: {:?}", generation_file);
        let generation = fs::read_to_string(&generation_file)?;
        debug!(
            "after read generation_file: {:?}, {:?}",
            generation_file, generation
        );
        if generation.len() != GENERATION_LEN {
            return Err(Error::StorageNotFound("empty generation"));
        }

        Ok(generation)
    }

    // verify ensures the current shadow WAL state matches where it left off from
    // the real WAL. Returns generation & WAL sync information. If info.reason is
    // not blank, verification failed and a new generation should be started.
    fn verify(&mut self) -> Result<SyncInfo> {
        // get existing generation
        let generation = self.current_generation()?;
        let db_file = fs::metadata(&self.config.path)?;
        let db_size = db_file.len();
        let db_mod_time = db_file.modified()?;

        // total bytes of real WAL.
        let wal_file = fs::metadata(&self.wal_file)?;
        let wal_size = align_frame(self.page_size, wal_file.len());

        // get current shadow wal index
        let (index, _total_size) = self.current_shadow_index(&generation)?;
        if index > MAX_WAL_INDEX {
            return Err(Error::ExceedMaxWalIndex("exceed max wal index"));
        }
        let shadow_wal_file = self.shadow_wal_file(&generation, index);

        // Determine shadow WAL current size.

        Ok(SyncInfo { generation, index })
    }

    fn checkpoint(&mut self, mode: &str) -> Result<()> {
        let generation = self.current_generation()?;

        self.do_checkpoint(&generation, mode)
    }

    // checkpoint performs a checkpoint on the WAL file and initializes a
    // new shadow WAL file.
    fn do_checkpoint(&mut self, generation: &str, mode: &str) -> Result<()> {
        if self.checkpoint_mutex.try_lock().is_none() {
            return Ok(());
        }

        // Execute checkpoint and immediately issue a write to the WAL to ensure
        // a new page is written.
        self.exec_checkpoint(mode)?;
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

        // Open db file descriptor
        let mut reader = OpenOptions::new().read(true).open(&self.config.path)?;
        let bytes = reader.metadata()?.len() as usize;
        let mut buffer = Vec::with_capacity(bytes);
        let mut encoder = EncoderBuilder::new().build(&mut buffer)?;

        let mut temp_buffer = vec![0; 10240];

        loop {
            let bytes_read = reader.read(&mut temp_buffer)?;
            if bytes_read == 0 {
                break; // EOF
            }
            encoder.write_all(&temp_buffer[..bytes_read])?;
        }
        let (compressed_data, result) = encoder.finish();
        result?;

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

pub async fn run_database(config: DatabaseConfig) -> Result<()> {
    let (mut database, mut db_receiver) = Database::try_create(config)?;
    let timeout_duration = Duration::from_secs(1);
    loop {
        select! {
            cmd = db_receiver.recv() => {
                //s.command(cmd).await?
                println!("cmd: {:?}", cmd);
                match cmd {
                    Some(cmd) => database.handle_db_command(cmd).await?,
                    None => {}
                }
            }
            _ = sleep(timeout_duration) => {
                database.sync().await?;
            }
        }
    }
    Ok(())
}
