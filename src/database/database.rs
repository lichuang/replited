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
use rusqlite::Connection;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tokio::task::JoinHandle;
use uuid::timestamp;
use uuid::NoContext;
use uuid::Uuid;

use crate::base::format_integer_with_leading_zeros;
use crate::config::DatabaseConfig;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::align_frame;
use crate::sqlite::read_last_checksum;
use crate::sqlite::WALFrame;
use crate::sqlite::WALHeader;
use crate::sqlite::WALFRAME_HEADER_SIZE;
use crate::sync::Sync;
use crate::sync::SyncCommand;

const GenerationLen: usize = 32;

struct Database {
    config: DatabaseConfig,
    generations_dir: String,
    generation_path: String,
    shadow_wal_dir: String,
    db_dir: String,
    wal_file: String,
    page_size: u32,

    connection: Connection,

    // database connection for transaction
    tx_connection: Option<Connection>,

    // for sync
    sync_notifier: Sender<SyncCommand>,
    sync: Vec<Arc<Sync>>,
    sync_handle: Vec<JoinHandle<()>>,
}

#[derive(Debug)]
struct SyncInfo {
    pub generation: String,
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

    fn init_directory(config: &DatabaseConfig) -> Result<(String, String, String)> {
        let file_path = PathBuf::from(&config.path);
        let db_name = file_path.file_name().unwrap().to_str().unwrap();
        let dir_path = file_path.parent().unwrap_or_else(|| Path::new("."));
        let generations_dir = format!(
            "{}/.{}-litesync/generations/",
            dir_path.to_str().unwrap(),
            db_name,
        );
        let generation_path = format!(
            "{}/.{}-litesync/generation",
            dir_path.to_str().unwrap(),
            db_name,
        );
        fs::create_dir_all(&generations_dir)?;

        Ok((
            dir_path.to_str().unwrap().to_string(),
            generations_dir,
            generation_path,
        ))
    }

    fn try_create(config: DatabaseConfig) -> Result<Self> {
        info!("start database with config: {:?}\n", config);
        let connection = Connection::open(&config.path)?;

        Database::init_params(&connection)?;

        Database::create_internal_tables(&connection)?;

        let page_size = connection.pragma_query_value(None, "page_size", |row| row.get(0))?;
        let wal_file = format!("{}-wal", config.path);

        // init path
        let (db_dir, generations_dir, generation_path) = Database::init_directory(&config)?;

        // init replicate
        let (sync_notifier, rx) = broadcast::channel(16);
        let mut sync = Vec::with_capacity(config.replicate.len());
        let mut sync_handle = Vec::with_capacity(config.replicate.len());
        for replicate in &config.replicate {
            let s = Sync::new(replicate.clone())?;
            let h = Sync::start(s.clone(), rx.resubscribe())?;
            sync_handle.push(h);
            sync.push(s);
        }

        let mut db = Self {
            config,
            connection,
            db_dir,
            generations_dir,
            generation_path,
            wal_file,
            page_size,
            shadow_wal_dir: "".to_string(),
            tx_connection: None,
            sync_notifier,
            sync,
            sync_handle,
        };

        db.acquire_read_lock()?;
        Ok(db)
    }

    fn sync(&mut self) -> Result<()> {
        debug!("sync database: {}", self.config.path);

        if let Err(e) = self.ensure_wal_exists() {
            error!("ensure_wal_exists error: {:?}", e);
            return Ok(());
        }

        let ret = self.verify();
        if let Err(e) = ret {
            error!("verify fail: {:?}", e);
            if e.code() == Error::STORAGE_NOT_FOUND {
                debug!("generation not exists, try to create new generation...");
                self.create_generation()?;
            } else {
                error!("verify error: {:?}", e);
                return Err(e);
            }
        }

        // notify the database has been changed
        self.sync_notifier.send(SyncCommand::DbChanged)?;

        Ok(())
    }

    fn shadow_wal_file(&self, index: u32) -> String {
        let shadow_wal_dir = &self.shadow_wal_dir;
        let i = format_integer_with_leading_zeros(index);
        let file = format!("{}.wal", i);
        Path::new(shadow_wal_dir)
            .join(file)
            .as_path()
            .to_str()
            .unwrap()
            .to_string()
    }

    fn create_generation(&mut self) -> Result<()> {
        // Generate random generation using UUID V7
        let timestamp = timestamp::Timestamp::now(NoContext);
        let generation = Uuid::new_v7(timestamp).as_simple().to_string();
        fs::write(&self.generation_path, generation.clone())?;

        // Generate new directory.
        let wal_dir_path = Path::new(&self.generations_dir)
            .join(generation)
            .join("wal");
        debug!("create new wal dir {:?}", wal_dir_path);
        fs::create_dir_all(&wal_dir_path)?;

        let wal_dir_path = wal_dir_path.as_path().to_str().unwrap();
        self.shadow_wal_dir = wal_dir_path.to_string();
        self.init_shadow_wal_file(self.shadow_wal_file(0))?;
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
        shadow_wal_file.write(&wal_header.data)?;
        shadow_wal_file.flush()?;
        debug!("create shadow wal file {}", shadow_wal);

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

        let _ = self.connection.execute(
            "INSERT INTO _litesync_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1;",
            (),
        )?;

        Ok(())
    }

    // parse_wal_path returns the index for the WAL file.
    // Returns an error if the path is not a valid WAL path.
    // fn parse_wal_path(&self, wal_file: &str) -> Result<i32> {}

    // current_shadow_wal_index returns the current WAL index & total size.
    fn current_shadow_wal_index(&self, generation: &String) -> Result<()> {
        let wal_dir_path = Path::new(&self.generations_dir)
            .join(generation)
            .join("wal");
        let entries = fs::read_dir(&wal_dir_path)?;

        let mut total_size = 0;
        for entry in entries {
            if let Ok(entry) = entry {
                let file_type = entry.file_type()?;
                if file_type.is_file() {
                    let metadata = entry.metadata()?;
                    total_size += metadata.size();
                }
            }
        }
        Ok(())
    }

    // current_generation returns the name of the generation saved to the "generation"
    // file in the meta data directory.
    // Returns error if none exists.
    fn current_generation(&self) -> Result<String> {
        debug!("read generation_path: {:?}", self.generation_path);
        let generation = fs::read_to_string(&self.generation_path)?;
        debug!(
            "after read generation_path: {:?}, {:?}",
            self.generation_path, generation
        );
        if generation.len() != GenerationLen {
            return Err(Error::StorageNotFound("empty generation"));
        }

        Ok(generation)
    }

    // verify ensures the current shadow WAL state matches where it left off from
    // the real WAL. Returns generation & WAL sync information. If info.reason is
    // not blank, verification failed and a new generation should be started.
    fn verify(&mut self) -> Result<SyncInfo> {
        let generation = self.current_generation()?;
        let db_file = fs::metadata(&self.config.path)?;
        let db_size = db_file.len();
        let db_mod_time = db_file.modified()?;

        let wal_file = fs::metadata(&self.wal_file)?;
        let wal_size = align_frame(self.page_size, wal_file.len());

        self.current_shadow_wal_index(&generation)?;
        Ok(SyncInfo { generation })
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.release_read_lock();
    }
}

pub async fn run_database(config: DatabaseConfig) -> Result<()> {
    let mut database = Database::try_create(config)?;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        database.sync()?;
    }
    Ok(())
}
