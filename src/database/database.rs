use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use log::debug;
use log::error;
use log::info;
use rusqlite::Connection;
use tokio::sync::broadcast::Receiver;
use tokio::time::timeout;
use uuid::timestamp;
use uuid::NoContext;
use uuid::Uuid;

use crate::config::DatabaseConfig;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::WALHeader;
use crate::sqlite::WALFRAME_HEADER_SIZE;

struct Database {
    config: DatabaseConfig,
    connection: Connection,
    generations_dir: String,
    generation_path: String,
    db_dir: String,
    wal_file: String,
    page_size: u64,
}

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

        // for replicate in &self.config.replicate {}

        Ok(Self {
            config,
            connection,
            db_dir,
            generations_dir,
            generation_path,
            wal_file,
            page_size,
        })
    }

    fn sync(&mut self) -> Result<()> {
        if let Err(e) = self.ensure_wal_exists() {
            error!("ensure_wal_exists error: {:?}", e);
            return Ok(());
        }

        if let Err(e) = self.verify() {
            error!("verify fail: {:?}", e);
            if e.code() == Error::STORAGE_NOT_FOUND {
                debug!("try to create new generation...");
                self.create_generation()?;
            } else {
                error!("verify error: {:?}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    fn create_generation(&self) -> Result<()> {
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

        self.init_shadow_wal_file(wal_dir_path.as_path().to_str().unwrap())?;
        Ok(())
    }

    fn init_shadow_wal_file(&self, shadow_wal: &str) -> Result<()> {
        debug!("init_shadow_wal_file {}", shadow_wal);
        // read wal file header
        let wal_header = WALHeader::read(&self.wal_file)?;
        debug!("wal header: {:?}", wal_header);
        if wal_header.page_size != self.page_size {
            return Err(Error::SqliteWalHeaderError("Invalid page size"));
        }
        Ok(())
    }

    fn ensure_wal_exists(&self) -> Result<()> {
        let stat = fs::metadata(&self.wal_file)?;
        if !stat.is_file() {
            return Err(Error::SqliteWalError(format!(
                "wal {} is not a file",
                self.wal_file,
            )));
        }

        if stat.len() >= WALFRAME_HEADER_SIZE {
            return Ok(());
        }

        let _ = self.connection.execute(
            "INSERT INTO _litesync_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1;",
            (),
        )?;

        Ok(())
    }

    fn align_frame(&self, offset: u64) -> u64 {
        if offset < WALFRAME_HEADER_SIZE {
            return 0;
        }

        let frame_size = WALFRAME_HEADER_SIZE + self.page_size;
        let frame_num = (offset - WALFRAME_HEADER_SIZE) / frame_size;

        (frame_num * frame_size) + WALFRAME_HEADER_SIZE
    }

    fn current_shadow_wal_index(&self, generation: &String) -> Result<()> {
        let wal_dir_path = Path::new(&self.generations_dir)
            .join(generation)
            .join("wal");
        let wal_dir = fs::read_dir(&wal_dir_path);

        match wal_dir {
            Ok(wal_dir) => {}
            Err(e) => {
                println!("read_dir {:?} error: {}", wal_dir_path, e);
            }
        }

        Ok(())
    }

    fn verify(&mut self) -> Result<SyncInfo> {
        debug!("read generation_path: {:?}", self.generation_path);
        let generation = fs::read_to_string(&self.generation_path)?;
        debug!(
            "after read generation_path: {:?}, {:?}",
            self.generation_path, generation
        );
        if generation.is_empty() {
            return Err(Error::StorageNotFound("empty generation"));
        }

        let db_file = fs::metadata(&self.config.path)?;
        let db_size = db_file.len();
        let db_mod_time = db_file.modified()?;

        let wal_file = fs::metadata(&self.wal_file)?;
        let wal_size = self.align_frame(wal_file.len());

        self.current_shadow_wal_index(&generation)?;
        Ok(SyncInfo { generation })
    }
}

pub async fn run_database(config: DatabaseConfig, mut rx: Receiver<&str>) -> Result<()> {
    let mut database = Database::try_create(config)?;
    loop {
        let result = timeout(Duration::from_secs(1), rx.recv()).await;

        match result {
            Ok(message) => {
                log::info!("Received message: {:?}", message);
                break;
            }
            Err(_) => database.sync()?,
        }
    }
    Ok(())
}
