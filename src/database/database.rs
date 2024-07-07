use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use log::info;
use rusqlite::Connection;
use tokio::sync::broadcast::Receiver;
use tokio::time::timeout;

use crate::config::DatabaseConfig;
use crate::error::Result;

struct Database {
    config: DatabaseConfig,
    connection: Connection,
    generations_path: String,
    db_dir: String,
    page_size: u32,
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

    fn init_directory(config: &DatabaseConfig) -> Result<(String, String)> {
        let file_path = PathBuf::from(&config.path);
        let db_name = file_path.file_name().unwrap().to_str().unwrap();
        let dir_path = file_path.parent().unwrap_or_else(|| Path::new("."));
        let generations_path = format!(
            "{}/.{}-litesync/generations/",
            dir_path.to_str().unwrap(),
            db_name,
        );
        fs::create_dir_all(&generations_path).unwrap();

        Ok((dir_path.to_str().unwrap().to_string(), generations_path))
    }

    fn try_create(config: DatabaseConfig) -> Result<Self> {
        info!("start database with config: {:?}\n", config);
        let connection = Connection::open(&config.path)?;

        Database::init_params(&connection)?;

        Database::create_internal_tables(&connection)?;

        let page_size: u32 = connection.pragma_query_value(None, "page_size", |row| row.get(0))?;

        // init path
        let (db_dir, generations_path) = Database::init_directory(&config)?;

        // for replicate in &self.config.replicate {}

        Ok(Self {
            config,
            connection,
            db_dir,
            generations_path,
            page_size,
        })
    }

    fn sync(&self) -> Result<()> {
        println!("sync Database");
        Ok(())
    }
}

pub async fn run_database(config: DatabaseConfig, mut rx: Receiver<&str>) -> Result<()> {
    let database = Database::try_create(config)?;
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
