use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fs;

use serde::Deserialize;

use super::StorageParams;
use crate::error::Error;
use crate::error::Result;

const DEFAULT_MIN_CHECKPOINT_PAGE_NUMBER: u64 = 1000;
const DEFAULT_MAX_CHECKPOINT_PAGE_NUMBER: u64 = 10000;
const DEFAULT_TRUNCATE_PAGE_NUMBER: u64 = 500000;
const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 60;

#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct ReplicateConfig {
    pub log: LogConfig,

    pub database: Vec<ReplicateDbConfig>,
}

impl ReplicateConfig {
    pub fn load(config_file: &str) -> Result<Self> {
        let toml_str = match fs::read_to_string(config_file) {
            Ok(toml_str) => toml_str,
            Err(e) => {
                return Err(Error::ReadConfigFail(format!(
                    "read config file {} fail: {:?}",
                    config_file, e,
                )));
            }
        };

        let config: ReplicateConfig = match toml::from_str(&toml_str) {
            Ok(config) => config,
            Err(e) => {
                return Err(Error::ParseConfigFail(format!(
                    "parse config file {} fail: {:?}",
                    config_file, e,
                )));
            }
        };

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.database.is_empty() {
            return Err(Error::InvalidConfig(
                "config MUST has at least one database config",
            ));
        }
        for db in &self.database {
            db.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct RestoreConfig {
    pub log: LogConfig,

    pub database: RestoreDbConfig,
}

impl RestoreConfig {
    pub fn load(config_file: &str) -> Result<Self> {
        let toml_str = match fs::read_to_string(config_file) {
            Ok(toml_str) => toml_str,
            Err(e) => {
                return Err(Error::ReadConfigFail(format!(
                    "read config file {} fail: {:?}",
                    config_file, e,
                )));
            }
        };

        let config: RestoreConfig = match toml::from_str(&toml_str) {
            Ok(config) => config,
            Err(e) => {
                return Err(Error::ParseConfigFail(format!(
                    "parse config file {} fail: {:?}",
                    config_file, e,
                )));
            }
        };

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        self.database.validate()?;
        Ok(())
    }
}

/// ReplicateConfig for logging.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct LogConfig {
    pub level: LogLevel,
    pub dir: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<LogLevel> for log::LevelFilter {
    fn from(level: LogLevel) -> Self {
        match &level {
            LogLevel::Trace => log::LevelFilter::Trace,
            LogLevel::Debug => log::LevelFilter::Debug,
            LogLevel::Info => log::LevelFilter::Info,
            LogLevel::Warn => log::LevelFilter::Warn,
            LogLevel::Error => log::LevelFilter::Error,
            LogLevel::Off => log::LevelFilter::Off,
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            dir: "/var/log/replited".to_string(),
        }
    }
}

impl Display for LogConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "level={:?}, dir={}", self.level, self.dir)
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct RestoreDbConfig {
    // db file full path
    pub db: String,

    // replicate of db file config
    pub replicate: StorageConfig,
}

impl RestoreDbConfig {
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct ReplicateDbConfig {
    // db file full path
    pub db: String,

    // replicates of db file config
    pub replicate: Vec<StorageConfig>,

    // Minimum threshold of WAL size, in pages, before a passive checkpoint.
    // A passive checkpoint will attempt a checkpoint but fail if there are
    // active transactions occurring at the same time.
    #[serde(default = "default_min_checkpoint_page_number")]
    pub min_checkpoint_page_number: u64,

    // Maximum threshold of WAL size, in pages, before a forced checkpoint.
    // A forced checkpoint will block new transactions and wait for existing
    // transactions to finish before issuing a checkpoint and resetting the WAL.
    //
    // If zero, no checkpoints are forced. This can cause the WAL to grow
    // unbounded if there are always read transactions occurring.
    #[serde(default = "default_max_checkpoint_page_number")]
    pub max_checkpoint_page_number: u64,

    // Threshold of WAL size, in pages, before a forced truncation checkpoint.
    // A forced truncation checkpoint will block new transactions and wait for
    // existing transactions to finish before issuing a checkpoint and
    // truncating the WAL.
    //
    // If zero, no truncates are forced. This can cause the WAL to grow
    // unbounded if there's a sudden spike of changes between other
    // checkpoints.
    #[serde(default = "default_truncate_page_number")]
    pub truncate_page_number: u64,

    // Seconds between automatic checkpoints in the WAL. This is done to allow
    // more fine-grained WAL files so that restores can be performed with
    // better precision.
    #[serde(default = "default_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,
}

fn default_min_checkpoint_page_number() -> u64 {
    DEFAULT_MIN_CHECKPOINT_PAGE_NUMBER
}

fn default_max_checkpoint_page_number() -> u64 {
    DEFAULT_MAX_CHECKPOINT_PAGE_NUMBER
}

fn default_truncate_page_number() -> u64 {
    DEFAULT_TRUNCATE_PAGE_NUMBER
}

fn default_checkpoint_interval_secs() -> u64 {
    DEFAULT_CHECKPOINT_INTERVAL_SECS
}

impl Debug for ReplicateDbConfig {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ReplicateDbConfig")
            .field("db", &self.db)
            .field("storage", &self.replicate)
            .field(
                "min_checkpoint_page_number",
                &self.min_checkpoint_page_number,
            )
            .finish()
    }
}

impl ReplicateDbConfig {
    fn validate(&self) -> Result<()> {
        if self.replicate.is_empty() {
            return Err(Error::InvalidConfig(
                "database MUST has at least one replicate config",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct StorageConfig {
    pub allow_insecure: bool,
    pub params: StorageParams,
}

impl Debug for StorageConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("StorageS3Config")
            .field("allow_insecure", &self.allow_insecure)
            .field("params", &self.params)
            .finish()
    }
}
