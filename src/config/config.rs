use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fs;

use serde::Deserialize;

use crate::error::Error;
use crate::error::Result;

#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct Config {
    pub log: LogConfig,
}

impl Config {
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

        let config: Config = match toml::from_str(&toml_str) {
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
        Ok(())
    }
}

/// Config for logging.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct LogConfig {
    pub level: LogLevel,
    pub dir: String,
    pub to_stderr: bool,
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
            dir: "/var/log/litesync".to_string(),
            to_stderr: false,
        }
    }
}

impl Display for LogConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "level={:?}, dir={}, to_stderr={}",
            self.level, self.dir, self.to_stderr
        )
    }
}
