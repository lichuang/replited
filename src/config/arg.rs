use clap::Parser;
use clap::Subcommand;

use super::config::Config;
use super::config::LogConfig;
use crate::error::Result;

#[derive(Parser, Debug)]
#[command(author="litesync", version, about="Sync sqlite database", long_about = None)]
pub struct Arg {
    #[command(subcommand)]
    pub command: ArgCommand,
}

impl Arg {
    pub fn log_config(&self) -> Result<LogConfig> {
        match &self.command {
            ArgCommand::Replicate(replicate) => {
                let config_file = &replicate.config;
                let config = Config::load(config_file)?;
                Ok(config.log.clone())
            } //_ => Ok(LogConfig::default()),
        }
    }
}

#[derive(Subcommand, Clone, Debug)]
pub enum ArgCommand {
    #[command(author="litesync", version, about="Replicate sqlite database", long_about = None)]
    Replicate(ReplicateOption),
}

#[derive(Parser, Debug, Clone)]
pub struct ReplicateOption {
    #[arg(short, long, short = 'c', default_value = "/etc/litesync.toml")]
    config: String,
    //#[arg(short, long, default_value = "")]
    // db_path: String,

    //#[arg(short, long, default_value = "")]
    // replica_url: Vec<String>,
}
