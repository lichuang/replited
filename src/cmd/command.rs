use super::Replicate;
use super::Restore;
use crate::config::Arg;
use crate::config::ArgCommand;
use crate::config::Config;
use crate::error::Result;

pub const REPLICATE_CMD: &str = "replicate";
pub const RESTORE_CMD: &str = "restore";

#[async_trait::async_trait]
pub trait Command {
    async fn run(&mut self) -> Result<()>;
}

pub fn command(arg: Arg, config: Config) -> Result<Box<dyn Command>> {
    match arg.cmd {
        ArgCommand::Replicate => Ok(Replicate::new(config)),
        ArgCommand::Restore { overwrite } => {
            println!("restore: {}", overwrite);
            Ok(Restore::new(config))
        }
    }
}
