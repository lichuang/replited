use super::Replicate;
use super::Restore;
use crate::config::Arg;
use crate::config::ArgCommand;
use crate::error::Result;

pub const REPLICATE_CMD: &str = "replicate";
pub const RESTORE_CMD: &str = "restore";

#[async_trait::async_trait]
pub trait Command {
    async fn run(&mut self) -> Result<()>;
}

pub fn command(arg: Arg) -> Result<Box<dyn Command>> {
    match &arg.cmd {
        ArgCommand::Replicate => Ok(Replicate::try_create(&arg.config)?),
        ArgCommand::Restore(options) => Ok(Restore::try_create(&arg.config, options.clone())?),
    }
}
