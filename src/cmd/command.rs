use super::Replicate;
use crate::config::Arg;
use crate::config::ArgCommand;
use crate::config::Config;
use crate::error::Result;

pub trait Command {
    async fn run(&self) -> Result<()>;
}

pub fn command(arg: Arg, config: Config) -> Result<Box<impl Command>> {
    match &arg.command {
        ArgCommand::Replicate => Replicate::try_create(config),
    }
}
