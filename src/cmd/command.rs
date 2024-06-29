use super::Replicate;
use crate::config::ArgCommand;
use crate::config::Args;

pub trait Command {
    async fn run(&self);
}

pub fn command(args: Args) -> Box<impl Command> {
    match &args.command {
        ArgCommand::Replicate(option) => Replicate::try_create(option.clone()),
    }
}
