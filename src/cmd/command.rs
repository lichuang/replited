use super::Replicate;
use crate::config::Arg;
use crate::config::ArgCommand;

pub trait Command {
    async fn run(&self);
}

pub fn command(arg: Arg) -> Box<impl Command> {
    match &arg.command {
        ArgCommand::Replicate(option) => Replicate::try_create(option.clone()),
    }
}
