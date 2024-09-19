use super::Replicate;
use crate::config::Arg;
use crate::config::Config;
use crate::error::Result;

pub const REPLICATE_CMD: &str = "replicate";

pub trait Command {
    async fn run(&mut self) -> Result<()>;
}

pub fn command(arg: Arg, config: Config) -> Result<Box<impl Command>> {
    match arg.global_opts.cmd.as_str() {
        REPLICATE_CMD => Replicate::try_create(config),
        _ => unreachable!("invalid cmd: {}", arg.global_opts.cmd),
    }
}
