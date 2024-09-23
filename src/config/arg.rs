use clap::Parser;
use clap::Subcommand;

use crate::error::Error;
use crate::error::Result;

#[derive(Parser, Debug)]
#[command(author="replited", version, about="Replicate sqlite to every where", long_about = None)]
pub struct Arg {
    #[arg(short, long, default_value = "/etc/replited.toml")]
    pub config: String,

    #[command(subcommand)]
    pub cmd: ArgCommand,
}

#[derive(Subcommand, Clone, Debug)]
pub enum ArgCommand {
    Replicate,

    Restore(RestoreOptions),
}

#[derive(Parser, Debug, Clone)]
pub struct RestoreOptions {
    // restore db path in config file
    #[arg(short, long, default_value = "")]
    pub db: String,

    // restore db generation string.
    // when empty, use the most recent genaration from replicates.
    #[arg(short, long, default_value = "")]
    pub generation: String,

    // if overwrite exsiting db in the same path
    #[arg(short, long, default_value_t = false)]
    pub overwrite: bool,
}

impl RestoreOptions {
    pub fn validate(&self) -> Result<()> {
        if self.db.is_empty() {
            println!("restore MUST Specify db path in config");
            return Err(Error::InvalidArg("arg MUST Specify db path in config"));
        }
        Ok(())
    }
}
