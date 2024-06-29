use clap::Parser;
use clap::Subcommand;

#[derive(Parser, Debug)]
#[command(author="litesync", version, about="Sync sqlite database", long_about = None)]
pub struct Arg {
    #[command(subcommand)]
    pub command: ArgCommand,

    #[arg(short, long, short = 'c', default_value = "/etc/litesync.toml")]
    pub config: String,
}

impl Arg {}

#[derive(Subcommand, Clone, Debug)]
pub enum ArgCommand {
    #[command(author="litesync", version, about="Replicate sqlite database", long_about = None)]
    Replicate,
}
