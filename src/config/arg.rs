use clap::Args;
use clap::Parser;
use clap::Subcommand;

#[derive(Parser, Debug)]
#[command(author="replited", version, about="Sync sqlite database", long_about = None)]
pub struct Arg {
    #[clap(flatten)]
    pub global_opts: GlobalOptions,

    #[command(subcommand)]
    pub command: ArgCommand,
}

impl Arg {}

#[derive(Subcommand, Clone, Debug)]
pub enum ArgCommand {
    #[command(author="replited", version, about="Replicate sqlite database", long_about = None)]
    Replicate,
}

#[derive(Debug, Args)]
pub struct GlobalOptions {
    #[arg(short, long, short = 'c', default_value = "/etc/replited.toml")]
    pub config: String,
}
