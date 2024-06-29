use clap::Parser;
use clap::Subcommand;

#[derive(Parser, Debug)]
#[command(author="litesync", version, about="Sync sqlite database", long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub command: ArgCommand,
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
