use clap::Args;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author="replited", version, about="Sync sqlite database", long_about = None)]
pub struct Arg {
    #[clap(flatten)]
    pub global_opts: GlobalOptions,
}

#[derive(Debug, Args)]
pub struct GlobalOptions {
    #[arg(long, default_value = "/etc/replited.toml")]
    pub config: String,

    #[arg(long)]
    pub cmd: String,
}
