use clap::Parser;
use clap::Subcommand;

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

    Restore {
        // if overwrite exsiting db in the same path
        #[arg(short, long, default_value_t = false)]
        overwrite: bool,
    },
}
