#![allow(dead_code)]
#![allow(incomplete_features)]

mod base;
mod cmd;
mod config;
mod database;
mod error;
mod log;
mod runtime;

use clap::Parser;
use config::Arg;
use config::Config;
use log::init_log;

use crate::cmd::command;
use crate::cmd::Command;
use crate::runtime::GlobalIORuntime;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arg = Arg::parse();
    println!("arg: {:?}\n", arg);
    let config = Config::load(&arg.global_opts.config)?;
    let log_config = config.log.clone();

    init_log(log_config)?;

    let cmd = command(arg, config)?;

    GlobalIORuntime::init(10)?;

    cmd.run().await?;

    Ok(())
}
