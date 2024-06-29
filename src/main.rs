#![allow(dead_code)]
#![allow(incomplete_features)]

mod cmd;
mod config;
mod database;
mod error;
mod log;

use clap::Parser;
use config::Arg;
use log::init_log;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arg = Arg::parse();
    let log_config = arg.log_config()?;

    println!("hello: {:?}", arg);

    init_log(log_config)?;
    Ok(())
}
