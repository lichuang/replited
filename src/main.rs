#![allow(dead_code)]
#![allow(incomplete_features)]

mod base;
mod cmd;
mod config;
mod database;
mod error;
mod log;
mod restore;
mod sqlite;
mod sync;

use clap::Parser;
use config::Arg;

use crate::cmd::command;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arg = Arg::parse();
    println!("arg: {:?}\n", arg);

    let mut cmd = command(arg)?;

    cmd.run().await?;

    Ok(())
}
