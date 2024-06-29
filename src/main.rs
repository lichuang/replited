#![allow(incomplete_features)]

mod cmd;
mod config;
mod db;
mod exception;

use clap::Parser;
use config::Args;

fn main() {
    let args = Args::parse();

    println!("hello: {:?}", args);
}
