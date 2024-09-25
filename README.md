# replited(Replicate SQLITE Daemon)

[![GitHub stars](https://img.shields.io/github/stars/lichuang/replited?label=Stars&logo=github)](https://github.com/lichuang/replited)
[![GitHub forks](https://img.shields.io/github/forks/lichuang/replited?label=Forks&logo=github)](https://github.com/lichuang/replited)

<!-- MarkdownTOC autolink="true" -->
- [Introduction](#introduction)
- [Getting started](#getting-started)
  - [Sub commands](#sub-commands)
    - [Replicate](#replicate)
    - [Restore](#restore)
  - [Config](#config)
<!-- /MarkdownTOC -->

## Introduction

inspired by [Litestream](https://litestream.io/), with the power of [Rust](https://www.rust-lang.org/) and [OpenDAL](https://opendal.apache.org/), replited target to replicate sqlite to everywhere(file system,s3,ftp,google drive,dropbox,etc).

## Getting started
`replited` support sub command:
* `repicate`: replicate db to replicates;
* `restore`: restore a db from replicates;


```
replited -h
Replicate sqlite to everywhere

Usage: replited [OPTIONS] <COMMAND>

Commands:
  replicate
  restore
  help       Print this message or the help of the given subcommand(s)

Options:
  -c, --config <CONFIG>  [default: /etc/replited.toml]
  -h, --help             Print help
  -V, --version          Print version
```
 
### Sub commands
#### Replicate
`repicate` sub command will run a background process to replicate db to replicates in config periodically, example:
```
replited  --config ./etc/sample.toml  replicate
```

#### Restore
`restore` sub command will restore db from replicates in config, example:
```
replited  --config ./etc/sample.toml restore --db /Users/codedump/local/sqlite/test.db --output ./test.db
```

command options:
* `db`: which db will be restore from config
* `output`: which path will restored db saved

## Config

## Stargazers over time
[![Stargazers over time](https://starchart.cc/lichuang/replited.svg?variant=adaptive)](https://starchart.cc/lichuang/replited)

                    
