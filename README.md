# replited(Replicate SQLITE Daemon)

[![GitHub stars](https://img.shields.io/github/stars/lichuang/replited?label=Stars&logo=github)](https://github.com/lichuang/replited)
[![GitHub forks](https://img.shields.io/github/forks/lichuang/replited?label=Forks&logo=github)](https://github.com/lichuang/replited)

<!-- MarkdownTOC autolink="true" -->
- [Introduction](#introduction)
- [Why replited](#why-replited)
- [Support Backend](#support-backend)
- [Quick Start](#quick-start)
- [Config](#config)
- [Sub commands](#sub-commands)
	- [Replicate](#replicate)
  - [Restore](#restore)
  <!-- /MarkdownTOC -->

## Introduction

Inspired by [Litestream](https://litestream.io/), with the power of [Rust](https://www.rust-lang.org/) and [OpenDAL](https://opendal.apache.org/), replited target to replicate sqlite to everywhere(file system,s3,ftp,google drive,dropbox,etc).

## Why replited
* Using sqlite's [WAL](https://sqlite.org/wal.html) mechanism, instead of backing up full data every time, do incremental backup of data to reduce the amount of synchronised data;
* Support for multiple types of storage backends,such as s3,gcs,ftp,local file system,etc.

## Support Backend

| Type                       | Services                                                     |
| -------------------------- | ------------------------------------------------------------ |
| Standard Storage Protocols | ftp                                    |
| Object Storage Services    | [gcs] [s3]![CI](https://github.com/lichuang/replited/actions/workflows/s3_integration_test.yml/badge.svg) |
| File Storage Services      | fs![CI](https://github.com/lichuang/replited/actions/workflows/fs_integration_test.yml/badge.svg)                                                          |

[gcs]: https://cloud.google.com/storage
[s3]: https://aws.amazon.com/s3/



## Quick Start

Start a daemon to replicate sqlite:

```shell
replited --config {config file} replicate 
```

Restore sqlite from backend:

```shell
replited --config {config file} restore --db {db in config file} --output {output sqlite db file path}
```

## Config

See [config.md](./config.md)


## Sub commands
### Replicate
`repicate` sub command will run a background process to replicate db to replicates in config periodically, example:
```
replited  --config ./etc/sample.toml  replicate
```

### Restore
`restore` sub command will restore db from replicates in config, example:
```
replited  --config ./etc/sample.toml restore --db /Users/codedump/local/sqlite/test.db --output ./test.db
```

command options:
* `db`: which db will be restore from config
* `output`: which path will restored db saved

## Stargazers over time
[![Stargazers over time](https://starchart.cc/lichuang/replited.svg?variant=adaptive)](https://starchart.cc/lichuang/replited)

​                    
