# replited(Replicate SQLITE Daemon)

[![GitHub stars](https://img.shields.io/github/stars/lichuang/replited?label=Stars&logo=github)](https://github.com/lichuang/replited)
[![GitHub forks](https://img.shields.io/github/forks/lichuang/replited?label=Forks&logo=github)](https://github.com/lichuang/replited)

<!-- MarkdownTOC autolink="true" -->
- [Introduction](#introduction)
- [Support Backend](#support-backend)
- [Quick Start](#quick-start)
- [Config](#config)
- [Sub commands](#sub-commands)
	- [Replicate](#replicate)
  - [Restore](#restore)
  <!-- /MarkdownTOC -->

## Introduction

inspired by [Litestream](https://litestream.io/), with the power of [Rust](https://www.rust-lang.org/) and [OpenDAL](https://opendal.apache.org/), replited target to replicate sqlite to everywhere(file system,s3,ftp,google drive,dropbox,etc).

## Support Backend

| Type                       | Services                                                     |
| -------------------------- | ------------------------------------------------------------ |
| Standard Storage Protocols |                                      |
| Object Storage Services    | [s3] |
| File Storage Services      | fs                                                           |

[s3]: https://aws.amazon.com/s3/



## Quick Start

Start a daemon to replicate sqlite:

```shell
replited replicate --config {config file}
```

Restore sqlite from backend:

```shell
replited restore --config {config file} --db {db in config file} --output {output sqlite db file path}
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
