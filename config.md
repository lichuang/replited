<!-- MarkdownTOC autolink="true" -->
- [Overview](#overview)
- [Log config](#log-config)
- [Database config](#database-config)
	- [Replicate Config](#replicate-config)
		- [File System Params](#file-system-params) 
		- [S3 Params](#s3-params)
  
  <!-- /MarkdownTOC -->

## Overview

replited use `toml` as its config file format, the structure of config is:

* Log config;
* One or more database configs:
  * sqlite database file path;
  * one or more database replicate backend.
  

## Log Config

| item  |  value    |
| :---- | ---- |
| level |  Trace/Debug/Info/Warn/Error    |
| dir   |  log files directory    |

## Database Config
| item  |  value    |
| :---- | ---- |
| db | sqlite database file path |
| replicate | one or more database replicate backend |

### Replicate Config
| item  |  value    |
| :---- | ---- |
| name | replicate type name, see below |
| params | params of backend, see below |

#### File System Params
| item  |  value    |
| :---- | ---- |
| name | "fs" |
| params | root directory of file system backend |