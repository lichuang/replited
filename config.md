<!-- MarkdownTOC autolink="true" -->
- [Overview](#overview)
- [Log config](#log-config)
- [Database config](#database-config)
	- [Replicate Config](#replicate-config)
 		- [Gcs Params](#gcs-params) 
		- [File System Params](#file-system-params)
  		- [Ftp](#ftp-params) 
		- [S3 Params](#s3-params)
  
  <!-- /MarkdownTOC -->

## Overview

replited use `toml` as its config file format, the structure of config is:

* Log config;
* One or more database configs:
  * sqlite database file path;
  * one or more database replicate backend.

See config sample in [sample.toml](./etc/sample.toml)

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
| name | replicate backend config name, cannot duplicate |
| params | params of backend, see below |

#### Gcs Params
| item  |  value    |
| :---- | ---- |
| params.type | "Gcs" |
| params.endpoint | Endpoint of this backend, must be full uri, use "https://storage.googleapis.com" by default. |
| params.root | Root URI of gcs operations. |
| params.bucket | Bucket name of this backend. |
| params.credential | Credentials string for GCS service OAuth2 authentication. |

#### File System Params
| item  |  value    |
| :---- | ---- |
| params.type | "Fs" |
| params.root | root directory of file system backend |

#### Ftp Params
| item  |  value    |
| :---- | ---- |
| params.type | "Ftp" |
| params.endpoint | Endpoint of this ftp, use "ftps://127.0.0.1" by default. |
| params.root | root directory of file system backend, use "/" by default. |
| params.user | user of ftp backend. |
| params.password | password of ftp backend. |


#### S3 Params
| item  |  value    |
| :---- | ---- |
| params.type | "S3" |
| params.endpoint | Endpoint of this backend, must be full uri, use "https://s3.amazonaws.com" by default. |
| params.region | Region represent the signing region of this endpoint.If `region` is empty, use env value `AWS_REGION` if it is set, or else use `us-east-1` by default. |
| params.bucket | Bucket name of this backend. |
| params.access_key_id | access_key_id of this backend. |
| params.secret_access_key | secret_access_key of this backend. |
| params.root | root of this backend. |
