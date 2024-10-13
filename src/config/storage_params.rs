use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::base::mask_string;

/// Storage params which contains the detailed storage info.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StorageParams {
    Azb(Box<StorageAzblobConfig>),
    Fs(Box<StorageFsConfig>),
    Ftp(Box<StorageFtpConfig>),
    Gcs(Box<StorageGcsConfig>),
    S3(Box<StorageS3Config>),
}

impl StorageParams {
    pub fn root(&self) -> String {
        match self {
            StorageParams::Azb(s) => s.root.clone(),
            StorageParams::Fs(s) => s.root.clone(),
            StorageParams::Ftp(s) => s.root.clone(),
            StorageParams::Gcs(s) => s.root.clone(),
            StorageParams::S3(s) => s.root.clone(),
        }
    }
}

/// StorageParams will be displayed by `{protocol}://{key1=value1},{key2=value2}`
impl Display for StorageParams {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            StorageParams::Azb(v) => write!(
                f,
                "azblob | container={},root={},endpoint={}",
                v.container, v.root, v.endpoint
            ),
            StorageParams::Fs(v) => write!(f, "fs | root={}", v.root),
            StorageParams::Ftp(v) => {
                write!(f, "ftp | root={},endpoint={}", v.root, v.endpoint)
            }
            StorageParams::Gcs(v) => write!(
                f,
                "gcs | bucket={},root={},endpoint={}",
                v.bucket, v.root, v.endpoint
            ),
            StorageParams::S3(v) => {
                write!(
                    f,
                    "s3 | bucket={},root={},endpoint={}",
                    v.bucket, v.root, v.endpoint
                )
            }
        }
    }
}

/// Config for storage backend azblob.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAzblobConfig {
    pub endpoint: String,
    pub container: String,
    pub account_name: String,
    pub account_key: String,
    pub root: String,
}

impl Debug for StorageAzblobConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("StorageAzblobConfig")
            .field("endpoint", &self.endpoint)
            .field("container", &self.container)
            .field("root", &self.root)
            .field("account_name", &self.account_name)
            .field("account_key", &mask_string(&self.account_key, 3))
            .finish()
    }
}

/// Config for FTP and FTPS data source
pub const STORAGE_FTP_DEFAULT_ENDPOINT: &str = "ftps://127.0.0.1";
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFtpConfig {
    pub endpoint: String,
    pub root: String,
    pub username: String,
    pub password: String,
}

impl Default for StorageFtpConfig {
    fn default() -> Self {
        Self {
            endpoint: STORAGE_FTP_DEFAULT_ENDPOINT.to_string(),
            username: "".to_string(),
            password: "".to_string(),
            root: "/".to_string(),
        }
    }
}

impl Debug for StorageFtpConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("StorageFtpConfig")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("username", &self.username)
            .field("password", &mask_string(self.password.as_str(), 3))
            .finish()
    }
}

/// Config for storage backend fs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFsConfig {
    pub root: String,
}

impl Default for StorageFsConfig {
    fn default() -> Self {
        Self {
            root: "_data".to_string(),
        }
    }
}

/// Config for storage backend GCS.
pub static STORAGE_GCS_DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct StorageGcsConfig {
    pub endpoint: String,
    pub bucket: String,
    pub root: String,
    pub credential: String,
}

impl Default for StorageGcsConfig {
    fn default() -> Self {
        Self {
            endpoint: STORAGE_GCS_DEFAULT_ENDPOINT.to_string(),
            bucket: String::new(),
            root: String::new(),
            credential: String::new(),
        }
    }
}

impl Debug for StorageGcsConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("StorageGcsConfig")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("credential", &mask_string(&self.credential, 3))
            .finish()
    }
}

/// Config for storage backend s3.
pub static STORAGE_S3_DEFAULT_ENDPOINT: &str = "https://s3.amazonaws.com";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageS3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,

    pub root: String,
}

impl Default for StorageS3Config {
    fn default() -> Self {
        StorageS3Config {
            endpoint: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
            region: "".to_string(),
            bucket: "".to_string(),
            access_key_id: "".to_string(),
            secret_access_key: "".to_string(),
            root: "".to_string(),
        }
    }
}

impl Debug for StorageS3Config {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("StorageS3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("access_key_id", &mask_string(&self.access_key_id, 3))
            .field(
                "secret_access_key",
                &mask_string(&self.secret_access_key, 3),
            )
            .finish()
    }
}
