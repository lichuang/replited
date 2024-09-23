use std::env;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use log::warn;
use opendal::raw::HttpClient;
use opendal::services;
use opendal::Builder;
use opendal::Operator;
use reqwest_hickory_resolver::HickoryResolver;

use crate::config::StorageFsConfig;
use crate::config::StorageParams;
use crate::config::StorageS3Config;
use crate::error::Result;

/// The global dns resolver for opendal.
static GLOBAL_HICKORY_RESOLVER: LazyLock<Arc<HickoryResolver>> =
    LazyLock::new(|| Arc::new(HickoryResolver::default()));

pub fn init_operator(cfg: &StorageParams) -> Result<Operator> {
    let op = match cfg {
        StorageParams::Fs(cfg) => build_operator(init_fs_operator(cfg)?)?,
        StorageParams::S3(cfg) => build_operator(init_s3_operator(cfg)?)?,
    };

    Ok(op)
}

pub fn build_operator<B: Builder>(builder: B) -> Result<Operator> {
    let op = Operator::new(builder)?;

    Ok(op.finish())
}

/// Create a new http client for storage.
fn new_storage_http_client() -> Result<HttpClient> {
    let mut builder = reqwest::ClientBuilder::new();

    // Disable http2 for better performance.
    builder = builder.http1_only();

    // Set dns resolver.
    builder = builder.dns_resolver(GLOBAL_HICKORY_RESOLVER.clone());

    // Pool max idle per host controls connection pool size.
    // Default to no limit, set to `0` for disable it.
    let pool_max_idle_per_host = env::var("_LITESYNC_INTERNAL_POOL_MAX_IDLE_PER_HOST")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    builder = builder.pool_max_idle_per_host(pool_max_idle_per_host);

    // Connect timeout default to 30s.
    let connect_timeout = env::var("_LITESYNC_INTERNAL_CONNECT_TIMEOUT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30);
    builder = builder.connect_timeout(Duration::from_secs(connect_timeout));

    // Enable TCP keepalive if set.
    if let Ok(v) = env::var("_LITESYNC_INTERNAL_TCP_KEEPALIVE") {
        if let Ok(v) = v.parse::<u64>() {
            builder = builder.tcp_keepalive(Duration::from_secs(v));
        }
    }

    Ok(HttpClient::build(builder)?)
}

/// init_s3_operator will init a opendal s3 operator with input s3 config.
fn init_s3_operator(cfg: &StorageS3Config) -> Result<impl Builder> {
    let mut builder = services::S3::default()
        // Endpoint.
        .endpoint(&cfg.endpoint_url)
        // Bucket.
        .bucket(&cfg.bucket);

    // Region
    if !cfg.region.is_empty() {
        builder = builder.region(&cfg.region);
    } else if let Ok(region) = env::var("AWS_REGION") {
        // Try to load region from env if not set.
        builder = builder.region(&region);
    } else {
        // FIXME: we should return error here but keep those logic for compatibility.
        warn!(
            "Region is not specified for S3 storage, we will attempt to load it from profiles. If it is still not found, we will use the default region of `us-east-1`."
        );
        builder = builder.region("us-east-1");
    }

    // Credential.
    builder = builder
        .access_key_id(&cfg.access_key_id)
        .secret_access_key(&cfg.secret_access_key)
        .session_token(&cfg.security_token)
        .role_arn(&cfg.role_arn)
        .external_id(&cfg.external_id)
        // It's safe to allow anonymous since opendal will perform the check first.
        .allow_anonymous()
        // Root.
        .root(&cfg.root);

    // Disable credential loader
    if cfg.disable_credential_loader {
        builder = builder.disable_config_load().disable_ec2_metadata();
    }

    // Enable virtual host style
    if cfg.enable_virtual_host_style {
        builder = builder.enable_virtual_host_style();
    }

    builder = builder.http_client(new_storage_http_client()?);

    Ok(builder)
}

/// init_fs_operator will init a opendal fs operator.
fn init_fs_operator(cfg: &StorageFsConfig) -> Result<impl Builder> {
    let mut builder = services::Fs::default();

    let mut path = cfg.root.clone();
    if !path.starts_with('/') {
        path = env::current_dir().unwrap().join(path).display().to_string();
    }
    builder = builder.root(&path);

    Ok(builder)
}
