pub trait SyncClient {
    async fn run(&mut self) -> Result<()>;
}

// pub fn new_sync_client(config: StorageConfig) -> Result<Box<impl SyncClient>> {}
