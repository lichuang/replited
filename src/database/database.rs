use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::config::DatabaseConfig;
use crate::error::Error;
use crate::error::Result;
use crate::runtime::GlobalIORuntime;
use crate::runtime::TrySpawn;

pub struct Database {
    pub config: DatabaseConfig,
}

impl Database {
    pub fn try_create(config: DatabaseConfig) -> Result<Self> {
        Ok(Self { config })
    }

    pub async fn run(db: Arc<Database>) -> Result<JoinHandle<Result<()>>> {
        let handle = GlobalIORuntime::instance().spawn(async move {
            // println!("start database with config: {:?}\n", self.config);
            // self.main()
            Ok(())
        });
        //.await
        //.expect("join must succeed");

        Ok(handle)
    }

    pub fn main(&self) -> Result<()> {
        println!("start database with config: {:?}\n", self.config);
        Ok(())
    }
}
