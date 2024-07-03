use crate::config::DatabaseConfig;
use crate::error::Result;

pub struct Database {
    pub config: DatabaseConfig,
}

impl Database {
    pub fn try_create(config: DatabaseConfig) -> Result<Self> {
        Ok(Self { config })
    }

    pub async fn run(&self) -> Result<()> {
        println!("start database with config: {:?}\n", self.config);
        Ok(())
    }
}
