use crate::config::DatabaseConfig;
use crate::error::Result;

pub struct Database {}

impl Database {
    pub fn try_create(config: &DatabaseConfig) -> Result<Self> {
        Ok(Self {})
    }

    pub async fn run(&self) -> Result<()> {
        Ok(())
    }
}
