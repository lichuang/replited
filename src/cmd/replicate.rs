use std::sync::Arc;

use super::command::Command;
use crate::config::Config;
use crate::database::Database;
use crate::error::Result;

pub struct Replicate {
    pub databases: Vec<Arc<Database>>,
}

impl Replicate {
    pub fn try_create(config: Config) -> Result<Box<Self>> {
        let mut databases = vec![];
        for db in &config.database {
            let database = Database::try_create(db.clone())?;
            databases.push(Arc::new(database));
        }
        Ok(Box::new(Replicate { databases }))
    }
}

impl Command for Replicate {
    async fn run(&self) -> Result<()> {
        let mut handles = vec![];
        for database in &self.databases {
            let datatase = database.clone();
            let handle = tokio::spawn(async move {
                let _ = datatase.as_ref().run().await;
            });

            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }
        Ok(())
    }
}
