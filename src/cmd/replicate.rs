use super::command::Command;
use crate::config::Config;
use crate::database::run_database;
use crate::error::Result;

pub struct Replicate {
    config: Config,
}

impl Replicate {
    pub fn try_create(config: Config) -> Result<Box<Self>> {
        Ok(Box::new(Replicate { config }))
    }
}

impl Command for Replicate {
    async fn run(&mut self) -> Result<()> {
        let mut handles = vec![];
        for database in &self.config.database {
            let datatase = database.clone();
            let handle = tokio::spawn(async move {
                let _ = run_database(datatase).await;
            });

            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }
        Ok(())
    }
}
