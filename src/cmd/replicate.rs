use super::command::Command;
use crate::config::Config;
use crate::database::run_database;
use crate::error::Result;
use crate::log::init_log;

pub struct Replicate {
    config: Config,
}

impl Replicate {
    pub fn try_create(config: &str) -> Result<Box<Self>> {
        let config = Config::load(config)?;
        let log_config = config.log.clone();

        init_log(log_config)?;
        Ok(Box::new(Replicate { config }))
    }
}

#[async_trait::async_trait]
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
