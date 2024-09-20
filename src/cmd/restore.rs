use super::command::Command;
use crate::config::Config;
use crate::error::Result;
use crate::restore::run_restore;

pub struct Restore {
    config: Config,
}

impl Restore {
    pub fn new(config: Config) -> Box<Self> {
        Box::new(Restore { config })
    }
}

#[async_trait::async_trait]
impl Command for Restore {
    async fn run(&mut self) -> Result<()> {
        let mut handles = vec![];
        for config in &self.config.database {
            let config = config.clone();
            let handle = tokio::spawn(async move {
                let _ = run_restore(config).await;
            });
            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }

        Ok(())
    }
}
