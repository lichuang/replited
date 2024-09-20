use super::command::Command;
use crate::config::RestoreConfig;
use crate::error::Result;
use crate::log::init_log;
use crate::restore::run_restore;

pub struct Restore {
    config: RestoreConfig,
    overwrite: bool,
}

impl Restore {
    pub fn try_create(config: &str, overwrite: bool) -> Result<Box<Self>> {
        let config = RestoreConfig::load(config)?;
        let log_config = config.log.clone();

        init_log(log_config)?;
        Ok(Box::new(Restore { config, overwrite }))
    }
}

#[async_trait::async_trait]
impl Command for Restore {
    async fn run(&mut self) -> Result<()> {
        let _ = run_restore(&self.config.database, self.overwrite).await;

        Ok(())
    }
}
