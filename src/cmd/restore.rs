use super::command::Command;
use crate::config::Config;
use crate::config::RestoreOptions;
use crate::error::Result;
use crate::log::init_log;
use crate::restore::run_restore;

pub struct Restore {
    config: Config,
    options: RestoreOptions,
}

impl Restore {
    pub fn try_create(config: &str, options: RestoreOptions) -> Result<Box<Self>> {
        let config = Config::load(config)?;
        let log_config = config.log.clone();

        init_log(log_config)?;
        Ok(Box::new(Restore { config, options }))
    }
}

#[async_trait::async_trait]
impl Command for Restore {
    async fn run(&mut self) -> Result<()> {
        self.options.validate()?;

        for config in &self.config.database {
            if config.db == self.options.db {
                let _ = run_restore(config, &self.options).await;
                return Ok(());
            }
        }

        Ok(())
    }
}
