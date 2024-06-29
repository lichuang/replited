use super::command::Command;
use crate::config::Config;
use crate::database::Database;
use crate::error::Result;

pub struct Replicate {
    pub databases: Vec<Database>,
}

impl Replicate {
    pub fn try_create(config: Config) -> Result<Box<Self>> {
        let mut databases = vec![];
        for db in &config.database {
            let database = Database::try_create(db)?;
            databases.push(database);
        }
        Ok(Box::new(Replicate { databases }))
    }
}

impl Command for Replicate {
    async fn run(&self) -> Result<()> {
        Ok(())
    }
}
