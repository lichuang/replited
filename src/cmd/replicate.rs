use super::command::Command;
use crate::config::ReplicateOption;

pub struct Replicate {}

impl Replicate {
    pub fn try_create(option: ReplicateOption) -> Box<Self> {
        Box::new(Replicate {})
    }
}

impl Command for Replicate {
    async fn run(&self) {}
}
