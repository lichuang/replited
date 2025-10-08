use std::fmt::Display;
use std::fmt::Formatter;

use uuid::NoContext;
use uuid::Uuid;
use uuid::timestamp;

use crate::error::Result;

#[derive(Eq, PartialEq, PartialOrd, Debug, Clone, Default)]
pub struct Generation {
    uuid: Uuid,
    generation: String,
}

impl Generation {
    pub fn new() -> Self {
        let timestamp = timestamp::Timestamp::now(NoContext);
        Self::from_uuid(Uuid::new_v7(timestamp))
    }

    fn from_uuid(uuid: Uuid) -> Self {
        let generation = uuid.simple().to_string();
        Generation { uuid, generation }
    }

    pub fn try_create(generation: &str) -> Result<Self> {
        Ok(Self::from_uuid(Uuid::parse_str(generation)?))
    }

    pub fn as_str(&self) -> &str {
        &self.generation
    }

    pub fn is_empty(&self) -> bool {
        self.uuid.is_nil()
    }
}

impl Display for Generation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.generation)
    }
}
