use std::fs;

use log::error;

use crate::error::Result;

pub struct TempFile {
    file: String,
}

impl TempFile {
    pub fn try_create(file: String) -> Result<Self> {
        if fs::exists(&file)? {
            fs::remove_file(&file)?;
        }
        Ok(Self { file })
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_file(&self.file) {
            error!("remove_file err: {:?}", e);
        }
    }
}
