use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use super::error_code::INTERNAL_ERROR_CODE;

#[derive(thiserror::Error)]
pub struct Error {
    code: u32,
    name: String,
    display_text: String,
    detail: String,
    cause: Option<Box<dyn std::error::Error + Sync + Send>>,
}

impl Error {
    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn display_text(&self) -> String {
        if let Some(cause) = &self.cause {
            format!("{}\n{:?}", self.display_text, cause)
        } else {
            self.display_text.clone()
        }
    }

    pub fn message(&self) -> String {
        let msg = self.display_text();
        if self.detail.is_empty() {
            msg
        } else {
            format!("{}\n{}", msg, self.detail)
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}. Code: {}, Text = {}.",
            self.name,
            self.code(),
            self.message(),
        )
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}. Code: {}, Text = {}.",
            self.name,
            self.code(),
            self.message(),
        )
    }
}

impl Error {
    /// All std error will be converted to InternalError
    pub fn from_std_error<T: std::error::Error>(error: T) -> Self {
        Error {
            code: INTERNAL_ERROR_CODE,
            name: String::from("FromStdError"),
            display_text: error.to_string(),
            detail: String::new(),
            cause: None,
        }
    }

    pub fn from_string(error: String) -> Self {
        Error {
            code: INTERNAL_ERROR_CODE,
            name: String::from("Internal"),
            display_text: error,
            detail: String::new(),
            cause: None,
        }
    }

    pub fn from_string_no_backtrace(error: String) -> Self {
        Error {
            code: INTERNAL_ERROR_CODE,
            name: String::from("Internal"),
            display_text: error,
            detail: String::new(),
            cause: None,
        }
    }

    pub fn create(
        code: u32,
        name: impl ToString,
        display_text: String,
        detail: String,
        cause: Option<Box<dyn std::error::Error + Sync + Send>>,
    ) -> Error {
        Error {
            code,
            display_text,
            detail,
            cause,
            name: name.to_string(),
        }
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        Error::create(
            self.code(),
            &self.name,
            self.display_text(),
            self.detail.clone(),
            None,
        )
    }
}
