use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use backtrace::Backtrace;

use super::backtrace::capture;

#[derive(Clone)]
pub enum ErrorCodeBacktrace {
    Serialized(Arc<String>),
    Symbols(Arc<Backtrace>),
    Address(Arc<Backtrace>),
}

#[derive(thiserror::Error)]
pub struct Error {
    code: u32,
    name: String,
    display_text: String,
    detail: String,
    cause: Option<Box<dyn std::error::Error + Sync + Send>>,
    backtrace: Option<ErrorCodeBacktrace>,
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

    pub fn backtrace(&self) -> Option<ErrorCodeBacktrace> {
        self.backtrace.clone()
    }

    // output a string without backtrace
    pub fn simple_string(&self) -> String {
        format!(
            "{}. Code: {}, Text = {}.",
            self.name,
            self.code(),
            self.message(),
        )
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
        )?;

        match self.backtrace.as_ref() {
            None => write!(
                f,
                "\n\n<Backtrace disabled by default. Please use RUST_BACKTRACE=1 to enable> "
            ),
            Some(backtrace) => match backtrace {
                ErrorCodeBacktrace::Symbols(backtrace) => write!(f, "\n\n{:?}", backtrace),
                ErrorCodeBacktrace::Serialized(backtrace) => write!(f, "\n\n{}", backtrace),
                ErrorCodeBacktrace::Address(backtrace) => {
                    let frames_address = backtrace
                        .frames()
                        .iter()
                        .map(|f| (f.ip() as usize, f.symbol_address() as usize))
                        .collect::<Vec<_>>();
                    write!(f, "\n\n{:?}", frames_address)
                }
            },
        }
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
            code: Error::INTERNAL,
            name: String::from("FromStdError"),
            display_text: error.to_string(),
            detail: String::new(),
            cause: None,
            backtrace: capture(),
        }
    }

    pub fn from_error_code(code: u32, display_text: impl ToString) -> Self {
        Error {
            code,
            name: String::new(),
            display_text: display_text.to_string(),
            detail: String::new(),
            cause: None,
            backtrace: capture(),
        }
    }

    pub fn from_string(error: String) -> Self {
        Error {
            code: Error::INTERNAL,
            name: String::from("Internal"),
            display_text: error,
            detail: String::new(),
            cause: None,
            backtrace: capture(),
        }
    }

    pub fn from_string_no_backtrace(error: String) -> Self {
        Error {
            code: Error::INTERNAL,
            name: String::from("Internal"),
            display_text: error,
            detail: String::new(),
            cause: None,
            backtrace: capture(),
        }
    }

    pub fn create(
        code: u32,
        name: impl ToString,
        display_text: String,
        detail: String,
        cause: Option<Box<dyn std::error::Error + Sync + Send>>,
        backtrace: Option<ErrorCodeBacktrace>,
    ) -> Error {
        Error {
            code,
            display_text,
            detail,
            cause,
            name: name.to_string(),
            backtrace,
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
            self.backtrace(),
        )
    }
}
