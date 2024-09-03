use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::PoisonError;

use super::error_code::INTERNAL_ERROR_CODE;
use crate::database::DbCommand;
use crate::error::Error;
use crate::sync::SyncCommand;

#[derive(thiserror::Error)]
enum OtherErrors {
    AnyHow { error: anyhow::Error },
}

impl Display for OtherErrors {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{}", error),
        }
    }
}

impl Debug for OtherErrors {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{:?}", error),
        }
    }
}

impl From<log::SetLoggerError> for Error {
    fn from(error: log::SetLoggerError) -> Self {
        Error::InitLoggerError(format!("Set logger error: {}", error))
    }
}

impl From<log4rs::config::runtime::ConfigErrors> for Error {
    fn from(error: log4rs::config::runtime::ConfigErrors) -> Self {
        Error::InitLoggerError(format!("Init logger config error: {}", error))
    }
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Error::create(
            INTERNAL_ERROR_CODE,
            "anyhow",
            format!("{}, source: {:?}", error, error.source()),
            String::new(),
            Some(Box::new(OtherErrors::AnyHow { error })),
        )
    }
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Error::SqliteError(format!("sqlite error: {}", error))
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        use std::io::ErrorKind;

        let msg = format!("{} ({})", error.kind(), &error);

        match error.kind() {
            ErrorKind::NotFound => Error::StorageNotFound(msg),
            ErrorKind::PermissionDenied => Error::StoragePermissionDenied(msg),
            ErrorKind::UnexpectedEof => Error::UnexpectedEOFError(msg),
            _ => Error::StorageOther(msg),
        }
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(e: std::array::TryFromSliceError) -> Error {
        Error::from_std_error(e)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Error {
        Error::from_std_error(e)
    }
}

impl From<opendal::Error> for Error {
    fn from(e: opendal::Error) -> Error {
        Error::from_std_error(e)
    }
}

impl From<tokio::sync::mpsc::error::SendError<SyncCommand>> for Error {
    fn from(e: tokio::sync::mpsc::error::SendError<SyncCommand>) -> Error {
        Error::from_std_error(e)
    }
}

impl From<tokio::sync::mpsc::error::SendError<DbCommand>> for Error {
    fn from(e: tokio::sync::mpsc::error::SendError<DbCommand>) -> Error {
        Error::from_std_error(e)
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for Error {
    fn from(e: tokio::sync::broadcast::error::RecvError) -> Error {
        Error::from_std_error(e)
    }
}
