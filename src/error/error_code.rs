#![allow(non_snake_case)]

use crate::error::Error;

macro_rules! build_error {
    ($($(#[$meta:meta])* $body:ident($code:expr)),*$(,)*) => {
        impl Error {
            $(
                paste::item! {
                    $(
                        #[$meta]
                    )*
                    pub const [< $body:snake:upper >]: u32 = $code;
                }
                $(
                    #[$meta]
                )*
                pub fn $body(display_text: impl Into<String>) -> Error {
                    Error::create(
                        $code,
                        stringify!($body),
                        display_text.into(),
                        String::new(),
                        None,
                    )
                }
            )*
        }
    }
}

build_error! {
    Ok(0),

    // config file error
    EmptyConfigFile(1),
    InvalidConfig(2),
    ReadConfigFail(3),
    ParseConfigFail(4),

    // logger error
    InitLoggerError(10),
    /// Internal means this is the internal error that no action
    /// can be taken by neither developers or users.
    /// In most of the time, they are code bugs.
    ///
    /// If there is an error that are unexpected and no other actions
    /// to taken, please use this error code.
    ///
    /// # Notes
    ///
    /// This error should never be used to for error checking. An error
    /// that returns as internal error could be assigned a separate error
    /// code at anytime.
    Internal(11),

    // storage error
    StorageNotFound(51),
    StoragePermissionDenied(52),
    StorageOther(53),
    InvalidPath(54),

    // database error
    SpawnDatabaseTaskError(80),

    // tokio error
    TokioError(100),

    // sqlite error
    SqliteError(120),
    SqliteWalError(121),
    SqliteWalHeaderError(122),
    SqliteWalFrameHeaderError(123),
    NoSnapshotError(124),
    NoWalsegmentError(125),
    BadShadowWalError(126),

    // other error
    PanicError(140),
    UnexpectedEofError(141),
}
