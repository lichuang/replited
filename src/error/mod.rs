mod backtrace;
#[allow(clippy::module_inception)]
mod error;
mod error_code;
mod error_into;

pub(crate) use backtrace::capture;
pub use error::Error;
pub(crate) use error::ErrorCodeBacktrace;
pub use error::Result;
