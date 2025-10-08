use std::num::NonZeroUsize;

use logforth::append::file::FileBuilder;
use logforth::layout::TextLayout;
use logforth::record::LevelFilter;

use crate::config::LogConfig;
use crate::config::LogLevel;
use crate::error::Error;
use crate::error::Result;

pub fn init_log(log_config: LogConfig) -> Result<()> {
    let level: LevelFilter = match log_config.level {
        LogLevel::Error => LevelFilter::Error,
        LogLevel::Warn => LevelFilter::Warn,
        LogLevel::Info => LevelFilter::Info,
        LogLevel::Debug => LevelFilter::Debug,
        LogLevel::Trace => LevelFilter::Trace,
        LogLevel::Off => LevelFilter::Off,
    };

    let file = FileBuilder::new(log_config.dir, "replited")
        .rollover_size(NonZeroUsize::new(1024 * 4096).unwrap()) // bytes
        .max_log_files(NonZeroUsize::new(9).unwrap())
        .filename_suffix("log")
        .layout(TextLayout::default().no_color())
        .build()
        .map_err(Error::from_std_error)?;

    logforth::starter_log::builder()
        .dispatch(|d| d.filter(level).append(file))
        .apply();

    Ok(())
}
