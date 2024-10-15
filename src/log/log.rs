use log::LevelFilter;
use logforth::append::rolling_file::NonBlockingBuilder;
use logforth::append::rolling_file::RollingFileWriter;
use logforth::append::RollingFile;
use logforth::layout::TextLayout;
use logforth::Dispatch;
use logforth::Logger;

use crate::config::LogConfig;
use crate::error::Result;

pub fn init_log(log_config: LogConfig) -> Result<()> {
    let level: LevelFilter = log_config.level.into();

    let rolling = RollingFileWriter::builder()
        .max_file_size(1024 * 4096) // bytes
        .max_log_files(9)
        .filename_prefix("replited")
        .filename_suffix("log")
        .build(log_config.dir)?;
    let (writer, guard) = NonBlockingBuilder::default().finish(rolling);
    std::mem::forget(guard);

    Logger::new()
        .dispatch(
            Dispatch::new()
                .filter(level)
                .layout(TextLayout::default().no_color())
                .append(RollingFile::new(writer)),
        )
        .apply()?;

    Ok(())
}
