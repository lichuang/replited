use log::LevelFilter;
use log4rs;
use log4rs::append::rolling_file::policy::compound::roll::fixed_window::FixedWindowRoller;
use log4rs::append::rolling_file::policy::compound::trigger::size::SizeTrigger;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::Appender;
use log4rs::config::Config;
use log4rs::config::Logger;
use log4rs::config::Root;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::threshold::ThresholdFilter;

use crate::config::LogConfig;
use crate::error::Result;

pub fn init_log(log_config: LogConfig) -> Result<()> {
    let level: LevelFilter = log_config.level.into();
    let log_line_pattern = "[{d(%Y-%m-%d %H:%M:%s)} {({l}):5.5}] {m}{n}";

    let log_file = format!("{}/litesync_log", log_config.dir);
    let log_file_pattern = format!("{}/litesync_log_{{}}", log_config.dir);
    let roller_count = 9;
    let roller_base = 1;
    let fixed_window_roller = FixedWindowRoller::builder()
        .base(roller_base)
        .build(&log_file_pattern, roller_count)?;

    let size_limit = 1024 * 4096;
    let size_trigger = SizeTrigger::new(size_limit);

    let compound_policy =
        CompoundPolicy::new(Box::new(size_trigger), Box::new(fixed_window_roller));

    let file_appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(&log_line_pattern)))
        .build(&log_file, Box::new(compound_policy))?;

    let bulder = Config::builder()
        .appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(level)))
                .build("logfile", Box::new(file_appender)),
        )
        .logger(
            Logger::builder()
                .appender("logfile")
                .build("logfile", level),
        );

    let config = bulder.build(Root::builder().appender("logfile").build(level))?;

    let _ = log4rs::init_config(config)?;

    Ok(())
}
