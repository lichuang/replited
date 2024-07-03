use std::sync::Arc;

use super::runtime::Runtime;
use crate::base::GlobalInstance;
use crate::error::Result;

pub struct GlobalIORuntime;

impl GlobalIORuntime {
    pub fn init(num_cpus: usize) -> Result<()> {
        let thread_num = std::cmp::max(num_cpus, num_cpus::get() / 2);
        let thread_num = std::cmp::max(2, thread_num);

        GlobalInstance::set(Arc::new(Runtime::with_worker_threads(
            thread_num,
            Some("IO-worker".to_owned()),
        )?));
        Ok(())
    }

    pub fn instance() -> Arc<Runtime> {
        GlobalInstance::get()
    }
}
