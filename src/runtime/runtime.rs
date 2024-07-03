use std::future::Future;
use std::time::Duration;
use std::time::Instant;

use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::ThreadJoinHandle;
use crate::error::Error;
use crate::error::Result;
use crate::runtime::Thread;

/// Methods to spawn tasks.
pub trait TrySpawn {
    /// Tries to spawn a new asynchronous task, returning a tokio::JoinHandle for it.
    ///
    /// It allows to return an error before spawning the task.
    #[track_caller]
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static;

    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    ///
    /// A default impl of this method just calls `try_spawn` and just panics if there is an error.
    #[track_caller]
    fn spawn<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.try_spawn(task).unwrap()
    }
}

/// Tokio Runtime wrapper.
/// If a runtime is in an asynchronous context, shutdown it first.
pub struct Runtime {
    /// Runtime handle.
    handle: Handle,
}

impl Runtime {
    fn create(_name: Option<String>, builder: &mut Builder) -> Result<Self> {
        let runtime = builder
            .build()
            .map_err(|tokio_error| Error::TokioError(tokio_error.to_string()))?;

        let handle = runtime.handle().clone();

        Ok(Runtime { handle })
    }

    /// Spawns a new tokio runtime with a default thread count on a background
    /// thread and returns a `Handle` which can be used to spawn tasks via
    /// its executor.
    pub fn with_default_worker_threads() -> Result<Self> {
        let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();

        #[cfg(debug_assertions)]
        {
            // We need to pass the thread name in the unit test, because the thread name is the test name
            if matches!(std::env::var("UNIT_TEST"), Ok(var_value) if var_value == "TRUE") {
                if let Some(thread_name) = std::thread::current().name() {
                    runtime_builder.thread_name(thread_name);
                }
            }

            runtime_builder.thread_stack_size(20 * 1024 * 1024);
        }

        Self::create(None, runtime_builder.enable_all())
    }

    #[allow(unused_mut)]
    pub fn with_worker_threads(workers: usize, mut thread_name: Option<String>) -> Result<Self> {
        let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();

        #[cfg(debug_assertions)]
        {
            // We need to pass the thread name in the unit test, because the thread name is the test name
            if matches!(std::env::var("UNIT_TEST"), Ok(var_value) if var_value == "TRUE") {
                if let Some(cur_thread_name) = std::thread::current().name() {
                    thread_name = Some(cur_thread_name.to_string());
                }
            }

            runtime_builder.thread_stack_size(20 * 1024 * 1024);
        }

        if let Some(thread_name) = &thread_name {
            runtime_builder.thread_name(thread_name);
        }

        Self::create(
            thread_name,
            runtime_builder.enable_all().worker_threads(workers),
        )
    }
}

impl TrySpawn for Runtime {
    #[track_caller]
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        Ok(self.handle.spawn(task))
    }
}
