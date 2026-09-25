mod hidden;
use std::{default, num::NonZero};

use crate::hidden::ThreadHandler;

pub trait JobQueue<'scope>: Sync {
    fn pop(&self) -> Option<Box<dyn FnOnce() + 'scope>>;
}

pub struct ThreadPool<'scope, T: hidden::ThreadSpawner<'scope, ()> = hidden::UnscopedSpawner> {
    handlers: Vec<T::JoinHandleType>,
}
fn thread_run<'scope,T>(queue: &T)
where
    T: JobQueue<'scope>,
{
    while let Some(func) = queue.pop() {
        func();
    }
}

impl<'scope, T> ThreadPool<'scope, T>
where
    T: hidden::ThreadSpawner<'scope, ()>,
{
    #[must_use]
    fn new_with_spawner<QUEUE: JobQueue<'scope>, RefType: hidden::SharedClone<QUEUE> + Send + 'scope>(
        count: NonZero<u64>,
        queue: RefType,
        spawner: &T,
    ) -> Self {
        Self {
            handlers: (0..count.into())
                .map(|_| {
                    let tmp = queue.clone();
                    spawner.spawn(move || thread_run(tmp.borrow()))
                })
                .collect(),
        }
    }

    pub fn wait(self) -> std::thread::Result<()> {
        self.handlers.into_iter().try_for_each(ThreadHandler::join)
    }
}

impl<'scope, 'env> ThreadPool<'scope, &'scope std::thread::Scope<'scope, 'env>> {
    #[must_use]
    pub fn new_with_scope<QUEUE: JobQueue<'scope>, RefType: hidden::SharedClone<QUEUE> + Send + 'scope>(
        count: NonZero<u64>,
        queue: RefType,
        spawner: &'scope std::thread::Scope<'scope, 'env>,
    ) -> Self {
        Self::new_with_spawner(count, queue, &spawner)
    }
}

impl<T> ThreadPool<'static, T>
where
    T: hidden::ThreadSpawner<'static, ()> + 'static + default::Default,
{
    #[must_use]
    pub fn new<QUEUE: JobQueue<'static>, RefType: hidden::SharedClone<QUEUE> + Send + 'static>(
        count: NonZero<u64>,
        queue: RefType,
    ) -> Self {
        Self::new_with_spawner(count, queue, &T::default())
    }
}
