use std::{borrow::Borrow, default, rc, sync::Arc, thread};

pub trait ThreadHandler<T>{
    fn join(self)->thread::Result<T>;
}

impl <T>ThreadHandler<T> for thread::JoinHandle<T>{
    fn join(self)->thread::Result<T> {
        self.join()
    }
}

impl <T>ThreadHandler<T> for thread::ScopedJoinHandle<'_,T>{
    fn join(self)->thread::Result<T> {
        self.join()
    }
}

pub trait ThreadSpawner<'scope,T> {
    type JoinHandleType:ThreadHandler<T>;
    fn spawn<F>(&self, f: F) -> Self::JoinHandleType
    where
        F: FnOnce()->T + Send + 'scope,
        T: Send + 'scope;
}


impl<'scope,ReturnType> ThreadSpawner<'scope,ReturnType> for &'scope thread::Scope<'scope, '_> {
    type JoinHandleType = thread::ScopedJoinHandle<'scope, ReturnType>;
    fn spawn<F>(&self, f: F) -> Self::JoinHandleType
    where
        F: FnOnce()->ReturnType + Send + 'scope,
        ReturnType: Send + 'scope
    {
        thread::Scope::spawn(self, f)
    }
}

#[derive(default::Default)]
pub struct UnscopedSpawner;
impl <T>ThreadSpawner<'static,T> for UnscopedSpawner {
    type JoinHandleType = thread::JoinHandle<T>;
    fn spawn<F>(&self, f: F) -> Self::JoinHandleType
    where
        F: FnOnce()->T + Send + 'static,
        T: Send + 'static
    {
        thread::spawn(f)
    }
}

pub unsafe trait SharedClone<T>: Clone + Borrow<T> {}

unsafe impl<T> SharedClone<T> for &T {}
unsafe impl<T> SharedClone<T> for Arc<T> {}
unsafe impl<T> SharedClone<T> for rc::Rc<T> {}
