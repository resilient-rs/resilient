use std::future::Future;

pub trait Policy<T, E> {
    fn call<F, Fut>(&self, f: F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Send;
}
