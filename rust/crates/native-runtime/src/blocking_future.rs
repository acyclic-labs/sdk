//! CPU-heavy futures share the blocking pool without holding a thread at await.
use std::{
    future::Future,
    io,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};
use tokio::sync::Notify;

struct Ready(Notify);
impl Wake for Ready {
    fn wake(self: Arc<Self>) {
        self.0.notify_one();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify_one();
    }
}

/// Polls a CPU-heavy future on the shared blocking pool, releasing its thread
/// whenever it yields. Only its waker schedules another poll. Dropping the
/// caller retires the future after an in-flight poll returns; synchronous work
/// requiring interruption must carry its own cancellation signal.
pub async fn blocking_future<F>(future: F) -> io::Result<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let ready = Arc::new(Ready(Notify::new()));
    let mut future = Box::pin(future);
    loop {
        let wake = ready.clone();
        let (next, result) = crate::spawn_blocking(move || {
            let waker = Waker::from(wake);
            let result = future.as_mut().poll(&mut Context::from_waker(&waker));
            (future, result)
        })
        .await?;
        match result {
            Poll::Ready(value) => return Ok(value),
            Poll::Pending => {
                future = next;
                ready.0.notified().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_waiting_future_releases_the_only_blocking_thread() -> std::io::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()?;
        runtime.block_on(async {
            let (started, start) = tokio::sync::oneshot::channel();
            let (release, wait) = tokio::sync::oneshot::channel();
            let pending = super::blocking_future(async move {
                started.send(()).unwrap();
                wait.await.unwrap();
                42
            });
            let release = async {
                start.await.unwrap();
                crate::spawn_blocking(move || release.send(()).unwrap())
                    .await
                    .unwrap();
            };
            let (result, ()) = tokio::join!(pending, release);
            assert_eq!(result?, 42);
            Ok(())
        })
    }
}
