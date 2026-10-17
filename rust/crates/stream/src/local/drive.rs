#[cfg(not(acyclic_loom))]
use super::spawn;
use std::task::Poll;

// A stream may stop being polled indefinitely. Transfer a pending finite read
// to an independent actor so fair lock handoff can never strand its permit in
// the consumer. Ready reads do not spawn or allocate a forwarding channel.
pub(super) async fn drive<T: Send + 'static>(
    mut future: futures::future::BoxFuture<'static, T>,
    #[cfg(acyclic_loom)] retain_pending: bool,
) -> T {
    #[cfg(acyclic_loom)]
    if retain_pending {
        return future.await;
    }
    let ready = std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
    if let Poll::Ready(value) = ready {
        return value;
    }
    let (output, completed) = crate::query_sync::oneshot::channel();
    let worker = async move {
        let result = future.await;
        // Publish only after the read has released every provider lock.
        let _ = output.send(result);
    };
    #[cfg(not(acyclic_loom))]
    drop(spawn(worker));
    #[cfg(acyclic_loom)]
    loom::thread::spawn(move || super::follow_model::park(worker));
    completed.await.expect("read worker disappeared")
}
