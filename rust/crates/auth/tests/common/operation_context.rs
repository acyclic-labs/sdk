use acyclic_auth::operation::{Context, Stopped};
use futures::channel::oneshot;

/// The fixture's original caller retains the sole terminal producer. No clock
/// or expiry is simulated: these controls explicitly deliver the stop event.
pub fn invocation() -> (Context, oneshot::Sender<Stopped>) {
    let (send, receive) = oneshot::channel();
    let context = Context::from_stopped(
        0,
        async move { receive.await.unwrap_or(Stopped::Cancelled) },
    );
    (context, send)
}
