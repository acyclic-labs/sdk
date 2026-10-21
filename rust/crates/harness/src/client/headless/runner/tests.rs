use super::*;
use crate::Error;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

struct Payload {
    bytes: [u8; 8192],
    polled: bool,
    released: Arc<AtomicBool>,
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.released.store(true, Ordering::SeqCst);
    }
}
impl Future for Payload {
    type Output = Result<HeadlessOutcome>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(self.bytes.iter().all(|byte| *byte == 7));
        if !self.polled {
            self.polled = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(Err(Error::Unauthorized("fixture authority".into())))
        }
    }
}
struct Client(Arc<AtomicBool>);
impl Client {
    fn payload(&self) -> Payload {
        Payload {
            bytes: [7; 8192],
            polled: false,
            released: self.0.clone(),
        }
    }
}
impl HeadlessClient for Client {
    fn prompt<W: Write>(
        &mut self,
        _: FileValue,
        _: OperationId,
        _: &mut W,
    ) -> impl Future<Output = Result<HeadlessOutcome>> {
        self.payload()
    }
    fn input<W: Write>(
        &mut self,
        _: OperationId,
        _: impl Stream<Item = Result<String>> + Unpin,
        _: &mut W,
    ) -> impl Future<Output = Result<HeadlessOutcome>> {
        self.payload()
    }
}
fn check(
    future: impl Future<Output = Result<HeadlessOutcome>>,
    released: &AtomicBool,
    complete: bool,
) {
    assert!(std::mem::size_of_val(&future) <= std::mem::size_of::<Payload>());
    let mut future = Box::pin(future);
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert!(!released.load(Ordering::SeqCst));
    if complete {
        assert!(
            matches!(future.as_mut().poll(&mut cx), Poll::Ready(Err(Error::Unauthorized(reason))) if reason == "fixture authority")
        );
    }
    drop(future);
    assert!(released.load(Ordering::SeqCst));
}

#[test]
fn forwarding_retains_one_payload_and_releases_it_synchronously() {
    let operation = OperationId::from_bytes([1; 16]);
    for complete in [false, true] {
        let released = Arc::new(AtomicBool::new(false));
        let mut client = Client(released.clone());
        let component = crate::task_volume::Component::new(operation.to_string()).unwrap();
        let command = FileValue::new(
            crate::task_volume::FileKey::Session {
                session: component.clone(),
                file: crate::task_volume::TaskFile::Request(component),
            },
            Vec::new(),
        );
        let mut output = Vec::new();
        check(
            run_headless::<Client>(&mut client, command, operation, &mut output),
            &released,
            complete,
        );
        released.store(false, Ordering::SeqCst);
        check(
            run_jsonl::<Client>(
                &mut client,
                operation,
                futures::stream::empty(),
                &mut output,
            ),
            &released,
            complete,
        );
    }
}
