use super::*;
use std::sync::mpsc;
use tracing_subscriber::prelude::*;

#[derive(Debug, Eq, PartialEq)]
enum Entered {
    Waiting,
    Running,
}

struct Waiting(mpsc::Sender<Entered>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Waiting {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().name() == "acyclic.runtime.fence_wait" {
            let _ = self.0.send(Entered::Waiting);
        }
    }
}

#[test]
fn borrowed_operation_waits_for_the_shared_queue_predecessor() -> io::Result<()> {
    waits_for_predecessor(
        "borrowed operation bypassed its shared predecessor",
        |file, previous, entered| {
            crate::with_file_admission(file.as_file(), || observe_running(&previous, &entered))
        },
    )
}

fn observe_running(previous: &OperationFence, entered: &mpsc::Sender<Entered>) -> io::Result<bool> {
    entered.send(Entered::Running).map_err(io::Error::other)?;
    Ok(previous
        .state
        .lock()
        .map_err(|_| io::ErrorKind::Other)?
        .complete)
}

fn waits_for_predecessor(
    oracle: &'static str,
    run: impl FnOnce(NativeFile, Arc<OperationFence>, mpsc::Sender<Entered>) -> io::Result<bool>
    + Send
    + 'static,
) -> io::Result<()> {
    let file = NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
    let health = file.health.clone();
    let predecessor = Arc::new(OperationFence::default());
    *file.health.tail.lock().map_err(|_| io::ErrorKind::Other)? = Some(predecessor.clone());
    let (entered, events) = mpsc::channel();
    let subscriber = tracing_subscriber::registry().with(Waiting(entered.clone()));
    let previous = predecessor.clone();
    let worker = std::thread::spawn(move || {
        tracing::subscriber::with_default(subscriber, || run(file, previous, entered))
    });
    // Both the correct waiter and the broken early executor send an event.
    // Release the predecessor and join before asserting, including the mutant.
    let first = events.recv();
    predecessor.complete();
    let was_complete = worker.join().map_err(|_| io::ErrorKind::Other)??;
    assert_eq!(
        first.map_err(io::Error::other)?,
        Entered::Waiting,
        "{oracle}"
    );
    assert!(
        was_complete,
        "prepared operation ran before its predecessor settled"
    );
    assert!(
        health
            .tail
            .lock()
            .map_err(|_| io::ErrorKind::Other)?
            .is_none()
    );
    Ok(())
}

#[test]
fn native_completion_uses_the_same_queue_predecessor() -> io::Result<()> {
    let file = NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
    let predecessor = Arc::new(OperationFence::default());
    *file.health.tail.lock().map_err(|_| io::ErrorKind::Other)? = Some(predecessor.clone());
    let called = Arc::new(AtomicU64::new(0));
    let observed = called.clone();
    let mut completion = file.control_async(
        move |_, _| {
            observed.fetch_add(1, Ordering::Release);
            Ok(())
        },
        None,
    )?;
    let waker = completion_waker();
    let polled = Pin::new(&mut completion).poll(&mut Context::from_waker(&waker));
    let shares = completion
        .predecessor
        .as_ref()
        .is_some_and(|pending| Arc::ptr_eq(pending, &predecessor));
    let issued_early = called.load(Ordering::Acquire);
    predecessor.complete();
    if let Poll::Ready(result) = polled {
        result?;
    } else {
        complete(completion)?;
    }
    assert!(shares, "native completion bypassed its shared predecessor");
    assert_eq!(
        issued_early, 0,
        "native completion issued before its predecessor settled"
    );
    assert_eq!(called.load(Ordering::Acquire), 1);
    assert!(
        file.health
            .tail
            .lock()
            .map_err(|_| io::ErrorKind::Other)?
            .is_none()
    );
    Ok(())
}
