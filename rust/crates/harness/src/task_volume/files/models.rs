//! Models the two linearization orders of publication and follow registration.
//! Authorization is pure and checked separately. Only the task-volume/provider
//! synchronization is constructed inside Loom; these use the real writer/query.
use super::fixtures::volume;
use super::*;
use crate::{
    AgentId,
    task_volume::{ChangeStream, Component, TaskFile},
};
use acyclic_stream::MemoryStream;
use futures::Stream;
use loom::sync::atomic::{AtomicBool, Ordering};
use std::{
    pin::Pin,
    task::{Context, Poll, Wake, Waker},
};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

struct Woken(AtomicBool);
impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn files() -> Arc<TaskFiles<MemoryStream>> {
    let volume = volume(AgentId::from_bytes([1; 16]));
    let inner = loom::future::block_on(Box::pin(TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("task-files-model").unwrap(),
    )))
    .unwrap();
    // The immutable authorization boundary has no shared state. The production
    // methods below still enforce the file role and exact byte/generation bounds.
    Arc::new(TaskFiles {
        inner,
        volume,
        maximum_bytes: 4096,
    })
}

fn request() -> FileValue {
    FileValue::new(
        FileKey::Task {
            task: Component::new("task").unwrap(),
            file: TaskFile::Request(Component::new("request").unwrap()),
        },
        b"request".to_vec(),
    )
}
fn query() -> Query {
    Query::Requests(Component::new("task").unwrap())
}
fn next(
    stream: &mut ChangeStream,
    waker: &Waker,
) -> Poll<Option<super::super::Result<super::super::Change>>> {
    Pin::new(stream).poll_next(&mut Context::from_waker(waker))
}

fn gap(broken: bool) {
    let files = files();
    // These are the same two production methods called by snapshot_follow.
    // Force publication into their handoff interval instead of modeling all
    // unrelated backend operations before and after this boundary concurrently.
    let snapshot = loom::future::block_on(Box::pin(files.inner.snapshot(&query(), 8))).unwrap();
    let expected = request();
    let writer = files.clone();
    let write = expected.clone();
    loom::thread::spawn(move || {
        loom::future::block_on(Box::pin(writer.write(snapshot.generation, &write))).unwrap()
    })
    .join()
    .unwrap();
    let cursor = if broken {
        // Wrong: a second cut skips a write absent from the original snapshot.
        loom::future::block_on(Box::pin(files.inner.snapshot(&query(), 8)))
            .unwrap()
            .cursor
    } else {
        snapshot.cursor
    };
    let mut changes =
        loom::future::block_on(Box::pin(files.inner.follow(query(), cursor))).unwrap();
    let observed = match next(&mut changes, Waker::noop()) {
        Poll::Ready(Some(Ok(change))) => change.file,
        _ => None,
    };
    assert_eq!(
        observed.as_ref(),
        Some(&expected),
        "snapshot/follow skipped request"
    );
}

fn wake(broken: bool) {
    let files = files();
    let mut observation =
        loom::future::block_on(Box::pin(files.observe(query(), None, 8))).unwrap();
    let snapshot = observation.snapshot.take().unwrap();
    assert!(snapshot.files.is_empty());
    let expected = request();
    let writer = files.clone();
    let write = expected.clone();
    let producer = loom::thread::spawn(move || {
        loom::future::block_on(Box::pin(writer.write(snapshot.generation, &write))).unwrap()
    });
    let woken = Arc::new(Woken(AtomicBool::new(false)));
    let waker = Waker::from(woken.clone());
    let first = next(
        &mut observation.changes,
        if broken { Waker::noop() } else { &waker },
    );
    producer.join().unwrap();
    let observed = match first {
        Poll::Ready(Some(Ok(change))) => change.file,
        Poll::Pending => {
            assert!(
                woken.0.load(Ordering::SeqCst),
                "accepted request did not wake its follower"
            );
            match next(&mut observation.changes, &waker) {
                Poll::Ready(Some(Ok(change))) => change.file,
                _ => None,
            }
        }
        other => panic!("unexpected follow result: {other:?}"),
    };
    assert_eq!(
        observed.as_ref(),
        Some(&expected),
        "accepted request was not delivered"
    );
}

fn check(scenario: fn(bool), diagnostic: &str) {
    loom::model(move || scenario(false));
    let error = std::panic::catch_unwind(move || loom::model(move || scenario(true)))
        .expect_err("broken control was accepted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains(diagnostic),
        "unexpected negative control: {message}"
    );
}

#[test]
fn a_request_accepted_during_the_snapshot_handoff_is_replayed() {
    check(gap, "snapshot/follow skipped request");
}
#[test]
fn a_request_accepted_after_registration_wakes_and_reaches_its_follower() {
    check(wake, "accepted request did not wake its follower");
}
