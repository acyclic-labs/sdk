//! Exercise the bound writer's actual task-volume/provider locks and follow.
use super::fixtures::{authority, volume};
use super::*;
use crate::{
    AgentId,
    task_volume::{Component, TaskFile},
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

fn scenario(broken: bool, volume: VolumeRef, verifier: AuthorityVerifier, scope: Scope) {
    let provider = Arc::new(MemoryStream::default());
    let files = Arc::new(
        loom::future::block_on(Box::pin(TaskFiles::open(
            provider, &volume, &verifier, &scope, 4096,
        )))
        .unwrap(),
    );
    let task = Component::new("task").unwrap();
    let query = Query::Requests(task.clone());
    let initial = loom::future::block_on(Box::pin(files.observe(query.clone(), None, 8))).unwrap();
    let generation = initial.snapshot.unwrap().generation;
    drop(initial.changes);
    let request = FileValue::new(
        FileKey::Task {
            task,
            file: TaskFile::Request(Component::new("request").unwrap()),
        },
        b"request".to_vec(),
    );
    let writing = files.clone();
    let expected = request.clone();
    let writer = loom::thread::spawn(move || {
        loom::future::block_on(Box::pin(writing.write(generation, &request))).unwrap();
    });
    // The writer may publish between the atomic snapshot and follow registration.
    let mut observation =
        loom::future::block_on(Box::pin(files.observe(query.clone(), None, 8))).unwrap();
    let mut observed = observation
        .snapshot
        .take()
        .unwrap()
        .files
        .into_iter()
        .next();
    let woken = Arc::new(Woken(AtomicBool::new(false)));
    let waker = Waker::from(woken.clone());
    let mut context = Context::from_waker(&waker);
    let first = if observed.is_none() {
        Pin::new(&mut observation.changes).poll_next(&mut context)
    } else {
        Poll::Pending
    };
    writer.join().unwrap();
    if broken {
        // Broken handoff: take a newer cursor while discarding its snapshot.
        // The accepted request is then behind the follow and cannot be delivered.
        observation = loom::future::block_on(Box::pin(files.observe(query, None, 8))).unwrap();
        observed = None;
    } else if observed.is_none() {
        match first {
            Poll::Ready(Some(Ok(change))) => observed = change.file,
            Poll::Pending => {
                assert!(
                    woken.0.load(Ordering::SeqCst),
                    "accepted request did not wake its follower"
                );
            }
            other => panic!("unexpected first follow result: {other:?}"),
        }
    }
    if observed.is_none() {
        if let Poll::Ready(Some(Ok(change))) =
            Pin::new(&mut observation.changes).poll_next(&mut context)
        {
            observed = change.file;
        }
    }
    assert_eq!(
        observed.as_ref(),
        Some(&expected),
        "snapshot/follow skipped request"
    );
}

#[test]
fn bound_request_writes_have_no_snapshot_gap_or_lost_wakeup() {
    let agent = AgentId::from_bytes([1; 16]);
    let volume = volume(agent);
    let (verifier, scope) = authority(agent, &volume, true);
    let fixture = (volume.clone(), verifier.clone(), scope.clone());
    loom::model(move || scenario(false, volume.clone(), verifier.clone(), scope.clone()));
    let error = std::panic::catch_unwind(move || {
        loom::model(move || {
            scenario(
                true,
                fixture.0.clone(),
                fixture.1.clone(),
                fixture.2.clone(),
            )
        })
    })
    .expect_err("broken snapshot handoff was accepted");
    let diagnostic = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        diagnostic.contains("snapshot/follow skipped request"),
        "unexpected negative control: {diagnostic}"
    );
}
