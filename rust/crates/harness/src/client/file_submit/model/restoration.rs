//! Public reopening raced against confirmation of its actual accepted checkpoint.
use super::super::{FileClient, FileClientError};
use super::{
    Barrier, Event, FAULT, Fault, Input,
    setup::{self, Setup, block_on},
};
use crate::{Error, task_volume::Query};
use acyclic_stream::StreamProvider;
use loom::sync::atomic::Ordering;

pub(super) fn run(input: Input, fault: Option<Fault>) -> Event {
    let Setup {
        local,
        remote,
        outbox,
        target,
        outbox_path,
        target_path,
        cap,
        limits,
        mut client,
        request,
        intent,
        target_base,
    } = setup::prepare(Barrier::Restoration);
    // Real publication succeeds, but its new checkpoint is still only accepted.
    // No fault is active during preparation; a fixture failure is infrastructure.
    let prepared = block_on(client.flush());
    assert!(
        matches!(prepared, Err(FileClientError::Harness(Error::Conflict(ref reason)))
        if reason == "outbox checkpoint awaits durability")
    );
    assert_eq!(client.pending(), 1);
    let checkpoint = block_on(local.inner.tail(outbox_path)).unwrap();
    assert_eq!(checkpoint, intent + 1);
    assert!(cap.load(Ordering::SeqCst) < checkpoint);
    drop(client);

    let publisher = input.concurrent.then(|| {
        let cap = cap.clone();
        loom::thread::spawn(move || cap.store(u64::MAX, Ordering::SeqCst))
    });
    FAULT.with(|current| current.set(fault));
    let first = block_on(FileClient::open(outbox.clone(), target.clone(), limits));
    // Save this forbidden effect before the publisher can repair the final state.
    let premature = first.is_ok() && cap.load(Ordering::SeqCst) < checkpoint;
    let first_valid = match &first {
        Ok(_) => true,
        Err(FileClientError::Harness(Error::Conflict(reason))) => {
            reason == "outbox checkpoint awaits durability"
        }
        _ => false,
    };
    drop(first);
    if let Some(publisher) = publisher {
        publisher.join().unwrap();
    } else {
        cap.store(u64::MAX, Ordering::SeqCst);
    }

    FAULT.with(|current| current.set(fault.filter(|fault| matches!(fault, Fault::Refuse))));
    let confirmed = block_on(FileClient::open(outbox, target.clone(), limits));
    FAULT.with(|current| current.set(None));
    let snapshot = block_on(target.snapshot(&Query::Tasks, 1)).unwrap();
    let progress = first_valid
        && confirmed
            .is_ok_and(|client| client.pending() == 0 && client.cursor().position == intent)
        && snapshot.files == [request]
        && block_on(remote.inner.tail(target_path)).unwrap() == target_base + 1;
    Event {
        barrier: Barrier::Restoration,
        premature,
        progress,
    }
}
