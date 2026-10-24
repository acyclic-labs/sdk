//! One real pending request, one client actor, one monotone prefix publisher.
use super::super::{FileClient, FileClientDurability, FileClientError, FileClientLimits};
use super::{
    Barrier, Event, FAULT, Fault, Input,
    provider::{Monitor, Provider},
};
use crate::{
    Error,
    task_volume::{FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{StreamPath, StreamProvider};
use loom::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

pub(super) fn run(input: Input, fault: Option<Fault>) -> Event {
    let cap = Arc::new(AtomicU64::new(u64::MAX));
    let local = Arc::new(Provider::new(if input.barrier == Barrier::Target {
        None
    } else {
        Some(cap.clone())
    }));
    let remote = Arc::new(Provider::new(if input.barrier == Barrier::Target {
        Some(cap.clone())
    } else {
        None
    }));
    let local_path = StreamPath::new("client/outbox").unwrap();
    let target_path = StreamPath::new("task/inbox").unwrap();
    let outbox = Arc::new(block_on(TaskVolume::open(local.clone(), local_path.clone())).unwrap());
    let target = Arc::new(block_on(TaskVolume::open(remote.clone(), target_path.clone())).unwrap());
    let local_base = block_on(local.inner.tail(local_path.clone())).unwrap();
    let target_base = block_on(remote.inner.tail(target_path.clone())).unwrap();
    // Setup has reported no prefix beyond these bases. Future checkpoint and
    // request each append exactly one record, asserted below. Setting the cap
    // now cannot regress any previously reported durability.
    let intent = local_base + 2;
    cap.store(
        match input.barrier {
            Barrier::Intent => intent - 1,
            Barrier::Target => target_base,
            Barrier::Checkpoint => intent,
        },
        Ordering::SeqCst,
    );
    let limits = FileClientLimits {
        commands: 1,
        bytes: 1,
        durability: FileClientDurability::Durable,
    };
    let mut client = block_on(FileClient::open(outbox.clone(), target.clone(), limits)).unwrap();
    let request = Write::new(
        FileKey::parse(".tasks/task/requests/op.json").unwrap(),
        vec![7],
    );
    block_on(client.enqueue(request.clone())).unwrap();
    assert_eq!(block_on(local.inner.tail(local_path)).unwrap(), intent);
    let required = if input.barrier == Barrier::Intent {
        intent
    } else {
        target_base + 1
    };
    let monitor = Arc::new(Monitor {
        prefix: cap.clone(),
        required,
        premature: Mutex::new(false),
    });
    match input.barrier {
        Barrier::Intent => {
            assert!(remote.monitor.set(monitor.clone()).is_ok());
        }
        Barrier::Target => {
            assert!(local.monitor.set(monitor.clone()).is_ok());
        }
        Barrier::Checkpoint => (),
    }
    let writer = input.concurrent.then(|| {
        let cap = cap.clone();
        loom::thread::spawn(move || cap.store(u64::MAX, Ordering::SeqCst))
    });
    FAULT.with(|current| current.set(fault));
    let first = block_on(client.flush());
    // Capture the early-retirement cut BEFORE joining or releasing the writer.
    // Later success cannot repair this observation. No protocol decision uses it.
    let early_retirement = input.barrier == Barrier::Checkpoint
        && client.pending() == 0
        && cap.load(Ordering::SeqCst) < intent + 1;
    let premature = *monitor.premature.lock().unwrap() || early_retirement;
    if let Some(writer) = writer {
        writer.join().unwrap();
    } else {
        cap.store(u64::MAX, Ordering::SeqCst);
    }
    // Retain blanket refusal for the progress control, remove only missing-gate
    // faults for the ordinary exact retry after all prefixes become durable.
    FAULT.with(|current| current.set(fault.filter(|fault| matches!(fault, Fault::Refuse))));
    let retry = block_on(client.flush());
    FAULT.with(|current| current.set(None));
    let pending = client.pending();
    let acknowledged = client.cursor().position;
    drop(client);
    let restored = block_on(FileClient::open(outbox, target.clone(), limits));
    let snapshot = block_on(target.snapshot(&Query::Tasks, 1)).unwrap();
    let first_valid = matches!(
        first,
        Ok(1) | Err(FileClientError::Harness(Error::Conflict(_)))
    );
    let progress = first_valid
        && retry.is_ok()
        && first.unwrap_or(0) + retry.unwrap_or(0) == 1
        && pending == 0
        && acknowledged == intent
        && restored.is_ok_and(|client| client.pending() == 0 && client.cursor().position == intent)
        && snapshot.files == [request]
        && block_on(remote.inner.tail(target_path)).unwrap() == target_base + 1;
    Event {
        barrier: input.barrier,
        premature,
        progress,
    }
}
