//! One real pending request, one client actor, one monotone prefix publisher.
use super::super::{FileClient, FileClientError};
use super::{
    Barrier, Event, FAULT, Fault, Input,
    provider::Monitor,
    setup::{self, Setup, block_on},
};
use crate::{Error, task_volume::Query};
use acyclic_stream::StreamProvider;
use loom::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

pub(super) fn run(input: Input, fault: Option<Fault>) -> Event {
    let Setup {
        local,
        remote,
        outbox,
        target,
        outbox_path: _,
        target_path,
        cap,
        limits,
        mut client,
        request,
        intent,
        target_base,
    } = setup::prepare(input.barrier);
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
        Barrier::Checkpoint | Barrier::Restoration => (),
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
