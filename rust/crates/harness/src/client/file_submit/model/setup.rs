//! Shared production construction, without a second outbox implementation.
use super::super::{FileClient, FileClientDurability, FileClientLimits};
use super::{Barrier, FAULT, provider::Provider};
use crate::task_volume::{FileKey, TaskVolume, Write};
use acyclic_stream::{StreamPath, StreamProvider};
use loom::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub(super) fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

pub(super) struct Setup {
    pub(super) local: Arc<Provider>,
    pub(super) remote: Arc<Provider>,
    pub(super) outbox: Arc<TaskVolume<Provider>>,
    pub(super) target: Arc<TaskVolume<Provider>>,
    pub(super) outbox_path: StreamPath,
    pub(super) target_path: StreamPath,
    pub(super) cap: Arc<AtomicU64>,
    pub(super) limits: FileClientLimits,
    pub(super) client: FileClient<Provider, Provider>,
    pub(super) request: Write,
    pub(super) intent: u64,
    pub(super) target_base: u64,
}

pub(super) fn prepare(barrier: Barrier) -> Setup {
    FAULT.with(|current| current.set(None));
    let cap = Arc::new(AtomicU64::new(u64::MAX));
    let local = Arc::new(Provider::new(if barrier == Barrier::Target {
        None
    } else {
        Some(cap.clone())
    }));
    let remote = Arc::new(Provider::new(if barrier == Barrier::Target {
        Some(cap.clone())
    } else {
        None
    }));
    let outbox_path = StreamPath::new("client/outbox").unwrap();
    let target_path = StreamPath::new("task/inbox").unwrap();
    let outbox = Arc::new(block_on(TaskVolume::open(local.clone(), outbox_path.clone())).unwrap());
    let target = Arc::new(block_on(TaskVolume::open(remote.clone(), target_path.clone())).unwrap());
    let local_base = block_on(local.inner.tail(outbox_path.clone())).unwrap();
    let target_base = block_on(remote.inner.tail(target_path.clone())).unwrap();
    // Each initial checkpoint/request appends one record. Install the cap now,
    // no lower than any prefix already reported; durability never regresses.
    let intent = local_base + 2;
    cap.store(
        match barrier {
            Barrier::Intent => intent - 1,
            Barrier::Target => target_base,
            Barrier::Checkpoint | Barrier::Restoration => intent,
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
    assert_eq!(
        block_on(local.inner.tail(outbox_path.clone())).unwrap(),
        intent
    );
    Setup {
        local,
        remote,
        outbox,
        target,
        outbox_path,
        target_path,
        cap,
        limits,
        client,
        request,
        intent,
        target_base,
    }
}
