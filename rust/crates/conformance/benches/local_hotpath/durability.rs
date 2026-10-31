//! Bounded projection of the existing collector's typed native samples.
use acyclic_native_runtime::obs::{DurabilitySyscall, Outcome, Sample, Sink};
use serde::Serialize;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Default)]
struct Fields {
    calls: [[AtomicU64; 3]; 7],
    missing: [AtomicU64; 5],
}

/// A sink on the one collector, without a new operation owner or field parser.
#[derive(Clone, Default)]
pub(super) struct Projection(Arc<Fields>);

#[derive(Serialize)]
pub(super) struct Snapshot {
    pub(super) calls: [[Option<u64>; 3]; 7],
    pub(super) missing: [Option<u64>; 5],
}

fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_add(1)
    });
}

fn count(counter: &AtomicU64) -> Option<u64> {
    let value = counter.load(Ordering::Relaxed);
    (value != u64::MAX).then_some(value)
}

impl Sink for Projection {
    fn record(&self, sample: &Sample<'_>) {
        if sample.metadata.name() != "acyclic.runtime.durability" {
            return;
        }
        let fields = sample.durability.unwrap_or_default();
        let syscall = match fields.syscall {
            None => 0,
            Some(DurabilitySyscall::Fsync) => 1,
            Some(DurabilitySyscall::Fdatasync) => 2,
            Some(DurabilitySyscall::FullFsync) => 3,
            Some(DurabilitySyscall::BarrierFsync) => 4,
            Some(DurabilitySyscall::FlushFileBuffers) => 5,
            Some(DurabilitySyscall::Syncfs) => 6,
        };
        let outcome = match sample.outcome {
            Outcome::Ok => 0,
            Outcome::Err => 1,
            Outcome::Incomplete => 2,
        };
        increment(&self.0.calls[syscall][outcome]);
        for (index, missing) in [
            fields.requested.is_none(),
            fields.syscall.is_none(),
            fields.descriptor.is_none(),
            sample.parent_id.is_none(),
            sample.outcome == Outcome::Err && fields.os_error.is_none(),
        ]
        .into_iter()
        .enumerate()
        {
            if missing {
                increment(&self.0.missing[index]);
            }
        }
    }
}

impl Projection {
    pub(super) fn snapshot(&self) -> Snapshot {
        // Individual atomic observations, never a concurrent transactional cut.
        Snapshot {
            calls: std::array::from_fn(|syscall| {
                std::array::from_fn(|outcome| count(&self.0.calls[syscall][outcome]))
            }),
            missing: std::array::from_fn(|index| count(&self.0.missing[index])),
        }
    }
}
