//! Bounded projection of the existing collector's typed native samples.
use acyclic_native_runtime::obs::{DurabilitySyscall, NativeTaskTerminal, Outcome, Sample, Sink};
use serde::Serialize;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Default)]
struct Fields {
    calls: [[AtomicU64; 3]; 7],
    missing: [AtomicU64; 5],
    tasks: [TaskFields; 2],
}

#[derive(Default)]
struct TaskFields {
    queued: [AtomicU64; 3],
    started: [AtomicU64; 3],
    terminal: [AtomicU64; 5],
    panicked: [AtomicU64; 3],
    outcome: [AtomicU64; 3],
    missing_parent: AtomicU64,
    missing_io_error: AtomicU64,
}

/// A sink on the one collector, without a new operation owner or field parser.
#[derive(Clone, Default)]
pub(super) struct Projection(Arc<Fields>);

#[derive(Serialize)]
pub(super) struct Snapshot {
    pub(super) calls: [[Option<u64>; 3]; 7],
    pub(super) missing: [Option<u64>; 5],
    pub(super) tasks: [TaskSnapshot; 2],
}

#[derive(Serialize)]
pub(super) struct TaskSnapshot {
    queued: [Option<u64>; 3],
    started: [Option<u64>; 3],
    terminal: [Option<u64>; 5],
    panicked: [Option<u64>; 3],
    outcome: [Option<u64>; 3],
    missing_parent: Option<u64>,
    missing_io_error: Option<u64>,
}

fn outcome_index(outcome: Outcome) -> usize {
    match outcome {
        Outcome::Ok => 0,
        Outcome::Err => 1,
        Outcome::Incomplete => 2,
    }
}

fn boolean_index(value: Option<bool>) -> usize {
    match value {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    }
}

fn counts<const N: usize>(counters: &[AtomicU64; N]) -> [Option<u64>; N] {
    std::array::from_fn(|index| count(&counters[index]))
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
    fn native_task_details(&self) -> bool {
        true
    }

    fn record(&self, sample: &Sample<'_>) {
        let task = match sample.metadata.name() {
            "acyclic.runtime.persistence" => Some(&self.0.tasks[0]),
            "acyclic.runtime.blocking_io" => Some(&self.0.tasks[1]),
            _ => None,
        };
        if let Some(counters) = task {
            let task = sample.native_task.unwrap_or_default();
            increment(&counters.queued[boolean_index(task.queued)]);
            increment(&counters.started[boolean_index(task.started)]);
            increment(&counters.panicked[boolean_index(task.panicked)]);
            increment(&counters.outcome[outcome_index(sample.outcome)]);
            let terminal = match task.terminal {
                None => 0,
                Some(NativeTaskTerminal::Ok) => 1,
                Some(NativeTaskTerminal::IoError) => 2,
                Some(NativeTaskTerminal::Panic) => 3,
                Some(NativeTaskTerminal::Missing) => 4,
            };
            increment(&counters.terminal[terminal]);
            if sample.parent_id.is_none() {
                increment(&counters.missing_parent);
            }
            if task.terminal == Some(NativeTaskTerminal::IoError) && task.os_error.is_none() {
                increment(&counters.missing_io_error);
            }
            return;
        }
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
        let outcome = outcome_index(sample.outcome);
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
            calls: std::array::from_fn(|syscall| counts(&self.0.calls[syscall])),
            missing: counts(&self.0.missing),
            tasks: std::array::from_fn(|index| {
                let task = &self.0.tasks[index];
                TaskSnapshot {
                    queued: counts(&task.queued),
                    started: counts(&task.started),
                    terminal: counts(&task.terminal),
                    panicked: counts(&task.panicked),
                    outcome: counts(&task.outcome),
                    missing_parent: count(&task.missing_parent),
                    missing_io_error: count(&task.missing_io_error),
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn original_task_projection_preserves_inline_work_and_abandonment() -> std::io::Result<()> {
        use super::*;
        use acyclic_native_runtime::{InlineBlocking, run_blocking_io};
        use tracing_subscriber::layer::SubscriberExt as _;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("original");
        std::fs::write(&path, b"original native work")?;
        let projection = Projection::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry()
                .with(acyclic_native_runtime::obs::Operations(projection.clone())),
        );
        tracing::dispatcher::with_default(&dispatch, || -> std::io::Result<()> {
            let parent = tracing::info_span!("original-request");
            let _entered = parent.enter();
            let read = |path| {
                futures::executor::block_on(run_blocking_io(move || std::fs::read(path), None)?)?
            };
            {
                let _inline = InlineBlocking::enter();
                assert_eq!(read(path.clone())?, b"original native work");
            }
            let before = projection.snapshot();
            let before = &before.tasks[1];
            assert_eq!(before.queued, [Some(0), Some(1), Some(0)]);
            assert_eq!(before.started, [Some(0), Some(0), Some(1)]);
            assert_eq!(
                before.terminal,
                [Some(0), Some(1), Some(0), Some(0), Some(0)]
            );
            assert_eq!(before.panicked, [Some(0), Some(1), Some(0)]);
            assert_eq!(before.outcome, [Some(1), Some(0), Some(0)]);
            assert_eq!(before.missing_parent, Some(0));

            // This is the real unsubmitted job, whose original span closes
            // without queue transfer, closure entry or terminal publication.
            drop(run_blocking_io(|| panic!("abandoned task executed"), None)?);
            let after = projection.snapshot();
            let after = &after.tasks[1];
            assert_eq!(after.queued, [Some(1), Some(1), Some(0)]);
            assert_eq!(after.started, [Some(1), Some(0), Some(1)]);
            assert_eq!(
                after.terminal,
                [Some(1), Some(1), Some(0), Some(0), Some(0)]
            );
            assert_eq!(after.panicked, [Some(1), Some(1), Some(0)]);
            assert_eq!(after.outcome, [Some(1), Some(0), Some(1)]);

            // Losing numeric precision cannot turn a later real terminal into
            // zero work. The same fixed counter retires as unknown.
            projection.0.tasks[1].terminal[1].store(u64::MAX - 1, Ordering::Relaxed);
            let _inline = InlineBlocking::enter();
            futures::executor::block_on(run_blocking_io(|| (), None)?)?;
            futures::executor::block_on(run_blocking_io(|| (), None)?)?;
            assert_eq!(projection.snapshot().tasks[1].terminal[1], None);
            assert!(
                projection
                    .snapshot()
                    .calls
                    .iter()
                    .flatten()
                    .all(|count| *count == Some(0))
            );
            Ok(())
        })
    }
}
