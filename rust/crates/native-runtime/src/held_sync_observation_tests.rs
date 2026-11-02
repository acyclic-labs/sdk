use super::*;
use crate::{
    Durability,
    obs::{Metrics, Operations},
};
use acyclic_grpc_observability::tests::Capture;
use acyclic_observability::{DurabilityRequest, DurabilitySyscall, Export, Outcome};
use std::sync::Arc;
use tracing_subscriber::layer::SubscriberExt as _;

#[test]
fn physical_attempts_preserve_errors_and_original_worker_context() -> io::Result<()> {
    if crate::tests::passed_in_exclusive_process(
        "held_sync::observation_tests::physical_attempts_preserve_errors_and_original_worker_context",
    )? {
        return Ok(());
    }
    tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
        let allocations = acyclic_test_support::measure(|| {
            assert!(observe_sync(Durability::Full, "disabled-control", 0, || Ok(())).is_ok());
        });
        assert_eq!(allocations.count_total, 0);
    });
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("durable-record");
    let file = std::fs::File::create(&path)?;
    subscriber_unwind_preserves_attempt(&file, &path)?;
    let credit = Arc::new(tokio::sync::Semaphore::new(64));
    let (export, mut receiver) = Export::channel(Arc::clone(&credit), false);
    let capture = Capture::default();
    let metrics = Metrics::default();
    let query = metrics.query("acyclic.runtime.durability");
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(capture.clone())
            .with(Operations((metrics, export.clone()))),
    );
    #[cfg(unix)]
    let descriptor = {
        use std::os::fd::AsRawFd as _;
        file.as_raw_fd() as u64
    };
    #[cfg(windows)]
    let descriptor = {
        use std::os::windows::io::AsRawHandle as _;
        file.as_raw_handle() as usize as u64
    };
    #[cfg(target_vendor = "apple")]
    let full_attempts = 2;
    #[cfg(not(target_vendor = "apple"))]
    let full_attempts = 1;
    let mut native_error = None;
    tracing::dispatcher::with_default(&dispatch, || -> io::Result<()> {
        let denied = crate::tests::ReadBudgetMeter::new(0, 0);
        let before = query.snapshot();
        let refusal = crate::sync_file(&file, Durability::Full, Some(&denied))
            .expect_err("refused admission cannot reach a native attempt");
        assert_eq!(refusal.kind(), io::ErrorKind::Other);
        assert_eq!(
            query.snapshot(),
            before,
            "admission refusal became an OS call"
        );
        crate::persistence_syscall(|| {
            crate::persistence_syscall(|| crate::sync_file(&file, Durability::Full, None))
        })?;
        assert_eq!(
            query.snapshot().ok,
            full_attempts,
            "nested markers counted as OS calls"
        );
        crate::sync_data(&file, Durability::Full)?;
        assert_eq!(query.snapshot().ok, full_attempts * 2);
        // These keep the original weaker request even on platforms whose only
        // physical implementation is a full flush. No flush policy changes.
        let admitted = crate::tests::ReadBudgetMeter::new(1, 0);
        for index in 0..4 {
            match index {
                0 => crate::sync_file(&file, Durability::Barrier, None)?,
                1 => crate::sync_file(&file, Durability::Barrier, Some(&admitted))?,
                2 => {
                    #[cfg(unix)]
                    {
                        use std::os::fd::AsFd as _;
                        crate::order_descriptor_once(file.as_fd())?;
                    }
                    #[cfg(windows)]
                    {
                        use std::os::windows::io::AsHandle as _;
                        crate::order_descriptor_once(file.as_handle())?;
                    }
                }
                _ => crate::sync_data(&file, Durability::Barrier)?,
            }
            assert_eq!(
                query.snapshot().ok,
                full_attempts * 2 + index + 1,
                "one actual ordering attempt must count once"
            );
            assert_eq!(query.snapshot().err, 0);
        }
        assert_eq!(
            admitted
                .used_operations
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        #[cfg(unix)]
        let error = {
            use std::os::fd::AsFd as _;
            let invalid = std::fs::File::open("/dev/null")?;
            sync_descriptor_once(invalid.as_fd()).expect_err("device cannot flush")
        };
        #[cfg(windows)]
        let error = {
            use std::os::windows::io::AsHandle as _;
            let readonly = std::fs::File::open(&path)?;
            sync_descriptor_once(readonly.as_handle()).expect_err("read-only handle cannot flush")
        };
        native_error = error.raw_os_error();
        assert!(native_error.is_some());
        // Injected Interrupted outcomes exercise the same per-attempt observer.
        // They are synthetic controls, separate from the real descriptor calls.
        let before = query.snapshot().err;
        for _ in 0..2 {
            let result = observe_sync(Durability::Full, "injected-interrupted", 0, || {
                Err(io::ErrorKind::Interrupted.into())
            });
            assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::Interrupted));
        }
        assert_eq!(
            query.snapshot().err - before,
            2,
            "retrying wrapper hid attempts"
        );
        #[cfg(any(windows, target_os = "linux"))]
        {
            let before = query.snapshot();
            crate::flush_file_data(&file)?;
            crate::flush_file_all(&file)?;
            assert_eq!(query.snapshot().ok - before.ok, 2);
            #[cfg(unix)]
            let invalid = std::fs::File::open("/dev/null")?;
            #[cfg(windows)]
            let invalid = std::fs::File::open(&path)?;
            let original = invalid.sync_all().expect_err("descriptor cannot flush");
            let observed = crate::flush_file_all(&invalid).expect_err("descriptor cannot flush");
            assert_eq!(observed.raw_os_error(), original.raw_os_error());
            assert_eq!(query.snapshot().err - before.err, 1);
        }
        Ok(())
    })?;
    let runtime = crate::coordination_runtime()?;
    runtime.block_on(async {
        let job = tracing::dispatcher::with_default(&dispatch, || {
            tracing::info_span!("original-durability-owner").in_scope(|| {
                crate::spawn_persistence(
                    "streams",
                    "commit",
                    crate::PersistenceKey::new(17, 3),
                    move || {
                        // A foreign ambient subscriber cannot steal the job's leaf.
                        tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
                            crate::sync_file(&file, Durability::Full, None)
                        })
                    },
                )
            })
        });
        job.await?
    })?;
    #[cfg(any(windows, target_os = "linux"))]
    assert_eq!(query.snapshot().ok, 9);
    #[cfg(not(any(windows, target_os = "linux")))]
    assert_eq!(query.snapshot().ok, full_attempts * 3 + 4);
    assert_eq!(export.stats().full, Some(0));
    drop(dispatch);
    drop(export);
    // The original collector is the channel producer. Its retirement ends this
    // event wait; each returned record keeps its own output credit until drop.
    let records = runtime.block_on(async {
        let mut records = Vec::new();
        while let Some(record) = receiver.recv().await {
            records.push(record);
        }
        records
    });
    let attempts: Vec<_> = records
        .iter()
        .map(|record| record.sample())
        .filter(|sample| sample.metadata.name() == "acyclic.runtime.durability")
        .collect();
    let first = attempts.first().expect("original full flush");
    let full = first.durability.expect("typed full flush");
    assert_eq!(full.requested, Some(DurabilityRequest::Full));
    #[cfg(unix)]
    assert_eq!(full.syscall, Some(DurabilitySyscall::Fsync));
    #[cfg(target_vendor = "apple")]
    assert_eq!(
        attempts[1].durability.unwrap().syscall,
        Some(DurabilitySyscall::FullFsync)
    );
    #[cfg(windows)]
    assert_eq!(full.syscall, Some(DurabilitySyscall::FlushFileBuffers));
    assert_eq!(full.descriptor, Some(descriptor));
    assert_eq!(full.os_error, None);
    let data = attempts
        .get(full_attempts as usize)
        .expect("original data flush");
    #[cfg(target_os = "linux")]
    assert_eq!(
        data.durability.unwrap().syscall,
        Some(DurabilitySyscall::Fdatasync)
    );
    #[cfg(not(target_os = "linux"))]
    assert_eq!(data.durability.unwrap().syscall, full.syscall);
    let barrier_start = (full_attempts * 2) as usize;
    for (index, attempt) in attempts[barrier_start..barrier_start + 4]
        .iter()
        .enumerate()
    {
        let evidence = attempt.durability.unwrap();
        assert_eq!(evidence.requested, Some(DurabilityRequest::Barrier));
        assert_eq!(evidence.descriptor, Some(descriptor));
        #[cfg(target_os = "linux")]
        assert_eq!(
            evidence.syscall,
            Some(if index == 3 {
                DurabilitySyscall::Fdatasync
            } else {
                DurabilitySyscall::Fsync
            })
        );
        #[cfg(target_vendor = "apple")]
        assert_eq!(evidence.syscall, Some(DurabilitySyscall::BarrierFsync));
        #[cfg(windows)]
        assert_eq!(evidence.syscall, Some(DurabilitySyscall::FlushFileBuffers));
        #[cfg(not(target_os = "linux"))]
        let _ = index;
    }
    let failed_index = barrier_start + 4;
    let failed = attempts
        .get(failed_index)
        .expect("actual failed native attempt");
    assert_eq!(failed.outcome, Outcome::Err);
    assert_eq!(failed.durability.unwrap().os_error, native_error);
    assert!(failed.error_kind.is_none(), "failure detail was disabled");
    for injected in attempts
        .get(failed_index + 1..failed_index + 3)
        .expect("two injected Interrupted attempts")
    {
        let evidence = injected.durability.unwrap();
        assert_eq!(injected.outcome, Outcome::Err);
        assert_eq!(
            evidence.syscall, None,
            "synthetic attempt claimed an OS call"
        );
        assert_eq!(evidence.os_error, None);
        assert_eq!(evidence.descriptor, Some(0));
    }
    let worker = attempts.last().expect("original worker attempt");
    let worker_attempt = worker.durability.unwrap();
    assert_eq!(worker_attempt.requested, full.requested);
    assert_eq!(worker_attempt.descriptor, full.descriptor);
    assert_eq!(worker_attempt.os_error, None);
    #[cfg(not(target_vendor = "apple"))]
    assert_eq!(worker_attempt.syscall, full.syscall);
    #[cfg(target_vendor = "apple")]
    assert_eq!(worker_attempt.syscall, Some(DurabilitySyscall::FullFsync));
    let evidence = capture
        .0
        .lock()
        .map_err(|_| io::Error::other("capture poisoned"))?;
    let original = evidence
        .spans
        .iter()
        .rfind(|span| {
            span.id == worker.span_id.into_u64()
                && span.name == "acyclic.runtime.durability"
                && span.parent_id == worker.parent_id.map(tracing::Id::into_u64)
        })
        .expect("original worker attempt");
    assert_eq!(
        worker.parent_id.map(tracing::Id::into_u64),
        original.parent_id
    );
    let task = &evidence.spans[original.parent.expect("original native task")];
    assert_eq!(task.name, "acyclic.runtime.persistence");
    let parent = &evidence.spans[task.parent.expect("original worker wait")];
    assert_eq!(parent.name, "waiting");
    let owner = &evidence.spans[parent.parent.expect("original worker owner")];
    assert_eq!(owner.name, "original-durability-owner");
    assert!(original.closed && task.closed && parent.closed && owner.closed);
    drop(attempts);
    assert_eq!(credit.available_permits() + records.len(), 64);
    drop(records);
    assert_eq!(credit.available_permits(), 64);
    Ok(())
}

fn subscriber_unwind_preserves_attempt(
    file: &std::fs::File,
    path: &std::path::Path,
) -> io::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing::{
        Subscriber,
        field::Visit,
        span::{Attributes, Record},
    };
    use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

    #[derive(Clone, Copy, PartialEq)]
    enum Cut {
        Create,
        Error,
        Outcome,
        Close,
        OutcomeAndClose,
    }
    struct Failure {
        target: &'static str,
        cut: Cut,
        panics: AtomicUsize,
        attempts: AtomicUsize,
    }
    impl Failure {
        fn trip(&self, cut: Cut) {
            if (self.cut == cut
                || self.cut == Cut::OutcomeAndClose && matches!(cut, Cut::Outcome | Cut::Close))
                && self.panics.fetch_add(1, Ordering::SeqCst)
                    < if self.cut == Cut::OutcomeAndClose {
                        2
                    } else {
                        1
                    }
            {
                panic!("original durability subscriber control");
            }
        }
    }
    #[derive(Clone)]
    struct Probe(Arc<Failure>);
    impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Probe {
        fn on_new_span(&self, attributes: &Attributes<'_>, _: &tracing::Id, _: Context<'_, S>) {
            if attributes.metadata().name() == "acyclic.runtime.durability" {
                self.0.attempts.fetch_add(1, Ordering::SeqCst);
            }
            if attributes.metadata().name() == self.0.target {
                self.0.trip(Cut::Create);
            }
        }
        fn on_record(&self, id: &tracing::Id, record: &Record<'_>, context: Context<'_, S>) {
            if context
                .metadata(id)
                .is_none_or(|metadata| metadata.name() != self.0.target)
            {
                return;
            }
            // The original layer shares its failure state with the caller.
            struct Fields<'a>(&'a Failure);
            impl Visit for Fields<'_> {
                fn record_debug(&mut self, field: &tracing::field::Field, _: &dyn std::fmt::Debug) {
                    match field.name() {
                        "error.os" => self.0.trip(Cut::Error),
                        "outcome" => self.0.trip(Cut::Outcome),
                        _ => {}
                    }
                }
            }
            record.record(&mut Fields(&self.0));
        }
        fn on_close(&self, id: tracing::Id, context: Context<'_, S>) {
            if context
                .metadata(&id)
                .is_some_and(|metadata| metadata.name() == self.0.target)
            {
                self.0.trip(Cut::Close);
            }
        }
    }

    #[cfg(unix)]
    let invalid = std::fs::File::open("/dev/null")?;
    #[cfg(windows)]
    let invalid = std::fs::File::open(path)?;
    #[cfg(unix)]
    let _ = path;
    let native_error = invalid
        .sync_all()
        .expect_err("original descriptor cannot flush")
        .raw_os_error();
    assert!(native_error.is_some());
    for succeeds in [false, true] {
        for cut in [
            Cut::Create,
            Cut::Error,
            Cut::Outcome,
            Cut::Close,
            Cut::OutcomeAndClose,
        ] {
            if succeeds && cut == Cut::Error {
                continue;
            }
            let failure = Arc::new(Failure {
                target: "acyclic.runtime.durability",
                cut,
                panics: AtomicUsize::new(0),
                attempts: AtomicUsize::new(0),
            });
            let dispatch = tracing::Dispatch::new(
                tracing_subscriber::registry().with(Probe(Arc::clone(&failure))),
            );
            let attempts = std::cell::Cell::new(0);
            let result = tracing::dispatcher::with_default(&dispatch, || {
                observe_sync(Durability::Full, "native-control", 0, || {
                    attempts.set(attempts.get() + 1);
                    if succeeds {
                        file.sync_all()
                    } else {
                        invalid.sync_all()
                    }
                })
            });
            assert_eq!(attempts.get(), 1, "subscriber changed native attempt count");
            assert_eq!(
                failure.panics.load(Ordering::SeqCst),
                if cut == Cut::OutcomeAndClose { 2 } else { 1 }
            );
            if succeeds {
                result?;
            } else {
                assert_eq!(
                    result.expect_err("original native refusal").raw_os_error(),
                    native_error
                );
            }
        }
    }
    // Public file flushes use only the original physical-attempt observer.
    // Restoring either duplicate wrapper trips this subscriber before I/O.
    for data in [false, true] {
        let flush = |file: &std::fs::File| {
            if data {
                crate::sync_data(file, Durability::Full)
            } else {
                crate::sync_file(file, Durability::Full, None)
            }
        };
        let expected_error = flush(&invalid)
            .expect_err("original public flush refusal")
            .raw_os_error();
        let failure = Arc::new(Failure {
            target: if data {
                "acyclic.runtime.sync_data"
            } else {
                "acyclic.runtime.sync_file"
            },
            cut: Cut::Create,
            panics: AtomicUsize::new(0),
            attempts: AtomicUsize::new(0),
        });
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(Probe(Arc::clone(&failure))),
        );
        tracing::dispatcher::with_default(&dispatch, || -> io::Result<()> {
            flush(file)?;
            assert_eq!(
                flush(&invalid)
                    .expect_err("original public flush refusal")
                    .raw_os_error(),
                expected_error
            );
            Ok(())
        })?;
        assert_eq!(failure.panics.load(Ordering::SeqCst), 0);
        #[cfg(target_vendor = "apple")]
        let expected_attempts = 4;
        #[cfg(not(target_vendor = "apple"))]
        let expected_attempts = 2;
        assert_eq!(failure.attempts.load(Ordering::SeqCst), expected_attempts);
        let reached = std::cell::Cell::new(false);
        let restored = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracing::dispatcher::with_default(&dispatch, || {
                let duplicate = if data {
                    tracing::debug_span!("acyclic.runtime.sync_data")
                } else {
                    tracing::debug_span!("acyclic.runtime.sync_file")
                };
                duplicate.in_scope(|| reached.set(true));
            });
        }));
        assert!(restored.is_err() && !reached.get());
        assert_eq!(failure.panics.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
