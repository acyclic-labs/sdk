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
    let mut native_error = None;
    tracing::dispatcher::with_default(&dispatch, || -> io::Result<()> {
        crate::persistence_syscall(|| {
            crate::persistence_syscall(|| crate::sync_file(&file, Durability::Full, None))
        })?;
        assert_eq!(query.snapshot().ok, 1, "nested markers counted as OS calls");
        crate::sync_data(&file, Durability::Full)?;
        assert_eq!(query.snapshot().ok, 2);
        #[cfg(not(target_vendor = "apple"))]
        {
            let result = crate::sync_file(&file, Durability::Barrier, None);
            assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::Unsupported));
            assert_eq!(
                query.snapshot().ok,
                2,
                "unsupported barrier counted as a flush"
            );
            assert_eq!(query.snapshot().err, 0);
        }
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
    assert_eq!(query.snapshot().ok, 5);
    #[cfg(not(any(windows, target_os = "linux")))]
    assert_eq!(query.snapshot().ok, 3);
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
    #[cfg(target_os = "linux")]
    assert_eq!(full.syscall, Some(DurabilitySyscall::Fsync));
    #[cfg(target_vendor = "apple")]
    assert_eq!(full.syscall, Some(DurabilitySyscall::FullFsync));
    #[cfg(windows)]
    assert_eq!(full.syscall, Some(DurabilitySyscall::FlushFileBuffers));
    assert_eq!(full.descriptor, Some(descriptor));
    assert_eq!(full.os_error, None);
    let data = attempts.get(1).expect("original data flush");
    #[cfg(target_os = "linux")]
    assert_eq!(
        data.durability.unwrap().syscall,
        Some(DurabilitySyscall::Fdatasync)
    );
    #[cfg(not(target_os = "linux"))]
    assert_eq!(data.durability.unwrap().syscall, full.syscall);
    let failed = attempts.get(2).expect("actual failed native attempt");
    assert_eq!(failed.outcome, Outcome::Err);
    assert_eq!(failed.durability.unwrap().os_error, native_error);
    assert!(failed.error_kind.is_none(), "failure detail was disabled");
    for injected in attempts
        .get(3..5)
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
    assert_eq!(worker.durability, Some(full));
    let evidence = capture
        .0
        .lock()
        .map_err(|_| io::Error::other("capture poisoned"))?;
    let original = evidence
        .spans
        .iter()
        .rfind(|span| span.id == worker.span_id.into_u64())
        .expect("original worker attempt");
    assert_eq!(
        worker.parent_id.map(tracing::Id::into_u64),
        original.parent_id
    );
    let parent = &evidence.spans[original.parent.expect("original worker parent")];
    assert_eq!(parent.name, "waiting");
    let owner = &evidence.spans[parent.parent.expect("original worker owner")];
    assert_eq!(owner.name, "original-durability-owner");
    assert!(original.closed && parent.closed && owner.closed);
    drop(attempts);
    assert_eq!(credit.available_permits() + records.len(), 64);
    drop(records);
    assert_eq!(credit.available_permits(), 64);
    Ok(())
}
