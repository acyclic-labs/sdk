use super::*;
use crate::{
    Durability,
    obs::{Metrics, Operations},
};
use acyclic_grpc_observability::tests::Capture;
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
    let capture = Capture::default();
    let metrics = Metrics::default();
    let query = metrics.query("acyclic.runtime.durability");
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(capture.clone())
            .with(Operations(metrics)),
    );
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
        {
            let evidence = capture
                .0
                .lock()
                .map_err(|_| io::Error::other("capture poisoned"))?;
            let last = evidence.spans.last().expect("actual failed flush span");
            assert_eq!(
                last.fields.get("error.os"),
                error.raw_os_error().map(|code| code.to_string()).as_ref()
            );
            assert_eq!(last.fields.get("outcome").map(String::as_str), Some("err"));
        }
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
    assert_eq!(query.snapshot().ok, 3);
    assert!(capture.has_ancestor("acyclic.runtime.durability", "original-durability-owner"));
    let evidence = capture
        .0
        .lock()
        .map_err(|_| io::Error::other("capture poisoned"))?;
    let last = evidence.spans.last().expect("worker physical attempt");
    assert_eq!(last.name, "acyclic.runtime.durability");
    assert_eq!(
        last.fields.get("requested").map(String::as_str),
        Some("full")
    );
    assert!(last.fields.contains_key("descriptor"));
    assert!(last.closed);
    Ok(())
}
