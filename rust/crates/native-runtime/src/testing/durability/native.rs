//! Held native handles and namespace calls use the original fault controller.
use super::*;
use crate::{NativeReadMeter, RenameMode};

#[derive(Clone, Copy, Debug)]
enum Operation {
    Write,
    UnmeteredWrite,
    Resize,
    Publish,
    Rename,
}

fn retains_cut(
    directory: &std::path::Path,
    operation: Operation,
    fault: DurableFault,
    bytes: &[u8],
    erase: bool,
) -> bool {
    let from = directory.join("from");
    let to = directory.join("to");
    for path in [&from, &to] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => assert_eq!(
                error.kind(),
                std::io::ErrorKind::NotFound,
                "reset cut fixture: {error}"
            ),
        }
    }
    std::fs::write(&from, b"original").unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&from)
        .unwrap();
    let meter: Arc<dyn NativeReadMeter> =
        Arc::new(crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX));
    let renamed = std::cell::Cell::new(0);
    let (schedule, _) = Schedule::seeded(71);
    schedule.cut_at(0, fault);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let result = runtime.block_on(schedule.scope(async {
        let work = async {
            match operation {
                Operation::Write => {
                    crate::write_all_at(&file, 0, bytes, Some((meter.as_ref(), false)))
                }
                Operation::UnmeteredWrite => crate::write_all_at(&file, 0, bytes, None),
                Operation::Resize => crate::set_len(&file, 3, meter.as_ref()),
                Operation::Publish => crate::publish_new(&from, &to, Some(&meter)),
                Operation::Rename => crate::namespace::durable_rename_observed(
                    &from,
                    &to,
                    RenameMode::NoReplace,
                    Some(&meter),
                    || renamed.set(renamed.get() + 1),
                ),
            }
        };
        if erase {
            super::super::untracked(work).await
        } else {
            work.await
        }
    }));
    let cause = result
        .as_ref()
        .err()
        .and_then(|error| error.get_ref())
        .and_then(|error| error.downcast_ref::<InjectedStorageFault>());
    let expected_operation = match operation {
        Operation::Write | Operation::UnmeteredWrite => "file:write",
        Operation::Resize => "file:set_len",
        Operation::Publish => "namespace:publish_new",
        Operation::Rename => "namespace:rename",
    };
    if !cause.is_some_and(|cause| {
        cause.ordinal == 0 && cause.fault == fault && cause.operation == expected_operation
    }) {
        if !erase {
            eprintln!(
                "{operation:?} {fault:?}: {result:?}; trace {:?}",
                schedule.durable_writes()
            );
        }
        return false;
    }
    let after = fault == DurableFault::After;
    match operation {
        Operation::Write | Operation::UnmeteredWrite => {
            let transferred = match fault {
                DurableFault::After => bytes.len(),
                DurableFault::Torn(count) => count.min(bytes.len() - 1),
                _ => 0,
            };
            let mut expected = b"original".to_vec();
            expected.resize(expected.len().max(transferred), 0);
            expected.splice(0..transferred, bytes.iter().take(transferred).copied());
            assert_eq!(std::fs::read(&from).unwrap(), expected);
            assert_eq!(
                schedule.durable_writes().first().unwrap().transferred,
                (transferred != 0).then_some(transferred)
            );
        }
        Operation::Resize => assert_eq!(file.metadata().unwrap().len(), if after { 3 } else { 8 }),
        Operation::Publish => {
            assert!(from.exists());
            assert_eq!(to.exists(), after);
        }
        Operation::Rename => {
            assert_eq!(from.exists(), !after);
            assert_eq!(to.exists(), after);
            assert_eq!(renamed.get(), usize::from(after));
        }
    }
    assert_eq!(
        schedule.durable_writes().len(),
        1,
        "operation dispatched after the cut"
    );
    true
}

fn namespace_interruption(operation: Operation, metered: bool, reissue: bool) -> bool {
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir().unwrap();
    let from = directory.path().join("from");
    let to = directory.path().join("to");
    std::fs::write(&from, b"original").unwrap();
    let observer = Arc::new(crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX));
    let meter: Arc<dyn NativeReadMeter> = observer.clone();
    let meter = metered.then_some(&meter);
    let renamed = std::cell::Cell::new(0);
    let invoke = || match operation {
        Operation::Publish => crate::publish_new(&from, &to, meter),
        Operation::Rename => crate::namespace::durable_rename_observed(
            &from,
            &to,
            RenameMode::NoReplace,
            meter,
            || renamed.set(renamed.get() + 1),
        ),
        _ => Err(io::ErrorKind::InvalidInput.into()),
    };
    let (schedule, _) = Schedule::seeded(71);
    schedule.cut_at(0, DurableFault::Interrupted);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let result = runtime.block_on(schedule.scope(async {
        let result = invoke();
        if reissue
            && result
                .as_ref()
                .is_err_and(|error| error.kind() == io::ErrorKind::Interrupted)
        {
            // Restore the former policy through the same physical namespace
            // operation and original controller. This extra call must fail the
            // first-result oracle without depending on a hang or a deadline.
            invoke()
        } else {
            result
        }
    }));
    let trace = schedule.durable_writes();
    let is_rename = matches!(operation, Operation::Rename);
    let first_admissions = u64::from(metered) * (1 + u64::from(cfg!(windows) && is_rename));
    let refused = result.as_ref().err().is_some_and(|error| {
        error.kind() == io::ErrorKind::Interrupted
            && error
                .get_ref()
                .and_then(|error| error.downcast_ref::<InjectedStorageFault>())
                .is_some_and(|cause| cause.ordinal == 0 && cause.fault == DurableFault::Interrupted)
    }) && trace.len() == 1
        && trace.first().is_some_and(|event| {
            event.transferred.is_none() && event.failure == Some(io::ErrorKind::Interrupted)
        })
        && observer.used_operations.load(Ordering::Acquire) == first_admissions
        && renamed.get() == 0
        && from.exists()
        && !to.exists()
        && std::fs::read(&from).unwrap() == b"original"
        && schedule.storage_fault().is_none();
    if !refused {
        return false;
    }
    // Only a later explicit invocation may perform the publication. It retains
    // the original parent flush and exact successful rename callback.
    runtime
        .block_on(schedule.scope(async { invoke() }))
        .unwrap();
    assert_eq!(std::fs::read(&to).unwrap(), b"original");
    assert_eq!(from.exists(), matches!(operation, Operation::Publish));
    assert_eq!(renamed.get(), usize::from(is_rename));
    assert_eq!(
        observer.used_operations.load(Ordering::Acquire),
        u64::from(metered) * if is_rename { 4 } else { 2 },
    );
    let trace = schedule.durable_writes();
    assert_eq!(trace.len(), if cfg!(unix) && is_rename { 3 } else { 2 });
    assert!(
        trace
            .get(1)
            .is_some_and(|call| call.transferred == Some(0) && call.failure.is_none())
    );
    if cfg!(unix) && is_rename {
        let flush = trace.get(2).unwrap();
        assert_eq!(
            flush.operation,
            if metered {
                "file:sync"
            } else {
                "file:sync_parent"
            }
        );
        assert_eq!(flush.transferred, Some(0));
        assert!(flush.failure.is_none());
    }
    true
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
fn namespace_calls_return_one_interruption_without_losing_custody_or_parent_durability() {
    for operation in [Operation::Publish, Operation::Rename] {
        for metered in [false, true] {
            assert!(namespace_interruption(operation, metered, false));
            assert!(!namespace_interruption(operation, metered, true));
        }
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn native_calls_keep_physical_effects_and_causes(
        bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 2..32),
        prefix in 0usize..32,
    ) {
        // Reuse only the empty parent namespace. Every cut still starts with
        // newly created original bytes, its own held handle and fresh schedule.
        let directory = tempfile::tempdir().unwrap();
        for operation in [Operation::Write, Operation::UnmeteredWrite, Operation::Resize, Operation::Publish, Operation::Rename] {
            for fault in [DurableFault::Before, DurableFault::After, DurableFault::Failed,
                DurableFault::DiskFull, DurableFault::Torn(prefix)] {
                proptest::prop_assert!(retains_cut(directory.path(), operation, fault, &bytes, false), "{operation:?} lost {fault:?}");
                // Erasing the real scoped boundary must fail the same oracle.
                proptest::prop_assert!(!retains_cut(directory.path(), operation, fault, &bytes, true), "erased {operation:?} escaped the control");
            }
        }
    }
}
