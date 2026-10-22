//! Held native handles and namespace calls use the original fault controller.
use super::*;
use crate::{NativeReadMeter, RenameMode};

#[derive(Clone, Copy, Debug)]
enum Operation {
    Write,
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
            Err(error) => panic!("reset cut fixture: {error}"),
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
                Operation::Resize => crate::set_len(&file, 3, meter.as_ref()),
                Operation::Publish => crate::publish_new(&from, &to, Some(&meter)),
                Operation::Rename => {
                    crate::durable_rename(&from, &to, RenameMode::NoReplace, Some(&meter))
                }
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
        Operation::Write => "file:write",
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
        Operation::Write => {
            let transferred = match fault {
                DurableFault::After => bytes.len(),
                DurableFault::Torn(count) => count.min(bytes.len() - 1),
                _ => 0,
            };
            let mut expected = b"original".to_vec();
            expected.resize(expected.len().max(transferred), 0);
            expected[..transferred].copy_from_slice(&bytes[..transferred]);
            assert_eq!(std::fs::read(&from).unwrap(), expected);
            assert_eq!(
                schedule.durable_writes()[0].transferred,
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
        }
    }
    assert_eq!(
        schedule.durable_writes().len(),
        1,
        "operation dispatched after the cut"
    );
    true
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn metered_native_calls_keep_physical_effects_and_causes(
        bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 2..32),
        prefix in 0usize..32,
    ) {
        // Reuse only the empty parent namespace. Every cut still starts with
        // newly created original bytes, its own held handle and fresh schedule.
        let directory = tempfile::tempdir().unwrap();
        for operation in [Operation::Write, Operation::Resize, Operation::Publish, Operation::Rename] {
            for fault in [DurableFault::Before, DurableFault::After, DurableFault::Failed,
                DurableFault::DiskFull, DurableFault::Torn(prefix)] {
                proptest::prop_assert!(retains_cut(directory.path(), operation, fault, &bytes, false), "{operation:?} lost {fault:?}");
                // Erasing the real scoped boundary must fail the same oracle.
                proptest::prop_assert!(!retains_cut(directory.path(), operation, fault, &bytes, true), "erased {operation:?} escaped the control");
            }
        }
    }
}
