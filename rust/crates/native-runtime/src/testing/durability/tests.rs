use super::*;
use std::io::Write;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

fn write_pair(schedule: &Schedule) -> (Vec<u8>, Vec<io::Result<()>>) {
    let mut file = tempfile::tempfile().unwrap();
    let results = runtime().block_on(schedule.scope(async {
        [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .map(|bytes| crate::write_file_all(&mut file, bytes))
            .collect()
    }));
    use std::io::{Read, Seek};
    file.rewind().unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    (bytes, results)
}

#[test]
fn every_write_cut_preserves_its_exact_prefix_and_cause() {
    let (schedule, _) = Schedule::seeded(71);
    let (bytes, results) = write_pair(&schedule);
    assert_eq!(bytes, b"firstsecond");
    assert!(results.into_iter().all(|result| result.is_ok()));
    let count = schedule.durable_writes().len();
    assert_eq!(count, 2, "an original file write escaped enumeration");
    for cut in 0..count {
        for fault in [
            DurableFault::Before,
            DurableFault::After,
            DurableFault::Failed,
            DurableFault::DiskFull,
            DurableFault::Torn(0),
            DurableFault::Torn(1),
            DurableFault::Torn(99),
        ] {
            let (schedule, _) = Schedule::seeded(71);
            schedule.cut_at(cut, fault);
            let (actual, results) = write_pair(&schedule);
            let prefix = if cut == 0 {
                b"".as_slice()
            } else {
                b"first".as_slice()
            };
            let offered = if cut == 0 {
                b"first".as_slice()
            } else {
                b"second".as_slice()
            };
            let kept = match fault {
                DurableFault::After => offered.len(),
                DurableFault::Torn(count) => count.min(offered.len() - 1),
                _ => 0,
            };
            assert_eq!(actual, [prefix, &offered[..kept]].concat());
            for (index, result) in results.into_iter().enumerate() {
                if index < cut {
                    assert!(result.is_ok());
                    continue;
                }
                let error = result.unwrap_err();
                let cause = error
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<InjectedStorageFault>()
                    .unwrap();
                assert_eq!((cause.ordinal, cause.fault), (cut, fault));
                if fault == DurableFault::DiskFull {
                    assert_eq!(error.kind(), io::ErrorKind::StorageFull);
                }
            }
            assert_eq!(
                schedule.durable_writes().len(),
                cut + 1,
                "write dispatched after a crash"
            );
        }
    }
}

#[test]
fn physical_write_accounting_precedes_acknowledgement_loss() {
    for fault in [
        DurableFault::Before,
        DurableFault::After,
        DurableFault::Torn(2),
        DurableFault::Failed,
        DurableFault::DiskFull,
    ] {
        let (schedule, _) = Schedule::seeded(71);
        schedule.cut_at(0, fault);
        let mut file = tempfile::tempfile().unwrap();
        let mut operations = 0;
        let mut transferred = 0;
        let result = runtime().block_on(schedule.scope(async {
            crate::write_file_with(b"retained", |bytes| {
                let result = file.write(bytes);
                operations += 1;
                transferred += result.as_ref().copied().unwrap_or(0);
                result
            })
        }));
        let expected = match fault {
            DurableFault::After => 8,
            DurableFault::Torn(count) => count,
            _ => 0,
        };
        assert_eq!(transferred, expected);
        assert_eq!(operations, usize::from(expected != 0));
        assert_eq!(file.metadata().unwrap().len(), transferred as u64);
        assert_eq!(
            schedule.durable_writes()[0].transferred,
            (operations != 0).then_some(transferred)
        );
        assert_eq!(
            schedule.storage_fault().unwrap().fault,
            fault,
            "crash cause disappeared before reopen"
        );
        assert_eq!(result.is_ok(), matches!(fault, DurableFault::Torn(_)));
    }
}

// The public parent flush must own exactly one boundary, including when reached
// by namespace publication. An outer After cut must reach the physical flush.
#[cfg(target_os = "linux")]
fn parent_flush_cut(fault: Option<DurableFault>, nested: bool) {
    let directory = tempfile::tempdir().unwrap();
    let (schedule, _) = Schedule::seeded(71);
    if let Some(fault) = fault {
        schedule.cut_at(0, fault);
    }
    let result = runtime().block_on(schedule.scope(async {
        let flush = || crate::sync_parent(directory.path(), crate::Durability::Full);
        if nested {
            durable_operation("file:sync_parent", flush)
        } else {
            flush()
        }
    }));
    let trace = schedule.durable_writes();
    assert_eq!(trace.len(), 1, "nested parent flush boundary");
    assert_eq!(trace[0].operation, "file:sync_parent");
    let completed = fault.is_none() || fault == Some(DurableFault::After);
    assert_eq!(
        trace[0].transferred,
        completed.then_some(0),
        "parent flush was skipped"
    );
    assert_eq!(result.is_ok(), fault.is_none());
    if let Some(fault) = fault {
        let error = result.unwrap_err();
        let cause = error
            .get_ref()
            .unwrap()
            .downcast_ref::<InjectedStorageFault>()
            .unwrap();
        assert_eq!(
            (cause.ordinal, cause.operation, cause.fault),
            (0, "file:sync_parent", fault)
        );
        // The lost acknowledgement latches the crash for every later entry.
        runtime().block_on(schedule.scope(async {
            assert!(crate::sync_parent(directory.path(), crate::Durability::Full).is_err());
        }));
        assert_eq!(schedule.durable_writes(), trace);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn parent_flush_has_one_boundary_and_keeps_after_cut_physical_completion() {
    for fault in [
        None,
        Some(DurableFault::Before),
        Some(DurableFault::After),
        Some(DurableFault::Failed),
        Some(DurableFault::DiskFull),
        Some(DurableFault::Torn(0)),
    ] {
        parent_flush_cut(fault, false);
    }
}

// Restoring the doubly hooked parent flush is rejected before any cut latches.
#[cfg(target_os = "linux")]
#[test]
#[should_panic(expected = "nested durable boundary: file:sync_parent inside file:sync_parent")]
fn nested_parent_flush_negative_control() {
    parent_flush_cut(Some(DurableFault::After), true);
}

// The same structural rejection covers data writes, uncut and on every thread.
#[test]
#[should_panic(expected = "nested durable boundary: file:write inside file:write")]
fn nested_write_boundary_is_rejected() {
    let mut file = tempfile::tempfile().unwrap();
    let (schedule, _) = Schedule::seeded(71);
    runtime()
        .block_on(schedule.scope(async {
            crate::write_file_with(b"outer", |bytes| crate::write_file(&mut file, bytes))
        }))
        .unwrap();
}

fn native_control_fault(fault: DurableFault, forget_context: bool) {
    let mut original = tempfile::tempfile().unwrap();
    original.write_all(b"retained").unwrap();
    let file = crate::NativeFile::from_file(original, None)
        .unwrap_or_else(|(error, _)| panic!("native file: {error}"));
    let (schedule, mut events) = Schedule::seeded(71);
    schedule.cut_at(0, fault);
    let result = runtime().block_on(async {
        let mut operation = Box::pin(schedule.scope(async {
            if forget_context {
                FORGET_HOST_CONTEXT.with(|fault| fault.set(true));
            }
            file.set_len_async(3, None).unwrap().await
        }));
        let result = loop {
            tokio::select! {
                biased;
                boundary = events.recv() => boundary.unwrap().release(),
                result = &mut operation => break result,
            }
        };
        drop(operation);
        schedule.drained().await;
        result
    });
    assert!(result.is_err(), "native control lost the injected fault");
    let error = result.unwrap_err();
    let cause = error
        .get_ref()
        .unwrap()
        .downcast_ref::<InjectedStorageFault>()
        .unwrap();
    assert_eq!(cause.ordinal, 0);
    assert_eq!(cause.operation, "file:truncate");
    assert_eq!(cause.fault, fault);
    assert_eq!(
        file.as_file().metadata().unwrap().len(),
        if fault == DurableFault::After { 3 } else { 8 }
    );
    assert_eq!(schedule.durable_writes().len(), 1);
}

#[test]
fn native_file_controls_retain_the_scoped_fault_and_original_file_effect() {
    for fault in [
        DurableFault::Before,
        DurableFault::After,
        DurableFault::Torn(1),
        DurableFault::Failed,
        DurableFault::DiskFull,
    ] {
        native_control_fault(fault, false);
    }
}

#[test]
#[should_panic(expected = "native control lost the injected fault")]
fn negative_control_losing_native_file_context_escapes_the_fault() {
    native_control_fault(DurableFault::Before, true);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn original_short_writes_are_preserved(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..64), width in 1usize..16) {
        struct Short { bytes: Vec<u8>, width: usize }
        impl Write for Short {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                let count = bytes.len().min(self.width);
                self.bytes.extend_from_slice(&bytes[..count]);
                Ok(count)
            }
            fn flush(&mut self) -> io::Result<()> { Ok(()) }
        }
        let (schedule, _) = Schedule::seeded(71);
        let mut writer = Short { bytes: Vec::new(), width };
        runtime().block_on(schedule.scope(async { crate::write_file_all(&mut writer, &bytes) })).unwrap();
        proptest::prop_assert_eq!(&writer.bytes, &bytes);
        let trace = schedule.durable_writes();
        proptest::prop_assert_eq!(trace.len(), bytes.len().div_ceil(width));
        proptest::prop_assert_eq!(trace.iter().map(|call| call.transferred.unwrap()).sum::<usize>(), bytes.len());
    }
}

#[test]
#[should_panic(expected = "acknowledged bytes were lost")]
fn negative_control_skipping_the_original_file_write_loses_acknowledged_bytes() {
    let (schedule, _) = Schedule::seeded(71);
    schedule.0.state().writes.skip_write = true;
    let (bytes, results) = write_pair(&schedule);
    assert!(results.into_iter().all(|result| result.is_ok()));
    assert_eq!(bytes, b"firstsecond", "acknowledged bytes were lost");
}
