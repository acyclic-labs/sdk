use super::*;
use crate::kernel::AsyncBlobSource as _;

fn budget() -> WorkBudget {
    WorkCounters {
        backend_read_operations: 16,
        source_bytes_read: 32,
        allocation_operations: 128,
        peak_allocation_bytes: 1024 * 1024,
        items_examined: 32,
        bytes_copied: 32,
        ..WorkCounters::UNCHARGED
    }
}

fn fixture_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        options.custom_flags(windows::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED.0);
    }
    options.open(path)
}

#[cfg(unix)]
#[test]
#[allow(
    unsafe_code,
    reason = "advisory locks observe the real imported descriptor's physical retirement"
)]
fn cancelled_native_import_retires_std_file_before_preserving_original_failure()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::fd::AsRawFd as _;
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("cancelled-import");
    std::fs::write(&path, b"actual")?;
    let file = fixture_file(&path)?;
    let contender = fixture_file(&path)?;
    assert_eq!(
        // SAFETY: file owns a live descriptor and flock takes no pointers.
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    assert_ne!(
        // SAFETY: contender owns a live descriptor and flock takes no pointers.
        unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let mut scope = CaptureWorkScope::new(&original)?;
    cancellation.cancel();
    assert!(matches!(
        native_positional_file(file, &scope.native),
        Err(CaptureError::Kernel(error)) if matches!(*error, crate::FsError::Cancelled(_))
    ));
    assert_eq!(
        // SAFETY: contender remains open after the imported file is retired.
        unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    scope.release()?;
    Ok(())
}

#[tokio::test]
async fn captured_source_stale_and_drop_retire_only_terminal_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("captured-stale");
    std::fs::write(&path, b"abcdefgh")?;
    let cancellation = CancellationToken::new();
    let setup = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut scope = CaptureWorkScope::new(&setup)?;
    let native = std::sync::Arc::new(native_positional_file(fixture_file(&path)?, &scope.native)?);
    let original = setup.native_volume_work(budget())?;
    let reader =
        acyclic_native_runtime::AsyncRangeReader::new(std::sync::Arc::clone(&native), 0, 8);
    let mut source = reader;
    std::fs::write(&path, b"abc")?;
    let mut destination = [0xa5; 8];
    assert!(matches!(
        source
            .read_with_native_work(&mut destination, &original, &cancellation,)
            .await,
        Err(crate::FsError::SourceSnapshotStale)
    ));
    assert_eq!(destination, [0xa5; 8]);
    drop(source);
    scope.release()?;
    drop(native);
    Ok(())
}

#[tokio::test]
async fn original_native_byte_and_copy_refusals_precede_io()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("original-denied");
    std::fs::write(&path, b"abc")?;
    for counter in ["source_bytes_read", "bytes_copied"] {
        let cancellation = CancellationToken::new();
        let mut limit = budget();
        if counter == "source_bytes_read" {
            limit.source_bytes_read = 2;
        } else {
            limit.bytes_copied = 2;
        }
        let setup = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let scope = CaptureWorkScope::new(&setup)?;
        let native =
            std::sync::Arc::new(native_positional_file(fixture_file(&path)?, &scope.native)?);
        let original = setup.native_volume_work(limit)?;
        let mut source = acyclic_native_runtime::AsyncRangeReader::new(native, 0, 3);
        let mut destination = [0xa5; 8];
        let error = source
            .read_with_native_work(&mut destination, &original, &cancellation)
            .await
            .expect_err("a denied full range must not become truncated success");
        assert!(matches!(error,
                crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
                    crate::WorkError::BudgetExceeded { counter: refused, .. }
                )) if refused == counter));
        assert_eq!(destination, [0xa5; 8]);
        assert_eq!(source.remaining(), 3);
    }
    Ok(())
}

#[test]
fn owned_native_ranges_preserve_bytes_bounds_custody_and_failure_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    use proptest::prelude::*;
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let temporary = tempfile::tempdir()?;
    let file = temporary.path().join("owned-range");
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 128,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // Property: successful owned reads retain exactly the bounded source prefix
    // on the original ledger, perform no initialization copy, and retire every claim.
    // Short files fail without advancing the cursor or losing physical charges.
    runner.run(
        &(
            proptest::collection::vec(any::<u8>(), 0..96),
            0_usize..128,
            0_usize..128,
            0_usize..128,
        ),
        |(body, offset, length, maximum)| {
            std::fs::write(&file, &body).unwrap();
            let cancellation = CancellationToken::new();
            let setup =
                NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
            let native = native_positional_file(fixture_file(&file).unwrap(), &setup).unwrap();
            let requested = length.min(maximum);
            let allowance = WorkBudget {
                source_bytes_read: requested as u64,
                bytes_copied: 0,
                ..WorkBudget::UNBOUNDED
            };
            let original = NativeMaterializationWork::new(allowance, &cancellation).unwrap();
            let initial = original.live_allocation_bytes();
            let mut source = acyclic_native_runtime::AsyncRangeReader::new(
                &native,
                offset as u64,
                length as u64,
            );
            let expected = body.len().saturating_sub(offset).min(requested);
            let result = runtime.block_on(source.read_owned_with_native_work(
                maximum,
                &original,
                &cancellation,
            ));
            owned_cursor_matches_outcome(&result, source.remaining(), length as u64)?;
            if expected != requested {
                prop_assert!(matches!(result, Err(crate::FsError::SourceSnapshotStale)));
            } else {
                let chunk = result.unwrap().unwrap();
                prop_assert_eq!(
                    chunk.bytes.as_ref(),
                    body.get(offset..offset + expected).unwrap_or_default()
                );
                prop_assert!(chunk.logical_owned_bytes >= requested as u64);
                if requested != 0 {
                    prop_assert!(chunk.bytes.matches_meter(&original.runtime_meter()));
                }
                prop_assert_eq!(original.receipt().bytes_copied, 0);
                drop(chunk);
            }
            prop_assert_eq!(original.receipt().source_bytes_read, expected as u64);
            prop_assert!(original.receipt().verify(allowance).is_ok());
            prop_assert_eq!(original.live_allocation_bytes(), initial);
            Ok(())
        },
    )?;
    Ok(())
}

fn owned_cursor_matches_outcome(
    result: &Result<Option<crate::kernel::OwnedSourceChunk>, crate::FsError>,
    remaining: u64,
    before: u64,
) -> proptest::test_runner::TestCaseResult {
    use proptest::prelude::*;
    let consumed = result
        .as_ref()
        .ok()
        .and_then(Option::as_ref)
        .map_or(0, |chunk| chunk.bytes.len() as u64);
    prop_assert_eq!(remaining.checked_add(consumed), Some(before));
    Ok(())
}

#[test]
fn owned_native_worker_completes_then_cancellation_preserves_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    use proptest::prelude::*;
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let temporary = tempfile::tempdir()?;
    let file = temporary.path().join("late-cancel-range");
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 128,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // Property: failure after successful physical completion consumes no range,
    // retires storage, preserves actual charges, and permits a retry of the prefix.
    // Accepting the raw read before final validation is the broken variant.
    runner.run(
        &(proptest::collection::vec(any::<u8>(), 1..96), 1_usize..128),
        |(body, wanted)| {
            std::fs::write(&file, &body).unwrap();
            let maximum = wanted.min(body.len());
            for advance_early in [false, true] {
                let setup_cancel = CancellationToken::new();
                let setup =
                    NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &setup_cancel).unwrap();
                let native = std::sync::Arc::new(
                    native_positional_file(fixture_file(&file).unwrap(), &setup).unwrap(),
                );
                let cancellation = CancellationToken::new();
                let original =
                    NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
                let initial = original.live_allocation_bytes();
                let mut source =
                    acyclic_native_runtime::AsyncRangeReader::new(native, 0, body.len() as u64);
                let storage = native_source_storage(maximum, &original, &cancellation).unwrap();
                let finish = |read: std::io::Result<_>| {
                    assert!(read.is_ok(), "the worker must complete before cancellation");
                    assert_eq!(original.receipt().source_bytes_read, maximum as u64);
                    cancellation.cancel();
                    accept_native_source_chunk(read, maximum, &original, &cancellation)
                };
                let result = if advance_early {
                    let read = runtime.block_on(source.read_spare(
                        storage,
                        maximum,
                        original.runtime_meter(),
                        true,
                        std::convert::identity,
                    ));
                    finish(read)
                } else {
                    runtime.block_on(source.read_spare(
                        storage,
                        maximum,
                        original.runtime_meter(),
                        true,
                        finish,
                    ))
                };
                prop_assert!(matches!(result, Err(crate::FsError::Cancelled(_))));
                let checked =
                    owned_cursor_matches_outcome(&result, source.remaining(), body.len() as u64);
                if advance_early {
                    prop_assert!(
                        checked.is_err(),
                        "the property must reject early advancement"
                    );
                } else {
                    checked?;
                    let retry_cancel = CancellationToken::new();
                    let retry =
                        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &retry_cancel)
                            .unwrap();
                    let chunk = runtime
                        .block_on(source.read_owned_with_native_work(
                            maximum,
                            &retry,
                            &retry_cancel,
                        ))
                        .unwrap()
                        .unwrap();
                    prop_assert_eq!(chunk.bytes.as_ref(), &body[..maximum]);
                    drop(chunk);
                }
                prop_assert_eq!(original.receipt().source_bytes_read, maximum as u64);
                prop_assert_eq!(original.receipt().bytes_copied, 0);
                prop_assert_eq!(original.live_allocation_bytes(), initial);
            }
            Ok(())
        },
    )?;
    Ok(())
}

#[tokio::test]
async fn owned_native_refusals_and_cancellation_precede_reads()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("owned-denied");
    std::fs::write(&path, b"abc")?;
    for counter in [
        "source_bytes_read",
        "bytes_copied",
        "publication_header",
        "cancelled",
    ] {
        let cancellation = CancellationToken::new();
        let setup = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let native = std::sync::Arc::new(native_positional_file(fixture_file(&path)?, &setup)?);
        let mut limit = budget();
        match counter {
            "source_bytes_read" => limit.source_bytes_read = 2,
            "bytes_copied" => limit.bytes_copied = 0,
            "publication_header" => {
                limit.peak_allocation_bytes =
                    NativeMaterializationWork::accounting_state_bytes() + 3;
            }
            _ => {}
        }
        let original =
            NativeMaterializationWork::new(limit, &cancellation).map_err(|(error, _)| error)?;
        let initial = original.live_allocation_bytes();
        if counter == "cancelled" {
            cancellation.cancel();
        }
        let mut source = acyclic_native_runtime::AsyncRangeReader::new(native, 0, 3);
        let result = source
            .read_owned_with_native_work(8, &original, &cancellation)
            .await;
        if counter == "bytes_copied" {
            let chunk = result?.ok_or("owned source must return its chunk")?;
            assert_eq!(chunk.bytes.as_ref(), b"abc");
            assert_eq!(source.remaining(), 0);
            assert_eq!(original.receipt().source_bytes_read, 3);
            assert_eq!(original.receipt().bytes_copied, 0);
            drop(chunk);
            assert_eq!(original.live_allocation_bytes(), initial);
            continue;
        }
        if counter == "cancelled" {
            assert!(matches!(result, Err(crate::FsError::Cancelled(_))));
        } else {
            let expected = if counter == "publication_header" {
                "peak_allocation_bytes"
            } else {
                counter
            };
            assert!(matches!(result, Err(crate::FsError::NativeKernel(
                crate::NativeTreeMaterializationError::Work(WorkError::BudgetExceeded { counter: refused, .. })
            )) if refused == expected));
        }
        assert_eq!(source.remaining(), 3);
        assert_eq!(original.receipt().source_bytes_read, 0);
        assert_eq!(original.receipt().backend_read_operations, 0);
        assert_eq!(original.live_allocation_bytes(), initial);
    }
    Ok(())
}
