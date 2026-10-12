use crate::{
    CancellationToken, IdempotencyKey, NativeMaterializationWork, WorkBudget, WorkCounters,
};
use std::fs;

fn entries() -> Result<(tempfile::TempDir, PathBuf, PathBuf), NativeExchangeError> {
    let temporary = tempfile::tempdir()?;
    let live = temporary.path().join("live");
    let prepared = temporary.path().join("prepared");
    fs::write(&live, b"before")?;
    fs::write(&prepared, b"after")?;
    Ok((temporary, live, prepared))
}

fn trees() -> Result<(tempfile::TempDir, PathBuf, PathBuf), NativeExchangeError> {
    let temporary = tempfile::tempdir()?;
    let live = temporary.path().join("live");
    let prepared = temporary.path().join("prepared");
    fs::create_dir(&live)?;
    fs::create_dir(&prepared)?;
    Ok((temporary, live, prepared))
}

fn fixture_journal(
    live: &Path,
    prepared: &Path,
    operation: IdempotencyKey,
    carried: Vec<PathBuf>,
    recovery: Vec<u8>,
    phase: NativeExchangePhase,
) -> NativeExchangeJournal {
    NativeExchangeJournal {
        version: NATIVE_EXCHANGE_JOURNAL_VERSION,
        operation,
        carried,
        recovery,
        live: live.to_path_buf(),
        prepared: prepared.to_path_buf(),
        roots: [
            root_identity(live).expect("live identity"),
            root_identity(prepared).expect("prepared identity"),
        ],
        phase,
    }
}

fn run(
    budget: WorkBudget,
    cancellation: &CancellationToken,
    operation: impl FnOnce(&NativeMaterializationWork) -> Result<(), NativeExchangeError>,
) -> Result<WorkCounters, (NativeExchangeError, WorkCounters)> {
    let work = NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(error, work)| (error.into(), work))?;
    let result = operation(&work);
    result
        .map(|()| work.receipt())
        .map_err(|error| (error, work.receipt()))
}

use super::*;

fn exchange_paths(
    left: &Path,
    right: &Path,
    work: &NativeMaterializationWork,
) -> Result<(), NativeExchangeError> {
    if left == right {
        return Err(NativeExchangeError::InvalidLayout);
    }
    let mut work = work;
    let left_parent =
        crate::native_host::open_root_directory(left.parent().expect("fixture path"), &mut work)
            .map(|(directory, _)| directory)?;
    let right_parent =
        crate::native_host::open_root_directory(right.parent().expect("fixture path"), &mut work)
            .map(|(directory, _)| directory)?;
    exchange_native_entries(
        &left_parent,
        Path::new(left.file_name().expect("fixture path")),
        &right_parent,
        Path::new(right.file_name().expect("fixture path")),
        &mut work,
    )
    .map_err(Into::into)
}

fn recover_path(live: &Path, work: &NativeMaterializationWork) -> Result<(), NativeExchangeError> {
    let mut work = work;
    let parent =
        crate::native_host::open_root_directory(live.parent().expect("fixture path"), &mut work)
            .map(|(directory, _)| directory)?;
    recover_native_entry_exchange(
        &parent,
        Path::new(live.file_name().expect("fixture path")),
        &mut work,
    )
    .map_err(Into::into)
}

fn finite_entry_budget() -> WorkBudget {
    WorkCounters {
        backend_read_operations: 20_000,
        backend_write_operations: 20_000,
        durability_operations: 100,
        bytes_copied: 1_000_000,
        bytes_encoded: 1_000_000,
        source_bytes_read: 1_000_000,
        source_path_components: 1_000,
        output_bytes: 1_000_000,
        items_examined: 20_000,
        allocation_operations: 20_000,
        peak_allocation_bytes: 1_000_000,
        ..WorkCounters::default()
    }
}

#[cfg(windows)]
fn entry_witness_fixture(live: &Path, prepared: &Path, witness: &Path) {
    let cancellation = CancellationToken::new();
    let ledger = NativeMaterializationWork::new(finite_entry_budget(), &cancellation)
        .expect("original native meter");
    let mut work = &ledger;
    let parent = crate::native_host::open_root_directory(live.parent().expect("parent"), &mut work)
        .map(|(directory, _)| directory)
        .expect("held fixture parent");
    let prepared_parent = crate::native_host::open_root_directory(
        prepared.parent().expect("prepared parent"),
        &mut work,
    )
    .map(|(directory, _)| directory)
    .expect("held prepared parent");
    let live_name = Path::new(live.file_name().expect("live name"));
    let prepared_name = Path::new(prepared.file_name().expect("prepared name"));
    let roots = [
        crate::native_host::entry_identity(&parent, live_name, &mut work)
            .expect("original identity")
            .expect("original"),
        crate::native_host::entry_identity(&prepared_parent, prepared_name, &mut work)
            .expect("prepared identity")
            .expect("prepared"),
    ];
    create_entry_witness(
        &parent,
        live_name,
        &prepared_parent,
        prepared_name,
        Path::new(witness.file_name().expect("witness name")),
        roots,
        &mut work,
    )
    .expect("durable original witness");
}

#[test]
fn bounded_entry_exchange_rejects_zero_and_cancelled_work_before_io()
-> Result<(), NativeExchangeError> {
    let (temporary, live, prepared) = entries()?;
    let cancellation = CancellationToken::new();
    let failure = run(WorkBudget::default(), &cancellation, |work| {
        exchange_paths(&live, &prepared, work)
    })
    .expect_err("zero allowance");
    assert_eq!(failure.1, WorkCounters::default());
    cancellation.cancel();
    let failure = run(finite_entry_budget(), &cancellation, |work| {
        exchange_paths(&live, &prepared, work)
    })
    .expect_err("cancelled");
    assert!(matches!(failure.0, NativeExchangeError::Cancelled));
    assert_eq!(failure.1, WorkCounters::default());
    assert_eq!(fs::read(&live)?, b"before");
    assert_eq!(fs::read(&prepared)?, b"after");
    Ok(())
}

#[cfg(windows)]
#[test]
fn bounded_entry_journal_failure_cleans_only_its_provisional_handle()
-> Result<(), NativeExchangeError> {
    for deny_bytes in [true, false] {
        let (temporary, live, prepared) = entries()?;
        let witness = entry_exchange_witness(&live)?;
        let scratch = scratch(&live)?;
        let mut budget = finite_entry_budget();
        if deny_bytes {
            budget.output_bytes = (ENTRY_WITNESS.len() - 1) as u64;
        } else {
            budget.durability_operations = 0;
        }
        let cancellation = CancellationToken::new();
        let failure = run(budget, &cancellation, |work| {
            exchange_paths(&live, &prepared, work)
        })
        .expect_err("journal admission denied before physical edits");
        assert!(matches!(failure.0, NativeExchangeError::Work(_)));
        assert!(failure.1.backend_write_operations > 0);
        assert_eq!(failure.1.durability_operations, 0);
        if deny_bytes {
            assert_eq!(failure.1.output_bytes, 0);
        } else {
            assert!(failure.1.output_bytes > 0);
        }
        assert!(!witness.exists());
        assert!(!scratch.exists());
        assert_eq!(fs::read(&live)?, b"before");
        assert_eq!(fs::read(&prepared)?, b"after");
        run(finite_entry_budget(), &cancellation, |work| {
            exchange_paths(&live, &prepared, work)
        })
        .expect("the real durable request remains retryable");
        assert_eq!(fs::read(&live)?, b"after");
        assert_eq!(fs::read(&prepared)?, b"before");
    }
    Ok(())
}

#[cfg(all(feature = "native-watch", unix))]
#[test]
fn bounded_entry_exchange_failure_retains_the_physical_edit_prefix()
-> Result<(), NativeExchangeError> {
    let (temporary, live, prepared) = entries()?;
    let mut budget = finite_entry_budget();
    budget.durability_operations = 0;
    let failure = run(budget, &CancellationToken::new(), |work| {
        exchange_paths(&live, &prepared, work)
    })
    .expect_err("durability denied");
    assert!(matches!(failure.0, NativeExchangeError::Work(_)));
    assert!(failure.1.backend_write_operations > 0);
    assert_eq!(failure.1.durability_operations, 0);
    assert_eq!(fs::read(&live)?, b"after");
    assert_eq!(fs::read(&prepared)?, b"before");
    Ok(())
}

#[cfg(all(feature = "native-watch", unix))]
#[test]
fn bounded_exchange_keeps_original_parents_when_the_lexical_root_is_replaced()
-> Result<(), NativeExchangeError> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("root");
    let saved = temporary.path().join("saved");
    fs::create_dir(&root)?;
    fs::write(root.join("live"), b"before")?;
    fs::write(root.join("prepared"), b"after")?;
    let ledger = NativeMaterializationWork::new(finite_entry_budget(), &CancellationToken::new())
        .expect("original native meter");
    let mut work = &ledger;
    let parent = crate::native_host::open_root_directory(&root, &mut work)
        .map(|(directory, _)| directory)
        .expect("original held parent");
    fs::rename(&root, &saved)?;
    fs::create_dir(&root)?;
    fs::write(root.join("live"), b"user-live")?;
    fs::write(root.join("prepared"), b"user-prepared")?;
    exchange_native_entries(
        &parent,
        Path::new("live"),
        &parent,
        Path::new("prepared"),
        &mut work,
    )
    .expect("held physical exchange");
    assert_eq!(fs::read(saved.join("live"))?, b"after");
    assert_eq!(fs::read(saved.join("prepared"))?, b"before");
    assert_eq!(fs::read(root.join("live"))?, b"user-live");
    assert_eq!(fs::read(root.join("prepared"))?, b"user-prepared");
    Ok(())
}

#[cfg(windows)]
#[test]
fn bounded_entry_retry_rejects_a_different_request_without_adopting_it()
-> Result<(), NativeExchangeError> {
    let (temporary, live, prepared) = entries()?;
    let other = temporary.path().join("other");
    let witness = entry_exchange_witness(&live)?;
    fs::write(&other, b"unrelated request")?;
    entry_witness_fixture(&live, &prepared, &witness);
    let cancellation = CancellationToken::new();
    let failure = run(finite_entry_budget(), &cancellation, |work| {
        exchange_paths(&live, &other, work)
    })
    .expect_err("original request path identity remains authoritative");
    assert!(matches!(
        failure.0,
        NativeExchangeError::IncompatibleJournal
    ));
    assert_eq!(failure.1.backend_write_operations, 0);
    assert!(witness.exists());
    assert_eq!(fs::read(&live)?, b"before");
    assert_eq!(fs::read(&prepared)?, b"after");
    assert_eq!(fs::read(&other)?, b"unrelated request");
    run(finite_entry_budget(), &cancellation, |work| {
        exchange_paths(&live, &prepared, work)
    })
    .expect("original rolled-back request proceeds to actual publication");
    assert_eq!(fs::read(&live)?, b"after");
    Ok(())
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_finishes_in_the_original_other_parent() -> Result<(), NativeExchangeError>
{
    let temporary = tempfile::tempdir()?;
    let live_parent = temporary.path().join("checkout");
    let prepared_parent = temporary.path().join("operation");
    fs::create_dir(&live_parent)?;
    fs::create_dir(&prepared_parent)?;
    let live = live_parent.join("live");
    let prepared = prepared_parent.join("prepared");
    let witness = entry_exchange_witness(&live)?;
    let scratch = scratch(&live)?;
    fs::write(&live, b"before")?;
    fs::write(&prepared, b"after")?;
    entry_witness_fixture(&live, &prepared, &witness);
    fs::rename(&live, &scratch)?;
    fs::rename(&prepared, &live)?;
    run(finite_entry_budget(), &CancellationToken::new(), |work| {
        recover_path(&live, work)
    })
    .expect("held original other-parent recovery");
    assert_eq!(fs::read(&live)?, b"after");
    assert_eq!(fs::read(&prepared)?, b"before");
    assert!(!scratch.exists());
    assert!(!witness.exists());
    Ok(())
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_keeps_the_same_lost_ack_decision() -> Result<(), NativeExchangeError> {
    let (temporary, live, prepared) = entries()?;
    let witness = entry_exchange_witness(&live)?;
    let scratch = scratch(&live)?;
    entry_witness_fixture(&live, &prepared, &witness);
    fs::rename(&live, &scratch)?;
    fs::rename(&prepared, &live)?;
    fs::rename(&scratch, &prepared)?;
    let cancellation = CancellationToken::new();
    let recovered = run(finite_entry_budget(), &cancellation, |work| {
        recover_path(&live, work)
    })
    .expect("restart recovers original decision");
    assert!(recovered.backend_write_operations > 0);
    assert!(!witness.exists());
    run(finite_entry_budget(), &cancellation, |work| {
        recover_path(&live, work)
    })
    .expect("reopened cleanup is idempotent");
    assert_eq!(fs::read(&live)?, b"after");
    assert_eq!(fs::read(&prepared)?, b"before");
    Ok(())
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_never_adopts_a_replacement_preimage() -> Result<(), NativeExchangeError> {
    for completed in [false, true] {
        let (temporary, live, prepared) = entries()?;
        let witness = entry_exchange_witness(&live)?;
        let scratch = scratch(&live)?;
        let saved = temporary.path().join("saved");
        entry_witness_fixture(&live, &prepared, &witness);
        if completed {
            fs::rename(&live, &scratch)?;
            fs::rename(&prepared, &live)?;
        }
        fs::rename(&live, &saved)?;
        fs::write(&live, b"user replacement")?;
        let failure = run(finite_entry_budget(), &CancellationToken::new(), |work| {
            recover_path(&live, work)
        })
        .expect_err("replacement is not original authority");
        assert!(matches!(
            failure.0,
            NativeExchangeError::IncompatibleJournal
        ));
        assert!(failure.1.backend_read_operations > 0);
        assert_eq!(failure.1.backend_write_operations, 0);
        assert_eq!(fs::read(&live)?, b"user replacement");
        assert_eq!(
            fs::read(&saved)?,
            if completed {
                b"after".as_slice()
            } else {
                b"before".as_slice()
            }
        );
        assert!(witness.exists());
        if completed {
            assert_eq!(fs::read(&scratch)?, b"before");
        }
    }
    Ok(())
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_preserves_a_replaced_physical_parent() -> Result<(), NativeExchangeError>
{
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("root");
    let saved = temporary.path().join("saved");
    fs::create_dir(&root)?;
    let live = root.join("live");
    let prepared = root.join("prepared");
    let witness = entry_exchange_witness(&live)?;
    fs::write(&live, b"before")?;
    fs::write(&prepared, b"after")?;
    entry_witness_fixture(&live, &prepared, &witness);
    let ledger = NativeMaterializationWork::new(finite_entry_budget(), &CancellationToken::new())
        .expect("original native meter");
    let mut work = &ledger;
    let parent = crate::native_host::open_root_directory(&root, &mut work)
        .map(|(directory, _)| directory)
        .expect("original held root");
    fs::rename(&root, &saved)?;
    fs::create_dir(&root)?;
    fs::write(root.join("live"), b"user-live")?;
    fs::write(root.join("prepared"), b"user-prepared")?;
    recover_native_entry_exchange(&parent, Path::new("live"), &mut work)
        .expect_err("persisted right parent no longer names the original root");
    assert_eq!(fs::read(root.join("live"))?, b"user-live");
    assert_eq!(fs::read(root.join("prepared"))?, b"user-prepared");
    assert_eq!(fs::read(saved.join("live"))?, b"before");
    assert!(
        saved
            .join(witness.file_name().expect("witness name"))
            .exists()
    );
    Ok(())
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_rejects_unbounded_lengths_before_buffer_allocation()
-> Result<(), NativeExchangeError> {
    use std::io::{Seek as _, Write as _};
    let (temporary, live, prepared) = entries()?;
    let witness = entry_exchange_witness(&live)?;
    entry_witness_fixture(&live, &prepared, &witness);
    let mut journal = fs::OpenOptions::new().write(true).open(&witness)?;
    journal.seek(std::io::SeekFrom::Start(96))?;
    journal.write_all(&u64::MAX.to_le_bytes())?;
    drop(journal);
    let failure = run(finite_entry_budget(), &CancellationToken::new(), |work| {
        recover_path(&live, work)
    })
    .expect_err("overflowing buffer length rejected");
    assert!(matches!(
        failure.0,
        NativeExchangeError::Work(crate::WorkError::Overflow)
    ));
    assert_eq!(failure.1.backend_write_operations, 0);
    assert!(failure.1.peak_allocation_bytes < finite_entry_budget().peak_allocation_bytes);
    assert_eq!(fs::read(&live)?, b"before");
    assert_eq!(fs::read(&prepared)?, b"after");
    assert!(witness.exists());
    Ok(())
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_retains_journal_replacements_and_trailing_data()
-> Result<(), NativeExchangeError> {
    use std::io::Write as _;
    let (temporary, live, prepared) = entries()?;
    let witness = entry_exchange_witness(&live)?;
    entry_witness_fixture(&live, &prepared, &witness);
    let mut journal = fs::OpenOptions::new().append(true).open(&witness)?;
    journal.write_all(b"user trailing bytes")?;
    drop(journal);
    let failure = run(finite_entry_budget(), &CancellationToken::new(), |work| {
        recover_path(&live, work)
    })
    .expect_err("noncanonical witness retained");
    assert!(matches!(
        failure.0,
        NativeExchangeError::IncompatibleJournal
    ));
    assert_eq!(failure.1.backend_write_operations, 0);
    assert!(fs::read(&witness)?.ends_with(b"user trailing bytes"));
    assert_eq!(fs::read(&live)?, b"before");
    assert_eq!(fs::read(&prepared)?, b"after");
    Ok(())
}

#[cfg(windows)]
#[test]
fn bounded_entry_exchange_preserves_raw_windows_names() -> Result<(), NativeExchangeError> {
    use std::os::windows::ffi::OsStringExt as _;
    let temporary = tempfile::tempdir()?;
    let live_name: Vec<_> = "live".encode_utf16().chain([0xd800]).collect();
    let prepared_name: Vec<_> = "prepared".encode_utf16().chain([0xdc00]).collect();
    let live = temporary
        .path()
        .join(std::ffi::OsString::from_wide(&live_name));
    let prepared = temporary
        .path()
        .join(std::ffi::OsString::from_wide(&prepared_name));
    fs::write(&live, b"before")?;
    fs::write(&prepared, b"after")?;
    run(finite_entry_budget(), &CancellationToken::new(), |work| {
        exchange_paths(&live, &prepared, work)
    })
    .expect("raw native exchange");
    assert_eq!(fs::read(&live)?, b"after");
    assert_eq!(fs::read(&prepared)?, b"before");
    Ok(())
}

#[test]
fn carrying_recovery_keeps_a_concurrently_recreated_live_path() -> Result<(), NativeExchangeError> {
    let (temporary, live, prepared) = trees()?;
    fs::write(live.join("private"), b"original")?;
    let journal = temporary.path().join("exchange.json");
    let operation = IdempotencyKey::from_bytes([30; 16]);
    let carried = vec![PathBuf::from("private")];
    prepare_native_exchange(
        &journal,
        &live,
        &prepared,
        operation,
        carried.clone(),
        Vec::new(),
    )?;
    carry(&live, &prepared, &carried)?;

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    BEFORE_ROLLBACK_RENAME.with(|hook| {
        hook.replace(Some(barrier.clone()));
    });
    std::thread::scope(|scope| {
        scope.spawn(|| {
            barrier.wait();
            assert!(!live.join("private").exists());
            fs::write(live.join("private"), b"user edit").expect("user edit");
            barrier.wait();
        });
        assert!(matches!(
            recover_native_exchange(&journal),
            Err(NativeExchangeError::CarriedPathConflict(path)) if path == carried[0]
        ));
    });
    assert_eq!(fs::read(live.join("private"))?, b"user edit");
    assert_eq!(fs::read(prepared.join("private"))?, b"original");
    assert_eq!(read_journal(&journal)?.operation, operation);

    fs::rename(live.join("private"), live.join("saved-user-edit"))?;
    assert!(!recover_native_exchange(&journal)?.published);
    assert_eq!(fs::read(live.join("private"))?, b"original");
    assert_eq!(fs::read(live.join("saved-user-edit"))?, b"user edit");
    assert!(!journal.exists());
    Ok(())
}

#[test]
fn carrying_recovery_replays_a_partially_completed_rollback() -> Result<(), NativeExchangeError> {
    let (temporary, live, prepared) = trees()?;
    let carried = vec![PathBuf::from("first"), PathBuf::from("second")];
    for path in &carried {
        fs::write(live.join(path), b"original")?;
    }
    let journal = temporary.path().join("exchange.json");
    prepare_native_exchange(
        &journal,
        &live,
        &prepared,
        IdempotencyKey::from_bytes([33; 16]),
        carried.clone(),
        Vec::new(),
    )?;
    carry(&live, &prepared, &carried)?;
    move_back(&prepared, &live, &carried[..1])?;
    fs::write(live.join("first"), b"later user edit")?;
    assert!(!recover_native_exchange(&journal)?.published);
    assert_eq!(fs::read(live.join("first"))?, b"later user edit");
    assert_eq!(fs::read(live.join("second"))?, b"original");
    assert!(!journal.exists());
    Ok(())
}

#[test]
fn carrying_recovery_rejects_replaced_roots_without_mutation() -> Result<(), NativeExchangeError> {
    for replace_live in [true, false] {
        let (temporary, live, prepared) = trees()?;
        fs::write(live.join("private"), b"original")?;
        let journal = temporary.path().join("exchange.json");
        let carried = vec![PathBuf::from("private")];
        prepare_native_exchange(
            &journal,
            &live,
            &prepared,
            IdempotencyKey::from_bytes([31; 16]),
            carried.clone(),
            Vec::new(),
        )?;
        carry(&live, &prepared, &carried)?;
        let replaced = if replace_live { &live } else { &prepared };
        let saved = temporary.path().join("saved");
        fs::rename(replaced, &saved)?;
        fs::create_dir(replaced)?;
        fs::write(replaced.join("private"), b"unrelated")?;
        assert!(matches!(
            recover_native_exchange(&journal),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert_eq!(fs::read(replaced.join("private"))?, b"unrelated");
        let original = if replace_live { &prepared } else { &saved };
        assert_eq!(fs::read(original.join("private"))?, b"original");
        assert!(journal.exists());
    }
    Ok(())
}

#[test]
fn invalid_carried_paths_fail_before_preparation_or_recovery_effects()
-> Result<(), NativeExchangeError> {
    let (temporary, live, prepared) = trees()?;
    fs::write(prepared.join("valid"), b"untouched")?;
    let journal_path = temporary.path().join("exchange.json");
    let mut invalid = vec![
        PathBuf::new(),
        PathBuf::from("."),
        PathBuf::from("../outside"),
        PathBuf::from("nested/../../outside"),
        live.clone(),
    ];
    #[cfg(windows)]
    invalid.extend([PathBuf::from("C:relative"), PathBuf::from("\\rooted")]);
    for path in invalid.drain(..) {
        let carried = vec![PathBuf::from("valid"), path];
        assert!(matches!(
            prepare_native_exchange(
                &journal_path,
                &live,
                &prepared,
                IdempotencyKey::from_bytes([32; 16]),
                carried.clone(),
                Vec::new()
            ),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert!(!journal_path.exists());
        let journal = fixture_journal(
            &live,
            &prepared,
            IdempotencyKey::from_bytes([32; 16]),
            carried,
            Vec::new(),
            NativeExchangePhase::Carrying,
        );
        write_journal(&journal_path, &journal)?;
        assert!(matches!(
            recover_native_exchange(&journal_path),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert_eq!(fs::read(prepared.join("valid"))?, b"untouched");
        assert!(!live.join("valid").exists());
        fs::remove_file(&journal_path)?;
    }
    Ok(())
}

#[cfg(windows)]
#[test]
fn entry_exchange_recovery_preserves_both_rename_prefixes() -> Result<(), NativeExchangeError> {
    for published in [false, true] {
        let (temporary, live, prepared) = entries()?;
        let scratch = scratch(&live)?;
        let witness = entry_exchange_witness(&live)?;
        entry_witness_fixture(&live, &prepared, &witness);
        fs::rename(&live, &scratch)?;
        if published {
            fs::rename(&prepared, &live)?;
        }
        recover_path(
            &live,
            &NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .expect("test work"),
        )?;
        assert_eq!(
            fs::read(&live)?,
            if published {
                b"after".as_slice()
            } else {
                b"before".as_slice()
            }
        );
        assert!(!scratch.exists());
        if published {
            assert_eq!(fs::read(&prepared)?, b"before");
        }
    }
    Ok(())
}

#[cfg(windows)]
#[test]
fn entry_exchange_recovery_preserves_an_unowned_scratch_collision()
-> Result<(), NativeExchangeError> {
    let temporary = tempfile::tempdir()?;
    let live = temporary.path().join("live");
    let scratch = scratch(&live)?;
    fs::write(&live, b"live")?;
    fs::write(&scratch, b"unrelated")?;

    assert!(matches!(
        recover_path(
            &live,
            &NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .expect("test work")
        ),
        Err(NativeExchangeError::IncompatibleJournal)
    ));
    assert_eq!(fs::read(&live)?, b"live");
    assert_eq!(fs::read(&scratch)?, b"unrelated");
    Ok(())
}

#[test]
fn recovery_recognizes_an_exchange_completed_before_journal_removal()
-> Result<(), NativeExchangeError> {
    let (temporary, live, prepared) = trees()?;
    fs::write(live.join("old"), b"old")?;
    fs::write(prepared.join("new"), b"new")?;
    let journal_path = temporary.path().join("exchange.json");
    let journal = fixture_journal(
        &live,
        &prepared,
        IdempotencyKey::from_bytes([1; 16]),
        Vec::new(),
        vec![7; 16],
        NativeExchangePhase::Exchanging,
    );
    write_journal(&journal_path, &journal)?;
    exchange_paths(
        &live,
        &prepared,
        &NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("test work"),
    )?;

    let outcome = publish_native_exchange(
        &journal_path,
        &live,
        &prepared,
        journal.operation,
        Vec::new(),
    )?;
    assert!(outcome.published);
    assert_eq!(outcome.displaced.as_deref(), Some(prepared.as_path()));
    assert_eq!(outcome.recovery, vec![7; 16]);
    assert_eq!(fs::read(live.join("new"))?, b"new");
    assert_eq!(fs::read(prepared.join("old"))?, b"old");
    assert!(!journal_path.exists());
    Ok(())
}

#[test]
fn prepared_exchange_is_idempotent_and_rejects_other_requests() -> Result<(), NativeExchangeError> {
    let (temporary, live, prepared) = trees()?;
    let journal_path = temporary.path().join("exchange.json");
    let operation = IdempotencyKey::from_bytes([12; 16]);
    let carried = vec![PathBuf::from("private")];

    prepare_native_exchange(
        &journal_path,
        &live,
        &prepared,
        operation,
        carried.clone(),
        vec![21; 32],
    )?;
    prepare_native_exchange(
        &journal_path,
        &live,
        &prepared,
        operation,
        carried.clone(),
        vec![21; 32],
    )?;

    let journal = read_journal(&journal_path)?;
    assert_eq!(journal.operation, operation);
    assert_eq!(journal.carried, carried);
    assert_eq!(journal.recovery, vec![21; 32]);
    assert_eq!(journal.phase, NativeExchangePhase::Carrying);
    assert!(matches!(
        prepare_native_exchange(
            &journal_path,
            &live,
            &prepared,
            IdempotencyKey::from_bytes([13; 16]),
            Vec::new(),
            Vec::new()
        ),
        Err(NativeExchangeError::IncompatibleJournal)
    ));
    assert!(matches!(
        publish_native_exchange(
            &journal_path,
            &live,
            &prepared,
            IdempotencyKey::from_bytes([13; 16]),
            Vec::new(),
        ),
        Err(NativeExchangeError::IncompatibleJournal)
    ));
    assert_eq!(read_journal(&journal_path)?.operation, operation);
    Ok(())
}

#[test]
fn publication_rejects_a_journal_for_different_roots() -> Result<(), NativeExchangeError> {
    let temporary = tempfile::tempdir()?;
    let first_live = temporary.path().join("first-live");
    let first_prepared = temporary.path().join("first-prepared");
    let second_live = temporary.path().join("second-live");
    let second_prepared = temporary.path().join("second-prepared");
    for path in [&first_live, &first_prepared, &second_live, &second_prepared] {
        fs::create_dir(path)?;
    }
    fs::write(second_live.join("old"), b"old")?;
    fs::write(second_prepared.join("new"), b"new")?;
    let journal_path = temporary.path().join("exchange.json");
    write_journal(
        &journal_path,
        &fixture_journal(
            &first_live,
            &first_prepared,
            IdempotencyKey::from_bytes([5; 16]),
            Vec::new(),
            Vec::new(),
            NativeExchangePhase::Carrying,
        ),
    )?;

    assert!(matches!(
        publish_native_exchange(
            &journal_path,
            &second_live,
            &second_prepared,
            IdempotencyKey::from_bytes([6; 16]),
            Vec::new(),
        ),
        Err(NativeExchangeError::IncompatibleJournal)
    ));
    assert_eq!(fs::read(second_live.join("old"))?, b"old");
    assert_eq!(fs::read(second_prepared.join("new"))?, b"new");
    assert!(journal_path.exists());
    Ok(())
}

#[test]
fn non_v1_journal_versions_fail_closed_without_mutation() -> Result<(), Box<dyn std::error::Error>> {
    let (temporary, live, prepared) = trees()?;
    fs::write(live.join("old"), b"old")?;
    fs::write(prepared.join("new"), b"new")?;
    let journal_path = temporary.path().join("exchange.json");
    for obsolete_version in [0, 2, 3, u32::MAX] {
        let journal = NativeExchangeJournal {
            version: obsolete_version,
            ..fixture_journal(
                &live,
                &prepared,
                IdempotencyKey::from_bytes([10; 16]),
                Vec::new(),
                Vec::new(),
                NativeExchangePhase::Carrying,
            )
        };
        fs::write(&journal_path, serde_json::to_vec(&journal)?)?;

        assert!(matches!(
            publish_native_exchange(
                &journal_path,
                &live,
                &prepared,
                IdempotencyKey::from_bytes([11; 16]),
                Vec::new(),
            ),
            Err(NativeExchangeError::UnsupportedJournalVersion(version))
                if version == obsolete_version
        ));
        assert_eq!(fs::read(live.join("old"))?, b"old");
        assert_eq!(fs::read(prepared.join("new"))?, b"new");
        assert_eq!(fs::read(&journal_path)?, serde_json::to_vec(&journal)?);
    }
    Ok(())
}

#[cfg(windows)]
#[test]
fn windows_recovery_preserves_both_exchange_prefixes() -> Result<(), NativeExchangeError> {
    for published in [false, true] {
        let (temporary, live, prepared) = trees()?;
        fs::create_dir(live.join("private"))?;
        fs::write(live.join("old"), b"old")?;
        fs::write(live.join("private/key"), b"secret")?;
        fs::write(prepared.join("new"), b"new")?;
        let journal_path = temporary.path().join("exchange.json");
        let mut journal = fixture_journal(
            &live,
            &prepared,
            IdempotencyKey::from_bytes([if published { 8 } else { 7 }; 16]),
            vec![PathBuf::from("private")],
            Vec::new(),
            NativeExchangePhase::Carrying,
        );
        write_journal(&journal_path, &journal)?;
        carry(&live, &prepared, &journal.carried)?;
        journal.phase = NativeExchangePhase::Exchanging;
        write_journal(&journal_path, &journal)?;
        let scratch = scratch(&live)?;
        durable_rename(
            &live,
            &scratch,
            acyclic_native_runtime::RenameMode::NoReplace,
            None,
        )?;
        if published {
            durable_rename(
                &prepared,
                &live,
                acyclic_native_runtime::RenameMode::NoReplace,
                None,
            )?;
        }
        let outcome = recover_native_exchange(&journal_path)?;
        assert_eq!(outcome.published, published);
        assert_eq!(
            outcome.displaced.as_deref(),
            published.then_some(prepared.as_path())
        );
        let (original, replacement) = if published {
            (&prepared, &live)
        } else {
            (&live, &prepared)
        };
        assert_eq!(fs::read(original.join("old"))?, b"old");
        assert_eq!(fs::read(replacement.join("new"))?, b"new");
        assert_eq!(fs::read(live.join("private/key"))?, b"secret");
        assert!(!scratch.exists());
        assert!(!journal_path.exists());
    }
    Ok(())
}

#[test]
fn exchange_publishes_whole_tree_and_carries_nested_exclusion() -> Result<(), NativeExchangeError> {
    let temporary = tempfile::tempdir()?;
    let live = temporary.path().join("live");
    let prepared = temporary.path().join("prepared");
    fs::create_dir_all(live.join("private/nested"))?;
    fs::create_dir_all(&prepared)?;
    fs::write(live.join("old"), b"old")?;
    fs::write(live.join("private/nested/key"), b"secret")?;
    fs::write(prepared.join("new"), b"new")?;
    let journal = temporary.path().join("exchange.json");
    let outcome = publish_native_exchange(
        &journal,
        &live,
        &prepared,
        IdempotencyKey::from_bytes([2; 16]),
        vec![PathBuf::from("private/nested")],
    )?;
    assert!(outcome.published);
    assert_eq!(fs::read(live.join("new"))?, b"new");
    assert_eq!(fs::read(live.join("private/nested/key"))?, b"secret");
    assert_eq!(fs::read(prepared.join("old"))?, b"old");
    assert!(!journal.exists());
    Ok(())
}

#[test]
fn stale_completed_journal_does_not_suppress_the_next_publication()
-> Result<(), NativeExchangeError> {
    let (temporary, live, prepared) = trees()?;
    fs::write(live.join("first"), b"first")?;
    fs::write(prepared.join("second"), b"second")?;
    let journal_path = temporary.path().join("exchange.json");
    let stale = fixture_journal(
        &live,
        &prepared,
        IdempotencyKey::from_bytes([3; 16]),
        Vec::new(),
        Vec::new(),
        NativeExchangePhase::Exchanging,
    );
    write_journal(&journal_path, &stale)?;
    exchange_paths(
        &live,
        &prepared,
        &NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("test work"),
    )?;

    let recovered = recover_native_exchange(&journal_path)?;
    assert!(recovered.published);
    remove_entry(&prepared)?;
    fs::create_dir(&prepared)?;
    fs::write(prepared.join("third"), b"third")?;

    let outcome = publish_native_exchange(
        &journal_path,
        &live,
        &prepared,
        IdempotencyKey::from_bytes([4; 16]),
        Vec::new(),
    )?;
    assert!(outcome.published);
    assert_eq!(fs::read(live.join("third"))?, b"third");
    assert_eq!(fs::read(prepared.join("second"))?, b"second");
    assert!(!journal_path.exists());
    Ok(())
}

#[test]
fn entry_exchange_preserves_both_complete_entries() -> Result<(), NativeExchangeError> {
    let temporary = tempfile::tempdir()?;
    let left = temporary.path().join("left");
    let right = temporary.path().join("right");
    fs::create_dir(&left)?;
    fs::create_dir(&right)?;
    fs::write(left.join("value"), b"left")?;
    fs::write(right.join("value"), b"right")?;
    exchange_paths(
        &left,
        &right,
        &NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("test work"),
    )?;
    assert_eq!(fs::read(left.join("value"))?, b"right");
    assert_eq!(fs::read(right.join("value"))?, b"left");
    Ok(())
}
