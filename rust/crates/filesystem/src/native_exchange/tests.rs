fn entries() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let live = temporary.path().join("live");
    let prepared = temporary.path().join("prepared");
    std::fs::write(&live, b"before").expect("original");
    std::fs::write(&prepared, b"after").expect("prepared");
    (temporary, live, prepared)
}

fn trees() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let live = temporary.path().join("live");
    let prepared = temporary.path().join("prepared");
    std::fs::create_dir(&live).expect("live tree");
    std::fs::create_dir(&prepared).expect("prepared tree");
    (temporary, live, prepared)
}

fn fixture_journal(
    live: &Path,
    prepared: &Path,
    operation: crate::IdempotencyKey,
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

pub(super) fn receipt(
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
    operation: impl FnOnce(&crate::NativeMaterializationWork) -> Result<(), NativeExchangeError>,
) -> Result<crate::OperationReceipt<()>, (NativeExchangeError, crate::WorkCounters)> {
    let work = crate::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(error, work)| (error.into(), work))?;
    let result = operation(&work);
    result
        .map(|value| crate::OperationReceipt {
            value,
            work: work.receipt(),
        })
        .map_err(|error| (error, work.receipt()))
}

use super::*;

fn finite_entry_budget() -> crate::WorkBudget {
    crate::WorkCounters {
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
        ..crate::WorkCounters::default()
    }
}

#[cfg(windows)]
fn entry_witness_fixture(live: &Path, prepared: &Path, witness: &Path) {
    let cancellation = crate::CancellationToken::new();
    let ledger =
        crate::materializer::NativeMaterializationWork::new(finite_entry_budget(), &cancellation)
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
fn bounded_entry_exchange_rejects_zero_and_cancelled_work_before_io() {
    let (temporary, live, prepared) = entries();
    let cancellation = crate::CancellationToken::new();
    let failure = receipt(crate::WorkBudget::default(), &cancellation, |work| {
        exchange_native_entries(&live, &prepared, work)
    })
    .expect_err("zero allowance");
    assert_eq!(failure.1, crate::WorkCounters::default());
    cancellation.cancel();
    let failure = receipt(finite_entry_budget(), &cancellation, |work| {
        exchange_native_entries(&live, &prepared, work)
    })
    .expect_err("cancelled");
    assert!(matches!(failure.0, NativeExchangeError::Cancelled));
    assert_eq!(failure.1, crate::WorkCounters::default());
    assert_eq!(std::fs::read(&live).expect("original preserved"), b"before");
    assert_eq!(
        std::fs::read(&prepared).expect("prepared preserved"),
        b"after"
    );
}

#[cfg(windows)]
#[test]
fn bounded_entry_journal_failure_cleans_only_its_provisional_handle() {
    for deny_bytes in [true, false] {
        let (temporary, live, prepared) = entries();
        let witness = entry_exchange_witness(&live).expect("witness");
        let scratch = scratch(&live).expect("scratch");
        let mut budget = finite_entry_budget();
        if deny_bytes {
            budget.output_bytes = (ENTRY_WITNESS.len() - 1) as u64;
        } else {
            budget.durability_operations = 0;
        }
        let cancellation = crate::CancellationToken::new();
        let failure = receipt(budget, &cancellation, |work| {
            exchange_native_entries(&live, &prepared, work)
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
        assert_eq!(std::fs::read(&live).expect("original retained"), b"before");
        assert_eq!(
            std::fs::read(&prepared).expect("prepared retained"),
            b"after"
        );
        receipt(finite_entry_budget(), &cancellation, |work| {
            exchange_native_entries(&live, &prepared, work)
        })
        .expect("the real durable request remains retryable");
        assert_eq!(std::fs::read(&live).expect("published"), b"after");
        assert_eq!(std::fs::read(&prepared).expect("displaced"), b"before");
    }
}

#[cfg(all(feature = "native-watch", unix))]
#[test]
fn bounded_entry_exchange_failure_retains_the_physical_edit_prefix() {
    let (temporary, live, prepared) = entries();
    let mut budget = finite_entry_budget();
    budget.durability_operations = 0;
    let failure = receipt(budget, &crate::CancellationToken::new(), |work| {
        exchange_native_entries(&live, &prepared, work)
    })
    .expect_err("durability denied");
    assert!(matches!(failure.0, NativeExchangeError::Work(_)));
    assert_eq!(failure.1.backend_write_operations, 1);
    assert_eq!(failure.1.durability_operations, 0);
    assert_eq!(
        std::fs::read(&live).expect("actual published bytes"),
        b"after"
    );
    assert_eq!(
        std::fs::read(&prepared).expect("actual displaced bytes"),
        b"before"
    );
}

#[cfg(all(feature = "native-watch", unix))]
#[test]
fn bounded_exchange_keeps_original_parents_when_the_lexical_root_is_replaced() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let saved = temporary.path().join("saved");
    std::fs::create_dir(&root).expect("root");
    std::fs::write(root.join("live"), b"before").expect("original");
    std::fs::write(root.join("prepared"), b"after").expect("prepared");
    let ledger = crate::materializer::NativeMaterializationWork::new(
        finite_entry_budget(),
        &crate::CancellationToken::new(),
    )
    .expect("original native meter");
    let mut work = &ledger;
    let parent = crate::native_host::open_root_directory(&root, &mut work)
        .map(|(directory, _)| directory)
        .expect("original held parent");
    std::fs::rename(&root, &saved).expect("displace original root");
    std::fs::create_dir(&root).expect("user replacement root");
    std::fs::write(root.join("live"), b"user-live").expect("user live");
    std::fs::write(root.join("prepared"), b"user-prepared").expect("user prepared");
    exchange_native_entries_held(
        &parent,
        Path::new("live"),
        &parent,
        Path::new("prepared"),
        &mut work,
    )
    .expect("held physical exchange");
    assert_eq!(
        std::fs::read(saved.join("live")).expect("held published"),
        b"after"
    );
    assert_eq!(
        std::fs::read(saved.join("prepared")).expect("held displaced"),
        b"before"
    );
    assert_eq!(
        std::fs::read(root.join("live")).expect("user live preserved"),
        b"user-live"
    );
    assert_eq!(
        std::fs::read(root.join("prepared")).expect("user prepared preserved"),
        b"user-prepared"
    );
}

#[cfg(windows)]
#[test]
fn bounded_entry_retry_rejects_a_different_request_without_adopting_it() {
    let (temporary, live, prepared) = entries();
    let other = temporary.path().join("other");
    let witness = entry_exchange_witness(&live).expect("witness");
    std::fs::write(&other, b"unrelated request").expect("other request");
    entry_witness_fixture(&live, &prepared, &witness);
    let cancellation = crate::CancellationToken::new();
    let failure = receipt(finite_entry_budget(), &cancellation, |work| {
        exchange_native_entries(&live, &other, work)
    })
    .expect_err("original request path identity remains authoritative");
    assert!(matches!(
        failure.0,
        NativeExchangeError::IncompatibleJournal
    ));
    assert_eq!(failure.1.backend_write_operations, 0);
    assert!(witness.exists());
    assert_eq!(std::fs::read(&live).expect("original retained"), b"before");
    assert_eq!(
        std::fs::read(&prepared).expect("original preparation retained"),
        b"after"
    );
    assert_eq!(
        std::fs::read(&other).expect("other request retained"),
        b"unrelated request"
    );
    receipt(finite_entry_budget(), &cancellation, |work| {
        exchange_native_entries(&live, &prepared, work)
    })
    .expect("original rolled-back request proceeds to actual publication");
    assert_eq!(
        std::fs::read(&live).expect("original request published"),
        b"after"
    );
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_finishes_in_the_original_other_parent() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let live_parent = temporary.path().join("checkout");
    let prepared_parent = temporary.path().join("operation");
    std::fs::create_dir(&live_parent).expect("checkout parent");
    std::fs::create_dir(&prepared_parent).expect("operation parent");
    let live = live_parent.join("live");
    let prepared = prepared_parent.join("prepared");
    let witness = entry_exchange_witness(&live).expect("witness");
    let scratch = scratch(&live).expect("scratch");
    std::fs::write(&live, b"before").expect("original");
    std::fs::write(&prepared, b"after").expect("prepared");
    entry_witness_fixture(&live, &prepared, &witness);
    std::fs::rename(&live, &scratch).expect("first physical edit");
    std::fs::rename(&prepared, &live).expect("publication before interrupted displacement");
    receipt(
        finite_entry_budget(),
        &crate::CancellationToken::new(),
        |work| recover_native_entry_exchange(&live, work),
    )
    .expect("held original other-parent recovery");
    assert_eq!(std::fs::read(&live).expect("published entry"), b"after");
    assert_eq!(
        std::fs::read(&prepared).expect("original displaced entry"),
        b"before"
    );
    assert!(!scratch.exists());
    assert!(!witness.exists());
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_keeps_the_same_lost_ack_decision() {
    let (temporary, live, prepared) = entries();
    let witness = entry_exchange_witness(&live).expect("witness");
    let scratch = scratch(&live).expect("scratch");
    entry_witness_fixture(&live, &prepared, &witness);
    std::fs::rename(&live, &scratch).expect("first rename");
    std::fs::rename(&prepared, &live).expect("publication");
    std::fs::rename(&scratch, &prepared).expect("displacement before lost acknowledgement");
    let cancellation = crate::CancellationToken::new();
    let recovered = receipt(finite_entry_budget(), &cancellation, |work| {
        recover_native_entry_exchange(&live, work)
    })
    .expect("restart recovers original decision");
    assert!(recovered.work.backend_write_operations > 0);
    assert!(!witness.exists());
    receipt(finite_entry_budget(), &cancellation, |work| {
        recover_native_entry_exchange(&live, work)
    })
    .expect("reopened cleanup is idempotent");
    assert_eq!(
        std::fs::read(&live).expect("published identity retained"),
        b"after"
    );
    assert_eq!(
        std::fs::read(&prepared).expect("original preimage retained"),
        b"before"
    );
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_never_adopts_a_replacement_preimage() {
    for completed in [false, true] {
        let (temporary, live, prepared) = entries();
        let witness = entry_exchange_witness(&live).expect("witness");
        let scratch = scratch(&live).expect("scratch");
        let saved = temporary.path().join("saved");
        entry_witness_fixture(&live, &prepared, &witness);
        if completed {
            std::fs::rename(&live, &scratch).expect("first rename");
            std::fs::rename(&prepared, &live).expect("publication");
        }
        std::fs::rename(&live, &saved).expect("retain replaced physical entry");
        std::fs::write(&live, b"user replacement").expect("user replacement");
        let failure = receipt(
            finite_entry_budget(),
            &crate::CancellationToken::new(),
            |work| recover_native_entry_exchange(&live, work),
        )
        .expect_err("replacement is not original authority");
        assert!(matches!(
            failure.0,
            NativeExchangeError::IncompatibleJournal
        ));
        assert!(failure.1.backend_read_operations > 0);
        assert_eq!(failure.1.backend_write_operations, 0);
        assert_eq!(
            std::fs::read(&live).expect("user replacement retained"),
            b"user replacement"
        );
        assert_eq!(
            std::fs::read(&saved).expect("original binding retained"),
            if completed {
                b"after".as_slice()
            } else {
                b"before".as_slice()
            }
        );
        assert!(witness.exists());
        if completed {
            assert_eq!(
                std::fs::read(&scratch).expect("original preimage retained"),
                b"before"
            );
        }
    }
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_preserves_a_replaced_physical_parent() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let saved = temporary.path().join("saved");
    std::fs::create_dir(&root).expect("root");
    let live = root.join("live");
    let prepared = root.join("prepared");
    let witness = entry_exchange_witness(&live).expect("witness");
    std::fs::write(&live, b"before").expect("original");
    std::fs::write(&prepared, b"after").expect("prepared");
    entry_witness_fixture(&live, &prepared, &witness);
    let ledger = crate::materializer::NativeMaterializationWork::new(
        finite_entry_budget(),
        &crate::CancellationToken::new(),
    )
    .expect("original native meter");
    let mut work = &ledger;
    let parent = crate::native_host::open_root_directory(&root, &mut work)
        .map(|(directory, _)| directory)
        .expect("original held root");
    std::fs::rename(&root, &saved).expect("displace original physical parent");
    std::fs::create_dir(&root).expect("user replacement root");
    std::fs::write(root.join("live"), b"user-live").expect("user live");
    std::fs::write(root.join("prepared"), b"user-prepared").expect("user prepared");
    recover_native_entry_exchange_held(&parent, Path::new("live"), &mut work)
        .expect_err("persisted right parent no longer names the original root");
    assert_eq!(
        std::fs::read(root.join("live")).expect("user live preserved"),
        b"user-live"
    );
    assert_eq!(
        std::fs::read(root.join("prepared")).expect("user prepared preserved"),
        b"user-prepared"
    );
    assert_eq!(
        std::fs::read(saved.join("live")).expect("original preserved"),
        b"before"
    );
    assert!(
        saved
            .join(witness.file_name().expect("witness name"))
            .exists()
    );
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_rejects_unbounded_lengths_before_buffer_allocation() {
    use std::io::{Seek as _, Write as _};
    let (temporary, live, prepared) = entries();
    let witness = entry_exchange_witness(&live).expect("witness");
    entry_witness_fixture(&live, &prepared, &witness);
    let mut journal = std::fs::OpenOptions::new()
        .write(true)
        .open(&witness)
        .expect("fixture journal");
    journal
        .seek(std::io::SeekFrom::Start(96))
        .expect("first native path length");
    journal
        .write_all(&u64::MAX.to_le_bytes())
        .expect("hostile durable length");
    drop(journal);
    let failure = receipt(
        finite_entry_budget(),
        &crate::CancellationToken::new(),
        |work| recover_native_entry_exchange(&live, work),
    )
    .expect_err("overflowing buffer length rejected");
    assert!(matches!(
        failure.0,
        NativeExchangeError::Work(crate::WorkError::Overflow)
    ));
    assert_eq!(failure.1.backend_write_operations, 0);
    assert!(failure.1.peak_allocation_bytes < finite_entry_budget().peak_allocation_bytes);
    assert_eq!(std::fs::read(&live).expect("original retained"), b"before");
    assert_eq!(
        std::fs::read(&prepared).expect("prepared retained"),
        b"after"
    );
    assert!(witness.exists());
}

#[cfg(windows)]
#[test]
fn bounded_entry_recovery_retains_journal_replacements_and_trailing_data() {
    use std::io::Write as _;
    let (temporary, live, prepared) = entries();
    let witness = entry_exchange_witness(&live).expect("witness");
    entry_witness_fixture(&live, &prepared, &witness);
    let mut journal = std::fs::OpenOptions::new()
        .append(true)
        .open(&witness)
        .expect("fixture journal");
    journal
        .write_all(b"user trailing bytes")
        .expect("external journal change");
    drop(journal);
    let failure = receipt(
        finite_entry_budget(),
        &crate::CancellationToken::new(),
        |work| recover_native_entry_exchange(&live, work),
    )
    .expect_err("noncanonical witness retained");
    assert!(matches!(
        failure.0,
        NativeExchangeError::IncompatibleJournal
    ));
    assert_eq!(failure.1.backend_write_operations, 0);
    assert!(
        std::fs::read(&witness)
            .expect("journal retained")
            .ends_with(b"user trailing bytes")
    );
    assert_eq!(std::fs::read(&live).expect("original retained"), b"before");
    assert_eq!(
        std::fs::read(&prepared).expect("prepared retained"),
        b"after"
    );
}

#[cfg(windows)]
#[test]
fn bounded_entry_exchange_preserves_raw_windows_names() {
    use std::os::windows::ffi::OsStringExt as _;
    let temporary = tempfile::tempdir().expect("temporary directory");
    let live_name: Vec<_> = "live".encode_utf16().chain([0xd800]).collect();
    let prepared_name: Vec<_> = "prepared".encode_utf16().chain([0xdc00]).collect();
    let live = temporary
        .path()
        .join(std::ffi::OsString::from_wide(&live_name));
    let prepared = temporary
        .path()
        .join(std::ffi::OsString::from_wide(&prepared_name));
    std::fs::write(&live, b"before").expect("raw original name");
    std::fs::write(&prepared, b"after").expect("raw prepared name");
    receipt(
        finite_entry_budget(),
        &crate::CancellationToken::new(),
        |work| exchange_native_entries(&live, &prepared, work),
    )
    .expect("raw native exchange");
    assert_eq!(
        std::fs::read(&live).expect("raw published binding"),
        b"after"
    );
    assert_eq!(
        std::fs::read(&prepared).expect("raw displaced binding"),
        b"before"
    );
}

#[test]
fn carrying_recovery_keeps_a_concurrently_recreated_live_path() {
    let (temporary, live, prepared) = trees();
    std::fs::write(live.join("private"), b"original").expect("original entry");
    let journal = temporary.path().join("exchange.json");
    let operation = crate::IdempotencyKey::from_bytes([30; 16]);
    let carried = vec![PathBuf::from("private")];
    prepare_native_exchange(
        &journal,
        &live,
        &prepared,
        operation,
        carried.clone(),
        Vec::new(),
    )
    .expect("prepare exchange");
    carry(&live, &prepared, &carried).expect("crash after carry");

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    BEFORE_ROLLBACK_RENAME.with(|hook| {
        hook.replace(Some(barrier.clone()));
    });
    std::thread::scope(|scope| {
        scope.spawn(|| {
            barrier.wait();
            assert!(!live.join("private").exists());
            std::fs::write(live.join("private"), b"user edit").expect("user edit");
            barrier.wait();
        });
        assert!(matches!(
            recover_native_exchange(&journal),
            Err(NativeExchangeError::CarriedPathConflict(path)) if path == carried[0]
        ));
    });
    assert_eq!(
        std::fs::read(live.join("private")).expect("user entry"),
        b"user edit"
    );
    assert_eq!(
        std::fs::read(prepared.join("private")).expect("original entry"),
        b"original"
    );
    assert_eq!(
        read_journal(&journal).expect("retained journal").operation,
        operation
    );

    std::fs::rename(live.join("private"), live.join("saved-user-edit"))
        .expect("resolve conflict without discarding user edit");
    assert!(
        !recover_native_exchange(&journal)
            .expect("retry recovery")
            .published
    );
    assert_eq!(
        std::fs::read(live.join("private")).expect("restored entry"),
        b"original"
    );
    assert_eq!(
        std::fs::read(live.join("saved-user-edit")).expect("saved user entry"),
        b"user edit"
    );
    assert!(!journal.exists());
}

#[test]
fn carrying_recovery_replays_a_partially_completed_rollback() {
    let (temporary, live, prepared) = trees();
    let carried = vec![PathBuf::from("first"), PathBuf::from("second")];
    for path in &carried {
        std::fs::write(live.join(path), b"original").expect("original entry");
    }
    let journal = temporary.path().join("exchange.json");
    prepare_native_exchange(
        &journal,
        &live,
        &prepared,
        crate::IdempotencyKey::from_bytes([33; 16]),
        carried.clone(),
        Vec::new(),
    )
    .expect("prepare exchange");
    carry(&live, &prepared, &carried).expect("carry entries");
    move_back(&prepared, &live, &carried[..1]).expect("crash after first rollback rename");
    std::fs::write(live.join("first"), b"later user edit").expect("edit returned entry");
    assert!(
        !recover_native_exchange(&journal)
            .expect("resume rollback")
            .published
    );
    assert_eq!(
        std::fs::read(live.join("first")).expect("returned entry"),
        b"later user edit"
    );
    assert_eq!(
        std::fs::read(live.join("second")).expect("remaining entry"),
        b"original"
    );
    assert!(!journal.exists());
}

#[test]
fn carrying_recovery_rejects_replaced_roots_without_mutation() {
    for replace_live in [true, false] {
        let (temporary, live, prepared) = trees();
        std::fs::write(live.join("private"), b"original").expect("original entry");
        let journal = temporary.path().join("exchange.json");
        let carried = vec![PathBuf::from("private")];
        prepare_native_exchange(
            &journal,
            &live,
            &prepared,
            crate::IdempotencyKey::from_bytes([31; 16]),
            carried.clone(),
            Vec::new(),
        )
        .expect("prepare exchange");
        carry(&live, &prepared, &carried).expect("crash after carry");
        let replaced = if replace_live { &live } else { &prepared };
        let saved = temporary.path().join("saved");
        std::fs::rename(replaced, &saved).expect("displace original root");
        std::fs::create_dir(replaced).expect("replacement root");
        std::fs::write(replaced.join("private"), b"unrelated").expect("replacement entry");
        assert!(matches!(
            recover_native_exchange(&journal),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert_eq!(
            std::fs::read(replaced.join("private")).expect("replacement entry"),
            b"unrelated"
        );
        let original = if replace_live { &prepared } else { &saved };
        assert_eq!(
            std::fs::read(original.join("private")).expect("original entry"),
            b"original"
        );
        assert!(journal.exists());
    }
}

#[test]
fn invalid_carried_paths_fail_before_preparation_or_recovery_effects() {
    let (temporary, live, prepared) = trees();
    std::fs::write(prepared.join("valid"), b"untouched").expect("prepared entry");
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
                crate::IdempotencyKey::from_bytes([32; 16]),
                carried.clone(),
                Vec::new()
            ),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert!(!journal_path.exists());
        let journal = fixture_journal(
            &live,
            &prepared,
            crate::IdempotencyKey::from_bytes([32; 16]),
            carried,
            Vec::new(),
            NativeExchangePhase::Carrying,
        );
        write_journal(&journal_path, &journal).expect("malformed persisted journal");
        assert!(matches!(
            recover_native_exchange(&journal_path),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert_eq!(
            std::fs::read(prepared.join("valid")).expect("untouched entry"),
            b"untouched"
        );
        assert!(!live.join("valid").exists());
        std::fs::remove_file(&journal_path).expect("remove fixture journal");
    }
}

#[cfg(windows)]
#[test]
fn entry_exchange_recovery_preserves_both_rename_prefixes() {
    for published in [false, true] {
        let (temporary, live, prepared) = entries();
        let scratch = scratch(&live).expect("scratch path");
        let witness = entry_exchange_witness(&live).expect("witness path");
        entry_witness_fixture(&live, &prepared, &witness);
        std::fs::rename(&live, &scratch).expect("first physical edit");
        if published {
            std::fs::rename(&prepared, &live).expect("crash after publication");
        }
        recover_native_entry_exchange(
            &live,
            &crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .expect("test work"),
        )
        .expect("recover exchange");
        assert_eq!(
            std::fs::read(&live).expect("recovered entry"),
            if published {
                b"after".as_slice()
            } else {
                b"before".as_slice()
            }
        );
        assert!(!scratch.exists());
        if published {
            assert_eq!(
                std::fs::read(&prepared).expect("preserved preimage"),
                b"before"
            );
        }
    }
}

#[cfg(windows)]
#[test]
fn entry_exchange_recovery_preserves_an_unowned_scratch_collision() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let live = temporary.path().join("live");
    let scratch = scratch(&live).expect("scratch path");
    std::fs::write(&live, b"live").expect("live entry");
    std::fs::write(&scratch, b"unrelated").expect("scratch collision");

    assert!(matches!(
        recover_native_entry_exchange(
            &live,
            &crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .expect("test work")
        ),
        Err(NativeExchangeError::IncompatibleJournal)
    ));
    assert_eq!(std::fs::read(&live).expect("live entry"), b"live");
    assert_eq!(
        std::fs::read(&scratch).expect("scratch collision"),
        b"unrelated"
    );
}

#[test]
fn recovery_recognizes_an_exchange_completed_before_journal_removal() {
    let (temporary, live, prepared) = trees();
    std::fs::write(live.join("old"), b"old").expect("old file");
    std::fs::write(prepared.join("new"), b"new").expect("new file");
    let journal_path = temporary.path().join("exchange.json");
    let journal = fixture_journal(
        &live,
        &prepared,
        crate::IdempotencyKey::from_bytes([1; 16]),
        Vec::new(),
        vec![7; 16],
        NativeExchangePhase::Exchanging,
    );
    write_journal(&journal_path, &journal).expect("journal");
    exchange_native_entries(
        &live,
        &prepared,
        &crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("test work"),
    )
    .expect("exchange");

    let outcome = publish_native_exchange(
        &journal_path,
        &live,
        &prepared,
        journal.operation,
        Vec::new(),
    )
    .expect("recover publication retry");
    assert!(outcome.published);
    assert_eq!(outcome.displaced.as_deref(), Some(prepared.as_path()));
    assert_eq!(outcome.recovery, vec![7; 16]);
    assert_eq!(std::fs::read(live.join("new")).expect("new"), b"new");
    assert_eq!(std::fs::read(prepared.join("old")).expect("old"), b"old");
    assert!(!journal_path.exists());
}

#[test]
fn prepared_exchange_is_idempotent_and_rejects_other_requests() {
    let (temporary, live, prepared) = trees();
    let journal_path = temporary.path().join("exchange.json");
    let operation = crate::IdempotencyKey::from_bytes([12; 16]);
    let carried = vec![PathBuf::from("private")];

    prepare_native_exchange(
        &journal_path,
        &live,
        &prepared,
        operation,
        carried.clone(),
        vec![21; 32],
    )
    .expect("prepare exchange");
    prepare_native_exchange(
        &journal_path,
        &live,
        &prepared,
        operation,
        carried.clone(),
        vec![21; 32],
    )
    .expect("retry preparation");

    let journal = read_journal(&journal_path).expect("read journal");
    assert_eq!(journal.operation, operation);
    assert_eq!(journal.carried, carried);
    assert_eq!(journal.recovery, vec![21; 32]);
    assert_eq!(journal.phase, NativeExchangePhase::Carrying);
    assert!(matches!(
        prepare_native_exchange(
            &journal_path,
            &live,
            &prepared,
            crate::IdempotencyKey::from_bytes([13; 16]),
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
            crate::IdempotencyKey::from_bytes([13; 16]),
            Vec::new(),
        ),
        Err(NativeExchangeError::IncompatibleJournal)
    ));
    assert_eq!(
        read_journal(&journal_path)
            .expect("preserved journal")
            .operation,
        operation
    );
}

#[test]
fn publication_rejects_a_journal_for_different_roots() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let first_live = temporary.path().join("first-live");
    let first_prepared = temporary.path().join("first-prepared");
    let second_live = temporary.path().join("second-live");
    let second_prepared = temporary.path().join("second-prepared");
    for path in [&first_live, &first_prepared, &second_live, &second_prepared] {
        std::fs::create_dir(path).expect("exchange tree");
    }
    std::fs::write(second_live.join("old"), b"old").expect("old file");
    std::fs::write(second_prepared.join("new"), b"new").expect("new file");
    let journal_path = temporary.path().join("exchange.json");
    write_journal(
        &journal_path,
        &fixture_journal(
            &first_live,
            &first_prepared,
            crate::IdempotencyKey::from_bytes([5; 16]),
            Vec::new(),
            Vec::new(),
            NativeExchangePhase::Carrying,
        ),
    )
    .expect("journal");

    assert!(matches!(
        publish_native_exchange(
            &journal_path,
            &second_live,
            &second_prepared,
            crate::IdempotencyKey::from_bytes([6; 16]),
            Vec::new(),
        ),
        Err(NativeExchangeError::IncompatibleJournal)
    ));
    assert_eq!(std::fs::read(second_live.join("old")).expect("old"), b"old");
    assert_eq!(
        std::fs::read(second_prepared.join("new")).expect("new"),
        b"new"
    );
    assert!(journal_path.exists());
}

#[test]
fn non_v1_journal_versions_fail_closed_without_mutation() {
    let (temporary, live, prepared) = trees();
    std::fs::write(live.join("old"), b"old").expect("old file");
    std::fs::write(prepared.join("new"), b"new").expect("new file");
    let journal_path = temporary.path().join("exchange.json");
    for obsolete_version in [0, 2, 3, u32::MAX] {
        let journal = NativeExchangeJournal {
            version: obsolete_version,
            ..fixture_journal(
                &live,
                &prepared,
                crate::IdempotencyKey::from_bytes([10; 16]),
                Vec::new(),
                Vec::new(),
                NativeExchangePhase::Carrying,
            )
        };
        std::fs::write(
            &journal_path,
            serde_json::to_vec(&journal).expect("encode future journal"),
        )
        .expect("write future journal");

        assert!(matches!(
            publish_native_exchange(
                &journal_path,
                &live,
                &prepared,
                crate::IdempotencyKey::from_bytes([11; 16]),
                Vec::new(),
            ),
            Err(NativeExchangeError::UnsupportedJournalVersion(version))
                if version == obsolete_version
        ));
        assert_eq!(std::fs::read(live.join("old")).expect("old"), b"old");
        assert_eq!(std::fs::read(prepared.join("new")).expect("new"), b"new");
        assert_eq!(
            std::fs::read(&journal_path).expect("unchanged journal"),
            serde_json::to_vec(&journal).expect("original bytes")
        );
    }
}

#[cfg(windows)]
#[test]
fn windows_recovery_preserves_both_exchange_prefixes() {
    for published in [false, true] {
        let (temporary, live, prepared) = trees();
        std::fs::create_dir(live.join("private")).expect("private tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(live.join("private/key"), b"secret").expect("carried file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        let mut journal = fixture_journal(
            &live,
            &prepared,
            crate::IdempotencyKey::from_bytes([if published { 8 } else { 7 }; 16]),
            vec![PathBuf::from("private")],
            Vec::new(),
            NativeExchangePhase::Carrying,
        );
        write_journal(&journal_path, &journal).expect("carrying journal");
        carry(&live, &prepared, &journal.carried).expect("carry exclusion");
        journal.phase = NativeExchangePhase::Exchanging;
        write_journal(&journal_path, &journal).expect("exchange journal");
        let scratch = scratch(&live).expect("scratch path");
        durable_rename(
            &live,
            &scratch,
            acyclic_native_runtime::RenameMode::NoReplace,
            None,
        )
        .expect("move live to scratch");
        if published {
            durable_rename(
                &prepared,
                &live,
                acyclic_native_runtime::RenameMode::NoReplace,
                None,
            )
            .expect("publish replacement");
        }
        let outcome = recover_native_exchange(&journal_path).expect("recover exchange");
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
        assert_eq!(std::fs::read(original.join("old")).expect("old"), b"old");
        assert_eq!(std::fs::read(replacement.join("new")).expect("new"), b"new");
        assert_eq!(
            std::fs::read(live.join("private/key")).expect("carried"),
            b"secret"
        );
        assert!(!scratch.exists());
        assert!(!journal_path.exists());
    }
}

#[test]
fn exchange_publishes_whole_tree_and_carries_nested_exclusion() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let live = temporary.path().join("live");
    let prepared = temporary.path().join("prepared");
    std::fs::create_dir_all(live.join("private/nested")).expect("live tree");
    std::fs::create_dir_all(&prepared).expect("prepared tree");
    std::fs::write(live.join("old"), b"old").expect("old file");
    std::fs::write(live.join("private/nested/key"), b"secret").expect("excluded file");
    std::fs::write(prepared.join("new"), b"new").expect("new file");
    let journal = temporary.path().join("exchange.json");
    let outcome = publish_native_exchange(
        &journal,
        &live,
        &prepared,
        crate::IdempotencyKey::from_bytes([2; 16]),
        vec![PathBuf::from("private/nested")],
    )
    .expect("publish");
    assert!(outcome.published);
    assert_eq!(std::fs::read(live.join("new")).expect("new"), b"new");
    assert_eq!(
        std::fs::read(live.join("private/nested/key")).expect("excluded"),
        b"secret"
    );
    assert_eq!(std::fs::read(prepared.join("old")).expect("old"), b"old");
    assert!(!journal.exists());
}

#[test]
fn stale_completed_journal_does_not_suppress_the_next_publication() {
    let (temporary, live, prepared) = trees();
    std::fs::write(live.join("first"), b"first").expect("first live file");
    std::fs::write(prepared.join("second"), b"second").expect("first prepared file");
    let journal_path = temporary.path().join("exchange.json");
    let stale = fixture_journal(
        &live,
        &prepared,
        crate::IdempotencyKey::from_bytes([3; 16]),
        Vec::new(),
        Vec::new(),
        NativeExchangePhase::Exchanging,
    );
    write_journal(&journal_path, &stale).expect("stale journal");
    exchange_native_entries(
        &live,
        &prepared,
        &crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("test work"),
    )
    .expect("completed first exchange");

    let recovered = recover_native_exchange(&journal_path).expect("recover completed exchange");
    assert!(recovered.published);
    remove_entry(&prepared).expect("remove displaced tree");
    std::fs::create_dir(&prepared).expect("next prepared tree");
    std::fs::write(prepared.join("third"), b"third").expect("next prepared file");

    let outcome = publish_native_exchange(
        &journal_path,
        &live,
        &prepared,
        crate::IdempotencyKey::from_bytes([4; 16]),
        Vec::new(),
    )
    .expect("publish after stale completed journal");
    assert!(outcome.published);
    assert_eq!(std::fs::read(live.join("third")).expect("third"), b"third");
    assert_eq!(
        std::fs::read(prepared.join("second")).expect("second"),
        b"second"
    );
    assert!(!journal_path.exists());
}

#[test]
fn entry_exchange_preserves_both_complete_entries() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let left = temporary.path().join("left");
    let right = temporary.path().join("right");
    std::fs::create_dir(&left).expect("left tree");
    std::fs::create_dir(&right).expect("right tree");
    std::fs::write(left.join("value"), b"left").expect("left value");
    std::fs::write(right.join("value"), b"right").expect("right value");
    exchange_native_entries(
        &left,
        &right,
        &crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("test work"),
    )
    .expect("exchange entries");
    assert_eq!(std::fs::read(left.join("value")).expect("left"), b"right");
    assert_eq!(std::fs::read(right.join("value")).expect("right"), b"left");
}
