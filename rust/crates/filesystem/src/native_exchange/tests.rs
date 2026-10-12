use super::*;
use crate::demand::NativeSourceWork;
use crate::{CancellationToken, NativeMaterializationWork, WorkBudget, WorkCounters};
use std::{fs, sync::Arc};
type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Fixture {
    temporary: tempfile::TempDir,
    live: PathBuf,
    prepared: PathBuf,
    journal: PathBuf,
}
impl Fixture {
    fn new(directory: bool) -> std::io::Result<Self> {
        let temporary = tempfile::tempdir()?;
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        let journal = temporary.path().join("exchange.json");
        if directory {
            fs::create_dir(&live)?;
            fs::create_dir(&prepared)?;
            fs::write(live.join("old"), b"old")?;
            fs::write(prepared.join("new"), b"new")?;
        } else {
            fs::write(&live, b"before")?;
            fs::write(&prepared, b"after")?;
        }
        Ok(Self {
            temporary,
            live,
            prepared,
            journal,
        })
    }
    fn prepare(&self, carried: Vec<PathBuf>) -> Result<NativeExchangeJournal, NativeExchangeError> {
        prepare_native_exchange(
            &self.journal,
            &self.live,
            &self.prepared,
            crate::IdempotencyKey::from_bytes([1; 16]),
            carried,
            vec![7; 16],
        )?;
        read_journal(&self.journal)
    }
    fn entries<T>(
        &self,
        budget: WorkBudget,
        token: &CancellationToken,
        action: impl FnOnce(
            &cap_std::fs::Dir,
            &Path,
            &cap_std::fs::Dir,
            &Path,
            &mut NativeSourceWork,
        ) -> Result<T, crate::demand::DemandError>,
    ) -> Result<T, Box<dyn std::error::Error>> {
        let original = NativeMaterializationWork::new(budget, token).map_err(|(error, _)| error)?;
        let mut work = NativeSourceWork::from_native_materialization_work(&original);
        let (left, _) =
            crate::native_host::open_root_directory(self.live.parent().unwrap(), &mut work)?;
        let (right, _) =
            crate::native_host::open_root_directory(self.prepared.parent().unwrap(), &mut work)?;
        Ok(action(
            &left,
            Path::new(self.live.file_name().unwrap()),
            &right,
            Path::new(self.prepared.file_name().unwrap()),
            &mut work,
        )?)
    }
    fn exchange(&self, budget: WorkBudget, token: &CancellationToken) -> TestResult {
        self.entries(budget, token, |left, name, right, right_name, work| {
            exchange_native_entries(left, name, right, right_name, work)
        })
    }
    #[cfg(windows)]
    fn recover(&self) -> TestResult {
        self.entries(
            finite_budget(),
            &CancellationToken::new(),
            |left, name, _, _, work| recover_native_entry_exchange(left, name, work),
        )
    }
    #[cfg(windows)]
    fn witness(&self) -> TestResult {
        self.entries(
            finite_budget(),
            &CancellationToken::new(),
            |left, name, right, right_name, work| {
                let roots = [
                    crate::native_host::entry_identity(left, name, work)?.unwrap(),
                    crate::native_host::entry_identity(right, right_name, work)?.unwrap(),
                ];
                let witness = entry_exchange_witness(&self.live)?;
                create_entry_witness(
                    left,
                    name,
                    right,
                    right_name,
                    Path::new(witness.file_name().unwrap()),
                    roots,
                    work,
                )?;
                Ok(())
            },
        )
    }
}
fn finite_budget() -> WorkBudget {
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

#[test]
fn publication_and_interrupted_rollback_preserve_complete_trees() -> TestResult {
    let f = Fixture::new(true)?;
    fs::create_dir_all(f.live.join("private/nested"))?;
    fs::write(f.live.join("private/nested/key"), b"secret")?;
    let journal = f.prepare(vec!["private/nested".into()])?;
    f.prepare(journal.carried.clone())?;
    let published = publish_native_exchange(
        &f.journal,
        &f.live,
        &f.prepared,
        journal.operation,
        journal.carried,
    )?;
    assert!(published.published);
    assert_eq!(fs::read(f.live.join("new"))?, b"new");
    assert_eq!(fs::read(f.live.join("private/nested/key"))?, b"secret");
    assert_eq!(fs::read(f.prepared.join("old"))?, b"old");
    let journal = f.prepare(vec!["new".into(), "private/nested".into()])?;
    carry(&f.live, &f.prepared, &journal.carried)?;
    fs::create_dir_all(f.live.join("private/nested"))?;
    fs::write(f.live.join("private/nested/key"), b"user")?;
    assert!(matches!(
        recover_native_exchange(&f.journal),
        Err(NativeExchangeError::CarriedPathConflict(_))
    ));
    assert_eq!(fs::read(f.live.join("private/nested/key"))?, b"user");
    assert_eq!(fs::read(f.prepared.join("private/nested/key"))?, b"secret");
    fs::write(f.live.join("new"), b"user-new")?;
    fs::remove_dir_all(f.live.join("private"))?;
    assert!(!recover_native_exchange(&f.journal)?.published);
    assert_eq!(fs::read(f.live.join("private/nested/key"))?, b"secret");
    assert_eq!(fs::read(f.live.join("new"))?, b"user-new");
    Ok(())
}

#[test]
fn rollback_never_replaces_a_concurrent_binding() -> TestResult {
    let f = Fixture::new(true)?;
    fs::write(f.live.join("private"), b"original")?;
    let journal = f.prepare(vec!["private".into()])?;
    carry(&f.live, &f.prepared, &journal.carried)?;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    BEFORE_ROLLBACK_RENAME.with(|hook| *hook.borrow_mut() = Some(Arc::clone(&barrier)));
    let live = f.live.clone();
    let changed = std::thread::spawn(move || {
        barrier.wait();
        fs::write(live.join("private"), b"user").expect("concurrent binding");
        barrier.wait();
    });
    assert!(matches!(
        recover_native_exchange(&f.journal),
        Err(NativeExchangeError::CarriedPathConflict(_))
    ));
    changed.join().expect("concurrent writer");
    assert_eq!(fs::read(f.live.join("private"))?, b"user");
    assert_eq!(fs::read(f.prepared.join("private"))?, b"original");
    Ok(())
}

#[test]
fn journal_rejections_preserve_the_original_request_and_roots() -> TestResult {
    let f = Fixture::new(true)?;
    let original = f.prepare(Vec::new())?;
    for version in [0, 2, 3, u32::MAX] {
        let mut invalid = original.clone();
        invalid.version = version;
        write_journal(&f.journal, &invalid)?;
        assert!(
            matches!(recover_native_exchange(&f.journal), Err(NativeExchangeError::UnsupportedJournalVersion(v)) if v == version)
        );
    }
    let mut invalid_paths = vec![
        PathBuf::new(),
        ".".into(),
        "../outside".into(),
        "nested/../../outside".into(),
        f.live.clone(),
    ];
    #[cfg(windows)]
    invalid_paths.extend(["C:relative".into(), "\rooted".into()]);
    for path in invalid_paths {
        let mut invalid = original.clone();
        invalid.carried = vec![path];
        write_journal(&f.journal, &invalid)?;
        assert!(matches!(
            recover_native_exchange(&f.journal),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        remove_journal(&f.journal)?;
        assert!(
            prepare_native_exchange(
                &f.journal,
                &f.live,
                &f.prepared,
                original.operation,
                invalid.carried,
                vec![7; 16]
            )
            .is_err()
        );
        assert!(!f.journal.exists());
    }
    write_journal(&f.journal, &original)?;
    let other = Fixture::new(true)?;
    assert!(
        publish_native_exchange(
            &f.journal,
            &other.live,
            &other.prepared,
            original.operation,
            Vec::new()
        )
        .is_err()
    );
    assert!(
        prepare_native_exchange(
            &f.journal,
            &f.live,
            &f.prepared,
            crate::IdempotencyKey::from_bytes([2; 16]),
            Vec::new(),
            Vec::new()
        )
        .is_err()
    );
    for replaced in [&f.live, &f.prepared] {
        let saved = f.temporary.path().join("saved");
        fs::rename(replaced, &saved)?;
        fs::create_dir(replaced)?;
        assert!(matches!(
            recover_native_exchange(&f.journal),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        fs::remove_dir(replaced)?;
        fs::rename(saved, replaced)?;
    }
    assert_eq!(fs::read(f.live.join("old"))?, b"old");
    assert_eq!(fs::read(f.prepared.join("new"))?, b"new");
    Ok(())
}

#[test]
fn completed_publication_recovers_then_allows_the_next_request() -> TestResult {
    let f = Fixture::new(true)?;
    let mut journal = f.prepare(Vec::new())?;
    journal.phase = NativeExchangePhase::Exchanging;
    write_journal(&f.journal, &journal)?;
    f.exchange(finite_budget(), &CancellationToken::new())?;
    let recovered = publish_native_exchange(
        &f.journal,
        &f.live,
        &f.prepared,
        journal.operation,
        Vec::new(),
    )?;
    assert!(recovered.published);
    assert_eq!(recovered.recovery, journal.recovery);
    assert_eq!(fs::read(f.prepared.join("old"))?, b"old");
    fs::remove_dir_all(&f.prepared)?;
    fs::create_dir(&f.prepared)?;
    fs::write(f.prepared.join("third"), b"third")?;
    assert!(
        publish_native_exchange(
            &f.journal,
            &f.live,
            &f.prepared,
            crate::IdempotencyKey::from_bytes([2; 16]),
            Vec::new()
        )?
        .published
    );
    assert_eq!(fs::read(f.live.join("third"))?, b"third");
    assert_eq!(fs::read(f.prepared.join("new"))?, b"new");
    Ok(())
}

#[test]
fn entry_limits_and_cancellation_precede_edits() -> TestResult {
    let f = Fixture::new(false)?;
    let token = CancellationToken::new();
    assert!(f.exchange(WorkBudget::default(), &token).is_err());
    token.cancel();
    assert!(f.exchange(finite_budget(), &token).is_err());
    assert_eq!(fs::read(&f.live)?, b"before");
    assert_eq!(fs::read(&f.prepared)?, b"after");
    #[cfg(windows)]
    for deny_bytes in [true, false] {
        let mut budget = finite_budget();
        if deny_bytes {
            budget.output_bytes = (ENTRY_WITNESS.len() - 1) as u64;
        } else {
            budget.durability_operations = 0;
        }
        assert!(f.exchange(budget, &CancellationToken::new()).is_err());
        assert!(!entry_exchange_witness(&f.live)?.exists());
        assert!(!scratch(&f.live)?.exists());
        assert_eq!(fs::read(&f.live)?, b"before");
        assert_eq!(fs::read(&f.prepared)?, b"after");
    }
    f.exchange(finite_budget(), &CancellationToken::new())?;
    assert_eq!(fs::read(&f.live)?, b"after");
    assert_eq!(fs::read(&f.prepared)?, b"before");
    Ok(())
}

#[test]
fn held_entries_keep_the_original_parent_and_committed_prefix() -> TestResult {
    let f = Fixture::new(false)?;
    #[cfg(windows)]
    f.witness()?;
    let original = NativeMaterializationWork::new(finite_budget(), &CancellationToken::new())
        .map_err(|(error, _)| error)?;
    let mut work = NativeSourceWork::from_native_materialization_work(&original);
    let (parent, _) = crate::native_host::open_root_directory(
        f.live.parent().ok_or(NativeExchangeError::InvalidLayout)?,
        &mut work,
    )?;
    let saved = f.temporary.path().with_extension("saved");
    fs::rename(f.temporary.path(), &saved)?;
    fs::create_dir(f.temporary.path())?;
    fs::write(&f.live, b"user-live")?;
    fs::write(&f.prepared, b"user-prepared")?;
    #[cfg(unix)]
    {
        exchange_native_entries(
            &parent,
            Path::new("live"),
            &parent,
            Path::new("prepared"),
            &mut work,
        )?;
        assert_eq!(fs::read(saved.join("live"))?, b"after");
    }
    #[cfg(windows)]
    {
        assert!(recover_native_entry_exchange(&parent, Path::new("live"), &mut work).is_err());
        assert_eq!(fs::read(saved.join("live"))?, b"before");
        assert!(
            saved
                .join(entry_exchange_witness(&f.live)?.file_name().unwrap())
                .exists()
        );
    }
    assert_eq!(fs::read(&f.live)?, b"user-live");
    assert_eq!(fs::read(&f.prepared)?, b"user-prepared");
    fs::remove_dir_all(saved)?;
    #[cfg(unix)]
    {
        let f = Fixture::new(false)?;
        let mut budget = finite_budget();
        budget.durability_operations = 0;
        assert!(f.exchange(budget, &CancellationToken::new()).is_err());
        assert_eq!(fs::read(&f.live)?, b"after");
        assert_eq!(fs::read(&f.prepared)?, b"before");
    }
    Ok(())
}

#[cfg(windows)]
#[test]
fn entry_recovery_preserves_preimages_requests_and_both_rename_prefixes() -> TestResult {
    for published in [false, true] {
        let mut f = Fixture::new(false)?;
        let right = f.temporary.path().join("other-parent");
        fs::create_dir(&right)?;
        fs::rename(&f.prepared, right.join("prepared"))?;
        f.prepared = right.join("prepared");
        f.witness()?;
        let scratch = scratch(&f.live)?;
        fs::rename(&f.live, &scratch)?;
        if published {
            fs::rename(&f.prepared, &f.live)?;
        }
        f.recover()?;
        assert_eq!(
            fs::read(&f.live)?,
            if published {
                b"after".as_slice()
            } else {
                b"before".as_slice()
            }
        );
        assert_eq!(
            fs::read(&f.prepared)?,
            if published {
                b"before".as_slice()
            } else {
                b"after".as_slice()
            }
        );
        assert!(!scratch.exists());
        assert!(!entry_exchange_witness(&f.live)?.exists());
        f.recover()?;
    }
    for (replacement, completed) in [(0, false), (1, false), (0, true)] {
        let f = Fixture::new(false)?;
        f.witness()?;
        if completed {
            fs::rename(&f.live, scratch(&f.live)?)?;
            fs::rename(&f.prepared, &f.live)?;
        }
        let saved = f.temporary.path().join("saved");
        let path = if replacement == 0 {
            &f.live
        } else {
            &f.prepared
        };
        fs::rename(path, &saved)?;
        fs::write(path, b"user")?;
        assert!(f.recover().is_err());
        assert_eq!(fs::read(path)?, b"user");
        assert_eq!(
            fs::read(saved)?,
            if replacement == 0 && !completed {
                b"before".as_slice()
            } else {
                b"after".as_slice()
            }
        );
        assert!(entry_exchange_witness(&f.live)?.exists());
        if completed {
            assert_eq!(fs::read(scratch(&f.live)?)?, b"before");
        }
    }
    let f = Fixture::new(false)?;
    f.witness()?;
    let saved = scratch(&f.live)?;
    fs::rename(&f.live, &saved)?;
    fs::rename(&f.prepared, &f.live)?;
    fs::rename(saved, &f.prepared)?;
    f.exchange(finite_budget(), &CancellationToken::new())?;
    assert_eq!(fs::read(&f.live)?, b"after");
    assert_eq!(fs::read(&f.prepared)?, b"before");
    let mut f = Fixture::new(false)?;
    f.witness()?;
    let other = f.temporary.path().join("other");
    fs::write(&other, b"other")?;
    let original = f.prepared.clone();
    f.prepared = other;
    assert!(
        f.exchange(finite_budget(), &CancellationToken::new())
            .is_err()
    );
    f.prepared = original;
    f.exchange(finite_budget(), &CancellationToken::new())?;
    assert_eq!(fs::read(&f.live)?, b"after");
    let f = Fixture::new(false)?;
    fs::write(scratch(&f.live)?, b"unowned")?;
    assert!(f.recover().is_err());
    assert_eq!(fs::read(scratch(&f.live)?)?, b"unowned");
    Ok(())
}

#[cfg(windows)]
#[test]
fn witness_bounds_trailing_data_and_raw_names_are_preserved() -> TestResult {
    for malformed in [0, 1, 2] {
        let f = Fixture::new(false)?;
        f.witness()?;
        let witness = entry_exchange_witness(&f.live)?;
        let mut bytes = fs::read(&witness)?;
        if malformed == 0 {
            bytes[96..104].copy_from_slice(&u64::MAX.to_le_bytes());
        } else if malformed == 1 {
            bytes.push(1);
        } else {
            fs::remove_file(&witness)?;
            bytes = b"replacement witness".to_vec();
        }
        fs::write(&witness, &bytes)?;
        assert!(f.recover().is_err());
        assert_eq!(fs::read(&f.live)?, b"before");
        assert_eq!(fs::read(&f.prepared)?, b"after");
        assert_eq!(fs::read(witness)?, bytes);
    }
    use std::os::windows::ffi::OsStringExt;
    let mut f = Fixture::new(false)?;
    let live = f
        .temporary
        .path()
        .join(std::ffi::OsString::from_wide(&[0xd800, 0x61]));
    let prepared = f
        .temporary
        .path()
        .join(std::ffi::OsString::from_wide(&[0xd801, 0x62]));
    fs::rename(&f.live, &live)?;
    fs::rename(&f.prepared, &prepared)?;
    f.live = live;
    f.prepared = prepared;
    f.exchange(finite_budget(), &CancellationToken::new())?;
    assert_eq!(fs::read(&f.live)?, b"after");
    assert_eq!(fs::read(&f.prepared)?, b"before");
    Ok(())
}
