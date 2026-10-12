use super::*;
use acyclic_native_runtime::RenameMode::{NoReplace, Replace};

/// Persists the exact publication request.
pub fn prepare_native_exchange(
    journal_path: &Path,
    live: &Path,
    prepared: &Path,
    operation: crate::IdempotencyKey,
    carried: Vec<PathBuf>,
    recovery: Vec<u8>,
) -> Result<(), NativeExchangeError> {
    validate_layout(live, prepared)?;
    validate_carried_paths(&carried)?;
    let journal = NativeExchangeJournal {
        version: NATIVE_EXCHANGE_JOURNAL_VERSION,
        operation,
        recovery,
        live: live.to_path_buf(),
        prepared: prepared.to_path_buf(),
        carried,
        roots: [root_identity(live)?, root_identity(prepared)?],
        phase: NativeExchangePhase::Carrying,
    };
    match read_journal(journal_path) {
        Ok(existing) if existing == journal => Ok(()),
        Ok(_) => Err(NativeExchangeError::IncompatibleJournal),
        Err(NativeExchangeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            write_journal(journal_path, &journal)
        }
        Err(error) => Err(error),
    }
}

/// Publishes a prepared tree and carries excluded paths.
pub fn publish_native_exchange(
    journal_path: &Path,
    live: &Path,
    prepared: &Path,
    operation: crate::IdempotencyKey,
    carried: Vec<PathBuf>,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    validate_layout(live, prepared)?;
    if journal_path.exists() {
        let journal = read_journal(journal_path)?;
        if journal.live != live || journal.prepared != prepared {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
        if journal.operation != operation || journal.carried != carried {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
        let recovered = recover_native_exchange(journal_path)?;
        if recovered.published {
            return Ok(recovered);
        }
    }
    prepare_native_exchange(journal_path, live, prepared, operation, carried, Vec::new())?;
    let mut journal = read_journal(journal_path)?;
    if let Err(error) = carry(live, prepared, &journal.carried) {
        move_back(prepared, live, &journal.carried)?;
        remove_journal(journal_path)?;
        return Err(error);
    }
    journal.phase = NativeExchangePhase::Exchanging;
    write_journal(journal_path, &journal)?;
    if let Err(error) = exchange(live, prepared) {
        return match recover_native_exchange(journal_path) {
            Ok(outcome) if outcome.published => Ok(outcome),
            Ok(_) => Err(error),
            Err(recovery) => Err(recovery),
        };
    }
    remove_journal(journal_path)?;
    Ok(NativeExchangeOutcome {
        published: true,
        displaced: Some(prepared.to_path_buf()),
        recovery: journal.recovery,
    })
}

/// Recovers an interrupted tree publication.
pub fn recover_native_exchange(
    journal_path: &Path,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let journal = read_journal(journal_path)?;
    validate_layout(&journal.live, &journal.prepared)?;
    match journal.phase {
        NativeExchangePhase::Carrying => {
            if [
                root_identity(&journal.live)?,
                root_identity(&journal.prepared)?,
            ] != journal.roots
            {
                return Err(NativeExchangeError::IncompatibleJournal);
            }
            move_back(&journal.prepared, &journal.live, &journal.carried)?;
            remove_journal(journal_path)?;
            Ok(NativeExchangeOutcome {
                published: false,
                displaced: None,
                recovery: journal.recovery,
            })
        }
        NativeExchangePhase::Exchanging => recover_exchange(journal_path, &journal),
    }
}

pub(super) fn read_journal(path: &Path) -> Result<NativeExchangeJournal, NativeExchangeError> {
    let journal: NativeExchangeJournal =
        serde_json::from_slice(&std::fs::read(path)?).map_err(std::io::Error::other)?;
    match journal.version {
        NATIVE_EXCHANGE_JOURNAL_VERSION => {
            validate_carried_paths(&journal.carried)?;
            Ok(journal)
        }
        version => Err(NativeExchangeError::UnsupportedJournalVersion(version)),
    }
}

pub(super) fn validate_layout(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    if live == prepared || live.parent().is_none() || live.parent() != prepared.parent() {
        return Err(NativeExchangeError::InvalidLayout);
    }
    Ok(())
}

pub(super) fn validate_carried_paths(relative: &[PathBuf]) -> Result<(), NativeExchangeError> {
    for path in relative {
        if path.as_os_str().is_empty()
            || path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
    }
    Ok(())
}

pub(super) fn carry(
    from: &Path,
    into: &Path,
    relative: &[PathBuf],
) -> Result<(), NativeExchangeError> {
    for path in relative {
        let source = from.join(path);
        if !entry_exists(&source)? {
            continue;
        }
        let destination = into.join(path);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        remove_entry(&destination)?;
        std::fs::rename(source, destination)?;
    }
    Ok(())
}

pub(super) fn move_back(
    from: &Path,
    into: &Path,
    relative: &[PathBuf],
) -> Result<(), NativeExchangeError> {
    for path in relative {
        let source = from.join(path);
        if !entry_exists(&source)? {
            continue;
        }
        let destination = into.join(path);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        #[cfg(test)]
        BEFORE_ROLLBACK_RENAME.with(|hook| {
            if let Some(barrier) = hook.take() {
                barrier.wait();
                barrier.wait();
            }
        });
        match durable_rename(&source, &destination, NoReplace, None) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(NativeExchangeError::CarriedPathConflict(path.clone()));
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub(super) fn write_journal(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<(), NativeExchangeError> {
    use std::io::Write;
    let temporary = path.with_extension("next");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(journal).map_err(std::io::Error::other)?)?;
    file.sync_all()?;
    drop(file);
    durable_rename(&temporary, path, Replace, None)?;
    Ok(())
}

pub(super) fn remove_journal(path: &Path) -> Result<(), NativeExchangeError> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_parent(path).map_err(Into::into),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn entry_exists(path: &Path) -> Result<bool, std::io::Error> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub(super) fn root_identity(path: &Path) -> Result<[u8; 16], NativeExchangeError> {
    let root = cap_std::fs::Dir::open_ambient_dir(path, cap_std::ambient_authority())?;
    Ok(crate::NativeRootIdentity::from_metadata(&root.dir_metadata()?)?.to_bytes())
}

pub(super) fn remove_entry(path: &Path) -> Result<(), std::io::Error> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    }
}

pub(super) fn sync_parent(path: &Path) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("path has no parent"))?;
    acyclic_native_runtime::sync_parent(parent, acyclic_native_runtime::Durability::Full)
}

#[cfg(test)]
thread_local! {
    pub(super) static BEFORE_ROLLBACK_RENAME: std::cell::RefCell<Option<std::sync::Arc<std::sync::Barrier>>> = const { std::cell::RefCell::new(None) };
}

fn exchange(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| std::io::Error::other(error))?;
    let mut work = crate::demand::NativeSourceWork::from_native_materialization_work(&work);
    let (left, _) = crate::native_host::open_root_directory(
        live.parent().ok_or(NativeExchangeError::InvalidLayout)?,
        &mut work,
    )
    .map_err(std::io::Error::other)?;
    let (right, _) = crate::native_host::open_root_directory(
        prepared
            .parent()
            .ok_or(NativeExchangeError::InvalidLayout)?,
        &mut work,
    )
    .map_err(std::io::Error::other)?;
    exchange_native_entries(
        &left,
        Path::new(live.file_name().ok_or(NativeExchangeError::InvalidLayout)?),
        &right,
        Path::new(
            prepared
                .file_name()
                .ok_or(NativeExchangeError::InvalidLayout)?,
        ),
        &mut work,
    )
    .map_err(std::io::Error::other)?;
    Ok(())
}

fn recover_exchange(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let live = entry_exists(&journal.live)?
        .then(|| root_identity(&journal.live))
        .transpose()?;
    let prepared = entry_exists(&journal.prepared)?
        .then(|| root_identity(&journal.prepared))
        .transpose()?;
    #[cfg(windows)]
    let scratch = scratch(&journal.live)?;
    #[cfg(windows)]
    let saved = entry_exists(&scratch)?
        .then(|| root_identity(&scratch))
        .transpose()?;
    #[cfg(not(windows))]
    let saved: Option<[u8; 16]> = None;
    let [original, replacement] = journal.roots;
    let (published, displaced) = match (live, prepared, saved) {
        (Some(live), Some(prepared), None) if [live, prepared] == [original, replacement] => {
            move_back(&journal.prepared, &journal.live, &journal.carried)?;
            (false, None)
        }
        (Some(live), Some(prepared), None) if [live, prepared] == [replacement, original] => {
            (true, Some(journal.prepared.clone()))
        }
        (Some(live), _, None) if live == replacement => (true, None),
        #[cfg(windows)]
        (None, Some(prepared), Some(saved)) if [saved, prepared] == [original, replacement] => {
            move_back(&journal.prepared, &scratch, &journal.carried)?;
            durable_rename(&scratch, &journal.live, NoReplace, None)?;
            (false, None)
        }
        #[cfg(windows)]
        (Some(live), None, Some(saved)) if [saved, live] == [original, replacement] => {
            durable_rename(&scratch, &journal.prepared, NoReplace, None)?;
            (true, Some(journal.prepared.clone()))
        }
        _ => return Err(NativeExchangeError::IncompatibleJournal),
    };
    #[cfg(windows)]
    if entry_exists(&entry_exchange_witness(&journal.live)?)? {
        let work = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| std::io::Error::other(error))?;
        let mut work = crate::demand::NativeSourceWork::from_native_materialization_work(&work);
        let (parent, _) = crate::native_host::open_root_directory(
            journal
                .live
                .parent()
                .ok_or(NativeExchangeError::InvalidLayout)?,
            &mut work,
        )
        .map_err(std::io::Error::other)?;
        recover_native_entry_exchange(
            &parent,
            Path::new(
                journal
                    .live
                    .file_name()
                    .ok_or(NativeExchangeError::InvalidLayout)?,
            ),
            &mut work,
        )
        .map_err(std::io::Error::other)?;
    }
    remove_journal(path)?;
    Ok(NativeExchangeOutcome {
        published,
        displaced,
        recovery: journal.recovery.clone(),
    })
}
