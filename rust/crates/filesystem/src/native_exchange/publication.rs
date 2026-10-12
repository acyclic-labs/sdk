use super::*;

/// Exchanges a prepared sibling tree with a live root.
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
    if let Err(error) = exchange_native_entries(live, prepared, &crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map_err(|(error, _)| NativeExchangeError::from(error))?) {
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
/// Recovers one interrupted whole-tree publication.
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
pub(super) fn carry(from: &Path, into: &Path, relative: &[PathBuf]) -> Result<(), NativeExchangeError> {
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
pub(super) fn move_back(from: &Path, into: &Path, relative: &[PathBuf]) -> Result<(), NativeExchangeError> {
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
        match durable_rename(
            &source,
            &destination,
            acyclic_native_runtime::RenameMode::NoReplace, None,
        ) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(NativeExchangeError::CarriedPathConflict(path.clone()));
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
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
    // Schedule a real user binding at the rollback's final publication boundary.
    pub(super) static BEFORE_ROLLBACK_RENAME: std::cell::RefCell<Option<std::sync::Arc<std::sync::Barrier>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(windows)]
pub(super) fn recover_exchange(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let scratch = scratch(&journal.live)?;
    let live = entry_exists(&journal.live)?
        .then(|| root_identity(&journal.live))
        .transpose()?;
    let prepared = entry_exists(&journal.prepared)?
        .then(|| root_identity(&journal.prepared))
        .transpose()?;
    let scratch_identity = entry_exists(&scratch)?
        .then(|| root_identity(&scratch))
        .transpose()?;
    let original = journal.roots[0];
    let replacement = journal.roots[1];
    let displaced = if live == Some(original)
        && prepared == Some(replacement)
        && scratch_identity.is_none()
    {
        move_back(&journal.prepared, &journal.live, &journal.carried)?;
        None
    } else if live.is_none() && prepared == Some(replacement) && scratch_identity == Some(original)
    {
        move_back(&journal.prepared, &scratch, &journal.carried)?;
        durable_rename(
            &scratch,
            &journal.live,
            acyclic_native_runtime::RenameMode::NoReplace, None,
        )?;
        None
    } else if live == Some(replacement) && prepared.is_none() && scratch_identity == Some(original)
    {
        durable_rename(
            &scratch,
            &journal.prepared,
            acyclic_native_runtime::RenameMode::NoReplace, None,
        )?;
        Some(journal.prepared.clone())
    } else if live == Some(replacement) && prepared == Some(original) && scratch_identity.is_none()
    {
        Some(journal.prepared.clone())
    } else if live == Some(replacement) && scratch_identity.is_none() {
        None
    } else {
        return Err(NativeExchangeError::IncompatibleJournal);
    };
    if entry_exists(&entry_exchange_witness(&journal.live)?)? {
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map_err(|(error, _)| NativeExchangeError::from(error))?;
        recover_native_entry_exchange(&journal.live, &work)?;
    }
    remove_journal(path)?;
    Ok(NativeExchangeOutcome {
        published: live == Some(replacement),
        displaced,
        recovery: journal.recovery.clone(),
    })
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn recover_exchange(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let live = entry_exists(&journal.live)?
        .then(|| root_identity(&journal.live))
        .transpose()?;
    let prepared = entry_exists(&journal.prepared)?
        .then(|| root_identity(&journal.prepared))
        .transpose()?;
    let (published, displaced) = match (live, prepared) {
        (Some(live), Some(prepared)) if [live, prepared] == journal.roots => {
            move_back(&journal.prepared, &journal.live, &journal.carried)?;
            (false, None)
        }
        (Some(live), Some(prepared))
            if [live, prepared] == [journal.roots[1], journal.roots[0]] =>
        {
            (true, Some(journal.prepared.clone()))
        }
        (Some(live), _) if live == journal.roots[1] => (true, None),
        _ => return Err(NativeExchangeError::IncompatibleJournal),
    };
    remove_journal(path)?;
    Ok(NativeExchangeOutcome {
        published,
        displaced,
        recovery: journal.recovery.clone(),
    })
}
