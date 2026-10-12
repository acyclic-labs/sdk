use super::*;
use crate::native_host::HostRoot;

async fn persist_preparation<S: MaterializationJournalStore>(
    store: &S,
    mut journal: MaterializationJournal,
    work: &NativeMaterializationWork,
) -> Result<MaterializationJournal, MaterializationError<S::Error, NativeTreeMaterializationError>>
{
    journal.work = work.journal_work()?;
    let expected = next_revision(&mut journal.revision);
    let (stored, journal) = store
        .compare_and_swap(journal.plan.operation_id, expected, journal)
        .await
        .map_err(MaterializationError::Store)?;
    if !stored {
        return Err(MaterializationError::Contended);
    }
    Ok(journal)
}

impl NativeTreeMaterializationBackend {
    pub(super) async fn prepare_allocations<S, F, Fut, E>(
        store: &S,
        mut journal: MaterializationJournal,
        root: &Path,
        operation: &Path,
        work: &NativeMaterializationWork,
        before_effect: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        S: MaterializationJournalStore,
        E: From<MaterializationError<S::Error, NativeTreeMaterializationError>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let backend = MaterializationError::<S::Error, NativeTreeMaterializationError>::Backend;
        let invalid =
            MaterializationError::<S::Error, NativeTreeMaterializationError>::IncompatibleJournal;
        if !journal.is_valid_for(journal.plan.operation_id) {
            return Err(invalid.into());
        }
        check_native_plan(&journal.plan, work).map_err(backend)?;
        let expected_root = crate::NativeRootIdentity::from_bytes(journal.root_identity.ok_or(
            MaterializationError::<S::Error, NativeTreeMaterializationError>::IncompatibleJournal,
        )?);
        let mut progress = journal.preparation_progress.ok_or(
            MaterializationError::<S::Error, NativeTreeMaterializationError>::IncompatibleJournal,
        )?;
        if progress.pending.is_some() {
            return Err(backend(NativeTreeMaterializationError::IndeterminatePreparation).into());
        }
        use MaterializationPreparationAllocation::{Backup, Operation, Target};
        for step in [Operation, Target, Backup] {
            let known = match step {
                Operation => progress.operation,
                Target => progress.target,
                Backup => progress.backup,
            };
            if known.is_some() {
                continue;
            }
            before_effect().await?;
            preparation_parent(root, expected_root, operation, progress, work).map_err(backend)?;
            if store
                .load(journal.plan.operation_id)
                .await
                .map_err(MaterializationError::<S::Error, NativeTreeMaterializationError>::Store)?
                .as_ref()
                != Some(&journal)
            {
                return Err(
                    MaterializationError::<S::Error, NativeTreeMaterializationError>::PlanConflict
                        .into(),
                );
            }
            progress.pending = Some(step);
            journal.preparation_progress = Some(progress);
            journal = persist_preparation(store, journal, work).await?;
            if store
                .load(journal.plan.operation_id)
                .await
                .map_err(MaterializationError::<S::Error, NativeTreeMaterializationError>::Store)?
                .as_ref()
                != Some(&journal)
            {
                return Err(
                    MaterializationError::<S::Error, NativeTreeMaterializationError>::PlanConflict
                        .into(),
                );
            }
            let parent = preparation_parent(root, expected_root, operation, progress, work)
                .map_err(backend)?;
            let leaf = match step {
                Operation => Path::new(
                    operation
                        .file_name()
                        .ok_or_else(|| backend(NativeTreeMaterializationError::InvalidLayout))?,
                ),
                Target => Path::new("target"),
                Backup => Path::new("backup"),
            };
            work.with_original_leaf(|state| parent.create_dir(leaf, state))
                .map_err(native_materialization_work_error)
                .map_err(backend)?;
            crate::native_host::sync_directory_held(&parent.directory, &mut &*work)
                .map_err(backend)?;
            let identity = held_child(&parent, leaf, work)
                .map_err(backend)?
                .identity()
                .to_bytes();
            match step {
                Operation => progress.operation = Some(identity),
                Target => progress.target = Some(identity),
                Backup => progress.backup = Some(identity),
            }
            progress.pending = None;
            journal.preparation_progress = Some(progress);
            journal = persist_preparation(store, journal, work).await?;
            before_effect().await?;
            preparation_parent(root, expected_root, operation, progress, work).map_err(backend)?;
        }
        journal.preparation_identity = Some(MaterializationPreparationIdentity {
            operation: progress
                .operation
                .ok_or_else(|| backend(NativeTreeMaterializationError::InvalidLayout))?,
            target: progress
                .target
                .ok_or_else(|| backend(NativeTreeMaterializationError::InvalidLayout))?,
            backup: progress
                .backup
                .ok_or_else(|| backend(NativeTreeMaterializationError::InvalidLayout))?,
        });
        journal.preparation_progress = None;
        persist_preparation(store, journal, work)
            .await
            .map_err(Into::into)
    }
}

fn preparation_parent(
    root: &Path,
    expected_root: crate::NativeRootIdentity,
    operation: &Path,
    progress: MaterializationPreparationProgress,
    work: &NativeMaterializationWork,
) -> Result<HostRoot, NativeTreeMaterializationError> {
    let actual = work.root_identity(root)?;
    if actual != expected_root {
        return Err(NativeTreeMaterializationError::AllocationChanged {
            expected: expected_root,
            actual,
        });
    }
    let parent = HostRoot::open(
        operation
            .parent()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        &mut &*work,
    )?;
    require_identity(&parent, progress.parent)?;
    let Some(identity) = progress.operation else {
        return Ok(parent);
    };
    let operation = held_child(
        &parent,
        Path::new(
            operation
                .file_name()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        ),
        work,
    )?;
    require_identity(&operation, identity)?;
    for (name, identity) in [("target", progress.target), ("backup", progress.backup)] {
        if let Some(identity) = identity {
            require_identity(&held_child(&operation, Path::new(name), work)?, identity)?;
        }
    }
    Ok(operation)
}

fn require_identity(
    root: &HostRoot,
    identity: [u8; 16],
) -> Result<(), NativeTreeMaterializationError> {
    let expected = crate::NativeRootIdentity::from_bytes(identity);
    let actual = root.identity();
    if expected != actual {
        return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual });
    }
    Ok(())
}
