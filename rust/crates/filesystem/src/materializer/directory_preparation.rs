use super::*;
use crate::native_host::HostRoot;

async fn persist_preparation(
    store: &crate::LocalCoreStateStore,
    mut journal: MaterializationJournal,
) -> Result<MaterializationJournal, NativeWorkspacePublicationError> {
    let expected = next_revision(&mut journal.revision);
    let (stored, journal) = store
        .compare_and_swap(journal.plan.operation_id, expected, journal)
        .await?;
    if !stored {
        return Err(NativeWorkspacePublicationError::MismatchedJournal);
    }
    Ok(journal)
}

impl NativeTreeMaterializationBackend {
    pub(super) async fn prepare_allocations<F, Fut>(
        store: &crate::LocalCoreStateStore,
        mut journal: MaterializationJournal,
        root: &Path,
        operation: &Path,
        work: &NativeMaterializationWork,
        before_effect: &mut F,
    ) -> Result<MaterializationJournal, NativeWorkspacePublicationError>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), NativeWorkspacePublicationError>> + Send,
    {
        let progress = journal
            .preparation_progress
            .as_mut()
            .expect("validated preparation");
        let resuming = progress.pending;
        let parent_identity = progress.parent;
        if !resuming {
            before_effect().await?;
            progress.pending = true;
            journal = persist_preparation(store, journal).await?;
        }
        let expected_root = crate::NativeRootIdentity::from_bytes(
            journal.root_identity.expect("validated checkout identity"),
        );
        let completion = store.prepare_action(journal).await?;
        let actual = work.root_identity(root)?;
        if actual != expected_root {
            return Err(NativeTreeMaterializationError::AllocationChanged {
                expected: expected_root,
                actual,
            }
            .into());
        }
        let parent = HostRoot::open(
            operation
                .parent()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
            &mut &*work,
        )?;
        let expected = crate::NativeRootIdentity::from_bytes(parent_identity);
        if parent.identity() != expected {
            return Err(NativeTreeMaterializationError::AllocationChanged {
                expected,
                actual: parent.identity(),
            }
            .into());
        }
        let leaf = Path::new(
            operation
                .file_name()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        );
        let mut identities = [[0; 16]; 3];
        let mut parent = parent;
        for (index, leaf) in [leaf, Path::new("target"), Path::new("backup")]
            .into_iter()
            .enumerate()
        {
            before_effect().await?;
            let created = work.with_original_leaf(|state| {
                host_path(leaf, state, |path, work| parent.create_dir(path, work))
            });
            match created {
                Ok(()) => (),
                Err(crate::demand::DemandError::Io(error))
                    if resuming && error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(native_materialization_work_error(error).into()),
            }
            crate::native_host::sync_directory_held(&parent.directory, &mut &*work)?;
            let child = held_child(&parent, leaf, work)?;
            require_preparation_contents(&child, index == 0, work)?;
            identities[index] = child.identity().to_bytes();
            if index == 0 {
                parent = child;
            }
        }
        journal = completion.await?;
        journal.preparation_identity = Some(MaterializationPreparationIdentity {
            operation: identities[0],
            target: identities[1],
            backup: identities[2],
        });
        journal.preparation_progress = None;
        persist_preparation(store, journal).await
    }
}

// The original journal's exclusive record lock covers this check and all mkdirs.
// Before preparation is acknowledged, only the two empty staging roots may exist.
fn require_preparation_contents(
    root: &HostRoot,
    operation: bool,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let (mut reader, _cursor) = host_path(Path::new(""), &mut &*work, |path, work| {
        root.read_dir_stats(path, work)
    })?;
    while let Some((entry, _allocation)) = reader.next(&mut &*work)? {
        if !operation || !(entry.is_named("target") || entry.is_named("backup")) {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod crash_tests;
#[cfg(test)]
mod tests;

/// Creates shared staging ancestors through held directories before journaling the operation.
pub(super) async fn prepare_publication_parent<F, Fut>(
    operation: &Path,
    work: &NativeMaterializationWork,
    before_effect: &mut F,
) -> Result<HostRoot, NativeWorkspacePublicationError>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<(), NativeWorkspacePublicationError>> + Send,
{
    let requested = operation
        .parent()
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    let mut existing = requested;
    let mut parent = loop {
        match HostRoot::open(existing, &mut &*work) {
            Ok(parent) => break parent,
            Err(NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                existing = existing.parent().ok_or(error)?;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let suffix = requested
        .strip_prefix(existing)
        .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
    for component in suffix.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(NativeTreeMaterializationError::InvalidLayout.into());
        };
        let leaf = Path::new(name);
        before_effect().await?;
        let created = work.with_original_leaf(|state| {
            host_path(leaf, state, |path, work| parent.create_dir(path, work))
        });
        match created {
            Ok(()) => crate::native_host::sync_directory_held(&parent.directory, &mut &*work)?,
            Err(crate::demand::DemandError::Io(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(native_materialization_work_error(error).into()),
        }
        parent = held_child(&parent, leaf, work)?;
    }
    Ok(parent)
}
