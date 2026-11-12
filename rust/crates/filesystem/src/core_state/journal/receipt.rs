use super::*;

/// Only this fixed-size receipt changes. Intent and acknowledged preimages
/// are published once, before the receipt that makes them reachable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Receipt {
    version: u32,
    pub(super) revision: u64,
    root_identity: Option<[u8; 16]>,
    preparation_identity: Option<crate::MaterializationPreparationIdentity>,
    preparation_progress: Option<crate::MaterializationPreparationProgress>,
    pub(super) work: crate::WorkCounters,
    preimages: usize,
    pub(super) phase: crate::MaterializationPhase,
    applied: u32,
    restored: u32,
}

impl From<&MaterializationJournal> for Receipt {
    fn from(journal: &MaterializationJournal) -> Self {
        Self {
            version: journal.version,
            revision: journal.revision,
            root_identity: journal.root_identity,
            preparation_identity: journal.preparation_identity,
            preparation_progress: journal.preparation_progress,
            work: journal.work,
            preimages: journal.preimages.len(),
            phase: journal.phase,
            applied: journal.applied,
            restored: journal.restored,
        }
    }
}

impl Receipt {
    pub(super) fn version(&self) -> u32 {
        self.version
    }
}

fn data_paths<'a>(
    namespace: &'a Namespace,
    operation: OperationId,
    index: Option<usize>,
) -> Result<RecordPaths<'a>, LocalCoreStateStoreError> {
    let mut name = [0; 32];
    hex::encode_to_slice(operation.into_bytes(), &mut name).map_err(std::io::Error::other)?;
    let name = std::str::from_utf8(&name).map_err(std::io::Error::other)?;
    let (family, _custody) = match &namespace._allocation {
        Some(allocation) => {
            let (family, custody) = allocation
                .work()
                .display(&format_args!("materialization-data-v1/{name}"))?;
            (family, Some(custody))
        }
        None => (format!("materialization-data-v1/{name}"), None),
    };
    let mut key = [0; 9];
    let key = if let Some(index) = index {
        key[0] = 1;
        key[1..].copy_from_slice(
            &u64::try_from(index)
                .map_err(|_| LocalCoreStateStoreError::Integrity)?
                .to_be_bytes(),
        );
        key.as_slice()
    } else {
        &key[..1]
    };
    namespace.record(&family, key)
}
pub(super) fn publish(
    namespace: &Namespace,
    current: Option<&Receipt>,
    journal: &MaterializationJournal,
) -> Result<(), LocalCoreStateStoreError> {
    let operation = journal.plan.operation_id;
    let previous = current.map_or(0, |receipt| receipt.preimages);
    if previous > journal.preimages.len() {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    if current.is_none() {
        let paths = data_paths(namespace, operation, None)?;
        match &paths.allocation {
            Some(allocation) => {
                crate::native_host::create_directories(
                    paths.directory(),
                    &mut &*allocation.work(),
                )?;
            }
            None => std::fs::create_dir_all(paths.directory())?,
        }
        write_journaled(&paths, &journal.plan)?;
    }
    for (index, image) in journal.preimages.iter().enumerate().skip(previous) {
        let paths = data_paths(namespace, operation, Some(index))?;
        write_journaled(&paths, image)?;
    }
    Ok(())
}

pub(super) fn reconstruct(
    namespace: &Namespace,
    operation: OperationId,
    receipt: Receipt,
    mut custody: Option<crate::NativeAllocation>,
) -> Result<MaterializationJournal, LocalCoreStateStoreError> {
    if receipt.version != crate::materializer::JOURNAL_VERSION || receipt.revision == 0 {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    let paths = data_paths(namespace, operation, None)?;
    let (plan, plan_custody) = read_published::<crate::MaterializationPlan>(&paths)?
        .ok_or(LocalCoreStateStoreError::Integrity)?;
    if plan.operation_id != operation || receipt.preimages > plan.edits.len() {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    absorb(&mut custody, plan_custody)?;
    let mut preimages = Vec::new();
    if let Some(custody) = &mut custody {
        crate::materializer::reserve_buffer(&mut preimages, receipt.preimages, custody)?;
    } else {
        preimages
            .try_reserve_exact(receipt.preimages)
            .map_err(|_| LocalCoreStateStoreError::Integrity)?;
    }
    for index in 0..receipt.preimages {
        let paths = data_paths(namespace, operation, Some(index))?;
        let (image, image_custody) = read_published::<crate::MaterializationPreimage>(&paths)?
            .ok_or(LocalCoreStateStoreError::Integrity)?;
        absorb(&mut custody, image_custody)?;
        preimages.push(image);
    }
    Ok(MaterializationJournal {
        version: receipt.version,
        revision: receipt.revision,
        plan,
        root_identity: receipt.root_identity,
        preparation_identity: receipt.preparation_identity,
        preparation_progress: receipt.preparation_progress,
        work: receipt.work,
        preimages,
        phase: receipt.phase,
        applied: receipt.applied,
        restored: receipt.restored,
        custody,
    })
}

fn absorb(
    retained: &mut Option<crate::NativeAllocation>,
    additional: Option<crate::NativeAllocation>,
) -> Result<(), LocalCoreStateStoreError> {
    if let Some(mut additional) = additional {
        match retained {
            Some(retained) => retained.absorb(&mut additional)?,
            None => *retained = Some(additional),
        }
    }
    Ok(())
}

pub(super) fn remove(
    namespace: &Namespace,
    operation: OperationId,
) -> Result<(), LocalCoreStateStoreError> {
    let paths = data_paths(namespace, operation, None)?;
    let directory = paths.directory();
    let parent = directory
        .parent()
        .ok_or(LocalCoreStateStoreError::Integrity)?;
    let leaf = Path::new(
        directory
            .file_name()
            .ok_or(LocalCoreStateStoreError::Integrity)?,
    );
    match &paths.allocation {
        Some(allocation) => {
            let work = allocation.work();
            let root = match crate::native_host::HostRoot::open(parent, &mut &*work) {
                Ok(root) => root,
                Err(NativeTreeMaterializationError::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            };
            work.with_original_leaf(|state| {
                crate::native_host::host_path(leaf, state, |leaf, work| {
                    root.remove(leaf, None, work)
                })
            })?;
            crate::native_host::sync_directory_held(&root.directory(), &mut &*work)?;
        }
        None => match std::fs::remove_dir_all(directory) {
            Ok(()) => sync_directory(parent)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        },
    }
    Ok(())
}
