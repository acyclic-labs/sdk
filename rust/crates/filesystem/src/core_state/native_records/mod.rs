//! Stock CoreLog projections retain physical payload custody in the original invocation.
use super::*;
use crate::CancellationToken;
use crate::materializer::{
    NativeAllocation, NativeMaterializationWork as Work,
    NativeTreeMaterializationError as NativeError,
};
use crate::workspace_context::WorkspaceContextRoot;

type Error = LocalCoreStateStoreError;
type Owned<T> = NativeOwnedCoreState<T>;

pub(super) fn check(work: &Work, cxl: &CancellationToken) -> Result<(), Error> {
    cxl.check().map_err(|_| NativeError::Cancelled)?;
    work.cancellation()
        .check()
        .map_err(|_| NativeError::Cancelled)?;
    work.charge(crate::WorkCounters::UNCHARGED)?;
    Ok(())
}

fn overflow() -> Error {
    NativeError::Work(crate::WorkError::Overflow).into()
}

pub(super) fn checked_packet<T>(
    owned: Owned<T>,
    work: &Work,
    bytes: impl FnOnce(&T) -> Result<u64, Error>,
) -> Result<Owned<T>, Error> {
    // Keep the value-first tuple intact through every rejecting branch.
    let parts = owned.into_parts();
    if !parts.1.work().shares_allocation_context(work) {
        return Err(Error::NativeLedgerMismatch);
    }
    if parts.1.bytes() != bytes(&parts.0)? {
        return Err(Error::Integrity);
    }
    Ok(Owned::from_parts(parts.0, parts.1))
}

fn transfer<T>(owned: Owned<T>, work: &Work, held: &mut NativeAllocation) -> Result<T, Error> {
    let (value, mut allocation) = owned.into_parts();
    let result = if allocation.work().shares_allocation_context(work) {
        held.absorb(&mut allocation).map_err(Error::from)
    } else {
        Err(Error::NativeLedgerMismatch)
    };
    if let Err(error) = result {
        drop(value);
        drop(allocation);
        return Err(error);
    }
    Ok(value)
}

fn string(value: &str, work: &Work, held: &mut NativeAllocation) -> Result<String, Error> {
    let mut claim = work.claim(
        value.len() as u64,
        u64::from(!value.is_empty()),
        crate::WorkCounters::UNCHARGED,
    )?;
    let copying = work.reserve_work(crate::WorkCounters {
        bytes_copied: value.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    })?;
    let mut result = String::new();
    let outcome = (|| {
        result
            .try_reserve_exact(value.len())
            .map_err(NativeError::AllocationFailed)?;
        held.absorb(&mut claim)?;
        if result.capacity() > value.len() {
            let (mut excess, completed) =
                work.record_completed_allocation((result.capacity() - value.len()) as u64, None);
            held.absorb(&mut excess)?;
            completed?;
        }
        result.push_str(value);
        {
            let mut copy = copying;
            copy.record_completed(crate::WorkCounters {
                bytes_copied: value.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            })
        }?;
        Ok::<_, Error>(())
    })();
    if let Err(error) = outcome {
        drop(result);
        return Err(error);
    }
    Ok(result)
}

fn path(value: &Path, work: &Work, held: &mut NativeAllocation) -> Result<PathBuf, Error> {
    let (value, mut allocation) = crate::native_host::clone_path(value, &mut &*work)?;
    let result = held.absorb(&mut allocation);
    if let Err(error) = result {
        drop(value);
        return Err(error.into());
    }
    Ok(value)
}

fn clone_lineage(
    value: &WorkspaceLineageRecord,
    work: &Work,
    held: &mut NativeAllocation,
) -> Result<WorkspaceLineageRecord, Error> {
    Ok(WorkspaceLineageRecord {
        version: value.version,
        revision: value.revision,
        ready: value.ready,
        workspace_id: value.workspace_id,
        workspace_name: string(&value.workspace_name, work, held)?,
        parent_workspace_id: value.parent_workspace_id,
        parent_workspace_name: value
            .parent_workspace_name
            .as_deref()
            .map(|name| string(name, work, held))
            .transpose()?,
        fork_generation: value.fork_generation,
        initial_generation: value.initial_generation,
    })
}

fn clone_roots<'a>(
    roots: impl ExactSizeIterator<Item = (&'a crate::WorkspaceRootId, &'a WorkspaceContextRoot)>,
    work: &Work,
    held: &mut NativeAllocation,
) -> Result<FlatMap<crate::WorkspaceRootId, WorkspaceContextRoot>, Error> {
    let mut output = Vec::new();
    reserve(&mut output, roots.len(), work, held)?;
    for (id, root) in roots {
        work.charge(crate::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        root.source_path.to_str().ok_or(Error::Integrity)?;
        if let Some(mount) = &root.mount_path {
            mount.to_str().ok_or(Error::Integrity)?;
        }
        output.push((
            *id,
            WorkspaceContextRoot {
                root_id: root.root_id,
                source_path: path(&root.source_path, work, held)?,
                workspace_id: root.workspace_id,
                workspace_name: string(&root.workspace_name, work, held)?,
                parent_workspace_id: root.parent_workspace_id,
                mount_path: root
                    .mount_path
                    .as_deref()
                    .map(|value| path(value, work, held))
                    .transpose()?,
            },
        ));
    }
    Ok(FlatMap(output))
}

fn flatten_context(
    value: &WorkspaceContext,
    work: &Work,
    held: &mut NativeAllocation,
) -> Result<FlatWorkspaceContext, Error> {
    Ok(FlatWorkspaceContext {
        version: value.version,
        revision: value.revision,
        context_id: value.context_id,
        parent_context_id: value.parent_context_id,
        roots: clone_roots(value.roots.iter(), work, held)?,
        state: value.state,
    })
}

fn clone_flat_context(
    value: &FlatWorkspaceContext,
    work: &Work,
    held: &mut NativeAllocation,
) -> Result<FlatWorkspaceContext, Error> {
    let mut roots = clone_roots(
        value.roots.0.iter().map(|(id, root)| (id, root)),
        work,
        held,
    )?;
    measured_order(&mut roots.0, work)?;
    Ok(FlatWorkspaceContext {
        version: value.version,
        revision: value.revision,
        context_id: value.context_id,
        parent_context_id: value.parent_context_id,
        roots,
        state: value.state,
    })
}

pub(super) fn canonical_ids(
    values: &mut Vec<WorkspaceContextId>,
    work: &Work,
) -> Result<(), Error> {
    sort_records(values, work, Ord::cmp)?;
    let mut index = 1;
    while index < values.len() {
        work.charge(crate::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        if values[index - 1] == values[index] {
            work.charge(crate::WorkCounters {
                bytes_copied: ((values.len() - index - 1)
                    * std::mem::size_of::<WorkspaceContextId>())
                    as u64,
                ..Default::default()
            })?;
            values.remove(index);
        } else {
            index += 1;
        }
    }
    Ok(())
}

pub(super) fn clone_native_record_value(
    value: &RecordValue,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<Owned<RecordValue>, Error> {
    check(work, cxl)?;
    let mut held = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let value = match value {
        RecordValue::Context(value) => {
            RecordValue::FlatContext(flatten_context(value, work, &mut held)?)
        }
        RecordValue::FlatContext(value) => {
            RecordValue::FlatContext(clone_flat_context(value, work, &mut held)?)
        }
        RecordValue::Lineage(value) => RecordValue::Lineage(clone_lineage(value, work, &mut held)?),
        RecordValue::Children(value) => {
            let mut children = Vec::new();
            reserve(&mut children, value.0.len(), work, &mut held)?;
            for child in &value.0 {
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    bytes_copied: std::mem::size_of::<WorkspaceContextId>() as u64,
                    ..Default::default()
                })?;
                children.push(*child);
            }
            RecordValue::FlatChildren(children)
        }
        RecordValue::FlatChildren(value) => {
            let mut children = Vec::new();
            reserve(&mut children, value.len(), work, &mut held)?;
            let copying = work.reserve_work(crate::WorkCounters {
                bytes_copied: (value.len() * std::mem::size_of::<WorkspaceContextId>()) as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            children.extend_from_slice(value);
            {
                let mut copy = copying;
                copy.record_completed(crate::WorkCounters {
                    bytes_copied: (value.len() * std::mem::size_of::<WorkspaceContextId>()) as u64,
                    ..crate::WorkCounters::UNCHARGED
                })
            }?;
            canonical_ids(&mut children, work)?;
            RecordValue::FlatChildren(children)
        }
        RecordValue::ChildCount(value) => RecordValue::ChildCount(*value),
        RecordValue::ChildIndex(value) => RecordValue::ChildIndex(*value),
        RecordValue::LazyWorkspace(value) => RecordValue::LazyWorkspace(value.clone()),
    };
    held.substitute(native_record_owned_bytes(&value, work)?)?;
    Ok(Owned::from_parts(value, held))
}

pub(super) fn native_record_owned_bytes(value: &RecordValue, work: &Work) -> Result<u64, Error> {
    Ok(match value {
        RecordValue::Context(context) => native_context_owned_bytes(context, work)?,
        RecordValue::Children(children) => children
            .0
            .len()
            .checked_mul(std::mem::size_of::<WorkspaceContextId>())
            .ok_or_else(overflow)? as u64,
        RecordValue::Lineage(lineage) => native_lineage_owned_bytes(lineage, work)?,
        RecordValue::FlatContext(context) => native_flat_context_owned_bytes(context, work)?,
        RecordValue::FlatChildren(children) => children
            .capacity()
            .checked_mul(std::mem::size_of::<WorkspaceContextId>())
            .ok_or_else(overflow)? as u64,
        // These payloads have only fixed identifiers/scalars/Copy metadata.
        // Their enclosing resident vector, output or source Box funds its
        // compiled inline storage; they own no additional dynamic backing.
        RecordValue::ChildCount(_) | RecordValue::ChildIndex(_) | RecordValue::LazyWorkspace(_) => {
            0
        }
    })
}

pub(super) fn native_flat_context_owned_bytes(
    context: &FlatWorkspaceContext,
    work: &Work,
) -> Result<u64, Error> {
    roots_owned_bytes(
        context.roots.0.capacity(),
        context.roots.0.iter().map(|(_, root)| root),
        work,
    )
}

fn read<T: DeserializeOwned>(
    path: &Path,
    work: &Work,
    map: impl FnOnce(T) -> RecordValue,
) -> Result<Option<Owned<RecordValue>>, Error> {
    // Declare the guard before the decoder's physical result.
    let mut held = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let Some((value, allocation)) = measured_read_file::<Option<T>>(path, work)? else {
        return Ok(None);
    };
    let value = transfer(Owned::from_parts(value, allocation), work, &mut held)?;
    let Some(value) = value else {
        return Ok(None);
    };
    let mut value = map(value);
    value.normalize(Some(work))?;
    held.substitute(native_record_owned_bytes(&value, work)?)?;
    Ok(Some(Owned::from_parts(value, held)))
}

pub(super) fn read_native_record(
    key: RecordKey,
    path: &Path,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<Option<Owned<RecordValue>>, Error> {
    check(work, cxl)?;
    match key {
        RecordKey::Context(_) => read::<FlatWorkspaceContext>(path, work, RecordValue::FlatContext),
        RecordKey::Children(_) => {
            read::<Vec<WorkspaceContextId>>(path, work, RecordValue::FlatChildren)
        }
        RecordKey::ChildCount(_) => read::<ContextChildCount>(path, work, RecordValue::ChildCount),
        RecordKey::ChildIndex => read::<ContextChildIndex>(path, work, RecordValue::ChildIndex),
        RecordKey::Lineage(_) => read::<WorkspaceLineageRecord>(path, work, RecordValue::Lineage),
        RecordKey::LazyWorkspace(_) => {
            read::<LazyWorkspaceState>(path, work, RecordValue::LazyWorkspace)
        }
    }
}

pub(super) fn load_native_lineage(
    root: &Namespace,
    log: &mut CoreLog,
    id: WorkspaceId,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<Option<Owned<WorkspaceLineageRecord>>, Error> {
    let Some(owned) = log.get_native_value(root, RecordKey::Lineage(id), work, cxl)? else {
        return Ok(None);
    };
    let mut held = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let value = transfer(owned, work, &mut held)?;
    match value {
        RecordValue::Lineage(value) => Ok(Some(Owned::from_parts(value, held))),
        _ => Err(Error::Integrity),
    }
}

pub(super) fn compare_native_lineage(
    root: &Namespace,
    log: &mut CoreLog,
    id: WorkspaceId,
    expected: u64,
    replacement: Owned<WorkspaceLineageRecord>,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<bool, Error> {
    check(work, cxl)?;
    let replacement = checked_packet(replacement, work, |lineage| {
        native_lineage_owned_bytes(lineage, work)
    })?;
    let mut held = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let replacement = transfer(replacement, work, &mut held)?;
    if load_native_lineage(root, log, id, work, cxl)?
        .as_ref()
        .map_or(0, |value| value.as_state().revision)
        != expected
    {
        return Ok(false);
    }
    let mut change = Change::new(work)?;
    change.put_owned(
        RecordKey::Lineage(id),
        Owned::from_parts(RecordValue::Lineage(replacement), held),
        work,
    )?;
    cxl.check()
        .map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
    log.commit(root, change)?;
    Ok(true)
}

pub(super) fn load_native_context(
    root: &Namespace,
    log: &mut CoreLog,
    id: WorkspaceContextId,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<Option<Owned<WorkspaceContext>>, Error> {
    let Some(owned) = log.get_native_value(root, RecordKey::Context(id), work, cxl)? else {
        return Ok(None);
    };
    let mut held = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let value = transfer(owned, work, &mut held)?;
    let value = match value {
        RecordValue::Context(value) => value,
        RecordValue::FlatContext(value) => {
            let mut roots = BTreeMap::new();
            for (id, root) in value.roots.0 {
                check(work, cxl)?;
                let mut entry = work.claim(
                    std::mem::size_of::<(crate::WorkspaceRootId, WorkspaceContextRoot)>() as u64,
                    0,
                    crate::WorkCounters::UNCHARGED,
                )?;
                held.absorb(&mut entry)?;
                roots.insert(id, root);
            }
            let context = WorkspaceContext {
                version: value.version,
                revision: value.revision,
                context_id: value.context_id,
                parent_context_id: value.parent_context_id,
                roots,
                state: value.state,
            };
            let bytes = native_context_owned_bytes(&context, work)?;
            held.substitute(bytes)?;
            context
        }
        _ => return Err(Error::Integrity),
    };
    Ok(Some(Owned::from_parts(value, held)))
}

pub(super) fn native_lineage_owned_bytes(
    lineage: &WorkspaceLineageRecord,
    work: &Work,
) -> Result<u64, Error> {
    work.charge(crate::WorkCounters {
        items_examined: 1,
        ..Default::default()
    })?;
    (lineage.workspace_name.capacity() as u64)
        .checked_add(
            lineage
                .parent_workspace_name
                .as_ref()
                .map_or(0, |name| name.capacity() as u64),
        )
        .ok_or_else(overflow)
}

pub(super) fn native_context_owned_bytes(
    context: &WorkspaceContext,
    work: &Work,
) -> Result<u64, Error> {
    roots_owned_bytes(context.roots.len(), context.roots.values(), work)
}

fn roots_owned_bytes<'a>(
    capacity: usize,
    roots: impl Iterator<Item = &'a WorkspaceContextRoot>,
    work: &Work,
) -> Result<u64, Error> {
    let mut bytes = capacity
        .checked_mul(std::mem::size_of::<(
            crate::WorkspaceRootId,
            WorkspaceContextRoot,
        )>())
        .ok_or_else(overflow)? as u64;
    for root in roots {
        work.charge(crate::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        bytes = bytes
            .checked_add(root.source_path.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(root.workspace_name.capacity() as u64))
            .and_then(|bytes| {
                bytes.checked_add(
                    root.mount_path
                        .as_ref()
                        .map_or(0, |path| path.capacity() as u64),
                )
            })
            .ok_or_else(overflow)?;
    }
    Ok(bytes)
}

mod contexts;
pub(super) use contexts::*;
