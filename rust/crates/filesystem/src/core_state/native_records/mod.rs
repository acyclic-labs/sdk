//! Stock CoreLog projections retain physical payload custody in the original invocation.
use super::*;
use crate::CancellationToken;
use crate::materializer::{
    NativeAllocation, NativeMaterializationWork as Work,
    NativeTreeMaterializationError as NativeError,
};
use crate::workspace_context::WorkspaceContextRoot;

type Error = LocalCoreStateStoreError;
type Owned<T> = (T, crate::NativeAllocation);

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
    let parts = owned;
    if !parts.1.work().shares_allocation_context(work) {
        return Err(Error::NativeLedgerMismatch);
    }
    if parts.1.bytes() != bytes(&parts.0)? {
        return Err(Error::Integrity);
    }
    Ok((parts.0, parts.1))
}

fn transfer<T>(mut owned: Owned<T>, work: &Work, held: &mut NativeAllocation) -> Result<T, Error> {
    if !owned.1.work().shares_allocation_context(work) {
        return Err(Error::NativeLedgerMismatch);
    }
    held.absorb(&mut owned.1)?;
    Ok(owned.0)
}

fn string(value: &str, work: &Work, held: &mut NativeAllocation) -> Result<String, Error> {
    let delta = crate::WorkCounters {
        bytes_copied: value.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    let mut copying = work.reserve_work(delta)?;
    let mut result = Vec::new();
    reserve(&mut result, value.len(), work, held)?;
    result.extend_from_slice(value.as_bytes());
    copying.record_completed(delta)?;
    String::from_utf8(result).map_err(|_| Error::Integrity)
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
    roots: impl Iterator<Item = (&'a crate::WorkspaceRootId, &'a WorkspaceContextRoot)>,
    work: &Work,
    cxl: &CancellationToken,
    held: &mut NativeAllocation,
) -> Result<BTreeMap<crate::WorkspaceRootId, WorkspaceContextRoot>, Error> {
    let mut output = BTreeMap::new();
    for (id, root) in roots {
        check(work, cxl)?;
        work.charge(crate::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        root.source_path.to_str().ok_or(Error::Integrity)?;
        if let Some(mount) = &root.mount_path {
            mount.to_str().ok_or(Error::Integrity)?;
        }
        // Fund logical entries and deep payload; std tree backing is opaque.
        let mut entry = work.claim(
            std::mem::size_of::<(crate::WorkspaceRootId, WorkspaceContextRoot)>() as u64,
            0,
            crate::WorkCounters::UNCHARGED,
        )?;
        held.absorb(&mut entry)?;
        output.insert(
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
        );
    }
    Ok(output)
}

pub(super) fn clone_native_record_value(
    value: &RecordValue,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<Owned<RecordValue>, Error> {
    check(work, cxl)?;
    let mut held = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let value = match value {
        RecordValue::Context(value) => RecordValue::Context(WorkspaceContext {
            version: value.version,
            revision: value.revision,
            context_id: value.context_id,
            parent_context_id: value.parent_context_id,
            roots: clone_roots(value.roots.iter(), work, cxl, &mut held)?,
            state: value.state,
        }),
        RecordValue::FlatContext(value) => RecordValue::Context(WorkspaceContext {
            version: value.version,
            revision: value.revision,
            context_id: value.context_id,
            parent_context_id: value.parent_context_id,
            roots: clone_roots(
                value.roots.0.iter().map(|(id, root)| (id, root)),
                work,
                cxl,
                &mut held,
            )?,
            state: value.state,
        }),
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
            canonical_records(&mut children, work, Ord::cmp)?;
            RecordValue::FlatChildren(children)
        }
        RecordValue::ChildCount(value) => RecordValue::ChildCount(*value),
        RecordValue::ChildIndex(value) => RecordValue::ChildIndex(*value),
        RecordValue::LazyWorkspace(value) => RecordValue::LazyWorkspace(value.clone()),
    };
    held.substitute(native_record_owned_bytes(&value, work)?)?;
    Ok((value, held))
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
    let value = transfer((value, allocation), work, &mut held)?;
    let Some(value) = value else {
        return Ok(None);
    };
    let mut value = map(value);
    value.normalize(Some(work))?;
    held.substitute(native_record_owned_bytes(&value, work)?)?;
    Ok(Some((value, held)))
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
    match owned {
        (RecordValue::Lineage(value), allocation) => Ok(Some((value, allocation))),
        owned => {
            drop(owned);
            Err(Error::Integrity)
        }
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
    if load_native_lineage(root, log, id, work, cxl)?
        .as_ref()
        .map_or(0, |value| (&value.0).revision)
        != expected
    {
        return Ok(false);
    }
    let mut change = Change::new(work)?;
    let (record, allocation) = replacement;
    change.put_owned(
        RecordKey::Lineage(id),
        (RecordValue::Lineage(record), allocation),
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
    match owned {
        (RecordValue::Context(value), allocation) => Ok(Some((value, allocation))),
        owned => {
            drop(owned);
            Err(Error::Integrity)
        }
    }
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
