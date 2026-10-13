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
        return Err(crate::LazyWorkspaceError::NativeLedgerMismatch.into());
    }
    if parts.1.bytes() != bytes(&parts.0)? {
        return Err(Error::Integrity);
    }
    Ok((parts.0, parts.1))
}

pub(super) fn clone_native_record_value(
    value: &RecordValue,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<Owned<RecordValue>, Error> {
    check(work, cxl)?;
    let mut copied = 0_u64;
    let mut operations = 0_u64;
    let mut buffer = |bytes: &[u8]| -> Result<(), Error> {
        copied = copied
            .checked_add(bytes.len() as u64)
            .ok_or_else(overflow)?;
        operations += u64::from(!bytes.is_empty());
        Ok(())
    };
    let mut entries = 0_u64;
    let mut items = 0_u64;
    match value {
        RecordValue::Context(context) => {
            entries = context
                .roots
                .len()
                .checked_mul(std::mem::size_of::<(
                    crate::WorkspaceRootId,
                    WorkspaceContextRoot,
                )>())
                .ok_or_else(overflow)? as u64;
            for root in context.roots.values() {
                check(work, cxl)?;
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                root.source_path.to_str().ok_or(Error::Integrity)?;
                buffer(root.source_path.as_os_str().as_encoded_bytes())?;
                buffer(root.workspace_name.as_bytes())?;
                if let Some(mount) = &root.mount_path {
                    mount.to_str().ok_or(Error::Integrity)?;
                    buffer(mount.as_os_str().as_encoded_bytes())?;
                }
            }
        }
        RecordValue::Lineage(lineage) => {
            buffer(lineage.workspace_name.as_bytes())?;
            if let Some(parent) = &lineage.parent_workspace_name {
                buffer(parent.as_bytes())?;
            }
        }
        RecordValue::Children(children) => {
            items = children.0.len() as u64;
            entries = children
                .0
                .len()
                .checked_mul(std::mem::size_of::<WorkspaceContextId>())
                .ok_or_else(overflow)? as u64;
        }
        _ => {}
    }
    let bytes = entries.checked_add(copied).ok_or_else(overflow)?;
    // Main's clones allocate exact-length strings; std tree backing is opaque.
    let mut held = work.claim(
        bytes,
        operations,
        crate::WorkCounters {
            bytes_copied: if matches!(value, RecordValue::Children(_)) {
                entries
            } else {
                copied
            },
            items_examined: items,
            ..crate::WorkCounters::UNCHARGED
        },
    )?;
    let value = value.clone();
    held.resize(native_record_owned_bytes(&value, work)?)?;
    check(work, cxl)?;
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
        // These payloads have only fixed identifiers/scalars/Copy metadata.
        // Their enclosing resident vector, output or source Box funds its
        // compiled inline storage; they own no additional dynamic backing.
        RecordValue::ChildCount(_) | RecordValue::ChildIndex(_) | RecordValue::LazyWorkspace(_) => {
            0
        }
    })
}

fn read<T: Logged>(path: &Path, work: &Work) -> Result<Option<Owned<RecordValue>>, Error> {
    let Some((value, mut allocation)) = measured_read_file::<Option<T>>(path, work)? else {
        return Ok(None);
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.into_value();
    allocation.resize(native_record_owned_bytes(&value, work)?)?;
    Ok(Some((value, allocation)))
}

pub(super) fn read_native_record(
    key: RecordKey,
    path: &Path,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<Option<Owned<RecordValue>>, Error> {
    check(work, cxl)?;
    match key {
        RecordKey::Context(_) => read::<WorkspaceContext>(path, work),
        RecordKey::Children(_) => read::<ContextChildren>(path, work),
        RecordKey::ChildCount(_) => read::<ContextChildCount>(path, work),
        RecordKey::ChildIndex => read::<ContextChildIndex>(path, work),
        RecordKey::Lineage(_) => read::<WorkspaceLineageRecord>(path, work),
        RecordKey::LazyWorkspace(_) => read::<LazyWorkspaceState>(path, work),
    }
}

pub(super) fn load<T: Logged>(
    root: &Namespace,
    log: &mut CoreLog,
    id: T::Id,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<Option<Owned<T>>, Error> {
    let Some((value, allocation)) = log.get_native_value(root, T::key(id), work, cxl)? else {
        return Ok(None);
    };
    let value = T::from_value(value).ok_or(Error::Integrity)?;
    Ok(Some((value, allocation)))
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
    if load::<WorkspaceLineageRecord>(root, log, id, work, cxl)?
        .as_ref()
        .map_or(0, |value| (&value.0).revision)
        != expected
    {
        return Ok(false);
    }
    let mut change = Change::new(work)?;
    let (record, allocation) = replacement;
    change.put(Resident {
        key: RecordKey::Lineage(id),
        value: Some(RecordValue::Lineage(record)),
        allocation: Some(allocation),
    })?;
    cxl.check()
        .map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
    log.commit(root, change)?;
    Ok(true)
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
    let mut bytes = context
        .roots
        .len()
        .checked_mul(std::mem::size_of::<(
            crate::WorkspaceRootId,
            WorkspaceContextRoot,
        )>())
        .ok_or_else(overflow)? as u64;
    for root in context.roots.values() {
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
