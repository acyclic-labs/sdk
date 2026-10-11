//! Stock CoreLog projections retain physical payload custody in the original invocation.
use super::*;
use crate::materializer::{NativeAllocation, NativeMaterializationWork as Work, NativeTreeMaterializationError as NativeError};
use crate::workspace_context::WorkspaceContextRoot;
use crate::CancellationToken;

type Error = LocalCoreStateStoreError;
type Owned<T> = NativeOwnedCoreState<T>;

fn check(work: &Work, cxl: &CancellationToken) -> Result<(), Error> {
    cxl.check().map_err(|_| NativeError::Cancelled)?;
    work.charge(crate::WorkCounters::UNCHARGED)?;
    Ok(())
}

fn overflow() -> Error { NativeError::Work(crate::WorkError::Overflow).into() }

fn checked_packet<T>(owned: Owned<T>, work: &Work, bytes: impl FnOnce(&T) -> Result<u64, Error>) -> Result<Owned<T>, Error> {
    // Keep the value-first tuple intact through every rejecting branch.
    let parts = owned.into_parts();
    if !parts.1.matches_work(work) { return Err(Error::NativeLedgerMismatch); }
    if parts.1.bytes() != bytes(&parts.0)? { return Err(Error::Integrity); }
    Ok(Owned::from_parts(parts.0, parts.1))
}

fn transfer<T>(owned: Owned<T>, work: &Work, held: &mut NativeAllocation) -> Result<T, Error> {
    let (value, mut allocation) = owned.into_parts();
    let result = if allocation.matches_work(work) {
        held.absorb(&mut allocation).map_err(Error::from)
    } else { Err(Error::NativeLedgerMismatch) };
    if let Err(error) = result { drop(value); drop(allocation); return Err(error); }
    Ok(value)
}

// Claims happen before the allocator; completed excess is absorbed before its
// potentially failing reconciliation is returned. The caller declares custody
// before its physical values so every error path drops physical storage first.
fn reserve<T>(values: &mut Vec<T>, needed: usize, work: &Work, held: &mut NativeAllocation) -> Result<(), Error> {
    if needed <= values.capacity() { return Ok(()); }
    let old = values.capacity();
    let next = needed.max(old.checked_mul(2).ok_or_else(overflow)?).max(1);
    let old_bytes = old.checked_mul(std::mem::size_of::<T>()).ok_or_else(overflow)? as u64;
    let bytes = next.checked_mul(std::mem::size_of::<T>()).ok_or_else(overflow)? as u64;
    let mut claim = work.claim_allocation(bytes, u64::from(bytes != 0))?;
    let copying = work.reserve_copy((values.len() * std::mem::size_of::<T>()) as u64)?;
    values.try_reserve_exact(next - values.len()).map_err(NativeError::AllocationFailed)?;
    held.absorb(&mut claim)?;
    // The allocator has physically retired the old backing, not merely
    // overwritten its handles. Until here BOTH full capacities were funded.
    held.release(old_bytes)?;
    let actual = values.capacity().checked_mul(std::mem::size_of::<T>()).ok_or_else(overflow)? as u64;
    let capacity_result = if actual > bytes {
        let (mut excess, result) = work.record_completed_allocation(actual - bytes, None);
        held.absorb(&mut excess)?;
        result
    } else if actual < bytes { held.release(bytes - actual) } else { Ok(()) };
    let copy_result = copying.record((values.len() * std::mem::size_of::<T>()) as u64);
    capacity_result?;
    copy_result?;
    Ok(())
}

fn string(value: &str, work: &Work, held: &mut NativeAllocation) -> Result<String, Error> {
    let mut claim = work.claim_allocation(value.len() as u64, u64::from(!value.is_empty()))?;
    let copying = work.reserve_copy(value.len() as u64)?;
    let mut result = String::new();
    let outcome = (|| {
        result.try_reserve_exact(value.len()).map_err(NativeError::AllocationFailed)?;
        held.absorb(&mut claim)?;
        if result.capacity() > value.len() {
            let (mut excess, completed) = work.record_completed_allocation((result.capacity() - value.len()) as u64, None);
            held.absorb(&mut excess)?;
            completed?;
        }
        result.push_str(value);
        copying.record(value.len() as u64)?;
        Ok::<_, Error>(())
    })();
    if let Err(error) = outcome { drop(result); return Err(error); }
    Ok(result)
}

fn path(value: &Path, work: &Work, held: &mut NativeAllocation) -> Result<PathBuf, Error> {
    let (value, mut allocation) = crate::native_host::clone_path_with_work(value, &mut &*work)?;
    let result = held.absorb(&mut allocation);
    if let Err(error) = result { drop(value); return Err(error.into()); }
    Ok(value)
}

fn clone_lineage(value: &WorkspaceLineageRecord, work: &Work, held: &mut NativeAllocation) -> Result<WorkspaceLineageRecord, Error> {
    Ok(WorkspaceLineageRecord {
        version: value.version, revision: value.revision, ready: value.ready,
        workspace_id: value.workspace_id,
        workspace_name: string(&value.workspace_name, work, held)?,
        parent_workspace_id: value.parent_workspace_id,
        parent_workspace_name: value.parent_workspace_name.as_deref().map(|name| string(name, work, held)).transpose()?,
        fork_generation: value.fork_generation, initial_generation: value.initial_generation,
    })
}

fn flatten_context(value: &WorkspaceContext, work: &Work, held: &mut NativeAllocation) -> Result<FlatWorkspaceContext, Error> {
    let mut roots = Vec::new();
    reserve(&mut roots, value.roots.len(), work, held)?;
    for (id, root) in &value.roots {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
        let source = root.source_path.to_str().ok_or(Error::Integrity)?;
        let mount = root.mount_path.as_deref().map(|path| path.to_str().ok_or(Error::Integrity)).transpose()?;
        roots.push((*id, FlatWorkspaceContextRoot {
            root_id: root.root_id, source_path: string(source, work, held)?,
            workspace_id: root.workspace_id, workspace_name: string(&root.workspace_name, work, held)?,
            parent_workspace_id: root.parent_workspace_id,
            mount_path: mount.map(|path| string(path, work, held)).transpose()?,
        }));
    }
    Ok(FlatWorkspaceContext { version: value.version, revision: value.revision,
        context_id: value.context_id, parent_context_id: value.parent_context_id,
        roots: FlatMap(roots), state: value.state })
}

fn clone_flat_context(value: &FlatWorkspaceContext, work: &Work, held: &mut NativeAllocation) -> Result<FlatWorkspaceContext, Error> {
    let mut roots = Vec::new();
    reserve(&mut roots, value.roots.0.len(), work, held)?;
    for (id, root) in &value.roots.0 {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
        roots.push((*id, FlatWorkspaceContextRoot {
            root_id: root.root_id, source_path: string(&root.source_path, work, held)?,
            workspace_id: root.workspace_id, workspace_name: string(&root.workspace_name, work, held)?,
            parent_workspace_id: root.parent_workspace_id,
            mount_path: root.mount_path.as_deref().map(|value| string(value, work, held)).transpose()?,
        }));
    }
    measured_order(&mut roots, work)?;
    Ok(FlatWorkspaceContext { version: value.version, revision: value.revision,
        context_id: value.context_id, parent_context_id: value.parent_context_id,
        roots: FlatMap(roots), state: value.state })
}

fn canonical_ids(values: &mut Vec<WorkspaceContextId>, work: &Work) -> Result<(), Error> {
    for index in 1..values.len() {
        let mut position = index;
        while position != 0 {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if values[position - 1] <= values[position] { break; }
            work.charge(crate::WorkCounters { bytes_copied: (3 * std::mem::size_of::<WorkspaceContextId>()) as u64, ..Default::default() })?;
            values.swap(position - 1, position);
            position -= 1;
        }
    }
    let mut index = 1;
    while index < values.len() {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
        if values[index - 1] == values[index] {
            work.charge(crate::WorkCounters { bytes_copied: ((values.len() - index - 1) * std::mem::size_of::<WorkspaceContextId>()) as u64, ..Default::default() })?;
            values.remove(index);
        } else { index += 1; }
    }
    Ok(())
}

pub(super) fn clone_native_record_value(value: &RecordValue, work: &Work, cxl: &CancellationToken) -> Result<Owned<RecordValue>, Error> {
    check(work, cxl)?;
    let mut held = work.claim(0)?;
    let value = match value {
        RecordValue::Context(value) => RecordValue::FlatContext(flatten_context(value, work, &mut held)?),
        RecordValue::FlatContext(value) => RecordValue::FlatContext(clone_flat_context(value, work, &mut held)?),
        RecordValue::Lineage(value) => RecordValue::Lineage(clone_lineage(value, work, &mut held)?),
        RecordValue::Children(value) => {
            let mut children = Vec::new();
            reserve(&mut children, value.0.len(), work, &mut held)?;
            for child in &value.0 {
                work.charge(crate::WorkCounters { items_examined: 1, bytes_copied: std::mem::size_of::<WorkspaceContextId>() as u64, ..Default::default() })?;
                children.push(*child);
            }
            RecordValue::FlatChildren(children)
        }
        RecordValue::FlatChildren(value) => {
            let mut children = Vec::new();
            reserve(&mut children, value.len(), work, &mut held)?;
            let copying = work.reserve_copy((value.len() * std::mem::size_of::<WorkspaceContextId>()) as u64)?;
            children.extend_from_slice(value);
            copying.record((value.len() * std::mem::size_of::<WorkspaceContextId>()) as u64)?;
            canonical_ids(&mut children, work)?;
            RecordValue::FlatChildren(children)
        }
        RecordValue::ChildCount(value) => RecordValue::ChildCount(*value),
        RecordValue::ChildIndex(value) => RecordValue::ChildIndex(*value),
        RecordValue::LazyWorkspace(value) => RecordValue::LazyWorkspace(clone_native_lazy_state(value, work)?),
    };
    held.substitute(native_record_owned_bytes(&value, work)?)?;
    Ok(Owned::from_parts(value, held))
}

pub(super) fn native_record_owned_bytes(value: &RecordValue, work: &Work) -> Result<u64, Error> {
    Ok(match value {
        RecordValue::Context(context) => native_context_owned_bytes(context, work)?,
        RecordValue::Children(children) => children.0.len().checked_mul(std::mem::size_of::<WorkspaceContextId>()).ok_or_else(overflow)? as u64,
        RecordValue::Lineage(lineage) => native_lineage_owned_bytes(lineage, work)?,
        RecordValue::FlatContext(context) => native_flat_context_owned_bytes(context, work)?,
        RecordValue::FlatChildren(children) => children.capacity().checked_mul(std::mem::size_of::<WorkspaceContextId>()).ok_or_else(overflow)? as u64,
        // These payloads have only fixed identifiers/scalars/Copy metadata.
        // Their enclosing resident vector, output or source Box funds its
        // compiled inline storage; they own no additional dynamic backing.
        RecordValue::ChildCount(_) | RecordValue::ChildIndex(_) | RecordValue::LazyWorkspace(_) => 0,
    })
}

pub(super) fn native_flat_context_owned_bytes(context: &FlatWorkspaceContext, work: &Work) -> Result<u64, Error> {
    let mut bytes = context.roots.0.capacity().checked_mul(std::mem::size_of::<(crate::WorkspaceRootId, FlatWorkspaceContextRoot)>()).ok_or_else(overflow)? as u64;
    for (_, root) in &context.roots.0 {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
        bytes = bytes.checked_add(root.source_path.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(root.workspace_name.capacity() as u64))
            .and_then(|bytes| bytes.checked_add(root.mount_path.as_ref().map_or(0, |path| path.capacity() as u64)))
            .ok_or_else(overflow)?;
    }
    Ok(bytes)
}

fn read<T: DeserializeOwned>(path: &Path, work: &Work, map: impl FnOnce(T) -> RecordValue) -> Result<Option<Owned<RecordValue>>, Error> {
    // Declare the guard before the decoder's physical result.
    let mut held = work.claim(0)?;
    let Some((value, allocation)) = measured_read_file::<Option<T>>(path, work)? else { return Ok(None); };
    let value = transfer(Owned::from_parts(value, allocation), work, &mut held)?;
    let Some(value) = value else { return Ok(None); };
    let mut value = map(value);
    match &mut value {
        RecordValue::FlatContext(value) => measured_order(&mut value.roots.0, work)?,
        RecordValue::FlatChildren(value) => canonical_ids(value, work)?,
        _ => {}
    }
    held.substitute(native_record_owned_bytes(&value, work)?)?;
    Ok(Some(Owned::from_parts(value, held)))
}

pub(super) fn read_native_record(key: RecordKey, path: &Path, work: &Work, cxl: &CancellationToken) -> Result<Option<Owned<RecordValue>>, Error> {
    check(work, cxl)?;
    match key {
        RecordKey::Context(_) => read::<FlatWorkspaceContext>(path, work, RecordValue::FlatContext),
        RecordKey::Children(_) => read::<Vec<WorkspaceContextId>>(path, work, RecordValue::FlatChildren),
        RecordKey::ChildCount(_) => read::<ContextChildCount>(path, work, RecordValue::ChildCount),
        RecordKey::ChildIndex => read::<ContextChildIndex>(path, work, RecordValue::ChildIndex),
        RecordKey::Lineage(_) => read::<WorkspaceLineageRecord>(path, work, RecordValue::Lineage),
        RecordKey::LazyWorkspace(_) => read::<LazyWorkspaceState>(path, work, RecordValue::LazyWorkspace),
    }
}

pub(super) fn load_native_lineage(root: &Namespace, log: &mut CoreLog, id: WorkspaceId, work: &Work, cxl: &CancellationToken) -> Result<Option<Owned<WorkspaceLineageRecord>>, Error> {
    let Some(owned) = log.get_native_value(root, RecordKey::Lineage(id), work, cxl)? else { return Ok(None); };
    let mut held = work.claim(0)?;
    let value = transfer(owned, work, &mut held)?;
    match value {
        RecordValue::Lineage(value) => Ok(Some(Owned::from_parts(value, held))),
        _ => Err(Error::Integrity),
    }
}

pub(super) fn compare_native_lineage(root: &Namespace, log: &mut CoreLog, id: WorkspaceId, expected: u64, replacement: Owned<WorkspaceLineageRecord>, work: &Work, cxl: &CancellationToken) -> Result<bool, Error> {
    check(work, cxl)?;
    let replacement = checked_packet(replacement, work, |lineage| native_lineage_owned_bytes(lineage, work))?;
    let mut held = work.claim(0)?;
    let replacement = transfer(replacement, work, &mut held)?;
    if load_native_lineage(root, log, id, work, cxl)?.as_ref().map_or(0, |value| value.as_state().revision) != expected { return Ok(false); }
    let mut change = NativeChange::new(work)?;
    change.put_owned(RecordKey::Lineage(id), Owned::from_parts(RecordValue::Lineage(replacement), held), work)?;
    log.commit_native_change(root, change, work, cxl)?;
    Ok(true)
}

pub(super) fn load_native_context(root: &Namespace, log: &mut CoreLog, id: WorkspaceContextId, work: &Work, cxl: &CancellationToken) -> Result<Option<Owned<WorkspaceContext>>, Error> {
    let Some(owned) = log.get_native_value(root, RecordKey::Context(id), work, cxl)? else { return Ok(None); };
    let mut held = work.claim(0)?;
    let value = transfer(owned, work, &mut held)?;
    let value = match value {
        RecordValue::Context(value) => value,
        RecordValue::FlatContext(value) => {
            let mut roots = BTreeMap::new();
            // Keep the old flat backing and its funding until conversion ends.
            for (id, root) in &value.roots.0 {
                check(work, cxl)?;
                // Only compiled logical entries are known at the public ABI;
                // opaque std tree backing is excluded, never its deep payload.
                let mut entry = work.claim_allocation(std::mem::size_of::<(crate::WorkspaceRootId, WorkspaceContextRoot)>() as u64, 0)?;
                held.absorb(&mut entry)?;
                roots.insert(*id, WorkspaceContextRoot {
                    root_id: root.root_id, source_path: path(Path::new(&root.source_path), work, &mut held)?,
                    workspace_id: root.workspace_id, workspace_name: string(&root.workspace_name, work, &mut held)?,
                    parent_workspace_id: root.parent_workspace_id,
                    mount_path: root.mount_path.as_deref().map(|value| path(Path::new(value), work, &mut held)).transpose()?,
                });
            }
            let context = WorkspaceContext { version: value.version, revision: value.revision,
                context_id: value.context_id, parent_context_id: value.parent_context_id, roots, state: value.state };
            drop(value);
            let bytes = native_context_owned_bytes(&context, work)?;
            held.substitute(bytes)?;
            context
        }
        _ => return Err(Error::Integrity),
    };
    Ok(Some(Owned::from_parts(value, held)))
}

pub(super) fn native_lineage_owned_bytes(lineage: &WorkspaceLineageRecord, work: &Work) -> Result<u64, Error> {
    work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
    (lineage.workspace_name.capacity() as u64)
        .checked_add(lineage.parent_workspace_name.as_ref().map_or(0, |name| name.capacity() as u64))
        .ok_or_else(overflow)
}

pub(super) fn native_context_owned_bytes(context: &WorkspaceContext, work: &Work) -> Result<u64, Error> {
    let mut bytes = context.roots.len().checked_mul(std::mem::size_of::<(crate::WorkspaceRootId, WorkspaceContextRoot)>()).ok_or_else(overflow)? as u64;
    for root in context.roots.values() {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
        bytes = bytes.checked_add(root.source_path.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(root.workspace_name.capacity() as u64))
            .and_then(|bytes| bytes.checked_add(root.mount_path.as_ref().map_or(0, |path| path.capacity() as u64)))
            .ok_or_else(overflow)?;
    }
    Ok(bytes)
}

fn index_ready(root: &Namespace, log: &mut CoreLog, work: &Work, cxl: &CancellationToken) -> Result<bool, Error> {
    Ok(log.get_native_value(root, RecordKey::ChildIndex, work, cxl)?.is_some())
}

fn decode_filename(name: &[u8], work: &Work) -> Result<Option<WorkspaceContextId>, Error> {
    let Some(stem) = name.strip_suffix(b".json") else { return Ok(None); };
    if stem.len() != 32 { return Ok(None); }
    let mut bytes = [0; 16];
    // Decode the fixed-width metadata one actual character at a time. There is
    // no decoded-byte counter, and malformed names must retain only the real
    // examined prefix rather than charging a guessed opaque codec completion.
    for (index, byte) in stem.iter().copied().enumerate() {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return Ok(None),
        };
        if index % 2 == 0 { bytes[index / 2] = nibble << 4; }
        else { bytes[index / 2] |= nibble; }
    }
    Ok(Some(WorkspaceContextId::from_bytes(bytes)))
}

fn live_ids(root: &Namespace, log: &mut CoreLog, work: &Work, cxl: &CancellationToken) -> Result<Owned<Vec<WorkspaceContextId>>, Error> {
    check(work, cxl)?;
    let mut held = work.claim(0)?;
    let mut ids = Vec::new();
    let mut path_allocation = work.claim(0)?;
    let directory_path = measured_record_path(&root.root, WORKSPACE_CONTEXT_FAMILY, work, &mut path_allocation)?;
    let directory = match crate::native_host::open_root_directory_with_work(&directory_path, &mut &*work) {
        Ok(directory) => Some(directory),
        Err(NativeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(directory) = directory {
        let mut reader_allocation = work.claim(0)?;
        let (mut reader, mut allocation) = crate::native_host::read_dir_held_with_work(&directory, &mut &*work)?;
        reader_allocation.absorb(&mut allocation)?;
        loop {
            check(work, cxl)?;
            let mut entry_allocation = work.claim(0)?;
            #[cfg(unix)]
            let next = reader.next_with_work(&mut &*work)?;
            #[cfg(windows)]
            let next = reader.next_wide_with_work(&mut &*work)?;
            let Some((entry, allocation)) = next else { break; };
            let entry = transfer(Owned::from_parts(entry, allocation), work, &mut entry_allocation)?;
            #[cfg(unix)]
            let id = decode_filename(entry.name.as_encoded_bytes(), work)?;
            #[cfg(windows)]
            let id = {
                let mut ascii = [0_u8; 37];
                if entry.name.len() > ascii.len() || entry.name.iter().any(|unit| *unit > 127) { None }
                else {
                    work.charge(crate::WorkCounters { bytes_copied: entry.name.len() as u64, ..Default::default() })?;
                    for (target, unit) in ascii.iter_mut().zip(&entry.name) { *target = *unit as u8; }
                    decode_filename(&ascii[..entry.name.len()], work)?
                }
            };
            drop(entry);
            drop(entry_allocation);
            if let Some(id) = id { let needed = ids.len() + 1; reserve(&mut ids, needed, work, &mut held)?; ids.push(id); }
        }
    }
    {
        let residents = log.residents();
        for key in residents.keys() {
            check(work, cxl)?;
            if let RecordKey::Context(id) = key {
                let needed = ids.len() + 1;
                reserve(&mut ids, needed, work, &mut held)?;
                ids.push(*id);
            }
        }
    }
    canonical_ids(&mut ids, work)?;
    let mut index = 0;
    while index < ids.len() {
        let id = ids[index];
        match log.get_native_value(root, RecordKey::Context(id), work, cxl)? {
            Some(context) => {
                if context_link(context.as_state())?.0 != id { return Err(Error::Integrity); }
                index += 1;
            }
            None => {
                work.charge(crate::WorkCounters { bytes_copied: ((ids.len() - index - 1) * std::mem::size_of::<WorkspaceContextId>()) as u64, ..Default::default() })?;
                ids.remove(index);
            }
        }
    }
    Ok(Owned::from_parts(ids, held))
}


struct Bucket {
    parent: WorkspaceContextId,
    children: Vec<WorkspaceContextId>,
    allocation: NativeAllocation,
}

fn add_child(bucket: &mut Bucket, child: WorkspaceContextId, work: &Work) -> Result<(), Error> {
    for existing in &bucket.children {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
        if *existing == child { return Ok(()); }
    }
    let needed = bucket.children.len() + 1;
    reserve(&mut bucket.children, needed, work, &mut bucket.allocation)?;
    bucket.children.push(child);
    canonical_ids(&mut bucket.children, work)
}

fn bucket_position(buckets: &[Bucket], parent: WorkspaceContextId, work: &Work) -> Result<Option<usize>, Error> {
    for (index, bucket) in buckets.iter().enumerate() {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
        if bucket.parent == parent { return Ok(Some(index)); }
    }
    Ok(None)
}

fn empty_bucket(buckets: &mut Vec<Bucket>, parent: WorkspaceContextId, work: &Work, held: &mut NativeAllocation) -> Result<usize, Error> {
    if let Some(index) = bucket_position(buckets, parent, work)? { return Ok(index); }
    let allocation = work.claim(0)?;
    let needed = buckets.len() + 1;
    reserve(buckets, needed, work, held)?;
    buckets.push(Bucket { parent, children: Vec::new(), allocation });
    Ok(buckets.len() - 1)
}

fn put_buckets(change: &mut NativeChange, mut buckets: Vec<Bucket>, work: &Work) -> Result<(), Error> {
    // Match ordinary BTreeMap iteration order without allocating another map.
    for index in 1..buckets.len() {
        let mut position = index;
        while position > 0 {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if buckets[position - 1].parent <= buckets[position].parent { break; }
            work.charge(crate::WorkCounters { bytes_copied: (3 * std::mem::size_of::<Bucket>()) as u64, ..Default::default() })?;
            buckets.swap(position - 1, position);
            position -= 1;
        }
    }
    for bucket in buckets {
        let parent = bucket.parent;
        let count = child_bucket_count((!bucket.children.is_empty()).then_some(bucket.children.len()))?;
        if count.is_none() {
            // Retire the actual empty backing before its intrinsic guard; the
            // afterimage removes BOTH bucket and count, just as ordinary CAS.
            drop(bucket);
            change.put_scalar(RecordKey::Children(parent), None, work)?;
            change.put_scalar(RecordKey::ChildCount(parent), None, work)?;
        } else {
            let Bucket { parent, children, allocation } = bucket;
            change.put_owned(RecordKey::Children(parent), Owned::from_parts(RecordValue::FlatChildren(children), allocation), work)?;
            change.put_scalar(RecordKey::ChildCount(parent), count.map(RecordValue::ChildCount), work)?;
        }
    }
    Ok(())
}

fn context_link(value: &RecordValue) -> Result<(WorkspaceContextId, Option<WorkspaceContextId>), Error> {
    match value {
        RecordValue::Context(context) => Ok((context.context_id, context.parent_context_id)),
        RecordValue::FlatContext(context) => Ok((context.context_id, context.parent_context_id)),
        _ => Err(Error::Integrity),
    }
}

fn commit_index(root: &Namespace, log: &mut CoreLog, buckets: Owned<Vec<Bucket>>, work: &Work, cxl: &CancellationToken) -> Result<(), Error> {
    let mut held = work.claim(0)?;
    let buckets = transfer(buckets, work, &mut held)?;
    let mut change = NativeChange::new(work)?;
    put_buckets(&mut change, buckets, work)?;
    drop(held);
    change.put_scalar(RecordKey::ChildIndex, Some(RecordValue::ChildIndex(ContextChildIndex(1))), work)?;
    log.commit_native_change(root, change, work, cxl)
}

fn rebuild_index(root: &Namespace, log: &mut CoreLog, work: &Work, cxl: &CancellationToken) -> Result<(), Error> {
    let ids = live_ids(root, log, work, cxl)?;
    let mut held = work.claim(0)?;
    let mut buckets = Vec::new();
    for id in ids.as_state() {
        let value = log.get_native_value(root, RecordKey::Context(*id), work, cxl)?.ok_or(Error::Integrity)?;
        let (identity, parent) = context_link(value.as_state())?;
        if identity != *id { return Err(Error::Integrity); }
        if let Some(parent) = parent {
            let index = empty_bucket(&mut buckets, parent, work, &mut held)?;
            add_child(&mut buckets[index], identity, work)?;
        }
    }
    commit_index(root, log, Owned::from_parts(buckets, held), work, cxl)
}


pub(super) fn compare_native_context(root: &Namespace, log: &mut CoreLog, id: WorkspaceContextId, expected: u64, replacement: Owned<WorkspaceContext>, work: &Work, cxl: &CancellationToken) -> Result<bool, Error> {
    check(work, cxl)?;
    let replacement = checked_packet(replacement, work, |context| native_context_owned_bytes(context, work))?;
    let mut held = work.claim(0)?;
    let mut replacements = Vec::new();
    reserve(&mut replacements, 1, work, &mut held)?;
    let replacement = transfer(replacement, work, &mut held)?;
    replacements.push(replacement);
    compare_native_contexts(root, log, &[(id, expected)], Owned::from_parts(replacements, held), false, work, cxl)
}

pub(super) fn compare_native_contexts(root: &Namespace, log: &mut CoreLog, expected: &[(WorkspaceContextId, u64)], replacements: Owned<Vec<WorkspaceContext>>, exact: bool, work: &Work, cxl: &CancellationToken) -> Result<bool, Error> {
    check(work, cxl)?;
    let replacements = checked_packet(replacements, work, |contexts| {
        let mut bytes = contexts.capacity().checked_mul(std::mem::size_of::<WorkspaceContext>()).ok_or_else(overflow)? as u64;
        for context in contexts {
            bytes = bytes.checked_add(native_context_owned_bytes(context, work)?).ok_or_else(overflow)?;
        }
        Ok(bytes)
    })?;
    let mut after_allocation = work.claim(0)?;
    let mut after = transfer(replacements, work, &mut after_allocation)?;
    if exact && live_ids(root, log, work, cxl)?.as_state().len() != expected.len() { return Ok(false); }
    let mut before_allocation = work.claim(0)?;
    let mut before = Vec::new();
    reserve(&mut before, expected.len(), work, &mut before_allocation)?;
    for (id, revision) in expected {
        let current = load_native_context(root, log, *id, work, cxl)?;
        if current.as_ref().map_or(0, |value| value.as_state().revision) != *revision { return Ok(false); }
        before.push((*id, current));
    }
    // Stable insertion sort plus remove-earlier duplicates reproduces collect
    // into the ordinary BTreeMap (last replacement for a key wins).
    for index in 1..after.len() {
        let mut position = index;
        while position != 0 {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if after[position - 1].context_id <= after[position].context_id { break; }
            work.charge(crate::WorkCounters { bytes_copied: (3 * std::mem::size_of::<WorkspaceContext>()) as u64, ..Default::default() })?;
            after.swap(position - 1, position);
            position -= 1;
        }
    }
    let mut index = 1;
    while index < after.len() {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
        if after[index - 1].context_id == after[index].context_id {
            work.charge(crate::WorkCounters { bytes_copied: ((after.len() - index) * std::mem::size_of::<WorkspaceContext>()) as u64, ..Default::default() })?;
            let removed_bytes = native_context_owned_bytes(&after[index - 1], work)?;
            drop(after.remove(index - 1));
            after_allocation.release(removed_bytes)?;
        } else { index += 1; }
    }
    for record in &after {
        let mut found = false;
        for (id, _) in expected {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if *id == record.context_id { found = true; break; }
        }
        if !found { return Ok(false); }
    }
    if !index_ready(root, log, work, cxl)? {
        rebuild_index(root, log, work, cxl)?;
    }
    let mut bucket_allocation = work.claim(0)?;
    let mut buckets = Vec::new();
    for parent in before.iter().filter_map(|(_, record)| record.as_ref()?.as_state().parent_context_id)
        .chain(after.iter().filter_map(|record| record.parent_context_id)) {
        if bucket_position(&buckets, parent, work)?.is_some() { continue; }
        let mut held = work.claim(0)?;
        let mut children = Vec::new();
        if let Some(owned) = log.get_native_value(root, RecordKey::Children(parent), work, cxl)? {
            let value = transfer(owned, work, &mut held)?;
            match value {
                RecordValue::FlatChildren(value) => children = value,
                RecordValue::Children(value) => {
                    reserve(&mut children, value.0.len(), work, &mut held)?;
                    for child in value.0 {
                        work.charge(crate::WorkCounters { items_examined: 1, bytes_copied: std::mem::size_of::<WorkspaceContextId>() as u64, ..Default::default() })?;
                        children.push(child);
                    }
                }
                _ => return Err(Error::Integrity),
            }
        }
        let needed = buckets.len() + 1;
        reserve(&mut buckets, needed, work, &mut bucket_allocation)?;
        buckets.push(Bucket { parent, children, allocation: held });
    }
    for replacement in &after {
        let mut old_parent = None;
        let mut old_id = replacement.context_id;
        for (id, record) in &before {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if *id == replacement.context_id {
                if let Some(record) = record { old_parent = record.as_state().parent_context_id; old_id = record.as_state().context_id; }
                break;
            }
        }
        if old_parent == replacement.parent_context_id { continue; }
        if let Some(parent) = old_parent {
            if let Some(index) = bucket_position(&buckets, parent, work)? {
                let bucket = &mut buckets[index];
                let mut found = None;
                for (position, child) in bucket.children.iter().enumerate() {
                    work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                    if *child == old_id { found = Some(position); break; }
                }
                if let Some(position) = found {
                    work.charge(crate::WorkCounters { bytes_copied: ((bucket.children.len() - position - 1) * std::mem::size_of::<WorkspaceContextId>()) as u64, ..Default::default() })?;
                    bucket.children.remove(position);
                }
                // Keep this affected parent even when its last child leaves.
                // put_buckets emits the canonical absence/count afterimages.
            }
        }
        if let Some(parent) = replacement.parent_context_id {
            let index = empty_bucket(&mut buckets, parent, work, &mut bucket_allocation)?;
            add_child(&mut buckets[index], replacement.context_id, work)?;
        }
    }
    drop(before);
    drop(before_allocation);
    let mut change = NativeChange::new(work)?;
    for record in after {
        let bytes = native_context_owned_bytes(&record, work)?;
        let allocation = after_allocation.split_off(bytes)?;
        let key = RecordKey::Context(record.context_id);
        let public = Owned::from_parts(RecordValue::Context(record), allocation);
        let flat = clone_native_record_value(public.as_state(), work, cxl)?;
        drop(public);
        change.put_owned(key, flat, work)?;
    }
    drop(after_allocation);
    put_buckets(&mut change, buckets, work)?;
    drop(bucket_allocation);
    log.commit_native_change(root, change, work, cxl)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_record_vector_requires_full_old_and_new_capacity_before_reallocation() {
        let cancellation = CancellationToken::new();
        let element = std::mem::size_of::<u64>() as u64;
        let budget = crate::WorkBudget {
            peak_allocation_bytes: Work::accounting_state_bytes() + 5 * element,
            ..crate::WorkBudget::UNBOUNDED
        };
        let work = Work::new(budget, &cancellation).expect("exact original capacity limit");
        let mut held = work.claim(0).expect("original guard");
        let mut values = Vec::new();
        reserve(&mut values, 2, &work, &mut held).expect("actual original backing");
        assert_eq!(values.capacity(), 2);
        values.extend([11_u64, 13]);
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        let error = reserve(&mut values, 3, &work, &mut held).expect_err("delta-only funding would wrongly accept");
        assert!(matches!(error, Error::MaterializationWork(NativeError::Work(_))));
        assert_eq!(values, [11, 13]);
        assert_eq!(values.capacity(), 2);
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), live);
        drop(values);
        drop(held);
        assert_eq!(work.live_allocation_bytes(), Work::accounting_state_bytes());
    }
}
