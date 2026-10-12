use super::*;

fn index_ready(
    root: &Namespace,
    log: &mut CoreLog,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<bool, Error> {
    Ok(log
        .get_native_value(root, RecordKey::ChildIndex, work, cxl)?
        .is_some())
}

fn decode_filename(name: &[u8], work: &Work) -> Result<Option<WorkspaceContextId>, Error> {
    let Some(stem) = name.strip_suffix(b".json") else {
        return Ok(None);
    };
    if stem.len() != 32 {
        return Ok(None);
    }
    let mut bytes = [0; 16];
    // Decode the fixed-width metadata one actual character at a time. There is
    // no decoded-byte counter, and malformed names must retain only the real
    // examined prefix rather than charging a guessed opaque codec completion.
    for (index, byte) in stem.iter().copied().enumerate() {
        work.charge(crate::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return Ok(None),
        };
        if index % 2 == 0 {
            bytes[index / 2] = nibble << 4;
        } else {
            bytes[index / 2] |= nibble;
        }
    }
    Ok(Some(WorkspaceContextId::from_bytes(bytes)))
}

fn include_id(
    ids: &mut BTreeSet<WorkspaceContextId>,
    id: WorkspaceContextId,
    work: &Work,
    held: &mut NativeAllocation,
) -> Result<(), Error> {
    work.charge(crate::WorkCounters {
        items_examined: 1,
        ..crate::WorkCounters::UNCHARGED
    })?;
    if !ids.contains(&id) {
        held.absorb(&mut work.claim(
            std::mem::size_of::<WorkspaceContextId>() as u64,
            0,
            crate::WorkCounters::UNCHARGED,
        )?)?;
        ids.insert(id);
    }
    Ok(())
}

fn live_ids(
    root: &Namespace,
    log: &mut CoreLog,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<Owned<BTreeSet<WorkspaceContextId>>, Error> {
    check(work, cxl)?;
    let mut held = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut ids = BTreeSet::new();
    let mut path_allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let directory_path = measured_record_path(
        &root.root,
        WORKSPACE_CONTEXT_FAMILY,
        work,
        &mut path_allocation,
    )?;
    let directory = match crate::native_host::open_root_directory(&directory_path, &mut &*work) {
        Ok((directory, _)) => Some(directory),
        Err(NativeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(directory) = directory {
        let (reader, _reader_allocation) =
            crate::native_host::read_dir_held(&directory, &mut &*work)?;
        let mut reader = reader;
        loop {
            check(work, cxl)?;
            #[cfg(unix)]
            let next = reader.next(&mut &*work)?;
            #[cfg(windows)]
            let next = reader.next_wide(&mut &*work)?;
            let Some((entry, entry_allocation)) = next else {
                break;
            };
            // Retire the entry before its allocation on every exit.
            let entry = entry;
            #[cfg(unix)]
            let id = decode_filename(entry.name.as_encoded_bytes(), work)?;
            #[cfg(windows)]
            let id = {
                let mut ascii = [0_u8; 37];
                if entry.name.len() > ascii.len() || entry.name.iter().any(|unit| *unit > 127) {
                    None
                } else {
                    work.charge(crate::WorkCounters {
                        bytes_copied: entry.name.len() as u64,
                        ..Default::default()
                    })?;
                    for (target, unit) in ascii.iter_mut().zip(&entry.name) {
                        *target = *unit as u8;
                    }
                    decode_filename(&ascii[..entry.name.len()], work)?
                }
            };
            drop(entry);
            drop(entry_allocation);
            if let Some(id) = id {
                include_id(&mut ids, id, work, &mut held)?;
            }
        }
    }
    {
        let residents = log.residents();
        for key in residents.keys() {
            check(work, cxl)?;
            if let RecordKey::Context(id) = key {
                include_id(&mut ids, *id, work, &mut held)?;
            }
        }
    }
    let mut live = BTreeSet::new();
    for id in ids {
        match load::<WorkspaceContext>(root, log, id, work, cxl)? {
            Some(context) => {
                if context.0.context_id != id {
                    return Err(Error::Integrity);
                }
                // Both retained identifiers remain funded during the move.
                held.absorb(&mut work.claim(
                    std::mem::size_of::<WorkspaceContextId>() as u64,
                    0,
                    crate::WorkCounters::UNCHARGED,
                )?)?;
                live.insert(id);
            }
            None => {}
        }
        held.release(std::mem::size_of::<WorkspaceContextId>() as u64)?;
    }
    let ids = live;
    Ok((ids, held))
}

fn rebuild_index(
    root: &Namespace,
    log: &mut CoreLog,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<(), Error> {
    let ids = live_ids(root, log, work, cxl)?;
    let mut held = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut children = WorkspaceContextChildren::new();
    for id in &ids.0 {
        let context =
            load::<WorkspaceContext>(root, log, *id, work, cxl)?.ok_or(Error::Integrity)?;
        if context.0.context_id != *id {
            return Err(Error::Integrity);
        }
        if let Some(parent) = context.0.parent_context_id {
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            if !children.contains_key(&parent) {
                held.absorb(&mut work.claim(
                    std::mem::size_of::<(WorkspaceContextId, BTreeSet<WorkspaceContextId>)>()
                        as u64,
                    0,
                    crate::WorkCounters::UNCHARGED,
                )?)?;
            }
            held.absorb(&mut work.claim(
                std::mem::size_of::<WorkspaceContextId>() as u64,
                0,
                crate::WorkCounters::UNCHARGED,
            )?)?;
            children.entry(parent).or_default().insert(*id);
        }
    }
    let mut change = Change::new(work)?;
    for (parent, descendants) in children {
        let bytes = descendants
            .len()
            .checked_mul(std::mem::size_of::<WorkspaceContextId>())
            .ok_or_else(overflow)? as u64;
        change.put_children(Resident {
            key: RecordKey::Children(parent),
            value: Some(RecordValue::Children(ContextChildren(descendants))),
            allocation: Some(held.split_off(bytes)?),
        })?;
    }
    drop(held);
    change.put(
        (
            RecordKey::ChildIndex,
            Some(RecordValue::ChildIndex(ContextChildIndex(1))),
        )
            .into(),
    )?;
    cxl.check().map_err(|_| NativeError::Cancelled)?;
    log.commit(root, change)
}

pub(in crate::core_state) fn compare_native_context(
    root: &Namespace,
    log: &mut CoreLog,
    id: WorkspaceContextId,
    expected: u64,
    replacement: Owned<WorkspaceContext>,
    work: &Work,
    cxl: &CancellationToken,
) -> Result<bool, Error> {
    check(work, cxl)?;
    let current = load::<WorkspaceContext>(root, log, id, work, cxl)?;
    if current.as_ref().map_or(0, |record| record.0.revision) != expected
        || replacement.0.context_id != id
    {
        return Ok(false);
    }
    if !index_ready(root, log, work, cxl)? {
        rebuild_index(root, log, work, cxl)?;
    }
    let old_parent = current
        .as_ref()
        .and_then(|record| record.0.parent_context_id);
    let new_parent = replacement.0.parent_context_id;
    let old_id = current.as_ref().map_or(id, |record| record.0.context_id);
    let mut held = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut children = WorkspaceContextChildren::new();
    for parent in old_parent.into_iter().chain(new_parent) {
        check(work, cxl)?;
        work.charge(crate::WorkCounters {
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        if children.contains_key(&parent) {
            continue;
        }
        let descendants = match load::<ContextChildren>(root, log, parent, work, cxl)? {
            Some(mut owned) => {
                held.absorb(&mut owned.1)?;
                owned.0.0
            }
            None => BTreeSet::new(),
        };
        held.absorb(&mut work.claim(
            std::mem::size_of::<(WorkspaceContextId, BTreeSet<WorkspaceContextId>)>() as u64,
            0,
            crate::WorkCounters::UNCHARGED,
        )?)?;
        children.insert(parent, descendants);
    }
    if old_parent != new_parent {
        if new_parent.is_some_and(|parent| !children[&parent].contains(&id)) {
            held.absorb(&mut work.claim(
                std::mem::size_of::<WorkspaceContextId>() as u64,
                0,
                crate::WorkCounters::UNCHARGED,
            )?)?;
        }
        let removed = old_parent.is_some_and(|parent| children[&parent].contains(&old_id));
        update_context_children(
            &mut children,
            current.as_ref().map(|record| &record.0),
            Some(&replacement.0),
        );
        if removed {
            held.release(std::mem::size_of::<WorkspaceContextId>() as u64)?;
        }
    }
    drop(current);
    for root in replacement.0.roots.values() {
        check(work, cxl)?;
        root.source_path.to_str().ok_or(Error::Integrity)?;
        if let Some(mount) = &root.mount_path {
            mount.to_str().ok_or(Error::Integrity)?;
        }
    }
    let mut change = Change::new(work)?;
    let (record, allocation) = replacement;
    change.put(Resident {
        key: RecordKey::Context(id),
        value: Some(RecordValue::Context(record)),
        allocation: Some(allocation),
    })?;
    for parent in old_parent.into_iter().chain(new_parent) {
        let descendants = children.remove(&parent);
        let bytes = descendants
            .as_ref()
            .map_or(0, BTreeSet::len)
            .checked_mul(std::mem::size_of::<WorkspaceContextId>())
            .ok_or_else(overflow)? as u64;
        change.put_children(Resident {
            key: RecordKey::Children(parent),
            value: descendants.map(|set| RecordValue::Children(ContextChildren(set))),
            allocation: Some(held.split_off(bytes)?),
        })?;
        if old_parent == new_parent {
            break;
        }
    }
    drop(children);
    drop(held);
    cxl.check().map_err(|_| NativeError::Cancelled)?;
    log.commit(root, change)?;
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
        let mut held = work
            .claim(0, 0, crate::WorkCounters::UNCHARGED)
            .expect("original guard");
        let mut values = Vec::new();
        reserve(&mut values, 2, &work, &mut held).expect("actual original backing");
        assert_eq!(values.capacity(), 2);
        values.extend([11_u64, 13]);
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        let error = reserve(&mut values, 3, &work, &mut held)
            .expect_err("delta-only funding would wrongly accept");
        assert!(matches!(error, NativeError::Work(_)));
        assert_eq!(values, [11, 13]);
        assert_eq!(values.capacity(), 2);
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), live);
        drop(values);
        drop(held);
        assert_eq!(work.live_allocation_bytes(), Work::accounting_state_bytes());
    }
}
