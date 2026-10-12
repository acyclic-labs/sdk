use super::*;

pub(super) struct ProjectedEntry {
    pub(super) name: Vec<u16>,
    pub(super) component: Vec<u8>,
    pub(super) facts: EntryFacts,
    pub(super) info: PRJ_FILE_BASIC_INFO,
    pub(super) symlink_target: Option<crate::kernel::OwnedSourceChunk>,
    pub(super) directory_link: bool,
    pub(super) _source_ownership: crate::demand::SourcePayloadOwnership,
    pub(super) _allocation: crate::NativeAllocation,
}

pub(super) unsafe fn end_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
) -> HRESULT {
    HR_OK
}

pub(super) unsafe fn get_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
    _search_expression: PCWSTR,
    _buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    HR_OK
}

pub(super) unsafe fn placeholder(_callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    HR_FILE_NOT_FOUND
}

pub(super) fn projected_entry(
    projection: Projection<'_>,
    directory: &MountPath,
    component: (Vec<u8>, crate::demand::SourcePayloadOwnership),
    lookup: MountLookup,
    pin: Option<MountContentPin>,
) -> Result<Option<ProjectedEntry>, MountSourceError> {
    let (component, ownership) = component;
    let Some(info) = basic(lookup.node, Some(lookup.metadata)) else {
        return Ok(None);
    };
    let copied = crate::WorkCounters {
        bytes_copied: component.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    projection.work.admit(copied)?;
    let mut allocation = projection.work.claim(
        (component.len() as u64)
            .checked_add(2)
            .ok_or(crate::WorkError::Overflow)?,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    let Some(mut name) = decode_utf16_name(&component).filter(|name| !name.contains(&0)) else {
        return Ok(None);
    };
    name.try_reserve_exact(1)
        .map_err(MountSourceError::Allocation)?;
    allocation.reconcile_capacity((name.capacity() * size_of::<u16>()) as u64)?;
    name.push(0);
    projection.work.record_admitted(copied)?;
    let symlink_target = if lookup.node.kind == MountNodeKind::SymbolicLink {
        let target = projection
            .runtime
            .source
            .as_ref()
            .read_link(&directory.child(component.clone()), projection.work)?;
        if symlink_extended(target.bytes()).is_none() {
            return Ok(None);
        }
        Some(target)
    } else {
        None
    };
    let directory_link = symlink_target.as_ref().is_some_and(|target| {
        links_to_directory(
            projection,
            directory,
            lookup.metadata.windows_attributes,
            target.bytes(),
        )
    });
    Ok(Some(ProjectedEntry {
        name,
        component,
        facts: EntryFacts { lookup, pin },
        info,
        symlink_target,
        directory_link,
        _source_ownership: ownership,
        _allocation: allocation,
    }))
}

pub(super) fn links_to_directory(
    projection: Projection<'_>,
    directory: &MountPath,
    attributes: MetadataField<u32>,
    target: &[u8],
) -> bool {
    if let MetadataField::Value(attributes) = attributes {
        return attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0;
    }
    decode_utf16_name(target)
        .is_some_and(|target| resolves_to_directory(projection, directory, &target, 0))
}

pub(super) fn resolves_to_directory(
    projection: Projection<'_>,
    directory: &MountPath,
    target: &[u16],
    hops: u8,
) -> bool {
    const MOST_HOPS: u8 = 8;
    let text = String::from_utf16_lossy(target);
    if text.starts_with(['/', '\\']) || text.get(1..2) == Some(":") {
        return std::path::Path::new(&text).is_dir();
    }
    let mut path = directory.clone();
    for part in text.split(['/', '\\']) {
        match part {
            "" | "." => {}
            ".." => match path.parent() {
                Some(parent) => path = parent,
                None => return false,
            },
            name => path = path.child(name.encode_utf16().flat_map(u16::to_le_bytes).collect()),
        }
    }
    match projection
        .runtime
        .source
        .as_ref()
        .lookup_with_meter(&path, projection.work)
    {
        Ok(Some(lookup)) if lookup.node.kind == MountNodeKind::Directory => true,
        Ok(Some(lookup)) if lookup.node.kind == MountNodeKind::SymbolicLink && hops < MOST_HOPS => {
            let (Some(parent), Ok(next)) = (
                path.parent(),
                projection
                    .runtime
                    .source
                    .as_ref()
                    .read_link(&path, projection.work),
            ) else {
                return false;
            };
            decode_utf16_name(next.bytes())
                .is_some_and(|next| resolves_to_directory(projection, &parent, &next, hops + 1))
        }
        _ => false,
    }
}

pub(super) fn owned_component(
    projection: Projection<'_>,
    component: &[u8],
) -> Result<(Vec<u8>, crate::demand::SourcePayloadOwnership), MountSourceError> {
    let copied = crate::WorkCounters {
        bytes_copied: component.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    projection.work.admit(copied)?;
    let mut allocation = projection.work.claim(
        component.len() as u64,
        u64::from(!component.is_empty()),
        crate::WorkCounters::UNCHARGED,
    )?;
    let mut value = Vec::new();
    value
        .try_reserve_exact(component.len())
        .map_err(MountSourceError::Allocation)?;
    allocation.reconcile_capacity(value.capacity() as u64)?;
    value.extend_from_slice(component);
    projection.work.record_admitted(copied)?;
    Ok((
        value,
        crate::demand::SourcePayloadOwnership::from_native_allocation(allocation),
    ))
}

pub(super) fn source_listing(
    projection: Projection<'_>,
    path: &MountPath,
) -> Result<(Vec<ProjectedEntry>, crate::NativeAllocation), MountSourceError> {
    let lease = projection
        .runtime
        .source
        .as_ref()
        .acquire_binding_lease_with_meter(None, projection.work)?;
    let mut cursors = (
        Vec::<(Vec<u8>, crate::demand::SourcePayloadOwnership)>::new(),
        projection
            .work
            .claim(0, 0, crate::WorkCounters::UNCHARGED)?,
    );
    let mut entries = (
        Vec::<ProjectedEntry>::new(),
        projection
            .work
            .claim(0, 0, crate::WorkCounters::UNCHARGED)?,
    );
    loop {
        let page = projection
            .runtime
            .source
            .as_ref()
            .read_directory_with_meter(
                path,
                cursors.0.last().map(|(cursor, _)| cursor.as_slice()),
                DIRECTORY_PAGE_SIZE,
                projection.work,
            )?;
        let (page_entries, next_cursor, mut ownership) = page.into_parts();
        reserve(
            &mut entries.0,
            page_entries.len(),
            &mut entries.1,
            projection.work,
        )?;
        for entry in page_entries {
            projection.work.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            let lookup = MountLookup {
                node: entry.node,
                metadata: entry.metadata,
            };
            let component_ownership = ownership.split_owned_payload(entry.name.capacity() as u64)?;
            if let Some(projected) = projected_entry(
                projection,
                path,
                (entry.name, component_ownership),
                lookup,
                entry.pin,
            )? {
                let mut low = 0;
                let mut high = entries.0.len();
                let mut collision = false;
                while low < high {
                    projection.work.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED
                    })?;
                    let middle = low + (high - low) / 2;
                    // SAFETY: both names include NUL and retain their allocations.
                    let order = unsafe {
                        PrjFileNameCompare(
                            PCWSTR::from_raw(entries.0[middle].name.as_ptr()),
                            PCWSTR::from_raw(projected.name.as_ptr()),
                        )
                    };
                    if order == 0 {
                        collision = true;
                        break;
                    }
                    if order < 0 {
                        low = middle + 1;
                    } else {
                        high = middle;
                    }
                }
                if !collision {
                    projection.work.charge(crate::WorkCounters {
                        bytes_copied: ((entries.0.len() - low) * size_of::<ProjectedEntry>())
                            as u64,
                        ..crate::WorkCounters::UNCHARGED
                    })?;
                    entries.0.insert(low, projected);
                }
            }
        }
        let Some(next) = next_cursor else {
            break;
        };
        let next_ownership = ownership.split_owned_payload(next.capacity() as u64)?;
        for (previous, _) in &cursors.0 {
            projection.work.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            if previous == &next {
                return Err(MountSourceError::Invalid(
                    "directory listing repeated a cursor".to_owned(),
                ));
            }
        }
        reserve(&mut cursors.0, 1, &mut cursors.1, projection.work)?;
        cursors.0.push((next, next_ownership));
    }
    drop(lease);
    Ok(entries)
}

pub(super) unsafe fn file_data(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _byte_offset: u64,
    _length: u32,
) -> HRESULT {
    HR_UNEXPECTED
}
