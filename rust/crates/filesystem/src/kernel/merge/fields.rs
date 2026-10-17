enum OptionalResolution {
    Resolved(Option<FileRecord>),
    Conflict,
}

fn resolve_optional(
    base: Option<FileRecord>,
    ours: Option<FileRecord>,
    theirs: Option<FileRecord>,
) -> OptionalResolution {
    if ours == theirs || theirs == base {
        OptionalResolution::Resolved(ours)
    } else if ours == base {
        OptionalResolution::Resolved(theirs)
    } else {
        OptionalResolution::Conflict
    }
}

fn select_optional(
    side: MergeConflictSide,
    base: Option<FileRecord>,
    ours: Option<FileRecord>,
    theirs: Option<FileRecord>,
) -> Option<FileRecord> {
    match side {
        MergeConflictSide::Base => base,
        MergeConflictSide::Ours => ours,
        MergeConflictSide::Theirs => theirs,
    }
}

fn select_value<T: Clone>(side: MergeConflictSide, base: &T, ours: &T, theirs: &T) -> T {
    match side {
        MergeConflictSide::Base => base.clone(),
        MergeConflictSide::Ours => ours.clone(),
        MergeConflictSide::Theirs => theirs.clone(),
    }
}

pub(crate) fn resolve_three<T: Clone + Eq>(base: &T, ours: &T, theirs: &T) -> Option<T> {
    if ours == theirs || theirs == base {
        Some(ours.clone())
    } else if ours == base {
        Some(theirs.clone())
    } else {
        None
    }
}

async fn merge_file_fields_async<S: AsyncObjectStore>(
    store: &S,
    base: FileRecord,
    ours: FileRecord,
    theirs: FileRecord,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<FileRecord>, FsError> {
    let (Some(kind), Some(payload)) = (
        resolve_three(&base.kind, &ours.kind, &theirs.kind),
        resolve_three(&base.payload, &ours.payload, &theirs.payload),
    ) else {
        return Ok(None);
    };
    let metadata = merge_metadata_async(
        store,
        Some(base.metadata),
        ours.metadata,
        theirs.metadata,
        limits,
        accounting,
        cancellation,
    )
    .await?;
    Ok(metadata.map(|metadata| FileRecord {
        file_id: ours.file_id,
        kind,
        link_count: ours.link_count,
        metadata,
        payload,
    }))
}

async fn merge_records_async<S: AsyncObjectStore>(
    store: &S,
    [base, ours, theirs]: [Option<FileRecord>; 3],
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<FileRecord>, FsError> {
    let (Some(ours), Some(theirs)) = (ours, theirs) else {
        return Ok(None);
    };
    if let Some(base) = base {
        return merge_file_fields_async(
            store,
            base,
            ours,
            theirs,
            limits,
            accounting,
            cancellation,
        )
        .await;
    }
    if ours.kind != theirs.kind
        || ours.link_count != theirs.link_count
        || ours.payload != theirs.payload
    {
        return Ok(None);
    }
    let metadata = merge_metadata_async(
        store,
        None,
        ours.metadata,
        theirs.metadata,
        limits,
        accounting,
        cancellation,
    )
    .await?;
    Ok(metadata.map(|metadata| FileRecord { metadata, ..ours }))
}

async fn merge_metadata_async<S: AsyncObjectStore>(
    store: &S,
    base: Option<ObjectId>,
    ours: ObjectId,
    theirs: ObjectId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<ObjectId>, FsError> {
    if ours == theirs {
        return Ok(Some(ours));
    }
    if let Some(resolved) = base.and_then(|base| resolve_three(&base, &ours, &theirs)) {
        return Ok(Some(resolved));
    }
    let ours_fields = read_metadata(store, ours, limits, accounting, cancellation).await?;
    let theirs_fields = read_metadata(store, theirs, limits, accounting, cancellation).await?;
    let (merged, base_fields) = if let Some(base) = base {
        let fields = read_metadata(store, base, limits, accounting, cancellation).await?;
        (
            merge_metadata_fields(fields, ours_fields, theirs_fields),
            Some(fields),
        )
    } else {
        (
            merge_added_metadata_fields(ours_fields, theirs_fields),
            None,
        )
    };
    let Some(merged) = merged else {
        return Ok(None);
    };
    if merged == ours_fields {
        return Ok(Some(ours));
    }
    if merged == theirs_fields {
        return Ok(Some(theirs));
    }
    if Some(merged) == base_fields {
        return Ok(base);
    }
    write_metadata(store, merged, accounting, cancellation)
        .await
        .map(Some)
}

async fn write_metadata<S: AsyncObjectStore>(
    store: &S,
    metadata: FileMetadata,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    let (encoded, allocation) =
        crate::kernel::codec::encode_owned(accounting, |observe, reserve| {
            super::metadata::encode_file_metadata(metadata, observe, reserve)
        })?;
    let bytes = crate::foundation::usize_to_u64(encoded.capacity());
    put_encoded_with_accounting(
        store,
        ObjectKind::Metadata,
        Owned::new(encoded, allocation, bytes),
        accounting,
        cancellation,
    )
    .await
}

async fn empty_tree_async<S: AsyncObjectStore>(
    store: &S,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    let (encoded, allocation) =
        crate::kernel::codec::encode_owned(accounting, |observe, _reserve| {
            cancellation.check()?;
            super::tree::encode_tree_page(
                &super::TreePage::Leaf(Vec::new()),
                limits.maximum_page_items,
                observe,
                crate::FsError::EncodedAllocationFailed,
            )
        })?;
    let bytes = crate::foundation::usize_to_u64(encoded.capacity());
    put_encoded_with_accounting(
        store,
        ObjectKind::TreePage,
        Owned::new(encoded, allocation, bytes),
        accounting,
        cancellation,
    )
    .await
}

async fn read_metadata<S: AsyncObjectStore>(
    store: &S,
    object: ObjectId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<FileMetadata, FsError> {
    let (read, retained) = super::file_read::read_object_with_accounting(
        store,
        object,
        limits.maximum_object_bytes,
        accounting,
        cancellation,
    )
    .await?;
    let result = (|| {
        accounting.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        decode_file_metadata(&read.bytes, limits, &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })
        .map_err(FsError::from)
    })();
    drop(read);
    // Native provider bytes own their original guard internally; only the
    // standalone provider retention remains on the caller's ledger.
    if retained != 0 {
        retire_allocation(KernelOwnedAllocation::Standalone, retained, accounting)?;
    }
    result
}

async fn converge_metadata_async<S: AsyncObjectStore>(
    store: &S,
    ours: ObjectId,
    theirs: ObjectId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<ObjectId>, FsError> {
    // The added-record rule is exactly authored fields agree / later times.
    merge_metadata_async(store, None, ours, theirs, limits, accounting, cancellation).await
}

pub(crate) fn merge_metadata_fields(
    base: FileMetadata,
    ours: FileMetadata,
    theirs: FileMetadata,
) -> Option<FileMetadata> {
    fn later(
        base: &MetadataField<i64>,
        ours: &MetadataField<i64>,
        theirs: &MetadataField<i64>,
    ) -> MetadataField<i64> {
        resolve_three(base, ours, theirs).unwrap_or(match (ours, theirs) {
            (MetadataField::Value(ours), MetadataField::Value(theirs)) => {
                MetadataField::Value(*ours.max(theirs))
            }
            (MetadataField::Value(value), MetadataField::Unavailable)
            | (MetadataField::Unavailable, MetadataField::Value(value)) => {
                MetadataField::Value(*value)
            }
            (MetadataField::Unavailable, MetadataField::Unavailable) => MetadataField::Unavailable,
        })
    }
    Some(FileMetadata {
        posix_mode: resolve_three(&base.posix_mode, &ours.posix_mode, &theirs.posix_mode)?,
        posix_uid: resolve_three(&base.posix_uid, &ours.posix_uid, &theirs.posix_uid)?,
        posix_gid: resolve_three(&base.posix_gid, &ours.posix_gid, &theirs.posix_gid)?,
        posix_flags: resolve_three(&base.posix_flags, &ours.posix_flags, &theirs.posix_flags)?,
        windows_attributes: resolve_three(
            &base.windows_attributes,
            &ours.windows_attributes,
            &theirs.windows_attributes,
        )?,
        created_ns: later(&base.created_ns, &ours.created_ns, &theirs.created_ns),
        modified_ns: later(&base.modified_ns, &ours.modified_ns, &theirs.modified_ns),
        accessed_ns: later(&base.accessed_ns, &ours.accessed_ns, &theirs.accessed_ns),
        changed_ns: later(&base.changed_ns, &ours.changed_ns, &theirs.changed_ns),
        named_attributes: resolve_three(
            &base.named_attributes,
            &ours.named_attributes,
            &theirs.named_attributes,
        )?,
        acl: resolve_three(&base.acl, &ours.acl, &theirs.acl)?,
        security_descriptor: resolve_three(
            &base.security_descriptor,
            &ours.security_descriptor,
            &theirs.security_descriptor,
        )?,
    })
}

pub(crate) fn merge_added_metadata_fields(
    ours: FileMetadata,
    theirs: FileMetadata,
) -> Option<FileMetadata> {
    fn same<T: Copy + PartialEq>(ours: T, theirs: T) -> Option<T> {
        (ours == theirs).then_some(ours)
    }
    fn later(ours: MetadataField<i64>, theirs: MetadataField<i64>) -> MetadataField<i64> {
        match (ours, theirs) {
            (MetadataField::Value(ours), MetadataField::Value(theirs)) => {
                MetadataField::Value(ours.max(theirs))
            }
            (MetadataField::Value(value), MetadataField::Unavailable)
            | (MetadataField::Unavailable, MetadataField::Value(value)) => {
                MetadataField::Value(value)
            }
            (MetadataField::Unavailable, MetadataField::Unavailable) => MetadataField::Unavailable,
        }
    }
    Some(FileMetadata {
        posix_mode: same(ours.posix_mode, theirs.posix_mode)?,
        posix_uid: same(ours.posix_uid, theirs.posix_uid)?,
        posix_gid: same(ours.posix_gid, theirs.posix_gid)?,
        posix_flags: same(ours.posix_flags, theirs.posix_flags)?,
        windows_attributes: same(ours.windows_attributes, theirs.windows_attributes)?,
        created_ns: later(ours.created_ns, theirs.created_ns),
        modified_ns: later(ours.modified_ns, theirs.modified_ns),
        accessed_ns: later(ours.accessed_ns, theirs.accessed_ns),
        changed_ns: later(ours.changed_ns, theirs.changed_ns),
        named_attributes: same(ours.named_attributes, theirs.named_attributes)?,
        acl: same(ours.acl, theirs.acl)?,
        security_descriptor: same(ours.security_descriptor, theirs.security_descriptor)?,
    })
}

fn is_directory(record: Option<FileRecord>) -> bool {
    matches!(record, Some(record) if record.kind == super::FileKind::Directory)
}
fn directory_entries(record: Option<FileRecord>) -> Option<ObjectId> {
    match record?.payload {
        FilePayload::Directory { entries } => Some(entries),
        _ => None,
    }
}
pub(crate) fn file_table_mutation(
    file_id: FileId,
    before: Option<FileRecord>,
    after: Option<FileRecord>,
) -> Option<FileTableMutation> {
    match (before, after) {
        (None, Some(record)) => Some(FileTableMutation::Insert(record)),
        (Some(expected), None) => Some(FileTableMutation::Remove {
            file_id,
            expected: Some(expected),
        }),
        (Some(expected), Some(replacement)) => Some(FileTableMutation::Replace {
            expected,
            replacement,
        }),
        (None, None) => None,
    }
}
#[cfg(test)]
pub(crate) fn tree_mutation(
    name: LogicalName,
    before: Option<super::TreeEntry>,
    after: Option<super::TreeEntry>,
) -> Option<TreeMutation> {
    match (before, after) {
        (None, Some(entry)) => Some(TreeMutation::Insert(entry)),
        (Some(expected), None) => Some(TreeMutation::Remove {
            name,
            expected_file_id: Some(expected.file_id),
        }),
        (Some(expected), Some(entry)) => Some(TreeMutation::Replace {
            entry,
            expected_file_id: expected.file_id,
        }),
        (None, None) => None,
    }
}
