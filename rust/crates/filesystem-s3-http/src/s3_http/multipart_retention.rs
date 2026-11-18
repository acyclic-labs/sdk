/// Authenticates and returns every immutable object retained by active multipart Stream state.
pub async fn active_s3_multipart_objects<P: acyclic_stream::StreamProvider, O: AsyncObjectStore>(
    provider: &P,
    objects: &O,
    protocol_limits: FilesystemS3Limits,
    retention_limits: S3MultipartRetentionLimits,
    budget: crate::WorkBudget,
    cancellation: &CancellationToken,
) -> MultipartStorageResult<crate::OperationReceipt<Vec<ObjectId>>> {
    let protocol_limits = protocol_limits
        .validate()
        .map_err(|_| MultipartStorageError::InvalidLimits)?;
    let retention_limits = retention_limits
        .validate()
        .map_err(|_| MultipartStorageError::InvalidLimits)?;
    cancellation
        .check()
        .map_err(|_| MultipartStorageError::Cancelled)?;
    let (roots, mut work) =
        discover_s3_multipart_roots(provider, protocol_limits, retention_limits).await?;
    work.verify(budget)
        .map_err(|_| MultipartStorageError::BudgetExceeded)?;
    let mut retained = BTreeMap::new();
    for staged in roots {
        cancellation
            .check()
            .map_err(|_| MultipartStorageError::Cancelled)?;
        let remaining = work
            .remaining(budget)
            .map_err(|_| MultipartStorageError::BudgetExceeded)?;
        let (closure, nested) = crate::kernel::prove_blob_closure_async(
            objects,
            staged.root(),
            staged.logical_bytes(),
            ClosureLimits {
                decode: DecodeLimits::default(),
                profile: FilesystemProfile::Portable,
                symbolic_links: false,
                hard_links: false,
                sparse_files: false,
            },
            remaining,
            cancellation,
        )
        .await
        .map_err(|_| MultipartStorageError::InconsistentStream)?;
        work = work
            .checked_add(nested)
            .map_err(|_| MultipartStorageError::InconsistentStream)?;
        if retention_limits
            .maximum_object_bytes
            .is_some_and(|maximum| work.object_bytes_read > maximum)
        {
            return Err(MultipartStorageError::BudgetExceeded);
        }
        for object in closure {
            retained.insert(object, ());
        }
        if retention_limits
            .maximum_objects
            .is_some_and(|maximum| retained.len() as u64 > maximum)
        {
            return Err(MultipartStorageError::BudgetExceeded);
        }
    }
    work.verify(budget)
        .map_err(|_| MultipartStorageError::BudgetExceeded)?;
    Ok(crate::OperationReceipt {
        value: retained.into_keys().collect(),
        work,
    })
}

/// Reads one multipart upload registry stream to completion, validating and collecting every referenced upload path and accruing the work it took.
async fn collect_registry_upload_paths<P: acyclic_stream::StreamProvider>(
    provider: &P,
    registry: acyclic_stream::StreamPath,
    retention_limits: S3MultipartRetentionLimits,
    upload_paths: &mut BTreeMap<String, acyclic_stream::StreamPath>,
    examined: &mut u64,
    work: &mut crate::WorkCounters,
) -> MultipartStorageResult<()> {
    let suffix = registry
        .as_str()
        .strip_prefix("fs/s3-multipart/registry/")
        .ok_or(MultipartStorageError::InconsistentStream)?;
    if suffix.len() != 2 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(MultipartStorageError::InconsistentStream);
    }
    let tail = provider
        .tail(registry.clone())
        .await
        .map_err(MultipartStorageError::Stream)?;
    work.backend_read_operations = work
        .backend_read_operations
        .checked_add(1)
        .ok_or(MultipartStorageError::InconsistentStream)?;
    let mut from = 0_u64;
    while from < tail {
        let available = retention_limits
            .maximum_uploads
            .map_or(tail - from, |maximum| maximum.saturating_sub(*examined));
        if available == 0 {
            return Err(MultipartStorageError::BudgetExceeded);
        }
        let limit = u32::try_from(
            (tail - from)
                .min(available)
                .min(acyclic_stream::MAX_ITEMS as u64),
        )
        .map_err(|_| MultipartStorageError::InconsistentStream)?;
        let mut records = provider
            .read(acyclic_stream::ReadRequest {
                key: None,
                path: registry.clone(),
                from,
                limit,
                at_least: Default::default(),
            })
            .await
            .map_err(MultipartStorageError::Stream)?;
        work.backend_read_operations = work
            .backend_read_operations
            .checked_add(1)
            .ok_or(MultipartStorageError::InconsistentStream)?;
        let before = *examined;
        while let Some(record) = records.next().await {
            let record = record.map_err(MultipartStorageError::Stream)?;
            if record.sequence != from + (*examined - before) {
                return Err(MultipartStorageError::InconsistentStream);
            }
            let path = std::str::from_utf8(&record.value)
                .map_err(|_| MultipartStorageError::InconsistentStream)?;
            let path = acyclic_stream::StreamPath::new(path)
                .map_err(|_| MultipartStorageError::InconsistentStream)?;
            validate_multipart_stream_path(&path)?;
            upload_paths.insert(path.as_str().to_owned(), path);
            work.authority_records_read = work
                .authority_records_read
                .checked_add(1)
                .ok_or(MultipartStorageError::InconsistentStream)?;
            work.authority_bytes_read = work
                .authority_bytes_read
                .checked_add(record.value.len() as u64)
                .ok_or(MultipartStorageError::InconsistentStream)?;
            work.items_examined = work
                .items_examined
                .checked_add(1)
                .ok_or(MultipartStorageError::InconsistentStream)?;
            *examined = examined
                .checked_add(1)
                .ok_or(MultipartStorageError::InconsistentStream)?;
        }
        if *examined == before {
            return Err(MultipartStorageError::InconsistentStream);
        }
        from = from
            .checked_add(*examined - before)
            .ok_or(MultipartStorageError::InconsistentStream)?;
    }
    Ok(())
}

async fn discover_s3_multipart_roots<P: acyclic_stream::StreamProvider>(
    provider: &P,
    protocol_limits: FilesystemS3Limits,
    retention_limits: S3MultipartRetentionLimits,
) -> MultipartStorageResult<(Vec<StagedContent>, crate::WorkCounters)> {
    let root = acyclic_stream::StreamPath::new("fs/s3-multipart/registry")
        .map_err(|_| MultipartStorageError::InconsistentStream)?;
    let (registries, mut work) =
        bounded_children(provider, root, 256, "fs/s3-multipart/registry/").await?;
    let mut upload_paths = BTreeMap::new();
    let mut examined = 0_u64;
    for registry in registries {
        collect_registry_upload_paths(
            provider,
            registry,
            retention_limits,
            &mut upload_paths,
            &mut examined,
            &mut work,
        )
        .await?;
    }
    let mut roots = BTreeMap::new();
    for upload in upload_paths.into_values() {
        let snapshot = read_multipart_snapshot(provider, upload, protocol_limits).await?;
        work = work
            .checked_add(snapshot.work)
            .map_err(|_| MultipartStorageError::InconsistentStream)?;
        if matches!(
            snapshot.terminal,
            Some(MultipartTerminal::Completed(_) | MultipartTerminal::Aborted)
        ) {
            continue;
        }
        for part in snapshot.parts.into_values() {
            roots.insert(part.staged.root(), part.staged);
        }
    }
    Ok((roots.into_values().collect(), work))
}

fn validate_multipart_stream_path(path: &acyclic_stream::StreamPath) -> MultipartStorageResult {
    let suffix = path
        .as_str()
        .strip_prefix("fs/s3-multipart/")
        .ok_or(MultipartStorageError::InconsistentStream)?;
    let mut segments = suffix.split('/');
    let workspace = segments
        .next()
        .ok_or(MultipartStorageError::InconsistentStream)?;
    let upload = segments
        .next()
        .ok_or(MultipartStorageError::InconsistentStream)?;
    if segments.next().is_some()
        || workspace.len() != 32
        || upload.len() != 32
        || !workspace.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !upload.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(MultipartStorageError::InconsistentStream);
    }
    Ok(())
}

async fn bounded_children<P: acyclic_stream::StreamProvider>(
    provider: &P,
    parent: acyclic_stream::StreamPath,
    maximum: u32,
    exact_prefix: &str,
) -> MultipartStorageResult<(Vec<acyclic_stream::StreamPath>, crate::WorkCounters)> {
    let limit = maximum
        .checked_add(1)
        .ok_or(MultipartStorageError::InvalidLimits)?;
    let page = provider
        .children_page(acyclic_stream::ChildrenPageRequest {
            offset: 0,
            tail_stream: None,
            parent: Some(parent),
            limit,
            after: None,
            hierarchy_version: None,
        })
        .await
        .map_err(MultipartStorageError::Stream)?;
    let mut children = Vec::new();
    for child in page.children {
        let path = child.path;
        match path.as_str().strip_prefix(exact_prefix) {
            Some(remainder) if !remainder.contains('/') => {}
            _ => return Err(MultipartStorageError::InconsistentStream),
        }
        children.push(path);
    }
    if children.len() > maximum as usize {
        return Err(MultipartStorageError::BudgetExceeded);
    }
    let child_count = children.len() as u64;
    Ok((
        children,
        crate::WorkCounters {
            backend_read_operations: 1,
            items_examined: child_count,
            ..crate::WorkCounters::default()
        },
    ))
}
