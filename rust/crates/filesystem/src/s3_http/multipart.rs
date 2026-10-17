#[derive(Clone, Debug)]
struct MultipartPart {
    staged: StagedContent,
    etag: String,
}

#[derive(Clone, Debug)]
enum MultipartTerminal {
    Completing(Vec<(u32, String)>),
    Completed(crate::GenerationId),
    Aborted,
}

/// Outcome of driving one multipart upload toward its completion intent.
enum MultipartCompletionIntent {
    /// The upload is admitted at the requested part set and ready to commit.
    Admitted(Box<MultipartSnapshot>),
    /// The upload was already completed at this exact generation.
    AlreadyCompleted(crate::GenerationId),
}

#[derive(Clone, Debug)]
struct MultipartSnapshot {
    key: String,
    tail: u64,
    minimum_part_bytes: u64,
    parts: BTreeMap<u32, MultipartPart>,
    operations: BTreeMap<[u8; 16], (u32, String)>,
    terminal: Option<MultipartTerminal>,
    work: crate::WorkCounters,
}

/// Authenticates and returns every immutable object retained by active multipart Stream state.
pub async fn active_s3_multipart_objects<P: acyclic_stream::StreamProvider, O: AsyncObjectStore>(
    provider: &P,
    objects: &O,
    protocol_limits: FilesystemS3Limits,
    retention_limits: S3MultipartRetentionLimits,
    budget: crate::WorkBudget,
    cancellation: &CancellationToken,
) -> S3Result<crate::OperationReceipt<Vec<ObjectId>>> {
    let protocol_limits = protocol_limits.validate()?;
    let retention_limits = retention_limits.validate()?;
    cancellation
        .check()
        .map_err(|_| s3s::s3_error!(RequestTimeout))?;
    let (roots, mut work) =
        discover_s3_multipart_roots(provider, protocol_limits, retention_limits).await?;
    work.verify(budget).map_err(|_| s3s::s3_error!(SlowDown))?;
    let mut retained = BTreeMap::new();
    for staged in roots {
        cancellation
            .check()
            .map_err(|_| s3s::s3_error!(RequestTimeout))?;
        let remaining = work
            .remaining(budget)
            .map_err(|_| s3s::s3_error!(SlowDown))?;
        let (closure, nested) = crate::kernel::prove_blob_closure_async(
            objects,
            staged.root(),
            staged.logical_bytes(),
            ClosureLimits {
                decode: DecodeLimits::default(),
                maximum_objects: retention_limits.maximum_objects,
                maximum_files: 0,
                maximum_object_bytes: retention_limits.maximum_object_bytes,
                profile: FilesystemProfile::Portable,
                symbolic_links: false,
                hard_links: false,
                sparse_files: false,
            },
            remaining,
            cancellation,
        )
        .await
        .map_err(|_| s3s::s3_error!(InternalError))?;
        work = work
            .checked_add(nested)
            .map_err(|_| s3s::s3_error!(InternalError))?;
        if work.object_bytes_read > retention_limits.maximum_object_bytes {
            return Err(s3s::s3_error!(SlowDown));
        }
        for object in closure {
            retained.insert(object, ());
        }
        if retained.len() as u64 > retention_limits.maximum_objects {
            return Err(s3s::s3_error!(SlowDown));
        }
    }
    work.verify(budget).map_err(|_| s3s::s3_error!(SlowDown))?;
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
) -> S3Result<()> {
    let suffix = registry
        .as_str()
        .strip_prefix("fs/s3-multipart/registry/")
        .ok_or_else(|| s3s::s3_error!(InternalError))?;
    if suffix.len() != 2 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(s3s::s3_error!(InternalError));
    }
    let tail = provider
        .tail(registry.clone())
        .await
        .map_err(|error| stream_read_error(&error))?;
    work.backend_read_operations = work
        .backend_read_operations
        .checked_add(1)
        .ok_or_else(|| s3s::s3_error!(InternalError))?;
    let mut from = 0_u64;
    while from < tail {
        let available = retention_limits.maximum_uploads.saturating_sub(*examined);
        if available == 0 {
            return Err(s3s::s3_error!(SlowDown));
        }
        let limit = u32::try_from(
            (tail - from)
                .min(available)
                .min(acyclic_stream::MAX_ITEMS as u64),
        )
        .map_err(|_| s3s::s3_error!(InternalError))?;
        let mut records = provider
            .read(acyclic_stream::ReadRequest {
                path: registry.clone(),
                from,
                limit,
            })
            .await
            .map_err(|error| stream_read_error(&error))?;
        work.backend_read_operations = work
            .backend_read_operations
            .checked_add(1)
            .ok_or_else(|| s3s::s3_error!(InternalError))?;
        let before = *examined;
        while let Some(record) = records.next().await {
            let record = record.map_err(|error| stream_read_error(&error))?;
            if record.sequence != from + (*examined - before) {
                return Err(s3s::s3_error!(InternalError));
            }
            let path =
                std::str::from_utf8(&record.value).map_err(|_| s3s::s3_error!(InternalError))?;
            let path =
                acyclic_stream::StreamPath::new(path).map_err(|_| s3s::s3_error!(InternalError))?;
            validate_multipart_stream_path(&path)?;
            upload_paths.insert(path.as_str().to_owned(), path);
            work.authority_records_read = work
                .authority_records_read
                .checked_add(1)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
            work.authority_bytes_read = work
                .authority_bytes_read
                .checked_add(record.value.len() as u64)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
            work.items_examined = work
                .items_examined
                .checked_add(1)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
            *examined = examined
                .checked_add(1)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
        }
        if *examined == before {
            return Err(s3s::s3_error!(InternalError));
        }
        from = from
            .checked_add(*examined - before)
            .ok_or_else(|| s3s::s3_error!(InternalError))?;
    }
    Ok(())
}

async fn discover_s3_multipart_roots<P: acyclic_stream::StreamProvider>(
    provider: &P,
    protocol_limits: FilesystemS3Limits,
    retention_limits: S3MultipartRetentionLimits,
) -> S3Result<(Vec<StagedContent>, crate::WorkCounters)> {
    let root = acyclic_stream::StreamPath::new("fs/s3-multipart/registry")
        .map_err(|_| s3s::s3_error!(InternalError))?;
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
            .map_err(|_| s3s::s3_error!(InternalError))?;
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

fn validate_multipart_stream_path(path: &acyclic_stream::StreamPath) -> S3Result {
    let suffix = path
        .as_str()
        .strip_prefix("fs/s3-multipart/")
        .ok_or_else(|| s3s::s3_error!(InternalError))?;
    let mut segments = suffix.split('/');
    let workspace = segments
        .next()
        .ok_or_else(|| s3s::s3_error!(InternalError))?;
    let upload = segments
        .next()
        .ok_or_else(|| s3s::s3_error!(InternalError))?;
    if segments.next().is_some()
        || workspace.len() != 32
        || upload.len() != 32
        || !workspace.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !upload.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(s3s::s3_error!(InternalError));
    }
    Ok(())
}

async fn bounded_children<P: acyclic_stream::StreamProvider>(
    provider: &P,
    parent: acyclic_stream::StreamPath,
    maximum: u32,
    exact_prefix: &str,
) -> S3Result<(Vec<acyclic_stream::StreamPath>, crate::WorkCounters)> {
    let limit = maximum
        .checked_add(1)
        .ok_or_else(|| s3s::s3_error!(InvalidArgument))?;
    let page = provider
        .children_page(acyclic_stream::ChildrenPageRequest {
            parent: Some(parent),
            limit,
            after: None,
            hierarchy_version: None,
        })
        .await
        .map_err(|error| stream_read_error(&error))?;
    let mut children = Vec::new();
    for child in page.children {
        let path = child.path;
        match path.as_str().strip_prefix(exact_prefix) {
            Some(remainder) if !remainder.contains('/') => {}
            _ => return Err(s3s::s3_error!(InternalError)),
        }
        children.push(path);
    }
    if children.len() > maximum as usize {
        return Err(s3s::s3_error!(SlowDown));
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

impl<R, A, O> FilesystemS3Adapter<R, A, O>
where
    R: FilesystemS3Resolver<A, O>,
    A: StreamBackedAuthority,
    O: AsyncObjectStore,
{
    fn multipart_path(
        workspace: &Workspace<A, O>,
        upload_id: &str,
    ) -> S3Result<acyclic_stream::StreamPath> {
        if upload_id.len() != 32 || !upload_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(s3s::s3_error!(NoSuchUpload));
        }
        acyclic_stream::StreamPath::new(format!(
            "fs/s3-multipart/{}/{}",
            hex::encode(workspace.id().into_bytes()),
            upload_id.to_ascii_lowercase()
        ))
        .map_err(|_| s3s::s3_error!(InvalidRequest))
    }

    fn multipart_upload_id(workspace: &Workspace<A, O>, operation: IdempotencyKey) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(MULTIPART_DOMAIN);
        hasher.update(b"upload-id\0");
        hasher.update(&workspace.id().into_bytes());
        hasher.update(&operation.into_bytes());
        hex::encode(&hasher.finalize().as_bytes()[..16])
    }

    fn multipart_registry_path(upload_id: &str) -> S3Result<acyclic_stream::StreamPath> {
        let shard = upload_id
            .get(..2)
            .filter(|value| value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
        acyclic_stream::StreamPath::new(format!(
            "fs/s3-multipart/registry/{}",
            shard.to_ascii_lowercase()
        ))
        .map_err(|_| s3s::s3_error!(InvalidRequest))
    }

    fn multipart_cas_operation(
        upload_id: &str,
        phase: &[u8],
        operation: IdempotencyKey,
        expected_tail: u64,
    ) -> S3Result<acyclic_stream::IdempotencyKey> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(MULTIPART_DOMAIN);
        hasher.update(phase);
        hasher.update(upload_id.as_bytes());
        hasher.update(&operation.into_bytes());
        hasher.update(&expected_tail.to_le_bytes());
        acyclic_stream::IdempotencyKey::new(Bytes::copy_from_slice(hasher.finalize().as_bytes()))
            .map_err(|_| s3s::s3_error!(InvalidRequest))
    }

    async fn multipart_snapshot(
        &self,
        workspace: &Workspace<A, O>,
        upload_id: &str,
    ) -> S3Result<MultipartSnapshot> {
        let path = Self::multipart_path(workspace, upload_id)?;
        let provider = workspace.volume.fs.authority().stream_provider();
        read_multipart_snapshot(provider.as_ref(), path, self.limits).await
    }

    async fn multipart_append(
        &self,
        workspace: &Workspace<A, O>,
        upload_id: &str,
        expected_tail: u64,
        record: Bytes,
        operation: acyclic_stream::IdempotencyKey,
    ) -> S3Result<bool> {
        let provider = workspace.volume.fs.authority().stream_provider();
        match provider
            .append(acyclic_stream::AppendRequest {
                path: Self::multipart_path(workspace, upload_id)?,
                records: vec![record.into()],
                if_tail: Some(expected_tail),
                idempotency_key: Some(operation),
            })
            .await
            .map_err(|error| stream_write_error(&error))?
        {
            acyclic_stream::AppendOutcome::Committed(_) => Ok(true),
            acyclic_stream::AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }

    /// Drives the upload toward a `Completing(requested)` terminal intent, retrying CAS conflicts up to the configured bound.
    async fn admit_multipart_completion_intent(
        &self,
        workspace: &Workspace<A, O>,
        upload_id: &str,
        key: &str,
        operation: IdempotencyKey,
        requested: &[(u32, String)],
    ) -> S3Result<MultipartCompletionIntent> {
        for _ in 0..self.limits.maximum_multipart_cas_retries {
            let mut snapshot = self.multipart_snapshot(workspace, upload_id).await?;
            if snapshot.key != key {
                return Err(s3s::s3_error!(NoSuchUpload));
            }
            match &snapshot.terminal {
                Some(MultipartTerminal::Completed(generation)) => {
                    return Ok(MultipartCompletionIntent::AlreadyCompleted(*generation));
                }
                Some(MultipartTerminal::Completing(existing))
                    if existing.as_slice() == requested =>
                {
                    return Ok(MultipartCompletionIntent::Admitted(Box::new(snapshot)));
                }
                Some(_) => return Err(s3s::s3_error!(NoSuchUpload)),
                None => {
                    validate_completed_parts(
                        &snapshot.parts,
                        requested,
                        snapshot.minimum_part_bytes,
                    )?;
                    if self
                        .multipart_append(
                            workspace,
                            upload_id,
                            snapshot.tail,
                            encode_multipart_completing(requested)?,
                            Self::multipart_cas_operation(
                                upload_id,
                                b"complete-intent\0",
                                operation,
                                snapshot.tail,
                            )?,
                        )
                        .await?
                    {
                        snapshot.tail += 1;
                        snapshot.terminal = Some(MultipartTerminal::Completing(requested.to_vec()));
                        return Ok(MultipartCompletionIntent::Admitted(Box::new(snapshot)));
                    }
                }
            }
        }
        Err(s3s::s3_error!(SlowDown))
    }

    /// Drives the upload from a `Completing` intent to a `Completed` terminal record, retrying CAS conflicts up to the configured bound.
    async fn terminalize_multipart_completion(
        &self,
        workspace: &Workspace<A, O>,
        upload_id: &str,
        operation: IdempotencyKey,
        requested: &[(u32, String)],
        generation: crate::GenerationId,
    ) -> S3Result<bool> {
        for _ in 0..self.limits.maximum_multipart_cas_retries {
            let latest = self.multipart_snapshot(workspace, upload_id).await?;
            match latest.terminal {
                Some(MultipartTerminal::Completed(existing)) if existing == generation => {
                    return Ok(true);
                }
                Some(MultipartTerminal::Completing(ref existing))
                    if existing.as_slice() == requested =>
                {
                    if self
                        .multipart_append(
                            workspace,
                            upload_id,
                            latest.tail,
                            encode_multipart_completed(generation),
                            Self::multipart_cas_operation(
                                upload_id,
                                b"complete-terminal\0",
                                operation,
                                latest.tail,
                            )?,
                        )
                        .await?
                    {
                        return Ok(true);
                    }
                }
                _ => return Err(s3s::s3_error!(InternalError)),
            }
        }
        Ok(false)
    }
}
