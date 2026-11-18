/// Outcome of driving one multipart upload toward its completion intent.
enum MultipartCompletionIntent {
    /// The upload is admitted at the requested part set and ready to commit.
    Admitted(Box<MultipartSnapshot>),
    /// The upload was already completed at this exact generation.
    AlreadyCompleted(acyclic_fs::GenerationId),
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
        read_multipart_snapshot(provider.as_ref(), path, self.limits)
            .await
            .map_err(multipart_storage_error)
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
                idempotency_key: operation,
            })
            .await
            .map_err(|error| stream_write_error(&error))?
        {
            acyclic_stream::AppendOutcome::Committed(_) => Ok(true),
            acyclic_stream::AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }

    /// Conditionally records one completion intent, retaining exact caller resends.
    async fn admit_multipart_completion_intent(
        &self,
        workspace: &Workspace<A, O>,
        upload_id: &str,
        key: &str,
        operation: IdempotencyKey,
        requested: &[(u32, String)],
    ) -> S3Result<MultipartCompletionIntent> {
        let mut snapshot = self.multipart_snapshot(workspace, upload_id).await?;
        if snapshot.key != key {
            return Err(s3s::s3_error!(NoSuchUpload));
        }
        match &snapshot.terminal {
            Some(MultipartTerminal::Completed(generation)) => {
                return Ok(MultipartCompletionIntent::AlreadyCompleted(*generation));
            }
            Some(MultipartTerminal::Completing(existing)) if existing.as_slice() == requested => {
                return Ok(MultipartCompletionIntent::Admitted(Box::new(snapshot)));
            }
            Some(_) => return Err(s3s::s3_error!(NoSuchUpload)),
            None => {
                validate_completed_parts(&snapshot.parts, requested, snapshot.minimum_part_bytes)?;
                if self
                    .multipart_append(
                        workspace,
                        upload_id,
                        snapshot.tail,
                        encode_multipart_completing(requested).map_err(multipart_storage_error)?,
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
        Err(s3s::s3_error!(SlowDown))
    }

    /// Conditionally records completion of the original admitted intent.
    async fn terminalize_multipart_completion(
        &self,
        workspace: &Workspace<A, O>,
        upload_id: &str,
        operation: IdempotencyKey,
        requested: &[(u32, String)],
        generation: acyclic_fs::GenerationId,
    ) -> S3Result<bool> {
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
        Ok(false)
    }
}
