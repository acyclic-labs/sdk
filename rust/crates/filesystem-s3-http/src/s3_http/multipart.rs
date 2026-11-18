/// Outcome of driving one multipart upload toward its completion intent.
enum MultipartCompletionIntent {
    /// The upload is admitted at the requested part set and ready to commit.
    Admitted(Box<MultipartSnapshot>),
    /// The upload was already completed at this exact generation.
    AlreadyCompleted(acyclic_fs_format::object_reference::GenerationId),
}

impl<R, A, O> FilesystemS3Adapter<R, A, O>
where
    R: FilesystemS3Resolver<A, O>,
    A: StreamBackedAuthority,
    O: AsyncObjectStore,
{
    async fn multipart_snapshot(
        &self,
        workspace: &Workspace<A, O>,
        upload_id: &str,
    ) -> S3Result<MultipartSnapshot> {
        workspace
            .multipart_snapshot(upload_id, self.limits)
            .await
            .map_err(multipart_storage_error)
    }

    async fn multipart_append(
        &self,
        workspace: &Workspace<A, O>,
        upload_id: &str,
        expected_tail: u64,
        record: acyclic_fs::s3_multipart::MultipartUpdateRecord,
        operation: IdempotencyKey,
    ) -> S3Result<bool> {
        workspace
            .append_multipart_record(upload_id, expected_tail, record, operation)
            .await
            .map_err(multipart_storage_error)
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
                        operation,
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
        generation: acyclic_fs_format::object_reference::GenerationId,
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
                        operation,
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
