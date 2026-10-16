impl<R, A, O> FilesystemS3Adapter<R, A, O>
where
    R: FilesystemS3Resolver<A, O>,
    A: StreamBackedAuthority + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn s3_create_bucket(
        &self,
        request: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        if request.input.acl.is_some()
            || request.input.create_bucket_configuration.is_some()
            || request.input.grant_full_control.is_some()
            || request.input.grant_read.is_some()
            || request.input.grant_read_acp.is_some()
            || request.input.grant_write.is_some()
            || request.input.grant_write_acp.is_some()
            || request.input.object_lock_enabled_for_bucket.is_some()
            || request.input.object_ownership.is_some()
        {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let access_key = self.authenticated_access_key(&request).await?;
        let bucket = &request.input.bucket;
        validate_bucket_name(bucket)?;
        self.resolver
            .create_bucket(access_key, request_session_token(&request), bucket)
            .await?;
        self.scoped_principal(&request, bucket, true).await?;
        Ok(S3Response::new(CreateBucketOutput {
            bucket_arn: None,
            location: Some(format!("/{bucket}")),
        }))
    }

    async fn s3_list_buckets(
        &self,
        request: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        if request.input.bucket_region.is_some()
            || request.input.continuation_token.is_some()
            || request.input.max_buckets.is_some()
            || request.input.prefix.is_some()
        {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let access_key = self.authenticated_access_key(&request).await?;
        let mut names = self
            .resolver
            .list_buckets(access_key, request_session_token(&request))
            .await?;
        if names.len() > self.limits.maximum_list_entries_examined as usize {
            return Err(s3s::s3_error!(SlowDown));
        }
        names.sort_unstable();
        if names.windows(2).any(|pair| pair.first() == pair.last()) {
            return Err(s3s::s3_error!(InternalError));
        }
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(
                names
                    .into_iter()
                    .map(|name| Bucket {
                        name: Some(name),
                        ..Bucket::default()
                    })
                    .collect(),
            ),
            ..ListBucketsOutput::default()
        }))
    }

    async fn s3_delete_bucket(
        &self,
        request: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        if request.input.expected_bucket_owner.is_some() {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let principal = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?;
        if principal.bucket != request.input.bucket {
            return Err(s3s::s3_error!(AccessDenied));
        }
        let access_key = self.authenticated_access_key(&request).await?;
        self.resolver
            .delete_bucket(
                access_key,
                request_session_token(&request),
                &request.input.bucket,
            )
            .await?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn s3_create_multipart_upload(
        &self,
        request: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        validate_http_key(&request.input.key, self.limits.maximum_key_bytes)?;
        reject_create_multipart_extensions(&request.input)?;
        let operation = request_operation(&request)?;
        let upload_id = Self::multipart_upload_id(&workspace, operation);
        let path = Self::multipart_path(&workspace, &upload_id)?;
        let provider = workspace.volume.fs.authority().stream_provider();
        let record = encode_multipart_created(
            &request.input.key,
            self.limits.maximum_multipart_parts,
            self.limits.maximum_multipart_part_bytes,
            self.limits.minimum_multipart_part_bytes,
        )?;
        let registry = Self::multipart_registry_path(&upload_id)?;
        for _ in 0..self.limits.maximum_multipart_cas_retries {
            let (registry_condition, registry_tail) = match provider.tail(registry.clone()).await {
                Ok(tail) => (
                    acyclic_stream::CommitCondition::Tail {
                        path: registry.clone(),
                        expected: tail,
                    },
                    tail,
                ),
                Err(acyclic_stream::StreamError::NotFound) => (
                    acyclic_stream::CommitCondition::Absent {
                        path: registry.clone(),
                    },
                    0,
                ),
                Err(error) => return Err(stream_read_error(&error)),
            };
            let attempt =
                Self::multipart_cas_operation(&upload_id, b"create\0", operation, registry_tail)?;
            match provider
                .commit(acyclic_stream::CommitRequest {
                    conditions: vec![
                        registry_condition,
                        acyclic_stream::CommitCondition::Absent { path: path.clone() },
                    ],
                    mutations: vec![
                        acyclic_stream::CommitMutation::Append {
                            path: registry.clone(),
                            records: vec![Bytes::copy_from_slice(path.as_str().as_bytes()).into()],
                        },
                        acyclic_stream::CommitMutation::Append {
                            path: path.clone(),
                            records: vec![record.clone().into()],
                        },
                    ],
                    idempotency_key: attempt,
                })
                .await
                .map_err(|error| stream_write_error(&error))?
            {
                acyclic_stream::CommitOutcome::Committed(_) => break,
                acyclic_stream::CommitOutcome::Conflict(_) => {
                    if let Ok(snapshot) = self.multipart_snapshot(&workspace, &upload_id).await {
                        if snapshot.key != request.input.key {
                            return Err(s3s::s3_error!(InvalidRequest));
                        }
                        break;
                    }
                    continue;
                }
            }
        }
        if provider.tail(path.clone()).await.is_err() {
            return Err(s3s::s3_error!(SlowDown));
        }
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(request.input.bucket),
            key: Some(request.input.key),
            upload_id: Some(upload_id),
            ..CreateMultipartUploadOutput::default()
        }))
    }

    async fn s3_upload_part(
        &self,
        mut request: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        reject_upload_part_extensions(&request.input)?;
        let expected_crc32 = parse_crc32(request.input.checksum_crc32.as_deref())?;
        let part_number = u32::try_from(request.input.part_number)
            .map_err(|_| s3s::s3_error!(InvalidArgument))?;
        if part_number == 0 || part_number > self.limits.maximum_multipart_parts {
            return Err(s3s::s3_error!(InvalidArgument));
        }
        let operation = request_operation(&request)?;
        let transaction = workspace
            .begin_transaction(operation, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| s3_error(&S3Error::Workspace(failure.error)))?
            .value;
        let mut source =
            StreamingBodySource::new(request.input.body.take(), expected_crc32.is_some());
        let staged = transaction
            .stage_content(
                &mut source,
                self.limits.maximum_multipart_part_bytes,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| s3_error(&S3Error::Workspace(failure.error)))?
            .value;
        if request
            .input
            .content_length
            .is_some_and(|length| u64::try_from(length).ok() != Some(staged.logical_bytes()))
        {
            return Err(s3s::s3_error!(IncompleteBody));
        }
        verify_crc32(&source, expected_crc32)?;
        let etag_value = format!("\"{}\"", hex::encode(staged.root().digest.as_bytes()));
        for _ in 0..self.limits.maximum_multipart_cas_retries {
            let snapshot = self
                .multipart_snapshot(&workspace, &request.input.upload_id)
                .await?;
            if snapshot.key != request.input.key || snapshot.terminal.is_some() {
                return Err(s3s::s3_error!(NoSuchUpload));
            }
            if let Some((existing_number, existing_etag)) =
                snapshot.operations.get(&operation.into_bytes())
            {
                if *existing_number != part_number || existing_etag != &etag_value {
                    return Err(s3s::s3_error!(InvalidRequest));
                }
                return Ok(S3Response::new(UploadPartOutput {
                    checksum_crc32: request.input.checksum_crc32.clone(),
                    e_tag: Some(etag(existing_etag)?),
                    ..UploadPartOutput::default()
                }));
            }
            if snapshot.tail.saturating_add(2) >= u64::from(self.limits.maximum_multipart_records) {
                return Err(s3s::s3_error!(SlowDown));
            }
            if self
                .multipart_append(
                    &workspace,
                    &request.input.upload_id,
                    snapshot.tail,
                    encode_multipart_part(part_number, staged, operation, &etag_value),
                    Self::multipart_cas_operation(
                        &request.input.upload_id,
                        b"part\0",
                        operation,
                        snapshot.tail,
                    )?,
                )
                .await?
            {
                return Ok(S3Response::new(UploadPartOutput {
                    checksum_crc32: request.input.checksum_crc32.clone(),
                    e_tag: Some(etag(&etag_value)?),
                    ..UploadPartOutput::default()
                }));
            }
        }
        return Err(s3s::s3_error!(SlowDown));
    }

    async fn s3_list_parts(
        &self,
        request: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?
            .workspace;
        let snapshot = self
            .multipart_snapshot(&workspace, &request.input.upload_id)
            .await?;
        if snapshot.key != request.input.key
            || matches!(snapshot.terminal, Some(MultipartTerminal::Aborted))
        {
            return Err(s3s::s3_error!(NoSuchUpload));
        }
        let marker = request.input.part_number_marker.unwrap_or(0);
        let maximum = request.input.max_parts.unwrap_or(1_000).clamp(
            0,
            i32::try_from(self.limits.maximum_multipart_parts).unwrap_or(i32::MAX),
        );
        let maximum = usize::try_from(maximum).map_err(|_| s3s::s3_error!(InvalidArgument))?;
        let mut selected = snapshot.parts.iter().filter(|(number, _)| {
            i32::try_from(**number)
                .ok()
                .is_some_and(|value| value > marker)
        });
        let values = selected
            .by_ref()
            .take(maximum)
            .map(|(number, part)| {
                Ok(Part {
                    e_tag: Some(etag(&part.etag)?),
                    part_number: Some(
                        i32::try_from(*number).map_err(|_| s3s::s3_error!(InternalError))?,
                    ),
                    size: Some(
                        i64::try_from(part.staged.logical_bytes())
                            .map_err(|_| s3s::s3_error!(InternalError))?,
                    ),
                    ..Part::default()
                })
            })
            .collect::<S3Result<Vec<_>>>()?;
        let truncated = selected.next().is_some();
        let next_marker = truncated
            .then(|| values.last().and_then(|part| part.part_number))
            .flatten();
        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(request.input.bucket),
            key: Some(request.input.key),
            upload_id: Some(request.input.upload_id),
            max_parts: Some(i32::try_from(maximum).map_err(|_| s3s::s3_error!(InternalError))?),
            part_number_marker: Some(marker),
            next_part_number_marker: next_marker,
            is_truncated: Some(truncated),
            parts: (!values.is_empty()).then_some(values),
            ..ListPartsOutput::default()
        }))
    }

    async fn s3_complete_multipart_upload(
        &self,
        request: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        reject_complete_multipart_extensions(&request.input)?;
        let operation = request_operation(&request)?;
        let requested = completed_parts(request.input.multipart_upload.as_ref())?;
        let snapshot = match self
            .admit_multipart_completion_intent(
                &workspace,
                &request.input.upload_id,
                &request.input.key,
                operation,
                &requested,
            )
            .await?
        {
            MultipartCompletionIntent::AlreadyCompleted(generation) => {
                return Ok(S3Response::new(CompleteMultipartUploadOutput {
                    bucket: Some(request.input.bucket),
                    key: Some(request.input.key.clone()),
                    e_tag: Some(etag_for_generation(generation, &request.input.key)?),
                    ..CompleteMultipartUploadOutput::default()
                }));
            }
            MultipartCompletionIntent::Admitted(snapshot) => snapshot,
        };
        let mut transaction = workspace
            .begin_transaction(operation, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| s3_error(&S3Error::Workspace(failure.error)))?
            .value;
        let staged = requested
            .iter()
            .map(|(number, _)| {
                snapshot
                    .parts
                    .get(number)
                    .map(|part| part.staged)
                    .ok_or_else(|| s3s::s3_error!(InvalidPart))
            })
            .collect::<S3Result<Vec<_>>>()?;
        workspace
            .s3()
            .write_staged(&mut transaction, &request.input.key, &staged)
            .await
            .map_err(|error| s3_error(&error))?;
        let generation = committed_generation(
            transaction
                .commit(
                    None,
                    crate::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                    |_| async {
                        Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                            crate::OperationReceipt {
                                value: (),
                                work: crate::WorkCounters::default(),
                            },
                        )
                    },
                )
                .await
                .map_err(|failure| s3_error(&S3Error::Workspace(failure.error)))?
                .value,
        )?;
        let terminalized = self
            .terminalize_multipart_completion(
                &workspace,
                &request.input.upload_id,
                operation,
                &requested,
                generation.id(),
            )
            .await?;
        if !terminalized {
            return Err(s3s::s3_error!(SlowDown));
        }
        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(request.input.bucket),
            key: Some(request.input.key.clone()),
            e_tag: Some(etag_for_generation(generation.id(), &request.input.key)?),
            ..CompleteMultipartUploadOutput::default()
        }))
    }

    async fn s3_abort_multipart_upload(
        &self,
        request: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        if request.input.if_match_initiated_time.is_some() {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let operation = request_operation(&request)?;
        for _ in 0..self.limits.maximum_multipart_cas_retries {
            let snapshot = self
                .multipart_snapshot(&workspace, &request.input.upload_id)
                .await?;
            if snapshot.key != request.input.key {
                return Err(s3s::s3_error!(NoSuchUpload));
            }
            match snapshot.terminal {
                Some(MultipartTerminal::Aborted) => {
                    return Ok(S3Response::new(AbortMultipartUploadOutput::default()));
                }
                Some(_) => return Err(s3s::s3_error!(NoSuchUpload)),
                None => {
                    if self
                        .multipart_append(
                            &workspace,
                            &request.input.upload_id,
                            snapshot.tail,
                            encode_multipart_aborted(),
                            Self::multipart_cas_operation(
                                &request.input.upload_id,
                                b"abort\0",
                                operation,
                                snapshot.tail,
                            )?,
                        )
                        .await?
                    {
                        return Ok(S3Response::new(AbortMultipartUploadOutput::default()));
                    }
                }
            }
        }
        Err(s3s::s3_error!(SlowDown))
    }

    async fn s3_head_bucket(
        &self,
        request: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.scoped_principal(&request, &request.input.bucket, false)
            .await?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn s3_get_bucket_location(
        &self,
        request: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.scoped_principal(&request, &request.input.bucket, false)
            .await?;
        Ok(S3Response::new(GetBucketLocationOutput::default()))
    }

    async fn s3_list_objects(
        &self,
        request: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let principal = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?;
        if request
            .input
            .encoding_type
            .as_ref()
            .is_some_and(|value| value.as_str() != "url")
            || request.input.expected_bucket_owner.is_some()
            || request.input.optional_object_attributes.is_some()
            || request.input.request_payer.is_some()
        {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let encode = request.input.encoding_type.is_some();
        let delimiter = match request.input.delimiter.as_deref() {
            None | Some("") => None,
            Some("/") => Some('/'),
            Some(_) => return Err(s3s::s3_error!(NotImplemented)),
        };
        let maximum_keys = u32::try_from(request.input.max_keys.unwrap_or(1_000))
            .map_err(|_| s3s::s3_error!(InvalidArgument))?
            .min(self.limits.maximum_list_keys);
        let options = S3ListOptions {
            prefix: request.input.prefix.clone().unwrap_or_default(),
            delimiter,
            start_after: request.input.marker.clone(),
            maximum_keys,
            maximum_entries_examined: self.limits.maximum_list_entries_examined,
            ..S3ListOptions::default()
        };
        let view = principal.workspace.s3();
        let page = match principal.generation {
            Some(id) => {
                let generation = principal
                    .workspace
                    .generation(id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| s3_error(&S3Error::Workspace(failure.error)))?
                    .value;
                view.list_objects_at(&generation, options).await
            }
            None => view.list_objects(options).await,
        }
        .map_err(|error| s3_error(&error))?;
        let truncated = page.next_continuation.is_some();
        let next_marker = if truncated {
            page.objects
                .last()
                .map(|item| item.key.as_str())
                .into_iter()
                .chain(page.common_prefixes.last().map(String::as_str))
                .max()
                .map(str::to_owned)
        } else {
            None
        };
        let contents = page
            .objects
            .into_iter()
            .map(|item| {
                Ok(Object {
                    e_tag: Some(etag(&item.etag)?),
                    key: Some(if encode {
                        encode_s3_key(&item.key)
                    } else {
                        item.key
                    }),
                    size: Some(
                        i64::try_from(item.content_length)
                            .map_err(|_| s3s::s3_error!(InternalError))?,
                    ),
                    ..Object::default()
                })
            })
            .collect::<S3Result<Vec<_>>>()?;
        let common_prefixes = page
            .common_prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(if encode {
                    encode_s3_key(&prefix)
                } else {
                    prefix
                }),
            })
            .collect::<Vec<_>>();
        Ok(S3Response::new(ListObjectsOutput {
            common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
            contents: (!contents.is_empty()).then_some(contents),
            delimiter: request.input.delimiter,
            encoding_type: request.input.encoding_type,
            is_truncated: Some(truncated),
            marker: request
                .input
                .marker
                .map(|value| if encode { encode_s3_key(&value) } else { value }),
            max_keys: Some(i32::try_from(maximum_keys).map_err(|_| s3s::s3_error!(InternalError))?),
            name: Some(request.input.bucket),
            next_marker: next_marker
                .filter(|_| delimiter.is_some())
                .map(|value| if encode { encode_s3_key(&value) } else { value }),
            prefix: request
                .input
                .prefix
                .map(|value| if encode { encode_s3_key(&value) } else { value }),
            ..ListObjectsOutput::default()
        }))
    }
}
