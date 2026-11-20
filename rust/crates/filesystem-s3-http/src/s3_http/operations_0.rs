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
        create_bucket_validate(&request.input)?;
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
        list_buckets_validate(&request.input)?;
        let access_key = self.authenticated_access_key(&request).await?;
        let names = self
            .resolver
            .list_buckets(access_key, request_session_token(&request))
            .await?;
        for name in &names {
            self.scoped_principal(&request, name, false).await?;
        }
        Ok(S3Response::new(list_buckets_response(names)?))
    }

    async fn s3_delete_bucket(
        &self,
        request: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        delete_bucket_validate(&request.input)?;
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
        validate_http_key(&request.input.key)?;
        reject_create_multipart_extensions(&request.input)?;
        let operation = request_operation(&request)?;
        let upload_id = workspace.multipart_upload_id(operation);
        let record = encode_multipart_created(
            &request.input.key,
            self.limits.maximum_multipart_parts,
            self.limits.maximum_multipart_part_bytes,
            self.limits.minimum_multipart_part_bytes,
        )
        .map_err(multipart_storage_error)?;
        if !workspace
            .create_multipart_record(&upload_id, record, operation)
            .await
            .map_err(multipart_storage_error)?
        {
            let snapshot = self
                .multipart_snapshot(&workspace, &upload_id)
                .await
                .map_err(|error| {
                    if error.code() == &s3s::S3ErrorCode::NoSuchUpload {
                        s3s::s3_error!(SlowDown)
                    } else {
                        error
                    }
                })?;
            if snapshot.key != request.input.key {
                return Err(s3s::s3_error!(InvalidRequest));
            }
        }
        Ok(S3Response::new(create_multipart_upload_response(
            request.input,
            upload_id,
        )))
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
        let part_number = upload_part_validate(&request.input, &self.limits)?;
        let snapshot = self
            .multipart_snapshot(&workspace, &request.input.upload_id)
            .await?;
        if snapshot.key != request.input.key || snapshot.terminal.is_some() {
            return Err(s3s::s3_error!(NoSuchUpload));
        }
        if snapshot
            .maximum_parts
            .is_some_and(|maximum| part_number > maximum)
        {
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
                snapshot.maximum_part_bytes,
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
            return Ok(S3Response::new(upload_part_response(
                &request.input,
                existing_etag,
            )?));
        }
        if self
            .multipart_append(
                &workspace,
                &request.input.upload_id,
                snapshot.tail,
                encode_multipart_part(
                    part_number,
                    staged.root(),
                    staged.logical_bytes(),
                    operation,
                    &etag_value,
                ),
                operation,
            )
            .await?
        {
            return Ok(S3Response::new(upload_part_response(
                &request.input,
                &etag_value,
            )?));
        }
        Err(s3s::s3_error!(SlowDown))
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
        Ok(S3Response::new(list_parts_response(
            request.input,
            &snapshot,
            self.limits.maximum_list_keys,
        )?))
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
                return Ok(S3Response::new(complete_multipart_response(
                    &request.input,
                    generation,
                )?));
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
                    acyclic_fs::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                    |_| async {
                        Ok::<_, OperationFailure<acyclic_fs::WorkspaceError>>(OperationReceipt {
                            value: (),
                            work: WorkCounters::UNCHARGED,
                        })
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
        Ok(S3Response::new(complete_multipart_response(
            &request.input,
            generation.id(),
        )?))
    }

    async fn s3_abort_multipart_upload(
        &self,
        request: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        abort_multipart_validate(&request.input)?;
        let operation = request_operation(&request)?;
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
                        operation,
                    )
                    .await?
                {
                    return Ok(S3Response::new(AbortMultipartUploadOutput::default()));
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
        let (options, encode, maximum_keys, delimiter) =
            list_objects_request(&request.input, &self.limits)?;
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
        Ok(S3Response::new(list_objects_response(
            request.input,
            page,
            encode,
            maximum_keys,
            delimiter,
        )?))
    }
}
