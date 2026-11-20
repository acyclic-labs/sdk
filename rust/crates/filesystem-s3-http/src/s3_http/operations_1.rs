impl<R, A, O> FilesystemS3Adapter<R, A, O>
where
    R: FilesystemS3Resolver<A, O>,
    A: StreamBackedAuthority + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn s3_list_objects_v2(
        &self,
        request: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let principal = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?;
        let (options, encode, maximum_keys) =
            list_objects_v2_request(&request.input, &self.limits)?;
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
        Ok(S3Response::new(list_objects_v2_response(
            request.input,
            page,
            encode,
            maximum_keys,
        )?))
    }

    async fn s3_list_object_versions(
        &self,
        request: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let principal = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?;
        let (options, encode, maximum_keys) =
            list_object_versions_request(&request.input, &self.limits)?;
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
        Ok(S3Response::new(list_object_versions_response(
            request.input,
            page,
            encode,
            maximum_keys,
        )?))
    }

    async fn s3_head_object(
        &self,
        request: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let principal = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?;
        if request
            .input
            .version_id
            .as_deref()
            .is_some_and(|id| id != "null")
        {
            return Err(s3s::s3_error!(NoSuchVersion));
        }
        let view = principal.workspace.s3();
        let head = match principal.generation {
            Some(id) => {
                let generation = principal
                    .workspace
                    .generation(id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| s3_error(&S3Error::Workspace(failure.error)))?
                    .value;
                view.head_object_at(&generation, &request.input.key).await
            }
            None => view.head_object(&request.input.key).await,
        }
        .map_err(|error| s3_error(&error))?;
        Ok(S3Response::new(head_object_response(
            &request.input,
            &head.etag,
            head.content_length,
        )?))
    }

    async fn s3_get_object(
        &self,
        request: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let principal = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?;
        if request
            .input
            .version_id
            .as_deref()
            .is_some_and(|id| id != "null")
        {
            return Err(s3s::s3_error!(NoSuchVersion));
        }
        let view = principal.workspace.s3();
        let generation = match principal.generation {
            Some(id) => {
                principal
                    .workspace
                    .generation(id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| s3_error(&S3Error::Workspace(failure.error)))?
                    .value
            }
            None => {
                principal
                    .workspace
                    .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| s3_error(&S3Error::Workspace(failure.error)))?
                    .value
            }
        };
        let head = view
            .head_object_at(&generation, &request.input.key)
            .await
            .map_err(|error| s3_error(&error))?;
        check_conditions(
            &head.etag,
            request.input.if_match.as_ref(),
            request.input.if_none_match.as_ref(),
        )?;
        let selected = select_range(head.content_length, request.input.range.as_ref())?;
        let body = StreamingBlob::wrap(sync_wrapper::SyncStream::new(stream::try_unfold(
            (
                view,
                generation,
                request.input.key.clone(),
                selected.offset,
                selected.length,
                self.limits.response_chunk_bytes,
            ),
            |(view, generation, key, offset, remaining, chunk_bytes)| async move {
                if remaining == 0 {
                    return Ok(None);
                }
                let length = remaining.min(chunk_bytes);
                let bytes = view
                    .get_object_range_at(&generation, &key, ByteRange { offset, length })
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                if u64::try_from(bytes.len()).ok() != Some(length) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "canonical range reader returned a short chunk",
                    ));
                }
                Ok(Some((
                    Bytes::from_owner(bytes),
                    (
                        view,
                        generation,
                        key,
                        offset.saturating_add(length),
                        remaining - length,
                        chunk_bytes,
                    ),
                )))
            },
        )));
        Ok(S3Response::new(GetObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            body: Some(body),
            content_length: Some(
                i64::try_from(selected.length).map_err(|_| s3s::s3_error!(InternalError))?,
            ),
            content_range: selected.content_range,
            e_tag: Some(etag(&head.etag)?),
            ..GetObjectOutput::default()
        }))
    }

    async fn s3_put_object(
        &self,
        mut request: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        reject_put_extensions(&request.input)?;
        let expected_crc32 = parse_crc32(request.input.checksum_crc32.as_deref())?;
        let operation = request_operation(&request)?;
        let mut transaction = workspace
            .begin_transaction(operation, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| s3_error(&S3Error::Workspace(failure.error)))?
            .value;
        let mut source =
            StreamingBodySource::new(request.input.body.take(), expected_crc32.is_some());
        let staged = transaction
            .stage_content(
                &mut source,
                self.limits.maximum_request_bytes,
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
        workspace
            .s3()
            .write_staged(&mut transaction, &request.input.key, &[staged])
            .await
            .map_err(|error| s3_error(&error))?;
        let commit = transaction
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
            .value;
        let generation = committed_generation(commit)?;
        Ok(S3Response::new(PutObjectOutput {
            checksum_crc32: expected_crc32.map(|checksum| {
                base64::engine::general_purpose::STANDARD.encode(checksum.to_be_bytes())
            }),
            e_tag: Some(etag_for_generation(generation.id(), &request.input.key)?),
            ..PutObjectOutput::default()
        }))
    }

    async fn s3_post_object(
        &self,
        _request: S3Request<PostObjectInput>,
    ) -> S3Result<S3Response<PostObjectOutput>> {
        // s3s does not expose multipart form signature fields after authentication,
        // so this mode cannot share the adapter's mandatory freshness validation.
        Err(s3s::s3_error!(
            NotImplemented,
            "browser form uploads are not supported"
        ))
    }

    async fn s3_delete_object(
        &self,
        request: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        if request
            .input
            .version_id
            .as_deref()
            .is_some_and(|id| id != "null")
        {
            return Err(s3s::s3_error!(NoSuchVersion));
        }
        let version_id = request.input.version_id.clone();
        workspace
            .s3()
            .delete_object(&request.input.key, request_operation(&request)?)
            .await
            .map_err(|error| s3_error(&error))?;
        Ok(S3Response::new(DeleteObjectOutput {
            version_id,
            ..DeleteObjectOutput::default()
        }))
    }

    async fn s3_delete_objects(
        &self,
        request: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        let keys = delete_objects_keys(&request.input)?;
        workspace
            .s3()
            .delete_objects(&keys, request_operation(&request)?)
            .await
            .map_err(|error| s3_error(&error))?;
        Ok(S3Response::new(delete_objects_response(&request.input)))
    }

    async fn s3_copy_object(
        &self,
        request: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        let (source_bucket, source_key) = copy_object_source(&request.input)?;
        if source_bucket != request.input.bucket {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let commit = workspace
            .s3()
            .copy_object(source_key, &request.input.key, request_operation(&request)?)
            .await
            .map_err(|error| s3_error(&error))?;
        let generation = committed_generation(commit)?;
        Ok(S3Response::new(copy_object_response(
            &request.input,
            generation.id(),
        )?))
    }
}
