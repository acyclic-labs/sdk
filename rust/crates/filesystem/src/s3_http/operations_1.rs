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
        if request
            .input
            .encoding_type
            .as_ref()
            .is_some_and(|value| value.as_str() != "url")
            || request.input.fetch_owner == Some(true)
        {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let encode = request.input.encoding_type.is_some();
        let maximum_keys = request.input.max_keys.unwrap_or(1_000);
        let maximum_keys = u32::try_from(maximum_keys)
            .map_err(|_| s3s::s3_error!(InvalidArgument))?
            .min(self.limits.maximum_list_keys);
        let delimiter = match request.input.delimiter.as_deref() {
            None | Some("") => None,
            Some("/") => Some('/'),
            Some(_) => return Err(s3s::s3_error!(NotImplemented)),
        };
        let continuation = request
            .input
            .continuation_token
            .as_deref()
            .map(|token| S3ListCursor::decode(token, self.limits.maximum_key_bytes))
            .transpose()
            .map_err(|error| s3_error(&error))?;
        let options = S3ListOptions {
            prefix: request.input.prefix.clone().unwrap_or_default(),
            delimiter,
            start_after: request.input.start_after.clone(),
            continuation,
            maximum_keys,
            maximum_entries_examined: self.limits.maximum_list_entries_examined,
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
        let key_count = page
            .objects
            .len()
            .saturating_add(page.common_prefixes.len());
        let contents = page
            .objects
            .into_iter()
            .map(|value| {
                Ok(Object {
                    e_tag: Some(etag(&value.etag)?),
                    key: Some(if encode {
                        encode_s3_key(&value.key)
                    } else {
                        value.key
                    }),
                    size: Some(
                        i64::try_from(value.content_length)
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
        let next = page.next_continuation.map(|cursor| cursor.encode());
        Ok(S3Response::new(ListObjectsV2Output {
            common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
            contents: (!contents.is_empty()).then_some(contents),
            continuation_token: request.input.continuation_token,
            delimiter: request
                .input
                .delimiter
                .map(|value| if encode { encode_s3_key(&value) } else { value }),
            encoding_type: request.input.encoding_type,
            is_truncated: Some(next.is_some()),
            key_count: Some(i32::try_from(key_count).map_err(|_| s3s::s3_error!(InternalError))?),
            max_keys: Some(i32::try_from(maximum_keys).map_err(|_| s3s::s3_error!(InternalError))?),
            name: Some(request.input.bucket),
            next_continuation_token: next,
            prefix: request.input.prefix.map(
                |value| {
                    if encode { encode_s3_key(&value) } else { value }
                },
            ),
            start_after: request
                .input
                .start_after
                .map(|value| if encode { encode_s3_key(&value) } else { value }),
            ..ListObjectsV2Output::default()
        }))
    }

    async fn s3_list_object_versions(
        &self,
        request: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let principal = self
            .scoped_principal(&request, &request.input.bucket, false)
            .await?;
        if request.input.delimiter.is_some()
            || request
                .input
                .encoding_type
                .as_ref()
                .is_some_and(|value| value.as_str() != "url")
            || request.input.expected_bucket_owner.is_some()
            || request.input.optional_object_attributes.is_some()
            || request.input.request_payer.is_some()
            || request
                .input
                .version_id_marker
                .as_deref()
                .is_some_and(|id| id != "null")
        {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let encode = request.input.encoding_type.is_some();
        let maximum_keys = u32::try_from(request.input.max_keys.unwrap_or(1_000))
            .map_err(|_| s3s::s3_error!(InvalidArgument))?
            .min(self.limits.maximum_list_keys);
        let options = S3ListOptions {
            prefix: request.input.prefix.clone().unwrap_or_default(),
            delimiter: None,
            start_after: request.input.key_marker.clone(),
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
        let next_key_marker = truncated
            .then(|| page.objects.last().map(|item| item.key.clone()))
            .flatten()
            .map(|key| if encode { encode_s3_key(&key) } else { key });
        let versions = page
            .objects
            .into_iter()
            .map(|item| {
                Ok(ObjectVersion {
                    e_tag: Some(etag(&item.etag)?),
                    is_latest: Some(true),
                    key: Some(if encode {
                        encode_s3_key(&item.key)
                    } else {
                        item.key
                    }),
                    size: Some(
                        i64::try_from(item.content_length)
                            .map_err(|_| s3s::s3_error!(InternalError))?,
                    ),
                    version_id: Some("null".to_owned()),
                    ..ObjectVersion::default()
                })
            })
            .collect::<S3Result<Vec<_>>>()?;
        Ok(S3Response::new(ListObjectVersionsOutput {
            is_truncated: Some(truncated),
            encoding_type: request.input.encoding_type,
            key_marker: request
                .input
                .key_marker
                .map(|value| if encode { encode_s3_key(&value) } else { value }),
            max_keys: Some(i32::try_from(maximum_keys).map_err(|_| s3s::s3_error!(InternalError))?),
            name: Some(request.input.bucket),
            next_key_marker,
            next_version_id_marker: truncated.then(|| "null".to_owned()),
            prefix: request
                .input
                .prefix
                .map(|value| if encode { encode_s3_key(&value) } else { value }),
            versions: Some(versions),
            ..ListObjectVersionsOutput::default()
        }))
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
        check_conditions(
            &head.etag,
            request.input.if_match.as_ref(),
            request.input.if_none_match.as_ref(),
        )?;
        let selected = select_range(head.content_length, request.input.range.as_ref())?;
        Ok(S3Response::new(HeadObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            content_length: Some(
                i64::try_from(selected.length).map_err(|_| s3s::s3_error!(InternalError))?,
            ),
            content_range: selected.content_range,
            e_tag: Some(etag(&head.etag)?),
            ..HeadObjectOutput::default()
        }))
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
        let keys = request
            .input
            .delete
            .objects
            .iter()
            .map(|object| {
                if object.version_id.as_deref().is_some_and(|id| id != "null") {
                    return Err(s3s::s3_error!(NoSuchVersion));
                }
                Ok(object.key.clone())
            })
            .collect::<S3Result<Vec<_>>>()?;
        workspace
            .s3()
            .delete_objects(&keys, request_operation(&request)?)
            .await
            .map_err(|error| s3_error(&error))?;
        let deleted = (!request.input.delete.quiet.unwrap_or(false)).then(|| {
            request
                .input
                .delete
                .objects
                .iter()
                .map(|object| DeletedObject {
                    key: Some(object.key.clone()),
                    version_id: object.version_id.clone(),
                    ..DeletedObject::default()
                })
                .collect()
        });
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted,
            ..DeleteObjectsOutput::default()
        }))
    }

    async fn s3_copy_object(
        &self,
        request: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let workspace = self
            .scoped_principal(&request, &request.input.bucket, true)
            .await?
            .workspace;
        let (source_bucket, source_key) = match &request.input.copy_source {
            s3s::dto::CopySource::Bucket {
                bucket,
                key,
                version_id: None,
            } => (bucket.as_ref(), key.as_ref()),
            s3s::dto::CopySource::Bucket {
                version_id: Some(_),
                ..
            }
            | s3s::dto::CopySource::AccessPoint { .. }
            | s3s::dto::CopySource::Outpost { .. } => {
                return Err(s3s::s3_error!(NotImplemented));
            }
        };
        if source_bucket != request.input.bucket {
            return Err(s3s::s3_error!(NotImplemented));
        }
        let commit = workspace
            .s3()
            .copy_object(source_key, &request.input.key, request_operation(&request)?)
            .await
            .map_err(|error| s3_error(&error))?;
        let generation = committed_generation(commit)?;
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(etag_for_generation(generation.id(), &request.input.key)?),
                ..CopyObjectResult::default()
            }),
            ..CopyObjectOutput::default()
        }))
    }
}
