#[cfg(feature = "local")]
use super::transaction::put_group_sizes;
use super::*;

#[async_trait::async_trait]
impl ObjectsProvider for MemoryObjects {
    async fn put_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.put_batch",
                batch_len = requests.len()
            ),
            async {
                if requests.is_empty() {
                    return Vec::new();
                }
                let Ok(mut state) = self.lock_state() else {
                    return vec![Err(Unavailable.into()); requests.len()];
                };
                #[cfg(feature = "local")]
                if self.journal.is_some() {
                    return self.put_durable_batch_locked(&mut state, requests);
                }
                requests
                    .into_iter()
                    .map(|(query, body)| self.put_locked(&mut state, &query, body))
                    .collect()
            },
        )
        .await
    }

    async fn get_batch(
        &self,
        requests: Vec<(wire::GetObjectRequest, u64)>,
    ) -> Vec<Result<Object, Error>> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.get_batch",
                batch_len = requests.len()
            ),
            async {
                if requests.is_empty() {
                    return Vec::new();
                }
                let selected = {
                    let Ok(state) = self.lock_state() else {
                        return (0..requests.len())
                            .map(|_| Err(Unavailable.into()))
                            .collect();
                    };
                    requests
                        .into_iter()
                        .map(|(query, maximum)| get_locked(&state, &query, maximum))
                        .collect::<Vec<_>>()
                };
                let mut results = Vec::with_capacity(selected.len());
                for value in selected {
                    results.push(match value {
                        Ok(value) => value
                            .read(BodyReadMode::Ordinary)
                            .await
                            .map_err(|_| Error::from(Unavailable)),
                        Err(error) => Err(error),
                    });
                }
                results
            },
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn get_batch_native(
        &self,
        input: ObjectsNativeRequest<Vec<(wire::GetObjectRequest, u64)>>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<Object>>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let input = input;
            obs::traced(
                obs::span!(
                    INFO,
                    "acyclic.objects.get_batch",
                    batch_len = input.data().len()
                ),
                async {
                    meter.check()?;
                    let count = input.data().len();
                    if count == 0 {
                        return ObjectsBatch::new(Vec::new(), ReadAllocation::empty(&meter));
                    }
                    let mut selected = native_vec(count, &meter)?;
                    let mut results = native_vec(count, &meter)?;
                    {
                        let state = self.lock_state()?;
                        for (query, maximum) in input.data() {
                            selected.0.push(get_native_locked(
                                &state,
                                query,
                                *maximum,
                                &meter,
                                self.native_table_backend(),
                            ));
                        }
                    }
                    for value in selected.0.drain(..) {
                        results.0.push(match value {
                            Ok(value) => value.read(BodyReadMode::Native(Arc::clone(&meter))).await,
                            Err(error) => Err(error),
                        });
                    }
                    drop(selected);
                    let (values, allocation) = results;
                    ObjectsBatch::new(values, allocation)
                },
            )
            .await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        input: ObjectsNativeRequest<Vec<(wire::PutObjectHeader, OwnedBytes)>>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let mut input = input;
            obs::traced(
                obs::span!(
                    INFO,
                    "acyclic.objects.put_batch",
                    batch_len = input.data().len()
                ),
                async {
                    meter.check()?;
                    let mut results = native_vec(input.data().len(), &meter)?;
                    if input.data().is_empty() {
                        let (values, allocation) = results;
                        return ObjectsBatch::new(values, allocation);
                    }
                    {
                        let mut state = self.lock_state()?;
                        #[cfg(feature = "local")]
                        if let Some(journal) = &self.journal {
                            // Prepare group failure owners before mutation or I/O.
                            let mut plans =
                                native_vec(put_group_sizes(input.data()).count(), &meter)?;
                            for count in put_group_sizes(input.data()) {
                                plans.0.push((
                                    count,
                                    super::NativeObjectsSharedFailure::prepare(&meter)?,
                                ));
                            }
                            let mut staged = self.clone();
                            staged.defer_local = true;
                            let mut requests = input.data.drain(..);
                            for (count, mut failure) in plans.0.drain(..) {
                                let start = results.0.len();
                                let mut next = match meter
                                    .check()
                                    .map(|()| state.clone())
                                    .map_err(NativeObjectsError::from)
                                {
                                    Ok(next) => next,
                                    Err(error) => {
                                        Arc::get_mut(&mut failure)
                                            .expect("failure is unique before publication")
                                            .value = Some(error);
                                        for _ in requests.by_ref().take(count) {
                                            results.0.push(Err(NativeObjectsError::from(
                                                NativeObjectsCause::SharedFailure(Arc::clone(
                                                    &failure,
                                                )),
                                            )));
                                        }
                                        continue;
                                    }
                                };
                                for (query, body) in requests.by_ref().take(count) {
                                    results.0.push(
                                        staged.put_locked_native(&mut next, query, body, &meter),
                                    );
                                }
                                if next.sequence != state.sequence {
                                    match journal.commit(
                                        &state,
                                        &mut next,
                                        Some(Arc::clone(&meter)),
                                    ) {
                                        Ok(()) => *state = next,
                                        Err(error) => {
                                            Arc::get_mut(&mut failure)
                                                .expect("failure is unique before publication")
                                                .value = Some(error);
                                            for result in &mut results.0[start..] {
                                                if result.is_ok() {
                                                    *result = Err(NativeObjectsError::from(
                                                        NativeObjectsCause::SharedFailure(
                                                            Arc::clone(&failure),
                                                        ),
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            let (values, allocation) = results;
                            return ObjectsBatch::new(values, allocation);
                        }
                        for (query, body) in input.data.drain(..) {
                            results
                                .0
                                .push(self.put_locked_native(&mut state, query, body, &meter));
                        }
                    }
                    let (values, allocation) = results;
                    ObjectsBatch::new(values, allocation)
                },
            )
            .await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_native(
        &self,
        input: ObjectsNativeRequest<(wire::PutObjectHeader, OwnedBytes)>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsPutReceipt>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let mut input = input;
            obs::traced(
                obs::span!(INFO, "acyclic.objects.put", bytes = input.data().1.len()),
                async {
                    meter.check()?;
                    let mut state = self.lock_state()?;
                    let (query, body) = std::mem::take(&mut input.data);
                    self.put_locked_native(&mut state, query, body, &meter)
                },
            )
            .await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn get_native(
        &self,
        input: ObjectsNativeRequest<wire::GetObjectRequest>,
        maximum_bytes: u64,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Object>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let input = input;
            obs::traced(
                obs::span!(INFO, "acyclic.objects.get", bytes = obs::Empty),
                async {
                    let selected = {
                        let state = self.lock_state()?;
                        get_native_locked(
                            &state,
                            input.data(),
                            maximum_bytes,
                            &meter,
                            self.native_table_backend(),
                        )?
                    };
                    let result = selected
                        .read(BodyReadMode::Native(Arc::clone(&meter)))
                        .await?;
                    obs::record!("bytes" = result.body().len());
                    Ok(result)
                },
            )
            .await
        })
    }

    async fn create_bucket(&self, query: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.create_bucket"), async {
            request::bucket_name(&query.name)?;
            self.mutate(
                request::create_bucket_digest(&query)?,
                &query.mutation,
                |state| {
                    if state.buckets.contains_key(query.name.as_str()) {
                        return Err(AlreadyExists.into());
                    }
                    let info = wire::Bucket {
                        bucket: Some(wire::BucketRef {
                            name: query.name.clone(),
                        }),
                        created_at: Some(timestamp()?),
                    };
                    state.buckets.insert(
                        query.name.clone().into(),
                        Bucket {
                            info: info.clone().into(),
                            objects: OrdMap::new(),
                        },
                    );
                    Ok(info)
                },
            )
        })
        .await
    }
    async fn head_bucket(&self, query: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.head_bucket"), async {
            let name = request::bucket(&query.bucket)?;
            Ok((*self
                .lock_state()?
                .buckets
                .get(name)
                .ok_or(Error::from(NotFound))?
                .info)
                .clone())
        })
        .await
    }
    async fn delete_bucket(
        &self,
        query: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.delete_bucket"), async {
            let name = request::bucket(&query.bucket)?;
            self.mutate(
                request::delete_bucket_digest(&query)?,
                &query.mutation,
                |state| {
                    if state
                        .buckets
                        .get(name)
                        .is_some_and(|bucket| !bucket.objects.is_empty())
                        || state
                            .uploads
                            .values()
                            .any(|upload| upload.bucket.as_str() == name)
                    {
                        return Err(PreconditionFailed.into());
                    }
                    Ok(wire::DeleteBucketResponse {
                        existed: state.buckets.remove(name).is_some(),
                    })
                },
            )
        })
        .await
    }
    async fn put(
        &self,
        query: wire::PutObjectHeader,
        body: OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.put", bytes = body.len()),
            async {
                let mut state = self.lock_state()?;
                self.put_locked(&mut state, &query, body)
            },
        )
        .await
    }
    async fn get(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.get", bytes = obs::Empty),
            async {
                let selected = {
                    let state = self.lock_state()?;
                    get_locked(&state, &query, maximum_bytes)?
                };
                let object = selected
                    .read(BodyReadMode::Ordinary)
                    .await
                    .map_err(|_| Error::from(Unavailable))?;
                obs::record!("bytes" = object.body().len());
                Ok(object)
            },
        )
        .await
    }
    async fn head(
        &self,
        query: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.head"), async {
            let name = request::head(&query)?;
            let state = self.lock_state()?;
            let value = state
                .buckets
                .get(name)
                .and_then(|bucket| bucket.objects.get(query.object_key.as_str()))
                .ok_or(Error::from(NotFound))?;
            read_condition(&value.info, &query.if_match, &query.if_none_match)?;
            Ok(wire::HeadObjectResponse {
                object: Some((*value.info).clone()),
            })
        })
        .await
    }
    async fn delete(
        &self,
        query: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.delete"), async {
            let name = request::bucket(&query.bucket)?;
            self.mutate(request::delete_digest(&query)?, &query.mutation, |state| {
                let bucket = state.buckets.get_mut(name).ok_or(Error::from(NotFound))?;
                check_condition(
                    &query.preconditions,
                    bucket.objects.get(query.object_key.as_str()),
                )?;
                Ok(wire::DeleteObjectResponse {
                    existed: bucket.objects.remove(query.object_key.as_str()).is_some(),
                })
            })
        })
        .await
    }
    async fn list(
        &self,
        query: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.list", items = obs::Empty),
            async {
                let (name, limit) = request::list(&query)?;
                let cursor = self.decode_cursor(&query)?;
                let state = self.lock_state()?;
                let bucket = state.buckets.get(name).ok_or(Error::from(NotFound))?;
                // Scan strictly after the cursor until one page is full.
                let mut lower = match &cursor {
                    Some(cursor) if *cursor >= query.prefix => Bound::Excluded(cursor.clone()),
                    _ => Bound::Included(query.prefix.clone()),
                };
                let mut page = Vec::new();
                'scan: loop {
                    let mut next = None;
                    for (key, value) in bucket
                        .objects
                        .range::<_, str>((lower.as_ref().map(String::as_str), Bound::Unbounded))
                    {
                        let Some(suffix) = key.strip_prefix(&query.prefix) else {
                            break 'scan;
                        };
                        if !query.delimiter.is_empty()
                            && let Some(index) = suffix.find(&query.delimiter)
                        {
                            let prefix = key
                                .get(..query.prefix.len() + index + query.delimiter.len())
                                .ok_or(Error::from(InvalidArgument))?;
                            next = successor(prefix);
                            if cursor.as_deref().is_none_or(|cursor| prefix > cursor) {
                                page.push((prefix.to_owned(), None));
                            }
                            break;
                        }
                        page.push((key.to_string(), Some((*value.info).clone())));
                        if page.len() > limit {
                            break 'scan;
                        }
                    }
                    match next {
                        Some(next) if page.len() <= limit => lower = Bound::Included(next),
                        _ => break,
                    }
                }
                let is_truncated = page.len() > limit;
                page.truncate(limit);
                let continuation_token = if is_truncated {
                    page.last()
                        .map(|(key, _)| self.cursor(&query, key))
                        .transpose()?
                        .unwrap_or_default()
                } else {
                    String::new()
                };
                let mut response = wire::ListObjectsResponse {
                    is_truncated,
                    continuation_token,
                    ..Default::default()
                };
                for (key, info) in page {
                    if let Some(object) = info {
                        response.entries.push(wire::ListEntry {
                            object_key: key,
                            object: Some(object),
                        });
                    } else {
                        response.common_prefixes.push(key);
                    }
                }
                obs::record!("items" = response.entries.len() + response.common_prefixes.len());
                Ok(response)
            },
        )
        .await
    }
    async fn create_multipart(
        &self,
        query: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.create_multipart"),
            async {
                let name = request::bucket(&query.bucket)?;
                self.mutate(
                    request::create_multipart_digest(&query)?,
                    &query.mutation,
                    |state| {
                        if !state.buckets.contains_key(name) {
                            return Err(NotFound.into());
                        }
                        let id = next_id(state)?;
                        state.uploads.insert(
                            id.clone().into(),
                            Upload {
                                bucket: name.to_owned().into(),
                                key: query.object_key.clone().into(),
                                metadata: query.metadata.clone().into(),
                                parts: OrdMap::new(),
                            },
                        );
                        Ok(wire::MultipartUpload { upload_id: id })
                    },
                )
            },
        )
        .await
    }
    async fn upload_part(
        &self,
        query: wire::UploadPartHeader,
        body: OwnedBytes,
    ) -> Result<wire::UploadedPart, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.upload_part", bytes = body.len()),
            async {
                let name = request::bucket(&query.bucket)?;
                request::key(&query.object_key)?;
                if !(1..=10_000).contains(&query.part_number)
                    || body.len() as u64 > 5 * 1024 * 1024 * 1024
                {
                    return Err(InvalidArgument.into());
                }
                self.mutate(
                    request::upload_part_digest(&query, &body)?,
                    &query.mutation,
                    |state| {
                        upload(state, name, &query.object_key, &query.upload_id)?;
                        let receipt = wire::UploadedPart {
                            part_number: query.part_number,
                            etag: next_id(state)?,
                            size: body.len() as u64,
                        };
                        state
                            .uploads
                            .get_mut(query.upload_id.as_str())
                            .ok_or(Error::from(NotFound))?
                            .parts
                            .insert(
                                query.part_number,
                                (receipt.clone().into(), StoredBody::memory(body.clone())),
                            );
                        Ok(receipt)
                    },
                )
            },
        )
        .await
    }
    async fn list_parts(
        &self,
        query: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.list_parts", items = obs::Empty),
            async {
                let (name, limit) = request::list_parts(&query)?;
                let state = self.lock_state()?;
                let value = upload(&state, name, &query.object_key, &query.upload_id)?;
                let mut parts = value
                    .parts
                    .range(query.after_part_number + 1..)
                    .map(|(_, part)| part);
                let page: Vec<_> = parts
                    .by_ref()
                    .take(limit)
                    .map(|(part, _)| (**part).clone())
                    .collect();
                let truncated = parts.next().is_some();
                obs::record!("items" = page.len());
                Ok(wire::ListPartsResponse {
                    next_part_number: if truncated {
                        page.last().map_or(0, |part| part.part_number)
                    } else {
                        0
                    },
                    parts: page,
                    is_truncated: truncated,
                })
            },
        )
        .await
    }
    async fn complete_multipart(
        &self,
        query: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.complete_multipart"),
            async {
                let name = request::bucket(&query.bucket)?;
                self.mutate(
                    request::complete_multipart_digest(&query)?,
                    &query.mutation,
                    |state| {
                        let value = upload(state, name, &query.object_key, &query.upload_id)?;
                        let mut size = 0_usize;
                        for (index, selected) in query.parts.iter().enumerate() {
                            let (receipt, body) = value
                                .parts
                                .get(&selected.part_number)
                                .ok_or(Error::from(PreconditionFailed))?;
                            if selected != &**receipt
                                || (index + 1 < query.parts.len() && body.len() < 5 * 1024 * 1024)
                            {
                                return Err(PreconditionFailed.into());
                            }
                            size = size
                                .checked_add(body.len())
                                .ok_or(Error::from(QuotaExceeded))?;
                        }
                        if size > self.options.maximum_bytes {
                            return Err(QuotaExceeded.into());
                        }
                        let metadata = (*value.metadata).clone();
                        let mut bodies = Vec::with_capacity(query.parts.len());
                        for selected in &query.parts {
                            bodies.push(
                                value
                                    .parts
                                    .get(&selected.part_number)
                                    .ok_or(Error::from(PreconditionFailed))?
                                    .1
                                    .clone(),
                            );
                        }
                        let info = publish(
                            state,
                            name,
                            &query.object_key,
                            metadata,
                            &query.preconditions,
                            StoredBody::Composite {
                                parts: Arc::new(crate::v1::Guarded {
                                    value: bodies,
                                    _allocation: Default::default(),
                                }),
                                length: size,
                            },
                        )?;
                        state.uploads.remove(query.upload_id.as_str());
                        Ok(info)
                    },
                )
            },
        )
        .await
    }
    async fn abort_multipart(
        &self,
        query: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        obs::traced(obs::span!(INFO, "acyclic.objects.abort_multipart"), async {
            let name = request::bucket(&query.bucket)?;
            request::key(&query.object_key)?;
            self.mutate(
                request::abort_multipart_digest(&query)?,
                &query.mutation,
                |state| {
                    if state.uploads.contains_key(query.upload_id.as_str()) {
                        upload(state, name, &query.object_key, &query.upload_id)?;
                    }
                    Ok(wire::AbortMultipartResponse {
                        existed: state.uploads.remove(query.upload_id.as_str()).is_some(),
                    })
                },
            )
        })
        .await
    }
}
