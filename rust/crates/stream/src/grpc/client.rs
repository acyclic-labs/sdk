use super::*;

impl Client {
    fn records(&self, path: StreamPath, from: u64, limit: Option<u32>) -> RecordStream {
        let span = if limit.is_some() {
            tracing::info_span!(
                "acyclic.stream.grpc.read",
                rev = from,
                items = limit,
                outcome = Empty,
                error.kind = Empty
            )
        } else {
            tracing::info_span!(
                "acyclic.stream.grpc.follow",
                rev = from,
                outcome = Empty,
                error.kind = Empty
            )
        };
        let completion = Completion::new(span);
        let context = completion.span.clone();
        let cursor = RecordCursor {
            client: self.clone(),
            path,
            next: from,
            remaining: limit,
            active: None,
            buffered: Vec::new().into_iter(),
            completion,
        };
        context
            .scope(stream::unfold(cursor, |mut cursor| async move {
                let item = cursor.next().await?;
                Some((item, cursor))
            }))
            .boxed()
    }
}

#[async_trait]
impl StreamProvider for Client {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let body = wire::InspectIdempotencyRequest {
            idempotency_key: Bytes::copy_from_slice(idempotency_key.as_bytes()),
        };
        let response = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.inspect_idempotency(request).await })
            })
            .await?;
        decode_response(response, |response| {
            let observation = response
                .observation
                .map(observation_from_wire)
                .transpose()?;
            // A response for another retry identity is not this request's outcome.
            if observation
                .as_ref()
                .is_some_and(|observation| observation.idempotency_key != idempotency_key)
            {
                return Err(StreamError::Unavailable);
            }
            Ok(observation)
        })
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        let body = wire::TailRequest {
            path: path.to_string(),
        };
        let response = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.tail(request).await })
            })
            .await?;
        decode_response(response, |response| Ok(response.tail))
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        Ok(StreamBounds {
            tail: self.tail(path).await?,
        })
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let body = wire::AppendRequest {
            path: request.path.to_string(),
            records: request.records.into_iter().map(Bytes::from_owner).collect(),
            if_tail: request.if_tail,
            idempotency_key: Some(retry_identity(request.idempotency_key)),
        };
        let response = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.append(request).await })
            })
            .await?;
        decode_response(response, append_outcome_from_wire)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let body = wire::ForkRequest {
            source: request.source.to_string(),
            destination: request.destination.to_string(),
            at_tail: request.at_tail,
            idempotency_key: Some(retry_identity(request.idempotency_key)),
        };
        let response = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.fork(request).await })
            })
            .await?;
        decode_response(response, |receipt| {
            Ok(ForkReceipt {
                source: path(receipt.source)?,
                destination: path(receipt.destination)?,
                forked_at: receipt.forked_at,
                tail: receipt.tail,
                commit_id: commit_id(&receipt.commit_id)?,
            })
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        Ok(self.records(request.path, request.from, Some(request.limit)))
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        Ok(self.records(path, from, None))
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        let body = wire::ChildrenRequest {
            parent: request.parent.map(|path| path.to_string()),
            limit: request.limit,
        };
        let (response, mut completion) = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.children(request).await })
            })
            .await?;
        let mut response = completion.span.scope(response);
        let mut children = Vec::new();
        while let Some(item) = response.next().await {
            let item = item.map_err(|error| {
                completion.status(&error);
                status(&error)
            })?;
            let child = item
                .child
                .ok_or(StreamError::Unavailable)
                .and_then(|child| {
                    Ok(Child {
                        path: path(child.path)?,
                    })
                })
                .inspect_err(|_| completion.invalid_response())?;
            children.push(child);
        }
        completion.ok();
        Ok(stream::iter(children.into_iter().map(Ok)).boxed())
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        let body = wire::ChildrenPageRequest {
            parent: request.parent.map(|path| path.to_string()),
            after: request.after.map(|path| path.to_string()),
            hierarchy_version: request
                .hierarchy_version
                .map(|version| Bytes::copy_from_slice(version.as_bytes())),
            limit: request.limit,
        };
        let response = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.children_page(request).await })
            })
            .await?;
        decode_response(response, |response| {
            Ok(ChildrenPage {
                hierarchy_version: commit_id(&response.hierarchy_version)?,
                children: response
                    .children
                    .into_iter()
                    .map(|child| {
                        Ok(Child {
                            path: path(child.path)?,
                        })
                    })
                    .collect::<Result<_, StreamError>>()?,
                next_after: response.next_after.map(path).transpose()?,
            })
        })
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.commit_with_deadline(request, None).await
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.commit_with_deadline(request, Some(deadline_unix_millis))
            .await
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        let body = wire::ReadCommitRequest {
            commit_id: Bytes::copy_from_slice(commit_id.as_bytes()),
        };
        let response = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.read_commit(request).await })
            })
            .await?;
        decode_response(response, envelope_from_wire)
    }
}

impl Client {
    async fn commit_with_deadline(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
    ) -> Result<CommitOutcome, StreamError> {
        let body = wire::CommitRequest {
            conditions: request.conditions.into_iter().map(condition_wire).collect(),
            mutations: request.mutations.into_iter().map(mutation_wire).collect(),
            idempotency_key: Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
            deadline_unix_millis,
        };
        let response = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.commit(request).await })
            })
            .await?;
        decode_response(response, commit_outcome_from_wire)
    }
}

/// A fixed retry identity keeps transport retries of one append or fork exactly-once.
fn retry_identity(key: Option<IdempotencyKey>) -> Bytes {
    key.map_or_else(
        || Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        |key| Bytes::copy_from_slice(key.as_bytes()),
    )
}

/// One logical read or follow; follow reopens after transport loss or a clean EOF.
struct RecordCursor {
    client: Client,
    path: StreamPath,
    next: u64,
    remaining: Option<u32>,
    active: Option<(
        obs::Scoped<tonic::Streaming<wire::ReadResponse>>,
        Completion,
    )>,
    buffered: std::vec::IntoIter<Record>,
    completion: Completion,
}

impl RecordCursor {
    async fn next(&mut self) -> Option<Result<Record, StreamError>> {
        loop {
            if self.remaining == Some(0) {
                self.completion.ok();
                return None;
            }
            if let Some(record) = self.buffered.next() {
                if record.sequence < self.next {
                    continue;
                }
                if record.sequence > self.next {
                    return self.invalid(StreamError::Unavailable);
                }
                self.next = self.next.saturating_add(1);
                if let Some(remaining) = &mut self.remaining {
                    *remaining -= 1;
                    if *remaining == 0 {
                        self.completion.ok();
                    }
                }
                return Some(Ok(record));
            }
            if self.active.is_none() {
                match self.completion.span.scope(self.open()).await {
                    Ok(active) => self.active = Some(active),
                    Err(error) => return self.fail(error),
                }
            }
            let (body, rpc) = self.active.as_mut()?;
            let response = body.next().await;
            match &response {
                Some(Err(error)) => rpc.status(error),
                None => rpc.ok(),
                Some(Ok(_)) => {}
            }
            match response {
                Some(Ok(response)) => {
                    let records = match read_response(response, self.next, self.remaining) {
                        Ok(records) => records,
                        Err(error) => return self.invalid(error),
                    };
                    self.buffered = records;
                }
                Some(Err(error)) if !retryable(&error) => return self.fail(status(&error)),
                None if self.remaining.is_some() => {
                    self.completion.ok();
                    return None;
                }
                Some(Err(_)) | None => {
                    tokio::time::sleep(RETRY_DELAY).await;
                    self.active = None;
                }
            }
        }
    }

    fn invalid(&mut self, error: StreamError) -> Option<Result<Record, StreamError>> {
        if let Some((_, rpc)) = &mut self.active {
            rpc.invalid_response();
        }
        self.completion.invalid_response();
        self.fail(error)
    }

    /// Delivered errors terminate the logical cursor.
    fn fail(&mut self, error: StreamError) -> Option<Result<Record, StreamError>> {
        self.completion.finish(Some(error.code()), None);
        self.active = None;
        self.buffered = Vec::new().into_iter();
        self.remaining = Some(0);
        Some(Err(error))
    }

    async fn open(
        &self,
    ) -> Result<
        (
            obs::Scoped<tonic::Streaming<wire::ReadResponse>>,
            Completion,
        ),
        StreamError,
    > {
        let path = self.path.to_string();
        let (body, completion) = match self.remaining {
            Some(limit) => {
                let body = wire::ReadRequest {
                    path,
                    from: self.next,
                    limit,
                };
                self.client
                    .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                        Box::pin(async move { service.read(request).await })
                    })
                    .await?
            }
            None => {
                let body = wire::FollowRequest {
                    path,
                    from: self.next,
                };
                self.client
                    .unary(FOLLOW_ATTEMPT_TIMEOUT, body, |mut service, request| {
                        Box::pin(async move { service.follow(request).await })
                    })
                    .await?
            }
        };
        Ok((completion.span.scope(body), completion))
    }
}

/// Decodes one frame, which must be contiguous, not skip ahead, and respect the read limit.
fn read_response(
    value: wire::ReadResponse,
    next: u64,
    remaining: Option<u32>,
) -> Result<std::vec::IntoIter<Record>, StreamError> {
    let records: Vec<Record> = read_response_records(value)?
        .into_iter()
        .map(record)
        .collect::<Result<_, _>>()?;
    let first = records.first().map_or(next, |record| record.sequence);
    let contiguous = records
        .iter()
        .enumerate()
        .all(|(offset, record)| Some(record.sequence) == first.checked_add(offset as u64));
    let fresh = records
        .iter()
        .filter(|record| record.sequence >= next)
        .count();
    if first > next || !contiguous || remaining.is_some_and(|remaining| fresh > remaining as usize)
    {
        return Err(StreamError::Unavailable);
    }
    Ok(records.into_iter())
}
