use super::*;

impl Client {
    /// Abandons pending Session handshakes when their credential scope ends.
    /// Existing sessions remain owned by their logical cursors.
    pub fn abandon_session_attempts(&self) -> Result<(), StreamError> {
        self.sessions.abandon_pending()
    }
    /// This client's pooled Session, sharing its channel, credentials and
    /// connection pool. Session-native consumers, such as a swarm client,
    /// follow through it rather than opening a second connection.
    pub fn session_client(&self) -> Result<crate::session::Client, StreamError> {
        self.session()
    }
    fn session(&self) -> Result<crate::session::Client, StreamError> {
        self.sessions.get_or_try_init(|| {
            crate::session::Client::open(
                self.channel.clone(),
                self.authorization.clone(),
                self.credential.clone(),
                self.session_ca.clone(),
                self.session_verifier.clone(),
            )
        })
    }
    /// Receives canonical ALL/LATEST data through this client's shared session pool.
    pub async fn follow_data(
        &self,
        follow: wire::SessionFollow,
        from: u64,
        filter: crate::FollowFilter,
    ) -> Result<
        futures::stream::BoxStream<'static, Result<wire::SessionData, StreamError>>,
        StreamError,
    > {
        Ok(self
            .session()?
            .follow_data_with_ack(follow, from, self.acknowledge_deliveries, filter)
            .await?
            .boxed())
    }
    /// Disables automatic ACKs for translating clients.
    #[must_use]
    pub fn without_delivery_acknowledgements(mut self) -> Self {
        self.acknowledge_deliveries = false;
        self
    }

    /// Acknowledges an opaque token at this client's authenticated endpoint.
    pub async fn acknowledge_delivery(&self, token: Bytes) -> Result<(), StreamError> {
        if token.len() != 32 {
            return Err(StreamError::InvalidArgument);
        }
        let mut service = self.service();
        tokio::time::timeout(Duration::from_millis(500), async {
            service
                .acknowledge_delivery(
                    self.request(wire::AcknowledgeDeliveryRequest {
                        delivery_token: token,
                    })
                    .await?,
                )
                .await
        })
        .await
        .map_err(|_| StreamError::Unavailable)?
        .map_err(|error| status(&error))?;
        Ok(())
    }

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
    fn operation_claims(&self) -> &crate::OperationClaims {
        &self.operation_claims
    }

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
            ..Default::default()
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

    async fn watermarks(&self, path: StreamPath) -> Result<crate::StreamWatermarks, StreamError> {
        let response = self
            .unary(
                ATTEMPT_TIMEOUT,
                wire::TailRequest {
                    path: path.to_string(),
                    accepted: true,
                },
                |mut service, request| Box::pin(async move { service.tail(request).await }),
            )
            .await?;
        decode_response(response, crate::wire_codec::watermarks_from_wire)
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let body = wire::AppendRequest {
            path: request.path.to_string(),
            records: request
                .records
                .into_iter()
                .map(crate::wire_codec::append_record_wire)
                .collect(),
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
        // The first page opens the read, so an absent path or a start beyond the
        // tail fails here as on every provider; later pages fail in the stream.
        let mut records = self.records(request.path, request.from, Some(request.limit));
        let first = records.next().await;
        if let Some(Err(error)) = first {
            return Err(error);
        }
        Ok(stream::iter(first).chain(records).boxed())
    }

    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: crate::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.session()?
            .follow_with_ack(path, from, self.acknowledge_deliveries, filter)
            .await
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.session()?.children(request).await
    }

    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
        filter: crate::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.session()?
            .follow_records(
                path,
                from,
                Some(preceding),
                self.acknowledge_deliveries,
                filter,
            )
            .await
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
            deadline_unix_millis,
            ..commit_to_wire(&request)
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
    key.map_or_else(crate::new_identity_bytes, |key| {
        Bytes::copy_from_slice(key.as_bytes())
    })
}

/// A finite read cursor resumes after retryable transport loss.
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
                    let token = response.delivery_token.clone();
                    let records = match read_response(response, self.next, self.remaining) {
                        Ok(records) => records,
                        Err(error) => return self.invalid(error),
                    };
                    if self.client.acknowledge_deliveries && token.len() == 32 {
                        let _ = self.client.acknowledge_delivery(token).await;
                    }
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
        let limit = self.remaining.ok_or(StreamError::InvalidArgument)?;
        let body = wire::ReadRequest {
            path,
            from: self.next,
            limit,
        };
        let (body, completion) = self
            .client
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.read(request).await })
            })
            .await?;
        Ok((completion.span.scope(body), completion))
    }
}

/// Decodes one frame, which must be contiguous, not skip ahead, and respect the read limit.
use crate::wire_codec::read_response_at as read_response;
