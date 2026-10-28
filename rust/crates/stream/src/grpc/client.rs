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

    async fn head(&self, path: StreamPath) -> Result<crate::PathHead, StreamError> {
        let body = wire::HeadRequest {
            path: path.to_string(),
        };
        let response = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.head(request).await })
            })
            .await?;
        decode_response(response, crate::wire_codec::head_from_wire)
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

    async fn watermarks(&self, path: StreamPath) -> Result<crate::StreamWatermarks, StreamError> {
        let response = self
            .unary(
                ATTEMPT_TIMEOUT,
                wire::TailRequest {
                    path: path.to_string(),
                    accepted: true,
                    durable_through: None,
                },
                |mut service, request| Box::pin(async move { service.tail(request).await }),
            )
            .await?;
        decode_response(response, crate::wire_codec::watermarks_from_wire)
    }

    async fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> Result<Option<crate::DurableReceipt>, StreamError> {
        let response = self
            .unary(
                ATTEMPT_TIMEOUT,
                wire::TailRequest {
                    path: path.to_string(),
                    accepted: true,
                    durable_through: Some(through),
                },
                |mut service, request| Box::pin(async move { service.tail(request).await }),
            )
            .await?;
        let marks = decode_response(response, crate::wire_codec::watermarks_from_wire)?;
        Ok(crate::DurableReceipt::covering(path, &marks, through))
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
            idempotency_key: Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
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
            exclude: request
                .exclude
                .into_iter()
                .map(|path| path.to_string())
                .collect(),
            source: request.source.to_string(),
            destination: request.destination.to_string(),
            at_tail: request.at_tail,
            idempotency_key: Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
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
        // A bounded follow on the shared session: no per-read RPC or deadline.
        self.session()?
            .read_with_ack(request, self.acknowledge_deliveries)
            .await
    }

    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        self.session()?
            .follow_with_ack(request, self.acknowledge_deliveries)
            .await
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.session()?.children_page(request, 0).await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let body = commit_to_wire(&request);
        let response = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.commit(request).await })
            })
            .await?;
        decode_response(response, commit_outcome_from_wire)
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
