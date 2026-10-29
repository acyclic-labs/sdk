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
    /// The revision tracker every route and session pool of this client
    /// shares, across credential changes.
    #[cfg(all(feature = "http", not(target_arch = "wasm32")))]
    pub(crate) fn revisions(&self) -> Result<Arc<crate::RevisionTracker>, StreamError> {
        Ok(Arc::clone(&self.revisions))
    }
    fn session(&self) -> Result<crate::session::Client, StreamError> {
        self.sessions.get_or_try_init(|| {
            crate::session::Client::open(
                self.channel.clone(),
                self.authorization.clone(),
                self.credential.clone(),
                self.session_ca.clone(),
                self.session_verifier.clone(),
                Arc::clone(&self.revisions),
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
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(&self.operation_claims)
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
            // A recovered write floors later reads exactly like its lost reply would have.
            if let Some(revision) = observation.as_ref().and_then(|o| o.revision.as_ref()) {
                self.revisions.observe(revision);
            }
            Ok(observation)
        })
    }

    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        self.session()?.heads(request, 0).await
    }

    fn write(
        &self,
        request: crate::WriteRequest,
    ) -> crate::BoxProviderFuture<'_, std::result::Result<crate::WriteOutcome, crate::StreamError>>
    {
        match request {
            crate::WriteRequest::Append(request) => Box::pin(async move {
                let result: std::result::Result<crate::AppendOutcome, crate::StreamError> =
                    async move {
                        let body = wire::AppendRequest {
                            path: request.path.to_string(),
                            records: request
                                .records
                                .into_iter()
                                .map(crate::wire_codec::append_record_wire)
                                .collect(),
                            if_tail: request.if_tail,
                            idempotency_key: Bytes::copy_from_slice(
                                request.idempotency_key.as_bytes(),
                            ),
                        };
                        let response = self
                            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                                Box::pin(async move { service.append(request).await })
                            })
                            .await?;
                        let outcome = decode_response(response, append_outcome_from_wire)?;
                        if let AppendOutcome::Committed(receipt) = &outcome {
                            self.revisions.observe(&receipt.revision);
                        }
                        Ok(outcome)
                    }
                    .await;
                result.map(crate::WriteOutcome::Append)
            }),
            crate::WriteRequest::Fork(request) => Box::pin(async move {
                let result: std::result::Result<crate::ForkReceipt, crate::StreamError> =
                    async move {
                        let body = wire::ForkRequest {
                            exclude: request
                                .exclude
                                .into_iter()
                                .map(|path| path.to_string())
                                .collect(),
                            source: request.source.to_string(),
                            destination: request.destination.to_string(),
                            at_tail: request.at_tail,
                            idempotency_key: Bytes::copy_from_slice(
                                request.idempotency_key.as_bytes(),
                            ),
                        };
                        let response = self
                            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                                Box::pin(async move { service.fork(request).await })
                            })
                            .await?;
                        let receipt =
                            decode_response(response, crate::wire_codec::fork_receipt_from_wire)?;
                        self.revisions.observe(&receipt.revision);
                        Ok(receipt)
                    }
                    .await;
                result.map(crate::WriteOutcome::Fork)
            }),
            crate::WriteRequest::Commit(request) => Box::pin(async move {
                let result: std::result::Result<crate::CommitOutcome, crate::StreamError> =
                    async move {
                        let body = commit_to_wire(&request);
                        let response = self
                            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                                Box::pin(async move { service.commit(request).await })
                            })
                            .await?;
                        let outcome = decode_response(response, commit_outcome_from_wire)?;
                        if let CommitOutcome::Committed(receipt) = &outcome {
                            self.revisions.observe(&receipt.revision);
                        }
                        Ok(outcome)
                    }
                    .await;
                result.map(crate::WriteOutcome::Commit)
            }),
        }
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        // A bounded follow on the shared session: no per-read RPC or deadline.
        // The session forwards and raises this client's revision floor.
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
