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
        let session = self.session()?;
        let revisions = session.revisions();
        let minimum = revisions.floor(&crate::RevisionCut::default());
        let body = wire::TailRequest {
            path: path.to_string(),
            at_least: crate::wire_codec::cut_to_wire(&minimum),
            ..Default::default()
        };
        let response = self
            .unary(ATTEMPT_TIMEOUT, body, |mut service, request| {
                Box::pin(async move { service.tail(request).await })
            })
            .await
            .inspect_err(|error| revisions.refused(error))?;
        decode_response(response, |response| {
            let applied = response
                .applied
                .map(|applied| crate::wire_codec::issued_from_wire(Some(applied)))
                .transpose()?;
            revisions.served(&minimum, applied.as_ref())?;
            Ok(response.tail)
        })
    }

    async fn watermarks(&self, path: StreamPath) -> Result<crate::StreamWatermarks, StreamError> {
        let response = self
            .unary(
                ATTEMPT_TIMEOUT,
                wire::TailRequest {
                    path: path.to_string(),
                    accepted: true,
                    durable_through: None,
                    at_least: Default::default(),
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
                    at_least: Default::default(),
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
        let outcome = decode_response(response, append_outcome_from_wire)?;
        if let AppendOutcome::Committed(receipt) = &outcome {
            self.revisions.observe(&receipt.revision);
        }
        Ok(outcome)
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
        let receipt = decode_response(response, crate::wire_codec::fork_receipt_from_wire)?;
        self.revisions.observe(&receipt.revision);
        Ok(receipt)
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
        let outcome = decode_response(response, commit_outcome_from_wire)?;
        if let CommitOutcome::Committed(receipt) = &outcome {
            self.revisions.observe(&receipt.revision);
        }
        Ok(outcome)
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
