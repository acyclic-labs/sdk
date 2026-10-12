/// Authenticated remote provider.
#[derive(Clone)]
pub struct Client {
    channels: Arc<[Channel]>,
    authorization: MetadataValue<Ascii>,
    preferred: Arc<AtomicUsize>,
}

impl Client {
    /// Connects to a TLS endpoint with an account-bound bearer credential.
    pub async fn connect(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        Self::connect_endpoints([endpoint], bearer_token).await
    }

    /// Connects to independently reachable TLS endpoints with transparent failover.
    pub async fn connect_endpoints<I, S>(
        endpoints: I,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::connect_with_tls(endpoints, bearer_token, None)
    }

    /// Connects to a TLS endpoint augmented by one caller-pinned private CA certificate.
    pub async fn connect_with_ca_certificate(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectError> {
        Self::connect_endpoints_with_ca_certificate(
            [endpoint],
            bearer_token,
            certificate_pem.as_ref(),
        )
        .await
    }

    /// Connects eagerly through TLS and HTTP/2, retaining the resulting channel
    pub async fn connect_eager(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        Self::connect_eager_endpoints([endpoint], bearer_token, None).await
    }

    /// Eagerly connects through ambient roots plus one caller-pinned private CA.
    pub async fn connect_eager_with_ca_certificate(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectError> {
        Self::connect_eager_endpoints([endpoint], bearer_token, Some(certificate_pem.as_ref()))
            .await
    }

    /// Connects to independently reachable endpoints using one caller-pinned private CA.
    pub async fn connect_endpoints_with_ca_certificate<I, S>(
        endpoints: I,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::connect_with_tls(endpoints, bearer_token, Some(certificate_pem.as_ref()))
    }

    fn connect_with_tls<I, S>(
        endpoints: I,
        bearer_token: impl AsRef<str>,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let certificate_pem = certificate_pem.map(bounded_ca_certificate).transpose()?;
        let channels = Self::channels_with_tls(
            endpoints,
            certificate_pem,
            OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
        )?;
        Self::from_channels(channels, bearer_token)
    }

    async fn connect_eager_endpoints<I, S>(
        endpoints: I,
        bearer_token: impl AsRef<str>,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let authorization = Self::authorization(bearer_token.as_ref())?;
        let certificate_pem = certificate_pem.map(bounded_ca_certificate).transpose()?;
        let endpoints = Self::endpoints_with_tls(
            endpoints,
            certificate_pem,
            OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
        )?;
        let mut channels = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            channels.push(endpoint.connect().await?);
        }
        Ok(Self::from_authorization(channels.into(), authorization))
    }

    fn channels_with_tls<I, S>(
        endpoints: I,
        certificate_pem: Option<&[u8]>,
        connect_timeout: std::time::Duration,
    ) -> Result<Arc<[Channel]>, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Ok(
            Self::endpoints_with_tls(endpoints, certificate_pem, connect_timeout)?
                .into_iter()
                .map(|endpoint| endpoint.connect_lazy())
                .collect::<Vec<_>>()
                .into(),
        )
    }

    fn endpoints_with_tls<I, S>(
        endpoints: I,
        certificate_pem: Option<&[u8]>,
        connect_timeout: std::time::Duration,
    ) -> Result<Vec<Endpoint>, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut configured = Vec::new();
        for endpoint in endpoints.into_iter().take(MAX_ENDPOINTS + 1) {
            if configured.len() == MAX_ENDPOINTS {
                return Err(ConnectError::EndpointLimit);
            }
            let endpoint = endpoint.as_ref();
            if endpoint.len() > MAX_ENDPOINT_URI_BYTES {
                return Err(ConnectError::EndpointLimit);
            }
            // `Endpoint::new` enables the compiled WebPKI roots for HTTPS. The lower-level
            let mut endpoint = Endpoint::new(endpoint.to_owned())?.connect_timeout(connect_timeout);
            if endpoint.uri().scheme_str() != Some("https") {
                return Err(ConnectError::InsecureEndpoint);
            }
            if let Some(certificate_pem) = certificate_pem {
                endpoint = endpoint.tls_config(
                    ClientTlsConfig::new()
                        .with_enabled_roots()
                        .ca_certificate(Certificate::from_pem(certificate_pem)),
                )?;
            }
            configured.push(endpoint);
        }
        if configured.is_empty() {
            return Err(ConnectError::NoEndpoints);
        }
        Ok(configured)
    }

    fn from_channels(
        channels: Arc<[Channel]>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        let authorization = Self::authorization(bearer_token.as_ref())?;
        Ok(Self::from_authorization(channels, authorization))
    }

    fn authorization(bearer_token: &str) -> Result<MetadataValue<Ascii>, ConnectError> {
        if bearer_token.trim().is_empty()
            || bearer_token.len() > MAX_BEARER_TOKEN_BYTES
            || bearer_token
                .bytes()
                .any(|byte| matches!(byte, b'\r' | b'\n'))
        {
            return Err(ConnectError::InvalidCredential);
        }
        let mut authorization = format!("Bearer {bearer_token}")
            .parse::<MetadataValue<Ascii>>()
            .map_err(|_| ConnectError::InvalidCredential)?;
        authorization.set_sensitive(true);
        Ok(authorization)
    }

    fn from_authorization(channels: Arc<[Channel]>, authorization: MetadataValue<Ascii>) -> Self {
        Self {
            channels,
            authorization,
            preferred: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "callers derive index via `(start + offset) % channels.len()` against this same slice, so it is always in-bounds"
    )]
    fn service(
        channels: &[Channel],
        index: usize,
    ) -> wire::stream_service_client::StreamServiceClient<Channel> {
        wire::stream_service_client::StreamServiceClient::new(channels[index].clone())
    }

    fn request<T>(&self, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request
    }

    async fn unary<T, U, F>(&self, body: T, mut call: F) -> Result<RpcResponse<U>, StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<Channel>,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        self.unary_on(
            &self.channels,
            &self.preferred,
            body,
            OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
            &mut call,
        )
        .await
        .map(|(body, _, completion)| RpcResponse { body, completion })
    }

    async fn follow_unary<T, U, F>(
        &self,
        body: T,
        mut call: F,
    ) -> Result<(U, usize, Completion), StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<Channel>,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        self.unary_on(
            &self.channels,
            &self.preferred,
            body,
            FOLLOW_ENDPOINT_ATTEMPT_TIMEOUT,
            &mut call,
        )
        .await
    }

    async fn unary_on<T, U, F>(
        &self,
        channels: &[Channel],
        preferred: &AtomicUsize,
        body: T,
        attempt_timeout: std::time::Duration,
        call: &mut F,
    ) -> Result<(U, usize, Completion), StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<Channel>,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        let deadline = tokio::time::Instant::now() + OPERATION_DEADLINE;
        let mut last = None;
        let mut attempt = 0_u64;
        loop {
            let start = preferred.load(Ordering::Relaxed) % channels.len();
            for offset in 0..channels.len() {
                let index = (start + offset) % channels.len();
                let attempt_deadline = deadline.min(tokio::time::Instant::now() + attempt_timeout);
                attempt += 1;
                // One span describes one physical RPC attempt; the ordinal is not a logical retry total.
                let span = tracing::info_span!(
                    "acyclic.stream.grpc.call",
                    rpc = std::any::type_name::<T>().rsplit("::").next(),
                    attempt,
                    rpc.code = Empty,
                    outcome = Empty,
                    error.kind = Empty
                );
                let mut completion = Completion::new(span);
                let result = completion
                    .span
                    .scope(tokio::time::timeout_at(
                        attempt_deadline,
                        call(Self::service(channels, index), self.request(body.clone())),
                    ))
                    .await;
                let error = match result {
                    Ok(Ok(response)) => {
                        preferred.store(index, Ordering::Relaxed);
                        return Ok((response.into_inner(), index, completion));
                    }
                    Ok(Err(error)) => error,
                    Err(_) => Status::deadline_exceeded("endpoint attempt expired"),
                };
                completion.status(&error);
                if !retryable(&error) {
                    return Err(status(&error));
                }
                completion.span.in_scope(|| {
                    tracing::warn!(
                        attempt,
                        rpc.code = error.code() as u64,
                        "stream rpc retried"
                    );
                });
                last = Some(error);
            }
            tokio::time::sleep_until((tokio::time::Instant::now() + RETRY_DELAY).min(deadline))
                .await;
            if tokio::time::Instant::now() >= deadline {
                return Err(last.as_ref().map_or(StreamError::Unavailable, status));
            }
        }
    }

    async fn records(
        &self,
        path: StreamPath,
        from: u64,
        limit: Option<u32>,
    ) -> Result<RecordStream, StreamError> {
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
        Ok(context
            .scope(stream::unfold(
                RecordCursor {
                    client: self.clone(),
                    path,
                    next: from,
                    remaining: limit,
                    active: None,
                    buffered: VecDeque::new(),
                    completion,
                },
                |mut cursor| async move {
                    loop {
                        if cursor.remaining == Some(0) {
                            cursor.completion.ok();
                            return None;
                        }
                        if let Some(record) = cursor.buffered.pop_front() {
                            if record.sequence == cursor.next {
                                cursor.next = cursor.next.saturating_add(1);
                                if let Some(remaining) = &mut cursor.remaining {
                                    *remaining = remaining.saturating_sub(1);
                                    if *remaining == 0 {
                                        cursor.completion.ok();
                                    }
                                }
                                return Some((Ok(record), cursor));
                            }
                            if record.sequence < cursor.next {
                                continue;
                            }
                            if let Some(active) = cursor.active.as_mut() {
                                active.completion.invalid_response();
                            }
                            cursor.completion.invalid_response();
                            return cursor.failure(StreamError::Unavailable);
                        }
                        if cursor.active.is_none() {
                            match cursor.completion.span.scope(cursor.open()).await {
                                Ok(active) => cursor.active = Some(active),
                                Err(error) => return cursor.failure(error),
                            }
                        }
                        let Some(active) = cursor.active.as_mut() else {
                            return cursor.failure(StreamError::Unavailable);
                        };
                        let active_endpoint = active.endpoint;
                        let response = active.records.next().await;
                        match &response {
                            Some(Err(error)) => active.completion.status(error),
                            None => active.completion.ok(),
                            _ => {}
                        }
                        match response {
                            Some(Ok(response)) => match read_response(response) {
                                Ok(records) => cursor.buffered = records,
                                Err(error) => {
                                    active.completion.invalid_response();
                                    cursor.completion.invalid_response();
                                    return cursor.failure(error);
                                }
                            },
                            Some(Err(error)) if !retryable(&error) => {
                                return cursor.failure(status(&error));
                            }
                            None if cursor.remaining.is_some() => {
                                cursor.completion.ok();
                                return None;
                            }
                            Some(Err(_)) | None => {
                                cursor.advance_follow(active_endpoint);
                                tokio::time::sleep(RETRY_DELAY).await;
                                cursor.active = None;
                            }
                        }
                    }
                },
            ))
            .boxed())
    }
}

#[async_trait]
impl StreamProvider for Client {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let response = self
            .unary(
                wire::InspectIdempotencyRequest {
                    idempotency_key: bytes::Bytes::copy_from_slice(idempotency_key.as_bytes()),
                },
                |mut service, request| {
                    Box::pin(async move { service.inspect_idempotency(request).await })
                },
            )
            .await?;
        response.decode(|response| bind_observation(&idempotency_key, response.observation))
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.unary(
            wire::TailRequest {
                path: path.to_string(),
            },
            |mut service, request| Box::pin(async move { service.tail(request).await }),
        )
        .await?
        .decode(|response| Ok(response.tail))
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        let response = self
            .unary(
                wire::TailRequest {
                    path: path.to_string(),
                },
                |mut service, request| Box::pin(async move { service.tail(request).await }),
            )
            .await?;
        response.decode(|response| {
            Ok(StreamBounds {
                tail: response.tail,
            })
        })
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let idempotency_key = request.idempotency_key.map_or_else(
            || Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            |key| Bytes::copy_from_slice(key.as_bytes()),
        );
        let response = self
            .unary(
                wire::AppendRequest {
                    path: request.path.to_string(),
                    records: request
                        .records
                        .iter()
                        .map(|record| bytes::Bytes::copy_from_slice(record))
                        .collect(),
                    if_tail: request.if_tail,
                    idempotency_key: Some(idempotency_key),
                },
                |mut service, request| Box::pin(async move { service.append(request).await }),
            )
            .await?;
        response.decode(append_outcome_from_wire)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let idempotency_key = request.idempotency_key.map_or_else(
            || Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            |key| Bytes::copy_from_slice(key.as_bytes()),
        );
        let receipt = self
            .unary(
                wire::ForkRequest {
                    source: request.source.to_string(),
                    destination: request.destination.to_string(),
                    at_tail: request.at_tail,
                    idempotency_key: Some(idempotency_key),
                },
                |mut service, request| Box::pin(async move { service.fork(request).await }),
            )
            .await?;
        receipt.decode(|receipt| {
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
        self.records(request.path, request.from, Some(request.limit))
            .await
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.records(path, from, None).await
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        let body = wire::ChildrenRequest {
            parent: request.parent.map(|path| path.to_string()),
            limit: request.limit,
        };
        let mut last = None;
        for _ in 0..self.channels.len() {
            let response = self
                .unary(body.clone(), |mut service, request| {
                    Box::pin(async move { service.children(request).await })
                })
                .await?;
            let RpcResponse {
                body: response,
                mut completion,
            } = response;
            let mut response = completion.span.scope(response);
            let result = async {
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
                Ok(children)
            }
            .await;
            match result {
                Ok(children) => return Ok(stream::iter(children.into_iter().map(Ok)).boxed()),
                Err(error) if error != StreamError::Unavailable => return Err(error),
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap_or(StreamError::Unavailable))
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        let response = self
            .unary(
                wire::ChildrenPageRequest {
                    parent: request.parent.map(|path| path.to_string()),
                    after: request.after.map(|path| path.to_string()),
                    hierarchy_version: request
                        .hierarchy_version
                        .map(|version| bytes::Bytes::copy_from_slice(version.as_bytes())),
                    limit: request.limit,
                },
                |mut service, request| {
                    Box::pin(async move { service.children_page(request).await })
                },
            )
            .await?;
        response.decode(|response| {
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
        let response = self
            .unary(
                wire::CommitRequest {
                    conditions: request.conditions.into_iter().map(condition_wire).collect(),
                    mutations: request.mutations.into_iter().map(mutation_wire).collect(),
                    idempotency_key: bytes::Bytes::copy_from_slice(
                        request.idempotency_key.as_bytes(),
                    ),
                    deadline_unix_millis: None,
                },
                |mut service, request| Box::pin(async move { service.commit(request).await }),
            )
            .await?;
        response.decode(commit_outcome_from_wire)
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        let response = self
            .unary(
                wire::CommitRequest {
                    conditions: request.conditions.into_iter().map(condition_wire).collect(),
                    mutations: request.mutations.into_iter().map(mutation_wire).collect(),
                    idempotency_key: bytes::Bytes::copy_from_slice(
                        request.idempotency_key.as_bytes(),
                    ),
                    deadline_unix_millis: Some(deadline_unix_millis),
                },
                |mut service, request| Box::pin(async move { service.commit(request).await }),
            )
            .await?;
        response.decode(commit_outcome_from_wire)
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        let envelope = self
            .unary(
                wire::ReadCommitRequest {
                    commit_id: bytes::Bytes::copy_from_slice(commit_id.as_bytes()),
                },
                |mut service, request| Box::pin(async move { service.read_commit(request).await }),
            )
            .await?;
        envelope.decode(envelope_from_wire)
    }
}
