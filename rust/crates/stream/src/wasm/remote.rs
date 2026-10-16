//! Remote provider: canonical unary RPCs and the native multiplexed session pool.
use super::{socket, *};
use crate::*;
use async_trait::async_trait;

type Service = wire::stream_service_client::StreamServiceClient<tonic_web_wasm_client::Client>;
pub(super) struct Remote {
    endpoint: socket::Endpoint,
    sessions: crate::session::Client,
}
impl Remote {
    pub(super) fn session(&self) -> crate::session::Client {
        self.sessions.clone()
    }
    pub(super) fn connections(&self) -> crate::session::Connections {
        self.sessions.connections()
    }
    pub(super) fn new(url: &str, credential: js_sys::Function) -> Result<Self, StreamError> {
        let endpoint = socket::Endpoint::new(url, credential)?;
        let sessions = socket::sessions(endpoint.clone());
        Ok(Self { endpoint, sessions })
    }
    fn service(&self) -> Service {
        self.service_with_options(
            tonic_web_wasm_client::options::FetchOptions::new()
                .timeout(std::time::Duration::from_secs(1)),
        )
    }
    fn read_service(&self) -> Service {
        // A history is independently bounded messages, not a unary response with
        // one total deadline. Keep consuming until EOF, cancellation or I/O error.
        self.service_with_options(
            tonic_web_wasm_client::options::FetchOptions::new().streaming_response(),
        )
    }
    fn service_with_options(
        &self,
        options: tonic_web_wasm_client::options::FetchOptions,
    ) -> Service {
        use tonic_web_wasm_client::options::Redirect;
        Service::new(tonic_web_wasm_client::Client::new_with_options(
            self.endpoint.url.clone(),
            options
                .response_limits(
                    tonic_web_wasm_client::limits::UnaryResponseLimits::new(MAX_COMMAND_BYTES)
                        .expect("fixed positive command bound"),
                )
                .redirect(Redirect::Error),
        ))
        .max_decoding_message_size(MAX_COMMAND_BYTES)
        .max_encoding_message_size(MAX_COMMAND_BYTES)
    }
    async fn request<T>(&self, body: T) -> Result<tonic::Request<T>, StreamError> {
        let mut request = tonic::Request::new(body);
        let mut authorization = format!("Bearer {}", self.endpoint.token().await?)
            .parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()
            .map_err(|_| StreamError::AccessDenied)?;
        authorization.set_sensitive(true);
        request
            .metadata_mut()
            .insert("authorization", authorization);
        Ok(request)
    }
    async fn commit_deadline(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
    ) -> Result<CommitOutcome, StreamError> {
        let body = wire::CommitRequest {
            deadline_unix_millis: deadline,
            ..wire_codec::commit_to_wire(&request)
        };
        let response = self
            .service()
            .commit(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        wire_codec::commit_outcome_from_wire(response)
    }
}
#[async_trait(?Send)]
impl StreamProvider for Remote {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let body = wire::InspectIdempotencyRequest {
            idempotency_key: bytes::Bytes::copy_from_slice(key.as_bytes()),
        };
        let result = self
            .service()
            .inspect_idempotency(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        let observed = result
            .observation
            .map(wire_codec::observation_from_wire)
            .transpose()?;
        if observed.as_ref().is_some_and(|o| o.idempotency_key != key) {
            return Err(StreamError::Unavailable);
        }
        Ok(observed)
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        Ok(self
            .service()
            .tail(
                self.request(wire::TailRequest {
                    path: path.to_string(),
                    accepted: false,
                })
                .await?,
            )
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner()
            .tail)
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        Ok(StreamBounds {
            tail: self.tail(path).await?,
        })
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        let body = wire::TailRequest {
            path: path.to_string(),
            accepted: true,
        };
        let result = self
            .service()
            .tail(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        wire_codec::watermarks_from_wire(result)
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let body = wire::AppendRequest {
            path: request.path.to_string(),
            records: request
                .records
                .into_iter()
                .map(bytes::Bytes::from_owner)
                .collect(),
            if_tail: request.if_tail,
            idempotency_key: Some(identity(request.idempotency_key)),
        };
        let result = self
            .service()
            .append(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        wire_codec::append_outcome_from_wire(result)
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let body = wire::ForkRequest {
            source: request.source.to_string(),
            destination: request.destination.to_string(),
            at_tail: request.at_tail,
            idempotency_key: Some(identity(request.idempotency_key)),
        };
        let result = self
            .service()
            .fork(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        Ok(ForkReceipt {
            source: wire_codec::path(result.source)?,
            destination: wire_codec::path(result.destination)?,
            forked_at: result.forked_at,
            tail: result.tail,
            commit_id: wire_codec::commit_id(&result.commit_id)?,
        })
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        crate::memory::validate_limit(request.limit)?;
        let body = wire::ReadRequest {
            path: request.path.to_string(),
            from: request.from,
            limit: request.limit,
        };
        let source = self
            .read_service()
            .read(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        let state = (
            source,
            request.from,
            request.limit,
            Vec::<Record>::new().into_iter(),
        );
        Ok(Box::pin(futures::stream::try_unfold(
            state,
            |(mut source, mut next, mut remaining, mut pending)| async move {
                loop {
                    if let Some(record) = pending.next() {
                        if record.sequence < next {
                            continue;
                        }
                        next = next.checked_add(1).ok_or(StreamError::OutOfRange)?;
                        remaining -= 1;
                        return Ok(Some((record, (source, next, remaining, pending))));
                    }
                    if remaining == 0 {
                        return Ok(None);
                    }
                    match source.next().await {
                        Some(Ok(frame)) => {
                            pending = wire_codec::read_response_at(frame, next, Some(remaining))?;
                        }
                        Some(Err(error)) => return Err(wire_codec::status(&error)),
                        None => return Ok(None),
                    }
                }
            },
        )))
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.sessions.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> Result<RecordStream, StreamError> {
        self.sessions.follow_checked(path, from, preceding).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.sessions.children(request).await
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        let body = wire::ChildrenPageRequest {
            parent: request.parent.map(|p| p.to_string()),
            after: request.after.map(|p| p.to_string()),
            hierarchy_version: request
                .hierarchy_version
                .map(|v| bytes::Bytes::copy_from_slice(v.as_bytes())),
            limit: request.limit,
        };
        let result = self
            .service()
            .children_page(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        Ok(ChildrenPage {
            hierarchy_version: wire_codec::commit_id(&result.hierarchy_version)?,
            children: result
                .children
                .into_iter()
                .map(|c| {
                    Ok(Child {
                        path: wire_codec::path(c.path)?,
                    })
                })
                .collect::<Result<_, StreamError>>()?,
            next_after: result.next_after.map(wire_codec::path).transpose()?,
        })
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.commit_deadline(request, None).await
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.commit_deadline(request, Some(deadline)).await
    }
    async fn read_commit(&self, commit: CommitId) -> Result<CommittedEnvelope, StreamError> {
        let body = wire::ReadCommitRequest {
            commit_id: bytes::Bytes::copy_from_slice(commit.as_bytes()),
        };
        let result = self
            .service()
            .read_commit(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        wire_codec::envelope_from_wire(result)
    }
}
fn identity(key: Option<IdempotencyKey>) -> bytes::Bytes {
    key.map_or_else(
        || bytes::Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        |key| bytes::Bytes::copy_from_slice(key.as_bytes()),
    )
}
