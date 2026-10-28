//! Remote provider: canonical unary RPCs for writes and point lookups; reads,
//! children pages and follows share the native multiplexed session pool.
use super::{socket, *};
use crate::*;
use async_trait::async_trait;

type Service = wire::stream_service_client::StreamServiceClient<tonic_web_wasm_client::Client>;
pub(super) struct Remote {
    operation_claims: crate::OperationClaims,
    endpoint: socket::Endpoint,
    sessions: crate::session::Client,
    response_limits: tonic_web_wasm_client::limits::UnaryResponseLimits,
}
impl Remote {
    pub(super) fn session(&self) -> crate::session::Client {
        self.sessions.clone()
    }
    pub(super) fn new(url: &str, credential: js_sys::Function) -> Result<Self, StreamError> {
        let response_limits =
            tonic_web_wasm_client::limits::UnaryResponseLimits::new(MAX_COMMAND_BYTES)
                .ok_or(StreamError::InvalidArgument)?;
        let endpoint = socket::Endpoint::new(url, credential)?;
        let sessions = socket::sessions(endpoint.clone());
        Ok(Self {
            endpoint,
            sessions,
            response_limits,
            operation_claims: crate::OperationClaims::default(),
        })
    }
    fn service(&self) -> Service {
        self.service_with_options(
            tonic_web_wasm_client::options::FetchOptions::new()
                .timeout(std::time::Duration::from_secs(1)),
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
                .response_limits(self.response_limits)
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
}
#[async_trait(?Send)]
impl StreamProvider for Remote {
    fn operation_claims(&self) -> &crate::OperationClaims {
        &self.operation_claims
    }

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
        // A recovered write floors later reads exactly like its lost reply would have.
        if let Some(revision) = observed.as_ref().and_then(|o| o.revision.as_ref()) {
            self.sessions.revisions().observe(revision);
        }
        Ok(observed)
    }
    async fn head(&self, path: StreamPath) -> Result<crate::PathHead, StreamError> {
        let response = self
            .service()
            .head(
                self.request(wire::HeadRequest {
                    path: path.to_string(),
                })
                .await?,
            )
            .await
            .map_err(|error| wire_codec::status(&error))?
            .into_inner();
        wire_codec::head_from_wire(response)
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        let revisions = self.sessions.revisions();
        let minimum = revisions.floor(&crate::RevisionCut::default());
        let response = self
            .service()
            .tail(
                self.request(wire::TailRequest {
                    path: path.to_string(),
                    accepted: false,
                    durable_through: None,
                    at_least: wire_codec::cut_to_wire(&minimum),
                })
                .await?,
            )
            .await
            .map_err(|e| wire_codec::status(&e))
            .inspect_err(|error| revisions.refused(error))?
            .into_inner();
        let applied = response
            .applied
            .map(|applied| wire_codec::issued_from_wire(Some(applied)))
            .transpose()?;
        revisions.served(&minimum, applied.as_ref())?;
        Ok(response.tail)
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        let body = wire::TailRequest {
            path: path.to_string(),
            accepted: true,
            durable_through: None,
            at_least: Default::default(),
        };
        let result = self
            .service()
            .tail(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        wire_codec::watermarks_from_wire(result)
    }
    async fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> Result<Option<crate::DurableReceipt>, StreamError> {
        let result = self
            .service()
            .tail(
                self.request(wire::TailRequest {
                    path: path.to_string(),
                    accepted: true,
                    durable_through: Some(through),
                    at_least: Default::default(),
                })
                .await?,
            )
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        let marks = wire_codec::watermarks_from_wire(result)?;
        Ok(crate::DurableReceipt::covering(path, &marks, through))
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let key = request.idempotency_key;
        let body = wire::AppendRequest {
            path: request.path.to_string(),
            records: request
                .records
                .into_iter()
                .map(crate::wire_codec::append_record_wire)
                .collect(),
            if_tail: request.if_tail,
            idempotency_key: bytes::Bytes::copy_from_slice(key.as_bytes()),
        };
        let result = self
            .service()
            .append(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        let outcome = wire_codec::append_outcome_from_wire(result)?;
        self.sessions.revisions().appended(&outcome);
        Ok(outcome)
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let key = request.idempotency_key;
        let body = wire::ForkRequest {
            exclude: request
                .exclude
                .into_iter()
                .map(|path| path.to_string())
                .collect(),
            source: request.source.to_string(),
            destination: request.destination.to_string(),
            at_tail: request.at_tail,
            idempotency_key: bytes::Bytes::copy_from_slice(key.as_bytes()),
        };
        let result = self
            .service()
            .fork(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        let receipt = wire_codec::fork_receipt_from_wire(result)?;
        self.sessions.revisions().observe(&receipt.revision);
        Ok(receipt)
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.sessions.read(request).await
    }
    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        self.sessions.follow(request).await
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.sessions.children_page(request, 0).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let body = wire_codec::commit_to_wire(&request);
        let response = self
            .service()
            .commit(self.request(body).await?)
            .await
            .map_err(|e| wire_codec::status(&e))?
            .into_inner();
        let outcome = wire_codec::commit_outcome_from_wire(response)?;
        self.sessions.revisions().committed(&outcome);
        Ok(outcome)
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
