//! Authenticated hosted JSON transport implementing the canonical Stream provider.
use crate::{http_codec, http_validation, wire_codec, *};
pub use acyclic_auth::credential::CredentialProvider;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use prost::Message;
use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, HeaderValue},
};
use serde_json::Value;
use std::{net::IpAddr, sync::Arc, time::Duration};
use tracing::{Span, field::Empty};
mod session;
mod shared;

/// Invalid endpoint, bearer token, CA, or response bound.
#[derive(Debug, thiserror::Error)]
#[error("invalid Stream HTTP configuration")]
pub struct ConnectError;

/// Bounded authenticated HTTP provider. Mutations are never automatically retried.
#[derive(Clone)]
pub struct HttpStream {
    operation_claims: Arc<crate::OperationClaims>,
    client: Client,
    endpoint: Url,
    authorization: Option<HeaderValue>,
    credential: Option<CredentialProvider>,
    maximum: usize,
    sessions: Arc<crate::query_sync::SyncMutex<Option<crate::grpc::Client>>>,
    session_endpoint: tonic::transport::Endpoint,
    session_verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
}
impl HttpStream {
    /// Acknowledges an opaque hosted transport receipt.
    pub async fn acknowledge_delivery(&self, delivery_token: Bytes) -> Result<(), StreamError> {
        self.request(
            "delivery/acknowledge",
            wire::AcknowledgeDeliveryRequest { delivery_token }.encode_to_vec(),
        )
        .await?;
        Ok(())
    }
    /// Creates an HTTPS client; loopback HTTP is permitted for local test servers.
    pub fn new(
        endpoint: &str,
        token: &str,
        maximum_response_bytes: usize,
    ) -> Result<Self, ConnectError> {
        Self::with_ca_certificate(endpoint, token, maximum_response_bytes, None)
    }
    /// Resolve the current credential immediately before each physical request.
    pub fn with_credential_provider(
        endpoint: &str,
        credential: CredentialProvider,
        maximum: usize,
        ca: Option<&[u8]>,
    ) -> Result<Self, ConnectError> {
        Self::build(endpoint, None, Some(credential), maximum, ca)
    }

    /// Extends system trust with a caller-supplied PEM CA certificate.
    pub fn with_ca_certificate(
        endpoint: &str,
        token: &str,
        maximum: usize,
        ca: Option<&[u8]>,
    ) -> Result<Self, ConnectError> {
        Self::build(endpoint, Some(token), None, maximum, ca)
    }

    fn build(
        endpoint: &str,
        token: Option<&str>,
        credential: Option<CredentialProvider>,
        maximum: usize,
        ca: Option<&[u8]>,
    ) -> Result<Self, ConnectError> {
        let mut endpoint = Url::parse(endpoint).map_err(|_| ConnectError)?;
        let loopback = endpoint.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || maximum == 0
        {
            return Err(ConnectError);
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        let authorization = token.map(credential_header).transpose()?;
        let mut client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30));
        if let Some(ca) = ca {
            if ca.is_empty() || ca.len() > MAX_CA_CERTIFICATE_BYTES {
                return Err(ConnectError);
            }
            client = client.add_root_certificate(
                reqwest::Certificate::from_pem(ca).map_err(|_| ConnectError)?,
            );
        }
        let (session_endpoint, session_verifier) = session::session_endpoint(&endpoint, ca)?;
        Ok(Self {
            client: client.build().map_err(|_| ConnectError)?,
            endpoint,
            authorization,
            credential,
            maximum,
            sessions: Arc::new(crate::query_sync::SyncMutex::new(None)),
            operation_claims: Arc::new(crate::OperationClaims::default()),
            session_endpoint,
            session_verifier,
        })
    }
    async fn authorization(&self) -> Result<HeaderValue, StreamError> {
        if let Some(provider) = &self.credential {
            let token = provider
                .resolve()
                .await
                .map_err(|_| StreamError::AccessDenied)?;
            credential_header(&token).map_err(|_| StreamError::AccessDenied)
        } else {
            self.authorization.clone().ok_or(StreamError::AccessDenied)
        }
    }

    async fn request(&self, route: &'static str, bytes: Vec<u8>) -> Result<Value, StreamError> {
        self.request_json(route, move || http_codec::encode(route, &bytes))
            .await
    }

    async fn request_json(
        &self,
        route: &'static str,
        encode: impl FnOnce() -> Result<String, &'static str> + Send,
    ) -> Result<Value, StreamError> {
        let mut observation = HttpObservation {
            span: obs::OperationSpan::new(tracing::info_span!(
                "acyclic.stream.http.call",
                route,
                http.status = Empty,
                bytes = Empty,
                outcome = Empty,
                error.kind = Empty
            )),
            finished: false,
        };
        let result = observation
            .span
            .scope(async {
                let body = encode().map_err(contract_error)?;
                self.request_inner(route, body, &observation.span).await
            })
            .await;
        observation.finished = true;
        obs::finish(&observation.span, result)
    }

    async fn request_inner(
        &self,
        route: &'static str,
        body: String,
        span: &Span,
    ) -> Result<Value, StreamError> {
        let url = self
            .endpoint
            .join(&format!("v1/stream/{route}"))
            .map_err(|_| StreamError::Unavailable)?;
        let mut response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.authorization().await?)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| StreamError::Unavailable)?;
        let success = response.status().is_success();
        span.record("http.status", response.status().as_u16());
        if response
            .content_length()
            .is_some_and(|length| length > self.maximum as u64)
        {
            return Err(StreamError::Unavailable);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| StreamError::Unavailable)?
        {
            if chunk.len() > self.maximum.saturating_sub(body.len()) {
                return Err(StreamError::Unavailable);
            }
            body.extend_from_slice(&chunk);
        }
        span.record("bytes", body.len());
        let value: Value = serde_json::from_slice(&body).map_err(|_| StreamError::Unavailable)?;
        if !success {
            let code = value
                .as_str()
                .or_else(|| value.get("code").and_then(Value::as_str))
                .or_else(|| {
                    value
                        .get("error")
                        .and_then(|error| error.get("code"))
                        .and_then(Value::as_str)
                });
            return Err(code.map_or(StreamError::Unavailable, contract_error));
        }
        http_validation::validate(route, &value).map_err(|_| StreamError::Unavailable)?;
        Ok(value)
    }
    async fn read_page(&self, request: ReadRequest) -> Result<Vec<Record>, StreamError> {
        let from = request.from;
        let limit = request.limit;
        let path = request.path.clone();
        // Streams never shrink. Establish cursor validity before the read,
        if from > self.tail(path).await? {
            return Err(StreamError::OutOfRange);
        }
        let response = self
            .request(
                "read",
                wire::ReadRequest {
                    path: request.path.to_string(),
                    from,
                    limit,
                }
                .encode_to_vec(),
            )
            .await?;
        let records = parse_records(&response)?;
        if records.len() > limit as usize {
            return Err(StreamError::Unavailable);
        }
        let mut next = from;
        for record in &records {
            if record.sequence != next {
                return Err(StreamError::Unavailable);
            }
            next = next.checked_add(1).ok_or(StreamError::Unavailable)?;
        }
        Ok(records)
    }
    async fn commit_response(&self, value: &Value) -> Result<CommitOutcome, StreamError> {
        if value["ok"] == true {
            let id = parse_id(field(value, "commitId")?)?;
            let envelope = if let Some(envelope) = value.get("envelope") {
                let envelope = parse_envelope(envelope)?;
                if envelope.commit_id != id {
                    return Err(StreamError::Unavailable);
                }
                envelope
            } else {
                // Published compact-only HTTP servers need commit-read access.
                self.read_commit(id).await?
            };
            let tails = value["tails"].as_object().ok_or(StreamError::Unavailable)?;
            let forks = value["forks"].as_array().ok_or(StreamError::Unavailable)?;
            let mut actual_tails = std::collections::BTreeMap::new();
            let mut actual_forks = Vec::new();
            for mutation in &envelope.mutations {
                match mutation {
                    CommittedMutation::Append(append) => {
                        actual_tails.insert(append.path.to_string(), append.tail);
                    }
                    CommittedMutation::Fork(fork) => {
                        actual_forks.push((fork.destination.to_string(), fork.tail));
                    }
                }
            }
            let reported_tails = tails
                .iter()
                .map(|(path, tail)| Ok((path.clone(), parse_u64(tail)?)))
                .collect::<Result<std::collections::BTreeMap<_, _>, StreamError>>()?;
            let reported_forks = forks
                .iter()
                .map(|fork| {
                    Ok((
                        parse_string(&fork["path"])?.to_owned(),
                        parse_u64(&fork["tail"])?,
                    ))
                })
                .collect::<Result<Vec<_>, StreamError>>()?;
            if reported_tails != actual_tails || reported_forks != actual_forks {
                return Err(StreamError::Unavailable);
            }
            Ok(CommitOutcome::Committed(envelope))
        } else {
            let conflicts = value["conflicts"]
                .as_array()
                .ok_or(StreamError::Unavailable)?
                .iter()
                .map(|value| {
                    let path = parse_path(field(value, "path")?)?;
                    if value["expectedAbsent"] == true {
                        Ok(CommitConflict::Exists { path })
                    } else if let Some(expected) = value.get("expectedCommitId") {
                        Ok(CommitConflict::Head {
                            path,
                            expected: parse_id(expected)?,
                            actual: parse_id(field(value, "actualCommitId")?)?,
                        })
                    } else {
                        Ok(CommitConflict::Tail {
                            path,
                            expected: parse_u64(field(value, "expectedTail")?)?,
                            actual: value.get("actualTail").map(parse_u64).transpose()?,
                        })
                    }
                })
                .collect::<Result<Vec<_>, StreamError>>()?;
            if conflicts.len() > MAX_ITEMS {
                return Err(StreamError::Unavailable);
            }
            Ok(CommitOutcome::Conflict(conflicts))
        }
    }
    async fn commit_request(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
    ) -> Result<CommitOutcome, StreamError> {
        let mut wire = wire_codec::commit_to_wire(&request);
        wire.deadline_unix_millis = deadline;
        let response = self.request("commit", wire.encode_to_vec()).await?;
        self.commit_response(&response).await
    }
}

#[async_trait]
impl StreamProvider for HttpStream {
    fn operation_claims(&self) -> &crate::OperationClaims {
        &self.operation_claims
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let value = self
            .request(
                "idempotency/inspect",
                wire::InspectIdempotencyRequest {
                    idempotency_key: bytes::Bytes::copy_from_slice(key.as_bytes()),
                }
                .encode_to_vec(),
            )
            .await?;
        if value.is_null() {
            return Ok(None);
        }
        let observed = IdempotencyKey::new(parse_bytes(field(&value, "idempotencyKey")?)?)?;
        if observed != key {
            return Err(StreamError::Unavailable);
        }
        let request_digest = parse_bytes(field(&value, "requestDigest")?)?
            .as_ref()
            .try_into()
            .map_err(|_| StreamError::Unavailable)?;
        let outcome = field(&value, "outcome")?;
        let outcome = match parse_string(&outcome["type"])? {
            "append" => IdempotencyOutcome::Append(parse_append(&outcome["outcome"])?),
            "fork" => IdempotencyOutcome::Fork(parse_fork(&outcome["receipt"])?),
            "commit" => {
                IdempotencyOutcome::Commit(self.commit_response(&outcome["outcome"]).await?)
            }
            _ => return Err(StreamError::Unavailable),
        };
        Ok(Some(IdempotencyObservation {
            idempotency_key: observed,
            request_digest,
            outcome,
        }))
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        parse_u64(
            &self
                .request(
                    "tail",
                    wire::TailRequest {
                        path: path.to_string(),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .await?,
        )
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        Ok(StreamBounds {
            tail: self.tail(path).await?,
        })
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        self.session_client()?.watermarks(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        parse_append(
            &self
                .request_json("append", move || http_codec::encode_append(&request))
                .await?,
        )
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let source = request.source.clone();
        let destination = request.destination.clone();
        let wire = wire::ForkRequest {
            source: source.to_string(),
            destination: destination.to_string(),
            at_tail: request.at_tail,
            idempotency_key: request
                .idempotency_key
                .map(|key| bytes::Bytes::copy_from_slice(key.as_bytes())),
        };
        let result = parse_fork(&self.request("fork", wire.encode_to_vec()).await?)?;
        if result.source != source || result.destination != destination {
            return Err(StreamError::Unavailable);
        }
        Ok(result)
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        Ok(stream::iter(self.read_page(request).await?.into_iter().map(Ok)).boxed())
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.session_client()?.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> Result<RecordStream, StreamError> {
        self.session_client()?
            .follow_checked(path, from, preceding)
            .await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.session_client()?.children(request).await
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        let limit = request.limit;
        let value = self
            .request(
                "children/page",
                wire::ChildrenPageRequest {
                    parent: request.parent.map(|path| path.to_string()),
                    after: request.after.map(|path| path.to_string()),
                    hierarchy_version: request
                        .hierarchy_version
                        .map(|id| bytes::Bytes::copy_from_slice(id.as_bytes())),
                    limit,
                }
                .encode_to_vec(),
            )
            .await?;
        let children = parse_children(field(&value, "children")?)?;
        if children.len() > limit as usize {
            return Err(StreamError::Unavailable);
        }
        Ok(ChildrenPage {
            hierarchy_version: parse_id(field(&value, "hierarchyVersion")?)?,
            children,
            next_after: value
                .get("nextAfter")
                .filter(|value| !value.is_null())
                .map(parse_path)
                .transpose()?,
        })
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.commit_request(request, None).await
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.commit_request(request, Some(deadline)).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        let value = self
            .request(
                "commits/read",
                wire::ReadCommitRequest {
                    commit_id: bytes::Bytes::copy_from_slice(id.as_bytes()),
                }
                .encode_to_vec(),
            )
            .await?;
        let envelope = parse_envelope(&value)?;
        if envelope.commit_id != id {
            return Err(StreamError::Unavailable);
        }
        Ok(envelope)
    }
}
fn credential_header(token: &str) -> Result<HeaderValue, ConnectError> {
    if !acyclic_auth::credential::valid_credential(token) {
        return Err(ConnectError);
    }
    let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| ConnectError)?;
    value.set_sensitive(true);
    Ok(value)
}

// Dropping a pending physical HTTP request cancels that request, independently
struct HttpObservation {
    span: obs::OperationSpan,
    finished: bool,
}
impl Drop for HttpObservation {
    fn drop(&mut self) {
        if !self.finished {
            obs::failed(&self.span, "cancelled");
        }
    }
}

// The observation lives with the cursor, including an in-flight read or idle
include!("http/follow.rs");

#[cfg(test)]
mod tests {
    use super::{HttpStream, StreamError, wire};
    use prost::Message;

    #[test]
    fn http_credentials_are_current_sensitive_and_fail_before_transport() {
        use acyclic_auth::credential::{CredentialError, CredentialProvider};
        use futures::{executor::block_on, future};
        use proptest::prelude::*;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
            .run(
                &prop::collection::vec("[a-zA-Z0-9_-]{1,32}", 2..8),
                |values| {
                    let expected = values.clone();
                    let index = Arc::new(AtomicUsize::new(0));
                    let calls = index.clone();
                    let provider = HttpStream::with_credential_provider(
                        "https://fixture.invalid",
                        CredentialProvider::new(move || {
                            future::ready(
                                values
                                    .get(calls.fetch_add(1, Ordering::Relaxed))
                                    .cloned()
                                    .ok_or(CredentialError::Invalid),
                            )
                        }),
                        1024,
                        None,
                    )
                    .unwrap();
                    for value in &expected {
                        let header = block_on(provider.authorization()).unwrap();
                        prop_assert!(header.is_sensitive());
                        prop_assert_eq!(header.to_str().unwrap(), format!("Bearer {value}"));
                    }
                    prop_assert_eq!(index.load(Ordering::Relaxed), expected.len());
                    let result = block_on(
                        provider.request(
                            "tail",
                            wire::TailRequest {
                                path: "valid/path".into(),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                        ),
                    );
                    prop_assert!(matches!(result, Err(StreamError::AccessDenied)));
                    Ok(())
                },
            )
            .unwrap();
    }

    #[test]
    fn endpoint_and_credential_policy_matches_other_families() {
        let long = "t".repeat(crate::MAX_BEARER_TOKEN_BYTES + 1);
        for token in ["", " ", "a\r\nb", "a\0b", long.as_str()] {
            assert!(HttpStream::new("https://example.test", token, 1).is_err());
        }
        for endpoint in ["http://example.test", "https://u@example.test"] {
            assert!(HttpStream::new(endpoint, "t", 1).is_err());
        }
        for endpoint in ["http://localhost:1", "http://127.0.0.2:1", "http://[::1]:1"] {
            assert!(HttpStream::new(endpoint, "t", 1).is_ok());
        }
    }

    #[tokio::test]
    async fn filtered_http_failure_does_not_record_on_its_parent()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::{StreamError, StreamPath, StreamProvider};
        use tracing::{Instrument, instrument::WithSubscriber};
        use tracing_subscriber::{Layer, layer::Context, prelude::*};

        struct ParentMustNotBeRecorded;
        impl<S> Layer<S> for ParentMustNotBeRecorded
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            fn on_record(
                &self,
                id: &tracing::span::Id,
                _: &tracing::span::Record<'_>,
                context: Context<'_, S>,
            ) {
                if let Some(span) = context.span(id) {
                    assert_ne!(
                        span.metadata().name(),
                        "test.parent",
                        "filtered HTTP completion mutated its caller"
                    );
                }
            }
        }
        let subscriber = tracing_subscriber::registry().with(ParentMustNotBeRecorded.with_filter(
            tracing_subscriber::filter::filter_fn(|metadata| metadata.name() == "test.parent"),
        ));
        async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let provider = HttpStream::new(
                &format!("http://{}", listener.local_addr()?),
                "fixture-token",
                1024,
            )?;
            let server = tokio::spawn(async move {
                let connection = listener.accept().await;
                assert!(
                    connection.is_ok(),
                    "loopback fixture could not accept the request"
                );
                // Closing before a response forces the request completion error path.
            });
            let parent = tracing::info_span!(
                "test.parent",
                outcome = "sentinel",
                error.kind = "sentinel",
                bytes = 777_u64,
                http.status = 777_u64
            );
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                provider
                    .tail(StreamPath::new("filtered/test")?)
                    .instrument(parent),
            )
            .await?;
            tokio::time::timeout(std::time::Duration::from_secs(10), server).await??;
            assert_eq!(result, Err(StreamError::Unavailable));
            Ok(())
        }
        .with_subscriber(subscriber)
        .await
    }
}

#[cfg(test)]
mod http_follow_tests;
#[cfg(test)]
mod session_tests;
