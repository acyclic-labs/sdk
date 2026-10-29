//! Authenticated hosted JSON transport implementing the canonical Stream provider.
use crate::{http_codec, http_validation, wire_codec, *};
pub use acyclic_auth::credential::CredentialProvider;
use async_trait::async_trait;
use bytes::Bytes;
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
    /// The session pool's revision tracker, shared by every route of this client.
    fn revisions(&self) -> Result<Arc<crate::RevisionTracker>, StreamError> {
        self.session_client()?.revisions()
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
        // A well-formed body that breaks the response contract is the peer's fault.
        http_validation::validate(route, &value).map_err(|_| StreamError::InvalidResponse)?;
        Ok(value)
    }
    fn commit_response(value: &Value) -> Result<CommitOutcome, StreamError> {
        if value["ok"] == true {
            let revision = parse_revision(field(value, "revision")?)?;
            let receipt = crate::CommitReceipt {
                commit_id: parse_id(field(value, "commitId")?)?,
                committed_at_micros: parse_u64(field(value, "committedAtMicros")?)?,
                appends: field(value, "appends")?
                    .as_array()
                    .ok_or(StreamError::Unavailable)?
                    .iter()
                    .map(|range| {
                        Ok(crate::AppendRange {
                            path: parse_path(field(range, "path")?)?,
                            start: parse_u64(field(range, "start")?)?,
                            tail: parse_u64(field(range, "tail")?)?,
                        })
                    })
                    .collect::<Result<_, StreamError>>()?,
                forks: field(value, "forks")?
                    .as_array()
                    .ok_or(StreamError::Unavailable)?
                    .iter()
                    .map(|fork| parse_nested_fork(fork, &revision))
                    .collect::<Result<_, _>>()?,
                revision,
            };
            receipt.validate().map_err(|_| StreamError::Unavailable)?;
            Ok(CommitOutcome::Committed(receipt))
        } else {
            let conflicts = value["conflicts"]
                .as_array()
                .ok_or(StreamError::Unavailable)?
                .iter()
                .map(|value| {
                    let path = parse_path(field(value, "path")?)?;
                    if value["expectedPresent"] == true {
                        Ok(CommitConflict::Missing { path })
                    } else if value["expectedAbsent"] == true {
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
}

#[async_trait]
impl StreamProvider for HttpStream {
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(&self.operation_claims)
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
            "commit" => IdempotencyOutcome::Commit(parse_envelope(&outcome["outcome"])?),
            _ => return Err(StreamError::Unavailable),
        };
        let revision = match value.get("revision") {
            None | Some(serde_json::Value::Null) => None,
            Some(revision) => Some(parse_revision(revision)?),
        };
        // A recovered write floors later reads exactly like its lost reply would have.
        if let Some(revision) = &revision {
            self.revisions()?.observe(revision);
        }
        Ok(Some(IdempotencyObservation {
            idempotency_key: observed,
            request_digest,
            outcome,
            revision,
        }))
    }
    async fn heads(&self, mut request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        request.validate()?;
        request.at_least = self.revisions()?.floor(&request.at_least);
        let value = self
            .request(
                "heads",
                wire_codec::heads_request_to_wire(request.clone()).encode_to_vec(),
            )
            .await?;
        let heads = field(&value, "heads")?
            .as_array()
            .ok_or(StreamError::InvalidResponse)?
            .iter()
            .map(|head| {
                if head.is_null() {
                    Ok(None)
                } else {
                    parse_head_projection(head, request.projection).map(Some)
                }
            })
            .collect::<Result<crate::HeadSet<_>, _>>()?;

        let view = crate::Heads {
            projection: request.projection,
            snapshot: parse_revision(field(&value, "snapshot")?)?,
            heads,
            children: value
                .get("children")
                .filter(|value| !value.is_null())
                .map(parse_children_page)
                .transpose()?,
        };
        view.validate_for(&request)?;
        if parse_head_projection_name(field(&value, "projection")?)? != request.projection {
            return Err(StreamError::InvalidResponse);
        }
        self.revisions()?.observe(&view.snapshot);
        Ok(view)
    }

    fn write(
        &self,
        request: crate::WriteRequest,
    ) -> crate::BoxProviderFuture<'_, std::result::Result<crate::WriteOutcome, crate::StreamError>>
    {
        match request {
            crate::WriteRequest::Append(request) => Box::pin(futures::TryFutureExt::map_ok(
                async move {
                    let outcome = parse_append(
                        &self
                            .request_json("append", move || http_codec::encode_append(&request))
                            .await?,
                    )?;
                    if let AppendOutcome::Committed(receipt) = &outcome {
                        self.revisions()?.observe(&receipt.revision);
                    }
                    Ok(outcome)
                },
                crate::WriteOutcome::Append,
            )),
            crate::WriteRequest::Fork(request) => Box::pin(futures::TryFutureExt::map_ok(
                async move {
                    let source = request.source.clone();
                    let destination = request.destination.clone();
                    let wire = wire::ForkRequest {
                        exclude: request.exclude.iter().map(ToString::to_string).collect(),
                        source: source.to_string(),
                        destination: destination.to_string(),
                        at_tail: request.at_tail,
                        idempotency_key: bytes::Bytes::copy_from_slice(
                            request.idempotency_key.as_bytes(),
                        ),
                    };
                    let result = parse_fork(&self.request("fork", wire.encode_to_vec()).await?)?;
                    if result.source != source || result.destination != destination {
                        return Err(StreamError::Unavailable);
                    }
                    self.revisions()?.observe(&result.revision);
                    Ok(result)
                },
                crate::WriteOutcome::Fork,
            )),
            crate::WriteRequest::Commit(request) => Box::pin(futures::TryFutureExt::map_ok(
                async move {
                    let wire = wire_codec::commit_to_wire(&request);
                    let response = self.request("commit", wire.encode_to_vec()).await?;
                    let outcome = Self::commit_response(&response)?;
                    if let CommitOutcome::Committed(receipt) = &outcome {
                        self.revisions()?.observe(&receipt.revision);
                    }
                    Ok(outcome)
                },
                crate::WriteOutcome::Commit,
            )),
        }
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        // Reads and pages share the follow session; no JSON round trips. The
        // session forwards and raises this client's revision floor.
        self.session_client()?.read(request).await
    }
    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        self.session_client()?.follow(request).await
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
        use crate::{StreamError, StreamPath, StreamProviderExt as _};
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

fn parse_head_projection_name(value: &Value) -> Result<crate::HeadProjection, StreamError> {
    match value.as_str() {
        Some("history") => Ok(crate::HeadProjection::History),
        Some("watermarks") => Ok(crate::HeadProjection::Watermarks),
        Some("tail") => Ok(crate::HeadProjection::Tail),
        _ => Err(StreamError::InvalidResponse),
    }
}

fn parse_head_projection(
    value: &Value,
    projection: crate::HeadProjection,
) -> Result<crate::PathHead, StreamError> {
    let head = crate::PathHead {
        retained_through: value
            .get("retainedThrough")
            .filter(|value| !value.is_null())
            .map(parse_u64)
            .transpose()?,
        tail: parse_u64(field(&value, "tail")?)?,
        last: value
            .get("last")
            .filter(|last| !last.is_null())
            .map(parse_record)
            .transpose()?,
        durable: parse_u64(field(&value, "durable")?)?,
        origin: value
            .get("origin")
            .filter(|origin| !origin.is_null())
            .map(|origin| -> Result<crate::ForkOrigin, StreamError> {
                Ok(crate::ForkOrigin {
                    source: parse_path(field(origin, "source")?)?,
                    forked_at: parse_u64(field(origin, "forkedAt")?)?,
                    commit_id: parse_id(field(origin, "commitId")?)?,
                })
            })
            .transpose()?,
    };
    head.validate_projection(projection)?;
    Ok(head)
}

fn parse_children_page(value: &Value) -> Result<ChildrenPage, StreamError> {
    Ok(ChildrenPage {
        hierarchy_version: parse_id(field(value, "hierarchyVersion")?)?,
        next_after: value
            .get("nextAfter")
            .filter(|value| !value.is_null())
            .map(parse_path)
            .transpose()?,
        children: field(value, "children")?
            .as_array()
            .ok_or(StreamError::InvalidResponse)?
            .iter()
            .map(|child| {
                Ok(Child {
                    path: parse_path(field(child, "path")?)?,
                    tail_record: child
                        .get("tailRecord")
                        .filter(|value| !value.is_null())
                        .map(parse_record)
                        .transpose()?,
                    origin: child
                        .get("origin")
                        .filter(|value| !value.is_null())
                        .map(|origin| {
                            Ok(ForkOrigin {
                                source: parse_path(field(origin, "source")?)?,
                                forked_at: parse_u64(field(origin, "forkedAt")?)?,
                                commit_id: parse_id(field(origin, "commitId")?)?,
                            })
                        })
                        .transpose()?,
                })
            })
            .collect::<Result<_, StreamError>>()?,
    })
}
