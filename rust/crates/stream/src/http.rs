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
use std::sync::Arc;

mod session;
mod shared;

/// Invalid endpoint, bearer token, CA, or response bound.
#[derive(Debug, thiserror::Error)]
#[error("invalid Stream HTTP configuration: {0}")]
pub struct ConnectError(#[source] Box<dyn std::error::Error + Send + Sync>);

impl ConnectError {
    fn caused(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self(Box::new(source))
    }

    fn refused(reason: &'static str) -> Self {
        Self::caused(acyclic_auth::refusal::Cause::new(
            "stream.http.configuration",
            reason,
        ))
    }
}

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
        use acyclic_auth::credential::EndpointError;
        let endpoint =
            acyclic_auth::credential::base_endpoint(endpoint).map_err(|error| match error {
                EndpointError::Parse(error) => ConnectError::caused(error),
                EndpointError::Insecure => ConnectError::refused("insecure_endpoint"),
                EndpointError::Credentials => ConnectError::refused("endpoint_credentials"),
                EndpointError::Query => ConnectError::refused("endpoint_query"),
                EndpointError::Fragment => ConnectError::refused("endpoint_fragment"),
            })?;
        if maximum == 0 {
            return Err(ConnectError::refused("zero_response_capacity"));
        }
        let authorization = token.map(credential_header).transpose()?;
        let mut client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never());
        if let Some(ca) = ca {
            if ca.is_empty() {
                return Err(ConnectError::refused("empty_ca_certificate"));
            }
            client = client.add_root_certificate(
                reqwest::Certificate::from_pem(ca).map_err(ConnectError::caused)?,
            );
        }
        let (session_endpoint, session_verifier) = session::session_endpoint(&endpoint, ca)?;
        Ok(Self {
            client: client.build().map_err(ConnectError::caused)?,
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
    fn revisions(&self) -> Result<crate::query_sync::Arc<crate::RevisionTracker>, StreamError> {
        self.session_client()?.revisions()
    }
    async fn authorization(
        &self,
    ) -> Result<
        (
            HeaderValue,
            Option<acyclic_auth::credential::ResolvedCredential>,
        ),
        StreamError,
    > {
        if let Some(provider) = &self.credential {
            let token = provider
                .resolve()
                .await
                .map_err(|error| StreamError::AccessDenied.with_source(error))?;
            token
                .check()
                .await
                .map_err(|error| StreamError::AccessDenied.with_source(error))?;
            Ok((
                credential_header(&token)
                    .map_err(|error| StreamError::AccessDenied.with_source(error))?,
                Some(token),
            ))
        } else {
            self.authorization
                .clone()
                .map(|header| (header, None))
                .ok_or(StreamError::AccessDenied)
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
        let body = encode().map_err(contract_error)?;
        // Bytes count the original decoded HTTP body, excluding headers and framing.
        let work = self.request_inner(route, body);
        #[cfg(feature = "serde")]
        let result = crate::trace::observe(
            crate::trace::Request {
                name: "stream.http.request",
                mechanism: crate::trace::Mechanism::RoundTrip,
                parent: None,
                guarantee: Some("authenticated_bounded_json_response_at_original_route"),
            },
            work,
            |(_, bytes)| u64::try_from(*bytes).ok(),
        )
        .await;
        #[cfg(not(feature = "serde"))]
        let result = work.await;
        result.map(|(value, _)| value)
    }

    async fn request_inner(
        &self,
        route: &'static str,
        body: String,
    ) -> Result<(Value, usize), StreamError> {
        let url = self
            .endpoint
            .join(&format!("v1/stream/{route}"))
            .map_err(|error| StreamError::Unavailable.with_source(error))?;
        let (authorization, lease) = self.authorization().await?;
        let mut response = self
            .client
            .post(url)
            .header(AUTHORIZATION, authorization)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| StreamError::Unavailable.with_source(error))?;
        if response.status().as_u16() == 401 {
            if let Some(lease) = &lease {
                lease
                    .reject()
                    .await
                    .map_err(|error| StreamError::AccessDenied.with_source(error))?;
            }
        }
        let success = response.status().is_success();

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
            .map_err(|error| StreamError::Unavailable.with_source(error))?
        {
            if chunk.len() > self.maximum.saturating_sub(body.len()) {
                return Err(StreamError::Unavailable);
            }
            body.extend_from_slice(&chunk);
        }

        if success && let Some(lease) = &lease {
            lease
                .check()
                .await
                .map_err(|error| StreamError::AccessDenied.with_source(error))?;
        }
        let value: Value = serde_json::from_slice(&body)
            .map_err(|error| StreamError::Unavailable.with_source(error))?;
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
        http_validation::validate(route, &value).map_err(|reason| {
            StreamError::InvalidResponse.with_source(acyclic_auth::refusal::Cause::new(
                "stream.http.response",
                reason,
            ))
        })?;
        if let Some(lease) = &lease {
            lease
                .check()
                .await
                .map_err(|error| StreamError::AccessDenied.with_source(error))?;
        }
        Ok((value, body.len()))
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
                skipped: match value.get("skipped") {
                    Some(skipped) => skipped
                        .as_array()
                        .ok_or(StreamError::Unavailable)?
                        .iter()
                        .map(|index| {
                            index
                                .as_u64()
                                .and_then(|index| u32::try_from(index).ok())
                                .ok_or(StreamError::Unavailable)
                        })
                        .collect::<Result<_, _>>()?,
                    None => Vec::new(),
                },
            };
            receipt
                .validate()
                .map_err(|error| StreamError::Unavailable.with_source(error))?;
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

#[acyclic_contract_derive::implementation_methods(RPC_METHODS)]
#[async_trait]
impl StreamProvider for HttpStream {
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(&self.operation_claims)
    }

    fn credential_provider(&self) -> Option<&CredentialProvider> {
        self.credential.as_ref()
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
            .map_err(|error| StreamError::Unavailable.with_source(error))?;
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
    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        self.session_client()?.heads(request).await
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
            crate::WriteRequest::CommitAccepted(_) => {
                Box::pin(async { Err(StreamError::Unsupported) })
            }
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

#[cfg(not(target_arch = "wasm32"))]
impl crate::NativeStreamProvider for HttpStream {}

fn credential_header(token: &str) -> Result<HeaderValue, ConnectError> {
    acyclic_auth::credential::bearer_header(token).map_err(|error| match error {
        acyclic_auth::credential::CredentialError::Invalid => {
            ConnectError::refused("invalid_credential")
        }
        error => ConnectError::caused(error),
    })
}

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
                        let (header, _) = block_on(provider.authorization()).unwrap();
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
                    let error = result
                        .expect_err("exhausted original credential source must refuse dispatch");
                    prop_assert_eq!(error.category(), StreamError::AccessDenied);
                    prop_assert!(
                        std::iter::successors(std::error::Error::source(&error), |source| source
                            .source())
                        .any(|source| source.is::<CredentialError>())
                    );
                    Ok(())
                },
            )
            .unwrap();
    }

    #[test]
    fn configuration_failures_retain_causes_without_credentials() {
        use std::error::Error as _;
        fn same_cause_type<E: std::error::Error + 'static>(
            actual: &(dyn std::error::Error + 'static),
            _: &E,
        ) {
            assert!(actual.is::<E>());
        }
        let malformed = "://invalid";
        let expected = reqwest::Url::parse(malformed).unwrap_err();
        let error = HttpStream::new(malformed, "t", 1)
            .err()
            .expect("invalid URL");
        same_cause_type(error.source().unwrap(), &expected);

        for token in ["", " ", "a\r\nb", "a\0b", "never-retain-this-credential\n"] {
            let error = HttpStream::new("https://example.test", token, 1)
                .err()
                .expect("invalid credential");
            let cause = error
                .source()
                .unwrap()
                .downcast_ref::<acyclic_auth::refusal::Cause>()
                .unwrap();
            assert_eq!(cause.reason, "invalid_credential");
            assert!(!format!("{error} {error:?}").contains("never-retain-this-credential"));
        }
        for (endpoint, reason) in [
            ("http://example.test", "insecure_endpoint"),
            (
                "https://never-retain-this-credential@example.test",
                "endpoint_credentials",
            ),
            (
                "https://example.test?key=never-retain-this-credential",
                "endpoint_query",
            ),
            (
                "https://example.test#never-retain-this-credential",
                "endpoint_fragment",
            ),
        ] {
            let error = HttpStream::new(endpoint, "t", 1)
                .err()
                .expect("endpoint refused");
            let cause = error
                .source()
                .unwrap()
                .downcast_ref::<acyclic_auth::refusal::Cause>()
                .unwrap();
            assert_eq!(cause.reason, reason);
            assert!(!format!("{error} {error:?}").contains("never-retain-this-credential"));
        }
        for endpoint in ["http://localhost:1", "http://127.0.0.2:1", "http://[::1]:1"] {
            assert!(HttpStream::new(endpoint, &"t".repeat(64 * 1024), 1).is_ok());
        }
    }

    #[tokio::test]
    async fn closed_http_request_retains_original_transport_source()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::{StreamError, StreamPath, StreamProviderExt as _};
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
        });
        let result = provider
            .append(crate::AppendRequest {
                path: StreamPath::new("filtered/test")?,
                records: vec![crate::OwnedBytes::from_static(b"original-request").into()],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await;
        server.await?;
        let error = result.expect_err("the closed connection cannot return an append receipt");
        assert_eq!(error.code(), StreamError::Unavailable.code());
        assert!(error.uncertain());
        drop(provider);
        assert!(
            std::iter::successors(std::error::Error::source(&error), |source| source.source())
                .any(|source| source.is::<reqwest::Error>())
        );
        Ok(())
    }
}
#[cfg(test)]
mod http_follow_tests;
#[cfg(test)]
mod session_tests;
