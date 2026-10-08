//! Authenticated hosted JSON transport implementing the canonical Stream provider.
use crate::{http_codec, http_validation, wire_codec, *};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use prost::Message;
use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, HeaderValue},
};
use serde_json::Value;
use std::{collections::VecDeque, net::IpAddr, time::Duration};
use tracing::{Span, field::Empty};

/// Invalid endpoint, bearer token, CA, or response bound.
#[derive(Debug, thiserror::Error)]
#[error("invalid Stream HTTP configuration")]
pub struct ConnectError;

/// Bounded authenticated HTTP provider. Mutations are never automatically retried.
#[derive(Clone)]
pub struct HttpStream {
    client: Client,
    endpoint: Url,
    authorization: HeaderValue,
    maximum: usize,
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
    /// Extends system trust with a caller-supplied PEM CA certificate.
    pub fn with_ca_certificate(
        endpoint: &str,
        token: &str,
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
            || token.trim().is_empty()
            || token.len() > crate::MAX_BEARER_TOKEN_BYTES
            || maximum == 0
        {
            return Err(ConnectError);
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        let mut authorization =
            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| ConnectError)?;
        authorization.set_sensitive(true);
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
        Ok(Self {
            client: client.build().map_err(|_| ConnectError)?,
            endpoint,
            authorization,
            maximum,
        })
    }
    async fn request(&self, route: &'static str, bytes: Vec<u8>) -> Result<Value, StreamError> {
        let mut observation = HttpObservation {
            span: obs::OwnedSpan::new(tracing::info_span!(
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
            .scope(self.request_inner(route, bytes, &observation.span))
            .await;
        observation.finished = true;
        obs::finish(&observation.span, result)
    }

    async fn request_inner(
        &self,
        route: &'static str,
        bytes: Vec<u8>,
        span: &Span,
    ) -> Result<Value, StreamError> {
        let body = http_codec::encode(route, &bytes).map_err(contract_error)?;
        let url = self
            .endpoint
            .join(&format!("v1/stream/{route}"))
            .map_err(|_| StreamError::Unavailable)?;
        let mut response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| StreamError::Unavailable)?;
        let success = response.status().is_success();
        obs::record(span, "http.status", response.status().as_u16());
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
        obs::record(span, "bytes", body.len());
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
    async fn read_page(
        &self,
        request: ReadRequest,
        check_empty: bool,
    ) -> Result<Vec<Record>, StreamError> {
        let from = request.from;
        let limit = request.limit;
        let path = request.path.clone();
        // Streams never shrink. Establish cursor validity before the read,
        // so an append after an empty response cannot reclassify that cursor.
        if check_empty && from > self.tail(path).await? {
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
                // Canonical servers include the admitted envelope atomically.
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
    // Keep clock control local to follow: HTTP request deadlines retain their real
    // transport clock, while tests can observe and release each idle wait.
    async fn follow_with_delay<D, F>(
        &self,
        path: StreamPath,
        from: u64,
        delay: D,
    ) -> Result<RecordStream, StreamError>
    where
        D: FnMut(Duration) -> F + Send + 'static,
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut observation = FollowObservation::new(from);
        let tail = observation.span.scope(self.tail(path.clone())).await;
        match tail {
            Ok(tail) if from <= tail => {}
            Ok(_) => {
                observation.failed(&StreamError::OutOfRange);
                return Err(StreamError::OutOfRange);
            }
            Err(error) => {
                observation.failed(&error);
                return Err(error);
            }
        }
        observation.span.record("phase", "consumer");
        let context = observation.span.clone();
        let follow = stream::try_unfold(
            (
                self.clone(),
                path,
                from,
                VecDeque::<Record>::new(),
                delay,
                observation,
            ),
            |(provider, path, mut next, mut queued, mut delay, mut observation)| async move {
                let result = async {
                    loop {
                        if let Some(record) = queued.pop_front() {
                            next = record
                                .sequence
                                .checked_add(1)
                                .ok_or(StreamError::Unavailable)?;
                            if !observation.span.is_disabled() {
                                observation
                                    .span
                                    .record("rev", next)
                                    .record("queued", queued.len())
                                    .record("phase", "consumer");
                            }
                            return Ok(record);
                        }
                        observation.span.record("phase", "read");
                        let poll = obs::OwnedSpan::new(tracing::trace_span!(
                            "acyclic.stream.http.follow.poll"
                        ));
                        queued = poll
                            .scope(provider.read_page(
                                ReadRequest {
                                    path: path.clone(),
                                    from: next,
                                    limit: 256,
                                },
                                false,
                            ))
                            .await?
                            .into();
                        observation.span.record("queued", queued.len());
                        if queued.is_empty() {
                            observation.span.record("phase", "sleep");
                            let sleep = obs::OwnedSpan::new(tracing::debug_span!(
                                "acyclic.stream.http.follow.sleep",
                                delay_ms = 250_u64
                            ));
                            sleep.scope(delay(Duration::from_millis(250))).await;
                        }
                    }
                }
                .await;
                if let Err(error) = &result {
                    observation.failed(error);
                }
                Ok(Some((
                    result?,
                    (provider, path, next, queued, delay, observation),
                )))
            },
        );
        Ok(context.scope_mode(follow, !context.is_disabled()).boxed())
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
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let value = self
            .request(
                "idempotency/inspect",
                wire::InspectIdempotencyRequest {
                    idempotency_key: Bytes::copy_from_slice(key.as_bytes()),
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
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let wire = wire::AppendRequest {
            path: request.path.to_string(),
            records: request.records,
            if_tail: request.if_tail,
            idempotency_key: request
                .idempotency_key
                .map(|key| Bytes::copy_from_slice(key.as_bytes())),
        };
        parse_append(&self.request("append", wire.encode_to_vec()).await?)
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
                .map(|key| Bytes::copy_from_slice(key.as_bytes())),
        };
        let result = parse_fork(&self.request("fork", wire.encode_to_vec()).await?)?;
        if result.source != source || result.destination != destination {
            return Err(StreamError::Unavailable);
        }
        Ok(result)
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        Ok(stream::iter(self.read_page(request, true).await?.into_iter().map(Ok)).boxed())
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.follow_with_delay(path, from, tokio::time::sleep).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        let limit = request.limit;
        let value = self
            .request(
                "children",
                wire::ChildrenRequest {
                    parent: request.parent.map(|path| path.to_string()),
                    limit,
                }
                .encode_to_vec(),
            )
            .await?;
        let children = parse_children(&value)?;
        if children.len() > limit as usize {
            return Err(StreamError::Unavailable);
        }
        Ok(stream::iter(children.into_iter().map(Ok)).boxed())
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
                        .map(|id| Bytes::copy_from_slice(id.as_bytes())),
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
                    commit_id: Bytes::copy_from_slice(id.as_bytes()),
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
// Dropping a pending physical HTTP request cancels that request, independently
// of whether a logical follow cursor has completed.
struct HttpObservation {
    span: obs::OwnedSpan,
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
// wait. Dropping a cursor says nothing about why its consumer stopped.
struct FollowObservation {
    span: obs::OwnedSpan,
    terminal: bool,
}
impl FollowObservation {
    fn new(from: u64) -> Self {
        Self {
            span: obs::OwnedSpan::new_with_filtered_context(
                tracing::info_span!(
                    "acyclic.stream.http.follow",
                    rev = from,
                    queued = 0_u64,
                    phase = "tail",
                    terminal = Empty,
                    outcome = Empty,
                    error.kind = Empty
                ),
                false,
            ),
            terminal: false,
        }
    }
    fn failed(&mut self, error: &StreamError) {
        self.span.record("terminal", "error");
        obs::failed(&self.span, error.code());
        self.terminal = true;
    }
}
impl Drop for FollowObservation {
    fn drop(&mut self) {
        if !self.terminal {
            self.span.record("terminal", "dropped");
        }
    }
}
fn parse_string(value: &Value) -> Result<&str, StreamError> {
    value.as_str().ok_or(StreamError::Unavailable)
}
fn parse_u64(value: &Value) -> Result<u64, StreamError> {
    parse_string(value)?
        .parse()
        .map_err(|_| StreamError::Unavailable)
}
fn parse_path(value: &Value) -> Result<StreamPath, StreamError> {
    StreamPath::new(parse_string(value)?).map_err(|_| StreamError::Unavailable)
}
fn parse_bytes(value: &Value) -> Result<Bytes, StreamError> {
    http_validation::decode_base64(parse_string(value)?)
        .map(Bytes::from)
        .ok_or(StreamError::Unavailable)
}
fn parse_id(value: &Value) -> Result<CommitId, StreamError> {
    Ok(CommitId::from_bytes(
        parse_bytes(value)?
            .as_ref()
            .try_into()
            .map_err(|_| StreamError::Unavailable)?,
    ))
}
fn parse_records(value: &Value) -> Result<Vec<Record>, StreamError> {
    value
        .as_array()
        .ok_or(StreamError::Unavailable)?
        .iter()
        .map(|value| {
            let bytes = parse_bytes(field(value, "value")?)?;
            if bytes.len() > MAX_RECORD_BYTES {
                return Err(StreamError::Unavailable);
            }
            Ok(Record {
                sequence: parse_u64(field(value, "sequence")?)?,
                value: bytes,
                commit_id: parse_id(field(value, "commitId")?)?,
                committed_at_micros: parse_u64(field(value, "committedAtMicros")?)?,
            })
        })
        .collect()
}
fn parse_children(value: &Value) -> Result<Vec<Child>, StreamError> {
    value
        .as_array()
        .ok_or(StreamError::Unavailable)?
        .iter()
        .map(|value| {
            Ok(Child {
                path: parse_path(field(value, "path")?)?,
            })
        })
        .collect()
}
fn parse_append(value: &Value) -> Result<AppendOutcome, StreamError> {
    if value["ok"] == true {
        Ok(AppendOutcome::Committed(AppendReceipt {
            start: parse_u64(field(value, "start")?)?,
            end: parse_u64(field(value, "end")?)?,
            tail: parse_u64(field(value, "tail")?)?,
            commit_id: parse_id(field(value, "commitId")?)?,
        }))
    } else {
        Ok(AppendOutcome::TailConflict {
            actual_tail: parse_u64(field(value, "actualTail")?)?,
        })
    }
}
fn parse_fork(value: &Value) -> Result<ForkReceipt, StreamError> {
    Ok(ForkReceipt {
        source: parse_path(field(value, "source")?)?,
        destination: parse_path(field(value, "destination")?)?,
        forked_at: parse_u64(field(value, "forkedAt")?)?,
        tail: parse_u64(field(value, "tail")?)?,
        commit_id: parse_id(field(value, "commitId")?)?,
    })
}
fn parse_envelope(value: &Value) -> Result<CommittedEnvelope, StreamError> {
    let mutations = value["mutations"]
        .as_array()
        .ok_or(StreamError::Unavailable)?;
    if mutations.len() > MAX_ITEMS {
        return Err(StreamError::Unavailable);
    }
    let mutations = mutations
        .iter()
        .map(|value| {
            let records = value
                .get("records")
                .map(parse_records)
                .transpose()?
                .unwrap_or_default();
            if records.len() > MAX_ITEMS {
                return Err(StreamError::Unavailable);
            }
            match parse_string(field(value, "type")?)? {
                "append" => Ok(CommittedMutation::Append(CommittedAppend {
                    path: parse_path(field(value, "path")?)?,
                    start: parse_u64(field(value, "start")?)?,
                    end: parse_u64(field(value, "end")?)?,
                    tail: parse_u64(field(value, "tail")?)?,
                    records,
                })),
                "fork" => Ok(CommittedMutation::Fork(CommittedFork {
                    source: parse_path(field(value, "source")?)?,
                    destination: parse_path(field(value, "destination")?)?,
                    forked_at: parse_u64(field(value, "forkedAt")?)?,
                    tail: parse_u64(field(value, "tail")?)?,
                    records,
                })),
                _ => Err(StreamError::Unavailable),
            }
        })
        .collect::<Result<Vec<_>, StreamError>>()?;
    let envelope = CommittedEnvelope {
        commit_id: parse_id(field(value, "commitId")?)?,
        mutations,
    };
    wire_codec::envelope_from_wire(wire_codec::envelope_wire(envelope))
}
fn contract_error(code: &str) -> StreamError {
    match code {
        "stream_not_found" | "commit_not_found" => StreamError::NotFound,
        "destination_exists" => StreamError::AlreadyExists,
        "capacity_exhausted" => StreamError::Capacity,
        _ => StreamError::from_code(code).unwrap_or(StreamError::Unavailable),
    }
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value, StreamError> {
    value.get(name).ok_or(StreamError::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::HttpStream;

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
#[path = "http_follow_tests.rs"]
mod follow_tests;
