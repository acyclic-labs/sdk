//! Authenticated hosted JSON transport implementing the canonical Stream provider.
use crate::{http_codec, http_validation, wire_codec, *};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use prost::Message;
#[cfg(not(target_arch = "wasm32"))]
use reqwest::{Client, header::AUTHORIZATION};
use serde_json::Value;
use std::{collections::VecDeque, time::Duration};
use url::Url;

#[cfg(target_arch = "wasm32")]
use js_sys::{Function, Promise, Reflect, Uint8Array};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;
#[cfg(target_arch = "wasm32")]
use web_sys::{ReadableStreamDefaultReader, Request, RequestInit, RequestMode};

/// Invalid endpoint, bearer token, CA, or response bound.
#[derive(Debug, thiserror::Error)]
#[error("invalid Stream HTTP configuration")]
pub struct ConnectError;

/// Bounded authenticated HTTP provider. Mutations are never automatically retried.
#[derive(Clone)]
pub struct HttpStream {
    #[cfg(not(target_arch = "wasm32"))]
    client: Client,
    endpoint: Url,
    authorization: String,
    maximum: usize,
}

#[cfg(target_arch = "wasm32")]
struct BrowserResponse {
    status: u16,
    url: String,
    content_type: Option<String>,
    content_length: Option<u64>,
    body: Vec<u8>,
}

#[cfg(target_arch = "wasm32")]
struct BrowserAbortGuard {
    global: JsValue,
    controller: web_sys::AbortController,
    timer_id: JsValue,
    _timer: Closure<dyn FnMut()>,
    abort_on_drop: bool,
}

#[cfg(target_arch = "wasm32")]
impl BrowserAbortGuard {
    fn clear_timer(&self) {
        if let Ok(value) = Reflect::get(&self.global, &JsValue::from_str("clearTimeout")) {
            if let Ok(clear_timeout) = value.dyn_into::<Function>() {
                let _ = clear_timeout.call1(&self.global, &self.timer_id);
            }
        }
    }

    fn finish(&mut self) {
        self.clear_timer();
        self.abort_on_drop = false;
    }
}

#[cfg(target_arch = "wasm32")]
impl Drop for BrowserAbortGuard {
    fn drop(&mut self) {
        self.clear_timer();
        if self.abort_on_drop {
            self.controller.abort();
        }
    }
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
        crate::http_validation::validate_endpoint(endpoint).map_err(|_| ConnectError)?;
        let mut endpoint = Url::parse(endpoint).map_err(|_| ConnectError)?;
        if !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || token.trim().is_empty()
            || maximum == 0
        {
            return Err(ConnectError);
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        if token.bytes().any(|byte| matches!(byte, b'\r' | b'\n')) {
            return Err(ConnectError);
        }
        let authorization = format!("Bearer {token}");
        #[cfg(not(target_arch = "wasm32"))]
        let mut client = Client::builder();
        #[cfg(not(target_arch = "wasm32"))]
        {
            client = client.timeout(Duration::from_secs(30));
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            client = client.redirect(reqwest::redirect::Policy::none());
        }
        if let Some(ca) = ca {
            if ca.is_empty() || ca.len() > 64 * 1024 {
                return Err(ConnectError);
            }
            #[cfg(target_arch = "wasm32")]
            return Err(ConnectError);
            #[cfg(not(target_arch = "wasm32"))]
            {
                client = client.add_root_certificate(
                    reqwest::Certificate::from_pem(ca).map_err(|_| ConnectError)?,
                );
            }
        }
        Ok(Self {
            #[cfg(not(target_arch = "wasm32"))]
            client: client.build().map_err(|_| ConnectError)?,
            endpoint,
            authorization,
            maximum,
        })
    }

    #[cfg(target_arch = "wasm32")]
    async fn browser_fetch(
        &self,
        url: &Url,
        method: &str,
        body: Option<&[u8]>,
        accept: Option<&str>,
        content_type: Option<&str>,
        timeout_ms: i32,
    ) -> Result<BrowserResponse, String> {
        let init = RequestInit::new();
        init.set_method(method);
        init.set_mode(RequestMode::Cors);
        init.set_redirect(web_sys::RequestRedirect::Error);
        let controller = web_sys::AbortController::new().map_err(js_error)?;
        init.set_signal(Some(&controller.signal()));
        if let Some(body) = body {
            let bytes = Uint8Array::from(body);
            init.set_body(&JsValue::from(bytes));
        }
        let request = Request::new_with_str_and_init(url.as_str(), &init).map_err(js_error)?;
        request
            .headers()
            .set("authorization", &self.authorization)
            .map_err(js_error)?;
        if let Some(accept) = accept {
            request.headers().set("accept", accept).map_err(js_error)?;
        }
        if let Some(content_type) = content_type {
            request
                .headers()
                .set("content-type", content_type)
                .map_err(js_error)?;
        }
        let global = js_sys::global();
        let fetch = Reflect::get(&global, &JsValue::from_str("fetch"))
            .map_err(js_error)?
            .dyn_into::<Function>()
            .map_err(|_| "global fetch unavailable".to_owned())?;
        let timer = {
            let controller = controller.clone();
            Closure::wrap(Box::new(move || controller.abort()) as Box<dyn FnMut()>)
        };
        let set_timeout = Reflect::get(&global, &JsValue::from_str("setTimeout"))
            .map_err(js_error)?
            .dyn_into::<Function>()
            .map_err(|_| "global setTimeout unavailable".to_owned())?;
        let timer_id = set_timeout
            .call2(
                &global,
                timer.as_ref().unchecked_ref(),
                &JsValue::from_f64(timeout_ms as f64),
            )
            .map_err(js_error)?;
        let mut guard = BrowserAbortGuard {
            global: global.clone().into(),
            controller,
            timer_id,
            _timer: timer,
            abort_on_drop: true,
        };
        let promise = fetch
            .call1(&global, &request)
            .map_err(js_error)?
            .dyn_into::<Promise>()
            .map_err(|_| "global fetch returned a non-promise value".to_owned())?;
        let result = async {
            let response = JsFuture::from(promise)
                .await
                .map_err(js_error)?
                .dyn_into::<web_sys::Response>()
                .map_err(|_| "browser fetch returned a non-response value".to_owned())?;
            let content_length = response
                .headers()
                .get("content-length")
                .map_err(js_error)?
                .and_then(|value| value.parse().ok());
            let content_type = response.headers().get("content-type").map_err(js_error)?;
            let mut body = Vec::new();
            if let Some(stream) = response.body() {
                let reader = stream
                    .get_reader()
                    .dyn_into::<ReadableStreamDefaultReader>()
                    .map_err(|_| "browser response reader unavailable".to_owned())?;
                loop {
                    let result = JsFuture::from(reader.read()).await.map_err(js_error)?;
                    let done = Reflect::get(&result, &JsValue::from_str("done"))
                        .map_err(js_error)?
                        .as_bool()
                        .unwrap_or(false);
                    if done {
                        break;
                    }
                    let value =
                        Reflect::get(&result, &JsValue::from_str("value")).map_err(js_error)?;
                    let chunk = Uint8Array::new(&value);
                    if chunk.length() as usize > self.maximum.saturating_sub(body.len()) {
                        let _ = reader.cancel();
                        return Err("browser response exceeds configured bound".to_owned());
                    }
                    body.extend_from_slice(&chunk.to_vec());
                }
            }
            Ok(BrowserResponse {
                status: response.status(),
                url: response.url(),
                content_type,
                content_length,
                body,
            })
        }
        .await;
        if result.is_ok() {
            guard.finish();
        }
        result
    }
    /// Prove the canonical contract before selecting this transport.
    ///
    /// # Errors
    /// Rejects authentication failures, redirects, malformed responses, and identity mismatches.
    pub async fn verify_handshake(&self) -> Result<bool, crate::client::ConnectError> {
        use crate::client::ConnectError;
        use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
        use prost_reflect::{DescriptorPool, DynamicMessage};
        let malformed = || ConnectError::Negotiation("invalid control handshake response".into());
        let family = BindingFamily::Stream;
        let version = control::control_protocol_version(family);
        let route = control::handshake_http_route(family.name()).ok_or_else(malformed)?;
        let url = self
            .endpoint
            .join(route.trim_start_matches('/'))
            .map_err(|_| malformed())?;
        #[cfg(not(target_arch = "wasm32"))]
        let (status, final_url, content_type, content_length, bytes) = {
            let response = self
                .client
                .get(url.clone())
                .timeout(Duration::from_secs(10))
                .header(AUTHORIZATION, self.authorization.as_str())
                .header("accept", "application/json")
                .send()
                .await
                .map_err(|error| ConnectError::Transport(error.to_string()))?;
            let status = response.status().as_u16();
            let final_url = response.url().clone();
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let content_length = response.content_length();
            let maximum = self.maximum.min(control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES);
            if matches!(status, 404 | 405) {
                return Ok(false);
            }
            if !response.status().is_success() {
                return Err(ConnectError::HttpStatus(status));
            }
            if content_length.is_some_and(|length| length > maximum as u64) {
                return Err(malformed());
            }
            let mut bytes = Vec::new();
            let mut chunks = response.bytes_stream();
            while let Some(chunk) = chunks.next().await {
                let chunk = chunk.map_err(|error| ConnectError::Transport(error.to_string()))?;
                if chunk.len() > maximum.saturating_sub(bytes.len()) {
                    return Err(malformed());
                }
                bytes.extend_from_slice(&chunk);
            }
            (status, final_url, content_type, content_length, bytes)
        };
        #[cfg(target_arch = "wasm32")]
        let (status, final_url, content_type, content_length, bytes) = {
            let response = self
                .browser_fetch(&url, "GET", None, Some("application/json"), None, 10_000)
                .await
                .map_err(ConnectError::Transport)?;
            let status = response.status;
            let final_url = response.url;
            let content_type = response.content_type;
            let content_length = response.content_length;
            let bytes = response.body;
            if matches!(status, 404 | 405) {
                return Ok(false);
            }
            if !(200..300).contains(&status) {
                return Err(ConnectError::HttpStatus(status));
            }
            (status, final_url, content_type, content_length, bytes)
        };
        if final_url.as_str() != url.as_str() {
            return Err(malformed());
        }
        if matches!(status, 404 | 405) {
            return Ok(false);
        }
        if !content_type.as_deref().is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|kind| kind.trim().eq_ignore_ascii_case("application/json"))
        }) {
            return Err(malformed());
        }
        let maximum = self.maximum.min(control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES);
        if content_length.is_some_and(|length| length > maximum as u64) || bytes.len() > maximum {
            return Err(malformed());
        }
        let pool = DescriptorPool::decode(
            acyclic_sdk_contract_wire::protocol::protocol_descriptor().as_slice(),
        )
        .map_err(|_| malformed())?;
        let descriptor = pool
            .get_message_by_name("acyclic.protocol.v1.HandshakeResponse")
            .ok_or_else(malformed)?;
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let decoded =
            DynamicMessage::deserialize(descriptor, &mut deserializer).map_err(|_| malformed())?;
        deserializer.end().map_err(|_| malformed())?;
        control::validate_handshake_response(
            family,
            version,
            &[control::RequiredCapability {
                name: family.name(),
                version,
            }],
            &decoded.encode_to_vec(),
            maximum,
        )
        .map_err(|_| malformed())?;
        Ok(true)
    }

    async fn request(&self, route: &str, bytes: Vec<u8>) -> Result<Value, StreamError> {
        let body = http_codec::encode(route, &bytes).map_err(contract_error)?;
        let url = self
            .endpoint
            .join(&format!("v1/stream/{route}"))
            .map_err(|_| StreamError::Unavailable)?;
        #[cfg(not(target_arch = "wasm32"))]
        let (success, content_length, body) = {
            let mut response = self
                .client
                .post(url)
                .timeout(Duration::from_secs(30))
                .header(AUTHORIZATION, self.authorization.as_str())
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .map_err(|_| StreamError::Unavailable)?;
            let success = response.status().is_success();
            let content_length = response.content_length();
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
            (success, content_length, body)
        };
        #[cfg(target_arch = "wasm32")]
        let (success, content_length, body) = {
            let response = self
                .browser_fetch(
                    &url,
                    "POST",
                    Some(body.as_bytes()),
                    None,
                    Some("application/json"),
                    30_000,
                )
                .await
                .map_err(|_| StreamError::Unavailable)?;
            (
                (200..300).contains(&response.status),
                response.content_length,
                response.body,
            )
        };
        if content_length.is_some_and(|length| length > self.maximum as u64)
            || body.len() > self.maximum
        {
            return Err(StreamError::Unavailable);
        }
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
        let records = stream::iter(self.read_page(request, true).await?.into_iter().map(Ok));
        #[cfg(not(target_arch = "wasm32"))]
        return Ok(records.boxed());
        #[cfg(target_arch = "wasm32")]
        return Ok(records.boxed_local());
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        if from > self.tail(path.clone()).await? {
            return Err(StreamError::OutOfRange);
        }
        let follow = stream::try_unfold(
            (self.clone(), path, from, VecDeque::<Record>::new()),
            |(provider, path, mut next, mut queued)| async move {
                loop {
                    if let Some(record) = queued.pop_front() {
                        next = record
                            .sequence
                            .checked_add(1)
                            .ok_or(StreamError::Unavailable)?;
                        return Ok(Some((record, (provider, path, next, queued))));
                    }
                    queued = provider
                        .read_page(
                            ReadRequest {
                                path: path.clone(),
                                from: next,
                                limit: 256,
                            },
                            false,
                        )
                        .await?
                        .into();
                    if queued.is_empty() {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            },
        );
        #[cfg(not(target_arch = "wasm32"))]
        return Ok(follow.boxed());
        #[cfg(target_arch = "wasm32")]
        return Ok(follow.boxed_local());
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
        let children = stream::iter(children.into_iter().map(Ok));
        #[cfg(not(target_arch = "wasm32"))]
        return Ok(children.boxed());
        #[cfg(target_arch = "wasm32")]
        return Ok(children.boxed_local());
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
fn parse_string(value: &Value) -> Result<&str, StreamError> {
    value.as_str().ok_or(StreamError::Unavailable)
}

#[cfg(target_arch = "wasm32")]
fn js_error(error: JsValue) -> String {
    error.as_string().unwrap_or_else(|| format!("{error:?}"))
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
        "invalid_path" => StreamError::InvalidPath,
        "invalid_argument" => StreamError::InvalidArgument,
        "limit_exceeded" => StreamError::LimitExceeded,
        "not_found" | "stream_not_found" | "commit_not_found" => StreamError::NotFound,
        "already_exists" | "destination_exists" => StreamError::AlreadyExists,
        "out_of_range" => StreamError::OutOfRange,
        "hierarchy_changed" => StreamError::HierarchyChanged,
        "capacity" | "capacity_exhausted" => StreamError::Capacity,
        "access_denied" => StreamError::AccessDenied,
        "idempotency_mismatch" => StreamError::IdempotencyMismatch,
        "prefix_not_retained" => StreamError::PrefixNotRetained,
        "deadline_elapsed" => StreamError::DeadlineElapsed,
        "unsupported" => StreamError::Unsupported,
        _ => StreamError::Unavailable,
    }
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value, StreamError> {
    value.get(name).ok_or(StreamError::Unavailable)
}
