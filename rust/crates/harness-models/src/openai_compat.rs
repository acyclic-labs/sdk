//! Streaming [`ModelProvider`] over the OpenAI-compatible chat completions API.
//!
//! [`OpenAiCompatibleProvider`] `POST`s `{base_url}/chat/completions` with
//! `stream: true` and decodes the server-sent-event transcript into ordered
//! [`ModelEvent`]s:
//!
//! * `delta.content` fragments become [`ModelEvent::Content`];
//! * `delta.reasoning_content` / `delta.reasoning` fragments become
//!   [`ModelEvent::Reasoning`];
//! * `delta.tool_calls` fragments are accumulated by index and emitted as
//!   complete [`ModelEvent::ToolCall`]s once the stream finishes, because the
//!   Harness contract has no partial tool call;
//! * the finish reason and the inline `usage` object (sent on the final chunk,
//!   including `OpenRouter`'s `cost`) become [`ModelEvent::Completed`] metadata,
//!   encoded as [`CompletionMetadata`].
//!
//! Tool names cross the wire sanitised (`acyclic.shell` becomes `acyclic_shell`)
//! because the function-name grammar rejects dots; the mapping is reversed on
//! the way back so the Harness sees registered names.

use crate::{
    config::{Dialect, ProviderConfig},
    error::ProviderError,
    usage::{CompletionMetadata, Usage},
};
use acyclic_harness::{
    Result,
    model::{ModelAttempt, ModelEvent, ModelMessage, ModelProvider, ModelRequest},
    tool::ToolInvocation,
};
use bytes::{Buf as _, Bytes, BytesMut};
use futures::{
    FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future::BoxFuture, stream::BoxStream,
};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    time::Duration,
};

/// Request body keys owned by the provider; model options may not override them.
const RESERVED_KEYS: &[&str] = &[
    "model",
    "messages",
    "stream",
    "stream_options",
    "tools",
    "max_tokens",
    "user",
    "usage",
];

/// [`ModelProvider`] over the OpenAI-compatible streaming chat completions API.
pub struct OpenAiCompatibleProvider {
    config: ProviderConfig,
    http: reqwest::Client,
}

impl fmt::Debug for OpenAiCompatibleProvider {
    /// Renders only what [`ProviderConfig`]'s `Debug` does: no key, no full URL.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiCompatibleProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl OpenAiCompatibleProvider {
    /// Creates a provider with its own HTTP client.
    ///
    /// # Errors
    /// [`ProviderError::Invalid`] when the TLS-backed client cannot be built.
    pub fn new(config: ProviderConfig) -> std::result::Result<Self, ProviderError> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| ProviderError::invalid(error.without_url().to_string()))?;
        Ok(Self::with_client(config, http))
    }

    /// Creates a provider that shares an existing HTTP client.
    #[must_use]
    pub fn with_client(config: ProviderConfig, http: reqwest::Client) -> Self {
        Self { config, http }
    }

    /// Configuration in use.
    #[must_use]
    pub fn config(&self) -> &ProviderConfig {
        &self.config
    }

    /// Starts one request and yields ordered events with typed failures.
    ///
    /// [`ModelProvider::generate`] is this stream with each [`ProviderError`]
    /// converted into a Harness error.
    pub fn stream(
        &self,
        request: &ModelRequest,
    ) -> BoxStream<'_, std::result::Result<ModelEvent, ProviderError>> {
        let mapped = map_request(&self.config, request);
        futures::stream::once(async move {
            let MappedRequest { body, tool_names } = mapped?;
            let response = self.open_stream(&body).await?;
            let bytes = response
                .bytes_stream()
                .map_err(|error| ProviderError::Unavailable {
                    status: None,
                    message: format!("stream failed: {}", error.without_url()),
                });
            let bytes = idle_limited(bytes, self.config.idle_timeout);
            let decoder = StreamDecoder::new(tool_names)
                .with_dialect(self.config.dialect)
                .with_user(self.config.user.clone());
            Ok(decode_sse(bytes, decoder))
        })
        .try_flatten()
        .boxed()
    }

    async fn open_stream(
        &self,
        body: &Value,
    ) -> std::result::Result<reqwest::Response, ProviderError> {
        let mut attempt = 0;
        loop {
            match self.send_once(body).await {
                Ok(response) => return Ok(response),
                Err(SendFailure {
                    error,
                    retry_safe: true,
                }) if error.is_retryable() && attempt + 1 < self.config.retry.max_attempts => {
                    let retry_after = match &error {
                        ProviderError::RateLimited { retry_after, .. } => *retry_after,
                        _ => None,
                    };
                    tokio::time::sleep(self.config.retry.delay(attempt, retry_after)).await;
                    attempt += 1;
                }
                Err(SendFailure { error, .. }) => return Err(error),
            }
        }
    }

    /// Sends the request once.
    ///
    /// A failure is `retry_safe` only when the provider cannot have started a
    /// generation for it: it answered with an error status, or the connection
    /// was never established. Chat completions carry no idempotency key, so a
    /// reset or timeout after the body may have been sent is not retried.
    async fn send_once(&self, body: &Value) -> std::result::Result<reqwest::Response, SendFailure> {
        let mut request = self
            .http
            .post(self.config.completions_url())
            .bearer_auth(&self.config.api_key)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(body);
        if let Some(app) = &self.config.app {
            request = request
                .header("HTTP-Referer", &app.referer)
                .header("X-Title", &app.title);
        }
        let idle = self.config.idle_timeout;
        let response = match tokio::time::timeout(idle, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return Err(SendFailure {
                    retry_safe: error.is_connect(),
                    error: ProviderError::Unavailable {
                        status: None,
                        message: format!("request failed: {}", error.without_url()),
                    },
                });
            }
            Err(_) => {
                return Err(SendFailure {
                    retry_safe: false,
                    error: ProviderError::Unavailable {
                        status: None,
                        message: format!("no response headers within {}s", idle.as_secs_f64()),
                    },
                });
            }
        };
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        // The status alone still classifies the failure if the body stalls; the
        // placeholder tells the operator why any body-derived detail (such as an
        // OpenRouter key-limit 403) is missing instead of hanging the turn.
        let body = match tokio::time::timeout(idle, response.text()).await {
            Ok(Ok(body)) => body,
            Ok(Err(error)) => format!("<error body unreadable: {}>", error.without_url()),
            Err(_) => format!("<error body not received within {}s>", idle.as_secs_f64()),
        };
        Err(SendFailure {
            retry_safe: true,
            error: ProviderError::from_status(
                self.config.dialect,
                status.as_u16(),
                retry_after,
                &body,
            ),
        })
    }
}

/// A failed [`OpenAiCompatibleProvider::send_once`].
struct SendFailure {
    error: ProviderError,
    /// Whether resending cannot duplicate a generation the provider started.
    retry_safe: bool,
}

/// Fails the stream with [`ProviderError::Unavailable`] when `source` yields
/// nothing for `idle`, so a stalled connection cannot hang the turn.
fn idle_limited<'a, S>(
    source: S,
    idle: Duration,
) -> impl Stream<Item = std::result::Result<Bytes, ProviderError>> + Send + 'a
where
    S: Stream<Item = std::result::Result<Bytes, ProviderError>> + Send + 'a,
{
    futures::stream::unfold(Some(source.boxed()), move |source| async move {
        let mut source = source?;
        match tokio::time::timeout(idle, source.next()).await {
            Ok(Some(item)) => Some((item, Some(source))),
            Ok(None) => None,
            Err(_) => Some((
                Err(ProviderError::Unavailable {
                    status: None,
                    message: format!("stream idle for {}s", idle.as_secs_f64()),
                }),
                None,
            )),
        }
    })
}

impl ModelProvider for OpenAiCompatibleProvider {
    fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
        self.stream(&request)
            .map_err(acyclic_harness::Error::from)
            .boxed()
    }

    /// Chat completions expose no handle to resume an in-flight generation, so an
    /// interrupted attempt can never be continued from its observed prefix.
    /// `Ok(None)` reports that honestly; the stock executor then surfaces
    /// `Error::Indeterminate` for the operation.
    fn reconcile<'a>(
        &'a self,
        _attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        async { Ok(None) }.boxed()
    }
}

/// A Harness request mapped onto the chat completions wire format.
#[derive(Clone, Debug, PartialEq)]
pub struct MappedRequest {
    /// JSON request body.
    pub body: Value,
    /// Wire tool name to registered Harness tool name.
    pub tool_names: BTreeMap<String, String>,
}

/// Maps a Harness [`ModelRequest`] onto a streaming chat completions body.
///
/// The model name is `request.model.name`. An object in `request.model.options`
/// is merged into the body (for example `temperature`, `reasoning`, or
/// `OpenRouter`'s `provider` routing preferences). Message content follows the
/// stock executor: `system`/`user` content is a string (or an object with a
/// `text` field, otherwise serialised JSON); `assistant` content is
/// `{"tool_calls": [ToolInvocation]}` and/or text; `tool` content is
/// `{"call_id", "name", "result"}`.
///
/// # Errors
/// [`ProviderError::Invalid`] when the request has no messages, a role is
/// unknown, a tool message lacks its `call_id`, two tool names collide on the
/// wire, or model options are not an object or override a reserved key.
pub fn map_request(
    config: &ProviderConfig,
    request: &ModelRequest,
) -> std::result::Result<MappedRequest, ProviderError> {
    if request.messages.is_empty() {
        return Err(ProviderError::invalid("model request has no messages"));
    }
    let messages = request
        .messages
        .iter()
        .map(map_message)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut tool_names = BTreeMap::new();
    let mut tools = Vec::with_capacity(request.tools.len());
    for tool in &request.tools {
        let wire = wire_tool_name(&tool.name);
        if let Some(existing) = tool_names.insert(wire.clone(), tool.name.clone()) {
            return Err(ProviderError::invalid(format!(
                "tools {existing} and {} both map to wire name {wire}",
                tool.name
            )));
        }
        tools.push(json!({
            "type": "function",
            "function": {
                "name": wire,
                "description": tool.description,
                "parameters": tool.input_schema,
            },
        }));
    }
    let mut body = match &request.model.options {
        Value::Null => Map::new(),
        Value::Object(options) => {
            if let Some(key) = RESERVED_KEYS.iter().find(|key| options.contains_key(**key)) {
                return Err(ProviderError::invalid(format!(
                    "model options may not set reserved key {key}"
                )));
            }
            options.clone()
        }
        _ => {
            return Err(ProviderError::invalid(
                "model options must be an object or null",
            ));
        }
    };
    body.insert("model".into(), Value::String(request.model.name.clone()));
    body.insert("messages".into(), Value::Array(messages));
    body.insert("stream".into(), Value::Bool(true));
    body.insert("stream_options".into(), json!({"include_usage": true}));
    if config.dialect == Dialect::OpenRouter {
        body.insert("usage".into(), json!({"include": true}));
    }
    if let Some(user) = &config.user {
        body.insert("user".into(), Value::String(user.clone()));
    }
    if !tools.is_empty() {
        body.insert("tools".into(), Value::Array(tools));
    }
    if let Some(max) = request.max_output_tokens {
        body.insert("max_tokens".into(), Value::from(max));
    }
    Ok(MappedRequest {
        body: Value::Object(body),
        tool_names,
    })
}

fn map_message(message: &ModelMessage) -> std::result::Result<Value, ProviderError> {
    match message.role.as_str() {
        "system" | "user" => Ok(json!({
            "role": message.role,
            "content": text_content(&message.content),
        })),
        "assistant" => map_assistant(&message.content),
        "tool" => {
            let call_id = message
                .content
                .get("call_id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| ProviderError::invalid("tool message lacks call_id"))?;
            let result = message
                .content
                .get("result")
                .cloned()
                .unwrap_or(Value::Null);
            Ok(json!({
                "role": "tool",
                "tool_call_id": call_id,
                "content": text_content(&result),
            }))
        }
        other => Err(ProviderError::invalid(format!(
            "unsupported message role {other}"
        ))),
    }
}

fn map_assistant(content: &Value) -> std::result::Result<Value, ProviderError> {
    let mut mapped = Map::new();
    mapped.insert("role".into(), Value::String("assistant".into()));
    let calls = content.get("tool_calls").and_then(Value::as_array);
    let text = match calls {
        Some(_) => content.get("text").map(text_content).unwrap_or_default(),
        None => text_content(content),
    };
    mapped.insert(
        "content".into(),
        if text.is_empty() {
            Value::Null
        } else {
            Value::String(text)
        },
    );
    if let Some(calls) = calls {
        let calls = calls
            .iter()
            .map(|call| {
                let invocation: ToolInvocation =
                    serde_json::from_value(call.clone()).map_err(|error| {
                        ProviderError::invalid(format!("assistant tool call is malformed: {error}"))
                    })?;
                Ok(json!({
                    "id": invocation.call_id,
                    "type": "function",
                    "function": {
                        "name": wire_tool_name(&invocation.name),
                        "arguments": invocation.arguments.to_string(),
                    },
                }))
            })
            .collect::<std::result::Result<Vec<_>, ProviderError>>()?;
        mapped.insert("tool_calls".into(), Value::Array(calls));
    }
    Ok(Value::Object(mapped))
}

fn text_content(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => match other.get("text") {
            Some(Value::String(text)) => text.clone(),
            _ => other.to_string(),
        },
    }
}

/// Sanitises a Harness tool name for the function-name grammar
/// (`[A-Za-z0-9_-]`, at most 64 characters).
#[must_use]
pub fn wire_tool_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect()
}

/// Decodes a server-sent-event byte stream into ordered model events.
///
/// Every chunk is decoded as it arrives; tool calls and the completion are
/// emitted as soon as `[DONE]` arrives, without waiting for the body to close.
/// A transcript that ends without both a finish reason and `[DONE]` (so the
/// trailing usage chunk may be missing too) yields an error rather than a
/// fabricated completion.
pub fn decode_sse<'a, S>(
    source: S,
    decoder: StreamDecoder,
) -> BoxStream<'a, std::result::Result<ModelEvent, ProviderError>>
where
    S: Stream<Item = std::result::Result<Bytes, ProviderError>> + Send + 'a,
{
    let state = DecodeState {
        source: source.boxed(),
        buffer: BytesMut::new(),
        decoder: Some(decoder),
        pending: VecDeque::new(),
    };
    futures::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                return Some((Ok(event), state));
            }
            let decoder = state.decoder.as_mut()?;
            if let Some(frame) = take_sse_event(&mut state.buffer) {
                if let Err(error) = decoder
                    .push_frame(&frame)
                    .map(|events| state.pending.extend(events))
                {
                    state.decoder = None;
                    return Some((Err(error), state));
                }
                if decoder.is_done() {
                    let decoder = state.decoder.take()?;
                    match decoder.finish() {
                        Ok(events) => state.pending.extend(events),
                        Err(error) => return Some((Err(error), state)),
                    }
                }
                continue;
            }
            match state.source.next().await {
                Some(Ok(chunk)) => state.buffer.extend_from_slice(&chunk),
                Some(Err(error)) => {
                    state.decoder = None;
                    return Some((Err(error), state));
                }
                None => {
                    let trailing = state.buffer.split().freeze();
                    let mut decoder = state.decoder.take()?;
                    let finished = decoder.push_frame(&trailing).and_then(|events| {
                        state.pending.extend(events);
                        decoder.finish()
                    });
                    match finished {
                        Ok(events) => state.pending.extend(events),
                        Err(error) => return Some((Err(error), state)),
                    }
                }
            }
        }
    })
    .boxed()
}

struct DecodeState<'a> {
    source: BoxStream<'a, std::result::Result<Bytes, ProviderError>>,
    buffer: BytesMut,
    /// `None` once the transcript is finished or failed.
    decoder: Option<StreamDecoder>,
    pending: VecDeque<ModelEvent>,
}

/// Splits one complete SSE event (terminated by a blank line) off `buffer`.
///
/// Per the SSE grammar each line ends in `\r\n`, `\n`, or `\r`, and they may
/// be mixed, so an event ends wherever one line terminator is immediately
/// followed by another (`\n\n`, `\r\n\r\n`, `\n\r\n`, `\r\r`, ...).
fn take_sse_event(buffer: &mut BytesMut) -> Option<Bytes> {
    let (end, consumed) = find_event_end(buffer)?;
    let event = buffer.split_to(end).freeze();
    buffer.advance(consumed - end);
    Some(event)
}

/// Returns the index where the first event's content ends and where the blank
/// line terminating it ends.
fn find_event_end(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut index = 0;
    while index < bytes.len() {
        let Some(first) = line_terminator(bytes, index) else {
            index += 1;
            continue;
        };
        if let Some(second) = line_terminator(bytes, index + first) {
            return Some((index, index + first + second));
        }
        index += first;
    }
    None
}

/// Length of the line terminator starting at `index`, if any.
///
/// A trailing `\r` counts as a one-byte terminator: if the next chunk opens
/// with `\n`, that byte is left as an empty line, which decodes to nothing.
fn line_terminator(bytes: &[u8], index: usize) -> Option<usize> {
    match (bytes.get(index), bytes.get(index + 1)) {
        (Some(b'\r'), Some(b'\n')) => Some(2),
        (Some(b'\r' | b'\n'), _) => Some(1),
        _ => None,
    }
}

/// Joins the `data:` lines of one SSE event; comments such as `OpenRouter`'s
/// `: OPENROUTER PROCESSING` keep-alives are ignored.
fn sse_data(frame: &[u8]) -> String {
    String::from_utf8_lossy(frame)
        .split(['\r', '\n'])
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|line| line.strip_prefix(' ').unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Incremental decoder of chat completion chunks into model events.
#[derive(Debug)]
pub struct StreamDecoder {
    tool_names: BTreeMap<String, String>,
    dialect: Dialect,
    calls: BTreeMap<u64, PartialCall>,
    metadata: CompletionMetadata,
    done: bool,
}

#[derive(Debug, Default)]
struct PartialCall {
    id: Option<String>,
    name: String,
    arguments: String,
}

impl StreamDecoder {
    /// Creates a decoder that reverses `tool_names` on emitted tool calls.
    #[must_use]
    pub fn new(tool_names: BTreeMap<String, String>) -> Self {
        Self {
            tool_names,
            dialect: Dialect::OpenAi,
            calls: BTreeMap::new(),
            metadata: CompletionMetadata::default(),
            done: false,
        }
    }

    /// Classifies in-stream errors using `dialect`.
    #[must_use]
    pub fn with_dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Records the request attribution id in the completion metadata.
    #[must_use]
    pub fn with_user(mut self, user: Option<String>) -> Self {
        self.metadata.user = user;
        self
    }

    /// Whether the `[DONE]` sentinel has been seen; nothing after it is decoded.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Consumes one raw SSE event and returns the content/reasoning events it carries.
    ///
    /// # Errors
    /// [`ProviderError::Protocol`] when a data payload is not a JSON chunk, or
    /// the classified provider error when the chunk carries an `error` object
    /// (an `OpenRouter` mid-stream 402 is [`ProviderError::BudgetExhausted`]).
    pub fn push_frame(
        &mut self,
        frame: &[u8],
    ) -> std::result::Result<Vec<ModelEvent>, ProviderError> {
        let data = sse_data(frame);
        let data = data.trim();
        if data.is_empty() || self.done {
            return Ok(Vec::new());
        }
        if data == "[DONE]" {
            self.done = true;
            return Ok(Vec::new());
        }
        let chunk: Value = serde_json::from_str(data)
            .map_err(|error| ProviderError::protocol(format!("chunk is not JSON: {error}")))?;
        if let Some(error) = chunk.get("error") {
            return Err(self.stream_error(error));
        }
        for (key, slot) in [
            ("id", &mut self.metadata.id),
            ("model", &mut self.metadata.model),
            ("provider", &mut self.metadata.provider),
        ] {
            if let Some(value) = chunk.get(key).and_then(Value::as_str) {
                *slot = Some(value.to_owned());
            }
        }
        if let Some(usage) = chunk.get("usage").and_then(Usage::from_wire) {
            self.metadata.usage = Some(usage);
        }
        let mut events = Vec::new();
        for choice in chunk
            .get("choices")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(delta) = choice.get("delta") {
                self.push_delta(delta, &mut events);
            }
            if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                self.metadata.finish_reason = Some(reason.to_owned());
            }
        }
        Ok(events)
    }

    fn stream_error(&self, error: &Value) -> ProviderError {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(|| error.to_string(), str::to_owned);
        let status = error.get("code").and_then(|code| {
            code.as_u64()
                .or_else(|| code.as_str().and_then(|code| code.parse().ok()))
        });
        match status.and_then(|status| u16::try_from(status).ok()) {
            Some(status) => ProviderError::from_status(self.dialect, status, None, &message),
            None => ProviderError::Unavailable {
                status: None,
                message,
            },
        }
    }

    fn push_delta(&mut self, delta: &Value, events: &mut Vec<ModelEvent>) {
        if let Some(text) = delta.get("content").and_then(Value::as_str)
            && !text.is_empty()
        {
            events.push(ModelEvent::Content {
                delta: text.to_owned(),
            });
        }
        for key in ["reasoning_content", "reasoning"] {
            if let Some(text) = delta.get(key).and_then(Value::as_str)
                && !text.is_empty()
            {
                events.push(ModelEvent::Reasoning {
                    delta: text.to_owned(),
                });
            }
        }
        let calls = delta
            .get("tool_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten();
        for (position, call) in (0_u64..).zip(calls) {
            let index = call
                .get("index")
                .and_then(Value::as_u64)
                .unwrap_or(position);
            let partial = self.calls.entry(index).or_default();
            if let Some(id) = call.get("id").and_then(Value::as_str)
                && !id.is_empty()
            {
                partial.id = Some(id.to_owned());
            }
            if let Some(function) = call.get("function") {
                if let Some(name) = function.get("name").and_then(Value::as_str) {
                    partial.name.push_str(name);
                }
                if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                    partial.arguments.push_str(arguments);
                }
            }
        }
    }

    /// Emits the accumulated tool calls and the completion.
    ///
    /// # Errors
    /// [`ProviderError::Protocol`] when the transcript ended before `[DONE]`
    /// (the connection closed early, possibly before the trailing usage chunk)
    /// or `[DONE]` arrived without any finish reason; [`ProviderError::Invalid`]
    /// when a tool call carries no id (the result could never be correlated,
    /// so none is fabricated) or its arguments are not JSON.
    pub fn finish(self) -> std::result::Result<Vec<ModelEvent>, ProviderError> {
        if !self.done {
            return Err(ProviderError::protocol("stream ended before [DONE]"));
        }
        if self.metadata.finish_reason.is_none() {
            return Err(ProviderError::protocol(
                "stream reached [DONE] without a finish reason",
            ));
        }
        let mut events = Vec::with_capacity(self.calls.len() + 1);
        for (index, call) in self.calls {
            let name = self
                .tool_names
                .get(&call.name)
                .cloned()
                .unwrap_or(call.name);
            let call_id = call.id.ok_or_else(|| {
                ProviderError::invalid(format!("tool call {name} at index {index} has no id"))
            })?;
            let arguments = if call.arguments.trim().is_empty() {
                json!({})
            } else {
                serde_json::from_str(&call.arguments).map_err(|error| {
                    ProviderError::invalid(format!(
                        "tool call {name} arguments are not valid JSON: {error}"
                    ))
                })?
            };
            events.push(ModelEvent::ToolCall {
                call_id,
                name,
                arguments,
            });
        }
        events.push(ModelEvent::Completed {
            metadata: self.metadata.to_value(),
        });
        Ok(events)
    }
}
