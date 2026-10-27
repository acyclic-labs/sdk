//! One inference contract for every model provider, through its public API.
//! The caller supplies real captured responses or a live endpoint. These assertions
//! know no provider names, wire protocols, retry policies or credentials.

use crate::{
    Error, ModelTransportFailure, Result,
    conversation::Limits,
    model::{
        ModelContent, ModelContentPart, ModelDispatch, ModelEvent, ModelMessage, ModelProvider,
        ModelRole, PreparedModelRequest, ToolResultContent,
    },
};
use futures::{StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

mod counted;
use acyclic_stream::testing::{RequestLedger, Requests};
pub use counted::counted;

/// The complete inference inventory. A backend qualifies only after every case passes.
pub const SUITE: &[&str] = &[
    "streaming",
    "cancellation",
    "tools-results",
    "usage",
    "rate-limit",
    "context-overflow",
    "authentication",
    "deterministic-replay",
];

/// Setup requested by the common vectors. Backends configure endpoints and
/// captured responses; they cannot replace any assertion.
#[derive(Clone, Copy)]
pub enum Scenario {
    /// Successful text and usage.
    Text,
    /// A tool call followed by its paired result.
    Tools,
    /// A real rate refusal.
    RateLimit,
    /// A real context-capacity refusal.
    ContextOverflow,
    /// A real authentication refusal.
    Authentication,
    /// Repeatable captured responses, including for a live backend.
    Replay,
}

/// One fresh provider session, through the public model seam.
pub struct Session {
    /// Configured production provider.
    pub provider: Arc<dyn ModelProvider>,
    /// Actual dispatch ledger shared with the counted provider.
    pub requests: RequestLedger,
    /// Prepared public request.
    pub request: PreparedModelRequest,
    /// Dispatch identity bound to the request.
    pub dispatch: ModelDispatch,
    /// Observes release of the actual HTTP response body.
    pub released: Arc<dyn Fn() -> bool + Send + Sync>,
}

/// Backend setup only. Every backend executes the same complete vector inventory.
pub trait Backend {
    /// Supply a fresh provider and real response source for the requested case.
    fn session(&self, scenario: Scenario) -> Result<Session>;
    /// Exact token counts recorded independently from the original wire response.
    fn usage(&self) -> &ExpectedUsage;
    /// Exact counters for the tool-producing step and the paired-result step.
    fn tool_usage(&self) -> &[ExpectedUsage; 2];
}

/// Execute all inference vectors, unchanged, against one backend.
pub async fn verify(backend: &dyn Backend) -> Result<Vec<&'static str>> {
    let session = backend.session(Scenario::Text)?;
    streaming(
        session.provider.as_ref(),
        &session.request,
        session.dispatch,
        backend.usage(),
    )
    .await?;
    budget(&session, 1)?;
    let session = backend.session(Scenario::Text)?;
    cancellation(
        session.provider.as_ref(),
        &session.request,
        session.dispatch,
        || (session.released)(),
    )
    .await?;
    budget(&session, 1)?;
    let session = backend.session(Scenario::Tools)?;
    tools_and_results(
        session.provider.as_ref(),
        &session.request,
        session.dispatch,
        backend.tool_usage(),
    )
    .await?;
    budget(&session, 2)?;
    for (scenario, expected) in [
        (Scenario::RateLimit, Refusal::RateLimit),
        (Scenario::ContextOverflow, Refusal::ContextOverflow),
        (Scenario::Authentication, Refusal::Authentication),
    ] {
        let session = backend.session(scenario)?;
        refusal(
            session.provider.as_ref(),
            &session.request,
            session.dispatch,
            expected,
        )
        .await?;
        budget(&session, 1)?;
    }
    let session = backend.session(Scenario::Replay)?;
    deterministic_replay(
        session.provider.as_ref(),
        &session.request,
        session.dispatch,
    )
    .await?;
    budget(&session, 2)?;
    Ok(SUITE.to_vec())
}

fn budget(session: &Session, data: u64) -> Result<()> {
    let requests = session.requests.requests();
    require(
        requests.control() == 0 && requests.data() == data,
        "inference vector exceeded its zero-control dispatch budget or its ledger was not instrumented",
    )
}

/// Timing and exact provider dispatch for repeated, fully validated model output.
#[derive(Debug)]
pub struct Measurement {
    /// Completed requests after one unmeasured warmup.
    pub completions: u64,
    /// Time spent consuming and checking the completed streams.
    pub elapsed: std::time::Duration,
    /// Provider dispatch, including failures; hidden transport retries need their own ledger.
    pub requests: Requests,
}

/// Repeat the backend's real captured response through the original provider.
/// No generated or handwritten provider response is accepted by this suite.
pub async fn bench(backend: &dyn Backend) -> Result<Measurement> {
    use acyclic_stream::UnixMillisClock as _;
    let session = backend.session(Scenario::Replay)?;
    streaming(
        session.provider.as_ref(),
        &session.request,
        session.dispatch,
        backend.usage(),
    )
    .await?;
    let before = session.requests.requests();
    let clock = acyclic_stream::SystemUnixMillisClock;
    let started = clock.now_unix_millis();
    let completions = 64;
    for _ in 0..completions {
        let previous = session.requests.requests();
        streaming(
            session.provider.as_ref(),
            &session.request,
            session.dispatch,
            backend.usage(),
        )
        .await?;
        let requests = session.requests.requests().since(previous);
        require(
            requests.control() == 0 && requests.data() == 1,
            "inference benchmark exceeded its zero-control dispatch budget",
        )?;
    }
    Ok(Measurement {
        completions,
        elapsed: std::time::Duration::from_millis(clock.now_unix_millis().saturating_sub(started)),
        requests: session.requests.requests().since(before),
    })
}

/// Register the complete model vectors with an async fresh-backend factory.
pub use crate::model_conformance_suite as suite;
/// Implementation of [`suite!`].
#[macro_export]
macro_rules! model_conformance_suite {
    ($factory:path) => {
        $crate::model_conformance_suite!(model_conformance, $factory);
    };
    ($name:ident, $factory:path) => {
        #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
        #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
        async fn $name() -> std::result::Result<(), Box<dyn std::error::Error>> {
            let backend = $factory().await?;
            $crate::model::conformance::verify(&backend).await?;
            Ok(())
        }
    };
}

/// Exact provider usage counters retained in the public completion metadata.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExpectedUsage {
    /// JSON pointer relative to completion metadata for the input count.
    pub input: String,
    /// Exact captured input token count, including zero.
    pub input_tokens: u64,
    /// JSON pointer relative to completion metadata for the output count.
    pub output: String,
    /// Exact captured output token count, including zero.
    pub output_tokens: u64,
}

pub(crate) fn require(condition: bool, reason: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::Invalid(reason.into()))
    }
}

fn usage(metadata: &Value, expected: &ExpectedUsage) -> Result<()> {
    require(
        metadata.pointer(&expected.input).and_then(Value::as_u64) == Some(expected.input_tokens),
        "inference input usage differs from capture",
    )?;
    require(
        metadata.pointer(&expected.output).and_then(Value::as_u64) == Some(expected.output_tokens),
        "inference output usage differs from capture",
    )
}

fn completed_usage(events: &[ModelEvent], expected: &ExpectedUsage) -> Result<()> {
    match events.last() {
        Some(ModelEvent::Completed { metadata }) => usage(metadata, expected),
        _ => Err(Error::Invalid("inference usage completion absent".into())),
    }
}

/// Validates incremental output, exactly one final completion and exact usage counters.
pub async fn streaming(
    provider: &dyn ModelProvider,
    request: &PreparedModelRequest,
    dispatch: ModelDispatch,
    expected: &ExpectedUsage,
) -> Result<Vec<ModelEvent>> {
    let mut stream = provider.generate(request, dispatch);
    let mut events = Vec::new();
    let mut completed = false;
    let mut text = String::new();
    while let Some(event) = stream.next().await {
        let event = event?;
        require(!completed, "inference emitted after completion")?;
        match &event {
            ModelEvent::Content { delta } => text.push_str(delta),
            ModelEvent::Completed { metadata } => {
                usage(metadata, expected)?;
                completed = true;
            }
            _ => {}
        }
        events.push(event);
    }
    require(
        completed && !text.is_empty(),
        "inference did not stream text and complete",
    )?;
    Ok(events)
}

/// Dropping after the first visible delta must drop the backend's response immediately.
/// `released` observes the actual transport body, not a synthetic cancellation event.
pub async fn cancellation(
    provider: &dyn ModelProvider,
    request: &PreparedModelRequest,
    dispatch: ModelDispatch,
    released: impl Fn() -> bool,
) -> Result<()> {
    let mut stream = provider.generate(request, dispatch);
    loop {
        match stream.next().await.transpose()? {
            Some(ModelEvent::Content { delta }) if !delta.is_empty() => break,
            Some(ModelEvent::Completed { .. }) | None => {
                return Err(Error::Invalid(
                    "stream completed before cancellation point".into(),
                ));
            }
            _ => {}
        }
    }
    require(
        !released(),
        "stream consumed its response before cancellation",
    )?;
    drop(stream);
    require(released(), "cancellation retained the response body")
}

/// Replays identical captured responses at an identical admitted dispatch.
pub async fn deterministic_replay(
    provider: &dyn ModelProvider,
    request: &PreparedModelRequest,
    dispatch: ModelDispatch,
) -> Result<()> {
    let first: Vec<ModelEvent> = provider.generate(request, dispatch).try_collect().await?;
    let second: Vec<ModelEvent> = provider.generate(request, dispatch).try_collect().await?;
    require(
        first == second && !first.is_empty(),
        "inference replay changed public events",
    )
}

/// Public inference refusal categories; unknown and malformed requests cannot qualify.
#[derive(Clone, Copy, Debug)]
pub enum Refusal {
    /// A provider rate limit, after its configured bounded redelivery.
    RateLimit,
    /// Input exceeding the model's context window.
    ContextOverflow,
    /// Missing or rejected endpoint credentials.
    Authentication,
}

/// Requires a typed refusal and no successful event before it.
pub async fn refusal(
    provider: &dyn ModelProvider,
    request: &PreparedModelRequest,
    dispatch: ModelDispatch,
    expected: Refusal,
) -> Result<()> {
    let mut stream = provider.generate(request, dispatch);
    let Some(Err(error)) = stream.next().await else {
        return Err(Error::Invalid(
            "expected inference refusal before output".into(),
        ));
    };
    let matches = matches!(
        (expected, &error),
        (
            Refusal::RateLimit,
            Error::ModelUnavailable {
                failure: ModelTransportFailure::Status(429),
                ..
            }
        ) | (Refusal::ContextOverflow, Error::ModelContextOverflow)
            | (Refusal::Authentication, Error::Unauthorized(_))
            | (
                Refusal::Authentication,
                Error::ModelDiagnostic {
                    code: crate::wire::ErrorCode::Unauthorized,
                    ..
                }
            )
    );
    if !matches {
        return Err(Error::Invalid(format!(
            "inference refusal {expected:?} lost its typed classification: {error:?}"
        )));
    }
    require(
        stream.next().await.is_none(),
        "inference continued after refusal",
    )
}

/// Exercises a model tool call, then supplies its actual identity, arguments and
/// continuation metadata back through the public request API as a paired result.
pub async fn tools_and_results(
    provider: &dyn ModelProvider,
    request: &PreparedModelRequest,
    dispatch: ModelDispatch,
    expected: &[ExpectedUsage; 2],
) -> Result<()> {
    let events: Vec<ModelEvent> = provider.generate(request, dispatch).try_collect().await?;
    let calls: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ModelEvent::ToolCall(call) => Some(call.clone()),
            _ => None,
        })
        .collect();
    require(calls.len() == 1, "expected one registered tool call")?;
    let call = &calls[0];
    require(
        call.name == "echo" && call.arguments == json!({"value": 1}) && !call.call_id.is_empty(),
        "tool call lost its identity or arguments",
    )?;
    require(
        events
            .iter()
            .filter(|event| matches!(event, ModelEvent::Completed { .. }))
            .count()
            == 1,
        "tool step must complete exactly once",
    )?;
    require(
        matches!(events.last(), Some(ModelEvent::Completed { .. })),
        "tool output continued after completion",
    )?;
    completed_usage(&events, &expected[0])?;
    let mut followup = request.request().clone();
    let assistant: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ModelEvent::ToolCall(call) => Some(ModelContentPart::ToolCall(call.clone())),
            ModelEvent::ReasoningBlock { block } => Some(ModelContentPart::Reasoning {
                block: block.clone(),
            }),
            ModelEvent::Content { delta } => Some(ModelContentPart::Text {
                text: delta.clone(),
            }),
            _ => None,
        })
        .collect();
    followup.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Parts(assistant),
    });
    followup.messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: ModelContent::Part(ModelContentPart::ToolResult {
            call_id: call.call_id.clone(),
            name: call.name.clone(),
            content: ToolResultContent::Json {
                value: call.arguments.clone(),
            },
        }),
    });
    let followup = PreparedModelRequest::prepare(followup, Limits::default(), None)?;
    let events: Vec<ModelEvent> = provider
        .generate(
            &followup,
            ModelDispatch {
                step: dispatch.step + 1,
                request_digest: followup.request_digest(),
                ..dispatch
            },
        )
        .try_collect()
        .await?;
    require(
        matches!(events.last(), Some(ModelEvent::Completed { .. }))
            && events
                .iter()
                .any(|event| matches!(event, ModelEvent::Content { delta } if !delta.is_empty()))
            && events
                .iter()
                .filter(|event| matches!(event, ModelEvent::Completed { .. }))
                .count()
                == 1,
        "tool result did not produce a completed answer",
    )?;
    completed_usage(&events, &expected[1])
}
