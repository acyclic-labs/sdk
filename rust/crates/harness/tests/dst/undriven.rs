//! Every C2 timer rides the one simulated clock. Advancing that clock to a
//! deadline fires the timer exactly once, whether time is paused before it,
//! stops exactly on it, passes it, or the host is reopened first.
use super::*;
use acyclic_actors::local::{Clock as _, ClockControl, ControlledClock};
use acyclic_harness::{
    agent::{ConversationEntry, Phase},
    context::ModelContextCapacity,
    conversation::Limits,
    local::{Event, WorkspaceMounting, agent_conversation::Conversation},
    model::{
        ModelContent, ModelDispatch, ModelEvent, ModelMessage, ModelProvider, ModelRequest,
        ModelRole, ModelWait, PreparedModelRequest,
    },
    test_support::ScriptedModelProvider,
};
use acyclic_harness_providers::{
    Adapter, Authentication, Credentials, HttpRequest, HttpResponse, HttpTransport, ModelFeatures,
    ModelSpec, PromptCache, ProviderSpec, Registry, ResolvedModel, RetrySpec, TimeoutSpec,
};
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use family::{Generator, Variant};
use futures::StreamExt as _;
use oracle::{Plan, call, done, service, tag};
use serde_json::json;
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;

/// Timer names in the order their worlds run.
const TIMERS: [&str; 5] = [
    "wait_timeout",
    "call_deadline",
    "actor_alarm",
    "retry_after",
    "first_byte_deadline",
];

/// Clock positions one timer must survive.
const STATES: [&str; 4] = [
    "paused",
    "advanced_past",
    "advanced_exact",
    "reopened_after_crash",
];

/// One world per timer; every world reaches all four clock positions.
pub(super) const GENERATOR: Generator = Generator {
    class: "C2",
    seeds: every_timer_fires_once_on_the_simulated_clock::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = every_timer_fires_once_on_the_simulated_clock,
    values: u64,
    runner = super::run,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3, seed_4 => 4,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn timer(seed: u64) -> &'static str {
    TIMERS
        .get(usize::try_from(seed % TIMERS.len() as u64).unwrap_or(0))
        .copied()
        .unwrap_or("wait_timeout")
}

fn variants(seed: u64) -> Vec<Variant> {
    let timer = timer(seed);
    STATES
        .iter()
        .map(|state| Variant::from([("timer", timer), ("clock", *state)]))
        .collect()
}

/// A world's own simulated clock, durable files and model service. Each case
/// starts from the same base time; reopening reuses the same clock and state.
struct ClockWorld {
    _directory: tempfile::TempDir,
    checkout: PathBuf,
    state: PathBuf,
    clock: Arc<ControlledClock>,
    control: Mutex<ClockControl>,
    service: ScriptedModelProvider,
}

impl ClockWorld {
    fn new(seed: u64, plans: Vec<Plan>) -> std::result::Result<Self, String> {
        let directory = tempfile::Builder::new()
            .prefix("sdk-dst-clock-")
            .tempdir()
            .map_err(oracle::failed)?;
        let checkout = directory.path().join("checkout");
        std::fs::create_dir(&checkout).map_err(oracle::failed)?;
        let state = directory.path().join("state");
        let (clock, control) = ControlledClock::new(100);
        Ok(Self {
            _directory: directory,
            checkout,
            state,
            clock: Arc::new(clock),
            control: Mutex::new(control),
            service: service(plans),
        })
    }

    fn advance(&self, millis: u64) -> std::result::Result<(), String> {
        self.control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .advance_to(millis)
            .map_err(|error| error.to_string())
    }

    fn config(&self) -> Result<acyclic_harness::LocalHarnessConfig> {
        let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
        let provider = Arc::new(self.service.reopened());
        Ok(local_harness_config(self.checkout.clone(), (model, provider))?
            .without_user_settings()
            .workspace_mounting(WorkspaceMounting::Physical)
            .state_directory(self.state.clone())
            .clock(self.clock.clone()))
    }

    fn open(&self) -> EntryFuture<'_, Result<Arc<LocalHarness>>> {
        entry(async move { Ok(Arc::new(entry(LocalHarness::open(self.config()?)).await?)) })
    }

    fn service_requests(&self) -> usize {
        self.service.request_count()
    }
}

/// A prompt's wait is parked with this absolute deadline.
async fn parked(
    harness: &Arc<LocalHarness>,
    id: &str,
    prompt: &str,
) -> std::result::Result<
    (
        Arc<Conversation>,
        acyclic_harness::local::Observer,
        u64,
        u64,
    ),
    String,
> {
    let conversation = Conversation::open(harness.connect().await.map_err(oracle::failed)?);
    let mut observer = conversation
        .observe(None)
        .await
        .map_err(oracle::failed)?;
    let inbox = conversation
        .send(id, prompt)
        .await
        .map_err(oracle::failed)?;
    loop {
        match observer.next().await.map_err(oracle::failed)? {
            Event::Status(status) => {
                if let Phase::Waiting {
                    deadline_unix_ms: Some(deadline),
                    ..
                } = status.phase
                {
                    return Ok((conversation, observer, inbox, deadline));
                }
                if matches!(status.phase, Phase::Done { .. }) {
                    return Err(format!("{id} ended before its wait parked"));
                }
            }
            Event::Turn { entry, .. } if entry.inbox == inbox => {
                return Err(format!("{id} settled before its wait parked"));
            }
            _ => {}
        }
    }
}

async fn settled(
    observer: &mut acyclic_harness::local::Observer,
    inbox: u64,
) -> std::result::Result<(u64, ConversationEntry), String> {
    loop {
        match observer.next().await.map_err(oracle::failed)? {
            Event::Turn { sequence, entry } if entry.inbox == inbox => {
                return Ok((sequence, entry));
            }
            _ => {}
        }
    }
}

async fn drain(harness: &Arc<LocalHarness>) -> std::result::Result<(), String> {
    let (agents, root) = harness.agents().await.map_err(oracle::failed)?;
    agents.drain(root).await.map_err(oracle::failed)
}

async fn output(
    conversation: &Conversation,
    sequence: u64,
    entry: &ConversationEntry,
) -> std::result::Result<String, String> {
    conversation
        .output_text_at(sequence, entry)
        .await
        .map_err(oracle::failed)
}

fn wait_plan(timer: &str, child: &str) -> Vec<Plan> {
    let wait = match timer {
        "wait_timeout" => call("wait", "wait", json!({"timeout_ms": 20})),
        "actor_alarm" => call("wait", "wait", json!({"input": true, "timeout_ms": 20})),
        "call_deadline" => call(
            "wait",
            "wait",
            json!({"agents": [child], "timeout_ms": 20}),
        ),
        _ => unreachable!(),
    };
    let steps = if timer == "call_deadline" {
        vec![
            vec![call(
                "spawn",
                "spawn",
                json!({"name": child, "task": tag("child"), "role": "worker"}),
            )],
            vec![wait],
        ]
    } else {
        vec![vec![wait]]
    };
    let mut plans = vec![Plan {
        tag: if timer == "call_deadline" { "parent" } else { "wait" },
        steps,
        fail_once: None,
    }];
    if timer == "call_deadline" {
        plans.push(Plan {
            tag: "child",
            steps: vec![vec![call("wait", "wait", json!({"input": true}))]],
            fail_once: None,
        });
    }
    plans
}

async fn wait_case(
    timer: &str,
    state: &str,
    seed: u64,
) -> std::result::Result<(), String> {
    let child = format!("kid-{seed}-{state}");
    let world = ClockWorld::new(seed, wait_plan(timer, &child))?;
    let harness = world.open().await?;
    let id = format!("{timer}-{state}");
    let prompt_tag = if timer == "call_deadline" {
        tag("parent")
    } else {
        tag("wait")
    };
    let (mut conversation, mut observer, inbox, deadline) =
        parked(&harness, &id, &prompt_tag).await?;
    if deadline != 120 {
        return Err(format!(
            "{timer} {state}: deadline {deadline} did not use the world clock (100 + 20)"
        ));
    }
    match state {
        "paused" => {
            world.advance(deadline - 1)?;
            drain(&harness).await?;
            if !matches!(
                conversation.phase().await.map_err(oracle::failed)?,
                Phase::Waiting { deadline_unix_ms: Some(at), .. } if at == deadline
            ) {
                return Err(format!("{timer} paused: wait left before its deadline"));
            }
            world.advance(deadline)?;
            drain(&harness).await?;
        }
        "advanced_exact" => {
            world.advance(deadline)?;
            drain(&harness).await?;
        }
        "advanced_past" => {
            world.advance(deadline + 1)?;
            drain(&harness).await?;
        }
        "reopened_after_crash" => {
            drop(observer);
            drop(conversation);
            drop(harness);
            let reopened = world.open().await?;
            drain(&reopened).await?;
            world.advance(deadline)?;
            drain(&reopened).await?;
            let conversation = Conversation::open(reopened.connect().await.map_err(oracle::failed)?);
            let (sequence, entry) = loop {
                match conversation.reply(&id, inbox).await.map_err(oracle::failed)? {
                    acyclic_harness::agent::Reply::Answered { sequence, entry, .. } => {
                        break (sequence, entry);
                    }
                    acyclic_harness::agent::Reply::Dropped => {
                        return Err(format!("{timer} reopened: wait was dropped"));
                    }
                    acyclic_harness::agent::Reply::Pending
                    | acyclic_harness::agent::Reply::Continuing { .. } => {}
                }
            };
            let text = output(&conversation, sequence, &entry).await?;
            if text != done(if timer == "call_deadline" { "parent" } else { "wait" }) {
                return Err(format!("{timer} reopened: wait answered {text:?}"));
            }
            let after = world.service_requests();
            world.advance(deadline + 100)?;
            drain(&reopened).await?;
            if world.service_requests() != after {
                return Err(format!("{timer} reopened: deadline fired more than once"));
            }
            return Ok(());
        }
        _ => unreachable!(),
    }
    let (sequence, entry) = settled(&mut observer, inbox).await?;
    let text = output(&conversation, sequence, &entry).await?;
    let expected = done(if timer == "call_deadline" {
        "parent"
    } else {
        "wait"
    });
    if text != expected {
        return Err(format!("{timer} {state}: wait answered {text:?}"));
    }
    let after = world.service_requests();
    world.advance(deadline + 100)?;
    drain(&harness).await?;
    if world.service_requests() != after {
        return Err(format!("{timer} {state}: deadline fired more than once"));
    }
    Ok(())
}

struct TimedResponse {
    status: u16,
    body: &'static [u8],
    retry_after: Option<Duration>,
    hang: bool,
}

/// The provider's platform transport over the world's controlled clock.
struct ClockTransport {
    clock: Arc<ControlledClock>,
    responses: Mutex<VecDeque<TimedResponse>>,
    delays: mpsc::UnboundedSender<Duration>,
}

impl HttpTransport for ClockTransport {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        let response = self
            .responses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front();
        Box::pin(async move {
            let Some(response) = response else {
                return Err(Error::Invalid("provider response script exhausted".into()));
            };
            if response.hang {
                std::future::pending::<()>().await;
            }
            Ok(HttpResponse {
                status: response.status,
                body: Box::pin(futures::stream::iter([Ok(Bytes::from_static(
                    response.body,
                ))])),
                request_id: None,
                retry_after: response.retry_after,
            })
        })
    }

    fn delay(&self, duration: Duration) -> BoxProviderFuture<'static, Result<()>> {
        let clock = Arc::clone(&self.clock);
        let delays = self.delays.clone();
        Box::pin(async move {
            let _ = delays.send(duration);
            let deadline = clock
                .now_unix_millis()
                .saturating_add(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
            clock
                .sleep_until(deadline)
                .await
                .map_err(|error| Error::Invalid(error.to_string()))
        })
    }

    fn now(&self) -> Duration {
        Duration::from_millis(self.clock.now_unix_millis())
    }

    fn unix_ms(&self) -> Result<u64> {
        Ok(self.clock.now_unix_millis())
    }
}

fn model_spec() -> ModelSpec {
    ModelSpec {
        tool_profile: None,
        thinking_levels: false,
        max_context_tokens: None,
        capacity: ModelContextCapacity {
            context_tokens: 8192,
            output_tokens: Some(1024),
            output_shares_context: false,
        },
        features: ModelFeatures::default(),
    }
}

fn provider(
    transport: Arc<dyn HttpTransport>,
    timeouts: TimeoutSpec,
    retry: RetrySpec,
) -> std::result::Result<ResolvedModel, String> {
    let mut registry = Registry::with_transport(Credentials::default(), transport);
    registry
        .register(
            "dst".into(),
            ProviderSpec {
                adapter: Adapter::Chat,
                authentication: Authentication::Optional,
                base_url: "http://127.0.0.1:1/v1".into(),
                allow_http_host: None,
                headers: BTreeMap::new(),
                models: BTreeMap::from([("model".into(), model_spec())]),
                token_counter: None,
                max_event_bytes: Some(4096),
                retry,
                timeouts,
                prompt_cache: PromptCache::Native,
            },
        )
        .map_err(oracle::failed)?;
    registry.resolve("dst/model").map_err(oracle::failed)
}

fn request(provider: &ResolvedModel) -> std::result::Result<(PreparedModelRequest, ModelDispatch), String> {
    let prepared = PreparedModelRequest::prepare(
        ModelRequest {
            summary: false,
            compose_tools: false,
            model: provider.model().clone(),
            messages: vec![
                ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text("system".into()),
                },
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("user".into()),
                },
            ],
            tools: Vec::new(),
            sampling: Default::default(),
        },
        Limits::default(),
        None,
    )
    .map_err(oracle::failed)?;
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness::OperationId::new(),
        step: 0,
        request_digest: prepared.request_digest(),
    };
    Ok((prepared, dispatch))
}

async fn provider_exchange(
    timer: &str,
    state: &str,
    clock: &Arc<ControlledClock>,
    control: &mut ClockControl,
    transport: Arc<ClockTransport>,
    delays: &mut mpsc::UnboundedReceiver<Duration>,
) -> std::result::Result<Vec<ModelWait>, String> {
    let timeouts = if timer == "first_byte_deadline" {
        TimeoutSpec {
            total_ms: Some(20),
        }
    } else {
        TimeoutSpec::DEFAULT
    };
    let retry = RetrySpec {
        base_delay_ms: 0,
        max_delay_ms: 0,
    };
    let provider = provider(transport, timeouts, retry)?;
    let (prepared, dispatch) = request(&provider)?;
    let mut stream = provider.generate(&prepared, dispatch);
    let mut first_event = {
        let mut next = stream.next();
        tokio::pin!(next);
        let duration = tokio::select! {
            delay = delays.recv() => delay.ok_or_else(|| "provider timer was never registered".to_owned())?,
            event = &mut next => return Err(format!("{timer} {state}: exchange ended before its timer: {event:?}")),
        };
        let expected = Duration::from_millis(20);
        if duration != expected {
            return Err(format!("{timer} {state}: timer waited {duration:?}"));
        }
        let deadline = clock
            .now_unix_millis()
            .saturating_add(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
        match state {
            "paused" => {
                control
                    .advance_to(deadline - 1)
                    .map_err(|error| error.to_string())?;
                if futures::poll!(next.as_mut()).is_pending() {
                    control
                        .advance_to(deadline)
                        .map_err(|error| error.to_string())?;
                } else {
                    return Err(format!("{timer} paused: timer fired before its deadline"));
                }
            }
            "advanced_exact" => control
                .advance_to(deadline)
                .map_err(|error| error.to_string())?,
            "advanced_past" => control
                .advance_to(deadline + 1)
                .map_err(|error| error.to_string())?,
            _ => return Err(format!("{timer} {state}: provider reopen is not this case")),
        }
        next.await
    };
    let mut waits = Vec::new();
    loop {
        match first_event {
            Some(Ok(ModelEvent::Wait(wait))) => waits.push(wait),
            Some(Ok(ModelEvent::Completed { .. })) => break,
            Some(Ok(_)) => {}
            Some(Err(error)) => {
                return Err(format!("{timer} {state}: exchange failed: {error}"));
            }
            None => break,
        }
        first_event = stream.next().await;
    }
    Ok(waits)
}

fn responses(timer: &str) -> VecDeque<TimedResponse> {
    let first = match timer {
        "retry_after" => TimedResponse {
            status: 429,
            body: br#"{"detail":"rate limited"}"#,
            retry_after: Some(Duration::from_millis(20)),
            hang: false,
        },
        "first_byte_deadline" => TimedResponse {
            status: 0,
            body: b"",
            retry_after: None,
            hang: true,
        },
        _ => unreachable!(),
    };
    let second = TimedResponse {
        status: 200,
        body: b"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n",
        retry_after: None,
        hang: false,
    };
    VecDeque::from([first, second])
}

async fn provider_case(
    timer: &str,
    state: &str,
    seed: u64,
) -> std::result::Result<(), String> {
    let _ = seed;
    let (clock, mut control) = ControlledClock::new(100);
    let clock = Arc::new(clock);
    if state == "reopened_after_crash" {
        let (delays_tx, mut delays) = mpsc::unbounded_channel();
        let transport = Arc::new(ClockTransport {
            clock: Arc::clone(&clock),
            responses: Mutex::new(responses(timer)),
            delays: delays_tx,
        });
        let timeouts = if timer == "first_byte_deadline" {
            TimeoutSpec {
                total_ms: Some(20),
            }
        } else {
            TimeoutSpec::DEFAULT
        };
        let provider = provider(transport, timeouts, RetrySpec { base_delay_ms: 0, max_delay_ms: 0 })?;
        let (prepared, dispatch) = request(&provider)?;
        let mut stream = provider.generate(&prepared, dispatch);
        let mut next = stream.next();
        tokio::pin!(next);
        tokio::select! {
            delay = delays.recv() => {
                if delay != Some(Duration::from_millis(20)) {
                    return Err(format!("{timer} reopened: first timer was not registered"));
                }
            }
            event = &mut next => return Err(format!("{timer} reopened: first exchange ended: {event:?}")),
        }
        drop(next);
        drop(stream);
        drop(provider);
        let (delays_tx, mut delays) = mpsc::unbounded_channel();
        let transport = Arc::new(ClockTransport {
            clock: Arc::clone(&clock),
            responses: Mutex::new(responses(timer)),
            delays: delays_tx,
        });
        let waits = provider_exchange(
            timer,
            "advanced_exact",
            &clock,
            &mut control,
            transport,
            &mut delays,
        )
        .await?;
        return check_waits(timer, state, &waits);
    }
    let (delays_tx, mut delays) = mpsc::unbounded_channel();
    let transport = Arc::new(ClockTransport {
        clock: Arc::clone(&clock),
        responses: Mutex::new(responses(timer)),
        delays: delays_tx,
    });
    let waits = provider_exchange(timer, state, &clock, &mut control, transport, &mut delays).await?;
    check_waits(timer, state, &waits)
}

fn check_waits(
    timer: &str,
    state: &str,
    waits: &[ModelWait],
) -> std::result::Result<(), String> {
    let started = waits
        .iter()
        .filter(|wait| matches!(wait, ModelWait::Started { .. }))
        .count();
    let finished = waits
        .iter()
        .filter(|wait| matches!(wait, ModelWait::Finished { .. }))
        .count();
    if started != 1 || finished != 1 {
        return Err(format!(
            "{timer} {state}: saw {started} timer starts and {finished} finishes: {waits:?}"
        ));
    }
    Ok(())
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let timer = timer(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            for state in STATES {
                if matches!(timer, "retry_after" | "first_byte_deadline") {
                    provider_case(timer, state, seed).await?;
                } else {
                    wait_case(timer, state, seed).await?;
                }
            }
            Ok(true)
        })
    })
}
