//! Golden prompt-cache prefixes: every family's serialized request is a
//! byte-exact extension of the request before it, across a tool call, a
//! tools.changed delta, a waking notice and a fork whose child continues and
//! forks a grandchild of its own, and a history window that advances a page.
//! Then the window kernel: an overflow backstop keeps the header bytes, and
//! the turn's next step extends the evicted request.
//!
//! The run is the shared hosting composition (one [`Bound`] over in-memory
//! streams and objects, the stock preset's executors) with each provider
//! family's own projection and HTTP encoding. Only the transport is scripted.

use super::TestResult;
use acyclic_harness::{
    agent::{AgentId, AgentPath, InboxRecord, IntentId, Message, Sender},
    approvals::ApprovalPolicy,
    context::{CompactionPolicy, ModelContextCapacity, ThresholdCompaction},
    contract::{Capabilities, capability},
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef, wire::Notice},
    executor::Executor,
    filesystem::{
        PRIVATE_VOLUME, Tasks, TurnAuthority, TurnExecutors, TurnLimits,
        hosting::{Bound, Placement, RootRequest, RootSubmission, Turns},
    },
    model::ModelContent,
    resources::ProviderRef,
    runtime::{Bindings, RuntimeScope},
};
use acyclic_harness_providers::{
    Adapter, Authentication, Credentials, HttpRequest, HttpResponse, HttpTransport, ModelFeatures,
    ModelSpec, ProviderSpec, Registry, TokenCountField, TokenCounterSpec,
};
use acyclic_stream::{BoxProviderFuture, MemoryStream};
use bytes::Bytes;
use harness_mock_model::{Step, Tool};
use serde_json::json;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// The backstop marker's opening bytes.
const MARKER: &[u8] = b"[context backstop:";

fn holds(body: &[u8], needle: &[u8]) -> bool {
    body.windows(needle.len()).any(|window| window == needle)
}

/// One recorded generation request: the agent that sent it and its exact body.
struct Sent {
    agent: &'static str,
    body: Bytes,
}

/// Scripted replies in request order, then a plain answer for any later turn.
/// Counts are 32 tokens until a per-byte weight is scripted.
struct Scripted {
    path: &'static str,
    state: Mutex<(&'static str, VecDeque<Step>, Vec<Sent>)>,
    weight: std::sync::atomic::AtomicU64,
}

impl Scripted {
    fn label(&self, agent: &'static str) {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).0 = agent;
    }

    fn reply(&self, request: HttpRequest) -> acyclic_harness::Result<Bytes> {
        if request.url.ends_with("/count") {
            let tokens = match self.weight.load(Ordering::SeqCst) {
                0 => 32,
                weight => request.body.len() as u64 * weight,
            };
            return Ok(Bytes::from(format!(r#"{{"input_tokens":{tokens}}}"#)));
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let agent = state.0;
        state.2.push(Sent {
            agent,
            body: request.body,
        });
        let step = state.1.pop_front().unwrap_or_else(|| text("ok"));
        let frames = harness_mock_model::events(self.path, &step)
            .ok_or_else(|| acyclic_harness::Error::Invalid("unscripted reply".into()))?;
        Ok(Bytes::from(frames.concat()))
    }
}

impl HttpTransport for Scripted {
    fn delay(&self, _: Duration) -> BoxProviderFuture<'static, acyclic_harness::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn now(&self) -> Duration {
        Duration::ZERO
    }
    fn delay_with_native_work(
        &self,
        _: Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<acyclic_harness::NativeFutureBundle<'static, ()>> {
        acyclic_harness::NativeFutureBundle::new(work, || async { Ok(()) })
    }
    fn send(
        &self,
        request: HttpRequest,
    ) -> BoxProviderFuture<'_, acyclic_harness::Result<HttpResponse>> {
        Box::pin(async move {
            let bytes = self.reply(request)?;
            Ok(HttpResponse {
                retry_after: None,
                request_id: None,
                status: 200,
                body: Box::pin(futures::stream::iter([Ok(bytes)])),
            })
        })
    }
    fn supports_native(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<bool> {
        work.remaining()?;
        Ok(true)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<acyclic_harness::NativeFutureBundle<'a, HttpResponse>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let bytes = self.reply(request)?;
            HttpResponse::with_native_body(200, futures::stream::iter([Ok(bytes)]), work)
        })
    }
}

fn text(answer: &str) -> Step {
    Step {
        text: vec![answer.into()],
        ..Step::default()
    }
}

fn program(id: &str, code: &str) -> Step {
    Step {
        tools: vec![Tool {
            id: id.into(),
            name: "code".into(),
            namespace: None,
            arguments: json!({ "code": code }),
        }],
        ..Step::default()
    }
}

/// Turn executors whose tool table gains one tool once `changed` is set, as a
/// newly installed tool does mid-run.
struct Changing {
    before: Arc<dyn TurnExecutors>,
    after: Arc<dyn TurnExecutors>,
    changed: AtomicBool,
}

impl Changing {
    fn current(&self) -> &Arc<dyn TurnExecutors> {
        if self.changed.load(Ordering::SeqCst) {
            &self.after
        } else {
            &self.before
        }
    }
}

impl TurnExecutors for Changing {
    fn render_bytes(&self) -> Option<u64> {
        self.current().render_bytes()
    }
    fn templates(&self) -> bool {
        self.current().templates()
    }
    fn executor<'a>(
        &'a self,
        agent: &'a acyclic_harness::agent::AgentState,
        turn: acyclic_harness::OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness::Result<Arc<dyn Executor>>> {
        self.current().executor(agent, turn, input, authority)
    }
}

fn executors(
    model: Arc<acyclic_harness_providers::ResolvedModel>,
    extra: &[&str],
) -> TestResult<Arc<dyn TurnExecutors>> {
    let selected = ["code", "spawn"].map(String::from);
    let tools = acyclic_harness::tools::registry(
        &selected,
        |_| false,
        extra
            .iter()
            .map(|name| super::providers::echo_tool(name))
            .collect(),
    )?;
    let limits = Limits::default();
    let mut grants =
        acyclic_harness::tools::call_grants(&tools, selected.iter().map(String::as_str))?;
    grants.push(capability::MODEL_GENERATE.into());
    grants.push(capability::INTERACTION_ROUTE.into());
    let bindings = Bindings {
        scope: RuntimeScope::new(Capabilities::new(grants), limits)?,
        ..Bindings::default()
    }
    .build()?;
    Ok(Arc::new(
        acyclic_harness::filesystem::StockExecutors::new(
            (model.model().clone(), model),
            tools,
            (
                limits,
                // Usable: 2^20 - 1024 tokens.
                CompactionPolicy::Threshold(ThresholdCompaction {
                    response_reserve_tokens: 1024,
                    recent_tokens: 1,
                }),
            ),
            Arc::new(Tasks(bindings)),
        )
        .with_agent_tools(&selected)
        .with_templates(),
    ))
}

fn resolve(
    adapter: Adapter,
    transport: Arc<Scripted>,
) -> TestResult<acyclic_harness_providers::ResolvedModel> {
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport);
    registry.register(
        "recorded".into(),
        ProviderSpec {
            adapter,
            authentication: Authentication::Optional,
            base_url: "https://recorded.invalid/v1".into(),
            headers: BTreeMap::new(),
            models: BTreeMap::from([(
                "model".into(),
                ModelSpec {
                    tool_profile: None,
                    thinking_levels: false,
                    max_context_tokens: None,
                    capacity: ModelContextCapacity {
                        context_tokens: 1 << 20,
                        output_tokens: Some(1024),
                    },
                    features: ModelFeatures {
                        tools: true,
                        images: false,
                    },
                },
            )]),
            token_counter: Some(TokenCounterSpec {
                path: "count".into(),
                adapter,
                response_field: TokenCountField::InputTokens,
                framing_tokens: 0,
            }),
            max_event_bytes: Some(1 << 16),
            retry: acyclic_harness_providers::RetrySpec::DEFAULT,
            timeouts: acyclic_harness_providers::TimeoutSpec { total_ms: None },
        },
    )?;
    Ok(registry.resolve("recorded/model")?)
}

fn intent(text: &str) -> IntentId {
    IntentId::try_from(text).expect("valid intent")
}

fn user() -> Sender {
    Sender::User {
        principal: "user-1".into(),
    }
}

/// Where `later` first departs from `earlier`'s open message array, if it does.
fn departure(earlier: &[u8], later: &[u8]) -> Option<usize> {
    // Every request ends with the renderer's trailer: the conversation's `]`,
    // then any per-call controls, none of which contains `],"`. A body
    // without that shape departs at its end instead of skipping the check.
    let Some(open) = earlier.strip_suffix(b"]}").or_else(|| {
        earlier
            .windows(3)
            .rposition(|window| window == b"],\"")
            .and_then(|at| earlier.get(..at))
            .filter(|_| earlier.ends_with(b"}"))
    }) else {
        return Some(earlier.len());
    };
    let shared = open.iter().zip(later).take_while(|(a, b)| a == b).count();
    if shared == open.len() && later.get(open.len()) == Some(&b',') {
        None
    } else {
        Some(shared)
    }
}

/// `later` extends `earlier` byte for byte: the same body up to its last
/// message, then only appended messages.
fn extends(family: &str, what: &str, earlier: &[u8], later: &[u8]) -> TestResult {
    let Some(at) = departure(earlier, later) else {
        return Ok(());
    };
    let window = |bytes: &[u8]| {
        let start = at.saturating_sub(160);
        String::from_utf8_lossy(&bytes[start..(at + 160).min(bytes.len())]).into_owned()
    };
    Err(format!(
        "{family}: {what} rewrote its prompt prefix at byte {at}\n  before: {}\n  after:  {}",
        window(earlier),
        window(later)
    )
    .into())
}

async fn run(adapter: Adapter, path: &'static str, family: &str, messages: &[u8]) -> TestResult {
    let transport = Arc::new(Scripted {
        path,
        state: Mutex::new((
            "parent",
            VecDeque::from([
                // Turn 1: a tool call and its continuation.
                program("call-1", "1 + 1"),
                text("one"),
                // Turn 2: a waking notice.
                text("noticed"),
                // Turn 3: the tool table changed; the parent forks a child.
                program(
                    "call-2",
                    "await tools.spawn({task: 'Count to two', name: 'kid', role: 'worker'})",
                ),
                text("spawned"),
                // The child continues from the fork with its own tool call.
                program("call-3", "2 + 2"),
                text("kid done"),
                // Asked again, the child forks a grandchild.
                program(
                    "call-4",
                    "await tools.spawn({task: 'Count to three', name: 'grandkid', role: 'worker'})",
                ),
                text("spawned again"),
            ]),
            Vec::new(),
        )),
        weight: 0.into(),
    });
    let model = Arc::new(resolve(adapter, transport.clone())?);
    let changing = Arc::new(Changing {
        before: executors(model.clone(), &[])?,
        after: executors(model, &["acyclic.added"])?,
        changed: AtomicBool::new(false),
    });
    let stream = Arc::new(MemoryStream::default());
    let provider = ProviderRef::new("agents".into(), "filesystem".into(), "2".into(), &mut None)?;
    let issuer = acyclic_harness::core::AuthorityRoot::new(
        "agents",
        [7; 32],
        acyclic_harness::core::Authority {
            kind: acyclic_harness::core::AggregateKind::Conversation,
            id: "local".into(),
        },
    );
    let filesystem = acyclic_fs::Fs::new(
        acyclic_fs::StreamAuthorityStore::new(stream.clone()),
        acyclic_fs::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let bound = Bound::local_root(
        issuer,
        Placement {
            filesystem: filesystem.clone(),
            provider: provider.clone(),
            within: acyclic_actors::addressing::ActorReference::new(
                b"account", b"agents", b"owner",
            )?,
        },
        Turns {
            executors: changing.clone(),
            limits: TurnLimits {
                maximum_bytes: Some(1 << 20),
                // A page of four entries: the parent's later turns cross one
                // page cut, while the child's history stays on the first.
                history_turns: Some(4),
            },
            actors: acyclic_actors::domain::ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(acyclic_actors::local::SystemClock),
            approval_policy: ApprovalPolicy::accept_all("prefix-cache"),
        },
    )?;
    let id = AgentId::from_bytes([1; 16]);
    let RootSubmission::Created(root) = bound
        .create_root(
            &intent("root"),
            RootRequest {
                agent: id.clone(),
                fresh: vec![acyclic_harness::agent::VolumeBinding {
                    name: PRIVATE_VOLUME.into(),
                    volume: VolumeRef::new(
                        provider.clone(),
                        "agent-private".into(),
                        VolumeClass::AgentPrivate,
                        VolumeOwner::Agent(acyclic_harness::AgentId::from_bytes(id.to_bytes())),
                        &mut None,
                    )?,
                }],
                retained: Vec::new(),
                brief: None,
                policy: None,
            },
        )
        .await?
    else {
        return Err("uncertain root".into());
    };
    let agents = bound.agents();
    // A fork with the same directive preserves the entire admitted prefix.
    // Use the same real template reader as production, with a home override.
    let root_template = acyclic_harness::seed::TREE
        .iter()
        .find(|(path, _)| *path == "templates/orchestrator/AGENT.md")
        .unwrap()
        .1;
    let child_template =
        std::str::from_utf8(root_template)?.replace("context: top_level", "context: fork");
    let set_template = |agent: AgentPath| {
        let filesystem = filesystem.clone();
        let provider = &provider;
        let bound = &bound;
        let child_template = &child_template;
        async move {
            let state = bound.agents().state(&agent).await?;
            let authority = acyclic_harness::filesystem::agent_authority(
                bound.issuer(),
                bound.admission(),
                &agent,
                &state.volumes,
                &[],
            )
            .await?;
            let host =
                acyclic_harness::filesystem::FilesystemHost::new(filesystem, provider, None)?;
            let writer = acyclic_harness::filesystem::FilesystemContentPublisher::new(
                host,
                &authority.private.volume,
                &authority.verifier,
                &authority.scope,
                acyclic_harness::conversation::MAX_EXACT_JS_INTEGER,
                None,
            )?;
            use acyclic_harness::conversation::ContentPublisher;
            writer
                .stage(
                    acyclic_harness::OperationId::new(),
                    "templates/worker/AGENT.md",
                    child_template.as_bytes(),
                    "text/markdown",
                    "AGENT.md",
                    acyclic_harness::conversation::ContentAccess::default(),
                )?
                .await?;
            TestResult::Ok(())
        }
    };
    set_template(root.clone()).await?;
    agents.drain(&root).await?;
    let message = |to: &AgentPath, name: &'static str, prompt: &'static str| {
        let bound = &bound;
        let to = to.clone();
        async move {
            let body = bound
                .stage_text(&to, "message", &user(), &intent(name), (prompt, prompt))
                .await?;
            Ok::<_, acyclic_harness::agent::Error>(InboxRecord::Message(Message {
                from: user(),
                id: intent(name),
                body: Box::new(body),
                attachments: None,
            }))
        }
    };
    agents
        .send(
            &root,
            &user(),
            &message(&root, "m1", "Compute one plus one").await?,
        )
        .await?;
    agents.drain(&root).await?;
    let notice = Notice {
        source: "filesystem".into(),
        kind: "files.changed".into(),
        body: json!({"paths": ["notes.md"]}),
        wake: true,
        coalesce_key: None,
    };
    let waking = bound
        .stage_notice(&root, &user(), &intent("wake"), &notice)
        .await?;
    agents.send(&root, &user(), &waking).await?;
    agents.drain(&root).await?;
    changing.changed.store(true, Ordering::SeqCst);
    agents
        .send(
            &root,
            &user(),
            &message(&root, "m3", "Delegate counting").await?,
        )
        .await?;
    agents.drain(&root).await?;
    let child: AgentPath = acyclic_harness::tools::spawned_child(&root, "kid")?;
    // Child homes are fresh. A matching grandchild directive requires an
    // explicit override in its own parent's home as well.
    set_template(child.clone()).await?;
    transport.label("child");
    agents.drain(&child).await?;
    agents
        .send(
            &child,
            &user(),
            &message(&child, "m4", "Delegate further").await?,
        )
        .await?;
    agents.drain(&child).await?;
    let grandchild = acyclic_harness::tools::spawned_child(&child, "grandkid")?;
    transport.label("grandchild");
    agents.drain(&grandchild).await?;
    transport.label("child");
    agents.drain(&child).await?;
    transport.label("parent");
    agents.drain(&root).await?;
    // Turns that carry the parent's conversation across its first page cut.
    for name in ["m5", "m6", "m7", "m8", "m9", "m10", "m11"] {
        agents
            .send(&root, &user(), &message(&root, name, "Keep going").await?)
            .await?;
        agents.drain(&root).await?;
    }

    let sent = std::mem::take(&mut transport.state.lock().unwrap_or_else(|p| p.into_inner()).2);
    // Near, then (about 1.2M of 1M usable tokens) over, the window, each
    // turn with a tool call. The irreducible set stays far inside it.
    for (name, weight) in [("m12", 20), ("m13", 40)] {
        transport.weight.store(weight, Ordering::SeqCst);
        transport
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .1
            .extend([program(name, "3 + 3"), text("filled")]);
        agents
            .send(
                &root,
                &user(),
                &message(&root, name, "Fill the window").await?,
            )
            .await?;
        agents.drain(&root).await?;
    }
    let tight = std::mem::take(&mut transport.state.lock().unwrap_or_else(|p| p.into_inner()).2);
    let bodies = |agent| -> Vec<&Bytes> {
        sent.iter()
            .filter(|sent| sent.agent == agent)
            .map(|sent| &sent.body)
            .collect()
    };
    let (parent, child, grandchild) = (bodies("parent"), bodies("child"), bodies("grandchild"));
    assert!(parent.len() >= 5, "{family}: parent sent {}", parent.len());
    assert!(child.len() >= 4, "{family}: child sent {}", child.len());
    assert_eq!(grandchild.len(), 1, "{family}: grandchild requests");
    // The window advances only at its page cut: exactly one request starts
    // a new prefix, and every other one extends the request before it.
    let mut cut = None;
    for (index, pair) in parent.windows(2).enumerate() {
        if departure(pair[0], pair[1]).is_some() && cut.is_none() {
            cut = Some(index + 1);
            continue;
        }
        extends(
            family,
            &format!("parent request {}", index + 1),
            pair[0],
            pair[1],
        )?;
    }
    let cut = cut.ok_or_else(|| format!("{family}: the history window never advanced"))?;
    assert!(
        cut + 1 < parent.len(),
        "{family}: nothing follows the page cut"
    );
    // The child forks the parent's settled conversation: the turn before the
    // spawning one, whose last request was the parent's third.
    extends(family, "child request 0", parent[2], child[0])?;
    for (index, pair) in child.windows(2).enumerate() {
        extends(
            family,
            &format!("child request {}", index + 1),
            pair[0],
            pair[1],
        )?;
    }
    // The grandchild forks the child's settled first turn, whose last request
    // was the child's second, and keeps the child's directive where it was.
    extends(family, "grandchild request 0", child[1], grandchild[0])?;
    // The oracle rejects one changed byte inside the shared prefix.
    let mut rewritten = parent[1].to_vec();
    rewritten[parent[0].len() / 2] ^= 1;
    assert!(departure(parent[0], &rewritten).is_some());
    // Call controls follow the conversation, so a changed control never
    // rewrites the prefix, while a body without that trailer departs.
    let mut controls = parent[0].to_vec();
    controls.truncate(controls.len() - 1);
    controls.extend_from_slice(b",\"after\":0}");
    assert!(departure(&controls, parent[1]).is_none());
    assert!(departure(b"{\"messages\":[]}", parent[1]).is_some());

    let tight: Vec<&Bytes> = tight.iter().map(|sent| &sent.body).collect();
    let [near, before, evicted, continued] = tight[..] else {
        return Err(format!("{family}: {} requests near the window", tight.len()).into());
    };
    // Within the window nothing is evicted.
    assert!(
        !holds(before, MARKER),
        "{family}: evicted within the window"
    );
    extends(family, "the step near the window", near, before)?;
    // The backstop replaces the oldest messages with its marker: the bytes
    // before the message array stay cached, and the turn's next step extends
    // the evicted request.
    assert!(
        holds(evicted, MARKER),
        "{family}: the backstop never evicted"
    );
    let start = before
        .windows(messages.len())
        .position(|window| window == messages)
        .ok_or_else(|| format!("{family}: no message array"))?
        + messages.len();
    let kept = departure(before, evicted).unwrap_or(before.len());
    assert!(
        kept >= start,
        "{family}: the backstop rewrote the header at byte {kept} (messages at {start})"
    );
    extends(family, "the step after the backstop", evicted, continued)?;
    Ok(())
}

#[tokio::test]
async fn anthropic_prefixes_extend_byte_for_byte() -> TestResult {
    run(
        Adapter::Anthropic,
        "messages",
        "anthropic",
        b"\"messages\":[",
    )
    .await
}

#[tokio::test]
async fn responses_prefixes_extend_byte_for_byte() -> TestResult {
    run(Adapter::Responses, "responses", "responses", b"\"input\":[").await
}

#[tokio::test]
async fn gemini_prefixes_extend_byte_for_byte() -> TestResult {
    run(
        Adapter::Gemini,
        "models/model:streamGenerateContent",
        "gemini",
        b"\"contents\":[",
    )
    .await
}

#[tokio::test]
async fn chat_prefixes_extend_byte_for_byte() -> TestResult {
    run(Adapter::Chat, "chat/completions", "chat", b"\"messages\":[").await
}
