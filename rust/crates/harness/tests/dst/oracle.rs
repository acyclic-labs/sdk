//! The shared settlement and fault oracle, and the scripted world every class
//! generator drives: the shipping local composition, a deterministic model
//! service that answers from the visible history, and a crash at any durable
//! cut followed by an independent reopen.
use super::*;
use acyclic_harness::{
    conversation::{DirectoryTarget, PrivateDirectoryEntryKind},
    local::WorkspaceMounting,
    model::{ModelContentPart, ModelEvent, ModelRequest, ModelRole, ToolCall, ToolResultContent},
    test_support::{ScriptedEvent, model_visible_messages},
};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply, ReadTarget};
use acyclic_native_runtime::testing::{DurableFault, InjectedStorageFault};
use driver::{Driver, Outcome};
use std::sync::Mutex;

/// One call and the one result the model saw for it.
#[derive(Clone, Debug)]
pub(super) struct Settled {
    pub call: ToolCall,
    pub result: ToolResultContent,
}

impl Settled {
    pub fn succeeded(&self) -> bool {
        !matches!(self.result, ToolResultContent::Error { .. })
    }

    /// The result as the model reads it.
    pub fn rendered(&self) -> String {
        serde_json::to_string(&self.result).unwrap_or_default()
    }

    pub fn path(&self) -> Option<&str> {
        self.call
            .arguments
            .get("path")
            .and_then(serde_json::Value::as_str)
    }
}

/// The settlement invariant over one provider request: every visible call is
/// followed by exactly one result naming it, and no result names a call that
/// is absent or already settled. A request breaking it is one a provider
/// rejects, so the owner could never make progress again.
pub(super) fn settlements(request: &ModelRequest) -> std::result::Result<Vec<Settled>, String> {
    let mut calls: Vec<(ToolCall, Option<ToolResultContent>)> = Vec::new();
    for message in model_visible_messages(&request.messages) {
        for part in message.content.parts() {
            match part {
                ModelContentPart::ToolCall(call) => {
                    if calls.iter().any(|(seen, _)| seen.call_id == call.call_id) {
                        return Err(format!("call {} is visible twice", call.call_id));
                    }
                    calls.push((call.clone(), None));
                }
                ModelContentPart::ToolResult {
                    call_id, content, ..
                } => {
                    let Some((_, result)) =
                        calls.iter_mut().find(|(call, _)| &call.call_id == call_id)
                    else {
                        return Err(format!("result {call_id} names no earlier call"));
                    };
                    if result.replace(content.clone()).is_some() {
                        return Err(format!("call {call_id} settled twice"));
                    }
                }
                _ => {}
            }
        }
    }
    calls
        .into_iter()
        .map(|(call, result)| match result {
            Some(result) => Ok(Settled { call, result }),
            None => Err(format!(
                "call {} ({}) never settled",
                call.call_id, call.name
            )),
        })
        .collect()
}

/// Every request the service received satisfies [`settlements`].
pub(super) fn all_settled(requests: &[ModelRequest]) -> std::result::Result<(), String> {
    for (index, request) in requests.iter().enumerate() {
        settlements(request).map_err(|cause| format!("request {index}: {cause}"))?;
    }
    Ok(())
}

pub(super) fn call(id: &str, name: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall::new(id.into(), name.into(), arguments)
}

/// What one tagged prompt asks of the model: tool steps, then `done <tag>`.
pub(super) struct Plan {
    pub tag: &'static str,
    pub steps: Vec<Vec<ToolCall>>,
    /// Ends the first answer to this step with this failure after its
    /// calls, as a provider refusing or losing the exchange mid-stream does.
    pub fail_once: Option<(usize, ScriptedEvent)>,
}

/// The prompt that selects a plan: exactly its tag.
pub(super) fn tag(tag: &str) -> String {
    format!("[dst:{tag}]")
}

/// How a prompt appears in a request: inline, or as the content digest of
/// the input file the host stored it in.
pub(super) fn appears(tag_text: &str) -> [String; 2] {
    use sha2::Digest as _;
    let digest: [u8; 32] = sha2::Sha256::digest(tag_text.as_bytes()).into();
    [
        tag_text.to_owned(),
        format!(
            "\"sha256\":{}",
            serde_json::to_string(&digest).unwrap_or_default()
        ),
    ]
}

pub(super) fn done(tag: &str) -> String {
    format!("done {tag}")
}

/// A deterministic model service: each answer is a function of the request's
/// visible history alone, so every incarnation of a host sees the same model.
pub(super) fn service(plans: Vec<Plan>) -> ScriptedModelProvider {
    let failed = Mutex::new(Vec::<&'static str>::new());
    let needles: Vec<_> = plans.iter().map(|plan| appears(&tag(plan.tag))).collect();
    let answer = move |request: &ModelRequest| {
        let messages: Vec<_> = model_visible_messages(&request.messages).collect();
        let found = messages
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, message)| message.role == ModelRole::User)
            .find_map(|(index, message)| {
                // Prompts reach the model inline or as a stored input file.
                let body = serde_json::to_string(&message.content).unwrap_or_default();
                plans
                    .iter()
                    .zip(&needles)
                    .find(|(_, needles)| needles.iter().any(|needle| body.contains(needle)))
                    .map(|(plan, _)| (index, plan))
            });
        let Some((index, plan)) = found.filter(|_| !request.summary) else {
            // Name the unexpected request in the answer the world reports.
            let last: Vec<_> = messages.iter().rev().take(2).collect();
            return ScriptedTurn::Text(format!(
                "untagged request ending {}",
                serde_json::to_string(&last).unwrap_or_default()
            ));
        };
        let step = messages
            .iter()
            .skip(index)
            .filter(|message| {
                message
                    .content
                    .parts()
                    .iter()
                    .any(|part| matches!(part, ModelContentPart::ToolCall(_)))
            })
            .count();
        let Some(calls) = plan.steps.get(step) else {
            return ScriptedTurn::Text(done(plan.tag));
        };
        let mut events: Vec<_> = calls
            .iter()
            .map(|call| ScriptedEvent::Event(ModelEvent::ToolCall(call.clone())))
            .collect();
        let mut failed = failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, failure)) = plan
            .fail_once
            .as_ref()
            .filter(|(at, _)| *at == step && !failed.contains(&plan.tag))
        {
            failed.push(plan.tag);
            events.push(failure.clone());
        } else {
            events.push(ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::Value::Null,
            }));
        }
        ScriptedTurn::Events(events)
    };
    ScriptedModelProvider::responding(move |request| Ok(answer(request)))
}

/// How one prompt of an incarnation ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Prompted {
    Settled(String),
    Failed(String),
}

/// One host incarnation: what its prompts returned, what the service saw and
/// where its durable cut, if any, crashed it.
pub(super) struct Incarnation {
    pub prompts: Vec<(Prompted, usize)>,
    pub requests: Vec<ModelRequest>,
    /// Durable operations completed when each dispatch reached the service.
    pub dispatched_at: Vec<usize>,
    /// Durable operations the whole incarnation performed.
    pub durable: usize,
    pub crashed: Option<InjectedStorageFault>,
}

thread_local! {
    /// Live world slots on this test thread, keyed by case and seed. Reusing
    /// the first free slot makes a repeated world use the same absolute path.
    static WORLD_SLOTS: std::cell::RefCell<
        std::collections::BTreeMap<(String, u64), Vec<bool>>,
    > = const { std::cell::RefCell::new(std::collections::BTreeMap::new()) };
}

/// A world directory whose absolute path is a function of its case, seed,
/// slot and process, never of the host's random temporary name. Model requests
/// announce that path, so a random name would make an otherwise identical
/// replay differ; the process identity keeps concurrent processes apart.
struct WorldDirectory {
    path: std::path::PathBuf,
    key: (String, u64),
    slot: usize,
}

/// A process's start nonce, so two processes that reuse a pid still name
/// different world directories.
fn process_nonce() -> u64 {
    static NONCE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *NONCE.get_or_init(|| {
        let nanos = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_nanos(),
            Err(_) => 0,
        };
        let pid = u64::from(std::process::id());
        let mut hash = blake3::Hasher::new();
        hash.update(&pid.to_le_bytes());
        hash.update(&nanos.to_le_bytes());
        let bytes = hash.finalize();
        let mut nonce = [0; 8];
        nonce.copy_from_slice(&bytes.as_bytes()[..8]);
        u64::from_le_bytes(nonce)
    })
}

impl WorldDirectory {
    fn new(seed: u64) -> std::io::Result<Self> {
        let key = (
            std::thread::current().name().unwrap_or("dst").to_owned(),
            seed,
        );
        let slot = WORLD_SLOTS.with(|slots| {
            let mut slots = slots.borrow_mut();
            let used = slots.entry(key.clone()).or_default();
            match used.iter().position(|used| !used) {
                Some(slot) => {
                    used[slot] = true;
                    slot
                }
                None => {
                    used.push(true);
                    used.len() - 1
                }
            }
        });
        let digest = blake3::hash(format!("{}:{}:{slot}", key.0, key.1).as_bytes());
        let root = if Path::new("/dev/shm").is_dir() {
            std::path::PathBuf::from("/dev/shm")
        } else {
            std::env::temp_dir()
        };
        let path = root.join(format!(
            "acyclic-dst-{}-{}-{}",
            std::process::id(),
            process_nonce(),
            &digest.to_hex()[..16]
        ));
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
        }
        std::fs::create_dir(&path)?;
        Ok(Self { path, key, slot })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// The checkout identity is a function of the world's case and seed, not
    /// of the scheduler incarnation, so every incarnation reopens the same
    /// state.
    fn identity(&self) -> ([u8; 16], acyclic_fs::NativeRootGeneration) {
        let mut object = [0; 16];
        object.copy_from_slice(
            &blake3::hash(format!("dst-object:{}:{}", self.key.0, self.key.1).as_bytes())
                .as_bytes()[..16],
        );
        let handle = *blake3::hash(format!("dst-handle:{}:{}", self.key.0, self.key.1).as_bytes())
            .as_bytes();
        (object, acyclic_fs::NativeRootGeneration::FileHandle(handle))
    }
}

impl Drop for WorldDirectory {
    fn drop(&mut self) {
        WORLD_SLOTS.with(|slots| {
            if let Some(used) = slots.borrow_mut().get_mut(&self.key) {
                if let Some(used) = used.get_mut(self.slot) {
                    *used = false;
                }
            }
        });
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// A seeded world on disk. Incarnations share only its files and the model
/// service, never a host object.
pub(super) struct World {
    directory: WorldDirectory,
    pub service: ScriptedModelProvider,
    pub seed: u64,
    incarnations: u64,
    entropy: bool,
}

impl World {
    pub fn new(seed: u64, files: &[(&str, &str)], service: ScriptedModelProvider) -> Result<Self> {
        Self::with_entropy(seed, files, service, true)
    }

    /// The same world with the scheduler's entropy source erased, so a trace
    /// that still matches the seeded one proves the seed never reached it.
    pub fn without_entropy(
        seed: u64,
        files: &[(&str, &str)],
        service: ScriptedModelProvider,
    ) -> Result<Self> {
        Self::with_entropy(seed, files, service, false)
    }

    fn with_entropy(
        seed: u64,
        files: &[(&str, &str)],
        service: ScriptedModelProvider,
        entropy: bool,
    ) -> Result<Self> {
        let directory = WorldDirectory::new(seed).map_err(storage)?;
        let checkout = directory.path().join("checkout");
        std::fs::create_dir(&checkout).map_err(storage)?;
        for (path, body) in files {
            let path = checkout.join(path);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(storage)?;
            }
            std::fs::write(path, body).map_err(storage)?;
        }
        Ok(Self {
            directory,
            service,
            seed,
            incarnations: 0,
            entropy,
        })
    }

    /// Opens a host on the world's files and sends each group of prompts,
    /// each as message `id`, together; a group waits for all of its answers
    /// before the next is sent. With a cut, the incarnation crashes at that
    /// durable operation and every later prompt is lost with it.
    pub fn incarnation<'a>(
        &'a mut self,
        cut: Option<(usize, DurableFault)>,
        prompts: &'a [Vec<(&'a str, String)>],
    ) -> EntryFuture<'a, Result<Incarnation>> {
        entry(async move {
            // Each incarnation is an independent scheduler over the same files.
            let scheduler = self.seed ^ self.incarnations.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            self.incarnations += 1;
            let mut driver = if self.entropy {
                Driver::world(scheduler, cut)
            } else {
                Driver::without_entropy(scheduler, cut)
            };
            // Native timestamps are fixture inputs, not host facts: a world
            // replayed from the same seed must not depend on when it ran.
            driver.schedule.set_native_times_provider(|input| {
                const TIME_NS: i64 = 1_700_000_000_000_000_000;
                acyclic_native_runtime::testing::NativeTimes {
                    created_ns: input.times.created_ns.map(|_| TIME_NS),
                    modified_ns: input.times.modified_ns.map(|_| TIME_NS),
                    accessed_ns: input.times.accessed_ns.map(|_| TIME_NS),
                    changed_ns: input.times.changed_ns.map(|_| TIME_NS),
                }
            });
            let schedule = driver.schedule.clone();
            let counter = schedule.clone();
            let source_entropy = {
                let schedule = schedule.clone();
                Arc::new(move || {
                    let mut bytes = [0; 16];
                    schedule.fill_entropy(&mut bytes);
                    bytes
                })
            };
            // The checkout's root identity is a host file handle. A replay
            // supplies its own fixture identity so the owner record and every
            // later capture are a function of the world's case and seed alone,
            // and every incarnation reopens the same state.
            let checkout_identity = self.directory.identity();
            let dispatched_at = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::clone(&dispatched_at);
            let provider = Arc::new(self.service.reopened().with_dispatch_observer(move |_| {
                observed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(schedule.durable_write_count());
                Box::pin(async { Ok(()) })
            }));
            let results = Arc::new(Mutex::new(Vec::new()));
            let directory = self.directory.path();
            let run = {
                let provider = Arc::clone(&provider);
                let results = Arc::clone(&results);
                entry(async move {
                    let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
                    acyclic_harness::test_support::inject_checkout_identity(
                        checkout_identity.0,
                        checkout_identity.1,
                    );
                    let config = local_harness_config(
                        directory.join("checkout"),
                        (model, provider.clone()),
                    )?
                    .without_user_settings()
                    .workspace_mounting(WorkspaceMounting::Physical)
                    .source_entropy(source_entropy)
                    .state_directory(directory.join("state"));
                    let harness = Arc::new(entry(LocalHarness::open(config)).await?);
                    for group in prompts {
                        let outcomes =
                            futures::future::try_join_all(group.iter().map(|(id, prompt)| {
                                let harness = &harness;
                                entry(async move {
                                    Ok::<_, Error>(
                                        match entry(prompt_settled(harness, id, prompt))
                                            .await
                                            .map_err(|error| {
                                                Error::Invalid(format!("prompt {id}: {error}"))
                                            })? {
                                            PromptOutcome::Settled(_, output) => {
                                                Prompted::Settled(output.text)
                                            }
                                            PromptOutcome::Failed(message) => {
                                                Prompted::Failed(message)
                                            }
                                        },
                                    )
                                })
                            }))
                            .await?;
                        let requests = provider.request_count();
                        results
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .extend(outcomes.into_iter().map(|outcome| (outcome, requests)));
                    }
                    // Retire every native mount and worker before the next
                    // replay can reuse this world's deterministic path.
                    let harness = Arc::try_unwrap(harness).map_err(|_| {
                        Error::Invalid("world harness still has live connections".into())
                    })?;
                    harness.disconnect().await?;
                    acyclic_harness::test_support::clear_checkout_identity();
                    Ok::<_, Error>(())
                })
            };
            let crashed = match driver.run(run).await {
                Outcome::Completed(result) => {
                    result?;
                    None
                }
                Outcome::Crashed(cause) => Some(cause),
            };
            let prompts = std::mem::take(
                &mut *results
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            let dispatched_at = std::mem::take(
                &mut *dispatched_at
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            Ok(Incarnation {
                prompts,
                requests: provider.requests(),
                dispatched_at,
                durable: counter.durable_write_count(),
                crashed,
            })
        })
    }

    /// The root agent's current committed `findings/` files, read through the
    /// same live directory and range queries a client uses. A reopen sees only
    /// committed versions, so this is the publication fold after any crash.
    /// Every page after the first continues at the first page's generation,
    /// `items` entries at a time, and each file is read range by range to its
    /// whole length.
    pub fn findings(&self, items: u32) -> EntryFuture<'_, Result<Vec<(String, Vec<u8>)>>> {
        entry(async move {
            let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
            let provider = Arc::new(self.service.reopened());
            let config =
                local_harness_config(self.directory.path().join("checkout"), (model, provider))?
                    .without_user_settings()
                    .workspace_mounting(WorkspaceMounting::Physical)
                    .state_directory(self.directory.path().join("state"));
            let harness = Arc::new(entry(LocalHarness::open(config)).await?);
            let (_, root) = harness.agents().await?;
            let connection = harness.connect().await?;
            let mut findings = Vec::new();
            let mut cursor = None;
            let mut generation = None;
            loop {
                let request = AgentCommand::Directory {
                    agent: root.clone(),
                    target: DirectoryTarget::Primary {},
                    path: "findings".into(),
                    generation: generation.clone(),
                    after_generation: None,
                    page: AgentPageRequest::new(items, u32::MAX, cursor)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                };
                let AgentReply::Directory { page } = connection.execute(&request).await? else {
                    return Err(Error::Invalid("findings directory reply".into()));
                };
                for entry in &page.entries {
                    if entry.kind != PrivateDirectoryEntryKind::File {
                        continue;
                    }
                    let path = format!("findings/{}", entry.name);
                    let mut body = Vec::new();
                    loop {
                        let read = AgentCommand::Read {
                            agent: root.clone(),
                            offset: body.len() as u64,
                            target: ReadTarget::Path {
                                target: DirectoryTarget::Primary {},
                                path: path.clone(),
                                generation: page.generation.clone(),
                            },
                        };
                        let reply = connection.execute(&read).await?;
                        let (bytes, length) = reply
                            .content()
                            .map_err(|error| Error::Invalid(error.to_string()))?;
                        body.extend_from_slice(&bytes);
                        if body.len() as u64 >= length {
                            break;
                        }
                    }
                    findings.push((entry.name.clone(), body));
                }
                match page.next {
                    Some(next) => {
                        cursor = Some(next);
                        generation = Some(page.generation);
                    }
                    None => break,
                }
            }
            findings.sort();
            Ok(findings)
        })
    }
}

/// A crash at a seed-chosen durable cut inside a window between two
/// dispatches of an uncut twin, then an independent reopen.
pub(super) struct Crash<'a, P> {
    pub seed: u64,
    pub files: &'a [(&'a str, &'a str)],
    /// The model plans; each world gets a fresh service from them.
    pub plans: P,
    pub prompts: Vec<Vec<(&'a str, String)>>,
    /// Indexes of the dispatches opening and closing the window; a closing
    /// index past the last dispatch closes it at the end of the run.
    pub window: (usize, usize),
    pub mode: DurableFault,
}

/// Where a [`Crash`] landed.
pub(super) struct Crashed {
    pub cause: InjectedStorageFault,
    /// Whether the cut fell inside the selected window.
    pub in_window: bool,
}

impl std::fmt::Display for Crashed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} (in window: {})", self.cause, self.in_window)
    }
}

impl<'a, P: Fn() -> Vec<Plan>> Crash<'a, P> {
    /// Crashes the world, checks every request it sent, then reopens it and
    /// sends `recovery`.
    pub fn recover(
        self,
        recovery: Vec<Vec<(&'a str, String)>>,
    ) -> EntryFuture<'a, std::result::Result<(Crashed, Incarnation), String>>
    where
        P: 'a,
    {
        entry(async move {
            let mut twin =
                World::new(self.seed, self.files, service((self.plans)())).map_err(failed)?;
            let baseline = twin
                .incarnation(None, &self.prompts)
                .await
                .map_err(failed)?;
            let ends = |at: &[usize], end: usize| {
                (
                    at.get(self.window.0).copied(),
                    at.get(self.window.1).copied().unwrap_or(end),
                )
            };
            let (Some(open), close) = ends(&baseline.dispatched_at, baseline.durable) else {
                return Err(format!("the window never opened: {:?}", baseline.prompts));
            };
            let width = close.saturating_sub(open).max(1);
            // Default seeds cut mid-window, far from either edge; deeper
            // seeds walk the cut across the whole window.
            let ordinal = open + (width / 2 + usize::try_from(self.seed / 7).unwrap_or(0)) % width;
            let mut world =
                World::new(self.seed, self.files, service((self.plans)())).map_err(failed)?;
            let crashed = world
                .incarnation(Some((ordinal, self.mode)), &self.prompts)
                .await
                .map_err(failed)?;
            let cause = crashed
                .crashed
                .clone()
                .ok_or_else(|| format!("cut {ordinal} {:?} never crashed the host", self.mode))?;
            all_settled(&crashed.requests)?;
            let in_window = match ends(&crashed.dispatched_at, usize::MAX) {
                (Some(open), close) => open <= cause.ordinal && cause.ordinal < close,
                (None, _) => false,
            };
            let recovered = world.incarnation(None, &recovery).await.map_err(failed)?;
            Ok((Crashed { cause, in_window }, recovered))
        })
    }
}

/// How many times `edit` appended `+` to the word `base` in a rendered read.
pub(super) fn edits(rendered: &str) -> Option<usize> {
    let start = rendered.find("base")? + "base".len();
    Some(
        rendered
            .chars()
            .skip(start)
            .take_while(|character| *character == '+')
            .count(),
    )
}

/// The durable fault modes a crash variant selects, by name.
pub(super) fn crash_mode(name: &str, seed: u64) -> Option<DurableFault> {
    match name {
        "crash_before" => Some(DurableFault::Before),
        "crash_after" => Some(DurableFault::After),
        "crash_torn" => Some(DurableFault::Torn(usize::try_from(seed % 61).unwrap_or(0))),
        _ => None,
    }
}

/// Runs `body` on a seeded simulated runtime, as every generator world does.
pub(super) fn simulate<T>(
    seed: u64,
    body: impl FnOnce() -> EntryFuture<'static, std::result::Result<T, String>>,
) -> std::result::Result<T, String> {
    let runtime =
        acyclic_native_runtime::testing::seeded_runtime(seed).map_err(|error| error.to_string())?;
    runtime.block_on(async {
        tokio::time::pause();
        body().await
    })
}

pub(super) fn failed(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[test]
fn world_directories_name_their_process() {
    let directory = WorldDirectory::new(0).expect("world directory");
    let name = directory
        .path()
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .expect("world directory name");
    let process = format!("-{}-", std::process::id());
    assert!(
        name.contains(&process),
        "world directory {name} does not name process {process}"
    );
}
