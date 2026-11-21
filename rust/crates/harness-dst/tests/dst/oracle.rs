//! The shared settlement and fault oracle, and the scripted world every class
//! generator drives: the shipping local composition, a deterministic model
//! service that answers from the visible history, and a crash at any durable
//! cut followed by an independent reopen.
use super::*;
use acyclic_harness::{
    conversation::{DirectoryEntryKind, DirectoryTarget},
    model::{ModelContentPart, ModelEvent, ModelRequest, ModelRole, ToolCall, ToolResultContent},
    test_support::{ScriptedEvent, model_visible_messages},
};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply, ReadTarget};
use acyclic_native_runtime::testing::{DurableFault, InjectedStorageFault, Schedule};
use driver::{Driver, Outcome};
use std::{collections::BTreeMap, sync::Mutex};

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

/// A `spawn` call that briefs `name` with `tag(brief)`.
pub(super) fn spawn(id: &str, name: &str, brief: &'static str) -> ToolCall {
    call(
        id,
        "spawn",
        serde_json::json!({"name": name, "task": tag(brief), "role": "worker"}),
    )
}

/// A plan that settles as soon as its tag is prompted.
pub(super) fn settled(tag_name: &'static str) -> Plan {
    Plan {
        tag: tag_name,
        steps: Vec::new(),
        fail_once: None,
    }
}

/// Every `wait` result the service returned, in request order.
pub(super) fn wait_results(
    requests: &[ModelRequest],
) -> std::result::Result<Vec<(String, serde_json::Value)>, String> {
    let mut results = Vec::new();
    for request in requests {
        for settled in settlements(request)? {
            if settled.call.name != "wait" {
                continue;
            }
            let ToolResultContent::Json { value } = settled.result else {
                return Err(format!("wait returned {} not JSON", settled.rendered()));
            };
            results.push((settled.call.call_id.clone(), value));
        }
    }
    Ok(results)
}

/// A deterministic model service: each answer is a function of the request's
/// visible history alone, so every incarnation of a host sees the same model.
pub(super) fn service(plans: Vec<Plan>) -> ScriptedModelProvider {
    let failed = Mutex::new(Vec::<&'static str>::new());
    let needles: Vec<_> = plans.iter().map(|plan| appears(&tag(plan.tag))).collect();
    let answer = move |request: &ModelRequest| {
        let messages: Vec<_> = model_visible_messages(&request.messages).collect();
        // A plan is selected by the newest user brief, never by a tag that a
        // tool result or a child's answer happens to quote.
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
        // The executor may record each tool call as its own assistant
        // message, so a step is done once all of its calls appear.
        let mut made = messages
            .iter()
            .skip(index)
            .flat_map(|message| message.content.parts())
            .filter(|part| matches!(part, ModelContentPart::ToolCall(_)))
            .count();
        let mut failed = failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut step = 0;
        while let Some(calls) = plan.steps.get(step) {
            if calls.is_empty() {
                // An empty step is one model answer with no calls. A one-shot
                // failure on it must be emitted before the plan advances.
                if plan.fail_once.as_ref().is_some_and(|(at, _)| {
                    *at == usize::MAX || (*at == step && !failed.contains(&plan.tag))
                }) {
                    break;
                }
                step += 1;
                continue;
            }
            if made < calls.len() {
                break;
            }
            made -= calls.len();
            step += 1;
        }
        let Some(calls) = plan.steps.get(step) else {
            return ScriptedTurn::Text(done(plan.tag));
        };
        let mut events: Vec<_> = calls
            .iter()
            .map(|call| ScriptedEvent::Event(ModelEvent::ToolCall(call.clone())))
            .collect();
        if let Some((at, failure)) = plan
            .fail_once
            .as_ref()
            .filter(|(at, _)| *at == usize::MAX || (*at == step && !failed.contains(&plan.tag)))
        {
            if *at != usize::MAX {
                failed.push(plan.tag);
            }
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
    pub durable_trace: Vec<acyclic_native_runtime::testing::DurableWrite>,
    pub crashed: Option<InjectedStorageFault>,
}

/// A world directory whose absolute path is a function of its case, seed and
/// slot alone, never of its process or of the host's random temporary name.
/// Model requests and workspace announcements carry that path, so any other
/// name would make an otherwise identical world differ between processes.
/// The slot is the first whose lock file this world takes, one non-blocking
/// try per slot in order; the world holds it until it drops or its process
/// dies. Two live worlds never share a directory, in one process or two, and
/// a world run after another of its case and seed takes the same path.
pub(super) struct WorldDirectory {
    path: std::path::PathBuf,
    // Held for the world's lifetime: closing it frees the slot.
    _slot: std::fs::File,
}

impl WorldDirectory {
    pub(super) fn new(seed: u64) -> std::io::Result<Self> {
        Self::named(std::thread::current().name().unwrap_or("dst"), seed)
    }

    fn named(case: &str, seed: u64) -> std::io::Result<Self> {
        let root = if Path::new("/dev/shm").is_dir() {
            std::path::PathBuf::from("/dev/shm")
        } else {
            std::env::temp_dir()
        };
        let mut slot = 0_u64;
        loop {
            let digest = blake3::hash(format!("{case}:{seed}:{slot}").as_bytes());
            let path = root.join(format!("acyclic-dst-{:.16}", digest.to_hex().as_str()));
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path.with_extension("lock"))?;
            match lock.try_lock() {
                Ok(()) => {
                    // A holder that died left its directory; none of it
                    // reaches this world.
                    match std::fs::remove_dir_all(&path) {
                        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                            return Err(error);
                        }
                        _ => {}
                    }
                    std::fs::create_dir(&path)?;
                    return Ok(Self { path, _slot: lock });
                }
                Err(std::fs::TryLockError::WouldBlock) => slot += 1,
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for WorldDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

type SourceIdentities = Arc<Mutex<BTreeMap<[u8; 32], [u8; 32]>>>;
type Findings = Vec<(String, Vec<u8>)>;

#[derive(Clone, Copy, Debug)]
enum IncarnationCut {
    Ordinal(usize, DurableFault),
    Dispatch(usize, DurableFault),
}

/// A seeded world on disk. Incarnations reopen its files with the same model
/// service and native inputs, through a freshly opened harness.
pub(super) struct World {
    directory: WorldDirectory,
    pub service: ScriptedModelProvider,
    pub seed: u64,
    incarnations: u64,
    entropy: bool,
    complete: bool,
    trace_durability: bool,
    source_identities: SourceIdentities,
}

impl World {
    pub fn new(seed: u64, files: &[(&str, &str)], service: ScriptedModelProvider) -> Result<Self> {
        Self::with_entropy(seed, seed, files, service, true)
    }

    /// A world whose scheduler seed and directory key differ, so two seeds
    /// that map to the same class still own separate directories.
    pub fn new_keyed(
        seed: u64,
        directory_seed: u64,
        files: &[(&str, &str)],
        service: ScriptedModelProvider,
    ) -> Result<Self> {
        Self::with_entropy(seed, directory_seed, files, service, true)
    }

    /// A world whose scheduler has no entropy source and whose directory has a separate key.
    pub fn without_entropy_keyed(
        seed: u64,
        directory_seed: u64,
        files: &[(&str, &str)],
        service: ScriptedModelProvider,
    ) -> Result<Self> {
        Self::with_entropy(seed, directory_seed, files, service, false)
    }

    fn with_entropy(
        seed: u64,
        directory_seed: u64,
        files: &[(&str, &str)],
        service: ScriptedModelProvider,
        entropy: bool,
    ) -> Result<Self> {
        let directory = WorldDirectory::new(directory_seed).map_err(storage)?;
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
            complete: false,
            trace_durability: false,
            source_identities: Arc::default(),
        })
    }

    pub fn trace_durability(mut self) -> Self {
        self.trace_durability = true;
        self
    }

    fn scheduler_seed(&self) -> u64 {
        self.seed ^ self.incarnations.wrapping_mul(0x9e37_79b9_7f4a_7c15)
    }

    fn native_inputs(&self, schedule: &Schedule) {
        // Times and logical source keys are replay inputs. Native snapshot
        // checks still use the original physical file and timestamps.
        schedule.set_native_times_provider(|input| {
            const TIME_NS: i64 = 1_700_000_000_000_000_000;
            acyclic_native_runtime::testing::NativeTimes {
                created_ns: input.times.created_ns.map(|_| TIME_NS),
                modified_ns: input.times.modified_ns.map(|_| TIME_NS),
                accessed_ns: input.times.accessed_ns.map(|_| TIME_NS),
                changed_ns: input.times.changed_ns.map(|_| TIME_NS),
            }
        });
        // Share the mapping across independent opens, including reads after
        // a crash. All aliases of a physical identity get one logical key.
        let identities = Arc::clone(&self.source_identities);
        schedule.set_source_identity_provider(move |original| {
            let mut identities = identities
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let next = identities.len() as u128;
            *identities.entry(original).or_insert_with(|| {
                let mut logical = [0; 32];
                logical[..16].copy_from_slice(&next.to_le_bytes());
                logical
            })
        });
    }

    /// Completes each incarnation's runtime before it returns, so the next
    /// incarnation opens a cleanly closed host on the same state.
    #[must_use]
    pub fn completing(mut self) -> Self {
        self.complete = true;
        self
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
        self.incarnation_with_cut(
            cut.map(|(ordinal, fault)| IncarnationCut::Ordinal(ordinal, fault)),
            prompts,
        )
    }

    fn incarnation_with_cut<'a>(
        &'a mut self,
        cut: Option<IncarnationCut>,
        prompts: &'a [Vec<(&'a str, String)>],
    ) -> EntryFuture<'a, Result<Incarnation>> {
        entry(async move {
            // Each incarnation is an independent scheduler over the same files.
            let scheduler = self.scheduler_seed();
            self.incarnations += 1;
            let (ordinal_cut, dispatch_cut) = match cut {
                Some(IncarnationCut::Ordinal(ordinal, fault)) => (Some((ordinal, fault)), None),
                Some(IncarnationCut::Dispatch(index, fault)) => (None, Some((index, fault))),
                None => (None, None),
            };
            let mut driver = match (self.entropy, self.trace_durability) {
                (true, true) => Driver::new(scheduler, ordinal_cut),
                (true, false) => Driver::world(scheduler, ordinal_cut),
                (false, _) => Driver::without_entropy(scheduler, ordinal_cut),
            };
            self.native_inputs(&driver.schedule);
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
            let dispatched_at = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::clone(&dispatched_at);
            let provider = Arc::new(self.service.reopened().with_dispatch_observer(move |_| {
                let mut observed = observed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let ordinal = match dispatch_cut {
                    Some((index, fault)) if index == observed.len() => schedule.cut_next(fault),
                    _ => schedule.durable_write_count(),
                };
                observed.push(ordinal);
                Box::pin(async { Ok(()) })
            }));
            let results = Arc::new(Mutex::new(Vec::new()));
            let directory = self.directory.path();
            let complete = self.complete;
            let run = {
                let provider = Arc::clone(&provider);
                let results = Arc::clone(&results);
                entry(async move {
                    let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
                    let config = local_harness_config(
                        directory.join("checkout"),
                        (model, provider.clone()),
                    )?
                    .without_user_settings()
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
                    if complete {
                        let (agents, _) = harness.agents().await?;
                        entry(agents.runtime().complete()).await.map_err(storage)?;
                    }
                    // Retire every native mount and worker before the next
                    // replay can reuse this world's deterministic path.
                    let harness = Arc::try_unwrap(harness).map_err(|_| {
                        Error::Invalid("world harness still has live connections".into())
                    })?;
                    harness.disconnect().await?;
                    Ok::<_, Error>(())
                })
            };
            let crashed = match driver.run(run).await? {
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
                durable_trace: if self.trace_durability {
                    counter.durable_writes()
                } else {
                    Vec::new()
                },
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
    pub fn findings(&self, items: u32) -> EntryFuture<'_, Result<Findings>> {
        entry(async move {
            let mut driver = Driver::world(self.scheduler_seed(), None);
            self.native_inputs(&driver.schedule);
            match driver.run(self.read_findings(items)).await? {
                Outcome::Completed(result) => result,
                Outcome::Crashed(cause) => Err(storage(cause)),
            }
        })
    }

    fn read_findings(&self, items: u32) -> EntryFuture<'_, Result<Findings>> {
        entry(async move {
            let model = Model::new("scripted".into(), "dst".into(), "1".into())?;
            let provider = Arc::new(self.service.reopened());
            let config =
                local_harness_config(self.directory.path().join("checkout"), (model, provider))?
                    .without_user_settings()
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
                    if entry.kind != DirectoryEntryKind::File {
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
            drop(connection);
            Arc::try_unwrap(harness)
                .map_err(|_| Error::Invalid("world harness still has live connections".into()))?
                .disconnect()
                .await?;
            Ok(findings)
        })
    }
}

pub(super) enum CrashAt {
    /// The first original durable operation following this observed dispatch.
    Dispatch(usize),
    /// A seed-chosen operation between dispatches of an uncut twin. A closing
    /// index past the last dispatch uses the end of the turn.
    Window(usize, usize),
}

/// A crash at an original durable operation, then an independent reopen.
pub(super) struct Crash<'a, P> {
    pub seed: u64,
    pub files: &'a [(&'a str, &'a str)],
    /// The model plans; each world gets a fresh service from them.
    pub plans: P,
    pub prompts: Vec<Vec<(&'a str, String)>>,
    pub at: CrashAt,
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
            let cut = match self.at {
                CrashAt::Dispatch(index) => IncarnationCut::Dispatch(index, self.mode),
                CrashAt::Window(open, close) => {
                    let mut twin = World::new(self.seed, self.files, service((self.plans)()))
                        .map_err(failed)?;
                    let baseline = twin
                        .incarnation(None, &self.prompts)
                        .await
                        .map_err(failed)?;
                    let Some(open) = baseline.dispatched_at.get(open).copied() else {
                        return Err(format!("the window never opened: {:?}", baseline.prompts));
                    };
                    let close = baseline
                        .dispatched_at
                        .get(close)
                        .copied()
                        .unwrap_or(baseline.durable);
                    let width = close.saturating_sub(open).max(1);
                    // Default seeds cut mid-window; deeper seeds walk the window.
                    let ordinal =
                        open + (width / 2 + usize::try_from(self.seed / 7).unwrap_or(0)) % width;
                    IncarnationCut::Ordinal(ordinal, self.mode)
                }
            };
            let mut world =
                World::new(self.seed, self.files, service((self.plans)())).map_err(failed)?;
            let crashed = world
                .incarnation_with_cut(Some(cut), &self.prompts)
                .await
                .map_err(failed)?;
            let cause = crashed
                .crashed
                .ok_or_else(|| format!("cut {cut:?} never crashed the host"))?;
            all_settled(&crashed.requests)?;
            let in_window = match self.at {
                CrashAt::Dispatch(index) => {
                    crashed.dispatched_at.get(index) == Some(&cause.ordinal)
                }
                CrashAt::Window(open, close) => {
                    let close = crashed
                        .dispatched_at
                        .get(close)
                        .copied()
                        .unwrap_or(usize::MAX);
                    crashed
                        .dispatched_at
                        .get(open)
                        .is_some_and(|open| *open <= cause.ordinal && cause.ordinal < close)
                }
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

/// The case a [`world_directory_holder`] process holds, when set.
const HOLD_CASE: &str = "ACYCLIC_DST_HOLD_CASE";

/// Holds a world of the case `HOLD_CASE` names, with a file left in it, for
/// another process: it reports the path on stdout and holds the world until
/// its stdin closes. Without `HOLD_CASE` it holds nothing.
#[test]
fn world_directory_holder() {
    use std::io::{Read, Write};
    let Ok(case) = std::env::var(HOLD_CASE) else {
        return;
    };
    let world = WorldDirectory::named(&case, 0).expect("held world directory");
    std::fs::write(world.path().join("left"), b"held").expect("held file");
    let mut stdout = std::io::stdout();
    writeln!(stdout, "HELD {}", world.path().display()).expect("report held path");
    stdout.flush().expect("report held path");
    std::io::stdin()
        .read_to_end(&mut Vec::new())
        .expect("hold until released");
}

/// A [`world_directory_holder`] process holding a world of `case`.
struct Holder {
    process: std::process::Child,
    /// The rest of its output, read to its end once it is released.
    output: std::io::BufReader<std::process::ChildStdout>,
    /// The path it holds.
    held: std::path::PathBuf,
}

/// Starts a [`Holder`] of `case`, returned once it has reported its world.
fn hold_in_another_process(case: &str) -> std::result::Result<Holder, String> {
    use std::io::BufRead;
    let name = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, test)| test);
    let mut holder = std::process::Command::new(std::env::current_exe().map_err(failed)?)
        .args([
            &format!("{name}::world_directory_holder"),
            "--exact",
            "--nocapture",
        ])
        .env(HOLD_CASE, case)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(failed)?;
    let stdout = holder.stdout.take().ok_or("holder has no stdout")?;
    let mut output = std::io::BufReader::new(stdout);
    let mut line = String::new();
    let held = loop {
        line.clear();
        if output.read_line(&mut line).map_err(failed)? == 0 {
            return Err("holder exited without reporting its world".into());
        }
        if let Some(held) = line.trim_end().strip_prefix("HELD ") {
            break std::path::PathBuf::from(held);
        }
    };
    Ok(Holder {
        process: holder,
        output,
        held,
    })
}

/// Two live worlds of one case and seed never share a directory, whether they
/// live in one process or in two.
#[test]
fn live_world_directories_never_share_a_directory() {
    let case = "live-world-directories";
    let first = WorldDirectory::named(case, 0).expect("first world directory");
    let second = WorldDirectory::named(case, 0).expect("second world directory");
    assert_ne!(first.path(), second.path());
    let mut holder = hold_in_another_process("held-world-directories").expect("holder");
    let mine = WorldDirectory::named("held-world-directories", 0).expect("world directory");
    assert_ne!(mine.path(), holder.held);
    assert!(
        holder.held.join("left").exists(),
        "the holder's world was disturbed"
    );
    drop(holder.process.stdin.take());
    std::io::Read::read_to_end(&mut holder.output, &mut Vec::new()).expect("holder output");
    assert!(holder.process.wait().expect("holder exit").success());
}

/// A slot whose holder died starts empty: what the dead world left is removed
/// before the next world of its case and seed uses the path.
#[test]
fn world_directories_drop_what_a_dead_holder_left() {
    let case = "dead-world-directories";
    let mut holder = hold_in_another_process(case).expect("holder");
    holder.process.kill().expect("kill holder");
    holder.process.wait().expect("holder exit");
    let held = holder.held;
    let world = WorldDirectory::named(case, 0).expect("world directory");
    assert_eq!(world.path(), held);
    assert_eq!(
        std::fs::read_dir(world.path())
            .expect("world directory")
            .count(),
        0,
        "a dead holder's files reached the next world"
    );
}
