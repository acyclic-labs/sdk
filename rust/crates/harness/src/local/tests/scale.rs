//! Many concurrent agents in one process: per-agent cost and latency.
//!
//! Each root agent is a turn on its own local harness whose stateless
//! provider writes, reads and rewrites one file through the stock volume tools,
//! optionally spawns and waits for children, and then answers. The CI-sized
//! case asserts per-agent ceilings; `scripts/harness-scale.sh` ramps N.
use super::*;
use crate::{
    context::ModelTokenCount,
    model::{
        ModelContent, ModelContentPart, ModelDispatch, ModelRole, PreparedModelRequest,
        ToolResultContent,
    },
};
use futures::TryStreamExt as _;
use std::{
    collections::HashMap,
    sync::PoisonError,
    time::{Duration, Instant},
};

/// Stateless scripted agent: the step is the number of tool results so far.
#[derive(Clone)]
struct ScaleProvider {
    /// Children each root spawns and waits for after its own file work.
    children: usize,
    started: Instant,
    timings: Arc<Mutex<Timings>>,
    /// The executing task's reader: a child's task arrives as a file part.
    content: Option<(Arc<dyn crate::conversation::ContentResidencyVerifier>, u64)>,
}

#[derive(Default)]
struct Timings {
    /// Agent index -> instant of its previous model dispatch.
    last: HashMap<String, Duration>,
    first: Vec<Duration>,
    turns: Vec<Duration>,
}

fn user_text(request: &PreparedModelRequest) -> Option<&str> {
    user_contents(request)
        .find_map(|content| match content {
            ModelContent::Text(text) | ModelContent::Part(ModelContentPart::Text { text }) => {
                Some(text.as_str())
            }
            ModelContent::Parts(parts) => parts.iter().find_map(|part| match part {
                ModelContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            }),
            _ => None,
        })
        .filter(|text| text.starts_with("scale agent "))
}

fn user_contents(request: &PreparedModelRequest) -> impl Iterator<Item = &ModelContent> {
    request
        .request()
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::User)
        .map(|message| &message.content)
}

fn find_file(value: &Value) -> Option<Value> {
    match value {
        Value::Object(map) => map
            .get("file")
            .filter(|file| file.is_object())
            .cloned()
            .or_else(|| map.values().find_map(find_file)),
        Value::Array(items) => items.iter().find_map(find_file),
        _ => None,
    }
}

impl ScaleProvider {
    async fn prompt(&self, request: &PreparedModelRequest) -> Result<String> {
        if let Some(text) = user_text(request) {
            return Ok(text.to_owned());
        }
        let (reader, maximum) = self
            .content
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("no bound reader".into()))?;
        for content in user_contents(request) {
            for (file, _) in content.file_parts() {
                let bytes = reader.read(file, *maximum, None, None)?.await?;
                let text = String::from_utf8_lossy(&bytes);
                if let Some((_, prompt)) = text.split_once("scale agent ") {
                    let agent = prompt
                        .split(|c: char| !c.is_ascii_alphanumeric())
                        .next()
                        .unwrap_or_default();
                    return Ok(format!("scale agent {agent}"));
                }
            }
        }
        Err(Error::Invalid("no agent prompt".into()))
    }

    fn reply(&self, request: &PreparedModelRequest, prompt: &str) -> Result<Vec<ModelEvent>> {
        let agent = prompt
            .strip_prefix("scale agent ")
            .ok_or_else(|| Error::Invalid(format!("not a scale prompt: {prompt}")))?
            .to_owned();
        let now = self.started.elapsed();
        {
            let mut timings = self.timings.lock().unwrap_or_else(PoisonError::into_inner);
            match timings.last.insert(agent.clone(), now) {
                Some(previous) => timings.turns.push(now - previous),
                None => timings.first.push(now),
            }
        }
        let results = request
            .request()
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .filter_map(|part| match part {
                ModelContentPart::ToolResult { content, .. } => Some(content),
                _ => None,
            })
            .collect::<Vec<_>>();
        // A failed tool call ends the agent: the workload must really run.
        if let Some(ToolResultContent::Error { message }) = results.last() {
            return Err(Error::Invalid(format!(
                "scale agent {agent} step {} tool failed: {message}",
                results.len()
            )));
        }
        let file = results
            .last()
            .and_then(|content| find_file(&serde_json::to_value(content).ok()?));
        let call = |name: &str, arguments: Value| {
            vec![
                ModelEvent::ToolCall(crate::model::ToolCall::new(
                    format!("{name}-{}", results.len()),
                    name.into(),
                    arguments,
                )),
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]
        };
        let file = || {
            file.clone()
                .ok_or_else(|| Error::Invalid(format!("no file ref: {results:?}")))
        };
        let done = || {
            vec![
                ModelEvent::Content {
                    delta: format!("done {agent}"),
                },
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]
        };
        // A child runs one checkout command where native execution is built,
        // which gives it a native task view that retires when its turn
        // settles, then answers. It writes
        // nothing: concurrent sibling writes into the parent's workspace
        // currently fail admission with a generation conflict (see #1030).
        if agent.contains('c') {
            return Ok(
                if results.is_empty() && cfg!(feature = "native-execution") {
                    call(
                        crate::tools::shell::PLATFORM.name,
                        json!({"command": "echo child"}),
                    )
                } else {
                    done()
                },
            );
        }
        // A root writes, reads and rewrites its file, then spawns its children
        // one per step and waits for all of them. The rewrite goes by path:
        // `edit` pins the written file's generation, and the checkout capture
        // that precedes it commits the write's own materialization first, so
        // an edit of a just-written checkout file fails with a generation
        // conflict on every run (Q:/lanes/scale-10e5.md item 9).
        let children = self.children;
        let write = |text: &str| {
            call(
                "write",
                json!({"path": format!("agents/{agent}.txt"), "text": format!("{text} {agent}\n"),
                    "media_type": "text/plain", "display_name": "notes.txt"}),
            )
        };
        Ok(match results.len() {
            0 => write("alpha"),
            1 => call("read", json!({"file": file()?})),
            2 => write("beta"),
            step if step < 3 + children => {
                let child = step - 3;
                call(
                    "spawn",
                    json!({"task": format!("scale agent {agent}c{child}"), "name": format!("c{child}")}),
                )
            }
            step if children > 0 && step == 3 + children => call(
                "wait",
                json!({"agents": (0..children).map(|child| format!("c{child}")).collect::<Vec<_>>()}),
            ),
            _ => done(),
        })
    }
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

impl ModelProvider for ScaleProvider {
    fn bind_content(
        &self,
        reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
        maximum_bytes: u64,
    ) -> Option<Arc<dyn ModelProvider>> {
        Some(Arc::new(Self {
            content: Some((reader, maximum_bytes)),
            ..self.clone()
        }))
    }
    fn context_capacity(&self, _: &Model) -> Result<ModelContextCapacity> {
        Ok(capacity())
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move { Ok(request.serialized_token_upper_bound(None)?.0) })
    }
    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        _: ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        Box::pin(
            futures::stream::once(async move {
                let prompt = self.prompt(request).await?;
                let events = self.reply(request, &prompt)?;
                Ok::<_, Error>(futures::stream::iter(events.into_iter().map(Ok)))
            })
            .try_flatten(),
        )
    }
}

/// Process-wide OS resources, read from the OS on every platform.
#[derive(Clone, Copy, Debug, Default)]
struct Resources {
    rss: u64,
    threads: u64,
    fds: u64,
    mounts: u64,
    watches: u64,
    cpu_ticks: u64,
}

impl From<acyclic_native_runtime::testing::ProcessResources> for Resources {
    fn from(resources: acyclic_native_runtime::testing::ProcessResources) -> Self {
        Self {
            rss: resources.memory_bytes,
            threads: resources.threads,
            fds: resources.descriptors,
            mounts: resources.mounts,
            watches: resources.watches,
            cpu_ticks: resources.cpu_centiseconds,
        }
    }
}

/// Every counter, watches included.
fn resources() -> Result<Resources> {
    acyclic_native_runtime::testing::process_resources()
        .map(Resources::from)
        .map_err(storage)
}

/// Cheap counters for the concurrent sampler; watches are read once at the end.
fn sample() -> std::io::Result<Resources> {
    acyclic_native_runtime::testing::sample_process_resources().map(Resources::from)
}

fn diagnose(root: &Path) {
    let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    println!("scale diag fds={:#?}", descriptor_shapes(&root));
    diagnose_threads_and_files(&root);
}

/// This process's descriptors, counted by the shape of what each names.
fn descriptor_shapes(root: &Path) -> std::collections::BTreeMap<String, u64> {
    use acyclic_native_runtime::testing::Descriptor;
    let mut fds = std::collections::BTreeMap::<String, u64>::new();
    for descriptor in acyclic_native_runtime::testing::descriptors()
        .into_iter()
        .flatten()
    {
        let target = match descriptor {
            Descriptor::File(path) => path.display().to_string(),
            Descriptor::Other(kind) => kind,
        };
        let target = target.replace(&root.display().to_string(), "<root>");
        let shape: String = target
            .split(['/', '\\'])
            .map(|part| {
                if part.chars().all(|c| c.is_ascii_digit())
                    || part.len() >= 20
                    || part.starts_with('#')
                    || part.contains(":[")
                {
                    "*"
                } else {
                    part
                }
            })
            .collect::<Vec<_>>()
            .join("/");
        *fds.entry(shape).or_default() += 1;
    }
    fds
}

#[cfg(not(target_os = "linux"))]
fn diagnose_threads_and_files(_: &Path) {}

#[cfg(target_os = "linux")]
fn thread_names() -> std::collections::BTreeMap<String, u64> {
    let mut threads = std::collections::BTreeMap::<String, u64>::new();
    for entry in std::fs::read_dir("/proc/self/task")
        .into_iter()
        .flatten()
        .flatten()
    {
        let name = std::fs::read_to_string(entry.path().join("comm")).unwrap_or_default();
        *threads.entry(name.trim().to_owned()).or_default() += 1;
    }
    threads
}

#[cfg(target_os = "linux")]
fn diagnose_threads_and_files(root: &Path) {
    println!("scale diag threads={:#?}", thread_names());
    let mut files = Vec::new();
    let mut pending = vec![root.join("0")];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)
            .into_iter()
            .flatten()
            .flatten()
        {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                pending.push(entry.path());
            } else {
                files.push((meta.len(), entry.path().display().to_string()));
            }
        }
    }
    files.sort();
    for (bytes, path) in files.iter().rev() {
        println!("scale diag file {bytes} {path}");
    }
}

fn disk_bytes(path: &Path) -> (u64, u64) {
    let mut bytes = 0;
    let mut files = 0;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                pending.push(entry.path());
            } else {
                bytes += meta.len();
                files += 1;
            }
        }
    }
    (bytes, files)
}

/// The nearest-rank `percent`th value.
fn percentile(values: &mut [Duration], percent: usize) -> Duration {
    values.sort_unstable();
    let rank = (values.len().saturating_sub(1) * percent + 50) / 100;
    values.get(rank).copied().unwrap_or_default()
}

use acyclic_native_runtime::testing::scale::PerAgent;

/// What the run's descriptor peak holds: thread handles (Windows counts each
/// thread a handle refers to; Unix has none) and the rest, and the task-view
/// mounts live at that instant.
#[derive(Clone, Copy, Debug, Default)]
struct PeakShape {
    thread_handles: u64,
    other_handles: u64,
    live_views: u64,
}

impl PeakShape {
    fn now() -> Self {
        use acyclic_native_runtime::testing::Descriptor;
        let descriptors = acyclic_native_runtime::testing::descriptors().unwrap_or_default();
        let thread_handles = descriptors
            .iter()
            .filter(|descriptor| matches!(descriptor, Descriptor::Other(kind) if kind == "Thread"))
            .count() as u64;
        Self {
            thread_handles,
            other_handles: descriptors.len() as u64 - thread_handles,
            live_views: live_views(),
        }
    }
}

/// Task views of every agent in this process not yet retired.
fn live_views() -> u64 {
    #[cfg(feature = "native-execution")]
    return crate::local::checkout_tools::child_views::live_in_process() as u64;
    #[cfg(not(feature = "native-execution"))]
    0
}

#[derive(Debug)]
struct Report {
    agents: u64,
    elapsed: Duration,
    baseline: Resources,
    baseline_shape: PeakShape,
    peak: Resources,
    peak_shape: PeakShape,
    idle: Resources,
    idle_cpu_ticks: u64,
    disk_bytes: u64,
    disk_files: u64,
    first_p50: Duration,
    first_p99: Duration,
    turn_p50: Duration,
    turn_p99: Duration,
}

impl Report {
    fn per_agent(&self, value: impl Fn(&Resources) -> u64, at: &Resources) -> PerAgent {
        PerAgent::of(value(at).saturating_sub(value(&self.baseline)), self.agents)
    }
    fn print(&self) {
        println!(
            "scale agents={} elapsed={:?} rss/agent idle={}B peak={}B threads={}->{} idle={} fds={}->{} idle={} \
             mounts={}->{} watches={}->{} disk/agent={}B files/agent={} spawn p50={:?} p99={:?} \
             turn p50={:?} p99={:?} idle cpu ticks/s={}",
            self.agents,
            self.elapsed,
            self.per_agent(|r| r.rss, &self.idle),
            self.per_agent(|r| r.rss, &self.peak),
            self.baseline.threads,
            self.peak.threads,
            self.idle.threads,
            self.baseline.fds,
            self.peak.fds,
            self.idle.fds,
            self.baseline.mounts,
            self.peak.mounts,
            self.baseline.watches,
            self.peak.watches,
            PerAgent::of(self.disk_bytes, self.agents),
            PerAgent::of(self.disk_files, self.agents),
            self.first_p50,
            self.first_p99,
            self.turn_p50,
            self.turn_p99,
            self.idle_cpu_ticks,
        );
        let (base, peak) = (self.baseline_shape, self.peak_shape);
        let threads = peak.thread_handles.saturating_sub(base.thread_handles);
        let others = peak.other_handles.saturating_sub(base.other_handles);
        let views = peak.live_views.max(1);
        println!(
            "scale peak handles: thread={threads} ({}/agent, {}/live view) other={others} \
             ({}/agent, {}/live view) live views={}",
            PerAgent::of(threads, self.agents),
            PerAgent::of(threads, views),
            PerAgent::of(others, self.agents),
            PerAgent::of(others, views),
            peak.live_views,
        );
    }
    fn idle_fds(&self) -> PerAgent {
        self.per_agent(|r| r.fds, &self.idle)
    }
}

async fn open_agent(directory: &Path, provider: Arc<ScaleProvider>) -> Result<LocalHarness> {
    let project = volume(directory).await?;
    LocalHarness::open(
        volume_config(directory, project, provider)?
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await
}

/// One local harness admits one root turn at a time, so each concurrent root
/// agent is its own harness (session) in this one process.
/// The process before any measured agent: what every run's costs are
/// counted from.
#[derive(Clone, Copy)]
struct Baseline {
    resources: Resources,
    shape: PeakShape,
}

impl Baseline {
    fn now() -> Result<Self> {
        Ok(Self {
            resources: resources()?,
            shape: PeakShape::now(),
        })
    }
}

async fn run_agents(
    agents: u64,
    children: usize,
    directory: &Path,
    from: Baseline,
) -> Result<Report> {
    let provider = Arc::new(ScaleProvider {
        children,
        started: Instant::now(),
        timings: Arc::default(),
        content: None,
    });
    let Baseline {
        resources: baseline,
        shape: baseline_shape,
    } = from;
    let peak = Arc::new(Mutex::new((baseline, baseline_shape)));
    let sampling = Arc::new(std::sync::atomic::AtomicBool::new(true));
    // Diagnosis samples finely and keeps what the peak's descriptors name.
    let diagnosing = crate::native_environment::variable("ACYCLIC_SCALE_DIAG").is_some();
    if diagnosing {
        println!(
            "scale diag baseline fds={:#?}",
            descriptor_shapes(directory)
        );
    }
    let sampler = {
        let (peak, sampling) = (peak.clone(), sampling.clone());
        let root = std::fs::canonicalize(directory).unwrap_or_else(|_| directory.to_path_buf());
        std::thread::spawn(move || -> std::io::Result<()> {
            let mut shapes = None;
            while sampling.load(std::sync::atomic::Ordering::Relaxed) {
                let now = sample()?;
                let mut guard = peak.lock().unwrap_or_else(PoisonError::into_inner);
                let (peak, shape) = &mut *guard;
                peak.rss = peak.rss.max(now.rss);
                peak.threads = peak.threads.max(now.threads);
                if now.fds > peak.fds {
                    *shape = PeakShape::now();
                    if diagnosing {
                        shapes = Some(descriptor_shapes(&root));
                    }
                }
                peak.fds = peak.fds.max(now.fds);
                peak.mounts = peak.mounts.max(now.mounts);
                drop(guard);
                // Fine enough to catch the creation peak the bound checks.
                std::thread::sleep(Duration::from_millis(if diagnosing { 2 } else { 5 }));
            }
            if let Some(shapes) = shapes {
                println!("scale diag peak fds={shapes:#?}");
            }
            Ok(())
        })
    };
    let started = Instant::now();
    let mut running = tokio::task::JoinSet::new();
    for agent in 0..agents {
        let provider = provider.clone();
        let directory = directory.join(agent.to_string());
        running.spawn(async move {
            let harness = open_agent(&directory, provider).await?;
            let output = harness.run(&format!("scale agent {agent}")).await?;
            Ok::<_, Error>((harness, output))
        });
    }
    let mut idle_agents = Vec::new();
    while let Some(output) = running.join_next().await {
        let (harness, output) = output.map_err(storage)??;
        assert!(output.text.starts_with("done "), "{output:?}");
        idle_agents.push(harness);
    }
    // The negative control's leak: descriptors each idle agent, root or
    // child, keeps, on a file outside the agents' directories so only
    // descriptors grow.
    let leaked_per_agent = setting::<u64>(LEAKED_DESCRIPTORS).unwrap_or(0);
    let leak = tempfile::NamedTempFile::new().map_err(storage)?;
    let leaked = (0..agents * (1 + children as u64) * leaked_per_agent)
        .map(|_| std::fs::File::open(leak.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(storage)?;
    let elapsed = started.elapsed();
    sampling.store(false, std::sync::atomic::Ordering::Relaxed);
    sampler
        .join()
        .map_err(|_| Error::Storage("resource sampler panicked".into()))?
        .map_err(storage)?;
    if crate::native_environment::variable("ACYCLIC_SCALE_DIAG").is_some() {
        diagnose(directory);
    }
    let before_idle = resources()?;
    let idle_ms = setting("ACYCLIC_SCALE_IDLE_MS").unwrap_or(500);
    println!("scale idle begin pid={}", std::process::id());
    tokio::time::sleep(Duration::from_millis(idle_ms)).await;
    let idle = resources()?;
    if diagnosing {
        println!("scale diag idle fds={:#?}", descriptor_shapes(directory));
        #[cfg(target_os = "linux")]
        println!("scale diag idle threads={:#?}", thread_names());
    }
    let (disk_bytes, disk_files) = disk_bytes(directory);
    let mut timings = std::mem::take(
        &mut *provider
            .timings
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
    );
    drop(idle_agents);
    drop(leaked);
    let (mut peak, peak_shape) = *peak.lock().unwrap_or_else(PoisonError::into_inner);
    peak.watches = idle.watches;
    Ok(Report {
        agents: agents * (1 + children as u64),
        elapsed,
        baseline,
        baseline_shape,
        peak,
        peak_shape,
        idle,
        idle_cpu_ticks: (idle.cpu_ticks - before_idle.cpu_ticks) * 1000 / idle_ms,
        disk_bytes,
        disk_files,
        first_p50: percentile(&mut timings.first, 50),
        first_p99: percentile(&mut timings.first, 99),
        turn_p50: percentile(&mut timings.turns, 50),
        turn_p99: percentile(&mut timings.turns, 99),
    })
}

/// Idle blocking threads retire quickly so the idle sample counts only
/// threads that agents keep alive.
fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .thread_keep_alive(Duration::from_millis(50))
        .enable_all()
        .build()
        .map_err(storage)
}

async fn measure(agents: u64, children: usize, from: Baseline) -> Result<Report> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let report = run_agents(agents, children, directory.path(), from).await?;
    report.print();
    Ok(report)
}

/// Per-agent ceilings for the CI-sized run, held by the cost each further
/// agent adds (see [`Costs::marginal`]). Each is a ratchet: a fix that lowers
/// a cost lowers its ceiling in the same change.
mod bounds {
    /// Resident memory retained per idle agent.
    pub(super) const IDLE_RSS_BYTES: PerAgent = PerAgent::whole(12 * 1024 * 1024);
    /// Open descriptors retained per idle agent: none of its own. Store
    /// ownership holds none (store claims); journals, logs and held roots
    /// hold one only within the process's descriptor cache, so the 48-agent
    /// run's total stays under one per agent. Windows also keeps each
    /// watched root's directory handle, which its change notifications need.
    pub(super) use acyclic_native_runtime::testing::scale::IDLE_DESCRIPTORS as IDLE_FDS;
    /// Open descriptors at the run's peak, while all agents are created and
    /// run their first turn together. Materializations are admitted against
    /// one process-wide descriptor allowance and cached files against the
    /// cache's, so the peak is bounded by those and not by the agent count.
    #[cfg(not(windows))]
    pub(super) const PEAK_FDS: PerAgent = PerAgent::whole(3);
    #[cfg(windows)]
    pub(super) const PEAK_FDS: PerAgent = PerAgent::whole(4);
    /// The soft descriptor limit the bounded run starts under: macOS's stock
    /// limit, which the run must neither raise nor exhaust. Windows has no
    /// per-process descriptor limit.
    #[cfg(unix)]
    pub(super) const DESCRIPTOR_LIMIT: u64 = 256;
    /// inotify watches retained per idle agent.
    pub(super) const WATCHES: PerAgent = PerAgent::whole(7);
    /// Threads retained per idle agent beyond the process-wide pools: none,
    /// since source watches share one routing hub.
    pub(super) use acyclic_native_runtime::testing::scale::IDLE_THREADS;
    /// Process-wide pools that persist while idle: the native host's I/O and
    /// native workers, with headroom. Only a cost counted from the baseline
    /// includes them; the marginal cost excludes them by construction.
    pub(super) const POOL_THREADS: u64 = 32;
    /// Durable bytes and files per agent.
    pub(super) const DISK_BYTES: PerAgent = PerAgent::whole(1024 * 1024);
    pub(super) const DISK_FILES: PerAgent = PerAgent::whole(16);
    use super::PerAgent;
}

/// Per-agent costs, each measured as the bounds define it.
struct Costs {
    idle_rss: PerAgent,
    idle_fds: PerAgent,
    peak_fds: PerAgent,
    watches: PerAgent,
    idle_threads: PerAgent,
    disk_bytes: PerAgent,
    disk_files: PerAgent,
}

impl Costs {
    /// What each further agent adds: the difference between the runs of N
    /// and 2N agents, divided by the N agents between them. Whatever the
    /// process holds independently of its agent count (the native host's
    /// pools, the mount library's state, the bounded descriptor cache and
    /// admission allowance once each is full) is in both runs and cancels, so
    /// no excluded warm-up agent is needed and nothing but O(N) cost remains.
    /// Disk is the exception: each run has its own directory.
    fn marginal(small: &Report, large: &Report) -> Self {
        let agents = large.agents.saturating_sub(small.agents);
        let slope =
            |value: &dyn Fn(&Report) -> u64| PerAgent::marginal(value(small), value(large), agents);
        Self {
            idle_rss: slope(&|r| r.idle.rss),
            idle_fds: slope(&|r| r.idle.fds),
            peak_fds: slope(&|r| r.peak.fds),
            watches: slope(&|r| r.idle.watches),
            idle_threads: slope(&|r| r.idle.threads),
            // Each run writes its own directory, which holds nothing but its
            // agents' stores, so its disk use divides over its own agents.
            disk_bytes: PerAgent::of(large.disk_bytes, large.agents),
            disk_files: PerAgent::of(large.disk_files, large.agents),
        }
    }

    /// Everything counted from the cold baseline, divided by one run's
    /// agents: the process-wide constant included. The negative control for
    /// [`Self::marginal`].
    fn from_baseline(report: &Report) -> Self {
        Self {
            idle_rss: report.per_agent(|r| r.rss, &report.idle),
            idle_fds: report.per_agent(|r| r.fds, &report.idle),
            peak_fds: report.per_agent(|r| r.fds, &report.peak),
            watches: report.per_agent(|r| r.watches, &report.idle),
            idle_threads: PerAgent::of(
                report
                    .idle
                    .threads
                    .saturating_sub(report.baseline.threads + bounds::POOL_THREADS),
                report.agents,
            ),
            disk_bytes: PerAgent::of(report.disk_bytes, report.agents),
            disk_files: PerAgent::of(report.disk_files, report.agents),
        }
    }

    fn print(&self, measured: &str) {
        println!(
            "scale {measured} cost/agent: idle rss={}B idle fds={} peak fds={} watches={}              idle threads={} disk={}B files={}",
            self.idle_rss,
            self.idle_fds,
            self.peak_fds,
            self.watches,
            self.idle_threads,
            self.disk_bytes,
            self.disk_files,
        );
    }

    fn assert_bounds(&self, measured: &str) {
        use bounds::*;
        self.print(measured);
        let checks = [
            ("idle rss bytes", self.idle_rss, IDLE_RSS_BYTES),
            ("idle fds", self.idle_fds, IDLE_FDS),
            ("peak fds", self.peak_fds, PEAK_FDS),
            ("inotify watches", self.watches, WATCHES),
            ("idle threads", self.idle_threads, IDLE_THREADS),
            ("disk bytes", self.disk_bytes, DISK_BYTES),
            ("disk files", self.disk_files, DISK_FILES),
        ];
        let exceeded = checks
            .iter()
            .filter(|(_, value, ceiling)| value > ceiling)
            .map(|(name, value, ceiling)| format!("{name}/agent {value} > {ceiling}"))
            .collect::<Vec<_>>();
        assert!(
            exceeded.is_empty(),
            "per-agent ceilings exceeded: {exceeded:?}"
        );
    }
}

/// Bound the cost counted from the cold baseline instead of the marginal
/// cost: the negative control that shows the marginal cost is what excludes
/// the process-wide constant.
const FROM_BASELINE: &str = "ACYCLIC_SCALE_FROM_BASELINE";

/// Descriptors each idle agent keeps beyond the harness's own: the negative
/// control's leak.
const LEAKED_DESCRIPTORS: &str = "ACYCLIC_SCALE_LEAKED_DESCRIPTORS";

/// A bounded run: N then 2N roots, each with `children` children that each
/// run one checkout command, and the negative controls' faults.
struct Run {
    /// N, the smallest root count whose runs separate an O(1) per-agent cost
    /// from an O(N) one: the process-wide pools are full at N, so the step
    /// from N to 2N is the agents' own cost alone.
    roots: u64,
    children: u64,
    leaked_per_agent: u64,
    retain_child_views: bool,
    /// Mount each task view as its own native instance instead of a view of
    /// the process's shared one: the shared instance's negative control.
    unshared_task_views: bool,
    /// Bound the cost counted from the cold baseline: the marginal cost's
    /// negative control.
    from_baseline: bool,
}

impl Run {
    /// Roots that each spawn two children and wait for them to settle, so
    /// every run measures what children cost as well as roots.
    const AGENTS: Self = Self {
        roots: 2,
        children: 2,
        leaked_per_agent: 0,
        retain_child_views: false,
        unshared_task_views: false,
        from_baseline: false,
    };

    /// What the larger run's report line starts with.
    fn largest(&self) -> String {
        format!("scale agents={} ", 2 * self.roots * (1 + self.children))
    }
}

/// The CI-sized run, bounds asserted, in its own process so that concurrently
/// running tests do not perturb its process-wide counters.
fn bounded_run(run: &Run) -> Result<std::process::Output> {
    let executable = std::env::current_exe().map_err(storage)?;
    // Unix runs it under the stock soft descriptor limit, lowered by the
    // shell before exec; Windows has no per-process descriptor limit.
    #[cfg(unix)]
    let mut command = {
        let mut command = std::process::Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(format!(
                "ulimit -S -n {} && exec \"$0\" \"$@\"",
                bounds::DESCRIPTOR_LIMIT
            ))
            .arg(executable);
        command
    };
    #[cfg(not(unix))]
    let mut command = std::process::Command::new(executable);
    command
        .args([concat!(module_path!(), "::ramp")
            .split_once("::")
            .map_or("", |(_, name)| name)])
        .args(["--exact", "--ignored", "--nocapture", "--test-threads=1"])
        // N then 2N roots: the per-agent cost is the step between them.
        .env(
            "ACYCLIC_SCALE_AGENTS",
            format!("{},{}", run.roots, 2 * run.roots),
        )
        .env("ACYCLIC_SCALE_CHILDREN", run.children.to_string())
        .env("ACYCLIC_SCALE_IDLE_MS", "300")
        .env("ACYCLIC_SCALE_BOUNDS", "1")
        .env(LEAKED_DESCRIPTORS, run.leaked_per_agent.to_string())
        // One cached and one admitted descriptor: both process-wide bounds
        // are full at N, so neither adds to the step from N to 2N.
        .env(acyclic_native_runtime::CACHED_DESCRIPTORS_VARIABLE, "1")
        .env(acyclic_native_runtime::ADMITTED_DESCRIPTORS_VARIABLE, "1")
        .env_remove("ACYCLIC_SCALE_DIAG");
    if run.from_baseline {
        command.env(FROM_BASELINE, "1");
    } else {
        command.env_remove(FROM_BASELINE);
    }
    if run.retain_child_views {
        command.env(crate::local::children::RETAIN_CHILD_VIEWS, "1");
    } else {
        command.env_remove(crate::local::children::RETAIN_CHILD_VIEWS);
    }
    if run.unshared_task_views {
        command.env(crate::local::children::UNSHARED_TASK_VIEWS, "1");
    } else {
        command.env_remove(crate::local::children::UNSHARED_TASK_VIEWS);
    }
    command.output().map_err(storage)
}

/// Runs `run` and checks that it reached its larger count and that its
/// bounds held, or, given `failure`, that they failed naming it.
fn expect_run(run: &Run, failure: Option<&str>) -> Result<()> {
    let output = bounded_run(run)?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("{stdout}");
    let expected = match failure {
        None => output.status.success(),
        Some(failure) => !output.status.success() && format!("{stdout}{stderr}").contains(failure),
    };
    assert!(
        stdout.contains(&run.largest()) && expected,
        "{stdout}\n{stderr}"
    );
    Ok(())
}

/// Roots that write, read and spawn children, and the children that run a
/// checkout command and settle, stay within every per-agent bound: their
/// stores' change records, child views and bookkeeping are all counted.
#[test]
fn per_agent_cost_is_bounded() -> Result<()> {
    expect_run(&Run::AGENTS, None)
}

/// The bound fails an agent that keeps one descriptor more than it allows,
/// on whichever OS counts them, so the counts it checks are real.
#[test]
fn a_descriptor_leaked_per_agent_fails_the_bound() -> Result<()> {
    expect_run(
        &Run {
            leaked_per_agent: bounds::IDLE_FDS.0 / 1000 + 1,
            ..Run::AGENTS
        },
        Some("idle fds/agent"),
    )
}

/// Children that ran a checkout command through the stock controller keep no
/// task view once their turns settle; the views really existed.
#[cfg(feature = "native-execution")]
#[tokio::test(flavor = "multi_thread")]
async fn settled_children_keep_no_task_view() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let provider = Arc::new(ScaleProvider {
        children: 2,
        started: Instant::now(),
        timings: Arc::default(),
        content: None,
    });
    let harness = open_agent(directory.path(), provider).await?;
    let output = harness.run("scale agent 0").await?;
    assert!(output.text.starts_with("done "), "{output:?}");
    let views = &harness.workspace.as_ref().unwrap().children;
    assert_eq!((views.constructed(), views.live()), (2, 0));
    Ok(())
}

/// Settled children that keep task views mounted as instances of their own
/// exceed the bounds, so the bounds see each retained instance.
#[test]
#[cfg(feature = "native-execution")]
fn retained_child_views_fail_the_bound() -> Result<()> {
    expect_run(
        &Run {
            retain_child_views: true,
            unshared_task_views: true,
            ..Run::AGENTS
        },
        Some("per-agent ceilings exceeded"),
    )
}

/// Retained task views that are views of the process's shared instance cost
/// no mount, thread or descriptor, so even children that keep them stay
/// within the bounds; `retained_child_views_fail_the_bound` is the same run
/// with an instance per view.
#[test]
#[cfg(all(any(target_os = "linux", windows), feature = "native-execution"))]
fn retained_shared_child_views_stay_within_the_bound() -> Result<()> {
    expect_run(
        &Run {
            retain_child_views: true,
            ..Run::AGENTS
        },
        None,
    )
}

/// Counted from the cold baseline, the process-wide services' constant
/// handles (the native host's I/O pool threads and the native mount
/// library's state, which Windows counts as handles) exceed the idle bound,
/// so taking the step from N to 2N is what excludes them.
#[test]
#[cfg(all(windows, feature = "native-execution"))]
fn a_cost_counted_from_the_baseline_fails_the_idle_bound() -> Result<()> {
    expect_run(
        &Run {
            from_baseline: true,
            ..Run::AGENTS
        },
        Some("idle fds/agent"),
    )
}

/// A numeric run setting from the environment.
fn setting<T: std::str::FromStr>(name: &str) -> Option<T> {
    crate::native_environment::variable(name)?
        .to_str()?
        .parse()
        .ok()
}

/// Full ramp: `scripts/harness-scale.sh 100 1000 10000`.
#[test]
#[ignore = "scale ramp; run through scripts/harness-scale.sh"]
fn ramp() -> Result<()> {
    let agents = crate::native_environment::variable("ACYCLIC_SCALE_AGENTS")
        .and_then(|agents| agents.into_string().ok())
        .unwrap_or_else(|| "100,1000".into());
    // Idle stores' journals and logs hold descriptors only within the
    // process-wide bound `ACYCLIC_CACHED_DESCRIPTORS` sets, so it bounds
    // their total at any agent count.
    let children = setting("ACYCLIC_SCALE_CHILDREN").unwrap_or(0);
    let bounded = crate::native_environment::variable("ACYCLIC_SCALE_BOUNDS").is_some();
    let counts = agents
        .split(',')
        .map(|count| count.trim().parse::<u64>().map_err(storage))
        .collect::<Result<Vec<_>>>()?;
    runtime()?.block_on(async {
        // One baseline, taken before anything starts the process-wide
        // services, for every count: each run counts everything, including
        // what an earlier run left behind.
        let baseline = Baseline::now()?;
        let mut previous: Option<Report> = None;
        for count in &counts {
            let report = measure(*count, children, baseline).await?;
            if bounded
                && let Some(previous) = previous.filter(|previous| previous.agents < report.agents)
            {
                assert!(
                    report.idle_fds() <= previous.idle_fds(),
                    "idle fds/agent grew with the agent count: {} at {} -> {} at {}",
                    previous.idle_fds(),
                    previous.agents,
                    report.idle_fds(),
                    report.agents,
                );
                let marginal = Costs::marginal(&previous, &report);
                let from_baseline = Costs::from_baseline(&report);
                if crate::native_environment::variable(FROM_BASELINE).is_some() {
                    marginal.print("marginal");
                    from_baseline.assert_bounds("from-baseline");
                } else {
                    from_baseline.print("from-baseline");
                    marginal.assert_bounds("marginal");
                }
            }
            previous = Some(report);
        }
        if bounded && counts.len() != 2 {
            return Err(Error::Invalid(format!(
                "a bounded run measures exactly N and 2N roots, not {counts:?}"
            )));
        }
        Ok(())
    })
}
