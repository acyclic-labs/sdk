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
        // A child answers from one model call: concurrent sibling writes into
        // the parent's workspace currently fail admission with a generation
        // conflict (see #1030), so children measure dispatch cost only.
        if agent.contains('c') {
            return Ok(done());
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
        request: PreparedModelRequest,
        _: ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        Box::pin(
            futures::stream::once(async move {
                let prompt = self.prompt(&request).await?;
                let events = self.reply(&request, &prompt)?;
                Ok::<_, Error>(futures::stream::iter(events.into_iter().map(Ok)))
            })
            .try_flatten(),
        )
    }
}

/// Process-wide OS resources, read from procfs where available.
#[derive(Clone, Copy, Debug, Default)]
struct Resources {
    rss: u64,
    threads: u64,
    fds: u64,
    mounts: u64,
    watches: u64,
    cpu_ticks: u64,
}

#[cfg(target_os = "linux")]
fn resources() -> Resources {
    Resources {
        watches: std::fs::read_dir("/proc/self/fdinfo").map_or(0, |entries| {
            entries
                .filter_map(|entry| std::fs::read_to_string(entry.ok()?.path()).ok())
                .map(|info| {
                    info.lines()
                        .filter(|line| line.starts_with("inotify"))
                        .count() as u64
                })
                .sum()
        }),
        ..sample()
    }
}

/// Cheap counters for the concurrent sampler; inotify watches are read once at the end.
#[cfg(target_os = "linux")]
fn sample() -> Resources {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| rest.split_whitespace().next()?.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let fds = std::fs::read_dir("/proc/self/fd").map_or(0, |entries| entries.count() as u64);
    let mounts =
        std::fs::read_to_string("/proc/self/mountinfo").map_or(0, |m| m.lines().count() as u64);
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let cpu_ticks = stat.rsplit(')').next().map_or(0, |rest| {
        rest.split_whitespace()
            .skip(11)
            .take(2)
            .filter_map(|tick| tick.parse::<u64>().ok())
            .sum()
    });
    Resources {
        rss: field("VmRSS:") * 1024,
        threads: field("Threads:"),
        fds,
        mounts,
        watches: 0,
        cpu_ticks,
    }
}

#[cfg(target_os = "linux")]
fn diagnose(root: &Path) {
    let mut fds = std::collections::BTreeMap::<String, u64>::new();
    for entry in std::fs::read_dir("/proc/self/fd")
        .into_iter()
        .flatten()
        .flatten()
    {
        let target =
            std::fs::read_link(entry.path()).map_or("?".into(), |t| t.display().to_string());
        let target = target.replace(&root.display().to_string(), "<root>");
        let shape: String = target
            .split('/')
            .map(|part| {
                if part.chars().all(|c| c.is_ascii_digit())
                    || part.len() > 24
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
    let mut threads = std::collections::BTreeMap::<String, u64>::new();
    for entry in std::fs::read_dir("/proc/self/task")
        .into_iter()
        .flatten()
        .flatten()
    {
        let name = std::fs::read_to_string(entry.path().join("comm")).unwrap_or_default();
        *threads.entry(name.trim().to_owned()).or_default() += 1;
    }
    println!(
        "scale diag fds={fds:#?}
threads={threads:#?}"
    );
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

#[cfg(not(target_os = "linux"))]
fn diagnose(_: &Path) {}

#[cfg(not(target_os = "linux"))]
fn resources() -> Resources {
    Resources::default()
}

#[cfg(not(target_os = "linux"))]
fn sample() -> Resources {
    Resources::default()
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

/// A per-agent quantity in thousandths, printed with one decimal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PerAgent(u64);

impl PerAgent {
    fn of(total: u64, agents: u64) -> Self {
        Self(total.saturating_mul(1000) / agents.max(1))
    }
    #[cfg(target_os = "linux")]
    const fn whole(value: u64) -> Self {
        Self(value * 1000)
    }
}

impl std::fmt::Display for PerAgent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}", self.0 / 1000, self.0 % 1000 / 100)
    }
}

#[derive(Debug)]
struct Report {
    agents: u64,
    elapsed: Duration,
    baseline: Resources,
    peak: Resources,
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
async fn run_agents(agents: u64, children: usize, directory: &Path) -> Result<Report> {
    let provider = Arc::new(ScaleProvider {
        children,
        started: Instant::now(),
        timings: Arc::default(),
        content: None,
    });
    let baseline = resources();
    let peak = Arc::new(Mutex::new(baseline));
    let sampling = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let sampler = {
        let (peak, sampling) = (peak.clone(), sampling.clone());
        std::thread::spawn(move || {
            while sampling.load(std::sync::atomic::Ordering::Relaxed) {
                let now = sample();
                let mut peak = peak.lock().unwrap_or_else(PoisonError::into_inner);
                peak.rss = peak.rss.max(now.rss);
                peak.threads = peak.threads.max(now.threads);
                peak.fds = peak.fds.max(now.fds);
                peak.mounts = peak.mounts.max(now.mounts);
                drop(peak);
                std::thread::sleep(Duration::from_millis(100));
            }
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
    let elapsed = started.elapsed();
    sampling.store(false, std::sync::atomic::Ordering::Relaxed);
    sampler
        .join()
        .map_err(|_| Error::Storage("resource sampler panicked".into()))?;
    if crate::native_environment::variable("ACYCLIC_SCALE_DIAG").is_some() {
        diagnose(directory);
    }
    let before_idle = resources();
    let idle_ms = setting("ACYCLIC_SCALE_IDLE_MS").unwrap_or(500);
    println!("scale idle begin pid={}", std::process::id());
    tokio::time::sleep(Duration::from_millis(idle_ms)).await;
    let idle = resources();
    let (disk_bytes, disk_files) = disk_bytes(directory);
    let mut timings = std::mem::take(
        &mut *provider
            .timings
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
    );
    drop(idle_agents);
    let mut peak = *peak.lock().unwrap_or_else(PoisonError::into_inner);
    peak.watches = idle.watches;
    Ok(Report {
        agents: agents * (1 + children as u64),
        elapsed,
        baseline,
        peak,
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

fn measure(agents: u64, children: usize) -> Result<Report> {
    let directory = tempfile::tempdir().map_err(storage)?;
    // Idle blocking threads retire quickly so the idle sample counts only
    // threads that agents keep alive.
    let report = tokio::runtime::Builder::new_multi_thread()
        .thread_keep_alive(Duration::from_millis(50))
        .enable_all()
        .build()
        .map_err(storage)?
        .block_on(run_agents(agents, children, directory.path()))?;
    report.print();
    Ok(report)
}

/// Per-agent ceilings for the CI-sized run. Each is a ratchet: a fix that
/// lowers a cost lowers its ceiling in the same change.
#[cfg(target_os = "linux")]
mod bounds {
    /// Resident memory retained per idle agent.
    pub(super) const IDLE_RSS_BYTES: PerAgent = PerAgent::whole(12 * 1024 * 1024);
    /// Open descriptors retained per idle agent.
    pub(super) const IDLE_FDS: PerAgent = PerAgent::whole(16);
    /// inotify watches retained per idle agent.
    pub(super) const WATCHES: PerAgent = PerAgent::whole(7);
    /// Threads retained per idle agent beyond the process-wide pools: none,
    /// since source watches share one routing hub.
    pub(super) const IDLE_THREADS: PerAgent = PerAgent::whole(0);
    /// Process-wide pools that persist while idle: the native host's I/O and
    /// native workers, with headroom.
    pub(super) const POOL_THREADS: u64 = 32;
    /// Durable bytes and files per agent.
    pub(super) const DISK_BYTES: PerAgent = PerAgent::whole(1024 * 1024);
    pub(super) const DISK_FILES: PerAgent = PerAgent::whole(16);
    use super::PerAgent;
}

#[cfg(target_os = "linux")]
impl Report {
    fn assert_bounds(&self) {
        use bounds::*;
        let idle_rss = self.per_agent(|r| r.rss, &self.idle);
        let idle_fds = self.per_agent(|r| r.fds, &self.idle);
        let watches = self.per_agent(|r| r.watches, &self.idle);
        let threads = PerAgent::of(
            self.idle
                .threads
                .saturating_sub(self.baseline.threads + POOL_THREADS),
            self.agents,
        );
        let disk = PerAgent::of(self.disk_bytes, self.agents);
        let files = PerAgent::of(self.disk_files, self.agents);
        let checks = [
            ("idle rss bytes", idle_rss, IDLE_RSS_BYTES),
            ("idle fds", idle_fds, IDLE_FDS),
            ("inotify watches", watches, WATCHES),
            ("idle threads", threads, IDLE_THREADS),
            ("disk bytes", disk, DISK_BYTES),
            ("disk files", files, DISK_FILES),
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

/// The CI-sized run executes in its own process so that concurrently running
/// tests do not perturb its process-wide counters.
#[cfg(target_os = "linux")]
#[test]
fn per_agent_cost_is_bounded() -> Result<()> {
    let output = std::process::Command::new(std::env::current_exe().map_err(storage)?)
        .args([concat!(module_path!(), "::ramp")
            .split_once("::")
            .map_or("", |(_, name)| name)])
        .args(["--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .env("ACYCLIC_SCALE_AGENTS", "48")
        .env("ACYCLIC_SCALE_CHILDREN", "0")
        .env("ACYCLIC_SCALE_IDLE_MS", "300")
        .env("ACYCLIC_SCALE_BOUNDS", "1")
        .env_remove("ACYCLIC_SCALE_DIAG")
        .output()
        .map_err(storage)?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("{stdout}");
    assert!(
        output.status.success() && stdout.contains("scale agents=48 "),
        "{stdout}
{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
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
    for count in agents.split(',') {
        let children = setting("ACYCLIC_SCALE_CHILDREN").unwrap_or(0);
        let report = measure(count.trim().parse().map_err(storage)?, children)?;
        #[cfg(target_os = "linux")]
        if crate::native_environment::variable("ACYCLIC_SCALE_BOUNDS").is_some() {
            report.assert_bounds();
        }
        #[cfg(not(target_os = "linux"))]
        let _ = report;
    }
    Ok(())
}
