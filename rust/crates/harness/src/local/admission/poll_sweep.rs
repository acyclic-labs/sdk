//! Replay the real run state machine over in-process filesystem/stream providers.
use super::*;
use crate::{
    context::{CompactionPolicy, ThresholdCompaction},
    resources::ProviderRef,
    test_support::ScriptedTurn,
    tool::{JsonToolProjection, ToolDefinition},
};
use acyclic_fs::{Fs, LocalOptions};
use acyclic_stream::UnixMillisClock;
use std::cell::RefCell;
use std::sync::Arc;

mod model;
mod schedule;
use model::{Effect, ModelEffects};
use schedule::{Exit, Schedule, check_cut, drive};
mod coverage;
mod invariants;
mod read_contention;
mod read_profile;
mod requests;
mod seed;
mod selected_history;
use seed::Seed;

type Harness = LocalHarness;
const PROMPT: &str = "recover the original run input";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    Text,
    EffectAndContinuation,
    Compaction,
}

struct Clock;
impl UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        1
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    schedule: Arc<Schedule>,
    events:
        RefCell<tokio::sync::mpsc::UnboundedReceiver<acyclic_native_runtime::testing::Boundary>>,
    issuer: AuthorityIssuer,
    volumes: [VolumeRef; 4],
    provider: Arc<ModelEffects>,
    effect: Arc<Effect>,
    scenario: Scenario,
    history: RefCell<Option<crate::projection::SelectedModelContext>>,
}

impl Fixture {
    async fn new(scenario: Scenario, seed: Option<&Seed>) -> Result<Self> {
        let (schedule, events) = Schedule::new();
        let directory = match seed {
            Some(seed) => temporary_directory(&seed.parent),
            None => fixture_directory(),
        }
        .map_err(storage)?;
        let reference =
            ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
        let agent = AgentId::from_bytes([1; 16]);
        let volume = |name| {
            VolumeRef::new(
                reference.clone(),
                name,
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
                &mut None,
            )
        };
        let volumes = [
            VolumeRef::new(
                reference.clone(),
                "project".into(),
                VolumeClass::Project,
                VolumeOwner::Project("project".into()),
                &mut None,
            )?,
            volume("scratch".into())?,
            volume("journal".into())?,
            volume("tasks".into())?,
        ];
        let root = directory.path().join("filesystem");
        if let Some(seed) = seed {
            seed.restore(scenario, directory.path())?;
        } else {
            let fs = Fs::local(LocalOptions::new(root)).await.map_err(storage)?;
            let files = FilesystemHost::new(fs, &reference, None)?;
            for volume in &volumes {
                files.create_volume(volume).await?;
            }
        }
        let turns = match scenario {
            Scenario::Text => vec![ScriptedTurn::Text("done".into())],
            Scenario::EffectAndContinuation => vec![
                model::tool_turn(),
                ScriptedTurn::Text("root".into()),
                ScriptedTurn::Text("done".into()),
            ],
            Scenario::Compaction => vec![
                ScriptedTurn::Text("summary".into()),
                ScriptedTurn::Text("done".into()),
            ],
        };
        Ok(Self {
            directory,
            schedule: schedule.clone(),
            events: RefCell::new(events),
            issuer: AuthorityIssuer::new(
                "acyclic.local",
                [7; 32],
                Authority {
                    kind: if matches!(scenario, Scenario::Compaction) {
                        AggregateKind::Conversation
                    } else {
                        AggregateKind::Task
                    },
                    id: TaskId::from_bytes([2; 16]).to_string(),
                },
            ),
            volumes,
            provider: Arc::new(ModelEffects::new(
                turns,
                schedule.clone(),
                matches!(scenario, Scenario::Compaction),
            )),
            effect: Arc::new(Effect {
                schedule,
                calls: Default::default(),
            }),
            scenario,
            history: RefCell::new(seed.and_then(|seed| seed.history.clone())),
        })
    }

    async fn open(&self) -> Result<Harness> {
        // Reconstruct the host and discard all filesystem/runtime caches. Only
        // the two provider states and the external effect receipts survive.
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "open:begin");
        let fs = Fs::local(LocalOptions::new(self.directory.path().join("filesystem")))
            .await
            .map_err(storage)?;
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "open:filesystem");
        let stream = Arc::new(
            LocalStream::open_with_clock(
                self.directory.path().join("tasks"),
                Default::default(),
                Arc::new(Clock),
            )
            .await
            .map_err(storage)?,
        );
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "open:stream");
        let files = FilesystemHost::new(fs, self.volumes[0].provider(), None)?;
        let verifier = self.issuer.verifier(&mut None)?;
        let mut tools = ToolRegistry::default();
        if matches!(self.scenario, Scenario::EffectAndContinuation) {
            tools.register(
                (
                    Tool {
                        hook_names: &[],
                        definition: ToolDefinition {
                            name: "test.effect".into(),
                            revision: "1".into(),
                            description: "Increment an external counter".into(),
                            input_schema: serde_json::json!({"type":"object"}),
                            output_schema: serde_json::json!({"type":"object"}),
                            projection_schema: crate::tool::json_projection_schema(
                                serde_json::json!({"type":"object"}),
                            ),
                        },
                        executor: self.effect.clone(),
                        projection: Arc::new(JsonToolProjection),
                    },
                    None,
                ),
                &mut None,
                None,
            )?;
        }
        let (runtime, signed, task_files) = runtime::open(runtime::Configuration {
            files: files.clone(),
            #[cfg(feature = "native-execution")]
            workspace: None,
            stream: stream.clone(),
            task_volume: &self.volumes[3],
            issuer: &self.issuer,
            verifier: &verifier,
            agent: AgentId::from_bytes([1; 16]),
            project: if matches!(self.scenario, Scenario::EffectAndContinuation) {
                &self.volumes[1]
            } else {
                &self.volumes[0]
            },
            scratch: &self.volumes[1],
            journal: &self.volumes[2],
            tools: &tools,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            capabilities: &Capabilities::default(),
            limits: Limits::default(),
            run_limits: TaskRunLimits::default(),
            session_limits: SessionLimits {
                active_tasks: 2,
                total_tasks: 2,
                depth: 1,
                model_steps: 4,
            },
            concurrency: 1,
            clock: Arc::new(Clock),
        })
        .await?;
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "open:runtime");
        let compact = matches!(self.scenario, Scenario::Compaction);
        Ok(LocalHarness {
            issuer: Some(self.issuer.clone()),
            task_files,
            external_bindings: Default::default(),
            files: files.clone(),
            runtime,
            project: self.volumes[if matches!(self.scenario, Scenario::EffectAndContinuation) {
                1
            } else {
                0
            }]
            .clone(),
            scratch: self.volumes[1].clone(),
            journal: self.volumes[2].clone(),
            verifier,
            signed,
            model: Some((
                Model::new(
                    "scripted".into(),
                    "test".into(),
                    "1".into(),
                    serde_json::Value::Null,
                )?,
                self.provider.clone(),
            )),
            context_manifest: None,
            context_read_observer: None,
            #[cfg(feature = "hooks")]
            settings: Arc::new(
                crate::native_settings::SettingsConfig::new(
                    self.directory.path(),
                    self.directory.path().join("user-config"),
                )
                .codex_home(self.directory.path().join("user-config/.codex")),
            ),
            limits: Limits::default(),
            compaction: if compact {
                CompactionPolicy::Threshold(ThresholdCompaction {
                    response_reserve_tokens: 64,
                    recent_tokens: 16,
                    retention: Default::default(),
                })
            } else {
                CompactionPolicy::Disabled
            },
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            mcp_diagnostics: vec![],
            goal_continuations: matches!(self.scenario, Scenario::EffectAndContinuation).then_some(
                crate::goal::GoalContinuationPolicy {
                    max_continuations: 1,
                },
            ),
            admission: StateArc::new(Mutex::new(())),
            #[cfg(feature = "native-execution")]
            workspace: None,
            #[cfg(feature = "native-execution")]
            checkout_context: false,
        })
    }

    async fn initialize(&self, harness: &Harness) -> Result<()> {
        if matches!(self.scenario, Scenario::Compaction) {
            self.history
                .replace(Some(selected_history::initialize(harness).await?));
        }
        if matches!(self.scenario, Scenario::EffectAndContinuation) {
            FilesystemContentPublisher::new(
                harness.files.clone(),
                &harness.scratch,
                &harness.verifier,
                &harness.signed,
                harness.limits.file_bytes,
                None,
            )?
            .stage_once(
                OperationId::from_bytes([8; 16]),
                "GOAL.md",
                b"finish the objective",
                "text/markdown",
                "GOAL.md",
                None,
            )?
            .await?;
        }
        Ok(())
    }

    async fn run(&self, harness: &Harness) -> Result<crate::executor::TurnOutput> {
        let history = self.history.borrow().clone();
        match history {
            Some(history) => selected_history::run(harness, history).await,
            None => harness.run(PROMPT).await,
        }
    }

    fn assert_effects(&self) {
        let (models, tools) = match self.scenario {
            Scenario::Text => (1, 0),
            Scenario::EffectAndContinuation => (3, 1),
            Scenario::Compaction => (2, 0),
        };
        assert_eq!(
            self.provider.accepted(),
            models,
            "missing or duplicated model effect"
        );
        assert_eq!(
            self.effect.calls.lock().unwrap().len(),
            tools,
            "missing or duplicated tool effect"
        );
    }
}

struct Coverage {
    observations: invariants::Observations,
    points: Vec<&'static str>,
    unadmitted: bool,
    ready_reads: usize,
    admission_submission: Option<usize>,
    admission_committed: bool,
}

async fn case(
    scenario: Scenario,
    cut: Option<usize>,
    baseline: &[&'static str],
    seed: Option<&Seed>,
) -> Result<Coverage> {
    let profile = read_profile::start();
    // Each worker reuses its current-thread runtime. Bootstrap/recovery closures
    // use the shared host queue; the scoped adapter retains the original gates.
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let owned_seed = if seed.is_none() {
        Some(Seed::new(scenario).await?)
    } else {
        None
    };
    let seed = seed.or(owned_seed.as_ref());
    let restore_started = profile.map(|_| std::time::Instant::now());
    let fixture = Fixture::new(scenario, seed).await?;
    let _reads = read_profile::install(&fixture.schedule, profile, restore_started);
    let result: Result<Coverage> = Box::pin(async {
        let harness = fixture.open().await?;
        let schedule = &fixture.schedule;
        schedule.start();
        let (exit, hit) = drive(
            fixture.run(&harness),
            cut,
            schedule,
            &mut fixture.events.borrow_mut(),
        )
        .await;
        let points = schedule.points();
        assert_eq!(points.len(), hit, "uncontrolled I/O changed the schedule");
        if let Some(cut) = cut {
            check_cut(&exit, cut, hit).expect("every cancellation must fire");
            assert_eq!(
                points,
                baseline[..cut],
                "replay diverged before cancellation"
            );
        } else {
            let Exit::Completed(output) = exit else {
                panic!("baseline cancelled")
            };
            assert_eq!(output?.text, "done");
        }
        drop(harness);
        if cut.is_some() {
            // The driver joined the physical completion of detached provider work.
            let trace = schedule.drained();
            assert_eq!(
                schedule.drained(),
                trace,
                "publication or dispatch after cancel"
            );
        }
        schedule.recover();
        let reopened = fixture.open().await?;
        let current = reopened.current_turn().await?.0;
        let mut unadmitted = false;
        if let Some(current) = &current {
            let task = crate::runtime::task_id_for_admission(current.operation);
            if matches!(
                reopened.runtime.task_host().outcome(task).await,
                Err(Error::NotFound(_))
            ) {
                unadmitted = true;
                // Negative control for the pre-#672 implementation: direct outcome
                // and attach reject this actual accepted HEAD/admission cut.
                let definition = reopened
                    .runtime
                    .harness()
                    .task::<serde_json::Value, crate::executor::TurnOutput>(
                        "acyclic.stock_turn@1",
                    )?;
                assert!(matches!(
                    reopened.runtime.harness().attach(task, &definition).await,
                    Err(Error::NotFound(_))
                ));
                assert!(reopened.head_is_terminal(Some(current)).await?);
            }
        }
        let admission_committed = current.is_some() && !unadmitted;
        let (root, output) = if let Some(current) = current {
            let root = crate::runtime::task_id_for_admission(current.root);
            (root, reopened.resume(root).await?)
        } else {
            let output = fixture.run(&reopened).await?;
            (
                crate::runtime::task_id_for_admission(
                    reopened.current_turn().await?.0.unwrap().root,
                ),
                output,
            )
        };
        assert_eq!(output.text, "done");
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "case:recovered");
        fixture.assert_effects();
        for _ in 0..2 {
            assert_eq!(reopened.resume(root).await?.text, "done");
            fixture.assert_effects();
        }
        drop(reopened);
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "case:repeated-resume");
        let reopened = fixture.open().await?;
        assert_eq!(reopened.resume(root).await?.text, "done");
        fixture.assert_effects();
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "case:cold-resume");
        if matches!(scenario, Scenario::Compaction) {
            let admission = reopened.runtime.task_host().observe_admission(root).await?;
            assert!(
                !admission.grants.contains(
                    &reopened
                        .journal
                        .capability(VolumeOperation::Read, &mut None)?
                ),
                "compaction must not widen the task's host-journal authority"
            );
            let summaries = fixture.provider.summary_files();
            assert_eq!(
                summaries.len(),
                1,
                "compacted request must contain one summary"
            );
            let summary = &summaries[0];
            assert_eq!(summary.volume(), &reopened.scratch);
            assert_ne!(summary.volume(), &reopened.project);
            assert_ne!(summary.volume(), &reopened.journal);
            let reader = crate::filesystem::FilesystemContentVerifier::new(
                reopened.files.clone(),
                &reopened.verifier,
                &reopened.signed,
                reopened.limits.file_bytes,
                None,
            )?;
            use crate::conversation::ContentResidencyVerifier;
            assert_eq!(reader.read(summary, 32, None)?.await?.as_ref(), b"summary");
        }
        let observations = invariants::observe(&reopened, &fixture, root).await?;
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "case:observed");
        requests::check(&reopened, &fixture).await?;
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "case:audited");
        observations
            .check(scenario)
            .map_err(|reason| Error::Invalid(reason.into()))?;
        Ok(Coverage {
            observations,
            points,
            unadmitted,
            ready_reads: schedule.ready_reads(),
            admission_submission: schedule.admission_submission(),
            admission_committed,
        })
    })
    .await;
    let result = result.map_err(|error| Error::Storage(format!("{scenario:?} cut {cut:?}, in-flight boundary {:?}, accepted model effects {} and tool effects {}: {error:?}", fixture.schedule.failure(), fixture.provider.accepted(), fixture.effect.calls.lock().unwrap().len())));
    drop(fixture);
    tracing::trace!(target: "acyclic_harness::read_profile", phase = "case:cleanup");
    result
}

fn temporary_directory(preferred: &Path) -> std::io::Result<tempfile::TempDir> {
    tempfile::tempdir_in(preferred).or_else(|_| tempfile::tempdir())
}

fn fixture_directory() -> std::io::Result<tempfile::TempDir> {
    if cfg!(target_os = "linux") {
        return temporary_directory(Path::new("/dev/shm"));
    }
    // Keep native I/O on the build volume (including Windows Dev Drives), not
    // on an unrelated system-temp filesystem. No durability setting changes.
    match std::env::current_exe()?.parent() {
        Some(parent) => temporary_directory(parent),
        None => tempfile::tempdir(),
    }
}

#[test]
fn unavailable_shared_memory_falls_back_to_the_system_temporary_directory() {
    let parent = tempfile::tempdir().unwrap();
    let missing = parent.path().join("missing");
    let directory = temporary_directory(&missing).unwrap();
    assert!(directory.path().is_dir());
    assert!(!missing.exists());
    // Broken former implementation cannot create the same fixture.
    assert!(tempfile::tempdir_in(&missing).is_err());
}

fn sweep(scenarios: &[Scenario]) -> Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let started = std::time::Instant::now();
    // Cuts are named push barriers, independent of task polling and I/O latency.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let mut schedules = Vec::new();
    for &scenario in scenarios {
        let seed = runtime.block_on(Seed::new(scenario))?;
        let observed = runtime.block_on(Box::pin(case(scenario, None, &[], Some(&seed))))?;
        let baseline = observed.points;
        coverage::check_boundaries(scenario, &baseline, observed.ready_reads)?;
        assert!(!baseline.is_empty());
        eprintln!(
            "{scenario:?}: {} deterministic cancellation points",
            baseline.len()
        );
        schedules.push((scenario, baseline, seed));
    }
    // Replays own independent filesystems. Each bounded worker polls its driver
    // and owned completions together, without cross-thread barrier handoffs.
    let mut cuts: Vec<_> = schedules
        .iter()
        .flat_map(|(scenario, baseline, seed)| {
            (1..=baseline.len()).map(move |cut| (*scenario, baseline, seed, cut))
        })
        .collect();
    coverage::permute(&mut cuts, 711);
    coverage::check_interleaved(cuts.iter().map(|(scenario, _, _, _)| *scenario))?;
    for (scenario, baseline, _) in &schedules {
        coverage::check_cuts(
            *scenario,
            baseline.len(),
            cuts.iter()
                .filter(|(candidate, _, _, _)| candidate == scenario)
                .map(|(_, _, _, cut)| *cut),
        )?;
    }
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let completed = AtomicUsize::new(0);
    let unadmitted = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..std::thread::available_parallelism()
            .unwrap()
            .get()
            .min(64)
            .min(cuts.len()))
            .map(|_| {
                scope.spawn(|| -> Result<()> {
                    let io_threads = Arc::new(AtomicUsize::new(0));
                    let started_io = io_threads.clone();
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .max_blocking_threads(1)
                        .on_thread_start(move || {
                            started_io.fetch_add(1, Ordering::Relaxed);
                        })
                        .enable_all()
                        .build()
                        .map_err(storage)?;
                    loop {
                        if failed.load(Ordering::Relaxed) {
                            return Ok(());
                        }
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&(scenario, baseline, seed, cut)) = cuts.get(index) else {
                            assert_eq!(
                                io_threads.load(Ordering::Relaxed),
                                0,
                                "replay opened a private Tokio I/O pool"
                            );
                            return Ok(());
                        };
                        let coverage = runtime
                            .block_on(Box::pin(case(scenario, Some(cut), baseline, Some(&seed))))
                            .map_err(|error| {
                                let error = Error::Storage(format!(
                                    "{scenario:?} cut {cut}/{}: {error:?}",
                                    baseline.len()
                                ));
                                eprintln!("{error:?}");
                                failed.store(true, Ordering::Relaxed);
                                error
                            })?;
                        unadmitted.fetch_add(usize::from(coverage.unadmitted), Ordering::Relaxed);
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap()?;
        }
        Ok::<_, Error>(())
    })?;
    assert_eq!(
        completed.load(Ordering::Relaxed),
        cuts.len(),
        "a replay was skipped"
    );
    assert!(
        unadmitted.load(Ordering::Relaxed) > 0,
        "HEAD/admission crash window was not covered"
    );
    eprintln!("complete replay sweep: {:?}", started.elapsed());
    Ok(())
}

#[test]
fn run_cancellation_at_every_poll_recovers_head_and_admission() -> Result<()> {
    sweep(&[
        Scenario::Text,
        Scenario::EffectAndContinuation,
        Scenario::Compaction,
    ])
}

#[test]
fn indeterminate_admission_after_submitted_local_stream_commit_reconciles() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let seed = runtime.block_on(Seed::new(Scenario::Text))?;
    let observed = runtime.block_on(case(Scenario::Text, None, &[], Some(&seed)))?;
    let baseline = observed.points;
    coverage::check_boundaries(Scenario::Text, &baseline, observed.ready_reads)?;
    let cut: usize =
        serde_json::from_slice(include_bytes!("poll_sweep/coverage/admission_cut.json"))
            .map_err(storage)?;
    assert_eq!(
        observed.admission_submission,
        Some(cut),
        "named admission submission moved"
    );
    assert_eq!(
        baseline[cut - 1],
        "task:submitted",
        "named admission cut moved"
    );
    let recovered = runtime.block_on(case(Scenario::Text, Some(cut), &baseline, Some(&seed)))?;
    assert_eq!(recovered.points.len(), cut);
    assert!(
        recovered.admission_committed,
        "submitted admission did not commit while draining"
    );
    Ok(())
}

#[test]
fn compaction_summary_is_readable_before_any_cancellation() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .finish();
    let _tracing = tracing::subscriber::set_default(subscriber);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let observed = runtime.block_on(case(Scenario::Compaction, None, &[], None))?;
    coverage::check_boundaries(Scenario::Compaction, &observed.points, observed.ready_reads)?;
    Ok(())
}
