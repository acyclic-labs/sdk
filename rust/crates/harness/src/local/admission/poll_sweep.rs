//! Replay the real run state machine over in-process filesystem/stream providers.
use super::*;
use crate::{
    context::CompactionPolicy,
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
mod requests;
mod seed;
use seed::Seed;

type Harness = LocalHarness;
const PROMPT: &str = "recover the original run input";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    Text,
    EffectAndContinuation,
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
}

impl Fixture {
    async fn new(scenario: Scenario, seed: Option<&Seed>) -> Result<Self> {
        let (schedule, events) = Schedule::new();
        let directory = if cfg!(target_os = "linux") {
            temporary_directory(Path::new("/dev/shm"))
        } else {
            tempfile::tempdir()
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
            volume("project".into())?,
            volume("scratch".into())?,
            volume("journal".into())?,
            volume("tasks".into())?,
        ];
        let root = directory.path().join("filesystem");
        if let Some(seed) = seed {
            seed.restore(&root)?;
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
        };
        Ok(Self {
            directory,
            schedule: schedule.clone(),
            events: RefCell::new(events),
            issuer: AuthorityIssuer::new(
                "acyclic.local",
                [7; 32],
                Authority {
                    kind: AggregateKind::Task,
                    id: TaskId::from_bytes([2; 16]).to_string(),
                },
            ),
            volumes,
            provider: Arc::new(ModelEffects::new(turns, schedule.clone())),
            effect: Arc::new(Effect {
                schedule,
                calls: Default::default(),
            }),
            scenario,
        })
    }

    async fn open(&self) -> Result<Harness> {
        // Reconstruct the host and discard all filesystem/runtime caches. Only
        // the two provider states and the external effect receipts survive.
        let fs = Fs::local(LocalOptions::new(self.directory.path().join("filesystem")))
            .await
            .map_err(storage)?;
        let stream = Arc::new(
            LocalStream::open_with_clock(
                self.directory.path().join("tasks"),
                Default::default(),
                Arc::new(Clock),
            )
            .await
            .map_err(storage)?,
        );
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
        let publisher = FilesystemContentPublisher::new(
            files.clone(),
            &self.volumes[2],
            &verifier,
            &signed,
            Limits::default().file_bytes,
            None,
        )?;
        let bytes = crate::context::ContextManifest::new(Default::default(), Vec::new(), &tools)?
            .encode(Limits::default())?;
        let digest = blake3::hash(&bytes);
        let context_manifest = publisher
            .stage_once(
                OperationId::from_bytes(digest.as_bytes()[..16].try_into().unwrap()),
                &format!("inputs/context/{digest}.json"),
                &bytes,
                "application/json",
                "context-manifest.json",
                None,
            )?
            .await?
            .0;
        Ok(LocalHarness {
            task_files,
            files: files.clone(),
            runtime,
            project: self.volumes[0].clone(),
            scratch: self.volumes[1].clone(),
            journal: self.volumes[2].clone(),
            verifier,
            signed,
            model: Model::new(
                "scripted".into(),
                "test".into(),
                "1".into(),
                serde_json::Value::Null,
            )?,
            provider: self.provider.clone(),
            context_manifest: Some(context_manifest),
            context_read_observer: None,
            #[cfg(feature = "hooks")]
            hooks: crate::hooks::Hooks::default(),
            limits: Limits::default(),
            compaction: CompactionPolicy::Disabled,
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

    fn assert_effects(&self) {
        let (models, tools) = match self.scenario {
            Scenario::Text => (1, 0),
            Scenario::EffectAndContinuation => (3, 1),
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
    let fixture = Fixture::new(scenario, seed).await?;
    let result: Result<Coverage> = Box::pin(async {
        let harness = fixture.open().await?;
        fixture.initialize(&harness).await?;
        let schedule = &fixture.schedule;
        schedule.start();
        let (exit, hit) = drive(
            harness.run(PROMPT),
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
            let output = reopened.run(PROMPT).await?;
            (
                crate::runtime::task_id_for_admission(
                    reopened.current_turn().await?.0.unwrap().root,
                ),
                output,
            )
        };
        assert_eq!(output.text, "done");
        fixture.assert_effects();
        for _ in 0..2 {
            assert_eq!(reopened.resume(root).await?.text, "done");
            fixture.assert_effects();
        }
        drop(reopened);
        let reopened = fixture.open().await?;
        assert_eq!(reopened.resume(root).await?.text, "done");
        fixture.assert_effects();
        let observations = invariants::observe(&reopened, &fixture, root).await?;
        requests::check(&reopened, &fixture).await?;
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
    result.map_err(|error| Error::Storage(format!("{scenario:?} cut {cut:?}, in-flight boundary {:?}, accepted model effects {} and tool effects {}: {error:?}", fixture.schedule.failure(), fixture.provider.accepted(), fixture.effect.calls.lock().unwrap().len())))
}

fn temporary_directory(preferred: &Path) -> std::io::Result<tempfile::TempDir> {
    tempfile::tempdir_in(preferred).or_else(|_| tempfile::tempdir())
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
    let seed = runtime.block_on(Seed::new())?;
    let mut schedules = Vec::new();
    for &scenario in scenarios {
        let observed = runtime.block_on(Box::pin(case(scenario, None, &[], Some(&seed))))?;
        let baseline = observed.points;
        coverage::check_boundaries(scenario, &baseline, observed.ready_reads)?;
        assert!(!baseline.is_empty());
        eprintln!(
            "{scenario:?}: {} deterministic cancellation points",
            baseline.len()
        );
        schedules.push((scenario, baseline));
    }
    // Replays own independent filesystems. Each bounded worker polls its driver
    // and owned completions together, without cross-thread barrier handoffs.
    let cuts: Vec<_> = schedules
        .iter()
        .flat_map(|(scenario, baseline)| {
            (1..=baseline.len()).map(move |cut| (*scenario, baseline, cut))
        })
        .collect();
    for (scenario, baseline) in &schedules {
        coverage::check_cuts(
            *scenario,
            baseline.len(),
            cuts.iter()
                .filter(|(candidate, _, _)| candidate == scenario)
                .map(|(_, _, cut)| *cut),
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
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(storage)?;
                    loop {
                        if failed.load(Ordering::Relaxed) {
                            return Ok(());
                        }
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&(scenario, baseline, cut)) = cuts.get(index) else {
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
    sweep(&[Scenario::Text, Scenario::EffectAndContinuation])
}

#[test]
fn indeterminate_admission_after_submitted_local_stream_commit_reconciles() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let observed = runtime.block_on(case(Scenario::Text, None, &[], None))?;
    let baseline = observed.points;
    coverage::check_boundaries(Scenario::Text, &baseline, observed.ready_reads)?;
    let cut: usize = serde_json::from_slice(
        &std::fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/local/admission/poll_sweep/coverage/admission_cut.json"),
        )
        .map_err(storage)?,
    )
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
    let recovered = runtime.block_on(case(Scenario::Text, Some(cut), &baseline, None))?;
    assert_eq!(recovered.points.len(), cut);
    assert!(
        recovered.admission_committed,
        "submitted admission did not commit while draining"
    );
    Ok(())
}
