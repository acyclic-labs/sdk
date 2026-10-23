//! Hosting-contract adapter. External hosts implement `ActorRuntime` and `Volume`.
use crate::{CaseObservation, TestResult};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs};
use acyclic_harness::{
    AgentId, Error, IdempotencyKey, OperationId, Result,
    conversation::FileRef,
    filesystem::MemoryHarnessStorage,
    package::{
        ActorPackage, ActorRuntime, Bindings, EffectHost, Event, Manifest, Placement,
        VolumePlacement,
        in_process::{AdmissionAction, AdmissionPolicy, InProcessRuntime, effect_result_operation},
    },
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
    },
};
use acyclic_stream::{BoxProviderFuture, ProviderPlatform, StreamProvider};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

/// HTTP bridge for remote Actors and fs contract implementations.
pub mod remote;
mod supervised;
pub use supervised::Supervised;

/// Explicit host capabilities. Native fixture drivers require `local-driver:<name>`.
pub trait Substrate: ProviderPlatform {
    /// Returns explicitly implemented fixture capabilities.
    fn capabilities(&self) -> BTreeSet<String>;
    /// Runs expectation-free fixture input and returns raw observations.
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>>;
}

/// Public fs binding for immutable fixture and result publications.
pub trait Volume: ProviderPlatform {
    /// Publishes immutable JSON under a stable operation identity.
    fn put(&self, operation: OperationId, value: Value) -> BoxProviderFuture<'_, Result<FileRef>>;
    /// Loads and verifies an immutable reference.
    fn get(&self, file: FileRef) -> BoxProviderFuture<'_, Result<Value>>;
    /// Reconciles an earlier publication by stable operation identity.
    fn lookup(&self, operation: OperationId) -> BoxProviderFuture<'_, Result<Option<FileRef>>>;
}

impl<P, A, O> Volume for MemoryHarnessStorage<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn put(&self, operation: OperationId, value: Value) -> BoxProviderFuture<'_, Result<FileRef>> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(&value).map_err(codec)?;
            self.stage(
                operation,
                &format!("fixtures/{operation}.json"),
                &bytes,
                "application/json",
                "fixture.json",
            )
            .await
        })
    }
    fn get(&self, file: FileRef) -> BoxProviderFuture<'_, Result<Value>> {
        Box::pin(async move { serde_json::from_slice(&self.read(&file).await?).map_err(codec) })
    }
    fn lookup(&self, operation: OperationId) -> BoxProviderFuture<'_, Result<Option<FileRef>>> {
        Box::pin(async move {
            match self
                .read_delegated_path(
                    self.owner_scope(),
                    "",
                    &format!("fixtures/{operation}.json"),
                    None,
                )
                .await
            {
                Ok((file, _)) => Ok(Some(file)),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }
}
fn codec(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value> {
    value
        .get(name)
        .ok_or_else(|| Error::Invalid(format!("missing scenario field {name}")))
}

const FIXTURE_SOURCES: &[(&str, &str)] = &[
    ("substrate", include_str!("substrate.rs")),
    ("dispatch", include_str!("lib.rs")),
    ("execution_adapter", include_str!("execution.rs")),
    (
        "executor_observation",
        include_str!("execution/observation.rs"),
    ),
    (
        "executor",
        include_str!("../../harness/tests/e2e/execution.rs"),
    ),
    (
        "scenario",
        include_str!("../../harness/tests/e2e/scenario.rs"),
    ),
    (
        "expectations",
        include_str!("../../harness/tests/e2e/expectations.rs"),
    ),
    (
        "discovery",
        include_str!("../../harness/tests/support/discovery.rs"),
    ),
    (
        "scenario_data",
        include_str!("../../harness/tests/support/scenario_data.rs"),
    ),
    (
        "ordered_matcher",
        include_str!("../../harness/tests/support/ordered_matcher.rs"),
    ),
    (
        "scenario_matchers",
        include_str!("../../harness/tests/support/scenario_matchers.rs"),
    ),
    (
        "composer",
        include_str!("../../harness/tests/support/composer.rs"),
    ),
    ("composer_observation", include_str!("composer.rs")),
];

fn fixture_digest(sources: &[(&str, &str)]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for (name, source) in sources {
        hasher.update(name.as_bytes());
        hasher.update(&[0]);
        hasher.update(blake3::hash(source.as_bytes()).as_bytes());
    }
    *hasher.finalize().as_bytes()
}

/// Pinned package shared by reference and external runtimes. The deterministic
/// handler emits a compute command; all executor I/O stays in the effect host.
#[derive(Clone)]
pub struct HarnessPackage {
    manifest: Manifest,
    schema: Value,
}
impl HarnessPackage {
    /// Pins the fixture implementation and its explicit resource bindings.
    pub fn new(bindings: Bindings) -> Self {
        Self {
            manifest: Manifest {
                agent: MachineIdentity {
                    name: "harness.scenario".into(),
                    version: "1".into(),
                    digest: fixture_digest(FIXTURE_SOURCES),
                },
                effects: vec!["harness.scenario.compute".into()],
                bindings,
            },
            schema: json!({"type":"object"}),
        }
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn pinned_parsing_and_discovery_changes_change_package_identity() -> TestResult {
        let expected = HarnessPackage::new(fixture_bindings())
            .manifest
            .agent
            .digest;
        for critical in [
            "scenario",
            "discovery",
            "dispatch",
            "composer",
            "executor_observation",
        ] {
            let position = FIXTURE_SOURCES
                .iter()
                .position(|(name, _)| *name == critical)
                .ok_or_else(|| format!("missing pinned fixture source {critical}"))?;
            let mut changed = FIXTURE_SOURCES.to_vec();
            let (name, source) = changed.get(position).copied().ok_or("missing source")?;
            let mutation = format!("{source}\n// changed implementation\n");
            *changed.get_mut(position).ok_or("missing mutation slot")? = (name, &mutation);
            assert_ne!(
                expected,
                fixture_digest(&changed),
                "unbound source {critical}"
            );
        }
        Ok(())
    }
}
impl ActorPackage for HarnessPackage {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn machine(&self) -> Arc<dyn ResumableMachine> {
        Arc::new(self.clone())
    }
}
impl ResumableMachine for HarnessPackage {
    fn identity(&self) -> &MachineIdentity {
        &self.manifest.agent
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        let command = field(input, "command")?;
        let _: WorkflowCommand = serde_json::from_value(command.clone()).map_err(codec)?;
        Ok(json!({"command":command,"started":false}))
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        if field(state, "started")?.as_bool() == Some(false) && input == &json!({"start":true}) {
            return Ok(MachineTransition {
                state: json!({"started":true}),
                commands: vec![
                    serde_json::from_value(field(state, "command")?.clone()).map_err(codec)?,
                ],
                status: MachineStatus::Suspended,
            });
        }
        if field(state, "started")?.as_bool() != Some(true) {
            return Err(Error::Invalid("unexpected scenario input".into()));
        }
        let _: FileRef = serde_json::from_value(field(input, "output")?.clone()).map_err(codec)?;
        Ok(MachineTransition {
            state: input.clone(),
            commands: vec![],
            status: MachineStatus::Completed {
                value: input.clone(),
            },
        })
    }
}

/// Runner side of the package protocol: only the public Actors and fs contracts.
pub struct Actors<R, V> {
    /// Public Actors runtime implementation.
    pub runtime: R,
    /// Resolved fs publication binding shared with the effect host.
    pub volume: V,
    /// Package deployment bindings.
    pub bindings: Bindings,
}
impl<R: ActorRuntime, V: Volume> Substrate for Actors<R, V> {
    fn capabilities(&self) -> BTreeSet<String> {
        BTreeSet::from(["executor".into(), "composer".into()])
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(async move {
            let package = Arc::new(HarnessPackage::new(self.bindings.clone()));
            let identity = package.manifest().agent.clone();
            self.runtime.install(package).await?;
            let operation = OperationId::new();
            let placement = Placement {
                actor: AgentId::new(),
                node: "conformance".into(),
                epoch: 1,
            };
            let payload = self
                .volume
                .put(operation, json!({"recipe":recipe,"paths":paths}))
                .await?;
            let command = WorkflowCommand {
                operation_id: OperationId::new(),
                kind: "harness.scenario.compute".into(),
                payload,
            };
            let placed = self
                .runtime
                .place(placement.clone(), json!({"command":command}))
                .await?;
            validate_snapshot(&placed, &placement, &identity, 0, "placement")?;
            if placed.status != MachineStatus::Suspended || !placed.pending.is_empty() {
                return Err("host changed initial scenario state".into());
            }
            let event = Event {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new(format!("start:{operation}"))?,
                input: json!({"start":true}),
            };
            let admitted = self
                .runtime
                .deliver(placement.clone(), event.clone())
                .await?;
            validate_snapshot(&admitted, &placement, &identity, 1, "admission")?;
            if admitted.pending != [command] || admitted.status != MachineStatus::Suspended {
                return Err("host lost scenario admission or outbox".into());
            }
            if self.runtime.deliver(placement.clone(), event).await? != admitted {
                return Err("host changed an identical admission retry".into());
            }
            let completed = self.runtime.recover(placement.clone()).await?;
            validate_snapshot(&completed, &placement, &identity, 2, "completion")?;
            if !completed.pending.is_empty()
                || !matches!(&completed.status, MachineStatus::Completed { value } if value == &completed.checkpoint.state)
            {
                return Err("host lost scenario completion observation".into());
            }
            if self.runtime.recover(placement).await? != completed {
                return Err("host changed a settled scenario on recovery".into());
            }
            let output: FileRef =
                serde_json::from_value(field(&completed.checkpoint.state, "output")?.clone())?;
            Ok(serde_json::from_value(self.volume.get(output).await?)?)
        })
    }
}

fn validate_snapshot(
    snapshot: &acyclic_harness::package::Snapshot,
    placement: &Placement,
    identity: &MachineIdentity,
    revision: u64,
    phase: &str,
) -> TestResult {
    if &snapshot.placement != placement
        || &snapshot.checkpoint.machine != identity
        || snapshot.checkpoint.revision != revision
        || snapshot.consumed != revision
        || snapshot.cancelled
    {
        return Err(format!("host lost scenario {phase} observation").into());
    }
    Ok(())
}

/// Fixture-only policy: no ambient credentials, network or user home access.
pub struct FixturePolicy;
impl AdmissionPolicy for FixturePolicy {
    fn authorize<'a>(
        &'a self,
        _: AdmissionAction,
        _: &'a Manifest,
        _: Option<&'a Placement>,
        _: Option<&'a Event>,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// Provider-bound executor fixture installed by the effect host.
pub trait ExecutorFixture: ProviderPlatform {
    /// Runs a fixture against the host's real providers and returns observations.
    fn collect(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>>;
}

/// Explicit provider bundle for executor journals, files and context discovery.
pub struct Providers<P, A, O> {
    /// Runtime-resolved Streams provider.
    pub stream: Arc<P>,
    /// Runtime-resolved filesystem providers.
    pub filesystem: Fs<A, O>,
}
impl<P, A, O> ExecutorFixture for Providers<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn collect(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(crate::collect_on(
            recipe,
            paths,
            self.filesystem.clone(),
            self.stream.clone(),
        ))
    }
}

/// Reference compute binding. Results are retained by command ID for retry;
/// fencing and dispatch share a lock, so a closed placement cannot start work.
pub struct FixtureEffects<V> {
    /// Resolved fixture volume, also used to reconcile completed commands.
    pub volume: Arc<V>,
    /// Harness executor bound to the same host's resolved public providers.
    pub executor: Arc<dyn ExecutorFixture>,
    state: tokio::sync::Mutex<EffectState>,
    fenced: Mutex<BTreeSet<(AgentId, u64)>>,
}
#[derive(Default)]
struct EffectState {
    results: std::collections::BTreeMap<OperationId, Event>,
}
impl<V> FixtureEffects<V> {
    /// Creates an isolated fixture compute binding with explicit fs custody.
    pub fn new(volume: Arc<V>, executor: Arc<dyn ExecutorFixture>) -> Self {
        Self {
            volume,
            executor,
            state: tokio::sync::Mutex::new(EffectState::default()),
            fenced: Mutex::new(BTreeSet::new()),
        }
    }
}
impl<V: Volume + 'static> EffectHost for FixtureEffects<V> {
    fn execute(
        &self,
        placement: Placement,
        command: WorkflowCommand,
    ) -> BoxProviderFuture<'_, Result<Event>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if self
                .fenced
                .lock()
                .map_err(|_| Error::Storage("fixture fence poisoned".into()))?
                .contains(&(placement.actor, placement.epoch))
            {
                return Err(Error::Conflict("fixture placement fenced".into()));
            }
            if let Some(event) = state.results.get(&command.operation_id) {
                return Ok(event.clone());
            }
            if command.kind != "harness.scenario.compute" {
                return Err(Error::Unsupported("unknown fixture effect".into()));
            }
            let result_operation = effect_result_operation(command.operation_id);
            let output = if let Some(output) = self.volume.lookup(result_operation).await? {
                output
            } else {
                let value = self.volume.get(command.payload).await?;
                let paths =
                    serde_json::from_value(field(&value, "paths")?.clone()).map_err(codec)?;
                let observed = self
                    .executor
                    .collect(field(&value, "recipe")?.clone(), paths)
                    .await
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                self.volume
                    .put(
                        result_operation,
                        serde_json::to_value(observed).map_err(codec)?,
                    )
                    .await?
            };
            let event = Event {
                operation_id: result_operation,
                idempotency_key: IdempotencyKey::new(format!("result:{result_operation}"))?,
                input: json!({"output":output}),
            };
            state.results.insert(command.operation_id, event.clone());
            Ok(event)
        })
    }
    fn fence(&self, placement: Placement) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            let _drain = self.state.lock().await;
            self.fenced
                .lock()
                .map_err(|_| Error::Storage("fixture fence poisoned".into()))?
                .insert((placement.actor, placement.epoch));
            Ok(())
        })
    }
}
impl<V: Volume + ?Sized> Volume for Arc<V> {
    fn put(&self, operation: OperationId, value: Value) -> BoxProviderFuture<'_, Result<FileRef>> {
        (**self).put(operation, value)
    }
    fn get(&self, file: FileRef) -> BoxProviderFuture<'_, Result<Value>> {
        (**self).get(file)
    }
    fn lookup(&self, operation: OperationId) -> BoxProviderFuture<'_, Result<Option<FileRef>>> {
        (**self).lookup(operation)
    }
}

/// Credential-free resource names for isolated reference deployments.
pub fn fixture_bindings() -> Bindings {
    Bindings {
        streams: "conformance".into(),
        volume: "fixtures".into(),
        placement: VolumePlacement::Portable,
        model_https: "https://fixture.invalid".into(),
        model_secret: "unused-fixture".into(),
        compute: "scenario-executor".into(),
    }
}

/// Fresh provider lifetime per case, with a real durable Streams outbox.
pub struct InProcess;
impl Substrate for InProcess {
    fn capabilities(&self) -> BTreeSet<String> {
        BTreeSet::from(["executor".into(), "composer".into()])
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(async move {
            let directory = tempfile::tempdir()?;
            let stream = Arc::new(
                acyclic_stream::LocalStream::open(directory.path(), Default::default()).await?,
            );
            let filesystem = Fs::memory();
            let volume = Arc::new(
                MemoryHarnessStorage::from_providers(
                    AgentId::new(),
                    1_048_576,
                    filesystem.clone(),
                    stream.clone(),
                )
                .await?,
            );
            let bindings = fixture_bindings();
            let executor = Arc::new(Providers {
                stream: stream.clone(),
                filesystem,
            });
            let runtime = InProcessRuntime::new(
                stream,
                bindings.clone(),
                Arc::new(FixturePolicy),
                Arc::new(FixtureEffects::new(volume.clone(), executor)),
            );
            Actors {
                runtime: Supervised::new(runtime),
                volume,
                bindings,
            }
            .execute(recipe, paths)
            .await
        })
    }
}

/// Direct executor baseline, retaining exactly the same fixture and oracles.
pub struct Local;
impl Substrate for Local {
    fn capabilities(&self) -> BTreeSet<String> {
        BTreeSet::from(["executor".into(), "composer".into()])
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(crate::collect_on(
            recipe,
            paths,
            Fs::memory(),
            Arc::new(acyclic_stream::MemoryStream::default()),
        ))
    }
}
