use super::*;
use crate::filesystem::NativeProcessStopKind;
use crate::runtime::TaskSpawner as _;
use crate::runtime::TaskStateProvider;
use crate::wire::ErrorCode;
use crate::{
    conversation::{ContentGrant, ContentPublisher, VolumeOperation},
    core::{EffectGuarantee, EffectStatus, SchemaRegistry},
    effect_host::{ConversationEffectHost, TaskEffectPlan, task_effect_id},
    effects::{EffectDispatch, EffectProvider, EffectRegistry},
    filesystem::{
        BorrowedNativeVolumeBinding, FilesystemContentPublisher, FilesystemContentVerifier,
        FilesystemExecutionJournal, FilesystemHost, FilesystemTaskRuntime,
        NATIVE_PROCESS_EFFECT_KIND, NativePreparationRoot, NativeProcessAuthority,
        NativeProcessProvider, NativeProcessRequest, NativeProcessResult, NativeViewOptions,
        NativeVolumeNamespace, NativeVolumeView, workspace_ref,
    },
    interaction::InteractionResponse,
    workflow::WorkflowCommand,
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, Fs, MemoryAuthorityBackend, MemoryObjectBackend,
};

type Files<A = MemoryAuthorityBackend, O = MemoryObjectBackend> = FilesystemHost<A, O>;

type Process<P = MemoryStream> = NativeProcessProvider<P>;
type Journal<P = MemoryStream, A = MemoryAuthorityBackend, O = MemoryObjectBackend> =
    FilesystemExecutionJournal<P, A, O>;
type NativeRuntime<P = MemoryStream, A = MemoryAuthorityBackend, O = MemoryObjectBackend> =
    FilesystemTaskRuntime<P, A, O>;

fn assert_native_receipt_binding<P: StreamProvider>(
    owner: &TaskJournalOwner<P>,
    command: OperationId,
    plan: &TaskEffectPlan,
    receipt: &crate::filesystem::NativeProcessReceipt,
    status: &EffectStatus,
    observed: bool,
) -> Result<()> {
    let (task, fence) = owner.task_binding();
    let effect_id = task_effect_id(task, command, &mut None)?;
    assert_eq!(receipt.task, task);
    assert_eq!(receipt.command, command);
    assert_eq!(&receipt.original_fence, fence);
    assert_eq!(receipt.observed, observed);
    assert_eq!(
        receipt.effect,
        crate::effects::EffectObservation {
            provider: plan.provider.clone(),
            effect_id,
            attempt_id: crate::EffectAttemptId::from_bytes(effect_id.into_bytes()),
            request_digest: crate::executor::json::canonical_json_digest(&(
                &plan.provider,
                plan.guarantee,
                &plan.effect_kind,
                &plan.request,
            ), u64::MAX, &mut None)?,
            guarantee: plan.guarantee,
            status: status.clone(),
        }
    );
    Ok(())
}

struct ProcessFixture<
    P: StreamProvider = MemoryStream,
    A = MemoryAuthorityBackend,
    O = MemoryObjectBackend,
> {
    directory: tempfile::TempDir,
    task: NativeRuntime<P, A, O>,
    lease: crate::distributed::WorkLease,
    owner: Arc<TaskJournalOwner<P>>,
    files: Arc<Files<A, O>>,
    issuer: AuthorityIssuer,
    scope: Scope,
    source: VolumeRef,
    destination: VolumeRef,
    second_destination: VolumeRef,
    content: Arc<FilesystemContentVerifier<A, O>>,
    results: Arc<FilesystemContentPublisher<A, O>>,
    approvals: Arc<Journal<P, A, O>>,
    view: Arc<NativeVolumeView<A, O>>,
    preparation_root: NativePreparationRoot,
    command: OperationId,
    plan: TaskEffectPlan,
    lazy_mount: Option<
        Arc<
            acyclic_fs::LazyMount<
                A,
                O,
                acyclic_fs::NativeDemandSource,
                acyclic_fs::MemoryLazyWorkspaceStore,
            >,
        >,
    >,
    lazy_work: Option<acyclic_fs::NativeMaterializationWork>,
    project_policy: Option<crate::filesystem::FsRootProjectSourceProvider<A, O>>,
    project_generation: Option<(GenerationRef, acyclic_fs::NativeAllocation)>,
    _native_root_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<P, A, O> ProcessFixture<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn request(&self) -> Result<NativeProcessRequest> {
        crate::contract::json_from_slice(&self.content.read(&self.plan.request).await?)
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    async fn replace_request(&mut self, request: &NativeProcessRequest) -> Result<()> {
        self.plan.request = self
            .results
            .stage(
                self.command,
                "process/changed-request.json",
                &crate::contract::canonical_json_bytes(request)?,
                "application/json",
                "changed-request.json",
            )
            .await?;
        Ok(())
    }

    fn approval(&self) -> InteractionId {
        task_interaction_id(self.owner.task_binding().0, self.command)
    }

    fn authority(&self) -> NativeProcessAuthority {
        NativeProcessAuthority {
            verifier: self.issuer.verifier(&mut None).expect("verifier copy"),
            history_limits: crate::store::HistoryReadLimits {
                maximum_events: 128,
                maximum_bytes: 32 * 1024 * 1024,
            },
            content: self.content.clone(),
            result_content: self.content.clone(),
            results: self.results.clone(),
            approvals: self.approvals.clone(),
            authority_custody: None,
        }
    }

    async fn provider(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Process<P>>> {
        Process::new(
            self.owner.clone(),
            self.command,
            &self.plan,
            self.view.clone(),
            self.approval(),
            self.authority(),
            work,
        )?
        .await?
        .into_arc_with_native_work(work)
    }

    async fn inspect_project_candidate_with_native_work(
        &self,
        candidate: acyclic_fs::LazySnapshotRef,
        operation: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::filesystem::TaskSourceInspection<P, A, O>> {
        let policy = self.project_policy.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "fixture has no protected original Project allocation",
        })?;
        let generation = &self
            .project_generation
            .as_ref()
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "fixture has no protected original Project selection",
            })?
            .0;
        let (task, fence) = self.owner.task_binding();
        let context = self
            .task
            .task_context_with_native_work(task, fence, work)
            .await?;
        let owner = self
            .task
            .task_host()
            .journal_owner_with_native_work(task, fence, work)
            .await?
            .with_invocation_cancellation(work.cancellation())
            .into_native_shared(work)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let project = self.source.clone_with_native_work(work)?;
        let generation = crate::resources::GenerationRef::new(
            generation.as_resource().provider(),
            generation.as_resource().key(),
            generation.as_resource().version(),
            Some(work),
        )
        .map(|(value, custody)| (value, custody.expect("native generation custody")))?;
        let snapshot = (
            crate::source_authorization::ProjectSourceSnapshot {
                project: project.0,
                generation: generation.0,
                native_snapshot: Some(io.value_json(&candidate, 128 * 1024 * 1024)?),
            },
            project.1,
            generation.1,
        );
        let handle = owner
            .authorize_project_source_with_native_work(
                context.as_ref(),
                &self.issuer,
                policy,
                task,
                operation,
                &snapshot.0,
                1_000_000,
                128 * 1024 * 1024,
                work,
            )?
            .await?;
        let inspection = self
            .task
            .inspect_project_source_with_native_work(
                context.as_ref(),
                owner,
                handle,
                operation,
                task,
                &snapshot.0,
                work,
            )
            .await?;
        drop(snapshot);
        drop(io);
        Ok(inspection)
    }

    fn effects(&self, process: Arc<Process<P>>) -> Result<ConversationEffectHost<P>> {
        let mut registry = EffectRegistry::default().with_result_resolver(self.content.clone());
        registry.register(process, None)?;
        ConversationEffectHost::new(
            self.owner.stream(),
            self.issuer
                .verifier(&mut None)
                .expect("verifier copy")
                .audience()
                .clone(),
            self.issuer.clone(),
            self.scope.clone(),
            SchemaRegistry::new(),
            self.content.clone(),
            registry,
            None,
            None,
        )?
        .clone_with_native_work(self.view.work())
    }
}

struct NativeEffectMachine {
    identity: MachineIdentity,
    schema: Value,
}

#[derive(Clone, Copy)]
struct PreparationCommitFault {
    phase: acyclic_fs::MaterializationPhase,
    preimages: usize,
    applied: u32,
    lost_reply: bool,
}

struct ReceiptFaultStream<P = MemoryStream> {
    inner: P,
    fault: Mutex<Option<(&'static str, bool)>>,
    preparation_fault: Mutex<Option<PreparationCommitFault>>,
    cancel_observed: Mutex<Option<acyclic_fs::CancellationToken>>,
    hide_next_inspection: std::sync::atomic::AtomicBool,
}

impl<P> ReceiptFaultStream<P> {
    fn new(inner: P) -> Self {
        Self {
            inner,
            fault: Mutex::new(None),
            preparation_fault: Mutex::new(None),
            hide_next_inspection: std::sync::atomic::AtomicBool::new(false),
            cancel_observed: Mutex::new(None),
        }
    }
}

impl Default for ReceiptFaultStream {
    fn default() -> Self {
        Self::new(MemoryStream::default())
    }
}

#[async_trait::async_trait]
impl<P: StreamProvider + 'static> StreamProvider for ReceiptFaultStream<P> {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<
        Option<acyclic_stream::IdempotencyObservation>,
        acyclic_stream::StreamError,
    > {
        if self
            .hide_next_inspection
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<u64, acyclic_stream::StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        self.inner.append(request).await
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        self.inner.fork(request).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.inner.follow(path, from).await
    }
    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> std::result::Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
        self.inner.children(request).await
    }
    async fn children_page(
        &self,
        request: acyclic_stream::ChildrenPageRequest,
    ) -> std::result::Result<acyclic_stream::ChildrenPage, acyclic_stream::StreamError> {
        self.inner.children_page(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.inner.read_commit(id).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        let mut armed = self.fault.lock().await;
        let selected = armed.as_ref().is_some_and(|(kind, _)| request.mutations.iter().any(|mutation| {
            matches!(mutation, acyclic_stream::CommitMutation::Append { path, records }
                if path.as_str().starts_with("harness/v2/native-process/") && records.iter().any(|record| serde_json::from_slice::<Value>(record).is_ok_and(|value| value["kind"] == *kind)))
        }));
        let fault = if selected { armed.take() } else { None };
        drop(armed);
        let is_observed = request.mutations.iter().any(|mutation| {
            matches!(mutation, acyclic_stream::CommitMutation::Append { path, records }
                if path.as_str().starts_with("harness/v2/native-process/")
                    && records.iter().any(|record| serde_json::from_slice::<Value>(record)
                        .is_ok_and(|value| value["kind"] == "observed")))
        });
        if is_observed {
            if let Some(cancellation) = self.cancel_observed.lock().await.take() {
                cancellation.cancel();
                return Err(acyclic_stream::StreamError::Unavailable);
            }
        }
        let mut preparation = self.preparation_fault.lock().await;
        let preparation_selected = preparation.as_ref().is_some_and(|fault| {
            request.mutations.iter().any(|mutation| matches!(mutation,
                acyclic_stream::CommitMutation::Append { path, records }
                if path.as_str().starts_with("harness/v2/materialization/")
                    && records.iter().any(|bytes| serde_json::from_slice::<acyclic_fs::MaterializationJournal>(bytes)
                        .is_ok_and(|journal| journal.phase == fault.phase
                            && journal.preimages.len() == fault.preimages && journal.applied == fault.applied))
            ))
        });
        let preparation_fault = if preparation_selected {
            preparation.take()
        } else {
            None
        };
        drop(preparation);
        if fault.is_some_and(|(_, lost_reply)| !lost_reply)
            || preparation_fault.is_some_and(|fault| !fault.lost_reply)
        {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        let result = self.inner.commit(request).await?;
        if fault.is_some() || preparation_fault.is_some() {
            self.hide_next_inspection
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        Ok(result)
    }
}

struct PreparationSelection<A = MemoryAuthorityBackend, O = MemoryObjectBackend> {
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    generation: crate::resources::GenerationRef,
    path: String,
    writable: bool,
    verifier: crate::core::AuthorityVerifier,
    scope: Scope,
    publication: acyclic_fs::PublicationPermit,
}

struct PreparationFixture<P: StreamProvider> {
    _directory: tempfile::TempDir,
    task: NativeRuntime<P>,
    owner: Arc<TaskJournalOwner<P>>,
    allocation: NativePreparationRoot,
    options: NativeViewOptions,
    bindings: Vec<PreparationSelection>,
}

type PreparationView = NativeVolumeView<MemoryAuthorityBackend, MemoryObjectBackend>;

#[cfg(feature = "filesystem-local")]
#[derive(serde::Serialize, serde::Deserialize)]
struct NativeRestart {
    task: TaskId,
    fence: crate::scheduler::LeaseFence,
    scope: Scope,
    command: OperationId,
    plan: TaskEffectPlan,
    result_volume: VolumeRef,
    destination: VolumeRef,
    root: std::path::PathBuf,
}

#[cfg(feature = "filesystem-local")]
impl NativeRestart {
    fn from_fixture<P, A, O>(fixture: &ProcessFixture<P, A, O>) -> Self
    where
        P: StreamProvider + 'static,
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let (task, fence) = fixture.owner.task_binding();
        Self {
            task,
            fence: fence.clone(),
            scope: fixture.scope.clone(),
            command: fixture.command,
            plan: fixture.plan.clone(),
            result_volume: fixture.results.volume().clone(),
            destination: fixture.destination.clone(),
            root: fixture.view.manifest().options.root.clone(),
        }
    }
}

#[cfg(feature = "filesystem-local")]
type LocalFiles = Files<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
#[cfg(feature = "filesystem-local")]
type LocalNativeRuntime = NativeRuntime<
    acyclic_stream::LocalStream,
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
>;
#[cfg(feature = "filesystem-local")]
type LocalNativeProcess = Process<acyclic_stream::LocalStream>;

#[cfg(feature = "filesystem-local")]
struct LocalNativeRecovery {
    files: Arc<LocalFiles>,
    task: LocalNativeRuntime,
    owner: Arc<TaskJournalOwner<acyclic_stream::LocalStream>>,
    process: Arc<LocalNativeProcess>,
    effects: ConversationEffectHost<acyclic_stream::LocalStream>,
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn native_host_child() -> Result<()> {
    let Some(storage) = std::env::var_os("NATIVE_HOST_STORAGE") else {
        return Ok(());
    };
    let storage = std::path::PathBuf::from(storage);
    let filesystem = Fs::local(acyclic_fs::LocalOptions::new(storage.join("filesystem")))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let auxiliary = Fs::local(acyclic_fs::LocalOptions::new(storage.join("auxiliary")))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(storage.join("coordinator"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let directory =
        tempfile::tempdir_in(&storage).map_err(|error| Error::Storage(error.to_string()))?;
    let fixture = process_fixture_with_filesystems(
        stream,
        directory,
        filesystem,
        auxiliary,
        "host-death",
        true,
        None,
    )
    .await?;
    std::fs::write(
        storage.join("restart.json"),
        crate::contract::canonical_json_bytes(&NativeRestart::from_fixture(&fixture))?,
    )
    .map_err(|error| Error::Storage(error.to_string()))?;
    fixture
        .effects(fixture.provider(fixture.view.work()).await?)?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    Err(Error::Storage(
        "host-death controller failed to terminate host".into(),
    ))
}

impl ResumableMachine for NativeEffectMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        if state == &serde_json::json!(0) {
            let command: WorkflowCommand = serde_json::from_value(input.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            Ok(MachineTransition {
                state: serde_json::json!(1),
                commands: vec![command],
                status: MachineStatus::Suspended,
            })
        } else {
            Ok(MachineTransition {
                state: serde_json::json!(2),
                commands: Vec::new(),
                status: MachineStatus::Completed {
                    value: input.clone(),
                },
            })
        }
    }
}

#[test]
fn native_process_child() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let Ok(mode) = std::env::var("NATIVE_TEST_MODE") else {
        return Ok(());
    };
    assert!(std::env::var_os("CARGO_MANIFEST_DIR").is_none());
    if mode == "grandchild" {
        std::thread::sleep(std::time::Duration::from_secs(2));
        std::fs::write("escaped", b"escaped")?;
        return Ok(());
    }
    assert_eq!(std::env::var("NATIVE_TEST_TOKEN")?, "approved-env");
    if mode == "host-death" {
        std::fs::write("host-death-ready", b"ready")?;
        std::thread::sleep(std::time::Duration::from_secs(3));
    }
    if mode == "wait" || mode == "timeout" {
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "durable_host::tests::native_process_tests::native_process_child",
                "--nocapture",
            ])
            .env_clear()
            .env("NATIVE_TEST_MODE", "grandchild")
            .spawn()?;
        std::fs::write("ready", b"ready")?;
        std::thread::sleep(std::time::Duration::from_secs(30));
    } else {
        let input = if mode.starts_with("stdin-") {
            use std::io::Read as _;
            let mut input = Vec::new();
            std::io::stdin().read_to_end(&mut input)?;
            input
        } else {
            std::fs::read("src/input.txt")?
        };
        std::fs::write("dst/output.txt", input)?;
        if mode == "review-test" {
            std::fs::create_dir("dst/tests")?;
            std::fs::write("dst/tests/generated.txt", b"isolated test")?;
            assert_eq!(std::fs::read("dst/input.txt")?, b"pinned input");
            assert_eq!(std::fs::read("dst/tests/generated.txt")?, b"isolated test");
            assert!(!std::path::Path::new("src/tests/generated.txt").exists());
        }
        std::fs::write("dst/auxiliary.txt", std::fs::read("aux/input.txt")?)?;
        println!("approved stdout");
        eprintln!("approved stderr");
        if mode == "pause-write" || mode == "partial-publication" {
            if mode == "partial-publication" {
                std::fs::write("second-dst/output.txt", b"second edit")?;
            }
            std::fs::write("publication-ready", b"ready")?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !std::path::Path::new("publication-release").exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "publication race controller did not release child"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        }
        if mode == "flood" {
            use std::io::Write as _;
            std::io::stdout().write_all(&[255; 65_536])?;
        }
        if mode == "nonzero" {
            std::process::exit(22);
        }
    }
    Ok(())
}

mod authority;
mod execution;
mod fixture;
mod preparation;
mod receipts;
mod restart;
mod view;
use fixture::*;
