use super::*;
use crate::filesystem::NativeProcessStopKind;
use crate::{
    conversation::{ContentGrant, ContentPublisher, VolumeOperation},
    core::{EffectGuarantee, EffectStatus, SchemaRegistry},
    effect_host::{ConversationEffectHost, TaskEffectPlan, task_effect_id},
    effects::{EffectDispatch, EffectProvider, EffectRegistry},
    filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
        FilesystemHost, FilesystemTaskRuntime, NATIVE_PROCESS_EFFECT_KIND, NativeProcessAuthority,
        NativeProcessProvider, NativeProcessRequest, NativeProcessResult, NativeViewOptions,
        NativePreparationRoot, BorrowedNativeVolumeBinding, NativeVolumeNamespace, NativeVolumeView, workspace_ref,
    },
    interaction::InteractionResponse,
    workflow::WorkflowCommand,
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, Fs, MemoryAuthorityBackend, MemoryObjectBackend,
};

#[tokio::test]
async fn native_owner_guard_exhausts_phase_cancel_and_fence_conditions() -> Result<()> {
    use crate::scheduler::{OperationPhase, require_execution_owner};
    let fixture = process_fixture("write", true).await?;
    let operation = fixture
        .owner
        .host
        .coordinator
        .lock()
        .await
        .scheduler()
        .operation(fixture.owner.operation_id())
        .cloned()
        .ok_or_else(|| Error::NotFound("admitted native operation".into()))?;
    let (_, original_fence) = fixture.owner.task_binding();
    let mut checked = 0;
    for phase in [
        OperationPhase::WaitingForDependencies,
        OperationPhase::WaitingForCapacity,
        OperationPhase::Admitted,
        OperationPhase::Running,
        OperationPhase::WaitingForChildren,
        OperationPhase::Suspended,
        OperationPhase::Reconciling,
        OperationPhase::Terminal,
    ] {
        for cancelled in [false, true] {
            for settlement in [false, true] {
                for reserved in [false, true] {
                    for same_id in [false, true] {
                        for same_placement in [false, true] {
                            let mut state = operation.clone();
                            state.phase = phase;
                            state.cancellation_requested = cancelled;
                            if !reserved {
                                state.reservation = None;
                            }
                            let mut fence = original_fence.clone();
                            if !same_id {
                                fence.reservation_id.push_str("-stale");
                            }
                            if !same_placement {
                                fence.placement.push_str("-foreign");
                            }
                            let phase_allowed = match phase {
                                OperationPhase::Running => true,
                                OperationPhase::Reconciling => settlement,
                                _ => false,
                            };
                            let expected = phase_allowed
                                && reserved
                                && same_id
                                && same_placement
                                && (!cancelled || settlement);
                            assert_eq!(
                                require_execution_owner(&state, &fence, settlement).is_ok(),
                                expected,
                                "{phase:?}, cancelled={cancelled}, settlement={settlement}, reserved={reserved}, same_id={same_id}, same_placement={same_placement}"
                            );
                            checked += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(checked, 256);
    // This calls the same guard used by TaskJournalOwner; no process has run.
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    Ok(())
}

type Files<A = MemoryAuthorityBackend, O = MemoryObjectBackend> = FilesystemHost<A, O>;

#[tokio::test]
async fn result_storage_cannot_mutate_a_pinned_native_view() -> Result<()> {
    let fixture = process_fixture("write", true).await?;
    let mut authority = fixture.authority();
    authority.results = Arc::new(FilesystemContentPublisher::new(
        fixture.files.clone(),
        fixture.destination.clone(),
        &fixture.issuer.verifier(),
        &fixture.scope,
        65_536,
    )?);
    assert!(matches!(
        Process::new(
            fixture.owner.clone(),
            fixture.command,
            fixture.plan.clone(),
            fixture.view.clone(),
            fixture.approval,
            authority,
        )
        .await,
        Err(Error::Unsupported(_))
    ));
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert!(
        fixture
            .files
            .read(&destination, None, "/native-process/result.json", 64)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn readonly_schema_storage_does_not_require_result_write_authority() -> Result<()> {
    let mut fixture = process_fixture("write", true).await?;
    let setup = fixture.issuer.root_for_agent(
        AgentId::from_bytes([61; 16]),
        "schema-setup",
        Capabilities::new([fixture.source.capability(VolumeOperation::Write)?]),
    );
    let grant = ContentGrant::verify(
        &fixture.issuer.verifier(),
        &setup,
        &fixture.source,
        VolumeOperation::Write,
    )?;
    fixture.plan.result_schema = fixture
        .files
        .put_content(
            &fixture.source,
            &grant,
            "schema.json",
            b"{\"type\":\"object\"}",
            "application/schema+json",
            "schema.json",
            128,
            &IdempotencyKey::new("readonly-native-schema")?,
        )
        .await?;
    assert!(
        fixture
            .owner
            .require_volume_grant(&fixture.source, VolumeOperation::Write)
            .is_err()
    );
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name()?,
    )?;
    let before = fixture.files.resolve(&source).await?.generation;
    let status = fixture
        .effects(fixture.provider().await?)?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = status else {
        return Err(Error::Storage(format!(
            "readonly schema did not complete: {status:?}"
        )));
    };
    assert_eq!(result.volume(), fixture.results.volume());
    assert_ne!(result.volume(), fixture.plan.result_schema.volume());
    assert_eq!(fixture.files.resolve(&source).await?.generation, before);
    Ok(())
}
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
    let effect_id = task_effect_id(task, command)?;
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
            request_digest: crate::contract::canonical_json_digest(&(
                &plan.provider,
                plan.guarantee,
                &plan.effect_kind,
                &plan.request,
            ))?,
            guarantee: plan.guarantee,
            status: status.clone(),
        }
    );
    Ok(())
}

#[tokio::test]
async fn original_native_prepared_binding_preserves_approval_and_cancellation_before_launch() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        process_work_budget(1_000_000_000)?, &cancellation,
    ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let fixture = process_fixture_with_native_work(
        StreamClient::new(Arc::new(MemoryStream::default())), "write", &work,
    ).await?;
    assert!(fixture.view.original_native_work().is_some_and(|original| original.shares_ledger(&work)));
    let foreign_cancellation = acyclic_fs::CancellationToken::new();
    let foreign = acyclic_fs::NativeMaterializationWork::new(
        process_work_budget(1_000_000_000)?, &foreign_cancellation,
    ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let foreign_prefix = foreign.receipt();
    let original_prefix = work.receipt();
    assert!(matches!(Process::new_with_native_work(
        fixture.owner.clone(), fixture.command, &fixture.plan, fixture.view.clone(),
        fixture.approval, fixture.authority(), &foreign,
    ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, .. })));
    assert_eq!(foreign.receipt(), foreign_prefix);
    assert_eq!(work.receipt(), original_prefix);
    let request = fixture.request().await?;
    assert_eq!(
        request.approval_digest_with_native_work(fixture.owner.task_binding().0, fixture.command, &work)?,
        request.approval_digest(fixture.owner.task_binding().0, fixture.command)?,
    );
    let process = fixture.provider_with_native_work(&work).await?;
    let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
    let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
    let dispatch = EffectDispatch {
        provider: fixture.plan.provider.clone(), effect_id: effect, attempt_id: attempt,
        effect_kind: fixture.plan.effect_kind.clone(), request: fixture.plan.request.clone(),
        guarantee: fixture.plan.guarantee, request_digest: [0; 32],
    };
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(process.dispatch_with_native_work(dispatch, &work),
        Err(Error::NativeWork { .. })));
    assert_eq!(work.receipt(), before);
    assert!(fixture.view.operation_cancellation().is_cancelled());
    assert!(!fixture.view.manifest().options.root.join("dst/output.txt").exists());
    assert_eq!(fixture.owner.stream().bounds(
        &format!("harness/v2/native-process/{attempt}"),
    ).await?.tail, 0);
    Ok(())
}

#[tokio::test]
async fn original_native_success_hands_effect_and_receipt_custody_through_cancel_and_reconcile() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        process_work_budget(1_000_000_000)?, &cancellation,
    ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let fixture = process_fixture_with_native_work(
        StreamClient::new(Arc::new(MemoryStream::default())), "write", &work,
    ).await?;
    let process = fixture.provider_with_native_work(&work).await?;
    let effects = fixture.effects(process.clone())?;
    let (status, status_custody) = effects.run_task_effect_with_native_work(
        &fixture.owner, fixture.command, &fixture.plan, &work,
    ).await?;
    assert!(matches!(status, EffectStatus::Succeeded { .. }));
    assert!(status_custody.work().shares_ledger(&work));
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(fixture.owner.task_binding().0, fixture.command)?.into_bytes());
    assert_eq!(fixture.owner.stream().bounds(
        &format!("harness/v2/native-process/{attempt}"),
    ).await?.tail, 2);
    let query_prefix = work.receipt();
    let receipt = process.receipt_with_native_work(attempt, &work).await.unwrap().unwrap();
    assert_native_receipt_binding(&fixture.owner, fixture.command, &fixture.plan, &receipt, &status, true)?;
    let (observed, observed_custody) = process.reconcile_with_native_work(attempt, &work)?
        .await?.unwrap();
    assert_eq!(observed.status, status);
    assert!(observed_custody.work().shares_ledger(&work));
    assert_eq!(fixture.owner.stream().bounds(
        &format!("harness/v2/native-process/{attempt}"),
    ).await?.tail, 2);
    let (effect, receipt_custody) = receipt.into_effect_with_native_custody().unwrap();
    let query_completed = work.receipt();
    assert_eq!(query_completed.authority_records_appended, query_prefix.authority_records_appended);
    assert_eq!(query_completed.backend_write_operations, query_prefix.backend_write_operations);
    assert_eq!(query_completed.materializations, query_prefix.materializations);
    assert_eq!(effect, observed);
    assert!(receipt_custody.work().shares_ledger(&work));
    let before = work.receipt();
    let live = work.live_allocation_bytes();
    cancellation.cancel();
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), live);
    drop(effect);
    drop(receipt_custody);
    assert!(work.live_allocation_bytes() < live);
    drop(observed);
    drop(observed_custody);
    drop(status);
    drop(status_custody);
    Ok(())
}

#[tokio::test]
async fn original_native_receipt_commit_faults_reconcile_the_same_slot_without_redispatch() -> Result<()> {
    for (kind, lost_reply) in [
        ("launch", false), ("launch", true), ("observed", false), ("observed", true),
    ] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            process_work_budget(1_000_000_000)?, &cancellation,
        ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let stream = Arc::new(ReceiptFaultStream::default());
        let fixture = process_fixture_with_native_work(
            StreamClient::new(stream.clone()), "write", &work,
        ).await?;
        let process = fixture.provider_with_native_work(&work).await?;
        let effects = fixture.effects(process.clone())?;
        *stream.fault.lock().await = Some((kind, lost_reply));
        let outcome = effects.run_task_effect_with_native_work(
            &fixture.owner, fixture.command, &fixture.plan, &work,
        ).await;
        if let Ok((status, custody)) = outcome {
            assert_eq!((kind, lost_reply), ("observed", true));
            assert!(matches!(status, EffectStatus::Succeeded { .. }));
            assert!(custody.work().shares_ledger(&work));
            drop(status);
            drop(custody);
        }
        let attempt = crate::EffectAttemptId::from_bytes(
            task_effect_id(fixture.owner.task_binding().0, fixture.command)?.into_bytes());
        let path = format!("harness/v2/native-process/{attempt}");
        let tail = fixture.owner.stream().bounds(&path).await?.tail;
        assert_eq!(tail, match (kind, lost_reply) {
            ("launch", false) => 0,
            ("observed", true) => 2,
            _ => 1,
        });
        let receipt = process.receipt_with_native_work(attempt, &work).await.unwrap();
        let expected = match &receipt {
            Some(receipt) => {
                assert_eq!(receipt.observed, kind == "observed" && lost_reply);
                Some(&receipt.effect)
            }
            None => {
                assert_eq!((kind, lost_reply), ("launch", false));
                None
            }
        };
        let reconciled = process.reconcile_with_native_work(attempt, &work)?.await?;
        assert_eq!(reconciled.as_ref().map(|(effect, _)| effect), expected);
        if let Some((_, custody)) = &reconciled {
            assert!(custody.work().shares_ledger(&work));
        }
        assert_eq!(fixture.owner.stream().bounds(&path).await?.tail, tail);
        assert_eq!(fixture.view.manifest().options.root.join("dst/output.txt").exists(), kind == "observed");
        let output = fixture.view.manifest().options.root.join("dst/output.txt");
        if output.exists() {
            std::fs::remove_file(&output).map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        }
        drop(reconciled);
        let again = process.reconcile_with_native_work(attempt, &work)?.await?;
        assert_eq!(again.as_ref().map(|(effect, _)| effect), expected);
        assert!(!output.exists());
        assert_eq!(fixture.owner.stream().bounds(&path).await?.tail, tail);
    }
    Ok(())
}

#[tokio::test]
async fn original_native_cancel_at_observed_commit_keeps_launch_uncertain_without_new_allowance() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        process_work_budget(1_000_000_000)?, &cancellation,
    ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = process_fixture_with_native_work(
        StreamClient::new(stream.clone()), "write", &work,
    ).await?;
    let process = fixture.provider_with_native_work(&work).await?;
    *stream.cancel_observed.lock().await = Some(cancellation.clone());
    assert!(fixture.effects(process.clone())?.run_task_effect_with_native_work(
        &fixture.owner, fixture.command, &fixture.plan, &work,
    ).await.is_err());
    assert!(fixture.view.operation_cancellation().is_cancelled());
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(fixture.owner.task_binding().0, fixture.command)?.into_bytes());
    let path = format!("harness/v2/native-process/{attempt}");
    assert_eq!(fixture.owner.stream().bounds(&path).await?.tail, 1);
    let prefix = work.receipt();
    assert!(process.reconcile_with_native_work(attempt, &work).is_err());
    assert_eq!(work.receipt(), prefix);
    // A separate read-only query has its own admitted budget, never a replacement
    // process allowance or a reset of the cancelled original view token.
    let query_cancellation = acyclic_fs::CancellationToken::new();
    let query = acyclic_fs::NativeMaterializationWork::new(
        process_work_budget(1_000_000_000)?, &query_cancellation,
    ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let receipt = process.receipt_with_native_work(attempt, &query).await.unwrap().unwrap();
    assert!(!receipt.observed);
    assert_eq!(receipt.effect.status, EffectStatus::Indeterminate);
    assert_eq!(query.receipt().authority_records_appended, 0);
    assert_eq!(query.receipt().backend_write_operations, 0);
    assert_eq!(query.receipt().materializations, 0);
    let (effect, custody) = receipt.into_effect_with_native_custody().unwrap();
    assert!(custody.work().shares_ledger(&query));
    assert!(!custody.work().shares_ledger(&work));
    let output = fixture.view.manifest().options.root.join("dst/output.txt");
    assert!(output.exists());
    std::fs::remove_file(&output).map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
    let again = process.receipt_with_native_work(attempt, &query).await.unwrap().unwrap();
    assert_eq!(again.effect, effect);
    assert!(!output.exists());
    assert_eq!(fixture.owner.stream().bounds(&path).await?.tail, 1);
    assert!(fixture.view.operation_cancellation().is_cancelled());
    drop(effect);
    drop(custody);
    Ok(())
}

#[tokio::test]
async fn finite_native_binding_rejects_legacy_view_before_future_or_content_reads() -> Result<()> {
    let fixture = process_fixture("write", true).await?;
    let budget = fixture.request().await?.view.options.work_per_volume;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let before = work.receipt();
    let result = Process::new_with_native_work(
        fixture.owner.clone(), fixture.command, &fixture.plan, fixture.view.clone(),
        fixture.approval, fixture.authority(), &work,
    );
    assert!(matches!(result, Err(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Unsupported, ..
    })));
    assert_eq!(work.receipt(), before);
    assert_eq!(work.receipt().authority_records_appended, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
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
    approval: InteractionId,
    plan: TaskEffectPlan,
    // Test teardown retains the actual never-launched mount, not a fake view.
    lazy_mount: Option<Arc<acyclic_fs::LazyMount<A, O, acyclic_fs::NativeDemandSource,
        acyclic_fs::MemoryLazyWorkspaceStore>>>,
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

    fn authority(&self) -> NativeProcessAuthority {
        NativeProcessAuthority { verifier: self.issuer.verifier(), history_limits: crate::store::HistoryReadLimits { maximum_events: 128, maximum_bytes: 32 * 1024 * 1024 }, content: self.content.clone(),
        result_content: self.content.clone(),
        results: self.results.clone(),
        approvals: self.approvals.clone(), authority_custody: None, }
    }

    async fn provider(&self) -> Result<Arc<Process<P>>> {
        Ok(Arc::new(
            Process::new(
                self.owner.clone(),
                self.command,
                self.plan.clone(),
                self.view.clone(),
                self.approval,
                self.authority(),
            )
            .await?,
        ))
    }

    async fn provider_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Process<P>>> {
        Process::new_with_native_work(
            self.owner.clone(), self.command, &self.plan, self.view.clone(),
            self.approval, self.authority(), work,
        )?.await?.into_arc_with_native_work(work)
    }

    fn effects(&self, process: Arc<Process<P>>) -> Result<ConversationEffectHost<P>> {
        let mut registry = EffectRegistry::default().with_result_resolver(self.content.clone());
        registry.register(process)?;
        ConversationEffectHost::new(
            self.owner.stream(),
            self.issuer.verifier().audience().clone(),
            self.issuer.clone(),
            self.scope.clone(),
            SchemaRegistry::new(),
            self.content.clone(),
            registry,
        )
    }
}

async fn process_fixture(mode: &str, approve: bool) -> Result<ProcessFixture> {
    process_fixture_on(
        StreamClient::new(Arc::new(MemoryStream::default())),
        mode,
        approve,
    )
    .await
}

async fn process_fixture_on<P: StreamProvider + 'static>(
    stream: StreamClient<P>,
    mode: &str,
    approve: bool,
) -> Result<ProcessFixture<P>> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    process_fixture_with_filesystems(stream, directory, Fs::memory(), Fs::memory(), mode, approve)
        .await
}


fn process_work_budget(allowance: u64) -> Result<acyclic_fs::WorkBudget> {
    let mut budget = serde_json::to_value(acyclic_fs::WorkBudget::UNBOUNDED)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    for value in budget.as_object_mut()
        .ok_or_else(|| Error::Invalid("work counters".into()))?.values_mut() {
        *value = serde_json::json!(allowance);
    }
    serde_json::from_value(budget).map_err(|error| Error::Invalid(error.to_string()))
}

async fn process_fixture_with_native_work<P: StreamProvider + 'static>(
    stream: StreamClient<P>, mode: &str,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<ProcessFixture<P>> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    process_fixture_original(
        stream, directory, Fs::memory(), Fs::memory(), mode, true, Some(work),
    ).await
}

fn initialize_issued_view_fixture_services() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(acyclic_fs::NativeTreeMaterializationError::from)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    acyclic_fs::initialize_native_mount_services()
        .map_err(acyclic_fs::native_mount::MountLifecycleError::Native).map_err(Error::from)
}

#[tokio::test]
async fn issued_mixed_working_view_retains_actual_lazy_project_and_materialized_scratch() -> Result<()> {
    use std::path::Path;
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(process_work_budget(100_000_000)?, &cancellation)
        .map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
    let fixture = process_fixture_with_native_work(
        StreamClient::new(Arc::new(MemoryStream::default())), "mixed-issued-view", &work,
    ).await?;
    assert!(matches!(fixture.view.manifest().volumes[0].namespace, NativeVolumeNamespace::LazyMount { .. }));
    assert_eq!(fixture.view.manifest().volumes[0].volume.class(), VolumeClass::Project);
    assert!(matches!(fixture.view.manifest().volumes[1].namespace, NativeVolumeNamespace::MaterializedDirectory));
    let issued = fixture.view.task_working_view_with_native_work(&fixture.owner, &work)?.await?;
    let operation = OperationId::from_bytes([216; 16]);
    let before = work.receipt().source_bytes_read;
    let project = issued.read_builtin_file(operation, Path::new("src/cold.txt"), 64, &work)?.await?;
    assert_eq!(project.0, b"genuine cold project");
    assert!(project.1.matches_work(&work));
    assert!(work.receipt().source_bytes_read - before >= project.0.len() as u64);
    drop(project);
    // Demand observations may advance overlay/shadow state, never the original
    // bound workspace/source cut or the protected prepared declaration.
    issued.verify_with_native_work(&work)?.await?;
    issued.write_builtin_file(operation, Path::new("dst/issued.txt"), "scratch",
        false, None, &work)?.await?;
    let scratch = issued.read_builtin_file(operation, Path::new("dst/issued.txt"), 16, &work)?.await?;
    assert_eq!(scratch.0, b"scratch");
    drop(scratch);
    let before = work.receipt().backend_write_operations;
    assert!(issued.write_builtin_file(operation, Path::new("src/denied.txt"), "denied",
        false, None, &work)?.await.is_err());
    assert!(issued.write_builtin_file(operation, Path::new("results/denied.txt"), "denied",
        true, None, &work)?.await.is_err());
    assert_eq!(work.receipt().backend_write_operations, before);
    let foreign = acyclic_fs::NativeMaterializationWork::new(process_work_budget(100_000_000)?, &cancellation)
        .map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
    assert!(issued.read_builtin_file(operation, Path::new("src/cold.txt"), 64, &foreign).is_err());
    assert_eq!(foreign.receipt().source_bytes_read, 0);
    // This fixture has never created or launched a NativeProcessProvider.
    // Teardown uses its actual original mount, not an invented drain boolean.
    fixture.owner.verify_with_native_work(false, &work).await?;
    fixture.lazy_mount.as_ref().expect("actual fixture lazy mount")
        .abandon_with_work(&work.native_mount_work()).map_err(Error::from)?;
    Ok(())
}


#[tokio::test]
async fn issued_working_view_retains_genuine_paid_owner_and_builtin_io() -> Result<()> {
    use std::path::Path;
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(process_work_budget(100_000_000)?, &cancellation)
        .map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
    let fixture = process_fixture_with_native_work(
        StreamClient::new(Arc::new(MemoryStream::default())), "write", &work,
    ).await?;
    let issued = fixture.view.task_working_view_with_native_work(&fixture.owner, &work)?.await?;
    let again = fixture.view.task_working_view_with_native_work(&fixture.owner, &work)?.await?;
    assert!(issued.same_identity(&again));
    assert_eq!(issued.original_task(), fixture.owner.task_binding().0);
    assert_eq!(issued.original_fence(), fixture.owner.task_binding().1);
    assert_eq!(issued.manifest(), fixture.view.manifest());
    issued.verify_with_native_work(&work)?.await?;
    assert_eq!(issued.volume_for_builtin_path(Path::new("src/input.txt"), VolumeOperation::Read, &work)?,
        &fixture.source);
    let operation = OperationId::from_bytes([214; 16]);
    let before = work.receipt();
    issued.write_builtin_file(operation, Path::new("dst/nested/issued.txt"), "héllo",
        true, None, &work)?.await?;
    let output = issued.read_builtin_file(operation, Path::new("dst/nested/issued.txt"), 32, &work)?.await?;
    assert_eq!(output.0, "héllo".as_bytes());
    assert_eq!(output.1.bytes(), output.0.capacity() as u64);
    assert!(output.1.matches_work(&work));
    assert_eq!(work.receipt().output_bytes - before.output_bytes, "héllo".len() as u64);
    assert_eq!(work.receipt().source_bytes_read - before.source_bytes_read, "héllo".len() as u64);
    drop(output);
    assert!(issued.read_builtin_file(operation, Path::new("dst/nested/issued.txt"), 1, &work)?.await.is_err());
    let before = work.receipt().backend_write_operations;
    assert!(issued.write_builtin_file(operation, Path::new("src/denied.txt"), "denied",
        true, None, &work)?.await.is_err());
    assert!(issued.write_builtin_file(operation, Path::new("unmounted/denied.txt"), "denied",
        true, None, &work)?.await.is_err());
    assert_eq!(work.receipt().backend_write_operations, before);
    assert!(!fixture.view.manifest().options.root.join("src/denied.txt").exists());
    assert!(!fixture.view.manifest().options.root.join("unmounted").exists());
    let remaining = work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
    work.charge(acyclic_fs::WorkCounters { allocation_operations: remaining.allocation_operations,
        ..acyclic_fs::WorkCounters::UNCHARGED })
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    assert!(issued.read_builtin_file(operation, Path::new("dst/nested/issued.txt"), 32, &work).is_err());
    assert!(issued.write_builtin_file(operation, Path::new("dst/budget-denied.txt"), "denied",
        true, None, &work).is_err());
    assert_eq!(work.receipt().backend_write_operations, before);
    assert!(!fixture.view.manifest().options.root.join("dst/budget-denied.txt").exists());
    Ok(())
}

#[tokio::test]
async fn issued_working_view_denies_legacy_foreign_work_full_fence_and_stale_owner() -> Result<()> {
    use crate::runtime::DurableTaskHost as _;
    use std::path::Path;
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(process_work_budget(100_000_000)?, &cancellation)
        .map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
    let fixture = process_fixture_with_native_work(
        StreamClient::new(Arc::new(MemoryStream::default())), "write", &work,
    ).await?;
    let issued = fixture.view.task_working_view_with_native_work(&fixture.owner, &work)?.await?;
    let foreign_work = acyclic_fs::NativeMaterializationWork::new(process_work_budget(100_000_000)?, &cancellation)
        .map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
    assert!(matches!(issued.verify_with_native_work(&foreign_work),
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, .. })));
    assert_eq!(foreign_work.receipt().backend_write_operations, 0);
    let foreign = process_fixture_with_native_work(
        StreamClient::new(Arc::new(MemoryStream::default())), "write", &work,
    ).await?;
    assert_ne!(fixture.owner.task_binding().1, foreign.owner.task_binding().1);
    assert!(fixture.view.task_working_view_with_native_work(&foreign.owner, &work)?.await.is_err());
    let context = fixture.task.task_context_with_native_work(
        fixture.owner.task_binding().0, fixture.owner.task_binding().1, &work,
    ).await?;
    let guard = context.volume_callback_guard_with_native_work(&fixture.owner, &work).await?;
    let callback = guard.for_volume(&fixture.owner, &fixture.source, false, &work)?;
    let writable_callback = guard.for_volume(&fixture.owner, &fixture.destination, true, &work)?;
    let mount_work = work.native_mount_work();
    assert!(callback.check(&mount_work).expect("original callback admission").await.is_ok());
    assert!(writable_callback.check(&mount_work).expect("original writable callback admission").await.is_ok());
    let foreign_mount_work = foreign_work.native_mount_work();
    assert!(callback.check(&foreign_mount_work).expect("paid foreign callback frame").await.is_err());
    let before_guards = work.receipt();
    assert!(guard.for_volume(&fixture.owner, &fixture.source, true, &work).is_err());
    assert!(guard.for_volume(&foreign.owner, &fixture.source, false, &work).is_err());
    assert!(guard.for_volume(&fixture.owner, &fixture.source, false, &foreign_work).is_err());
    let ungranted = VolumeRef::new(fixture.source.provider().clone(), "ungranted",
        VolumeClass::AgentPrivate, fixture.source.owner().clone())?;
    assert!(guard.for_volume(&fixture.owner, &ungranted, false, &work).is_err());
    assert_eq!(work.receipt().backend_read_operations, before_guards.backend_read_operations);
    assert_eq!(work.receipt().backend_write_operations, before_guards.backend_write_operations);
    let legacy = process_fixture("write", true).await?;
    assert!(legacy.view.task_working_view_with_native_work(&legacy.owner, &work)?.await.is_err());
    fixture.task.task_host().cancel(fixture.owner.task_binding().0).await?;
    let before = work.receipt().backend_write_operations;
    assert!(issued.verify_with_native_work(&work)?.await.is_err());
    assert!(issued.write_builtin_file(OperationId::from_bytes([215; 16]),
        Path::new("dst/stale.txt"), "denied", true, None, &work)?.await.is_err());
    assert_eq!(work.receipt().backend_write_operations, before);
    assert!(!fixture.view.manifest().options.root.join("dst/stale.txt").exists());
    let pending = callback.check(&mount_work).expect("pre-cancellation callback admission");
    cancellation.cancel();
    let error = pending.await.expect_err("original cancellation denies callback");
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut original_cancelled = false;
    while let Some(cause) = source {
        if matches!(cause.downcast_ref::<Error>(), Some(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
        })) {
            original_cancelled = true;
            break;
        }
        source = cause.source();
    }
    assert!(original_cancelled, "callback preserves original typed harness cancellation");
    Ok(())
}

async fn process_fixture_with_filesystems<P, A, O>(
    stream: StreamClient<P>,
    directory: tempfile::TempDir,
    filesystem: Fs<A, O>,
    auxiliary_filesystem: Fs<A, O>,
    mode: &str,
    approve: bool,
) -> Result<ProcessFixture<P, A, O>>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    process_fixture_original(
        stream, directory, filesystem, auxiliary_filesystem, mode, approve, None,
    ).await
}

async fn process_fixture_original<P, A, O>(
    stream: StreamClient<P>,
    directory: tempfile::TempDir,
    filesystem: Fs<A, O>,
    auxiliary_filesystem: Fs<A, O>,
    mode: &str,
    approve: bool,
    original_work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> Result<ProcessFixture<P, A, O>>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let provider = ProviderRef::new("native-test", "filesystem", "1")?;
    let files = Arc::new(FilesystemHost::new(filesystem, provider.clone())?);
    let agent = AgentId::from_bytes([61; 16]);
    let auxiliary_provider = ProviderRef::new("native-test-aux", "filesystem", "1")?;
    let auxiliary_host = Arc::new(FilesystemHost::new(
        auxiliary_filesystem,
        auxiliary_provider.clone(),
    )?);
    let auxiliary = VolumeRef::new(
        auxiliary_provider,
        "auxiliary",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let source = VolumeRef::new(
        provider.clone(),
        "source",
        if mode == "mixed-issued-view" { VolumeClass::Project } else { VolumeClass::AgentPrivate },
        if mode == "mixed-issued-view" { VolumeOwner::Project("fixture-project".into()) } else { VolumeOwner::Agent(agent) },
    )?;
    let destination = VolumeRef::new(
        provider.clone(),
        "destination",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let second_destination = VolumeRef::new(
        provider.clone(),
        "second-destination",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let result_volume = VolumeRef::new(
        provider,
        "results",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let extra_sources = (0..if mode == "many-bash" { 16 } else { 0 })
        .map(|index| {
            VolumeRef::new(
                source.provider().clone(),
                format!("extra-{index}"),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let command = OperationId::from_bytes([62; 16]);
    let approval = task_interaction_id(TaskId::from_bytes([53; 16]), command);
    let conversation = Authority {
        kind: AggregateKind::Conversation,
        id: "native-test-conversation".into(),
    };
    let issuer = AuthorityIssuer::new("native-test", [63; 32], conversation);
    let grants = Capabilities::new(
        [
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "task:spawn:test.effect@1".into(),
            source.capability(VolumeOperation::Read)?,
            auxiliary.capability(VolumeOperation::Read)?,
            destination.capability(VolumeOperation::Read)?,
            destination.capability(VolumeOperation::Write)?,
            second_destination.capability(VolumeOperation::Read)?,
            second_destination.capability(VolumeOperation::Write)?,
            result_volume.capability(VolumeOperation::Read)?,
            result_volume.capability(VolumeOperation::Write)?,
            "effect:plan".into(),
            "effect:run".into(),
            "effect:provider:test.native".into(),
            "interaction:open".into(),
            "interaction:resolve".into(),
            capability::interaction_respond(approval),
            "interaction:route".into(),
            "conversation:bind".into(),
        ]
        .into_iter()
        .chain(
            extra_sources
                .iter()
                .map(|volume| volume.capability(VolumeOperation::Read))
                .collect::<Result<Vec<_>>>()?,
        ),
    );
    let scope = issuer.root_for_agent(agent, "owner", grants.clone());
    files.create_volume(&source).await?;
    files.create_volume(&destination).await?;
    files.create_volume(&second_destination).await?;
    files.create_volume(&result_volume).await?;
    auxiliary_host.create_volume(&auxiliary).await?;
    for volume in &extra_sources {
        files.create_volume(volume).await?;
    }
    let task = native_task_fixture(
        stream,
        files.clone(),
        result_volume.clone(),
        &issuer,
        &scope,
        grants,
    )
    .await?;
    let task_id = TaskId::from_bytes([53; 16]);
    let crate::distributed::WorkPull::Claimed(lease) = task
        .task_host()
        .pull_work(&crate::distributed::Worker {
            id: "native-worker".into(),
            available: Default::default(),
            labels: BTreeMap::new(),
        })
        .await?
    else {
        return Err(Error::NotFound("native lease".into()));
    };
    task.task_host().start_task(&lease).await?;
    let owner_fence = crate::scheduler::LeaseFence::from(&lease.reservation);
    let owner = match original_work {
        Some(work) => task.task_host().journal_owner_with_native_work(
            task_id, &owner_fence, work,
        ).await?.into_native_shared(work)?,
        None => Arc::new(task.task_host().journal_owner(task_id, owner_fence.clone()).await?),
    };
    let mut aggregate = crate::store::StreamAggregate::open(
        &owner.stream(),
        issuer.verifier().audience().clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?;
    aggregate
        .execute(crate::core::Command {
            operation_id: OperationId::from_bytes([64; 16]),
            idempotency_key: IdempotencyKey::new("native-conversation-bind")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: crate::core::Action::BindConversation { agent },
        })
        .await?;
    let setup_scope = issuer.root_for_agent(
        agent,
        "setup",
        Capabilities::new(
            [
                source.capability(VolumeOperation::Write)?,
                auxiliary.capability(VolumeOperation::Write)?,
            ]
            .into_iter()
            .chain(
                extra_sources
                    .iter()
                    .map(|volume| volume.capability(VolumeOperation::Write))
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
    );
    let setup_grant = ContentGrant::verify(
        &issuer.verifier(),
        &setup_scope,
        &source,
        VolumeOperation::Write,
    )?;
    files
        .put_content(
            &source,
            &setup_grant,
            "input.txt",
            b"pinned input",
            "text/plain",
            "input.txt",
            128,
            &IdempotencyKey::new("source-setup")?,
        )
        .await?;
    let auxiliary_grant = ContentGrant::verify(
        &issuer.verifier(),
        &setup_scope,
        &auxiliary,
        VolumeOperation::Write,
    )?;
    auxiliary_host
        .put_content(
            &auxiliary,
            &auxiliary_grant,
            "input.txt",
            b"auxiliary input",
            "text/plain",
            "input.txt",
            128,
            &IdempotencyKey::new("auxiliary-setup")?,
        )
        .await?;
    let auxiliary_generation = auxiliary_host
        .resolve(&workspace_ref(
            auxiliary.provider().clone(),
            &auxiliary.storage_name()?,
        )?)
        .await?
        .generation;
    let source_generation = files
        .resolve(&workspace_ref(
            source.provider().clone(),
            &source.storage_name()?,
        )?)
        .await?
        .generation;
    let destination_generation = files
        .resolve(&workspace_ref(
            destination.provider().clone(),
            &destination.storage_name()?,
        )?)
        .await?
        .generation;
    let second_generation = files
        .resolve(&workspace_ref(
            second_destination.provider().clone(),
            &second_destination.storage_name()?,
        )?)
        .await?
        .generation;
    let root = directory.path().join("view");
    std::fs::create_dir(&root).map_err(|error| Error::Storage(error.to_string()))?;
    let work = process_work_budget(10_000_000)?;
    let operation_directory = directory.path().join("preparation");
    for path in [&operation_directory, &operation_directory.join("target"), &operation_directory.join("backup")] {
        std::fs::create_dir(path).map_err(|error| Error::Storage(error.to_string()))?;
    }
    let (preparation_root, native_root_custody) = match original_work {
        Some(work) => {
            let (root, custody) = NativePreparationRoot::witness_with_native_work(
                &root, &operation_directory, work,
            )?.await?;
            (root, Some(custody))
        }
        None => (NativePreparationRoot::witness(
            root.clone(), operation_directory, work, &acyclic_fs::CancellationToken::new(),
        ).await.map_err(|(error, _)| error)?.value, None),
    };
    let mut bindings = vec![
        PreparationSelection {
            host: files.clone(),
            volume: source.clone(),
            generation: source_generation,
            path: "src".into(),
            writable: false,
            verifier: issuer.verifier(),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        },
        PreparationSelection {
            host: files.clone(),
            volume: destination.clone(),
            generation: destination_generation,
            path: "dst".into(),
            writable: true,
            verifier: issuer.verifier(),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        },
        PreparationSelection {
            host: auxiliary_host,
            volume: auxiliary,
            generation: auxiliary_generation,
            path: "aux".into(),
            writable: false,
            verifier: issuer.verifier(),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        },
    ];
    if mode == "partial-publication" {
        bindings.push(PreparationSelection { host: files.clone(),
        volume: second_destination.clone(),
        generation: second_generation,
        path: "second-dst".into(),
        writable: true,
        verifier: issuer.verifier(),
        scope: scope.clone(),
        publication: acyclic_fs::PublicationPermit::Unrestricted, });
    }
    for (index, volume) in extra_sources.into_iter().enumerate() {
        let grant = ContentGrant::verify(
            &issuer.verifier(),
            &setup_scope,
            &volume,
            VolumeOperation::Write,
        )?;
        files
            .put_content(
                &volume,
                &grant,
                "input.txt",
                b"extra input",
                "text/plain",
                "input.txt",
                128,
                &IdempotencyKey::new(format!("extra-source-{index}"))?,
            )
            .await?;
        let generation = files
            .resolve(&workspace_ref(
                volume.provider().clone(),
                &volume.storage_name()?,
            )?)
            .await?
            .generation;
        bindings.push(PreparationSelection { host: files.clone(),
        volume,
        generation,
        path: format!("extra-{index}"),
        writable: false,
        verifier: issuer.verifier(),
        scope: scope.clone(),
        publication: acyclic_fs::PublicationPermit::Unrestricted, });
    }
    let maximum_volumes =
        u32::try_from(bindings.len()).map_err(|error| Error::Invalid(error.to_string()))?;
    let options = NativeViewOptions { root, maximum_volumes, work_per_volume: work };
    let borrowed = borrow_preparation_bindings(&bindings);
    let mut lazy_mount = None;
    let view = match original_work {
        Some(work) => {
            let context = task.task_context_with_native_work(task_id, &owner_fence, work).await?;
            let guard_bytes = borrowed.len().checked_mul(
                std::mem::size_of::<crate::runtime::NativeVolumeCallbackGuard<P>>(),
            ).ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native source guard slot count overflow" })?;
            let custody = work.claim_allocation(guard_bytes as u64, u64::from(guard_bytes != 0))
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let mut guards = (Vec::with_capacity(borrowed.len()), custody);
            for _ in &borrowed {
                guards.0.push(context.volume_callback_guard_with_native_work(
                    &owner, work,
                ).await?);
            }
            let view = if mode == "mixed-issued-view" {
                let physical = directory.path().join("lazy-source");
                std::fs::create_dir(&physical).map_err(|error| Error::Storage(error.to_string()))?;
                std::fs::write(physical.join("cold.txt"), b"genuine cold project")
                    .map_err(|error| Error::Storage(error.to_string()))?;
                let demand = Arc::new(acyclic_fs::NativeDemandSource::open(&physical,
                    acyclic_fs::FilesystemProfile::Portable, acyclic_fs::VolumeLimits::default())
                    .await.map_err(|error| Error::Storage(error.to_string()))?);
                let workspace = files.open_volume_with_native_work(&source, work).await?;
                let lazy = acyclic_fs::LazyWorkspace::open_or_attach(workspace, demand,
                    acyclic_fs::MemoryLazyWorkspaceStore::default()).await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                let callback = guards.0[0].for_volume(&owner, &source, false, work)?;
                let path = options.root.join("src");
                work.create_owned_directory(&options.root, preparation_root.expected_identity,
                    std::path::Path::new("src"), true)?;
                let mut allocation = work.claim_allocation(std::mem::size_of::<acyclic_fs::LazyMount<
                    A, O, acyclic_fs::NativeDemandSource, acyclic_fs::MemoryLazyWorkspaceStore>>() as u64, 1)?;
                let mut mount = lazy.mount_with_work(path, acyclic_fs::MountOptions::read_only()
                    .publication(acyclic_fs::MountPublication::Manual), Some(callback),
                    &work.native_mount_work(), work.cancellation()).await.map_err(Error::from)?;
                mount.retain_payload_allocation(&mut allocation).map_err(Error::from)?;
                let mount = Arc::new(mount);
                let snapshot = mount.snapshot_with_work(&work.native_mount_work(), work.cancellation())
                    .await.map_err(Error::from)?;
                let namespace = NativeVolumeNamespace::LazyMount { snapshot };
                let mut mixed_borrowed = borrowed;
                mixed_borrowed[0].namespace = &namespace;
                let held = crate::filesystem::NativeExistingLazyVolume::capture_with_native_work(
                    &mount, &owner, &mixed_borrowed[0], &guards.0[0], work,
                )?.await?;
                lazy_mount = Some(Arc::clone(&mount));
                NativeVolumeView::prepare_mixed_with_native_work(&owner, &guards.0,
                    &preparation_root, &options, &mixed_borrowed, &[Some(&held), None, None], work)?.await?
            } else {
                NativeVolumeView::prepare_with_native_work(
                    &owner, &guards.0, &preparation_root, &options, &borrowed, work,
                )?.await?
            };
            view.into_arc_with_native_work(work)?
        },
        None => Arc::new(NativeVolumeView::prepare(
            &owner, preparation_root.clone(), options, borrowed,
            &acyclic_fs::CancellationToken::new(),
        ).await.map_err(|(cause, _)| cause)?.value),
    };
    let mut request = NativeProcessRequest {
        executable: std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?,
        argv: vec![
            "--exact".into(),
            "durable_host::tests::native_process_tests::native_process_child".into(),
            "--nocapture".into(),
        ],
        cwd: view.manifest().options.root.clone(),
        environment: BTreeMap::from([
            ("NATIVE_TEST_MODE".into(), mode.into()),
            ("NATIVE_TEST_TOKEN".into(), "approved-env".into()),
        ]),
        timeout_ms: if mode == "timeout" { 300 } else { 10_000 },
        control_timeout_ms: if mode == "timeout" { 50 } else { 250 },
        cancellation_poll_ms: 10,
        maximum_output_bytes: 8192,
        maximum_result_bytes: 65_536,
        mcp_stdio: None,
        standard_input: None,
        view: view.manifest().clone(),
    };
    if mode == "mcp-approval" || mode.starts_with("mcp-stdin-") {
        request.mcp_stdio = Some(crate::mcp::stdio::McpStdioRequest {
            initialization: OperationId::from_bytes([71; 16]),
            operation: OperationId::from_bytes([72; 16]),
            method: crate::mcp::stdio::McpStdioMethod::CallTool,
            params: serde_json::json!({"name":"echo","arguments":{}}),
            maximum_bytes: 4096,
        });
    }
    if mode.starts_with("stdin-") || mode.starts_with("mcp-stdin-") {
        request.standard_input = Some(if mode.ends_with("-empty") {
            Vec::new()
        } else {
            vec![0, 255, b'\n', b'x']
        });
    }
    if mode == "bash" || mode == "mutate-source-bash" || mode == "many-bash" {
        request.executable = if cfg!(windows) {
            "C:/Program Files/Git/bin/bash.exe"
        } else {
            "/bin/bash"
        }
        .into();
        request.argv = vec!["--noprofile".into(), "--norc".into(), "-c".into(),
            "IFS= read -r input < src/input.txt; printf '%s' \"$input\" > dst/output.txt; IFS= read -r aux < aux/input.txt; printf '%s' \"$aux\" > dst/auxiliary.txt; printf '%s' \"$0|$1|$2\"; printf '%s' \"$NATIVE_TEST_TOKEN\" >&2".into(),
            "approved-bash".into(), "first arg".into(), "second arg".into()];
        if mode == "mutate-source-bash" {
            request.argv[3].push_str("; printf 'unauthorized source edit' > src/input.txt");
        }
        if mode == "many-bash" {
            request.argv[3].push_str("; for index in {0..15}; do IFS= read -r value < extra-$index/input.txt; printf '%s|' \"$value\"; done > dst/many.txt");
        }
    }
    let content = Arc::new(FilesystemContentVerifier::new(
        files.clone(),
        issuer.verifier(),
        scope.clone(),
        1_000_000,
    )?);
    let results = Arc::new(FilesystemContentPublisher::new(
        files.clone(),
        result_volume.clone(),
        &issuer.verifier(),
        &scope,
        1_000_000,
    )?);
    let request_ref = results
        .stage(
            command,
            "process/request.json",
            &crate::contract::canonical_json_bytes(&request)?,
            "application/json",
            "request.json",
        )
        .await?;
    let schema = results
        .stage(
            command,
            "process/schema.json",
            b"{\"type\":\"object\"}",
            "application/schema+json",
            "schema.json",
        )
        .await?;
    let plan = TaskEffectPlan {
        provider: "test.native".into(),
        guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: NATIVE_PROCESS_EFFECT_KIND.into(),
        request: request_ref,
        result_schema: schema,
    };
    let approvals = Arc::new(FilesystemExecutionJournal::new(
        owner.stream(),
        files.clone(),
        result_volume,
        issuer.verifier(),
        scope.clone(),
        1_000_000,
    )?);
    approvals
        .open_interaction(
            approval,
            Interaction::approval(
                "Run the exact admitted process",
                command,
                request.approval_digest(owner.task_binding().0, command)?,
            )?,
        )
        .await?;
    approvals
        .resolve_interaction(
            approval,
            InteractionResponse::Approval {
                approved: approve,
                reason: None,
            },
            &scope,
        )
        .await?;
    Ok(ProcessFixture {
        directory,
        task,
        lease,
        owner,
        files,
        issuer,
        scope,
        source,
        destination,
        second_destination,
        content,
        results,
        approvals,
        view,
        preparation_root,
        command,
        approval,
        plan,
        lazy_mount,
        _native_root_custody: native_root_custody,
    })
}

async fn native_task_fixture<P, A, O>(
    stream: StreamClient<P>,
    files: Arc<Files<A, O>>,
    volume: VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
    grants: Capabilities,
) -> Result<NativeRuntime<P, A, O>>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let implementation = Arc::new(NativeEffectMachine {
        identity: MachineIdentity {
            name: "test.effect".into(),
            version: "1".into(),
            digest: [52; 32],
        },
        schema: serde_json::json!({"type":"integer"}),
    });
    let definition = TaskDefinition::<Value, Value>::resumable(
        implementation.clone(),
        serde_json::json!({"type":"integer"}),
        serde_json::json!({"type":"object"}),
    )?;
    let identity = definition.identity().clone();
    let machine = implementation.identity.clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let mut machines = MachineRegistry::default();
    machines.register(implementation)?;
    let runtime_scope = RuntimeScope::new(grants, Limits::default())?;
    let runtime = FilesystemTaskRuntime::open(
        stream,
        files,
        volume,
        issuer.verifier(),
        scope.clone(),
        runtime_scope.clone(),
        tasks,
        machines,
        crate::tool::ToolRegistry::default(),
        crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 1,
        },
        1,
        1_000_000,
    )
    .await?;
    runtime
        .task_host()
        .admit(TaskAdmissionRecord {
            operation_id: OperationId::from_bytes([53; 16]),
            task: identity,
            machine,
            input: serde_json::json!(0),
            input_schema: serde_json::json!({"type":"integer"}),
            output_schema: serde_json::json!({"type":"object"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
    Ok(runtime)
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
            inner, fault: Mutex::new(None), preparation_fault: Mutex::new(None),
            hide_next_inspection: std::sync::atomic::AtomicBool::new(false),
            cancel_observed: Mutex::new(None),
        }
    }
}

impl Default for ReceiptFaultStream {
    fn default() -> Self { Self::new(MemoryStream::default()) }
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
        let preparation_fault = if preparation_selected { preparation.take() } else { None };
        drop(preparation);
        if fault.is_some_and(|(_, lost_reply)| !lost_reply)
            || preparation_fault.is_some_and(|fault| !fault.lost_reply)
        {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        let result = self.inner.commit(request).await?;
        if fault.is_some() || preparation_fault.is_some() {
            // Hide the immediate keyed observation as well, so this is
            // an unknown commit rather than a recovered lost reply.
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

fn borrow_preparation_bindings<A, O>(
    bindings: &[PreparationSelection<A, O>],
) -> Vec<BorrowedNativeVolumeBinding<'_, A, O>> {
    bindings.iter().map(|binding| BorrowedNativeVolumeBinding {
        host: &binding.host, volume: &binding.volume, generation: &binding.generation,
        namespace: &NativeVolumeNamespace::MaterializedDirectory,
        path: &binding.path, writable: binding.writable, verifier: &binding.verifier,
        scope: &binding.scope, publication: binding.publication,
    }).collect()
}

async fn preparation_fixture_on<P: StreamProvider + 'static>(
    stream: StreamClient<P>, seed: &ProcessFixture,
) -> Result<PreparationFixture<P>> {
    let task = native_task_fixture(
        stream, seed.files.clone(), seed.results.volume().clone(), &seed.issuer, &seed.scope,
        seed.owner.input_grants.clone(),
    ).await?;
    let crate::distributed::WorkPull::Claimed(lease) = task.task_host().pull_work(&crate::distributed::Worker {
        id: "preparation-worker".into(), available: Default::default(), labels: BTreeMap::new(),
    }).await? else { return Err(Error::NotFound("native preparation lease".into())); };
    task.task_host().start_task(&lease).await?;
    let owner = Arc::new(task.task_host().journal_owner(
        TaskId::from_bytes([53; 16]), crate::scheduler::LeaseFence::from(&lease.reservation),
    ).await?);
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let root = directory.path().join("view");
    let operation = directory.path().join("operation");
    for path in [&root, &operation, &operation.join("target"), &operation.join("backup")] {
        std::fs::create_dir(path).map_err(|error| Error::Storage(error.to_string()))?;
    }
    let options = NativeViewOptions {
        root: root.clone(), maximum_volumes: 3, work_per_volume: seed.view.manifest().options.work_per_volume,
    };
    let allocation = NativePreparationRoot::witness(
        root, operation, options.work_per_volume, &acyclic_fs::CancellationToken::new(),
    ).await.map_err(|(error, _)| error)?.value;
    let second_generation = seed.files.resolve(&workspace_ref(
        seed.second_destination.provider().clone(), &seed.second_destination.storage_name()?,
    )?).await?.generation;
    let mut bindings = Vec::new();
    for (volume, path, writable) in [
        (&seed.source, "a-source", false), (&seed.destination, "b-destination", true),
    ] {
        let generation = seed.view.manifest().volumes.iter().find(|mapping| &mapping.volume == volume)
            .ok_or_else(|| Error::NotFound("original native preparation mapping".into()))?.generation.clone();
        bindings.push(PreparationSelection { host: seed.files.clone(), volume: volume.clone(), generation, path: path.into(), writable,
        verifier: seed.issuer.verifier(), scope: seed.scope.clone(),
        publication: acyclic_fs::PublicationPermit::Unrestricted, });
    }
    bindings.push(PreparationSelection { host: seed.files.clone(), volume: seed.second_destination.clone(), generation: second_generation,
    path: "c-second-destination".into(), writable: true,
    verifier: seed.issuer.verifier(), scope: seed.scope.clone(),
    publication: acyclic_fs::PublicationPermit::Unrestricted, });
    Ok(PreparationFixture { _directory: directory, task, owner, allocation, options, bindings })
}

type PreparationView = NativeVolumeView<MemoryAuthorityBackend, MemoryObjectBackend>;

#[tokio::test]
async fn native_builtin_actual_prepared_view_reads_and_writes_original_mounted_scratch() -> Result<()> {
    use std::path::Path;
    let seed = process_fixture("write", true).await?;
    let fixture = preparation_fixture_on(StreamClient::new(Arc::new(MemoryStream::default())), &seed).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let view = PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings), &cancellation,
    ).await.map_err(|(cause, _)| cause)?.value;
    let budget = fixture.options.budget_for_volume_count(fixture.bindings.len())?;
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
    let operation = OperationId::from_bytes([211; 16]);
    view.write_builtin_file(fixture.owner.native_view_owner(), operation,
        Path::new("b-destination/nested/file"), "héllo", true, None, &work).await?;
    let output = view.read_builtin_file(fixture.owner.native_view_owner(), operation,
        Path::new("b-destination/nested/file"), 32, &work).await?;
    assert_eq!(output.0, "héllo".as_bytes());
    assert_eq!(output.1.bytes(), output.0.capacity() as u64);
    assert!(output.1.shares_ledger(&work.retain_owned(0)?));
    assert_eq!(work.receipt().output_bytes, "héllo".len() as u64);
    assert_eq!(work.receipt().source_bytes_read, "héllo".len() as u64);
    drop(output);
    assert_eq!(std::fs::read(fixture.allocation.root.join("b-destination/nested/file"))
        .map_err(acyclic_fs::NativeTreeMaterializationError::from)?, "héllo".as_bytes());
    Ok(())
}

#[tokio::test]
async fn native_builtin_current_volume_grants_and_unmounted_results_deny_before_physical_writes() -> Result<()> {
    use std::path::Path;
    let seed = process_fixture("write", true).await?;
    let fixture = preparation_fixture_on(StreamClient::new(Arc::new(MemoryStream::default())), &seed).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let view = PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings), &cancellation,
    ).await.map_err(|(cause, _)| cause)?.value;
    let work = acyclic_fs::NativeMaterializationWork::new(
        fixture.options.budget_for_volume_count(fixture.bindings.len())?, &cancellation,
    ).map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
    assert!(fixture.owner.native_view_owner()
        .require_volume_grant_with_native_work(&seed.source, VolumeOperation::Write, &work).is_err());
    for path in ["a-source/input.txt", "results/unmounted.txt", "../results/escape.txt"] {
        assert!(view.write_builtin_file(fixture.owner.native_view_owner(), OperationId::from_bytes([212; 16]),
            Path::new(path), "forbidden", true, None, &work).await.is_err());
    }
    assert_eq!(work.receipt().output_bytes, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert_eq!(std::fs::read(fixture.allocation.root.join("a-source/input.txt"))
        .map_err(acyclic_fs::NativeTreeMaterializationError::from)?, b"pinned input");
    assert!(!fixture.allocation.root.join("results").exists());
    Ok(())
}

#[tokio::test]
async fn native_builtin_rejects_same_uuid_foreign_original_owner_and_cancelled_current_task() -> Result<()> {
    use crate::runtime::DurableTaskHost as _;
    use std::path::Path;
    let seed = process_fixture("write", true).await?;
    let fixture = preparation_fixture_on(StreamClient::new(Arc::new(MemoryStream::default())), &seed).await?;
    let foreign = preparation_fixture_on(StreamClient::new(Arc::new(MemoryStream::default())), &seed).await?;
    assert_eq!(fixture.owner.task_binding().0, foreign.owner.task_binding().0);
    let cancellation = acyclic_fs::CancellationToken::new();
    let view = PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings), &cancellation,
    ).await.map_err(|(cause, _)| cause)?.value;
    let work = acyclic_fs::NativeMaterializationWork::new(
        fixture.options.budget_for_volume_count(fixture.bindings.len())?, &cancellation,
    ).map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
    let operation = OperationId::from_bytes([213; 16]);
    assert!(view.read_builtin_file(foreign.owner.native_view_owner(), operation,
        Path::new("a-source/input.txt"), 32, &work).await.is_err());
    assert_eq!(work.receipt().source_bytes_read, 0);
    fixture.task.task_host().cancel(fixture.owner.task_binding().0).await?;
    assert!(view.write_builtin_file(fixture.owner.native_view_owner(), operation,
        Path::new("b-destination/after-cancel"), "forbidden", true, None, &work).await.is_err());
    assert_eq!(work.receipt().output_bytes, 0);
    assert!(!fixture.allocation.root.join("b-destination/after-cancel").exists());
    assert_eq!(std::fs::read(fixture.allocation.root.join("a-source/input.txt"))
        .map_err(acyclic_fs::NativeTreeMaterializationError::from)?, b"pinned input");
    Ok(())
}

#[tokio::test]
async fn original_native_preparation_resumes_physical_partial_and_lost_acknowledgements() -> Result<()> {
    use acyclic_fs::MaterializationPhase::{Applied, Applying, Preparing};
    let seed = process_fixture("write", true).await?;
    let original = PreparationView::inspect_preparation(
        &seed.owner, &seed.preparation_root, seed.view.manifest().options.work_per_volume,
        &acyclic_fs::CancellationToken::new(),
    ).await.map_err(|(error, _)| error)?.value
        .ok_or_else(|| Error::NotFound("completed original preparation journal".into()))?;
    assert_eq!(original.journal.plan, seed.view.preparation_receipts()[0].journal.plan);
    assert_eq!(original.journal.phase, Applied);
    assert_eq!(original.journal.plan.operation_id, NativePreparationRoot::operation(&seed.owner));
    let acyclic_fs::MaterializationIntent::NativePreparation { selection, original: request } = &original.journal.plan.intent else {
        return Err(Error::Storage("native root used a synthetic generation transition".into()));
    };
    assert!(!request.is_empty());
    assert_eq!(selection.len(), original.manifest.volumes.len());
    for (selected, mapping) in selection.iter().zip(&original.manifest.volumes) {
        assert_eq!(selected.provider_identity, crate::contract::canonical_json_bytes(mapping.volume.provider())?);
        assert_eq!(selected.generation.digest().as_bytes().as_slice(), mapping.generation.as_resource().key());
    }
    for (phase, preimages, applied, lost_reply) in [
        (Preparing, 0, 0, false), (Preparing, 0, 0, true),
        (Preparing, 1, 0, false), (Preparing, 1, 0, true),
        (Applying, 3, 1, false), (Applying, 3, 1, true),
        (Applied, 3, 3, false), (Applied, 3, 3, true),
    ] {
        let stream = Arc::new(ReceiptFaultStream::default());
        let fixture = preparation_fixture_on(StreamClient::new(stream.clone()), &seed).await?;
        *stream.preparation_fault.lock().await = Some(PreparationCommitFault { phase, preimages, applied, lost_reply });
        let failed = PreparationView::prepare(
            &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
            borrow_preparation_bindings(&fixture.bindings), &acyclic_fs::CancellationToken::new(),
        ).await.err().ok_or_else(|| Error::Storage("native preparation fault was not exercised".into()))?;
        assert!(stream.preparation_fault.lock().await.is_none());
        assert!(failed.1.backend_read_operations > 0);
        let before = PreparationView::inspect_preparation(
            &fixture.owner, &fixture.allocation, fixture.options.work_per_volume,
            &acyclic_fs::CancellationToken::new(),
        ).await.map_err(|(error, _)| error)?.value;
        let original_allocation = PreparationView::recover_allocation(
            &fixture.owner, &fixture.options, fixture.options.work_per_volume, &acyclic_fs::CancellationToken::new(),
        ).await.map_err(|(error, _)| error)?.value;
        assert_eq!(original_allocation.is_some(), before.is_some());
        if let Some(allocation) = &original_allocation {
            assert_eq!(allocation.root, fixture.allocation.root);
            assert_eq!(allocation.operation_directory, fixture.allocation.operation_directory);
            assert_eq!(allocation.expected_identity, fixture.allocation.expected_identity);
            assert_eq!(allocation.expected_preparation_identity, fixture.allocation.expected_preparation_identity);
        }
        if phase == Applying {
            assert_eq!(std::fs::read(fixture.allocation.root.join("a-source/input.txt"))
                .map_err(|error| Error::Storage(error.to_string()))?, b"pinned input");
            assert!(!fixture.allocation.root.join("b-destination").exists());
            assert!(failed.1.materializations > 0);
        }
        if let Some(receipt) = &before {
            assert_eq!(receipt.task, fixture.owner.task_binding().0);
            assert_eq!(&receipt.original_fence, fixture.owner.task_binding().1);
            assert_eq!(receipt.journal.root_identity, Some(fixture.allocation.expected_identity.to_bytes()));
            assert_eq!(receipt.journal.preparation_identity.as_ref(), Some(&fixture.allocation.expected_preparation_identity));
        } else {
            assert_eq!((phase, preimages, applied, lost_reply), (Preparing, 0, 0, false));
        }
        let recovered = PreparationView::prepare(
            &fixture.owner, original_allocation.unwrap_or_else(|| fixture.allocation.clone()), fixture.options.clone(),
            borrow_preparation_bindings(&fixture.bindings), &acyclic_fs::CancellationToken::new(),
        ).await.map_err(|(cause, _)| cause)?;
        let receipt = &recovered.value.preparation_receipts()[0];
        assert_eq!(receipt.journal.phase, Applied);
        assert_eq!(receipt.journal.applied, 3);
        assert_eq!(receipt.journal.plan.operation_id, NativePreparationRoot::operation(&fixture.owner));
        if let Some(before) = before {
            assert_eq!(before.journal.plan, receipt.journal.plan);
            assert_eq!(before.original_fence, receipt.original_fence);
            assert!(receipt.journal.work.backend_read_operations >= before.journal.work.backend_read_operations);
        }
        recovered.value.validate_for_dispatch(recovered.value.manifest().validation_budget()?).await
            .map_err(|(cause, _)| cause)?;
        for mapping in &recovered.value.manifest().volumes {
            assert!(mapping.path.is_dir());
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_preparation_cancel_cleanup_and_replaced_roots_never_adopt_user_data() -> Result<()> {
    use acyclic_fs::MaterializationPhase::{Applying, RolledBack};
    let seed = process_fixture("write", true).await?;
    for replace_root in [false, true] {
        let stream = Arc::new(ReceiptFaultStream::default());
        let fixture = preparation_fixture_on(StreamClient::new(stream.clone()), &seed).await?;
        *stream.preparation_fault.lock().await = Some(PreparationCommitFault {
            phase: Applying, preimages: 3, applied: 1, lost_reply: false,
        });
        let failed = PreparationView::prepare(
            &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
            borrow_preparation_bindings(&fixture.bindings), &acyclic_fs::CancellationToken::new(),
        ).await.err().ok_or_else(|| Error::Storage("partial native preparation unexpectedly completed".into()))?;
        assert!(failed.1.materializations > 0);
        let cancelled = acyclic_fs::CancellationToken::new();
        cancelled.cancel();
        let stopped = PreparationView::prepare(
            &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
            borrow_preparation_bindings(&fixture.bindings), &cancelled,
        ).await;
        assert!(stopped.is_err());
        fixture.task.task_host().cancel(fixture.owner.task_binding().0).await?;
        assert!(fixture.owner.verify(false).await.is_err());
        assert!(fixture.owner.verify(true).await.is_ok());
        assert!(PreparationView::prepare(
            &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
            borrow_preparation_bindings(&fixture.bindings), &acyclic_fs::CancellationToken::new(),
        ).await.is_err());
        if replace_root {
            let displaced = fixture._directory.path().join("displaced");
            std::fs::rename(&fixture.allocation.root, displaced).map_err(|error| Error::Storage(error.to_string()))?;
            std::fs::create_dir(&fixture.allocation.root).map_err(|error| Error::Storage(error.to_string()))?;
            std::fs::write(fixture.allocation.root.join("user.txt"), b"unrelated user data")
                .map_err(|error| Error::Storage(error.to_string()))?;
            assert!(PreparationView::recover_allocation(
                &fixture.owner, &fixture.options, fixture.options.work_per_volume, &acyclic_fs::CancellationToken::new(),
            ).await.is_err());
            let receipt = PreparationView::inspect_preparation(
                &fixture.owner, &fixture.allocation, fixture.options.work_per_volume,
                &acyclic_fs::CancellationToken::new(),
            ).await.map_err(|(error, _)| error)?.value
                .ok_or_else(|| Error::NotFound("original preparation evidence".into()))?;
            assert!(matches!(receipt.root, acyclic_fs::MaterializationRootObservation::Diverged { .. }));
            assert!(PreparationView::cleanup_preparation(
                &fixture.owner, &fixture.allocation, fixture.options.work_per_volume,
                &acyclic_fs::CancellationToken::new(),
            ).await.is_err());
            assert_eq!(std::fs::read(fixture.allocation.root.join("user.txt"))
                .map_err(|error| Error::Storage(error.to_string()))?, b"unrelated user data");
        } else {
            let cleaned = PreparationView::cleanup_preparation(
                &fixture.owner, &fixture.allocation, fixture.options.work_per_volume,
                &acyclic_fs::CancellationToken::new(),
            ).await.map_err(|(error, _)| error)?;
            let receipt = cleaned.value.ok_or_else(|| Error::NotFound("original cleanup evidence".into()))?;
            assert_eq!(receipt.journal.phase, RolledBack);
            assert!(cleaned.work.backend_write_operations > 0);
            assert!(std::fs::read_dir(&fixture.allocation.root).map_err(|error| Error::Storage(error.to_string()))?.next().is_none());
            assert!(std::fs::read_dir(fixture.allocation.operation_directory.join("target"))
                .map_err(|error| Error::Storage(error.to_string()))?.next().is_none());
            assert!(PreparationView::prepare(
                &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
                borrow_preparation_bindings(&fixture.bindings), &acyclic_fs::CancellationToken::new(),
            ).await.is_err());
        }
    }
    Ok(())
}

#[tokio::test]
#[cfg(feature = "filesystem-local")]
async fn native_preparation_incomplete_journal_reopens_on_actual_local_stream() -> Result<()> {
    let seed = process_fixture("write", true).await?;
    let storage = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let path = storage.path().join("coordinator");
    let provider = acyclic_stream::LocalStream::open(&path, Default::default()).await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let stream = Arc::new(ReceiptFaultStream::new(provider));
    let fixture = preparation_fixture_on(StreamClient::new(stream.clone()), &seed).await?;
    *stream.preparation_fault.lock().await = Some(PreparationCommitFault {
        phase: acyclic_fs::MaterializationPhase::Applying, preimages: 3, applied: 1, lost_reply: true,
    });
    let failed = PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings), &acyclic_fs::CancellationToken::new(),
    ).await.err().ok_or_else(|| Error::Storage("local preparation lost acknowledgement was not exercised".into()))?;
    assert!(failed.1.materializations > 0);
    let PreparationFixture { _directory, task, owner, allocation, options, bindings } = fixture;
    let (task_id, fence) = owner.task_binding();
    let fence = fence.clone();
    drop(owner);
    drop(task);
    drop(stream);
    let reopened = acyclic_stream::LocalStream::open(&path, Default::default()).await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let runtime = native_task_fixture(
        StreamClient::new(Arc::new(reopened)), seed.files.clone(), seed.results.volume().clone(),
        &seed.issuer, &seed.scope, seed.owner.input_grants.clone(),
    ).await?;
    let owner = runtime.task_host().journal_owner(task_id, fence.clone()).await?;
    let before = PreparationView::inspect_preparation(
        &owner, &allocation, options.work_per_volume, &acyclic_fs::CancellationToken::new(),
    ).await.map_err(|(error, _)| error)?.value
        .ok_or_else(|| Error::NotFound("reopened actual preparation journal".into()))?;
    assert_eq!(before.journal.phase, acyclic_fs::MaterializationPhase::Applying);
    assert_eq!(before.journal.applied, 1);
    assert_eq!(before.original_fence, fence);
    let prepared = PreparationView::prepare(
        &owner, allocation.clone(), options, borrow_preparation_bindings(&bindings), &acyclic_fs::CancellationToken::new(),
    ).await.map_err(|(cause, _)| cause)?;
    assert_eq!(prepared.value.preparation_receipts()[0].journal.plan, before.journal.plan);
    assert_eq!(prepared.value.preparation_receipts()[0].journal.phase, acyclic_fs::MaterializationPhase::Applied);
    prepared.value.validate_for_dispatch(prepared.value.manifest().validation_budget()?).await
        .map_err(|(cause, _)| cause)?;
    drop(_directory);
    Ok(())
}

#[tokio::test]
async fn native_preparation_nested_rollback_and_stage_replacement_are_identity_bound() -> Result<()> {
    let seed = process_fixture("write", true).await?;
    for replace_stage in [false, true] {
        let stream = Arc::new(ReceiptFaultStream::default());
        let mut fixture = preparation_fixture_on(StreamClient::new(stream.clone()), &seed).await?;
        for (binding, path) in fixture.bindings.iter_mut().zip(["group/source", "group/destination", "other/destination"]) {
            binding.path = path.into();
        }
        *stream.preparation_fault.lock().await = Some(PreparationCommitFault {
            phase: acyclic_fs::MaterializationPhase::Applying, preimages: 2, applied: 1, lost_reply: false,
        });
        let failure = PreparationView::prepare(
            &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
            borrow_preparation_bindings(&fixture.bindings), &acyclic_fs::CancellationToken::new(),
        ).await.err().ok_or_else(|| Error::Storage("nested physical preparation fault was not exercised".into()))?;
        assert!(failure.1.materializations > 0);
        assert_eq!(std::fs::read(fixture.allocation.root.join("group/source/input.txt"))
            .map_err(|error| Error::Storage(error.to_string()))?, b"pinned input");
        if replace_stage {
            let target = fixture.allocation.operation_directory.join("target");
            std::fs::rename(&target, fixture._directory.path().join("displaced-target"))
                .map_err(|error| Error::Storage(error.to_string()))?;
            std::fs::create_dir(&target).map_err(|error| Error::Storage(error.to_string()))?;
            std::fs::write(target.join("user.txt"), b"unrelated staging allocation")
                .map_err(|error| Error::Storage(error.to_string()))?;
            let receipt = PreparationView::inspect_preparation(
                &fixture.owner, &fixture.allocation, fixture.options.work_per_volume,
                &acyclic_fs::CancellationToken::new(),
            ).await.map_err(|(error, _)| error)?.value
                .ok_or_else(|| Error::NotFound("original staging evidence".into()))?;
            assert!(matches!(receipt.preparation, acyclic_fs::MaterializationPreparationObservation::Diverged { .. }));
            assert!(PreparationView::cleanup_preparation(
                &fixture.owner, &fixture.allocation, fixture.options.work_per_volume,
                &acyclic_fs::CancellationToken::new(),
            ).await.is_err());
            assert_eq!(std::fs::read(target.join("user.txt")).map_err(|error| Error::Storage(error.to_string()))?,
                b"unrelated staging allocation");
        } else {
            let cleaned = PreparationView::cleanup_preparation(
                &fixture.owner, &fixture.allocation, fixture.options.work_per_volume,
                &acyclic_fs::CancellationToken::new(),
            ).await.map_err(|(error, _)| error)?;
            assert_eq!(cleaned.value.ok_or_else(|| Error::NotFound("nested rollback journal".into()))?.journal.phase,
                acyclic_fs::MaterializationPhase::RolledBack);
            assert!(std::fs::read_dir(&fixture.allocation.root).map_err(|error| Error::Storage(error.to_string()))?.next().is_none());
        }
    }
    Ok(())
}

#[tokio::test]
async fn partial_native_preparation_rechecks_destination_head_before_attachment() -> Result<()> {
    let seed = process_fixture("write", true).await?;
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = preparation_fixture_on(StreamClient::new(stream.clone()), &seed).await?;
    *stream.preparation_fault.lock().await = Some(PreparationCommitFault {
        phase: acyclic_fs::MaterializationPhase::Applying, preimages: 3, applied: 1, lost_reply: false,
    });
    assert!(PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings), &acyclic_fs::CancellationToken::new(),
    ).await.is_err());
    let grant = ContentGrant::verify(&seed.issuer.verifier(), &seed.scope, &seed.destination, VolumeOperation::Write)?;
    seed.files.put_content(
        &seed.destination, &grant, "concurrent.txt", b"new destination head",
        "text/plain", "concurrent.txt", 128, &IdempotencyKey::new("native-preparation-concurrent-head")?,
    ).await?;
    let failure = PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings), &acyclic_fs::CancellationToken::new(),
    ).await.err().ok_or_else(|| Error::Storage("stale destination attached to recovered native root".into()))?;
    assert!(matches!(failure.0, Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, .. }));
    assert!(failure.1.authority_records_read > 0);
    assert!(!fixture.allocation.root.join("b-destination").exists());
    let receipt = PreparationView::inspect_preparation(
        &fixture.owner, &fixture.allocation, fixture.options.work_per_volume,
        &acyclic_fs::CancellationToken::new(),
    ).await.map_err(|(error, _)| error)?.value
        .ok_or_else(|| Error::NotFound("stale original preparation".into()))?;
    assert_eq!(receipt.journal.phase, acyclic_fs::MaterializationPhase::Applying);
    Ok(())
}

#[tokio::test]
async fn native_staging_leaf_replacement_is_rejected_at_actual_open_before_writes() -> Result<()> {
    let seed = process_fixture("write", true).await?;
    let filesystem = Fs::memory();
    let workspace = filesystem.create_workspace("bound-native-staging").await
        .map_err(|error| Error::Storage(error.to_string()))?;
    workspace.write("/input.txt", Bytes::from_static(b"original selected generation")).await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let generation = workspace.head().await.map_err(|error| Error::Storage(error.to_string()))?;
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let target = directory.path().join("target");
    std::fs::create_dir(&target).map_err(|error| Error::Storage(error.to_string()))?;
    let original = acyclic_fs::capture_root_identity(&target).map_err(|error| Error::Storage(error.to_string()))?;
    let options = acyclic_fs::MaterializeOptions::native(&target).expected_root_identity(Some(original));
    std::fs::rename(&target, directory.path().join("original-target"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir(&target).map_err(|error| Error::Storage(error.to_string()))?;
    let failure = generation.materialize(
        &options, seed.view.manifest().options.work_per_volume, &acyclic_fs::CancellationToken::new(),
    ).await.err().ok_or_else(|| Error::Storage("replacement staging leaf adopted".into()))?;
    assert!(failure.work.backend_read_operations > 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    assert!(std::fs::read_dir(&target).map_err(|error| Error::Storage(error.to_string()))?.next().is_none());
    assert!(std::fs::read_dir(directory.path().join("original-target"))
        .map_err(|error| Error::Storage(error.to_string()))?.next().is_none());
    Ok(())
}

#[tokio::test]
async fn native_precapture_identity_failure_retains_work_and_never_replays() -> Result<()> {
    let fixture = process_fixture("write", true).await?;
    let process = fixture.provider().await?;
    let root = fixture.view.manifest().options.root.clone();
    let original = root.with_file_name("original-native-root");
    std::fs::rename(&root, &original).map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir(&root).map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::write(root.join("user.txt"), b"external replacement")
        .map_err(|error| Error::Storage(error.to_string()))?;
    let effects = fixture.effects(process.clone())?;
    let status = effects.run_task_effect(
        &fixture.owner, fixture.command, fixture.plan.clone(),
    ).await?;
    assert_eq!(status, EffectStatus::Indeterminate);
    let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
    let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
    let receipt = process.receipt(attempt).await?
        .ok_or_else(|| Error::NotFound("native pre-capture observation".into()))?;
    assert_native_receipt_binding(
        &fixture.owner, fixture.command, &fixture.plan, &receipt, &status, true,
    )?;
    assert!(receipt.work.backend_read_operations > 0);
    assert!(receipt.work.allocation_operations > 0);
    assert!(receipt.invocation_failure.as_ref().is_some_and(|message| !message.is_empty()));
    assert_eq!(receipt.diagnostic, None);
    assert_eq!(receipt.publication_failure, None);
    assert!(!original.join("dst/output.txt").exists());
    assert_eq!(
        std::fs::read(root.join("user.txt")).map_err(|error| Error::Storage(error.to_string()))?,
        b"external replacement",
    );
    drop((effects, process));
    std::fs::remove_file(root.join("user.txt")).map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::remove_dir(&root).map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::rename(&original, &root).map_err(|error| Error::Storage(error.to_string()))?;
    let recovered = Arc::new(Process::recover(
        fixture.owner.clone(), fixture.command, fixture.plan.clone(),
        fixture.approval, fixture.authority(),
    ).await?);
    assert_eq!(recovered.receipt(attempt).await?, Some(receipt));
    assert_eq!(
        fixture.effects(recovered)?.reconcile_task_effect(
            &fixture.owner, fixture.command, &fixture.plan,
        ).await?,
        EffectStatus::Indeterminate,
    );
    assert!(!root.join("dst/output.txt").exists());
    Ok(())
}

#[tokio::test]
async fn receipt_commit_faults_never_replay_an_unknown_native_attempt() -> Result<()> {
    for (kind, lost_reply) in [
        ("launch", false),
        ("launch", true),
        ("observed", false),
        ("observed", true),
    ] {
        let stream = Arc::new(ReceiptFaultStream::default());
        let fixture = process_fixture_on(StreamClient::new(stream.clone()), "write", true).await?;
        *stream.fault.lock().await = Some((kind, lost_reply));
        let process = fixture.provider().await?;
        let effects = fixture.effects(process.clone())?;
        assert!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await
                .is_err()
        );
        assert!(
            stream.fault.lock().await.is_none(),
            "fault was not exercised"
        );
        let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
        let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
        let receipt = process.receipt(attempt).await?;
        if let Some(receipt) = &receipt {
            let retained_status = if kind == "observed" && lost_reply {
                receipt.effect.status.clone()
            } else {
                EffectStatus::Indeterminate
            };
            assert_native_receipt_binding(
                &fixture.owner,
                fixture.command,
                &fixture.plan,
                receipt,
                &retained_status,
                kind == "observed" && lost_reply,
            )?;
            assert_eq!(receipt.diagnostic, None);
            assert_eq!(receipt.publication_failure, None);
            assert_eq!(receipt.invocation_failure, None);
            if receipt.observed {
                assert!(receipt.work.backend_read_operations > 0);
                assert!(receipt.work.backend_write_operations > 0);
            } else {
                assert_eq!(receipt.work, acyclic_fs::WorkCounters::default());
            }
        } else {
            assert_eq!((kind, lost_reply), ("launch", false));
        }
        drop((effects, process));
        let recovered = Arc::new(
            Process::<ReceiptFaultStream>::recover(
                fixture.owner.clone(),
                fixture.command,
                fixture.plan.clone(),
                fixture.approval,
                fixture.authority(),
            )
            .await?,
        );
        let effects = fixture.effects(recovered.clone())?;
        let status = effects
            .reconcile_task_effect(&fixture.owner, fixture.command, &fixture.plan)
            .await?;
        if kind == "observed" && lost_reply {
            assert!(matches!(status, EffectStatus::Succeeded { .. }));
        } else {
            assert_eq!(status, EffectStatus::Indeterminate);
        }
        assert_eq!(recovered.receipt(attempt).await?, receipt);
        assert_eq!(
            crate::contract::canonical_json_bytes(&recovered.receipt(attempt).await?)?,
            crate::contract::canonical_json_bytes(&receipt)?,
        );
        assert_eq!(
            recovered.reconcile(attempt).await?,
            receipt.as_ref().map(|receipt| receipt.effect.clone()),
        );
        let root = &fixture.view.manifest().options.root;
        assert_eq!(root.join("dst/output.txt").exists(), kind == "observed");
        if kind == "observed" {
            std::fs::remove_file(root.join("dst/output.txt"))
                .map_err(|error| Error::Storage(error.to_string()))?;
        }
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            status
        );
        assert!(!root.join("dst/output.txt").exists());
        assert_eq!(recovered.receipt(attempt).await?, receipt);
        let destination = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name()?,
        )?;
        assert_eq!(
            fixture
                .files
                .read(&destination, None, "/output.txt", 64)
                .await
                .is_ok(),
            kind == "observed"
        );
    }
    Ok(())
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn receipt_reconciliation_reopens_all_local_stores_after_provider_objects_drop() -> Result<()>
{
    let storage = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let filesystem = Fs::local(acyclic_fs::LocalOptions::new(
        storage.path().join("filesystem"),
    ))
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    let auxiliary = Fs::local(acyclic_fs::LocalOptions::new(
        storage.path().join("auxiliary"),
    ))
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    let local = Arc::new(
        acyclic_stream::LocalStream::open(storage.path().join("coordinator"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let weak_stream = Arc::downgrade(&local);
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fixture = process_fixture_with_filesystems(
        StreamClient::new(local),
        directory,
        filesystem,
        auxiliary,
        "write",
        true,
    )
    .await?;
    let seed = NativeRestart::from_fixture(&fixture);
    let auxiliary = fixture.view.manifest().volumes[2].clone();
    let weak_files = Arc::downgrade(&fixture.files);
    let process = fixture.provider().await?;
    let weak_process = Arc::downgrade(&process);
    let status = fixture
        .effects(process.clone())?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    assert!(matches!(status, EffectStatus::Succeeded { .. }));
    let attempt =
        crate::EffectAttemptId::from_bytes(task_effect_id(seed.task, seed.command)?.into_bytes());
    let receipt = process
        .receipt(attempt)
        .await?
        .ok_or_else(|| Error::Storage("missing original local native receipt".into()))?;
    assert_native_receipt_binding(
        &fixture.owner,
        seed.command,
        &seed.plan,
        &receipt,
        &status,
        true,
    )?;
    assert_eq!(receipt.diagnostic, None);
    assert_eq!(receipt.publication_failure, None);
    assert_eq!(receipt.invocation_failure, None);
    assert!(receipt.work.backend_read_operations > 0);
    assert!(receipt.work.backend_write_operations > 0);
    drop(process);
    assert!(weak_process.upgrade().is_none());
    let ProcessFixture {
        directory,
        task,
        lease: _,
        owner,
        files,
        issuer: _,
        scope: _,
        source: _,
        destination: _,
        second_destination: _,
        content,
        results,
        approvals,
        view,
        command: _,
        approval: _,
        plan: _,
        preparation_root: _,
        lazy_mount: _,
        _native_root_custody: _,
    } = fixture;
    let native_output = seed.root.join("dst/output.txt");
    drop((task, owner, approvals, content, results, view, files));
    assert!(weak_stream.upgrade().is_none());
    assert!(weak_files.upgrade().is_none());
    let LocalNativeRecovery {
        files,
        effects,
        owner,
        process,
        ..
    } = reopen_native(storage.path(), &seed).await?;
    assert_eq!(
        effects
            .reconcile_task_effect(&owner, seed.command, &seed.plan)
            .await?,
        status
    );
    assert_eq!(process.receipt(attempt).await?.as_ref(), Some(&receipt));
    assert_eq!(
        crate::contract::canonical_json_bytes(&process.receipt(attempt).await?)?,
        crate::contract::canonical_json_bytes(&Some(&receipt))?,
    );
    std::fs::remove_file(&native_output).map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect(&owner, seed.command, seed.plan.clone())
            .await?,
        status
    );
    assert!(!native_output.exists());
    assert_eq!(process.receipt(attempt).await?, Some(receipt));
    let destination = workspace_ref(
        seed.destination.provider().clone(),
        &seed.destination.storage_name()?,
    )?;
    assert_eq!(
        files
            .read(&destination, None, "/output.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let auxiliary_files = FilesystemHost::new(
        Fs::local(acyclic_fs::LocalOptions::new(
            storage.path().join("auxiliary"),
        ))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?,
        auxiliary.volume.provider().clone(),
    )?;
    let auxiliary_ref = workspace_ref(
        auxiliary.volume.provider().clone(),
        &auxiliary.volume.storage_name()?,
    )?;
    assert_eq!(
        auxiliary_files
            .read(
                &auxiliary_ref,
                Some(&auxiliary.generation),
                "/input.txt",
                64
            )
            .await?
            .as_ref(),
        b"auxiliary input"
    );
    assert!(directory.path().exists());
    Ok(())
}

#[cfg(feature = "filesystem-local")]
#[derive(serde::Serialize, serde::Deserialize)]
struct NativeRestart {
    task: TaskId,
    fence: crate::scheduler::LeaseFence,
    scope: Scope,
    command: OperationId,
    approval: InteractionId,
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
            approval: fixture.approval,
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
async fn reopen_native(
    storage: &std::path::Path,
    seed: &NativeRestart,
) -> Result<LocalNativeRecovery> {
    let issuer = AuthorityIssuer::new(
        "native-test",
        [63; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "native-test-conversation".into(),
        },
    );
    let files = Arc::new(FilesystemHost::new(
        Fs::local(acyclic_fs::LocalOptions::new(storage.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        ProviderRef::new("native-test", "filesystem", "1")?,
    )?);
    let content = Arc::new(FilesystemContentVerifier::new(
        files.clone(),
        issuer.verifier(),
        seed.scope.clone(),
        1_000_000,
    )?);
    let results = Arc::new(FilesystemContentPublisher::new(
        files.clone(),
        seed.result_volume.clone(),
        &issuer.verifier(),
        &seed.scope,
        1_000_000,
    )?);
    let reopened = Arc::new(
        acyclic_stream::LocalStream::open(storage.join("coordinator"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let task = native_task_fixture(
        StreamClient::new(reopened),
        files.clone(),
        seed.result_volume.clone(),
        &issuer,
        &seed.scope,
        seed.scope.capabilities().clone(),
    )
    .await?;
    let owner = Arc::new(
        task.task_host()
            .journal_owner(seed.task, seed.fence.clone())
            .await?,
    );
    let approvals = Arc::new(FilesystemExecutionJournal::new(
        owner.stream(),
        files.clone(),
        seed.result_volume.clone(),
        issuer.verifier(),
        seed.scope.clone(),
        1_000_000,
    )?);
    let process = Arc::new(
        LocalNativeProcess::recover(
            owner.clone(),
            seed.command,
            seed.plan.clone(),
            seed.approval,
            NativeProcessAuthority { verifier: issuer.verifier(), history_limits: crate::store::HistoryReadLimits { maximum_events: 128, maximum_bytes: 32 * 1024 * 1024 }, content: content.clone(),
            result_content: content.clone(),
            results,
            approvals, authority_custody: None, },
        )
        .await?,
    );
    let mut registry = EffectRegistry::default().with_result_resolver(content.clone());
    registry.register(process.clone())?;
    let effects = ConversationEffectHost::new(
        owner.stream(),
        issuer.verifier().audience().clone(),
        issuer,
        seed.scope.clone(),
        SchemaRegistry::new(),
        content,
        registry,
    )?;
    Ok(LocalNativeRecovery {
        files,
        task,
        owner,
        process,
        effects,
    })
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
        StreamClient::new(stream),
        directory,
        filesystem,
        auxiliary,
        "host-death",
        true,
    )
    .await?;
    // Host-only restart metadata; it never enters the model's process request.
    std::fs::write(
        storage.join("restart.json"),
        crate::contract::canonical_json_bytes(&NativeRestart::from_fixture(&fixture))?,
    )
    .map_err(|error| Error::Storage(error.to_string()))?;
    fixture
        .effects(fixture.provider().await?)?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    Err(Error::Storage(
        "host-death controller failed to terminate host".into(),
    ))
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn actual_host_termination_reopens_launch_only_receipt_without_replay_or_slot_release()
-> Result<()> {
    use std::process::{Command, Stdio};
    let storage = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let mut child =
        Command::new(std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?)
            .args([
                "--exact",
                "durable_host::tests::native_process_tests::native_host_child",
                "--nocapture",
            ])
            .env_clear()
            .env("NATIVE_HOST_STORAGE", storage.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| Error::Storage(error.to_string()))?;
    let ready = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Ok(bytes) = std::fs::read(storage.path().join("restart.json"))
                && let Ok(seed) = crate::contract::json_from_slice::<NativeRestart>(&bytes)
                && seed.root.join("host-death-ready").exists()
            {
                break seed;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    // Always stop and reap the exact host child, including a setup timeout.
    child
        .kill()
        .map_err(|error| Error::Storage(error.to_string()))?;
    child
        .wait()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let seed = ready
        .map_err(|error| Error::Storage(format!("native host did not reach launch: {error}")))?;
    tokio::time::sleep(std::time::Duration::from_millis(3200)).await;
    let LocalNativeRecovery {
        files,
        task,
        owner,
        process,
        effects,
    } = reopen_native(storage.path(), &seed).await?;
    let attempt =
        crate::EffectAttemptId::from_bytes(task_effect_id(seed.task, seed.command)?.into_bytes());
    let path = acyclic_stream::StreamPath::new(format!("harness/v2/native-process/{attempt}"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        owner
            .stream()
            .bounds(path.as_str())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .tail,
        1
    );
    assert_eq!(
        effects
            .run_task_effect(&owner, seed.command, seed.plan.clone())
            .await?,
        EffectStatus::Indeterminate
    );
    assert_eq!(process.output(attempt).await?, None);
    let receipt = process
        .receipt(attempt)
        .await?
        .ok_or_else(|| Error::Storage("missing launch-only native receipt".into()))?;
    assert_native_receipt_binding(
        &owner,
        seed.command,
        &seed.plan,
        &receipt,
        &EffectStatus::Indeterminate,
        false,
    )?;
    assert_eq!(receipt.original_fence, seed.fence);
    assert_eq!(receipt.diagnostic, None);
    assert_eq!(receipt.publication_failure, None);
    assert_eq!(receipt.invocation_failure, None);
    // No publication was observed; this counts view work, not OS process work.
    assert_eq!(receipt.work, acyclic_fs::WorkCounters::default());
    let output = seed.root.join("dst/output.txt");
    if output.exists() {
        std::fs::remove_file(&output).map_err(|error| Error::Storage(error.to_string()))?;
    }
    task.task_host().cancel(seed.task).await?;
    assert!(owner.verify(false).await.is_err());
    assert!(owner.verify(true).await.is_ok());
    assert_eq!(
        effects
            .reconcile_task_effect(&owner, seed.command, &seed.plan)
            .await?,
        EffectStatus::Indeterminate
    );
    assert_eq!(process.receipt(attempt).await?, Some(receipt));
    assert_eq!(task.task_host().outcome(seed.task).await?, None);
    assert_eq!(
        owner
            .stream()
            .bounds(path.as_str())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .tail,
        1
    );
    assert!(!output.exists());
    let destination = workspace_ref(
        seed.destination.provider().clone(),
        &seed.destination.storage_name()?,
    )?;
    assert!(
        files
            .read(&destination, None, "/output.txt", 64)
            .await
            .is_err()
    );
    let coordinator = owner.host.coordinator.lock().await;
    let operation = coordinator
        .scheduler()
        .operation(owner.operation_id())
        .ok_or_else(|| Error::NotFound("native operation after host death".into()))?;
    assert!(operation.reservation.is_some());
    assert_ne!(operation.phase, crate::scheduler::OperationPhase::Terminal);
    Ok(())
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
        // An orphan on Unix finishes within this bound even after its host dies.
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

#[tokio::test]
async fn changed_invocations_cannot_use_the_original_approval() -> Result<()> {
    for change in [
        "argv",
        "cwd",
        "environment",
        "bounds",
        "authority",
        "volume-order",
        "mcp-initialization",
        "mcp-operation",
        "mcp-method",
        "mcp-params",
        "mcp-bytes",
        "stdin-bytes",
        "stdin-empty",
    ] {
        let mut fixture = process_fixture(
            if change.starts_with("mcp-") {
                "mcp-approval"
            } else {
                "write"
            },
            true,
        )
        .await?;
        drop(fixture.provider().await?);
        let mut request = fixture.request().await?;
        match change {
            "argv" => request.argv.swap(0, 1),
            "cwd" => request.cwd = request.view.volumes[0].path.clone(),
            "environment" => {
                request
                    .environment
                    .insert("NEW_VARIABLE".into(), "changed".into());
            }
            "bounds" => request.timeout_ms += 1,
            "authority" => request.view.volumes[0].authority_revision[0] ^= 1,
            "volume-order" => request.view.volumes.swap(0, 1),
            "stdin-bytes" => request.standard_input = Some(vec![0, 255, b'\n']),
            "stdin-empty" => request.standard_input = Some(Vec::new()),
            change if change.starts_with("mcp-") => {
                let exchange = request
                    .mcp_stdio
                    .as_mut()
                    .ok_or_else(|| Error::NotFound("MCP descriptor".into()))?;
                match change {
                    "mcp-initialization" => {
                        exchange.initialization = OperationId::from_bytes([73; 16]);
                    }
                    "mcp-operation" => exchange.operation = OperationId::from_bytes([74; 16]),
                    "mcp-method" => exchange.method = crate::mcp::stdio::McpStdioMethod::ListTools,
                    "mcp-params" => {
                        exchange.params = serde_json::json!({"name":"changed","arguments":{}});
                    }
                    "mcp-bytes" => exchange.maximum_bytes += 1,
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
        let original = fixture.request().await?;
        assert_ne!(
            original.approval_digest(fixture.owner.task_binding().0, fixture.command)?,
            request.approval_digest(fixture.owner.task_binding().0, fixture.command)?
        );
        fixture.replace_request(&request).await?;
        assert!(
            fixture.provider().await.is_err(),
            "accepted changed {change}"
        );
        assert!(
            !fixture
                .view
                .manifest()
                .options
                .root
                .join("dst/output.txt")
                .exists()
        );
    }
    Ok(())
}

#[tokio::test]
async fn absent_stdio_preserves_legacy_approval_and_stopped_mcp_remains_uncertain() -> Result<()> {
    let fixture = process_fixture("write", true).await?;
    let mut request = fixture.request().await?;
    let legacy =
        serde_json::to_value(&request).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(legacy.get("mcp_stdio").is_none());
    assert!(legacy.get("standard_input").is_none());
    let decoded: NativeProcessRequest = serde_json::from_value(legacy.clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(decoded.mcp_stdio.is_none());
    assert!(decoded.standard_input.is_none());
    assert_eq!(
        request.approval_digest(fixture.owner.task_binding().0, fixture.command)?,
        crate::contract::canonical_json_digest(&(
            NATIVE_PROCESS_EFFECT_KIND,
            fixture.owner.task_binding().0,
            fixture.command,
            legacy
        ))?
    );
    let initialization = OperationId::from_bytes([75; 16]);
    let operation = OperationId::from_bytes([76; 16]);
    request.mcp_stdio = Some(crate::mcp::stdio::McpStdioRequest {
        initialization,
        operation,
        method: crate::mcp::stdio::McpStdioMethod::CallTool,
        params: serde_json::json!({"name":"echo","arguments":{}}),
        maximum_bytes: 4096,
    });
    let value = serde_json::json!({"content":[],"structuredContent":{"sequence":u64::MAX}});
    let mut stdout = crate::contract::canonical_json_bytes(&serde_json::json!({"jsonrpc":"2.0",
        "id":initialization.to_string(),"result":{"protocolVersion":crate::mcp::PROTOCOL_VERSION,
        "capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}}))?;
    stdout.push(b'\n');
    stdout.extend(crate::contract::canonical_json_bytes(
        &serde_json::json!({"jsonrpc":"2.0",
        "id":operation.to_string(),"result":value}),
    )?);
    stdout.push(b'\n');
    let mut result = NativeProcessResult {
        success: false,
        exit_code: Some(1),
        stdout,
        stderr: vec![],
        stop: None,
    };
    assert_eq!(request.mcp_response(&result)?, value);
    result.stop = Some(crate::filesystem::NativeProcessStop {
        kind: NativeProcessStopKind::Timeout,
        message: "stopped capture".into(),
        cleanup_completed: true,
    });
    assert!(
        matches!(request.mcp_response(&result), Err(Error::Indeterminate(id)) if id == operation)
    );
    Ok(())
}

#[tokio::test]
#[cfg(windows)]
async fn native_view_rejects_physical_aliases_before_any_materialization() -> Result<()> {
    let fixture = process_fixture("write", true).await?;
    for names in [["same", "SAME"], ["parent", "PARENT/child"]] {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let mut options = fixture.view.manifest().options.clone();
        options.root = directory.path().to_path_buf();
        let operation_directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::create_dir(operation_directory.path().join("target")).map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::create_dir(operation_directory.path().join("backup")).map_err(|error| Error::Storage(error.to_string()))?;
        let allocation = NativePreparationRoot::witness(
            options.root.clone(), operation_directory.path().to_path_buf(), options.work_per_volume,
            &acyclic_fs::CancellationToken::new(),
        ).await.map_err(|(error, _)| error)?.value;
        let bindings = fixture.view.manifest().volumes[..2]
            .iter()
            .zip(names)
            .map(|(volume, path)| PreparationSelection { host: fixture.files.clone(),
            volume: volume.volume.clone(),
            generation: volume.generation.clone(),
            path: path.into(),
            writable: volume.writable,
            verifier: fixture.issuer.verifier(),
            scope: fixture.scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted, })
            .collect::<Vec<_>>();
        let failed = NativeVolumeView::prepare(
            &fixture.owner,
            allocation,
            options,
            borrow_preparation_bindings(&bindings),
            &acyclic_fs::CancellationToken::new(),
        ).await.err().ok_or_else(|| Error::Storage("physical alias admitted".into()))?;
        assert!(matches!(failed.0, Error::Invalid(_)));
        assert!(failed.1.backend_read_operations > 0);
        assert!(std::fs::read_dir(directory.path()).map_err(|error| Error::Storage(error.to_string()))?
            .next().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn publication_races_preserve_output_and_do_not_claim_joint_volume_success() -> Result<()> {
    for mode in ["pause-write", "partial-publication"] {
        let fixture = process_fixture(mode, true).await?;
        let process = fixture.provider().await?;
        let effects = Arc::new(fixture.effects(process.clone())?);
        let running = {
            let effects = effects.clone();
            let owner = fixture.owner.clone();
            let command = fixture.command;
            let plan = fixture.plan.clone();
            tokio::spawn(async move { effects.run_task_effect(&owner, command, plan).await })
        };
        let root = &fixture.view.manifest().options.root;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !root.join("publication-ready").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
        let changed = if mode == "pause-write" {
            &fixture.destination
        } else {
            &fixture.second_destination
        };
        let grant = ContentGrant::verify(
            &fixture.issuer.verifier(),
            &fixture.scope,
            changed,
            VolumeOperation::Write,
        )?;
        fixture
            .files
            .put_content(
                changed,
                &grant,
                "concurrent.txt",
                b"concurrent writer",
                "text/plain",
                "concurrent.txt",
                128,
                &IdempotencyKey::new("concurrent-destination")?,
            )
            .await?;
        std::fs::write(root.join("publication-release"), b"release")
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            running
                .await
                .map_err(|error| Error::Storage(error.to_string()))??,
            EffectStatus::Indeterminate
        );
        let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
        let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
        let output = process
            .output(attempt)
            .await?
            .ok_or_else(|| Error::Storage("publication fault lost native output".into()))?;
        let receipt = process
            .receipt(attempt)
            .await?
            .ok_or_else(|| Error::Storage("publication fault lost typed receipt".into()))?;
        assert_native_receipt_binding(
            &fixture.owner,
            fixture.command,
            &fixture.plan,
            &receipt,
            &EffectStatus::Indeterminate,
            true,
        )?;
        assert_eq!(
            process.reconcile(attempt).await?,
            Some(receipt.effect.clone()),
        );
        assert_eq!(receipt.diagnostic, Some(output.clone()));
        assert_eq!(receipt.invocation_failure, None);
        let failure = receipt
            .publication_failure
            .as_ref()
            .ok_or_else(|| Error::Storage("missing measured publication failure".into()))?;
        assert_eq!(receipt.work, failure.work);
        assert!(receipt.work.backend_read_operations > 0);
        if mode == "partial-publication" {
            // The completed first mapping's actual capture/publication is retained.
            assert!(receipt.work.backend_write_operations > 0);
            assert!(receipt.work.source_entries_visited > 0);
            assert!(receipt.work.source_bytes_read > 0);
        }
        let mapping = |volume: &VolumeRef| {
            fixture
                .view
                .manifest()
                .volumes
                .iter()
                .find(|mapping| &mapping.volume == volume)
                .cloned()
                .ok_or_else(|| Error::Storage("missing admitted destination mapping".into()))
        };
        assert_eq!(
            receipt.publication_failure,
            Some(crate::filesystem::NativePublicationFailure {
                destination: Some(mapping(changed)?),
                completed: if mode == "partial-publication" {
                    vec![mapping(&fixture.destination)?]
                } else {
                    Vec::new()
                },
                work: receipt.work,
                message: acyclic_fs::MountLifecycleError::Source(
                    acyclic_fs::MountSourceError::Stale,
                )
                .to_string(),
            })
        );
        let output: NativeProcessResult =
            crate::contract::json_from_slice(&fixture.content.read(&output).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(output.success && output.stop.is_none());
        assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
        let first = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name()?,
        )?;
        assert_eq!(
            fixture
                .files
                .read(&first, None, "/output.txt", 64)
                .await
                .is_ok(),
            mode == "partial-publication"
        );
        let changed_ref = workspace_ref(changed.provider().clone(), &changed.storage_name()?)?;
        assert_eq!(
            fixture
                .files
                .read(&changed_ref, None, "/concurrent.txt", 64)
                .await?
                .as_ref(),
            b"concurrent writer"
        );
        assert!(
            fixture
                .files
                .read(&changed_ref, None, "/output.txt", 64)
                .await
                .is_err()
        );
        std::fs::remove_file(root.join("dst/output.txt"))
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            EffectStatus::Indeterminate,
        );
        assert!(!root.join("dst/output.txt").exists());
        assert_eq!(process.receipt(attempt).await?.as_ref(), Some(&receipt));
        let weak = Arc::downgrade(&process);
        drop((effects, process));
        assert!(weak.upgrade().is_none());
        let recovered = Arc::new(
            Process::recover(
                fixture.owner.clone(),
                fixture.command,
                fixture.plan.clone(),
                fixture.approval,
                fixture.authority(),
            )
            .await?,
        );
        let effects = fixture.effects(recovered.clone())?;
        assert_eq!(recovered.receipt(attempt).await?.as_ref(), Some(&receipt));
        assert_eq!(
            crate::contract::canonical_json_bytes(&recovered.receipt(attempt).await?)?,
            crate::contract::canonical_json_bytes(&Some(&receipt))?,
        );
        assert_eq!(
            recovered.output(attempt).await?,
            receipt.diagnostic,
        );
        assert_eq!(
            effects
                .reconcile_task_effect(&fixture.owner, fixture.command, &fixture.plan)
                .await?,
            EffectStatus::Indeterminate,
        );
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            EffectStatus::Indeterminate
        );
        assert!(!root.join("dst/output.txt").exists());
        assert_eq!(recovered.receipt(attempt).await?, Some(receipt));
        assert_eq!(
            recovered
                .receipt(crate::EffectAttemptId::from_bytes([255; 16]))
                .await?,
            None,
        );
        assert_eq!(
            fixture
                .owner
                .stream()
                .bounds(&format!("harness/v2/native-process/{attempt}"))
                .await?
                .tail,
            2,
        );
        assert_eq!(
            fixture
                .task
                .task_host()
                .outcome(fixture.owner.task_binding().0)
                .await?,
            None
        );
        assert!(fixture.owner.verify(false).await.is_ok());
    }
    Ok(())
}

#[tokio::test]
async fn timeout_and_output_overflow_retain_cleanup_failure_without_publication() -> Result<()> {
    for mode in ["timeout", "flood"] {
        let fixture = process_fixture(mode, true).await?;
        let process = fixture.provider().await?;
        let effects = fixture.effects(process.clone())?;
        let status = effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?;
        let EffectStatus::Succeeded { result } = &status else {
            return Err(Error::Storage(format!(
                "stopped capture unobserved: {status:?}"
            )));
        };
        let attempt = crate::EffectAttemptId::from_bytes(
            task_effect_id(fixture.owner.task_binding().0, fixture.command)?.into_bytes(),
        );
        let receipt = process
            .receipt(attempt)
            .await?
            .ok_or_else(|| Error::Storage("missing stopped-capture receipt".into()))?;
        assert_native_receipt_binding(
            &fixture.owner,
            fixture.command,
            &fixture.plan,
            &receipt,
            &status,
            true,
        )?;
        assert_eq!(receipt.diagnostic, None);
        assert_eq!(receipt.publication_failure, None);
        assert_eq!(receipt.invocation_failure, None);
        // Stopped captures skip publication; OS/artifact work is not view work.
        assert_eq!(receipt.work, acyclic_fs::WorkCounters::default());
        assert_eq!(process.output(attempt).await?.as_ref(), Some(result));
        let output: NativeProcessResult =
            crate::contract::json_from_slice(&fixture.content.read(result).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(!output.success);
        let stop = output
            .stop
            .ok_or_else(|| Error::Storage("missing stopped-capture reason".into()))?;
        assert!(stop.cleanup_completed);
        assert_eq!(
            stop.kind,
            if mode == "timeout" {
                NativeProcessStopKind::Timeout
            } else {
                NativeProcessStopKind::OutputLimit
            }
        );
        assert!(!stop.message.is_empty());
        assert!(output.stdout.len() + output.stderr.len() <= 8192);
        if mode == "flood" {
            assert_eq!(output.stdout.len() + output.stderr.len(), 8192);
        }
        let destination = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name()?,
        )?;
        assert!(
            fixture
                .files
                .read(&destination, None, "/output.txt", 64)
                .await
                .is_err()
        );
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            status,
        );
        assert_eq!(process.receipt(attempt).await?.as_ref(), Some(&receipt));
        drop((effects, process));
        let recovered = Arc::new(
            Process::recover(
                fixture.owner.clone(),
                fixture.command,
                fixture.plan.clone(),
                fixture.approval,
                fixture.authority(),
            )
            .await?,
        );
        assert_eq!(recovered.receipt(attempt).await?.as_ref(), Some(&receipt));
        let retained = recovered
            .output(attempt)
            .await?
            .ok_or_else(|| Error::Storage("reopen lost stopped-capture output".into()))?;
        assert_eq!(&retained, result);
        let reopened_output: NativeProcessResult =
            crate::contract::json_from_slice(&fixture.content.read(&retained).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(reopened_output.stop.as_ref(), Some(&stop));
        let effects = fixture.effects(recovered.clone())?;
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            status
        );
        assert_eq!(recovered.receipt(attempt).await?, Some(receipt));
        assert_eq!(
            fixture
                .owner
                .stream()
                .bounds(&format!("harness/v2/native-process/{attempt}"))
                .await?
                .tail,
            2,
        );
        if mode == "timeout" {
            tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
            assert!(
                !fixture
                    .view
                    .manifest()
                    .options
                    .root
                    .join("escaped")
                    .exists()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn approved_bash_preserves_ordered_arguments_and_selected_environment() -> Result<()> {
    let fixture = process_fixture("bash", true).await?;
    assert_eq!(fixture.view.manifest().volumes.len(), 3);
    assert_ne!(
        fixture.view.manifest().volumes[0].volume.provider(),
        fixture.view.manifest().volumes[2].volume.provider()
    );
    let effects = fixture.effects(fixture.provider().await?)?;
    let status = effects
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = status else {
        return Err(Error::Storage(format!("Bash did not complete: {status:?}")));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&result).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(
        output.success,
        "Bash exit {:?}: {}",
        output.exit_code,
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"approved-bash|first arg|second arg");
    assert_eq!(output.stderr, b"approved-env");
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/output.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/auxiliary.txt", 64)
            .await?
            .as_ref(),
        b"auxiliary input"
    );
    Ok(())
}

#[tokio::test]
async fn bash_source_mutation_preserves_sdk_sources_and_rejects_all_publication() -> Result<()> {
    let fixture = process_fixture("mutate-source-bash", true).await?;
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name()?,
    )?;
    let before = fixture.files.resolve(&source).await?.generation;
    let process = fixture.provider().await?;
    let effects = fixture.effects(process.clone())?;
    assert_eq!(
        effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?,
        EffectStatus::Indeterminate
    );
    assert_eq!(fixture.files.resolve(&source).await?.generation, before);
    assert_eq!(
        fixture
            .files
            .read(&source, None, "/input.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert!(
        fixture
            .files
            .read(&destination, None, "/output.txt", 64)
            .await
            .is_err()
    );
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(fixture.owner.task_binding().0, fixture.command)?.into_bytes(),
    );
    let output = process
        .output(attempt)
        .await?
        .ok_or_else(|| Error::Storage("source mutation lost observed output".into()))?;
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&output).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(output.success);
    std::fs::remove_file(fixture.view.manifest().options.root.join("dst/output.txt"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?,
        EffectStatus::Indeterminate
    );
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn approved_bash_uses_nineteen_independently_selected_volumes() -> Result<()> {
    let fixture = process_fixture("many-bash", true).await?;
    assert_eq!(fixture.view.manifest().volumes.len(), 19);
    let status = fixture
        .effects(fixture.provider().await?)?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    assert!(matches!(status, EffectStatus::Succeeded { .. }));
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/many.txt", 512)
            .await?
            .as_ref(),
        "extra input|".repeat(16).as_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn stock_task_commands_select_existing_effect_registry_explicitly() -> Result<()> {
    use crate::filesystem::{EFFECT_TASK_COMMAND_KIND, TaskCommandHost as _, TaskCommandProgress};
    let fixture = process_fixture("write", true).await?;
    let (task, fence) = fixture.owner.task_binding();
    let context = fixture
        .task
        .harness()
        .durable_context(task, OperationId::from_bytes(task.into_bytes()))
        .await?;
    let payload = crate::contract::canonical_json_bytes(&fixture.plan)?;
    let payload_ref = fixture
        .results
        .stage(
            fixture.command,
            "process/effect-command.json",
            &payload,
            "application/json",
            "effect-command.json",
        )
        .await?;
    let command = WorkflowCommand {
        operation_id: fixture.command,
        kind: EFFECT_TASK_COMMAND_KIND.into(),
        payload: payload_ref,
    };
    let value =
        serde_json::to_value(&fixture.plan).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(
        fixture
            .task
            .commands()
            .execute(
                context.clone(),
                fence.clone(),
                command.clone(),
                value.clone()
            )
            .await,
        Err(Error::Unsupported(_))
    ));
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    let commands = fixture
        .task
        .commands()
        .with_effects(Arc::new(fixture.effects(fixture.provider().await?)?));
    let TaskCommandProgress::Ready(result) = commands
        .execute(
            context.clone(),
            fence.clone(),
            command.clone(),
            value.clone(),
        )
        .await?
    else {
        return Err(Error::Storage("native command was not ready".into()));
    };
    let Outcome::Succeeded(file) = serde_json::from_value::<Outcome<FileRef>>(result.clone())
        .map_err(|error| Error::Invalid(error.to_string()))?
    else {
        return Err(Error::Storage("native command was not successful".into()));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&file).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(output.success);
    let TaskCommandProgress::Ready(replayed) = commands
        .execute(context, fence.clone(), command.clone(), value)
        .await?
    else {
        return Err(Error::Storage("native command replay was not ready".into()));
    };
    assert_eq!(result, replayed);
    let mut session = fixture.task.open_task(task, fence.clone()).await?;
    session
        .step(
            OperationId::from_bytes([66; 16]),
            IdempotencyKey::new("native-workflow-command")?,
            serde_json::to_value(command).map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .await?;
    assert!(
        matches!(fixture.task.run_task(fixture.lease.clone(), &commands, 2).await?, crate::filesystem::TaskWorkerOutcome::Completed { task: completed } if completed == task)
    );
    assert!(
        matches!(fixture.task.task_host().outcome(task).await?, Some(Outcome::Succeeded(value)) if value["commands"][0]["value"] == result)
    );
    Ok(())
}

#[tokio::test]
async fn nonzero_exit_preserves_output_and_is_not_implicitly_replayed() -> Result<()> {
    let fixture = process_fixture("nonzero", true).await?;
    let process = fixture.provider().await?;
    let effects = fixture.effects(process)?;
    let status = effects
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = &status else {
        return Err(Error::Storage(format!(
            "nonzero invocation unobserved: {status:?}"
        )));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(result).await?)
            .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(!output.success);
    assert_eq!(output.exit_code, Some(22));
    assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("approved stderr"));
    std::fs::remove_file(fixture.view.manifest().options.root.join("dst/output.txt"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?,
        status
    );
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn real_process_requires_approval_and_durable_dispatch_then_reconciles_receipt() -> Result<()>
{
    let denied = process_fixture("write", false).await?;
    assert!(matches!(
        denied.provider().await,
        Err(Error::Unauthorized(_))
    ));
    assert!(
        !denied
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    let fixture = process_fixture("write", true).await?;
    let process = fixture.provider().await?;
    let effect_id = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
    let attempt = crate::EffectAttemptId::from_bytes(effect_id.into_bytes());
    assert_eq!(process.receipt(attempt).await?, None);
    let unadmitted = EffectDispatch {
        provider: fixture.plan.provider.clone(),
        effect_id,
        attempt_id: attempt,
        effect_kind: fixture.plan.effect_kind.clone(),
        request: fixture.plan.request.clone(),
        guarantee: fixture.plan.guarantee,
        request_digest: [0; 32],
    };
    assert!(matches!(
        process.dispatch(unadmitted).await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(process.receipt(attempt).await?, None);
    assert_eq!(
        fixture
            .owner
            .stream()
            .bounds(&format!("harness/v2/native-process/{attempt}"))
            .await?
            .tail,
        0,
    );
    let status = fixture
        .effects(process.clone())?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = &status else {
        return Err(Error::Storage(format!(
            "native process did not complete: {status:?}"
        )));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(result).await?)
            .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(output.success);
    assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("approved stderr"));
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/output.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&source, None, "/input.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let receipt = process
        .receipt(attempt)
        .await?
        .ok_or_else(|| Error::Storage("missing successful native receipt".into()))?;
    assert_native_receipt_binding(
        &fixture.owner,
        fixture.command,
        &fixture.plan,
        &receipt,
        &status,
        true,
    )?;
    assert_eq!(receipt.diagnostic, None);
    assert_eq!(receipt.publication_failure, None);
    assert_eq!(receipt.invocation_failure, None);
    assert!(receipt.work.backend_read_operations > 0);
    assert!(receipt.work.backend_write_operations > 0);
    let weak = Arc::downgrade(&process);
    drop(process);
    assert!(weak.upgrade().is_none());
    let recovered: Process = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        fixture.plan.clone(),
        fixture.approval,
        fixture.authority(),
    )
    .await?;
    assert_eq!(
        recovered
            .reconcile(attempt)
            .await?
            .map(|receipt| receipt.status),
        Some(status)
    );
    assert_eq!(
        crate::contract::canonical_json_bytes(&recovered.receipt(attempt).await?)?,
        crate::contract::canonical_json_bytes(&Some(&receipt))?,
    );
    assert_eq!(recovered.receipt(attempt).await?, Some(receipt));
    Ok(())
}

#[tokio::test]
async fn cancellation_after_actual_provider_drop_keeps_receipt_and_cleans_descendants() -> Result<()>
{
    let fixture = process_fixture("wait", true).await?;
    let process = fixture.provider().await?;
    let weak = Arc::downgrade(&process);
    let effects = fixture.effects(process.clone())?;
    let owner = fixture.owner.clone();
    let plan = fixture.plan.clone();
    let command = fixture.command;
    let dispatch =
        tokio::spawn(async move { effects.run_task_effect(&owner, command, plan).await });
    let root = fixture.view.manifest().options.root.clone();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root.join("ready").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    dispatch.abort();
    let _ = dispatch.await;
    drop(process);
    assert!(weak.upgrade().is_none());
    fixture
        .task
        .task_host()
        .cancel(fixture.owner.task_binding().0)
        .await?;
    let recovered = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        fixture.plan.clone(),
        fixture.approval,
        fixture.authority(),
    )
    .await?;
    let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
    let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
    let path = format!("harness/v2/native-process/{attempt}");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while fixture.owner.stream().bounds(&path).await?.tail != 2 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Result::Ok(())
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))??;
    let captured = recovered
        .output(attempt)
        .await?
        .ok_or_else(|| Error::Storage("missing cancellation output".into()))?;
    let receipt = recovered
        .receipt(attempt)
        .await?
        .ok_or_else(|| Error::Storage("missing cancelled native receipt".into()))?;
    assert_native_receipt_binding(
        &fixture.owner,
        fixture.command,
        &fixture.plan,
        &receipt,
        &EffectStatus::Succeeded { result: captured.clone() },
        true,
    )?;
    assert_eq!(receipt.diagnostic, None);
    assert_eq!(receipt.publication_failure, None);
    assert_eq!(receipt.invocation_failure, None);
    // Cancellation cleanup retains OS output but performs no view publication.
    assert!(receipt.work.allocation_operations > 0);
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&captured).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(!output.success);
    assert!(
        matches!(output.stop.as_ref(), Some(stop) if stop.kind == NativeProcessStopKind::ControlStopped && stop.cleanup_completed && !stop.message.is_empty())
    );
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    assert!(!root.join("escaped").exists());
    assert!(fixture.owner.verify(false).await.is_err());
    assert!(fixture.owner.verify(true).await.is_ok());
    drop(recovered);
    let recovered = Arc::new(
        Process::recover(
            fixture.owner.clone(),
            fixture.command,
            fixture.plan.clone(),
            fixture.approval,
            fixture.authority(),
        )
        .await?,
    );
    assert_eq!(recovered.receipt(attempt).await?.as_ref(), Some(&receipt));
    let retained = recovered
        .output(attempt)
        .await?
        .ok_or_else(|| Error::Storage("reopen lost cancellation cleanup output".into()))?;
    assert_eq!(retained, captured);
    let reopened_output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&retained).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(reopened_output, output);
    let known = fixture
        .effects(recovered.clone())?
        .reconcile_task_effect(&fixture.owner, fixture.command, &fixture.plan)
        .await?;
    assert_eq!(known, EffectStatus::Succeeded { result: captured });
    assert_eq!(recovered.receipt(attempt).await?, Some(receipt));
    assert_eq!(fixture.owner.stream().bounds(&path).await?.tail, 2);
    let (task, fence) = fixture.owner.task_binding();
    fixture
        .task
        .task_host()
        .settle_task(task, fence, Outcome::Cancelled)
        .await?;
    assert_eq!(
        fixture.task.task_host().outcome(task).await?,
        Some(Outcome::Cancelled)
    );
    assert!(fixture.owner.verify(true).await.is_err());
    assert!(recovered.receipt(attempt).await.is_err());
    assert!(fixture.directory.path().exists());
    Ok(())
}

#[tokio::test]
async fn native_root_and_dynamic_validation_admit_control_storage_before_io() -> Result<()> {
    let fixture = process_fixture("write", true).await?;
    let original = fixture.view.manifest().validation_budget()?;
    for allocation_operations in [0, 1] {
        let budget = acyclic_fs::WorkBudget { allocation_operations, ..original };
        let failed = <PreparationView as crate::filesystem::NativeProcessView<MemoryStream>>::validate_working_directory(
            &fixture.view, &fixture.view.manifest().options.root, budget,
        ).err().ok_or_else(|| Error::Storage("unadmitted dynamic validation frame".into()))?;
        assert_eq!(failed.1.allocation_operations, allocation_operations);
        assert_eq!(failed.1.backend_read_operations, 0);
        assert_eq!(failed.1.backend_write_operations, 0);
    }
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let root = directory.path().join("root");
    let operation = directory.path().join("operation");
    for path in [&root, &operation, &operation.join("target"), &operation.join("backup")] {
        std::fs::create_dir(path).map_err(|error| Error::Storage(error.to_string()))?;
    }
    let denied = NativePreparationRoot::witness(
        root, operation, acyclic_fs::WorkBudget { peak_allocation_bytes: 0, ..original },
        &acyclic_fs::CancellationToken::new(),
    ).await.err().ok_or_else(|| Error::Storage("unadmitted root witness control".into()))?;
    assert_eq!(denied.1.allocation_operations, 0);
    assert_eq!(denied.1.backend_read_operations, 0);
    assert_eq!(denied.1.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn native_preparation_adopts_actual_input_vector_capacity_before_root_io() -> Result<()> {
    let seed = process_fixture("write", true).await?;
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = preparation_fixture_on(StreamClient::new(stream), &seed).await?;
    let mut bindings = borrow_preparation_bindings(&fixture.bindings);
    bindings.reserve_exact(4096);
    let input_capacity = bindings.capacity() * std::mem::size_of::<BorrowedNativeVolumeBinding<'_, MemoryAuthorityBackend, MemoryObjectBackend>>();
    let mut options = fixture.options.clone();
    options.work_per_volume.peak_allocation_bytes = (input_capacity - 1) as u64;
    let failed = PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), options, bindings, &acyclic_fs::CancellationToken::new(),
    ).await.err().ok_or_else(|| Error::Storage("input capacity outside original allowance".into()))?;
    // The control ledger and the admitted preparation frame are both real
    // allocations; input custody is adopted without another allocation.
    assert_eq!(failed.1.allocation_operations, 2);
    assert_eq!(failed.1.backend_read_operations, 0);
    assert_eq!(failed.1.backend_write_operations, 0);
    assert!(std::fs::read_dir(&fixture.allocation.root)
        .map_err(|error| Error::Storage(error.to_string()))?.next().is_none());
    Ok(())
}

#[tokio::test]
async fn native_preparation_frame_admission_fails_before_body_or_root_io() -> Result<()> {
    let seed = process_fixture("write", true).await?;
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = preparation_fixture_on(StreamClient::new(stream), &seed).await?;
    for allocation_operations in [0, 1] {
        let mut options = fixture.options.clone();
        options.work_per_volume.allocation_operations = allocation_operations;
        let failed = PreparationView::prepare(
            &fixture.owner, fixture.allocation.clone(), options,
            borrow_preparation_bindings(&fixture.bindings[..1]),
            &acyclic_fs::CancellationToken::new(),
        ).await.err().ok_or_else(|| Error::Storage("unadmitted preparation frame".into()))?;
        assert_eq!(failed.1.allocation_operations, allocation_operations);
        assert_eq!(failed.1.backend_read_operations, 0);
        assert_eq!(failed.1.backend_write_operations, 0);
        assert!(std::fs::read_dir(&fixture.allocation.root)
            .map_err(|error| Error::Storage(error.to_string()))?.next().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn native_preparation_bounds_shared_codec_before_physical_root_open() -> Result<()> {
    let seed = process_fixture("write", true).await?;
    assert!(seed.view.manifest().volumes.iter().all(|mapping| {
        matches!(mapping.namespace, NativeVolumeNamespace::MaterializedDirectory)
    }));
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = preparation_fixture_on(StreamClient::new(stream), &seed).await?;
    let mut options = fixture.options.clone();
    options.work_per_volume.bytes_encoded = 0;
    let failed = PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), options,
        borrow_preparation_bindings(&fixture.bindings[..1]),
        &acyclic_fs::CancellationToken::new(),
    ).await.err().ok_or_else(|| Error::Storage("unadmitted canonical preparation intent".into()))?;
    assert_eq!(failed.1.bytes_encoded, 0);
    assert_eq!(failed.1.backend_read_operations, 0);
    assert_eq!(failed.1.backend_write_operations, 0);
    assert!(std::fs::read_dir(&fixture.allocation.root)
        .map_err(|error| Error::Storage(error.to_string()))?.next().is_none());
    Ok(())
}

#[tokio::test]
async fn native_preparation_decode_failure_retains_original_journal_read_work() -> Result<()> {
    let seed = process_fixture("write", true).await?;
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = preparation_fixture_on(StreamClient::new(stream), &seed).await?;
    let prepared = PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings),
        &acyclic_fs::CancellationToken::new(),
    ).await.map_err(|(error, _)| error)?;
    let budget = acyclic_fs::WorkBudget {
        items_examined: 0, ..prepared.value.manifest().validation_budget()?
    };
    let failed = PreparationView::inspect_preparation(
        &fixture.owner, &fixture.allocation, budget, &acyclic_fs::CancellationToken::new(),
    ).await.err().ok_or_else(|| Error::Storage("unadmitted original preparation decode".into()))?;
    assert_eq!(failed.1.items_examined, 0);
    assert!(failed.1.authority_records_read > 0);
    assert!(failed.1.authority_bytes_read > 0);
    assert!(failed.1.peak_allocation_bytes > 0);
    assert_eq!(failed.1.backend_write_operations, 0);
    let read_budget = acyclic_fs::WorkBudget {
        authority_bytes_read: acyclic_stream::MAX_RECORD_BYTES as u64 - 1,
        ..prepared.value.manifest().validation_budget()?
    };
    let denied = PreparationView::inspect_preparation(
        &fixture.owner, &fixture.allocation, read_budget, &acyclic_fs::CancellationToken::new(),
    ).await.err().ok_or_else(|| Error::Storage("unadmitted bounded journal fetch".into()))?;
    assert_eq!(denied.1.authority_records_read, 0);
    assert_eq!(denied.1.authority_bytes_read, 0);
    assert_eq!(denied.1.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn native_preparation_partial_typed_decode_keeps_acquired_quota_receipt() -> Result<()> {
    let seed = process_fixture("write", true).await?;
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = preparation_fixture_on(StreamClient::new(stream), &seed).await?;
    let prepared = PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings),
        &acyclic_fs::CancellationToken::new(),
    ).await.map_err(|failure| Error::Storage(failure.to_string()))?;
    let journal = &prepared.value.preparation_receipts()[0].journal;
    // The actual original fenced fault host writes a malformed next record to
    // the SAME journal. No recovery archive or provider-created authority.
    let mut invalid = serde_json::to_value(journal).map_err(|error| Error::Storage(error.to_string()))?;
    invalid["revision"] = serde_json::json!(journal.revision + 1);
    invalid["work"]["itemsExamined"] = serde_json::json!("invalid counter");
    let bytes = crate::contract::canonical_json_bytes(&invalid)?;
    let encoded_length = bytes.len() as u64;
    let operation = journal.plan.operation_id;
    let task = fixture.owner.task_binding().0;
    let path = acyclic_stream::StreamPath::new(format!("harness/v2/materialization/{task}/{operation}"))?;
    assert!(fixture.owner.append(
        path, journal.revision, &acyclic_stream::IdempotencyKey::new("malformed-native-journal")?,
        bytes::Bytes::from(bytes), crate::distributed::JournalWrite::Fresh,
    ).await?);
    let failed = PreparationView::inspect_preparation(
        &fixture.owner, &fixture.allocation, prepared.value.manifest().validation_budget()?,
        &acyclic_fs::CancellationToken::new(),
    ).await.err().ok_or_else(|| Error::Storage("partial typed journal decode unexpectedly succeeded".into()))?;
    assert!(failed.1.authority_bytes_read >= encoded_length);
    // The acquired schema quota, including temporary serde scratch, is real
    // ledger highwater even though typed construction fails before handoff.
    assert!(failed.1.peak_allocation_bytes > encoded_length);
    assert_eq!(failed.1.backend_write_operations, 0);
    assert_eq!(std::fs::read(fixture.allocation.root.join("a-source/input.txt"))
        .map_err(|error| Error::Storage(error.to_string()))?, b"pinned input");
    Ok(())
}

#[tokio::test]
async fn native_allocation_recovery_never_adopts_pending_directory_identity() -> Result<()> {
    let seed = process_fixture("write", true).await?;
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = preparation_fixture_on(StreamClient::new(stream.clone()), &seed).await?;
    *stream.preparation_fault.lock().await = Some(PreparationCommitFault {
        phase: acyclic_fs::MaterializationPhase::Preparing, preimages: 0, applied: 0, lost_reply: true,
    });
    let failed = PreparationView::prepare(
        &fixture.owner, fixture.allocation.clone(), fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings), &acyclic_fs::CancellationToken::new(),
    ).await.err().ok_or_else(|| Error::Storage("original preparation acknowledgement was not lost".into()))?;
    assert!(failed.1.backend_read_operations > 0);
    let original = PreparationView::inspect_preparation(
        &fixture.owner, &fixture.allocation, fixture.options.work_per_volume,
        &acyclic_fs::CancellationToken::new(),
    ).await.map_err(|(error, _)| error)?.value
        .ok_or_else(|| Error::NotFound("original preparation allocation journal".into()))?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let witness = acyclic_fs::NativeMaterializationWork::new(fixture.options.work_per_volume, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let parent = witness.root_identity(fixture.allocation.operation_directory.parent()
        .ok_or_else(|| Error::Storage("original allocation parent is absent".into()))?)
        .map_err(|error| Error::Storage(error.to_string()))?;
    // The original fenced fault host retains its known operation allocation,
    // but the target mkdir acknowledgement carries no durable target identity.
    // Existing physical directories must not fill that gap.
    let journal = original.journal;
    let mut pending = serde_json::to_value(&journal).map_err(|error| Error::Storage(error.to_string()))?;
    pending["revision"] = serde_json::json!(journal.revision + 1);
    pending["preparation_identity"] = serde_json::Value::Null;
    pending["preparation_progress"] = serde_json::json!({
        "parent": parent.to_bytes(),
        "operation": fixture.allocation.expected_preparation_identity.operation,
        "target": null, "backup": null, "pending": "Target",
    });
    let operation = journal.plan.operation_id;
    let task = fixture.owner.task_binding().0;
    assert!(fixture.owner.append(
        acyclic_stream::StreamPath::new(format!("harness/v2/materialization/{task}/{operation}"))?,
        journal.revision, &acyclic_stream::IdempotencyKey::new("pending-native-allocation")?,
        bytes::Bytes::from(crate::contract::canonical_json_bytes(&pending)?),
        crate::distributed::JournalWrite::Fresh,
    ).await?);
    let failed = PreparationView::recover_allocation(
        &fixture.owner, &fixture.options, fixture.options.work_per_volume, &cancellation,
    ).await.err().ok_or_else(|| Error::Storage("pending allocation was adopted from physical directories".into()))?;
    assert!(failed.0.to_string().contains("incomplete or indeterminate"));
    assert!(failed.1.authority_bytes_read > 0);
    assert_eq!(failed.1.backend_write_operations, 0);
    assert!(fixture.allocation.operation_directory.join("target").is_dir());
    Ok(())
}

#[tokio::test]
async fn literal_stdin_approval_binds_exact_bytes_and_explicit_empty_eof() -> Result<()> {
    let fixture = process_fixture("write", true).await?;
    let mut request = fixture.request().await?;
    let task = fixture.owner.task_binding().0;
    let absent = request.approval_digest(task, fixture.command)?;
    request.standard_input = Some(Vec::new());
    let eof = request.approval_digest(task, fixture.command)?;
    assert_ne!(absent, eof);
    request.standard_input = Some(vec![0, 255, b'\n', b'x']);
    let literal = request.approval_digest(task, fixture.command)?;
    assert_ne!(eof, literal);
    let encoded = crate::contract::canonical_json_bytes(&request)?;
    let decoded: NativeProcessRequest = crate::contract::json_from_slice(&encoded)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(decoded.standard_input.as_deref(), Some([0, 255, b'\n', b'x'].as_slice()));
    request.standard_input = Some(vec![0, 254, b'\n', b'x']);
    assert_ne!(literal, request.approval_digest(task, fixture.command)?);
    Ok(())
}

#[tokio::test]
async fn approved_literal_stdin_reaches_eof_without_terminating_the_process() -> Result<()> {
    for (mode, expected) in [
        ("stdin-empty", [].as_slice()),
        ("stdin-bytes", [0, 255, b'\n', b'x'].as_slice()),
    ] {
        let fixture = process_fixture(mode, true).await?;
        let effects = fixture.effects(fixture.provider().await?)?;
        let status = effects.run_task_effect(
            &fixture.owner, fixture.command, fixture.plan.clone(),
        ).await?;
        let EffectStatus::Succeeded { result } = status else {
            return Err(Error::Storage(format!("literal stdin did not finish normally: {status:?}")));
        };
        let output: NativeProcessResult = crate::contract::json_from_slice(
            &fixture.content.read(&result).await?,
        ).map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(output.success && output.stop.is_none());
        assert_eq!(output.exit_code, Some(0));
        // The real child only writes its artifact after read_to_end observes
        // EOF, then prints this sentinel and exits normally.
        assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
        let destination = workspace_ref(
            fixture.destination.provider().clone(), &fixture.destination.storage_name()?,
        )?;
        assert_eq!(fixture.files.read(&destination, None, "/output.txt", 64).await?.as_ref(), expected);
    }
    Ok(())
}

#[tokio::test]
async fn even_exactly_approved_mcp_requests_cannot_also_supply_literal_stdin() -> Result<()> {
    for mode in ["mcp-stdin-empty", "mcp-stdin-bytes"] {
        // These exact bytes and the MCP descriptor are present before the real
        // original approval is issued; denial is not an approval mismatch.
        let fixture = process_fixture(mode, true).await?;
        let request = fixture.request().await?;
        assert!(request.mcp_stdio.is_some() && request.standard_input.is_some());
        assert!(fixture.provider().await.is_err());
        assert!(!fixture.view.manifest().options.root.join("dst/output.txt").exists());
    }
    Ok(())
}

#[tokio::test]
async fn native_publication_factory_pins_original_generation_and_retains_denied_cleanup() -> Result<()> {
    for stale in [false, true] {
        let fixture = process_fixture("write", true).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let cleanup_cancellation = acyclic_fs::CancellationToken::new();
        let budget = fixture.view.manifest().validation_budget()?;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let destination = workspace_ref(
            fixture.destination.provider().clone(), &fixture.destination.storage_name()?,
        )?;
        let workspace = fixture.files.open_volume_with_native_work(&fixture.destination, &work).await?;
        let mapping = fixture.view.manifest().volumes.iter().find(|mapping| mapping.volume == fixture.destination)
            .ok_or_else(|| Error::NotFound("original destination mapping".into()))?;
        let selected = fixture.files.generation_with_native_work(&workspace, &mapping.generation, &work).await?;
        let original_generation = selected.id();
        drop(selected);
        let operation = acyclic_fs::OperationId::from_bytes([91; 16]);
        let before_query = work.receipt().backend_write_operations;
        let missing = fixture.owner.native_view_owner().recover_publication(
            &workspace, &fixture.destination, original_generation, operation,
            &work, &cancellation, &cleanup_cancellation,
        ).await?;
        assert!(missing.is_none());
        assert_eq!(work.receipt().backend_write_operations, before_query);
        let recovery_workspace = workspace.clone();
        if stale {
            fixture.files.write(&destination, "/concurrent.txt", b"new original head").await?;
        }
        // The host declares this distinct original filesystem operation before
        // requesting its actual gate; it is neither a task fence nor prep ID.
        let reserved = fixture.owner.native_view_owner().reserve_publication(
            &workspace, &fixture.destination, original_generation, operation,
            &work, &cancellation, &cleanup_cancellation,
        ).await;
        if stale {
            let failed = reserved.err().ok_or_else(|| Error::Storage("stale mapped generation received a permit".into()))?;
            let failed = crate::filesystem::NativePublicationReservationFailure::<MemoryAuthorityBackend, MemoryObjectBackend>
                ::from_error(&failed).expect("whole stale-generation reservation failure");
            assert!(failed.error.to_string().contains("differs from original selection"));
            assert!(failed.cleanup.is_none() && failed.cleanup_error.is_none());
            assert!(failed.unwitnessed_workspace.is_none());
            assert!(failed.work.backend_read_operations > 0);
            // The actual SDK write succeeds only after the mismatched gate
            // has really been released, not merely marked cleanup-complete.
            fixture.files.write(&destination, "/release-check.txt", b"settled").await?;
        } else {
            let receipt = reserved?;
            assert!(matches!(receipt.permit(), acyclic_fs::PublicationPermit::Reservation {
                operation_id, ..
            } if operation_id == operation.to_bytes()));
            let before_query = work.receipt().backend_write_operations;
            let source_mapping = fixture.view.manifest().volumes.iter()
                .find(|mapping| mapping.volume == fixture.source)
                .ok_or_else(|| Error::NotFound("original source generation mapping".into()))?;
            let source_digest = source_mapping.generation.as_resource().key().try_into()
                .map_err(|_| Error::Invalid("original source generation digest width".into()))?;
            let other_original_generation = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(source_digest));
            assert_ne!(other_original_generation, original_generation);
            let mismatched = fixture.owner.native_view_owner().recover_publication(
                &recovery_workspace, &fixture.destination, other_original_generation, operation,
                &work, &cancellation, &cleanup_cancellation,
            ).await.err().ok_or_else(|| Error::Storage("readonly query adopted a different original generation".into()))?;
            let mismatched_failure = crate::filesystem::NativePublicationReservationFailure::<MemoryAuthorityBackend, MemoryObjectBackend>
                ::from_error(&mismatched).expect("whole readonly selection failure");
            assert!(matches!(mismatched_failure.error, crate::filesystem::NativeReservationError::Selection(_)));
            assert!(mismatched_failure.cleanup.is_some());
            assert_eq!(mismatched_failure.work.backend_write_operations, before_query);
            // Read-only failure drops no physical gate: the next query still
            // returns the identical real proof held by the original owner.
            drop(mismatched);
            let reopened = fixture.owner.native_view_owner().recover_publication(
                &recovery_workspace, &fixture.destination, original_generation, operation,
                &work, &cancellation, &cleanup_cancellation,
            ).await?;
            let recovered = reopened.ok_or_else(|| Error::NotFound("actual original SDK gate after query".into()))?;
            assert_eq!(recovered.permit(), receipt.permit());
            assert_eq!(work.receipt().backend_write_operations, before_query);
            drop(recovered);
            let foreign = fixture.owner.native_view_owner().recover_publication(
                &recovery_workspace, &fixture.destination, original_generation,
                acyclic_fs::OperationId::from_bytes([92; 16]),
                &work, &cancellation, &cleanup_cancellation,
            ).await?;
            assert!(foreign.is_none());
            assert_eq!(work.receipt().backend_write_operations, before_query);
            let denied_work = acyclic_fs::NativeMaterializationWork::new(budget, &cleanup_cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let mut failed = receipt.into_cleanup().release(&fixture.owner.native_view_owner(), &denied_work, &cleanup_cancellation).await
                .err().ok_or_else(|| Error::Storage("foreign cleanup family denial was not exercised".into()))?;
            let retained = crate::filesystem::NativePublicationReservationFailure::<MemoryAuthorityBackend, MemoryObjectBackend>
                ::from_error(&failed).expect("whole denied cleanup owner and cause");
            assert_eq!(retained.work.backend_write_operations, 0);
            assert!(matches!(retained.error, crate::filesystem::NativeReservationError::Selection(_)));
            assert!(retained.cleanup.is_some());
            let original_failed_work = retained.work;
            let original_before_retry = work.receipt();
            let foreign_before_retry = denied_work.receipt();
            assert!(matches!(crate::filesystem::NativePublicationReservationFailure::<MemoryAuthorityBackend, MemoryObjectBackend>
                ::retry_cleanup_with_native_work(&mut failed, &fixture.owner.native_view_owner(), &denied_work, &cleanup_cancellation).await,
                Err(crate::filesystem::NativeReservationError::Selection(_))));
            assert_eq!(denied_work.receipt(), foreign_before_retry);
            assert_eq!(work.receipt(), original_before_retry);
            assert!(crate::filesystem::NativePublicationReservationFailure::<MemoryAuthorityBackend, MemoryObjectBackend>
                ::retry_cleanup_with_native_work(&mut failed, &fixture.owner.native_view_owner(), &work, &cleanup_cancellation)
                .await.expect("actual original gate cleanup"));
            let retained = crate::filesystem::NativePublicationReservationFailure::<MemoryAuthorityBackend, MemoryObjectBackend>
                ::from_error(&failed).expect("original failure retained after actual cleanup");
            assert!(retained.cleanup.is_none());
            assert_eq!(retained.work, original_failed_work);
            assert!(work.receipt().authority_records_read > original_before_retry.authority_records_read);
            fixture.files.write(&destination, "/release-check.txt", b"settled").await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn original_supervisor_stop_is_attempt_fenced_and_does_not_cancel_the_native_view() -> Result<()> {
    let fixture = process_fixture("wait", true).await?;
    let process = fixture.provider().await?;
    let effects = fixture.effects(process.clone())?;
    let owner = fixture.owner.clone();
    let command = fixture.command;
    let plan = fixture.plan.clone();
    let dispatch = tokio::spawn(async move { effects.run_task_effect(&owner, command, plan).await });
    let root = fixture.view.manifest().options.root.clone();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root.join("ready").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.map_err(|error| Error::Storage(error.to_string()))?;
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(fixture.owner.task_binding().0, fixture.command)?.into_bytes(),
    );
    let cleanup_cancellation = acyclic_fs::CancellationToken::new();
    let budget = fixture.view.manifest().validation_budget()?;
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cleanup_cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    assert!(matches!(
        process.signal_stop(crate::EffectAttemptId::from_bytes([255; 16]), &work, &cleanup_cancellation).await,
        Err(crate::filesystem::NativeReceiptError::Record(_)),
    ));
    // A reopened provider can inspect the original journal, but it never owns
    // the warm supervisor and cannot hydrate control from launch intent/PID.
    let reopened = Process::recover(
        fixture.owner.clone(), fixture.command, fixture.plan.clone(),
        fixture.approval, fixture.authority(),
    ).await?;
    assert!(matches!(
        reopened.signal_stop(attempt, &work, &cleanup_cancellation).await,
        Err(crate::filesystem::NativeReceiptError::Record(_)),
    ));
    let mut denied_budget = budget;
    denied_budget.items_examined = 0;
    let denied = acyclic_fs::NativeMaterializationWork::new(denied_budget, &cleanup_cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    assert!(matches!(
        process.signal_stop(attempt, &denied, &cleanup_cancellation).await,
        Err(crate::filesystem::NativeReceiptError::Native(_)),
    ));
    assert!(!dispatch.is_finished());
    process.signal_stop(attempt, &work, &cleanup_cancellation).await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let receipt = process.request_stop(attempt, &work, &cleanup_cancellation).await
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(receipt.observed);
    assert!(work.receipt().items_examined > 0);
    let status = dispatch.await.map_err(|error| Error::Storage(error.to_string()))??;
    let EffectStatus::Succeeded { result } = &status else {
        return Err(Error::Storage(format!("owned stop lost its actual captured result: {status:?}")));
    };
    assert_native_receipt_binding(
        &fixture.owner, fixture.command, &fixture.plan, &receipt, &status, true,
    )?;
    let output: NativeProcessResult = crate::contract::json_from_slice(
        &fixture.content.read(result).await?,
    ).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(!output.success);
    assert!(matches!(
        output.stop.as_ref(),
        Some(stop) if stop.kind == NativeProcessStopKind::ControlStopped && stop.cleanup_completed,
    ));
    // Stop is physical supervisor control, not task cancellation, view token
    // cancellation, filesystem publication, or namespace release authority.
    fixture.owner.verify(false).await?;
    fixture.view.validate_for_dispatch(budget).await
        .map_err(|(cause, _)| cause)?;
    assert!(fixture.files.read(&workspace_ref(
        fixture.destination.provider().clone(), &fixture.destination.storage_name()?,
    )?, None, "/output.txt", 64).await.is_err());
    assert_eq!(reopened.receipt(attempt).await?, Some(receipt));
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    assert!(!root.join("escaped").exists());
    Ok(())
}

#[tokio::test]
async fn original_warm_supervisor_stop_settles_after_real_task_cancellation() -> Result<()> {
    let fixture = process_fixture("wait", true).await?;
    let process = fixture.provider().await?;
    let effects = fixture.effects(process.clone())?;
    let owner = fixture.owner.clone();
    let command = fixture.command;
    let plan = fixture.plan.clone();
    let dispatch = tokio::spawn(async move { effects.run_task_effect(&owner, command, plan).await });
    let root = fixture.view.manifest().options.root.clone();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root.join("ready").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.map_err(|error| Error::Storage(error.to_string()))?;
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(fixture.owner.task_binding().0, fixture.command)?.into_bytes(),
    );
    // True-close may have already durably cancelled the task. Physical stop
    // still requires its same original current settlement fence, not dispatch.
    fixture.task.task_host().cancel(fixture.owner.task_binding().0).await?;
    let cleanup_cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        fixture.view.manifest().validation_budget()?, &cleanup_cancellation,
    ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    process.signal_stop(attempt, &work, &cleanup_cancellation).await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let receipt = process.request_stop(attempt, &work, &cleanup_cancellation).await
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(receipt.observed);
    assert!(receipt.work.allocation_operations > 0);
    let captured = process.output(attempt).await?
        .ok_or_else(|| Error::Storage("cancelled original supervisor lost captured output".into()))?;
    let output: NativeProcessResult = crate::contract::json_from_slice(
        &fixture.content.read(&captured).await?,
    ).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(
        output.stop.as_ref(),
        Some(stop) if stop.kind == NativeProcessStopKind::ControlStopped && stop.cleanup_completed,
    ));
    // The enclosing effect caller is not the supervisor being joined. Its
    // acknowledgement may be fenced by task closure; the receipt is canonical.
    let _ = dispatch.await.map_err(|error| Error::Storage(error.to_string()))?;
    assert!(fixture.owner.verify(false).await.is_err());
    assert!(fixture.owner.verify(true).await.is_ok());
    Ok(())
}
