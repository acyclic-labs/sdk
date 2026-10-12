use super::*;
use crate::{
    AgentId, Capabilities,
    conversation::{Attachment, MessageKind, ReferencedAttachments, VolumeClass, VolumeOwner},
    core::{AggregateKind, AuthorityIssuer, SchemaRegistry},
    filesystem::{ExactTextReplacement, FilesystemContentVerifier, WorkspaceMutation, workspace_ref},
    resources::ProviderRef,
};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_stream::{MemoryStream};
use std::sync::atomic::{AtomicUsize, Ordering};

type Host = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;

struct Fixture {
    host: Arc<Host>,
    parent: StreamAggregate<MemoryStream>,
    issuer: AuthorityIssuer,
    scope: Scope,
    project: VolumeRef,
    source: VolumeRef,
    private: VolumeRef,
    source_generation: GenerationRef,
}

async fn fixture() -> Result<Fixture> {
    let provider = ProviderRef::new("recursive-default-test", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let agent = AgentId::from_bytes([1; 16]);
    let authority = Authority { kind: AggregateKind::Conversation, id: "recursive-default-parent".into() };
    let project = VolumeRef::new(provider.clone(), "recursive-default-project", VolumeClass::Project, VolumeOwner::Project("parent".into()))?;
    let source = VolumeRef::new(provider.clone(), "recursive-default-source", VolumeClass::Project, VolumeOwner::Project("child".into()))?;
    let private = VolumeRef::new(provider, "recursive-default-private", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
    host.create_volume(&project).await?;
    host.create_volume(&private).await?;
    let issuer = AuthorityIssuer::new("recursive-default-owner", [4; 32], authority.clone());
    let mut grants = vec!["conversation:bind".to_owned(), "conversation:append".to_owned(), "fork:publish".to_owned(), "project:merge".to_owned()];
    for volume in [&project, &source, &private] {
        grants.push(volume.capability(VolumeOperation::Read)?);
        grants.push(volume.capability(VolumeOperation::Write)?);
    }
    let scope = issuer.root_for_agent(agent, "parent-owner", Capabilities::new(grants));
    let stream = Arc::new(MemoryStream::default());
    let mut parent = StreamAggregate::open(&stream, authority, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()).await?;
    parent.execute(Command { operation_id: OperationId::from_bytes([2; 16]), idempotency_key: IdempotencyKey::new("bind-parent")?,
        expected_revision: 0, scope: scope.clone(), causal_parent: None, action: Action::BindConversation { agent } }).await?;
    let project_ref = workspace_ref(project.provider().clone(), &project.storage_name()?)?;
    let base = host.apply(&project_ref, None, &[WorkspaceMutation::PutFile { path: "/value.txt".into(), bytes: b"base".to_vec().into() }], &IdempotencyKey::new("base-value")?).await?;
    let controller = ParentProjectController::new(host.as_ref(), parent.reducer(), &issuer.verifier(&mut None).expect("verifier copy"), &scope, &project, None)?;
    controller.fork_project(&base, &source, &IdempotencyKey::new("fork-source")?).await?;
    let source_ref = workspace_ref(source.provider().clone(), &source.storage_name()?)?;
    let source_generation = host.apply(&source_ref, None, &[WorkspaceMutation::PutFile { path: "/value.txt".into(), bytes: b"child".to_vec().into() }], &IdempotencyKey::new("source-value")?).await?;
    Ok(Fixture { host, parent, issuer, scope, project, source, private, source_generation })
}

#[tokio::test]
async fn restored_original_target_cannot_rebase_over_a_concurrent_user_write() -> Result<()> {
    let fixture = fixture().await?;
    let controller = ParentProjectController::new(fixture.host.as_ref(), fixture.parent.reducer(), &fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.project, None)?;
    let inspected = controller.prepare_project_import(&fixture.source, &fixture.source_generation).await?;
    let target = fixture.host.generation_ref_id(inspected.target_head())?;
    let project = workspace_ref(fixture.project.provider().clone(), &fixture.project.storage_name()?)?;
    let user_generation = fixture.host.apply(&project, Some(&target), &[WorkspaceMutation::PutFile {
        path: "/value.txt".into(), bytes: b"concurrent user".to_vec().into(),
    }], &IdempotencyKey::new("concurrent-user-edit")?).await?;
    let restored = controller.prepare_project_import_at(&fixture.source, &fixture.source_generation, &target).await?;
    assert_eq!(restored.target_head(), inspected.target_head());
    assert!(matches!(controller.apply_project_merge(&restored, OperationId::from_bytes([20; 16])).await?, JoinOutcome::StaleTarget(_)));
    assert_eq!(fixture.host.read(&project, Some(&user_generation), "/value.txt", 64).await?.as_ref(), b"concurrent user");
    assert_eq!(fixture.host.resolve(&project).await?.generation, user_generation);
    Ok(())
}

#[tokio::test]
async fn restored_applied_plan_observes_original_receipt_without_second_write() -> Result<()> {
    let fixture = fixture().await?;
    let controller = ParentProjectController::new(fixture.host.as_ref(), fixture.parent.reducer(), &fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.project, None)?;
    let inspected = controller.prepare_project_import(&fixture.source, &fixture.source_generation).await?;
    let target = fixture.host.generation_ref_id(inspected.target_head())?;
    let effect = OperationId::from_bytes([21; 16]);
    let first = controller.apply_project_merge(&inspected, effect).await?;
    let JoinOutcome::Applied(first) = first else { return Err(Error::Invalid("expected real Filesystem publication".into())); };
    let result = fixture.host.generation_ref(first.generation())?;
    let restored = controller.prepare_project_import_at(&fixture.source, &fixture.source_generation, &target).await?;
    let retried = controller.apply_project_merge(&restored, effect).await?;
    let JoinOutcome::AlreadyApplied(retried) = retried else { return Err(Error::Invalid("retry did not observe original publication".into())); };
    assert_eq!(fixture.host.generation_ref(retried.generation())?, result);
    let project = workspace_ref(fixture.project.provider().clone(), &fixture.project.storage_name()?)?;
    assert_eq!(fixture.host.resolve(&project).await?.generation, result);
    Ok(())
}

#[tokio::test]
async fn restored_plan_requires_original_signed_source_read_grant() -> Result<()> {
    let fixture = fixture().await?;
    let caller_only = fixture.issuer.root_for_agent(AgentId::from_bytes([1; 16]), "destination-only", Capabilities::new([
        "project:merge".to_owned(), fixture.project.capability(VolumeOperation::Write)?,
    ]));
    let controller = ParentProjectController::new(fixture.host.as_ref(), fixture.parent.reducer(), &fixture.issuer.verifier(&mut None).expect("verifier copy"), &caller_only, &fixture.project, None)?;
    let project = workspace_ref(fixture.project.provider().clone(), &fixture.project.storage_name()?)?;
    let target = fixture.host.resolve(&project).await?.generation;
    assert!(matches!(controller.prepare_project_import_at(&fixture.source, &fixture.source_generation, &target).await, Err(Error::Unauthorized(_))));
    assert_eq!(fixture.host.resolve(&project).await?.generation, target);
    Ok(())
}

#[tokio::test]
async fn exact_edit_uses_source_generation_and_rejects_ambiguous_overlap() -> Result<()> {
    let fixture = fixture().await?;
    let read = ContentGrant::verify(&fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.private, VolumeOperation::Read)?;
    let write = ContentGrant::verify(&fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.private, VolumeOperation::Write)?;
    let source = fixture.host.put_content(&fixture.private, &write, "scratch/value.txt", b"aaa", "text/plain", "value.txt", 64, &IdempotencyKey::new("initial-edit-source")?).await?;
    assert!(fixture.host.edit_text(&source, &read, &write, &ExactTextReplacement { old_text: "aa".into(), new_text: "b".into() }, 64, &IdempotencyKey::new("ambiguous-edit")?).await.is_err());
    let changed = fixture.host.put_content(&fixture.private, &write, "scratch/value.txt", b"user", "text/plain", "value.txt", 64, &IdempotencyKey::new("user-edit")?).await?;
    assert!(matches!(fixture.host.edit_text(&source, &read, &write, &ExactTextReplacement { old_text: "aaa".into(), new_text: "child".into() }, 64, &IdempotencyKey::new("stale-exact-edit")?).await, Err(Error::Conflict(_))));
    assert_eq!(fixture.host.read_content(&changed, &read, 64).await?, b"user");
    assert_eq!(fixture.host.read_content(&source, &read, 64).await?, b"aaa");
    Ok(())
}

async fn merge_plan(fixture: &Fixture, effect: OperationId) -> Result<(RecursiveMergeToolPlan, FileRef)> {
    let write = ContentGrant::verify(&fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.private, VolumeOperation::Write)?;
    let file = fixture.host.put_content(&fixture.private, &write, "merge/notice.txt", b"explicit project import", "text/plain", "notice.txt", 4096, &IdempotencyKey::new("merge-notice")?).await?;
    let controller = ParentProjectController::new(fixture.host.as_ref(), fixture.parent.reducer(), &fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.project, None)?;
    let inspected = controller.prepare_project_import(&fixture.source, &fixture.source_generation).await?;
    let plan = RecursiveMergeToolPlan { arguments: MergeToolArguments { source: TaskId::from_bytes([9; 16]), selections: Vec::new() },
        caller: TaskId::from_bytes([8; 16]), operation_id: effect, destination: fixture.parent.reducer().authority().clone(),
        source: Authority { kind: AggregateKind::Conversation, id: "recursive-default-child".into() }, source_project: fixture.source.clone(),
        source_generation: fixture.source_generation.clone(), target_project: fixture.project.clone(), expected_target_generation: fixture.host.generation_ref_id(inspected.target_head())?,
        authorized_source: None,
        notice: ConversationMessage { id: uuid::Uuid::from_bytes([30; 16]), sequence: 1, kind: MessageKind::Merge,
            content: file, attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() }, reply_to: None, tool_call_id: None, extensions: BTreeMap::new() } };
    let command_payload = fixture.host.put_content(&fixture.private, &write, "merge/command.json", &crate::contract::canonical_json_bytes(&plan)?, "application/json", "command.json", 65536, &IdempotencyKey::new("merge-command")?).await?;
    Ok((plan, command_payload))
}

#[tokio::test]
async fn durable_merge_rejects_foreign_receipts_and_success_shaped_nonoutcomes() -> Result<()> {
    let fixture = fixture().await?;
    let effect = OperationId::from_bytes([31; 16]);
    let (plan, payload) = merge_plan(&fixture, effect).await?;
    let machine = RecursiveToolMachine::new(Kind::Merge, 65536);
    let initial = machine.initialize(&encode(&plan.arguments, None)?)?;
    let command = WorkflowCommand { operation_id: derived(effect, "merge-apply", None)?, kind: MERGE_APPLY.into(), payload: payload.clone() };
    let waiting = machine.transition(&initial, &encode(&(InitialPlan { plan: Plan::Merge(Box::new(plan.clone())), command: command.clone() }), None)?)?;
    assert!(matches!(waiting.status, MachineStatus::Suspended));
    assert_eq!(waiting.commands, vec![command.clone()]);
    assert!(machine.transition(&waiting.state, &encode(&(Receipt { operation_id: OperationId::from_bytes([99; 16]), value: Outcome::Succeeded(json!({"status":"no_changes"})), next: None }), None)?).is_err());
    assert!(machine.transition(&waiting.state, &encode(&(Receipt { operation_id: command.operation_id, value: Outcome::Succeeded(json!({"status":"success"})), next: None }), None)?).is_err());
    assert!(matches!(machine.transition(&waiting.state, &encode(&(Receipt { operation_id: command.operation_id,
        value: Outcome::Indeterminate { operation_id: effect }, next: None }), None)?), Err(Error::Indeterminate(operation)) if operation == effect));
    let controller = ParentProjectController::new(fixture.host.as_ref(), fixture.parent.reducer(), &fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.project, None)?;
    let inspected = controller.prepare_project_import_at(&plan.source_project, &plan.source_generation, &plan.expected_target_generation).await?;
    let outcome = controller.apply_project_merge(&inspected, effect).await?;
    let receipt = controller.merge_receipt(&inspected, &outcome, plan.source.clone(), effect, plan.notice.clone())?;
    let mut foreign = receipt.clone(); foreign.operation_id = OperationId::from_bytes([98; 16]);
    assert!(validate_merge_receipt(&plan, &foreign, None).is_err());
    let next = WorkflowCommand { operation_id: derived(effect, "merge-publish", None)?, kind: MERGE_PUBLISH.into(), payload };
    let published = machine.transition(&waiting.state, &encode(&(Receipt { operation_id: command.operation_id,
        value: Outcome::Succeeded(json!({"status":"applied","receipt":receipt})), next: Some(next) }), None)?)?;
    assert!(matches!(published.status, MachineStatus::Suspended), "Filesystem publication alone must not claim history publication");
    Ok(())
}

#[test]
fn fork_initialization_retains_raw_child_message_and_independent_source_choice() -> Result<()> {
    let machine = RecursiveToolMachine::new(Kind::Fork, 4096);
    let arguments = ForkToolArguments { id: AgentId::from_bytes([40; 16]), msg: "exact é\0🦀\r\nchild task".into(), source: Some(TaskId::from_bytes([41; 16])) };
    let value = encode(&arguments, None)?;
    let State::Ready { arguments: retained } = decode(&machine.initialize(&value)?, None)? else { return Err(Error::Invalid("fork initialized beyond admission boundary".into())); };
    assert_eq!(retained, value);
    assert_eq!(arguments.source_task(TaskId::from_bytes([42; 16]), crate::conversation::Limits::default(), &mut None)?, TaskId::from_bytes([41; 16]));
    let mut oversized = arguments; oversized.msg = "x".repeat(4097);
    assert!(machine.initialize(&encode(&(oversized), None)?).is_err());
    assert_ne!(derived(OperationId::from_bytes([43; 16]), "prepare", None)?, derived(OperationId::from_bytes([43; 16]), "preparation-task", None)?);
    Ok(())
}

struct RejectUnexpectedSource(AtomicUsize);
impl RecursiveDefaultToolSource<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend> for RejectUnexpectedSource {
    fn owner<'a>(&'a self, _: &'a ToolContext, _: &'a ResumableTool, _: &'a ToolInvocation,
        _: &'a CancellationToken) -> BoxProviderFuture<'a, Result<RecursiveToolOwner<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>>> {
        self.0.fetch_add(1, Ordering::SeqCst); Box::pin(async { Err(Error::Unauthorized("unexpected source access".into())) })
    }
    fn select<'a>(&'a self, _: &'a ToolContext, _: &'a ForkToolArguments,
        _: &'a CancellationToken) -> BoxProviderFuture<'a, Result<RecursiveForkSourceIntent>> {
        self.0.fetch_add(1, Ordering::SeqCst); Box::pin(async { Err(Error::Unauthorized("unexpected source access".into())) })
    }
    fn fork<'a>(&'a self, _: &'a ToolContext, _: &'a ForkToolArguments, _: &'a RecursiveForkSourceIntent,
        _: Option<&'a RecursiveForkToolPlan>, _: &'a CancellationToken)
        -> BoxProviderFuture<'a, Result<RecursiveForkToolBinding<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>>> {
        self.0.fetch_add(1, Ordering::SeqCst); Box::pin(async { Err(Error::Unauthorized("unexpected source access".into())) })
    }
    fn merge<'a>(&'a self, _: &'a ToolContext, _: &'a MergeToolArguments, _: Option<&'a RecursiveMergeToolPlan>,
        _: &'a CancellationToken) -> BoxProviderFuture<'a, Result<RecursiveMergeToolBinding<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>>> {
        self.0.fetch_add(1, Ordering::SeqCst); Box::pin(async { Err(Error::Unauthorized("unexpected source access".into())) })
    }
    fn preparation<'a>(&'a self, _: &'a TaskContext, _: &'a crate::resource_preparation::TaskResourcePreparation,
        _: &'a CancellationToken) -> BoxProviderFuture<'a, Result<RetainedSourceBinding<MemoryAuthorityBackend, MemoryObjectBackend>>> {
        self.0.fetch_add(1, Ordering::SeqCst); Box::pin(async { Err(Error::Unauthorized("unexpected source access".into())) })
    }
}

#[tokio::test]
async fn actual_registry_keeps_shared_file_tools_and_refuses_contextless_recursive_execution() -> Result<()> {
    let source = Arc::new(RejectUnexpectedSource(AtomicUsize::new(0)));
    let storage = super::super::MemoryHarnessStorage::new(AgentId::new(), 65536).await?;
    let ordinary = storage.default_tools(crate::conversation::Limits {
        file_bytes: 65536, ..Default::default()
    }, Some(source.clone()))?;
    let registry = recursive_default_tools(Some(source.clone()), 65536)?;
    for expected in ["acyclic.read_file", "acyclic.write_file", "acyclic.edit_file", "acyclic.fork", "acyclic.merge"] {
        assert!(registry.tools.get(expected).is_some());
        assert_eq!(ordinary.get(expected).unwrap().definition, registry.tools.get(expected).unwrap().definition);
    }
    for name in ["acyclic.fork", "acyclic.merge"] {
        let tool = registry.tools.get(name).ok_or_else(|| Error::NotFound(name.into()))?;
        let durable = registry.resumable_tools.get(name, REVISION).ok_or_else(|| Error::NotFound(name.into()))?;
        assert_eq!(tool.definition, durable.definition);
        assert_eq!(tool.executor.resumable_machine_identity(), Some(durable.machine.identity().clone()));
        let invocation = ToolInvocation { operation_id: OperationId::from_bytes([50; 16]), call_id: "exact-call".into(), name: name.into(), arguments: json!({}) };
        assert!(tool.executor.authorize(None, &invocation).is_err());
        assert!(tool.executor.execute(invocation.clone()).await.is_err());
        assert!(tool.executor.reconcile(invocation).await.is_err());
    }
    assert_eq!(source.0.load(Ordering::SeqCst), 0);
    assert!(registry.tools.get("bash").is_none());
    Ok(())
}


#[test]
fn invocation_guard_cancels_when_a_polled_pending_future_is_dropped() {
    let cancellation = CancellationToken::new();
    let guard = InvocationCancellation(cancellation.clone());
    let mut future = Box::pin(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
    let waker = futures::task::noop_waker();
    let mut context = std::task::Context::from_waker(&waker);
    assert!(Future::poll(future.as_mut(), &mut context).is_pending());
    assert!(!cancellation.is_cancelled());
    drop(future);
    assert!(cancellation.is_cancelled());
}

#[tokio::test]
async fn dropped_invocation_guard_rejects_the_next_real_filesystem_request_before_work() -> Result<()> {
    let fixture = fixture().await?;
    let read = ContentGrant::verify(&fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.private, VolumeOperation::Read)?;
    let write = ContentGrant::verify(&fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.private, VolumeOperation::Write)?;
    let file = fixture.host.put_content(&fixture.private, &write, "cancel/original.txt", b"original",
        "text/plain", "original.txt", 64, &IdempotencyKey::new("cancel-original")?).await?;
    let cancellation = CancellationToken::new();
    let guard = InvocationCancellation(cancellation.clone());
    let budget = acyclic_fs::performance::work_budget(4096, 1 << 20);
    let live = fixture.host.read_content_measured(&file, &read, 64, budget, &cancellation).await
        .map_err(|failure| failure.error)?;
    assert_eq!(live.value, b"original");
    drop(guard);
    let failure = fixture.host.read_content_measured(&file, &read, 64, budget, &cancellation).await
        .expect_err("actual invoking token must reject a new backend request");
    assert_eq!(failure.work, acyclic_fs::WorkCounters::default());
    assert!(cancellation.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn inherited_grant_preflight_reuses_real_materializer_without_child_effects() -> Result<()> {
    let mut fixture = fixture().await?;
    let write = ContentGrant::verify(&fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.private, VolumeOperation::Write)?;
    let content = fixture.host.put_content(&fixture.private, &write, "history/content.txt", b"original caller history",
        "text/plain", "content.txt", 8192, &IdempotencyKey::new("history-content")?).await?;
    let attachment = fixture.host.put_content(&fixture.private, &write, "history/attachment.bin", b"immutable attachment",
        "application/octet-stream", "attachment.bin", 8192, &IdempotencyKey::new("history-attachment")?).await?;
    let attachments = fixture.host.put_attachment_manifest(&fixture.private, &write, "history/attachments.json",
        &[Attachment { file: attachment.clone(), label: Some("original".into()) }], 8192,
        &IdempotencyKey::new("history-manifest")?).await?;
    let manifest = match &attachments {
        ReferencedAttachments::Manifest { manifest, .. } => manifest.clone(),
        _ => return Err(Error::Invalid("real manifest publisher did not return a manifest".into())),
    };
    let reader = Arc::new(FilesystemContentVerifier::new(fixture.host.clone(), &fixture.issuer.verifier(&mut None).expect("verifier copy"),
        &fixture.scope, 8192, None)?);
    fixture.parent = fixture.parent.with_content_verifier(reader.clone());
    fixture.parent.execute(Command {
        operation_id: OperationId::from_bytes([60; 16]), idempotency_key: IdempotencyKey::new("history-message")?,
        expected_revision: fixture.parent.reducer().revision(), scope: fixture.scope.clone(), causal_parent: None,
        action: Action::AppendConversationMessage { message: Box::new(ConversationMessage {
            id: uuid::Uuid::from_bytes([60; 16]), sequence: 1, kind: MessageKind::User, content: content.clone(),
            attachments, reply_to: None, tool_call_id: None, extensions: BTreeMap::new(),
        }) },
    }).await?;
    let child_agent = AgentId::from_bytes([61; 16]);
    let child = VolumeRef::new(fixture.private.provider().clone(), "preflight-child",
        VolumeClass::AgentPrivate, VolumeOwner::Agent(child_agent))?;
    let child_workspace = workspace_ref(child.provider().clone(), &child.storage_name()?)?;
    let controller = ParentProjectController::new(fixture.host.as_ref(), fixture.parent.reducer(),
        &fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.project, None)?;
    assert!(fixture.host.resolve(&child_workspace).await.is_err());
    let projected = controller.inherited_reference_grants(fixture.parent.reducer(), child_agent, &[],
        1, 8, 8192, 16, reader.as_ref(), None).await?;
    assert!(fixture.host.resolve(&child_workspace).await.is_err());
    assert_eq!(projected.len(), 3);
    let member = projected.iter().find(|grant| grant.file == attachment)
        .ok_or_else(|| Error::NotFound("canonical attachment-member grant".into()))?;
    assert_eq!(member.attachment_manifest.as_ref(), Some(&manifest));
    assert!(projected.iter().all(|grant| grant.reader == child_agent));
    let donor = Capabilities::new([manifest.read_capability()?]);
    crate::filesystem::fork_preparer::require_reference_donor(&donor, member, None)?;
    let direct = projected.iter().find(|grant| grant.file == content)
        .ok_or_else(|| Error::NotFound("canonical content grant".into()))?;
    assert!(crate::filesystem::fork_preparer::require_reference_donor(&donor, direct, None).is_err());
    assert!(controller.inherited_reference_grants(fixture.parent.reducer(), child_agent, &[],
        1, 8, 8192, 2, reader.as_ref(), None).await.is_err());
    assert!(controller.inherited_reference_grants(fixture.parent.reducer(), child_agent, &[],
        1, 8, 1, 16, reader.as_ref(), None).await.is_err());
    assert!(controller.inherited_reference_grants(fixture.parent.reducer(), child_agent, &[],
        2, 8, 8192, 16, reader.as_ref(), None).await.is_err());
    fixture.host.create_volume(&child).await?;
    let actual = controller.materialize_inherited_conversation(fixture.parent.reducer(), &child, child_agent,
        &[], 1, 8, 8192, 16, reader.as_ref(), &IdempotencyKey::new("real-inherited-prefix")?).await?;
    assert_eq!(actual.reference_grants, projected);
    Ok(())
}

#[tokio::test]
async fn durable_source_pin_keeps_original_cut_when_real_parent_history_advances() -> Result<()> {
    let mut fixture = fixture().await?;
    let caller = TaskId::from_bytes([62; 16]);
    let child_agent = AgentId::from_bytes([63; 16]);
    let effect = OperationId::from_bytes([64; 16]);
    let arguments = ForkToolArguments { id: child_agent, msg: "sealed original é\0🦀\r\n".into(), source: None };
    let value = encode(&arguments, None)?;
    let write = ContentGrant::verify(&fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, &fixture.private, VolumeOperation::Write)?;
    let file = fixture.host.put_content(&fixture.private, &write, "source/arguments.json",
        &crate::contract::canonical_json_bytes(&value)?, "application/json", "arguments.json", 8192,
        &IdempotencyKey::new("source-arguments")?).await?;
    let project = workspace_ref(fixture.project.provider().clone(), &fixture.project.storage_name()?)?;
    let private = workspace_ref(fixture.private.provider().clone(), &fixture.private.storage_name()?)?;
    let stream_provider = ProviderRef::new("recursive-default-test", "stream", "1")?;
    let request = ForkRequest {
        operation_id: effect, parent: fixture.parent.reducer().authority().clone(),
        parent_revision: fixture.parent.reducer().revision(),
        child: Authority { kind: AggregateKind::Conversation, id: "new-source-pin-child".into() },
        child_agent, attached_agents: Vec::new(),
        preparation: crate::fork::ForkPreparation {
            child_project_volume: VolumeRef::new(fixture.project.provider().clone(), "new-source-pin-project",
                VolumeClass::Project, VolumeOwner::Project("new-source-pin-child".into()))?,
            child_private_volume: VolumeRef::new(fixture.private.provider().clone(), "new-source-pin-private",
                VolumeClass::AgentPrivate, VolumeOwner::Agent(child_agent))?,
            inherited_through_sequence: 0, maximum_inherited_messages: 8,
            maximum_inherited_bytes: 8192, maximum_inherited_references: 16, summary: None,
        },
        selections: vec![
            crate::fork::ForkSelection { required: true, revision: crate::fork::ResourceRevision::History(
                crate::resources::StreamRef::new(stream_provider, fixture.parent.reducer().authority().stream_path(&mut None)?.into_bytes(),
                    Some(fixture.parent.reducer().revision().to_string()))?) },
            crate::fork::ForkSelection { required: true, revision: crate::fork::ResourceRevision::Project {
                volume: fixture.project.clone(), generation: fixture.host.resolve(&project).await?.generation } },
            crate::fork::ForkSelection { required: true, revision: crate::fork::ResourceRevision::PrivateVolume {
                volume: fixture.private.clone(), generation: fixture.host.resolve(&private).await?.generation, paths: Vec::new() } },
        ], boundary: None,
    };
    let intent = RecursiveForkSourceIntent { arguments: arguments.clone(), caller, source_task: caller,
        child_operation_id: OperationId::from_bytes([65; 16]), allocation_digest: [66; 32], request };
    let machine = RecursiveToolMachine::new(Kind::Fork, 65536);
    let initial = machine.initialize(&value)?;
    let command = WorkflowCommand { operation_id: derived(effect, "source-stage", None)?, kind: FORK_SOURCE_STAGE.into(), payload: file.clone() };
    let pinned = machine.transition(&initial, &encode(&(SourcePin { intent: intent.clone(), command: command.clone() }), None)?)?;
    assert_eq!(pinned.status, MachineStatus::Suspended);
    assert_eq!(pinned.commands, vec![command.clone()]);
    fixture.parent = fixture.parent.with_content_verifier(Arc::new(FilesystemContentVerifier::new(
        fixture.host.clone(), &fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.scope, 8192, None)?));
    fixture.parent.execute(Command {
        operation_id: OperationId::from_bytes([67; 16]), idempotency_key: IdempotencyKey::new("later-parent-history")?,
        expected_revision: fixture.parent.reducer().revision(), scope: fixture.scope.clone(), causal_parent: None,
        action: Action::AppendConversationMessage { message: Box::new(ConversationMessage {
            id: uuid::Uuid::from_bytes([67; 16]), sequence: 1, kind: MessageKind::User, content: file,
            attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() }, reply_to: None, tool_call_id: None, extensions: BTreeMap::new(),
        }) },
    }).await?;
    let restored: State = decode(&pinned.state, None)?;
    let State::SourcePinned { intent: original, arguments: retained, .. } = restored else {
        return Err(Error::Invalid("source selection was not durably pinned".into()));
    };
    assert_eq!(original, intent);
    assert_eq!(retained, value);
    assert!(original.request.parent_revision < fixture.parent.reducer().revision());
    let mut changed = intent;
    changed.child_operation_id = OperationId::from_bytes([68; 16]);
    changed.allocation_digest = [69; 32];
    assert!(machine.transition(&pinned.state, &encode(&(SourcePin { intent: changed, command }), None)?).is_err());
    Ok(())
}
