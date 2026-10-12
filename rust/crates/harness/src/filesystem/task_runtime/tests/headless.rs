use super::*;
use crate::conversation::{ContentGrant, FileRef};

async fn runtime(fixture: &Fixture) -> Result<Runtime> {
    FilesystemTaskRuntime::open(
        fixture.stream.clone(), fixture.filesystem.clone(), &fixture.volume, &fixture.issuer.verifier(&mut None).expect("verifier copy"),
        &fixture.signed, &fixture.scope, &fixture.tasks, &fixture.machines,
        &fixture.tools, SessionLimits { active_tasks: 8, total_tasks: 16, depth: 4, model_steps: 8 },
        1, 65_536, fixture.clock.clone(), None, &[], &ResumableToolRegistry::default(), None,
    ).await
}

async fn admit(runtime: &Runtime, fixture: &Fixture, id: u8, grants: Capabilities) -> Result<TaskId> {
    let definition = fixture.tasks.get_version::<Value, Value>("test.deadline", "1", #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?;
    let scoped = runtime.harness().scoped(grants, Limits::default())?;
    match scoped.admit(OperationId::from_bytes([id; 16]), &definition, json!({}), None).await? {
        Admission::Accepted(task) => Ok(task.task_id()),
        _ => Err(Error::Storage("headless fixture admission failed".into())),
    }
}

async fn put(fixture: &Fixture, volume: &VolumeRef, path: &str, bytes: &[u8], key: &str) -> Result<FileRef> {
    let grant = ContentGrant::verify(&fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.signed, volume, VolumeOperation::Write)?;
    fixture.filesystem.put_content(volume, &grant, path, bytes, "text/plain", path.rsplit('/').next().unwrap_or(path),
        65_536, &IdempotencyKey::new(key)?).await
}

#[tokio::test]
async fn selected_source_never_adopts_its_grants_and_pins_project_and_scratch() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let project = VolumeRef::new(fixture.volume.provider().clone(), "project", VolumeClass::Project,
        VolumeOwner::Project("selected-project".into()))?;
    fixture.filesystem.create_volume(&project).await?;
    let agent = fixture.signed.agent().ok_or_else(|| Error::Invalid("fixture agent".into()))?;
    fixture.signed = fixture.issuer.root_for_agent(agent, "owner", Capabilities::new(
        fixture.signed.capabilities().iter().chain([
            project.capability(VolumeOperation::Read)?, project.capability(VolumeOperation::Write)?,
        ].iter().map(String::as_str)),
    ));
    fixture.scope = RuntimeScope::new(fixture.signed.capabilities().clone(), Limits::default())?;
    let old = put(&fixture, &project, "src/a.txt", b"old", "old").await?;
    let next = put(&fixture, &project, "src/b.txt", b"other", "other").await?;
    let old_generation = fixture.filesystem.file_generation(&next)?;
    let current = put(&fixture, &project, "src/a.txt", b"new", "new").await?;
    let scratch = put(&fixture, &fixture.volume, "scratch/read.txt", b"scratch", "scratch").await?;
    let scratch_generation = fixture.filesystem.file_generation(&scratch)?;
    let runtime = runtime(&fixture).await?;
    let source = admit(&runtime, &fixture, 111, fixture.scope.grants().clone()).await?;
    let limited = Capabilities::new(["task:spawn:test.deadline@1"]);
    let caller = admit(&runtime, &fixture, 112, limited).await?;
    let inspected = runtime.inspect_source(caller, source).await?;
    assert_eq!(inspected.source().operation_id.into_bytes(), source.into_bytes());
    assert!(inspected.project().is_err());
    assert!(matches!(inspected.read_file(&old).await, Err(Error::Unauthorized(_))));
    assert!(matches!(inspected.list(&project, "src", "src", &old_generation, None, 1).await,
        Err(Error::Unauthorized(_))));
    let owner = runtime.inspect_source(source, source).await?;
    assert!(owner.list(&project, "", "", &scratch_generation, None, 1).await.is_err());
    let page = owner.list(&project, "src", "src", &old_generation, None, 1).await?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "a.txt");
    assert!(page.has_more);
    let second = owner.list(&project, "src", "src", &old_generation, Some("a.txt"), 1).await?;
    assert_eq!(second.entries[0].name, "b.txt");
    let (_, bytes) = owner.read_path(&project, "src", "src/a.txt", &old_generation).await?;
    assert_eq!(bytes, b"old");
    assert!(owner.diff(&old, &current).await?);
    assert!(!owner.diff(&old, &old).await?);
    assert_eq!(owner.read_file(&scratch).await?.as_ref(), b"scratch".as_ref());
    assert!(owner.list(&project, "src", "src", &old_generation, None, 0).await.is_err());
    assert!(owner.list(&project, "src", "src", &old_generation, Some("a/b"), 1).await.is_err());
    assert!(owner.read_path(&project, "src", ".system/hidden", &old_generation).await.is_err());
    let foreign = VolumeRef::new(project.provider().clone(), "project", VolumeClass::Project,
        VolumeOwner::Project("wrong-owner".into()))?;
    assert!(matches!(owner.list(&foreign, "src", "src", &old_generation, None, 1).await,
        Err(Error::Unauthorized(_))));
    let wrong_generation = crate::resources::GenerationRef::new(
        ProviderRef::new("wrong", "filesystem", "2")?,
        old_generation.as_resource().key().to_vec(), None,
    )?;
    assert!(owner.list(&project, "src", "src", &wrong_generation, None, 1).await.is_err());
    assert!(runtime.inspect_source(source, TaskId::from_bytes([119; 16])).await.is_err());
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn direct_child_pages_use_the_owner_index_and_invalidate_changed_revision() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.scope = RuntimeScope::new(fixture.signed.capabilities().clone(), Limits::default())?;
    let runtime = runtime(&fixture).await?;
    let lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&lease).await?;
    let parent = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    for id in [121, 122, 123] {
        let child = runtime.harness().registered_child_admission(OperationId::from_bytes([id; 16]),
            "test.deadline", "1", json!({}), parent, &fixture.scope)?;
        assert!(matches!(runtime.task_host().admit_owned(child, fence.clone()).await?, Admission::Accepted(_)));
    }
    let first = runtime.children(parent, None, None, 1).await?;
    assert_eq!(first.entries.len(), 1);
    let cursor = first.next_after.clone().ok_or_else(|| Error::Storage("child cursor absent".into()))?;
    let second = runtime.children(parent, Some(first.revision), Some(cursor.clone()), 1).await?;
    assert_eq!(second.entries.len(), 1);
    assert_ne!(first.entries[0].task_id, second.entries[0].task_id);
    assert!(runtime.children(parent, None, Some(cursor.clone()), 1).await.is_err());
    assert!(runtime.children(parent, None, None, 0).await.is_err());
    let child = runtime.harness().registered_child_admission(OperationId::from_bytes([124; 16]),
        "test.deadline", "1", json!({}), parent, &fixture.scope)?;
    assert!(matches!(runtime.task_host().admit_owned(child, fence).await?, Admission::Accepted(_)));
    assert!(runtime.children(parent, Some(first.revision), Some(cursor), 1).await.is_err());
    assert!(runtime.children(TaskId::from_bytes([125; 16]), None, None, 1).await.is_err());
    let foreign_issuer = AuthorityIssuer::new("foreign-owner", [126; 32], Authority {
        kind: AggregateKind::Task, id: "foreign-task-owner".into(),
    });
    let agent = fixture.signed.agent().ok_or_else(|| Error::Invalid("fixture agent".into()))?;
    let signed = foreign_issuer.root_for_agent(agent, "owner", fixture.signed.capabilities().clone());
    let foreign = FilesystemTaskRuntime::open(
        fixture.stream.clone(), fixture.filesystem.clone(), &fixture.volume, &foreign_issuer.verifier(&mut None).expect("verifier copy"),
        &signed, &fixture.scope, &fixture.tasks, &fixture.machines, &fixture.tools,
        SessionLimits { active_tasks: 8, total_tasks: 16, depth: 4, model_steps: 8 }, 1, 65_536, fixture.clock.clone(), None, &[], &ResumableToolRegistry::default(), None,
    ).await?;
    assert!(foreign.children(parent, None, None, 1).await.is_err());
    assert!(foreign.inspect_source(parent, parent).await.is_err());
    Ok(())
}

struct ModelFixture(AtomicUsize);
impl ModelProvider for ModelFixture {
    fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
        Ok(crate::context::ModelContextCapacity { context_tokens: 131_072, output_tokens: 8_192 })
    }
    fn count_tokens(&self, request: &crate::model::PreparedModelRequest) -> Result<crate::context::ModelTokenCount> {
        Ok(crate::context::ModelTokenCount {
            request_digest: request.request_digest(),
            fixed_tokens: u32::try_from(request.bytes().len()).map_err(|_| Error::Invalid("fixture request size".into()))?,
            message_tokens: vec![0; request.request().messages.len()],
        })
    }
    fn generate<'a>(&'a self, _: crate::model::PreparedModelRequest, _: crate::model::ModelDispatch)
        -> acyclic_stream::BoxProviderStream<'a, Result<crate::model::ModelEvent>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::iter([
            Ok(crate::model::ModelEvent::Content { delta: "one retained response".into() }),
            Ok(crate::model::ModelEvent::Completed { metadata: json!({}) }),
        ]))
    }

}

#[tokio::test]
async fn public_headless_model_step_replays_the_owned_journal_and_rejects_retargeting() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let fs = Fs::memory();
    fixture.filesystem = Arc::new(FilesystemHost::new(fs.clone(), fixture.volume.provider().clone())?);
    fixture.filesystem.create_volume(&fixture.volume).await?;
    let project = VolumeRef::new(fixture.volume.provider().clone(), "restore-project", VolumeClass::Project,
        VolumeOwner::Project("restore".into()))?;
    fixture.filesystem.create_volume(&project).await?;
    let agent = fixture.signed.agent().ok_or_else(|| Error::Invalid("fixture agent".into()))?;
    fixture.signed = fixture.issuer.root_for_agent(agent, "owner", Capabilities::new(
        fixture.signed.capabilities().iter().chain([
            project.capability(VolumeOperation::Read)?, project.capability(VolumeOperation::Write)?,
        ].iter().map(String::as_str)),
    ));
    fixture.scope = RuntimeScope::new(fixture.signed.capabilities().clone(), Limits::default())?;
    let runtime = runtime(&fixture).await?;
    let lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&lease).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let provider = Arc::new(ModelFixture(AtomicUsize::new(0)));
    let execution = runtime.stock_execution(task, LeaseFence::from(&lease.reservation), OperationId::from_bytes([130; 16]),
        Model::new("fixture", "headless", "1", json!({}))?, provider.clone(), ContextPipeline::default(), None).await?
        .with_compaction_policy(crate::context::CompactionPolicy::Disabled);
    let input = TurnInput { operation_id: execution.operation_id(), input: crate::model::ModelContent::Text("headless".into()),
        selected_context: None, max_steps: 1 };
    assert!(execution.resolve_model_tools(&input, 0).await.is_err());
    let events = execution.model_step(&input, 0, &[]).await?;
    assert_eq!(events, execution.model_step(&input, 0, &[]).await?);
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    assert!(matches!(execution.resolve_model_tools(&input, 0).await?, crate::executor::ModelToolsProgress::Ready(messages) if messages.is_empty()));
    let mut changed = input.clone();
    changed.operation_id = OperationId::from_bytes([131; 16]);
    assert!(matches!(execution.model_step(&changed, 0, &[]).await, Err(Error::Unauthorized(_))));
    assert!(execution.model_step(&input, 1, &[]).await.is_err());
    let mut stale = LeaseFence::from(&lease.reservation);
    stale.reservation_id = "wrong-fence".into();
    assert!(runtime.stock_execution(task, stale, OperationId::new(), Model::new("fixture", "headless", "1", json!({}))?,
        provider.clone(), ContextPipeline::default(), None).await.is_err());
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    use acyclic_fs::{TransactionCommit, WorkspaceRestore};
    use crate::core::{Action, Command};
    use crate::conversation::{ConversationMessage, MessageKind};
    use crate::filesystem::{FilesystemContentVerifier, FilesystemForkPreparer, FilesystemForkVerifier};
    use crate::fork::{CompositeForkVerifier, ForkPreparation, ForkRequest, ForkSelection, ResourceRevision, StreamHistoryForkVerifier};
    use crate::store::StreamAggregate;

    let authority = Authority { kind: AggregateKind::Conversation, id: "restore-history".into() };
    let issuer = AuthorityIssuer::new("restore", [132; 32], authority.clone());
    let signed = issuer.root_for_agent(agent, "owner", Capabilities::new(
        fixture.signed.capabilities().iter().chain(["conversation:bind", "conversation:append", "fork:publish"]),
    ));
    let reader = Arc::new(FilesystemContentVerifier::new(fixture.filesystem.clone(), &issuer.verifier(), &signed, 65_536, None)?);
    let stream_provider = ProviderRef::new("restore", "stream", "2")?;
    let forks = Arc::new(CompositeForkVerifier::new(&[
        Arc::new(FilesystemForkVerifier::new(fixture.filesystem.clone(), 65_536)?),
        Arc::new(StreamHistoryForkVerifier::new(&stream_provider, &mut None)?),
    ], &mut None)?);
    let mut history = StreamAggregate::open(&fixture.stream, authority.clone(), issuer.verifier(&mut None)?, SchemaRegistry::new()).await?
        .with_content_verifier(reader.clone()).with_fork_verifier(forks.clone());
    history.execute(Command { operation_id: OperationId::from_bytes([133; 16]), idempotency_key: IdempotencyKey::new("restore-bind")?,
        expected_revision: 0, scope: signed.clone(), causal_parent: None, action: Action::BindConversation { agent } }, #[cfg(feature = "filesystem")] None, &mut None).await?;
    let message = put(&fixture, &fixture.volume, "restore/history.txt", b"original conversation", "restore-history").await?;
    history.execute(Command { operation_id: OperationId::from_bytes([134; 16]), idempotency_key: IdempotencyKey::new("restore-message")?,
        expected_revision: 1, scope: signed.clone(), causal_parent: None, action: Action::AppendConversationMessage {
            message: Box::new(ConversationMessage { id: uuid::Uuid::from_bytes([134; 16]), sequence: 1, kind: MessageKind::User,
                content: message.clone(), attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() }, reply_to: None, tool_call_id: None, extensions: BTreeMap::new() }),
        } }, #[cfg(feature = "filesystem")] None, &mut None).await?;
    let original_history = history.reducer().conversation().map(|state| state.messages().to_vec());
    let original_revision = history.reducer().revision();
    let workspace = fs.open_workspace(project.storage_name()?).await.map_err(|e| Error::Storage(e.to_string()))?;
    ContentGrant::verify(&fixture.issuer.verifier(), &fixture.signed, &project, VolumeOperation::Write)?;
    let restricted = fixture.issuer.root_for_agent(agent, "read-only", Capabilities::new([project.capability(VolumeOperation::Read)?]));
    assert!(ContentGrant::verify(&fixture.issuer.verifier(), &restricted, &project, VolumeOperation::Write).is_err());
    put(&fixture, &project, "src/a.txt", b"old", "restore-old").await?;
    let selected = workspace.head().await.map_err(|e| Error::Storage(e.to_string()))?;
    put(&fixture, &project, "src/a.txt", b"new", "restore-new").await?;
    let observed = workspace.head().await.map_err(|e| Error::Storage(e.to_string()))?;
    put(&fixture, &project, "concurrent.txt", b"user edit", "restore-concurrent").await?;
    let paths = vec!["src/a.txt".into()];
    assert!(matches!(workspace.restore_paths_from(&selected, &paths, observed.id(), acyclic_fs::IdempotencyKey::from_bytes([135; 16]))
        .await.map_err(|e| Error::Storage(e.to_string()))?, TransactionCommit::Conflict { .. }));
    let current = workspace.head().await.map_err(|e| Error::Storage(e.to_string()))?;
    assert!(matches!(workspace.restore_paths_from(&selected, &paths, current.id(), acyclic_fs::IdempotencyKey::from_bytes([136; 16]))
        .await.map_err(|e| Error::Storage(e.to_string()))?, TransactionCommit::Committed(_)));
    let saved = workspace.head().await.map_err(|e| Error::Storage(e.to_string()))?;
    put(&fixture, &project, "src/a.txt", b"later", "restore-later").await?;
    assert!(matches!(workspace.restore_generation(&saved, saved.id(), acyclic_fs::IdempotencyKey::from_bytes([137; 16]))
        .await.map_err(|e| Error::Storage(e.to_string()))?, WorkspaceRestore::Stale(_)));
    let current = workspace.head().await.map_err(|e| Error::Storage(e.to_string()))?;
    let restore_key = acyclic_fs::IdempotencyKey::from_bytes([138; 16]);
    assert!(matches!(workspace.restore_generation(&saved, current.id(), restore_key).await.map_err(|e| Error::Storage(e.to_string()))?,
        WorkspaceRestore::Restored(_)));
    assert!(matches!(workspace.restore_generation(&saved, current.id(), restore_key).await.map_err(|e| Error::Storage(e.to_string()))?,
        WorkspaceRestore::AlreadyRestored(_)));
    let selected_generation = fixture.filesystem.resolve(&crate::filesystem::workspace_ref(project.provider().clone(), &project.storage_name()?)?).await?.generation;
    let inspection = runtime.inspect_source(task, task).await?;
    assert_eq!(inspection.read_path(&project, "", "src/a.txt", &selected_generation).await?.1.as_ref(), b"old");
    assert_eq!(inspection.read_path(&project, "", "concurrent.txt", &selected_generation).await?.1.as_ref(), b"user edit");
    assert_eq!(history.reducer().revision(), original_revision);
    assert_eq!(history.reducer().conversation().map(|state| state.messages().to_vec()), original_history);
    assert_eq!(execution.model_step(&input, 0, &[]).await?, events);

    let child_agent = AgentId::from_bytes([139; 16]);
    let child_authority = Authority { kind: AggregateKind::Conversation, id: "restore-new-conversation".into() };
    let child_project = VolumeRef::new(project.provider().clone(), "restore-child", VolumeClass::Project, project.owner().clone())?;
    let child_private = VolumeRef::new(project.provider().clone(), "restore-child", VolumeClass::AgentPrivate, VolumeOwner::Agent(child_agent))?;
    let preparer = FilesystemForkPreparer::new(fixture.filesystem.clone(), history.reducer().clone(), issuer.verifier(), signed.clone(),
        project.clone(), stream_provider.clone(), reader)?;
    let request = ForkRequest { authorized_source: None, operation_id: OperationId::from_bytes([140; 16]), parent: authority.clone(),
        parent_revision: original_revision, child: child_authority.clone(), child_agent, attached_agents: Vec::new(), boundary: None,
        preparation: ForkPreparation { child_result_volume: None, summary: None, child_project_volume: child_project.clone(),
            child_private_volume: child_private.clone(), inherited_through_sequence: 1, maximum_inherited_messages: 1,
            maximum_inherited_bytes: 65_536, maximum_inherited_references: 8 },
        selections: vec![
            ForkSelection { required: true, revision: ResourceRevision::Project { volume: project.clone(), generation: selected_generation.clone() } },
            ForkSelection { required: true, revision: ResourceRevision::PrivateVolume { volume: fixture.volume.clone(),
                generation: fixture.filesystem.resolve(&crate::filesystem::workspace_ref(project.provider().clone(), &fixture.volume.storage_name()?)?).await?.generation,
                paths: Vec::new() } },
            ForkSelection { required: true, revision: ResourceRevision::History(crate::resources::StreamRef::new(stream_provider,
                authority.stream_path()?.into_bytes(), Some(original_revision.to_string()))?) },
        ] };
    let report = history.prepare_fork(&preparer, request).await?;
    let child_issuer = AuthorityIssuer::new("restore", [132; 32], child_authority.clone());
    let child_scope = child_issuer.root_for_agent(child_agent, "child", Capabilities::new([
        "conversation:bind".into(), child_private.capability(VolumeOperation::Read)?, child_private.capability(VolumeOperation::Write)?,
        child_project.capability(VolumeOperation::Read)?, child_project.capability(VolumeOperation::Write)?,
    ]));
    let mut child = StreamAggregate::open(&fixture.stream, child_authority, child_issuer.verifier(), SchemaRegistry::new()).await?
        .with_fork_verifier(forks);
    let seed = child.spawn_from_report(&mut history, report, signed, child_scope).await?;
    assert_eq!(seed.parent, authority);
    assert!(!seed.inherited_context.is_empty());
    assert_eq!(history.reducer().conversation().map(|state| state.messages().to_vec()), original_history);
    assert_eq!(child.reducer().conversation().and_then(|state| state.agent), Some(child_agent));
    assert_eq!(fixture.filesystem.read(&crate::filesystem::workspace_ref(project.provider().clone(), &child_project.storage_name()?)?, None, "/src/a.txt", 128).await?.as_ref(), b"old");
    let reopened = self::runtime(&fixture).await?;
    let replay = reopened.stock_execution(task, LeaseFence::from(&lease.reservation), input.operation_id,
        Model::new("fixture", "headless", "1", json!({}))?, provider.clone(), ContextPipeline::default(), None).await?
        .with_compaction_policy(crate::context::CompactionPolicy::Disabled);
    assert_eq!(replay.model_step(&input, 0, &[]).await?, events);
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    Ok(())
}
