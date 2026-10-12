
use super::*;
use acyclic_fs::{CancellationToken, WorkBudget};
use acyclic_stream::StreamPath;
use crate::{AgentId, Capabilities};
use crate::core::{AuthorityVerifier, Scope};

fn publication_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 32,
        authority_bytes_read: 1_048_576,
        backend_read_operations: 32,
        source_bytes_read: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        bytes_hashed: 1_048_576,
        authority_records_appended: 32,
        authority_bytes_written: 1_048_576,
        backend_write_operations: 32,
        output_bytes: 1_048_576,
        items_examined: 32,
        items_returned: 32,
        allocation_operations: 128,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

#[tokio::test]
async fn indexed_publication_cancellation_precedes_codec_and_sdk() -> Result<()> {
    let reducer = Reducer::new(authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas());
    let ApplyResult::Applied { event } = reducer.plan(&command(71)?)? else {
        return Err(Error::Invalid("fixture needs fresh event".into()));
    };
    let bytes = crate::wire_codec::encode_event(reducer.authority(), &event)?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut work = history::HistoryWork::new(publication_budget(), &cancellation);
    assert!(operations::IndexedPublication::new(
        reducer.authority(), &event, bytes, &mut Some(&mut work),
    ).is_err());
    Ok(())
}

#[tokio::test]
async fn immutable_stage_cancelled_before_commit_leaves_no_record() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let path = StreamPath::new("harness/v3/test-fork-stage/cancelled")?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut work = history::HistoryWork::new(publication_budget(), &cancellation);
    assert!(checkpoints::stage_immutable_record_with_work(
        &client, path.clone(), acyclic_stream::OwnedBytes::from_static(b"immutable"), &mut Some(&mut work),
    ).await.is_err());
    assert!(matches!(client.bounds(acyclic_stream::StreamPath::new(path.as_str())?).await, Err(acyclic_stream::StreamError::NotFound)));
    Ok(())
}

#[tokio::test]
async fn immutable_stage_reopen_preserves_exact_identity() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let path = StreamPath::new("harness/v3/test-fork-stage/reopen")?;
    let bytes = acyclic_stream::OwnedBytes::from_static(b"immutable");
    checkpoints::stage_immutable_record_with_work(&client, path.clone(), bytes.clone(), &mut None).await?;
    let reopened = client.clone();
    checkpoints::stage_immutable_record_with_work(&reopened, path.clone(), bytes, &mut None).await?;
    assert_eq!(reopened.bounds(acyclic_stream::StreamPath::new(path.as_str())?).await?.tail, 1);
    assert!(checkpoints::stage_immutable_record_with_work(
        &reopened, path, acyclic_stream::OwnedBytes::from_static(b"hostile"), &mut None).await.is_err());
    Ok(())
}

#[tokio::test]
async fn immutable_stage_zero_allowance_cannot_append() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let path = StreamPath::new("harness/v3/test-fork-stage/quota")?;
    let cancellation = CancellationToken::new();
    let mut work = history::HistoryWork::new(WorkBudget::default(), &cancellation);
    assert!(checkpoints::stage_immutable_record_with_work(
        &client, path.clone(), acyclic_stream::OwnedBytes::from_static(b"immutable"), &mut Some(&mut work),
    ).await.is_err());
    assert!(matches!(client.bounds(acyclic_stream::StreamPath::new(path.as_str())?).await, Err(acyclic_stream::StreamError::NotFound)));
    Ok(())
}

#[test]
fn operation_locator_schema_quota_and_hostile_digest_preserve_original_decoder() -> Result<()> {
    let location = operations::OperationLocation {
        authority: authority(),
        operation_id: OperationId::from_bytes([72; 16]),
        revision: 1,
        intent_digest: [3; 32],
        effect_position: None,
    };
    let bytes = crate::contract::canonical_json_bytes(&location)?;
    let cancellation = CancellationToken::new();
    let mut work = history::HistoryWork::new(publication_budget(), &cancellation);
    assert_eq!(operations::decode_location_with_work(&bytes, &mut Some(&mut work))?, location);
    let mut narrow = history::HistoryWork::new(
        WorkBudget { peak_allocation_bytes: 1, ..publication_budget() }, &cancellation,
    );
    assert!(operations::decode_location_with_work(&bytes, &mut Some(&mut narrow)).is_err());
    let mut hostile = serde_json::to_value(&location)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    hostile["intent_digest"] = serde_json::json!([3]);
    let hostile = crate::contract::canonical_json_bytes(&hostile)?;
    let mut work = history::HistoryWork::new(publication_budget(), &cancellation);
    assert!(operations::decode_location_with_work(&hostile, &mut Some(&mut work)).is_err());
    assert!(operations::decode_location_with_work(&hostile, &mut None).is_err());
    Ok(())
}

#[cfg(feature = "filesystem")]
struct GuardedForkFixture {
    fixture: crate::durable_host::tests::created_resource_access::CreationFixture,
    owner: crate::durable_host::TaskJournalOwner<MemoryStream>,
    lease: crate::distributed::WorkLease,
    parent: StreamAggregate<MemoryStream>,
    report: crate::fork::ForkReport,
    parent_scope: Scope,
    parent_verifier: AuthorityVerifier,
    child_scope: Scope,
    child_verifier: AuthorityVerifier,
}

#[cfg(feature = "filesystem")]
async fn guarded_fork_fixture() -> Result<GuardedForkFixture> {
    use crate::core::SchemaRegistry;
    use crate::filesystem::{FilesystemContentVerifier, FilesystemForkPreparer, FilesystemForkVerifier};
    use crate::fork::{CompositeForkVerifier, ForkPreparer, ResourceRevision, StreamHistoryForkVerifier};
    use crate::resources::StreamRef;
    use crate::conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
    let fixture = crate::durable_host::tests::created_resource_access::CreationFixture::new().await?;
    let created = fixture.create_filesystem_child_with_grants(&["fork:publish", "conversation:bind"]).await?;
    let stream_provider = crate::resources::ProviderRef::new("created-controls".into(), "stream".into(), "2".into(), &mut None)?;
    let verifier = Arc::new(CompositeForkVerifier::new(&[
        Arc::new(FilesystemForkVerifier::new(fixture.filesystem.clone(), 65_536)?),
        Arc::new(StreamHistoryForkVerifier::new(&stream_provider, &mut None)?),
    ], &mut None)?);
    let mut original = StreamAggregate::open(
        &fixture.stream, created.seed.parent.clone(), created.parent_verifier.clone(), SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?.with_fork_verifier(verifier.clone());
    original.publish_fork_report(created.report.clone(), created.parent_scope.clone(), None, &mut None).await?;
    let parent_issuer = crate::core::AuthorityIssuer::new("created-owner", [80; 32], created.seed.child.clone());
    let parent_scope = parent_issuer.issue("measured-fork-parent", &Capabilities::new([
        "conversation:bind".into(), "fork:publish".into(),
        created.private.capability(VolumeOperation::Read)?,
        created.project.capability(VolumeOperation::Read)?,
    ]), Some(created.seed.child_agent), None, &mut None).expect("scope proof");
    let mut parent = StreamAggregate::open(
        &fixture.stream, created.seed.child.clone(), parent_issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?.with_fork_verifier(verifier);
    parent.bind_published_child(&original, &created.seed, parent_scope.clone(), None, &mut None).await?;
    let task = crate::TaskId::from_bytes([84; 16]);
    assert!(matches!(fixture.admit_child(created.request.child.clone()).await?, crate::Admission::Accepted(id) if id == task));
    let crate::distributed::WorkPull::Claimed(lease) = fixture.host.recover_work(task, &mut None).await? else {
        return Err(Error::NotFound("measured fork original owner lease".into()));
    };
    let owner = fixture.host.journal_owner(task, (&lease.reservation).into()).await?;
    let child_agent = AgentId::from_bytes([94; 16]);
    let project = VolumeRef::new(fixture.filesystem_provider.clone(), "measured-child-project", VolumeClass::Project,
        VolumeOwner::Project("created-project".into()))?;
    let private = VolumeRef::new(fixture.filesystem_provider.clone(), "measured-child-private", VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent))?;
    let project_generation = created.seed.resources.iter().find_map(|resource| match &resource.revision {
        ResourceRevision::Project { generation, .. } => Some(generation.clone()), _ => None,
    }).ok_or_else(|| Error::Invalid("original created project generation missing".into()))?;
    let mut fork = created.report.request.clone();
    fork.operation_id = OperationId::from_bytes([95; 16]);
    fork.parent = created.seed.child.clone();
    fork.parent_revision = parent.reducer().revision();
    fork.child = Authority { kind: AggregateKind::Conversation, id: "measured-fork-grandchild".into() };
    fork.child_agent = child_agent;
    fork.preparation.child_project_volume = project.clone();
    fork.preparation.child_private_volume = private.clone();
    fork.selections[0].revision = ResourceRevision::History(StreamRef::new(
        stream_provider.clone(), fork.parent.stream_path(&mut None)?.into_bytes(), Some(fork.parent_revision.to_string()),
    )?);
    fork.selections[1].revision = ResourceRevision::Project { volume: created.project.clone(), generation: project_generation };
    fork.selections[2].revision = ResourceRevision::PrivateVolume {
        volume: created.private.clone(), generation: created.seed.child_private_generation.clone(), paths: Vec::new(),
    };
    let history = crate::history::HistoryReader::new(&fixture.stream, &fork.parent, &(parent_issuer.verifier(&mut None).expect("verifier copy")), &mut None)?
        .fork_history(parent.reducer(), 0, crate::history::HistoryReadLimits { maximum_events: 2, maximum_bytes: 65_536 }, &mut None).await?;
    let resolver = Arc::new(FilesystemContentVerifier::new(
        fixture.filesystem.clone(), &parent_issuer.verifier(&mut None).expect("verifier copy"), &parent_scope, 65_536, None,
    )?);
    let preparer = FilesystemForkPreparer::new(fixture.filesystem.clone(), parent.reducer().clone(),
        parent_issuer.verifier(&mut None).expect("verifier copy"), parent_scope.clone(), created.project.clone(), stream_provider, resolver,
        Some(history), None, #[cfg(not(target_arch = "wasm32"))] None,
    )?;
    let mut admission = fixture.admission(85, serde_json::json!({"action":"filesystem-io"}));
    admission.parent = Some(task);
    admission.grants = Capabilities::new([
        private.capability(VolumeOperation::Read)?, private.capability(VolumeOperation::Write)?,
        project.capability(VolumeOperation::Read)?, project.capability(VolumeOperation::Write)?,
        "conversation:bind".into(),
    ]);
    let (resources, proofs) = preparer.creation_allocations(&fork, task, None)?;
    admission = admission.with_resource_proofs(proofs)?;
    let mut request = created.request.clone();
    request.child = admission;
    request.resources = resources;
    request.payload = serde_json::to_value(&fork).map_err(|error| Error::Invalid(error.to_string()))?;
    let preparation = owner.resource_preparation(request).await?;
    let report = preparer.with_task_preparation(Arc::new(preparation))?.prepare(fork).await?;
    let child_issuer = crate::core::AuthorityIssuer::new("created-owner", [80; 32], report.request.child.clone());
    let child_scope = child_issuer.issue("measured-fork-child", &Capabilities::new(["conversation:bind".into()]), Some(child_agent), None, &mut None).expect("scope proof");
    Ok(GuardedForkFixture { fixture, owner, lease, parent, report, parent_scope,
        parent_verifier: parent_issuer.verifier(&mut None).expect("verifier copy"), child_scope, child_verifier: child_issuer.verifier(&mut None).expect("verifier copy") })
}

#[cfg(feature = "filesystem")]
fn guarded_publication_budget() -> WorkBudget {
    WorkBudget { authority_records_read: 4_096, authority_bytes_read: 16_777_216,
        backend_read_operations: 4_096, source_bytes_read: 16_777_216,
        bytes_copied: 16_777_216, bytes_encoded: 16_777_216, bytes_hashed: 16_777_216,
        authority_records_appended: 4_096, authority_bytes_written: 16_777_216,
        backend_write_operations: 4_096, output_bytes: 16_777_216, items_examined: 1_000_000,
        items_returned: 4_096, allocation_operations: 1_000_000, peak_allocation_bytes: 268_435_456,
        ..WorkBudget::default() }
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn guarded_fork_reopen_and_child_replay_keep_original_identity_and_owner_fence() -> Result<()> {
    use crate::core::SchemaRegistry;
    let mut setup = guarded_fork_fixture().await?;
    let cancellation = CancellationToken::new();
    let budget = guarded_publication_budget();
    let seed = setup.report.clone().into_seed(&mut None)?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let before = setup.parent.reducer().revision();
    assert!(setup.parent.publish_fork_report_measured(setup.report.clone(), setup.parent_scope.clone(),
        &setup.owner, budget, &cancelled).await.is_err());
    assert!(setup.parent.publish_fork_report_measured(setup.report.clone(), setup.parent_scope.clone(),
        &setup.owner, WorkBudget { peak_allocation_bytes: 1, ..budget }, &cancellation).await.is_err());
    let wrong_parent_agent = crate::core::AuthorityIssuer::new("created-owner", [80; 32], seed.parent.clone()).issue("not-original-admitted-parent", &Capabilities::new(["fork:publish".into()]), Some(AgentId::from_bytes([80; 16])), None, &mut None).expect("scope proof");
    assert!(setup.parent.publish_fork_report_measured(setup.report.clone(), wrong_parent_agent,
        &setup.owner, budget, &cancellation).await.is_err());
    let mut hostile_report = setup.report.clone();
    hostile_report.request.child_agent = AgentId::from_bytes([96; 16]);
    assert!(setup.parent.publish_fork_report_measured(hostile_report, setup.parent_scope.clone(),
        &setup.owner, budget, &cancellation).await.is_err());
    assert_eq!(setup.parent.reducer().revision(), before);
    setup.parent.publish_fork_report_measured(setup.report.clone(), setup.parent_scope.clone(),
        &setup.owner, budget, &cancellation).await.map_err(|failure| failure.error)?;
    let mut reopened = StreamAggregate::open(&setup.fixture.stream, seed.parent.clone(),
        setup.parent_verifier.clone(), SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?
        .with_fork_verifier(setup.parent.fork_verifier.clone().ok_or_else(|| Error::Invalid("original verifier missing".into()))?);
    let replay = reopened.publish_fork_report_measured(setup.report.clone(), setup.parent_scope.clone(),
        &setup.owner, budget, &cancellation).await.map_err(|failure| failure.error)?;
    assert!(matches!(replay.value, ApplyResult::Replayed { .. }));
    assert_eq!(reopened.reducer().revision(), before + 1);
    assert!(replay.work.backend_read_operations > 0);
    assert!(reopened.publish_fork_report_measured(setup.report.clone(), setup.parent_scope.clone(),
        &setup.owner, WorkBudget { backend_read_operations: replay.work.backend_read_operations - 1, ..budget },
        &cancellation).await.is_err());
    assert_eq!(setup.fixture.stream.bounds(acyclic_stream::StreamPath::new(seed.parent.stream_path(&mut None)?.as_str())?).await?.tail, before + 1);
    let mut child = StreamAggregate::open(&setup.fixture.stream, seed.child.clone(),
        setup.child_verifier.clone(), SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
    let partial = child.bind_published_child_measured(&reopened, &seed, setup.child_scope.clone(),
        &setup.owner, WorkBudget { authority_bytes_read: 0, ..budget }, &cancellation).await
        .expect_err("actual parent record must be charged before decoding");
    assert!(partial.work.authority_records_read > 0);
    assert!(partial.work.authority_bytes_read > 0);
    assert_eq!(child.reducer().revision(), 0);
    let mut hostile = seed.clone();
    hostile.parent_revision += 1;
    assert!(child.bind_published_child_measured(&reopened, &hostile, setup.child_scope.clone(),
        &setup.owner, budget, &cancellation).await.is_err());
    let wrong_agent = crate::core::AuthorityIssuer::new("created-owner", [80; 32], seed.child.clone()).issue("wrong-child-agent", &Capabilities::new(["conversation:bind".into()]), Some(AgentId::from_bytes([96; 16])), None, &mut None).expect("scope proof");
    assert!(child.bind_published_child_measured(&reopened, &seed, wrong_agent,
        &setup.owner, budget, &cancellation).await.is_err());
    child.bind_published_child_measured(&reopened, &seed, setup.child_scope.clone(),
        &setup.owner, budget, &cancellation).await.map_err(|failure| failure.error)?;
    let mut reopened_child = StreamAggregate::open(&setup.fixture.stream, seed.child.clone(),
        setup.child_verifier.clone(), SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
    reopened_child.bind_published_child_measured(&reopened, &seed, setup.child_scope.clone(),
        &setup.owner, budget, &cancellation).await.map_err(|failure| failure.error)?;
    assert_eq!(reopened_child.reducer().revision(), 1);
    assert_eq!(setup.fixture.stream.bounds(acyclic_stream::StreamPath::new(seed.child.stream_path(&mut None)?.as_str())?).await?.tail, 1);
    setup.fixture.host.settle_task(crate::TaskId::from_bytes([84; 16]), &(&setup.lease.reservation).into(), crate::Outcome::Failed { message: "test closes original publication lease".into() }, &mut None).await?;
    assert!(reopened.publish_fork_report_measured(setup.report, setup.parent_scope, &setup.owner, budget, &cancellation).await.is_err());
    assert_eq!(setup.fixture.stream.bounds(acyclic_stream::StreamPath::new(seed.parent.stream_path(&mut None)?.as_str())?).await?.tail, before + 1);
    Ok(())
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn guarded_child_rejects_original_event_one_with_wrong_binding_identity() -> Result<()> {
    use crate::core::SchemaRegistry;
    let mut setup = guarded_fork_fixture().await?;
    let cancellation = CancellationToken::new();
    let budget = guarded_publication_budget();
    let seed = setup.report.clone().into_seed(&mut None)?;
    setup.parent.publish_fork_report_measured(setup.report, setup.parent_scope,
        &setup.owner, budget, &cancellation).await.map_err(|failure| failure.error)?;
    let mut child = StreamAggregate::open(&setup.fixture.stream, seed.child.clone(),
        setup.child_verifier, SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
    child.execute(Command {
        operation_id: OperationId::from_bytes([97; 16]),
        idempotency_key: IdempotencyKey::new("hostile-original-child-binding")?,
        expected_revision: 0, scope: setup.child_scope.clone(), causal_parent: None,
        action: Action::BindConversation { agent: seed.child_agent },
    }, #[cfg(feature = "filesystem")] None, &mut None).await?;
    assert!(child.bind_published_child_measured(&setup.parent, &seed, setup.child_scope,
        &setup.owner, budget, &cancellation).await.is_err());
    assert_eq!(setup.fixture.stream.bounds(acyclic_stream::StreamPath::new(seed.child.stream_path(&mut None)?.as_str())?).await?.tail, 1);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_root_binding_keeps_original_actor_scope_fence_and_indexed_retry() -> Result<()> {
    use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
    let fixture = crate::durable_host::tests::created_resource_access::CreationFixture::new().await?;
    let owner = fixture.host.journal_owner(
        fixture.root, (&fixture.root_lease.reservation).into(),
    ).await?;
    let agent = owner.authenticated_binding_agent(&mut None)?
        .ok_or_else(|| Error::Invalid("original ROOT fixture has no authenticated agent".into()))?;
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "original-native-root-conversation".into(),
    };
    let original_issuer = crate::core::AuthorityIssuer::new("created-owner", [80; 32], audience.clone());
    let scope = original_issuer.issue("original-native-root-bind", &Capabilities::new(["conversation:bind".into()]), Some(agent), None, &mut None).expect("scope proof");
    let command = Command {
        operation_id: OperationId::from_bytes([124; 16]),
        idempotency_key: IdempotencyKey::new("original-native-root-bind")?,
        expected_revision: 0, scope, causal_parent: None,
        action: Action::BindConversation { agent },
    };
    let mut aggregate = StreamAggregate::open(
        &fixture.stream, audience.clone(), original_issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(guarded_publication_budget(), &cancellation)
        .expect("original ROOT ledger");
    let mut first_io = history::native_work_io(&native)?;
    let first = aggregate.execute(command.clone(), Some(&owner), &mut Some(&mut first_io)).await?;
    first_io.return_staged_output()?;
    let first_custody = first_io.into_custody();
    assert!(matches!(&first, ApplyResult::Applied { event }
        if event.revision == 1 && event.operation_id == command.operation_id));
    assert_eq!(aggregate.reducer().conversation().and_then(|state| state.agent), Some(agent));
    assert!(first_custody.bytes() > 0);
    assert!(first_custody.work().shares_ledger(&native));
    owner.authenticate_fork_parent(&audience, &mut None)?;
    let canonical = aggregate.operation_event(command.operation_id).await?
        .ok_or_else(|| Error::NotFound("original canonical ROOT event".into()))?;
    assert!(matches!(&first, ApplyResult::Applied { event } if event == &canonical));
    let mut retry_io = history::native_work_io(&native)?;
    let retry = aggregate.execute(command.clone(), Some(&owner), &mut Some(&mut retry_io)).await?;
    retry_io.return_staged_output()?;
    let retry_custody = retry_io.into_custody();
    assert!(matches!(&retry, ApplyResult::Replayed { event } if event == &canonical));
    assert!(retry_custody.work().shares_ledger(&native));
    assert_eq!(fixture.stream.bounds(acyclic_stream::StreamPath::new(audience.stream_path(&mut None)?.as_str())?).await?.tail, 1);

    let independent = NativeMaterializationWork::new(guarded_publication_budget(), &cancellation)
        .expect("independent original query ledger");
    let mut independent_retry_io = history::native_work_io(&independent)?;
    let independent_retry = aggregate.execute(command.clone(), Some(&owner), &mut Some(&mut independent_retry_io)).await?;
    independent_retry_io.return_staged_output()?;
    let independent_custody = independent_retry_io.into_custody();
    assert!(matches!(&independent_retry, ApplyResult::Replayed { event } if event == &canonical));
    assert!(independent_custody.work().shares_ledger(&independent));

    let mut wrong_actor = command.clone();
    wrong_actor.action = Action::BindConversation { agent: AgentId::from_bytes([125; 16]) };
    let mut missing_grant = command.clone();
    missing_grant.scope = original_issuer.issue("original-native-root-bind", &Capabilities::new([]), Some(agent), None, &mut None).expect("scope proof");
    let mut causal_binding = command.clone();
    causal_binding.causal_parent = Some(EventReference { authority: audience.clone(), revision: 1 });
    for invalid in [wrong_actor, missing_grant, causal_binding] {
        let mut io = history::native_work_io(&native)?;
        let error = aggregate.execute(invalid, Some(&owner), &mut Some(&mut io))
            .await.err().expect("original ROOT actor, grant and no-parent policy remain required");
        assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
    }

    let wrong_issuer = crate::core::AuthorityIssuer::new("created-owner", [81; 32], audience.clone());
    let mut wrong_scope = command.clone();
    wrong_scope.scope = wrong_issuer.issue("original-native-root-bind", &Capabilities::new(["conversation:bind".into()]), Some(agent), None, &mut None).expect("scope proof");
    let mut wrong_scope_io = history::native_work_io(&native)?;
    let wrong_scope_error = aggregate.execute(
        wrong_scope, Some(&owner), &mut Some(&mut wrong_scope_io),
    ).await.err().expect("original issuer key must authenticate the binding scope");
    assert_eq!(wrong_scope_error.code(), crate::wire::ErrorCode::Unauthorized);
    let denied = NativeMaterializationWork::new(
        WorkBudget { bytes_copied: 0, ..guarded_publication_budget() }, &cancellation,
    ).expect("zero-copy original ledger");
    let mut denied_io = history::native_work_io(&denied)?;
    assert!(matches!(aggregate.execute(
        command.clone(), Some(&owner), &mut Some(&mut denied_io),
    ).await, Err(Error::NativeWork {
        cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied", ..
        }), ..
    })));
    let cancelled_token = CancellationToken::new();
    let cancelled = NativeMaterializationWork::new(guarded_publication_budget(), &cancelled_token)
        .expect("original ledger before cancellation");
    cancelled_token.cancel();
    let mut cancelled_io = history::native_work_io(&cancelled)?;
    assert!(matches!(aggregate.execute(
        command, Some(&owner), &mut Some(&mut cancelled_io),
    ).await, Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(fixture.stream.bounds(acyclic_stream::StreamPath::new(audience.stream_path(&mut None)?.as_str())?).await?.tail, 1);
    drop(first);
    drop(first_custody);
    drop(retry);
    drop(retry_custody);
    drop(independent_retry);
    drop(independent_custody);
    assert_eq!(aggregate.reducer().revision(), 1);
    owner.authenticate_fork_parent(&audience, &mut None)?;
    let bootstrap = owner.root_bootstrap_binding_witness_with_native_work(&native).await?;
    let (original_root, root_custody) = owner.original_root_admission_with_native_work(&native).await?;
    owner.verify_root_bootstrap_context_with_native_work(
        &original_root, bootstrap.binding(), &native,
    ).await?;
    assert_ne!(owner.root_bootstrap_owner(), &audience);
    let creator = crate::core::AuthorityIssuer::new(
        "created-owner", [80; 32], owner.root_bootstrap_owner().clone(),
    );
    owner.authenticate_root_bootstrap_creator_with_native_work(&creator.verifier(&mut None).expect("verifier copy"), &native).await?;
    let publication = owner.root_bootstrap_publication_lease_with_native_work(&native).await?;
    let native_io = crate::filesystem::PreparationIo::from_native_work(&native)?;
    let verified = publication.verify_with_io(&native_io)?;
    assert!(matches!(&verified, crate::store::history::HistoryContentFuture::Native(_)));
    verified.await?;
    let ordinary_io = crate::filesystem::PreparationIo::from_budget(
        guarded_publication_budget(), &cancellation,
    )?;
    let verified = publication.verify_with_io(&ordinary_io)?;
    let frame_bytes = match &verified {
        crate::store::history::HistoryContentFuture::Ordinary { frame_bytes, .. } => *frame_bytes,
        _ => panic!("ordinary measured verification must retain its actual compiled frame"),
    };
    verified.await?;
    ordinary_io.release(frame_bytes)?;
    assert!(ordinary_io.receipt().backend_read_operations > 0);
    let denied_io = crate::filesystem::PreparationIo::from_budget(
        WorkBudget { backend_read_operations: 0, ..guarded_publication_budget() }, &cancellation,
    )?;
    let denied = publication.verify_with_io(&denied_io)?;
    let denied_frame = match &denied {
        crate::store::history::HistoryContentFuture::Ordinary { frame_bytes, .. } => *frame_bytes,
        _ => panic!("ordinary denied verification uses the same compiled frame"),
    };
    assert!(denied.await.is_err());
    denied_io.release(denied_frame)?;
    assert_eq!(denied_io.receipt().backend_read_operations, 0);
    drop(publication);
    let wrong_creator = crate::core::AuthorityIssuer::new(
        "created-owner", [81; 32], owner.root_bootstrap_owner().clone(),
    );
    let creator_denied = NativeMaterializationWork::new(guarded_publication_budget(), &cancellation)
        .expect("independent configured creator verification");
    let before = creator_denied.receipt();
    let error = owner.authenticate_root_bootstrap_creator_with_native_work(
        &wrong_creator.verifier(&mut None).expect("verifier copy"), &creator_denied,
    ).await.err().expect("same audience with a different key is not the configured creator");
    assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
    assert_eq!(creator_denied.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(creator_denied.receipt().backend_write_operations, before.backend_write_operations);
    let query = NativeMaterializationWork::new(guarded_publication_budget(), &cancellation)
        .expect("independent original bootstrap READ query");
    owner.authenticated_root_binding_for_read_with_native_work(&bootstrap, &query).await?;
    let mut substituted_root = original_root.clone();
    substituted_root.arguments = serde_json::json!({"substituted_original_root": true});
    let before = query.receipt();
    assert!(matches!(owner.verify_root_bootstrap_context_with_native_work(
        &substituted_root, bootstrap.binding(), &query,
    ).await, Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert_eq!(query.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(query.receipt().backend_write_operations, before.backend_write_operations);
    let foreign = crate::durable_host::tests::created_resource_access::CreationFixture::new_with_grants(
        &["foreign:bootstrap"],
    ).await?;
    let foreign_owner = foreign.host.journal_owner(
        foreign.root, (&foreign.root_lease.reservation).into(),
    ).await?;
    let before = query.receipt();
    assert!(matches!(foreign_owner.authenticated_root_binding_for_read_with_native_work(
        &bootstrap, &query,
    ).await, Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert_eq!(query.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(query.receipt().backend_write_operations, before.backend_write_operations);
    let before = cancelled.receipt();
    assert!(matches!(owner.verify_root_bootstrap_context_with_native_work(
        &original_root, bootstrap.binding(), &cancelled,
    ).await, Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(cancelled.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(cancelled.receipt().backend_write_operations, before.backend_write_operations);
    let mut source_io = crate::store::history::native_work_io(&query)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let observation_scope = creator.issue("owner", &fixture.root_scope.grants(), Some(agent), None, &mut None).expect("scope proof");
    let original_reservation = crate::distributed::original_reservation_proof_with_work(
        &fixture.stream, owner.root_bootstrap_owner(), &observation_scope, &creator.verifier(&mut None).expect("verifier copy"),
        original_root.operation_id, bootstrap.binding().fence(), &mut Some(&mut source_io),
    ).await?;
    fixture.host.verify_original_root_bootstrap_source_with_native_work(
        &original_reservation, &original_root, bootstrap.binding(), &query,
    )?;
    let before = query.receipt();
    assert!(matches!(fixture.host.verify_original_root_bootstrap_source_with_native_work(
        &original_reservation, &substituted_root, bootstrap.binding(), &query,
    ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert_eq!(query.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(query.receipt().backend_write_operations, before.backend_write_operations);
    drop(original_reservation);
    drop(source_io);
    drop(substituted_root);
    drop(original_root);
    drop(root_custody);
    drop(aggregate);
    drop(owner);
    Ok(())
}

fn projection_codec_budget() -> WorkBudget {
    WorkBudget {
        items_examined: 1_048_576,
        allocation_operations: 1_048_576,
        peak_allocation_bytes: 16 * 1_048_576,
        bytes_copied: 16 * 1_048_576,
        bytes_encoded: 16 * 1_048_576,
        bytes_hashed: 16 * 1_048_576,
        output_bytes: 16 * 1_048_576,
        ..publication_budget()
    }
}


#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn projection_locator_original_serde_cause_and_predecode_denials() -> Result<()> {
    use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
    let location = checkpoints::ProjectionLocation {
        authority: authority(), revision: 64, bytes: 1, digest: [4; 32],
        chunks: vec![checkpoints::ProjectionChunk { digest: [3; 32], bytes: 1 }],
    };
    let original = crate::contract::canonical_json_bytes(&location)?;
    let mut mutations = Vec::new();
    let mut duplicate = b"{\"revision\":64,".to_vec();
    duplicate.extend_from_slice(&original[1..]);
    mutations.push(duplicate);
    for field in ["authority", "revision", "bytes", "digest", "chunks"] {
        let mut value: serde_json::Value = crate::contract::json_from_slice(&original)?;
        value[field] = serde_json::json!("wrong field type");
        mutations.push(crate::contract::canonical_json_bytes(&value)?);
    }
    let mut value: serde_json::Value = crate::contract::json_from_slice(&original)?;
    value["chunks"][0]["digest"] = serde_json::json!([3]);
    mutations.push(crate::contract::canonical_json_bytes(&value)?);
    mutations.push(br#"{"authority":{},"revision":64,"bytes":1,"digest":[0],"chunks":[],"extra":true}"#.to_vec());
    for bytes in mutations {
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(projection_codec_budget(), &cancellation)
            .expect("finite original typed projection decoder");
        let mut io = history::native_work_io(&native).expect("original projection source guard");
        let error = checkpoints::decode_projection_location_with_work(&bytes, &mut Some(&mut io))
            .err().expect("concrete projection schema mutation rejects");
        let Error::NativeDecode { code, source, custody: Some(custody) } = error else {
            panic!("schema errors retain the original typed serde diagnostic");
        };
        assert_eq!(code, crate::wire::ErrorCode::Storage);
        assert!(source.is_data());
        assert!(source.line() > 0 && source.column() > 0);
        assert!(native.shares_ledger(custody.work()));
        assert!(custody.bytes() > 0);
        assert_eq!(io.into_custody().bytes(), 0);
        drop(source);
        drop(custody);
        native.admit_retained(projection_codec_budget().peak_allocation_bytes
            - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    }
    let malformed_schema = br#"{"authority":[]}"#;
    let cancellation = CancellationToken::new();
    let denied = NativeMaterializationWork::new(WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 1,
        ..projection_codec_budget()
    }, &cancellation).expect("ledger with no DTO allowance");
    let mut io = history::native_work_io(&denied).expect("original denied projection guard");
    assert!(matches!(checkpoints::decode_projection_location_with_work(malformed_schema, &mut Some(&mut io)),
        Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
            WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }), .. })));
    assert_eq!(io.into_custody().bytes(), 0);
    let denied = NativeMaterializationWork::new(
        WorkBudget { bytes_copied: 0, ..projection_codec_budget() }, &cancellation)
        .expect("original no-copy projection ledger");
    let mut io = history::native_work_io(&denied).expect("original no-copy source guard");
    assert!(matches!(checkpoints::decode_projection_location_with_work(malformed_schema, &mut Some(&mut io)),
        Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
            WorkError::BudgetExceeded { counter: "bytes_copied", .. }), .. })));
    assert_eq!(denied.receipt().bytes_copied, 0);
    assert_eq!(io.into_custody().bytes(), 0);
    let native = NativeMaterializationWork::new(projection_codec_budget(), &cancellation)
        .expect("original lexical controls");
    let mut io = history::native_work_io(&native).expect("original lexical guard");
    for bytes in [b"{".to_vec(), vec![b' '; acyclic_stream::MAX_RECORD_BYTES + 1]] {
        assert!(matches!(checkpoints::decode_projection_location_with_work(&bytes, &mut Some(&mut io)),
            Err(Error::NativeBoundary { .. })));
    }
    assert_eq!(io.into_custody().bytes(), 0);
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(projection_codec_budget(), &cancellation)
        .expect("original cancellable projection ledger");
    let mut io = history::native_work_io(&native).expect("original cancelled source guard");
    cancellation.cancel();
    assert!(matches!(checkpoints::decode_projection_location_with_work(&original, &mut Some(&mut io)),
        Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(native.receipt().items_examined, 0);
    assert_eq!(io.into_custody().bytes(), 0);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn projection_location_mutations_never_read_chunks_and_release_original_guards() -> Result<()> {
    use acyclic_fs::NativeMaterializationWork;
    use acyclic_stream::CommitMutation;
    for mode in 0..9 {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut aggregate = with_content(StreamAggregate::open(
            &client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?);
        for identity in 1..64 {
            let mut next = command(identity)?;
            next.expected_revision = aggregate.reducer().revision();
            aggregate.execute(next, #[cfg(feature = "filesystem")] None, &mut None).await?;
        }
        let mut next = command(64)?;
        next.expected_revision = 63;
        let ApplyResult::Applied { mut event } = aggregate.reducer().plan(&next)? else {
            panic!("fixture must issue original canonical event 64");
        };
        let publication = checkpoints::publication_with_work(&client, aggregate.reducer(), &event, &mut None).await?;
        let mut request = publication.request(
            &client, acyclic_stream::IdempotencyKey::new("projection-original-mutation")?, &mut None).await?.0;
        let canonical_path = authority().stream_path(&mut None)?;
        let suffix = canonical_path.strip_prefix("harness/v2/").expect("original canonical authority prefix");
        let head = format!("harness/v3/projection-checkpoints/{suffix}");
        for mutation in &mut request.mutations {
            let CommitMutation::Append { path, records } = mutation else { continue; };
            if path.as_str() == head {
                let mut location: serde_json::Value = crate::contract::json_from_slice(&records[0])?;
                match mode {
                    0 => location["authority"]["id"] = serde_json::json!("another-authority"),
                    1 => location["revision"] = serde_json::json!(63),
                    2 => location["bytes"] = serde_json::json!(0),
                    3 => location["digest"] = serde_json::json!([4]),
                    4 => location["chunks"] = serde_json::json!("not a Vec"),
                    5 => location["chunks"][0]["bytes"] = serde_json::json!(0),
                    6 => {
                        let mut duplicate = b"{\"revision\":64,".to_vec();
                        duplicate.extend_from_slice(&records[0][1..]);
                        records[0] = Bytes::from(duplicate);
                        continue;
                    }
                    _ => {}
                }
                records[0] = Bytes::from(crate::contract::canonical_json_bytes(&location)?);
            } else if mode == 7 && path.as_str() == authority().stream_path(&mut None)? {
                event.attestation[0] ^= 1;
                records[0] = Bytes::from(crate::wire_codec::encode_event(&authority(), &event)?);
            }
        }
        let detached = if mode == 8 {
            let index = request.mutations.iter().position(|mutation| {
                matches!(mutation, CommitMutation::Append { path, .. } if path.as_str() == head)
            }).expect("original atomic projection mutation");
            let CommitMutation::Append { mut records, .. } = request.mutations.remove(index) else {
                unreachable!("selected original Append")
            };
            Some(records.remove(0))
        } else { None };
        client.commit(request).await?;
        if let Some(bytes) = detached {
            acyclic_stream::Stream::new(std::sync::Arc::clone(&client), &head)?.append_at(bytes, 0).await?;
        }
        let before = provider.io_snapshot();
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(projection_codec_budget(), &cancellation)
            .expect("original finite cold projection ledger");
        let mut io = history::native_work_io(&native).expect("original cold projection guard");
        let error = checkpoints::load_with_work(
            &client, &authority(), &issuer().verifier(&mut None).expect("verifier copy"), checkpoints::default_projection_read_limits(),
            Some(64), &mut Some(&mut io)).await.err().expect("original projection mutation rejects");
        let after = provider.io_snapshot();
        let anchor_reads = u64::from(matches!(mode, 5 | 7 | 8));
        assert_eq!(native.receipt().authority_records_read, 1 + anchor_reads);
        assert_eq!(after.reads - before.reads, 1 + anchor_reads);
        assert_eq!(after.writes, before.writes);
        assert_eq!(io.into_custody().bytes(), 0);
        drop(error);
        native.admit_retained(projection_codec_budget().peak_allocation_bytes
            - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn cold_projection_original_cut_and_anchor_survive_later_checkpoint() -> Result<()> {
    use acyclic_fs::NativeMaterializationWork;
    let client = Arc::new(MemoryStream::default());
    let mut aggregate = with_content(StreamAggregate::open(
        &client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?);
    for identity in 1..=128 {
        let mut next = command(identity)?;
        next.expected_revision = aggregate.reducer().revision();
        aggregate.execute(next, #[cfg(feature = "filesystem")] None, &mut None).await?;
    }
    let original = checkpoints::load_with_work(
        &client, &authority(), &issuer().verifier(&mut None).expect("verifier copy"), checkpoints::default_projection_read_limits(),
        Some(64), &mut None).await?;
    assert_eq!(original.through_revision, 64);
    let original = original.snapshot.expect("the selected checkpoint exists");
    assert_eq!(original.revision, 64);
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(projection_codec_budget(), &cancellation)
        .expect("original finite historical projection ledger");
    let mut io = history::native_work_io(&native).expect("original historical projection guard");
    let loaded = checkpoints::load_with_work(
        &client, &authority(), &issuer().verifier(&mut None).expect("verifier copy"), checkpoints::default_projection_read_limits(),
        Some(64), &mut Some(&mut io)).await?;
    assert_eq!(loaded.through_revision, 64);
    let snapshot = loaded.snapshot.expect("native selected checkpoint exists");
    assert_eq!(snapshot, original);
    assert_eq!(snapshot.events.last(), original.events.last());
    drop(snapshot);
    assert_eq!(io.into_custody().bytes(), 0);
    native.admit_retained(projection_codec_budget().peak_allocation_bytes
        - NativeMaterializationWork::accounting_state_bytes())
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn cold_projection_original_audience_and_cancellation_precede_sdk_reads() -> Result<()> {
    use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError};
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    for cancelled in [false, true] {
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(projection_codec_budget(), &cancellation)
            .expect("original preflight ledger");
        let mut io = history::native_work_io(&native).expect("original preflight custody");
        if cancelled { cancellation.cancel(); }
        let other = crate::core::AuthorityIssuer::new("other-projection", [9; 32],
            Authority { kind: crate::core::AggregateKind::Task, id: "another-audience".into() });
        let verifier = if cancelled { issuer().verifier(&mut None).expect("verifier copy") } else { other.verifier(&mut None).expect("verifier copy") };
        let error = checkpoints::load_with_work(&client, &authority(), &verifier,
            checkpoints::default_projection_read_limits(), Some(64), &mut Some(&mut io))
            .await.err().expect("original audience/cancellation rejects before SDK");
        if cancelled {
            assert!(matches!(error, Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. }));
        } else {
            assert!(matches!(error, Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. }));
        }
        assert_eq!(native.receipt().authority_records_read, 0);
        assert_eq!(io.into_custody().bytes(), 0);
    }
    let sdk = provider.io_snapshot();
    assert_eq!(sdk.reads + sdk.tails as u64 + sdk.writes + sdk.inspections + sdk.commit_reads, 0);
    Ok(())
}
