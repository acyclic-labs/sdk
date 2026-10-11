// Same-engine publication receipt boundaries used by guarded fork and child bind.

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
    let reducer = Reducer::new(authority(), issuer().verifier(), schemas());
    let ApplyResult::Applied { event } = reducer.plan(&command(71)?)? else {
        return Err(Error::Invalid("fixture needs fresh event".into()));
    };
    let bytes = crate::wire_codec::encode_event(reducer.authority(), &event)?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut work = history::HistoryWork::new(publication_budget(), &cancellation);
    assert!(operations::IndexedPublication::new_with_work(
        reducer.authority(), &event, bytes, &mut Some(&mut work),
    ).is_err());
    Ok(())
}

#[tokio::test]
async fn immutable_stage_cancelled_before_commit_leaves_no_record() -> Result<()> {
    let client = StreamClient::new(Arc::new(MemoryStream::default()));
    let path = StreamPath::new("harness/v3/test-fork-stage/cancelled")?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut work = history::HistoryWork::new(publication_budget(), &cancellation);
    assert!(checkpoints::stage_immutable_record_with_work(
        &client, path.clone(), Bytes::from_static(b"immutable"), &mut Some(&mut work),
    ).await.is_err());
    assert!(matches!(client.bounds(path.as_str()).await, Err(acyclic_stream::StreamError::NotFound)));
    Ok(())
}

#[tokio::test]
async fn immutable_stage_reopen_preserves_exact_identity() -> Result<()> {
    let client = StreamClient::new(Arc::new(MemoryStream::default()));
    let path = StreamPath::new("harness/v3/test-fork-stage/reopen")?;
    let bytes = Bytes::from_static(b"immutable");
    checkpoints::stage_immutable_record(&client, path.clone(), bytes.clone()).await?;
    let reopened = client.clone();
    checkpoints::stage_immutable_record(&reopened, path.clone(), bytes).await?;
    assert_eq!(reopened.bounds(path.as_str()).await?.tail, 1);
    assert!(checkpoints::stage_immutable_record(
        &reopened, path, Bytes::from_static(b"hostile"),
    ).await.is_err());
    Ok(())
}

#[tokio::test]
async fn immutable_stage_zero_allowance_cannot_append() -> Result<()> {
    let client = StreamClient::new(Arc::new(MemoryStream::default()));
    let path = StreamPath::new("harness/v3/test-fork-stage/quota")?;
    let cancellation = CancellationToken::new();
    let mut work = history::HistoryWork::new(WorkBudget::default(), &cancellation);
    assert!(checkpoints::stage_immutable_record_with_work(
        &client, path.clone(), Bytes::from_static(b"immutable"), &mut Some(&mut work),
    ).await.is_err());
    assert!(matches!(client.bounds(path.as_str()).await, Err(acyclic_stream::StreamError::NotFound)));
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

#[tokio::test]
async fn checkpoint_publication_reuses_original_indexes_and_admits_clone_custody() -> Result<()> {
    let client = StreamClient::new(Arc::new(MemoryStream::default()));
    let mut aggregate = with_content(StreamAggregate::open(
        &client, authority(), issuer().verifier(), schemas(),
    ).await?);
    for identity in 1..64 {
        let mut next = command(identity)?;
        next.expected_revision = aggregate.reducer().revision();
        aggregate.execute(next).await?;
    }
    let mut next = command(64)?;
    next.expected_revision = aggregate.reducer().revision();
    let ApplyResult::Applied { event } = aggregate.reducer().plan(&next)? else {
        return Err(Error::Invalid("checkpoint fixture needs fresh event".into()));
    };
    let original = checkpoints::publication(&client, aggregate.reducer(), &event).await?;
    let budget = WorkBudget {
        peak_allocation_bytes: 256 * 1_048_576,
        bytes_encoded: 16 * 1_048_576,
        bytes_copied: 16 * 1_048_576,
        bytes_hashed: 16 * 1_048_576,
        items_examined: 1_048_576,
        ..publication_budget()
    };
    let cancellation = CancellationToken::new();
    let mut work = history::HistoryWork::new(budget, &cancellation);
    let measured = checkpoints::publication_with_work(
        &client, aggregate.reducer(), &event, &mut Some(&mut work),
    ).await?;
    assert_eq!(measured.path, original.path);
    assert_eq!(measured.expected_tail, original.expected_tail);
    assert_eq!(measured.bytes, original.bytes);
    let measured_request = measured.request(
        &client, acyclic_stream::IdempotencyKey::new("measured-checkpoint-equivalence")?,
    ).await?;
    let original_request = original.request(
        &client, acyclic_stream::IdempotencyKey::new("measured-checkpoint-equivalence")?,
    ).await?;
    assert_eq!(measured_request, original_request);
    assert!(work.receipt().peak_allocation_bytes > 0);
    let mut narrow = history::HistoryWork::new(
        WorkBudget { peak_allocation_bytes: 1, ..budget }, &cancellation,
    );
    assert!(checkpoints::publication_with_work(
        &client, aggregate.reducer(), &event, &mut Some(&mut narrow),
    ).await.is_err());
    assert_eq!(client.bounds(authority().stream_path()?.as_str()).await?.tail, 63);
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
    let stream_provider = crate::resources::ProviderRef::new("created-controls", "stream", "2")?;
    let verifier = Arc::new(CompositeForkVerifier::new(vec![
        Arc::new(FilesystemForkVerifier::new(fixture.filesystem.clone(), 65_536)?),
        Arc::new(StreamHistoryForkVerifier::new(stream_provider.clone())?),
    ])?);
    let mut original = StreamAggregate::open(
        &fixture.stream, created.seed.parent.clone(), created.parent_verifier.clone(), SchemaRegistry::new(),
    ).await?.with_fork_verifier(verifier.clone());
    original.publish_fork_report(created.report.clone(), created.parent_scope.clone()).await?;
    let parent_issuer = crate::core::AuthorityIssuer::new("created-owner", [80; 32], created.seed.child.clone());
    let parent_scope = parent_issuer.root_for_agent(created.seed.child_agent, "measured-fork-parent", Capabilities::new([
        "conversation:bind".into(), "fork:publish".into(),
        created.private.capability(VolumeOperation::Read)?,
        created.project.capability(VolumeOperation::Read)?,
    ]));
    let mut parent = StreamAggregate::open(
        &fixture.stream, created.seed.child.clone(), parent_issuer.verifier(), SchemaRegistry::new(),
    ).await?.with_fork_verifier(verifier);
    parent.bind_published_child(&original, &created.seed, parent_scope.clone()).await?;
    let task = crate::TaskId::from_bytes([84; 16]);
    assert!(matches!(fixture.admit_child(created.request.child.clone()).await?, crate::Admission::Accepted(id) if id == task));
    let crate::distributed::WorkPull::Claimed(lease) = fixture.host.recover_work(task).await? else {
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
        stream_provider.clone(), fork.parent.stream_path()?.into_bytes(), Some(fork.parent_revision.to_string()),
    )?);
    fork.selections[1].revision = ResourceRevision::Project { volume: created.project.clone(), generation: project_generation };
    fork.selections[2].revision = ResourceRevision::PrivateVolume {
        volume: created.private.clone(), generation: created.seed.child_private_generation.clone(), paths: Vec::new(),
    };
    let history = crate::history::HistoryReader::new(&fixture.stream, &fork.parent, parent_issuer.verifier())?
        .fork_history(parent.reducer(), 0, crate::history::HistoryReadLimits { maximum_events: 2, maximum_bytes: 65_536 }).await?;
    let resolver = Arc::new(FilesystemContentVerifier::new(
        fixture.filesystem.clone(), parent_issuer.verifier(), parent_scope.clone(), 65_536,
    )?);
    let preparer = FilesystemForkPreparer::new(fixture.filesystem.clone(), parent.reducer().clone(),
        parent_issuer.verifier(), parent_scope.clone(), created.project.clone(), stream_provider, resolver)?
        .with_history_context(history)?;
    let mut admission = fixture.admission(85, serde_json::json!({"action":"filesystem-io"}));
    admission.parent = Some(task);
    admission.grants = Capabilities::new([
        private.capability(VolumeOperation::Read)?, private.capability(VolumeOperation::Write)?,
        project.capability(VolumeOperation::Read)?, project.capability(VolumeOperation::Write)?,
        "conversation:bind".into(),
    ]);
    admission = admission.with_resource_proofs(preparer.creation_proofs(&fork, task)?)?;
    let mut request = created.request.clone();
    request.child = admission;
    request.resources = preparer.creation_resources(&fork)?;
    request.payload = serde_json::to_value(&fork).map_err(|error| Error::Invalid(error.to_string()))?;
    let preparation = owner.resource_preparation(request).await?;
    let report = preparer.with_task_preparation(Arc::new(preparation))?.prepare(fork).await?;
    let child_issuer = crate::core::AuthorityIssuer::new("created-owner", [80; 32], report.request.child.clone());
    let child_scope = child_issuer.root_for_agent(child_agent, "measured-fork-child",
        Capabilities::new(["conversation:bind".into()]));
    Ok(GuardedForkFixture { fixture, owner, lease, parent, report, parent_scope,
        parent_verifier: parent_issuer.verifier(), child_scope, child_verifier: child_issuer.verifier() })
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
    let seed = setup.report.clone().into_seed()?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let before = setup.parent.reducer().revision();
    assert!(setup.parent.publish_fork_report_measured(setup.report.clone(), setup.parent_scope.clone(),
        &setup.owner, budget, &cancelled).await.is_err());
    assert!(setup.parent.publish_fork_report_measured(setup.report.clone(), setup.parent_scope.clone(),
        &setup.owner, WorkBudget { peak_allocation_bytes: 1, ..budget }, &cancellation).await.is_err());
    let wrong_parent_agent = crate::core::AuthorityIssuer::new("created-owner", [80; 32], seed.parent.clone())
        .root_for_agent(AgentId::from_bytes([80; 16]), "not-original-admitted-parent", Capabilities::new(["fork:publish".into()]));
    assert!(setup.parent.publish_fork_report_measured(setup.report.clone(), wrong_parent_agent,
        &setup.owner, budget, &cancellation).await.is_err());
    let mut hostile_report = setup.report.clone();
    hostile_report.request.child_agent = AgentId::from_bytes([96; 16]);
    assert!(setup.parent.publish_fork_report_measured(hostile_report, setup.parent_scope.clone(),
        &setup.owner, budget, &cancellation).await.is_err());
    assert_eq!(setup.parent.reducer().revision(), before);
    // Discarding the original confirmed reply models a lost client reply, not a
    // synthesized SDK acknowledgment or another publication engine.
    setup.parent.publish_fork_report_measured(setup.report.clone(), setup.parent_scope.clone(),
        &setup.owner, budget, &cancellation).await.map_err(|failure| failure.error)?;
    let mut reopened = StreamAggregate::open(&setup.fixture.stream, seed.parent.clone(),
        setup.parent_verifier.clone(), SchemaRegistry::new()).await?
        .with_fork_verifier(setup.parent.fork_verifier.clone().ok_or_else(|| Error::Invalid("original verifier missing".into()))?);
    let replay = reopened.publish_fork_report_measured(setup.report.clone(), setup.parent_scope.clone(),
        &setup.owner, budget, &cancellation).await.map_err(|failure| failure.error)?;
    assert!(matches!(replay.value, ApplyResult::Replayed { .. }));
    assert_eq!(reopened.reducer().revision(), before + 1);
    assert!(replay.work.backend_read_operations > 0);
    assert!(reopened.publish_fork_report_measured(setup.report.clone(), setup.parent_scope.clone(),
        &setup.owner, WorkBudget { backend_read_operations: replay.work.backend_read_operations - 1, ..budget },
        &cancellation).await.is_err());
    assert_eq!(setup.fixture.stream.bounds(seed.parent.stream_path()?.as_str()).await?.tail, before + 1);
    let mut child = StreamAggregate::open(&setup.fixture.stream, seed.child.clone(),
        setup.child_verifier.clone(), SchemaRegistry::new()).await?;
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
    let wrong_agent = crate::core::AuthorityIssuer::new("created-owner", [80; 32], seed.child.clone())
        .root_for_agent(AgentId::from_bytes([96; 16]), "wrong-child-agent", Capabilities::new(["conversation:bind".into()]));
    assert!(child.bind_published_child_measured(&reopened, &seed, wrong_agent,
        &setup.owner, budget, &cancellation).await.is_err());
    child.bind_published_child_measured(&reopened, &seed, setup.child_scope.clone(),
        &setup.owner, budget, &cancellation).await.map_err(|failure| failure.error)?;
    let mut reopened_child = StreamAggregate::open(&setup.fixture.stream, seed.child.clone(),
        setup.child_verifier.clone(), SchemaRegistry::new()).await?;
    reopened_child.bind_published_child_measured(&reopened, &seed, setup.child_scope.clone(),
        &setup.owner, budget, &cancellation).await.map_err(|failure| failure.error)?;
    assert_eq!(reopened_child.reducer().revision(), 1);
    assert_eq!(setup.fixture.stream.bounds(seed.child.stream_path()?.as_str()).await?.tail, 1);
    setup.fixture.host.settle_task(crate::TaskId::from_bytes([84; 16]), (&setup.lease.reservation).into(),
        crate::Outcome::Failed { message: "test closes original publication lease".into() }).await?;
    assert!(reopened.publish_fork_report_measured(setup.report, setup.parent_scope, &setup.owner, budget, &cancellation).await.is_err());
    assert_eq!(setup.fixture.stream.bounds(seed.parent.stream_path()?.as_str()).await?.tail, before + 1);
    Ok(())
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn guarded_child_rejects_original_event_one_with_wrong_binding_identity() -> Result<()> {
    use crate::core::SchemaRegistry;
    let mut setup = guarded_fork_fixture().await?;
    let cancellation = CancellationToken::new();
    let budget = guarded_publication_budget();
    let seed = setup.report.clone().into_seed()?;
    setup.parent.publish_fork_report_measured(setup.report, setup.parent_scope,
        &setup.owner, budget, &cancellation).await.map_err(|failure| failure.error)?;
    let mut child = StreamAggregate::open(&setup.fixture.stream, seed.child.clone(),
        setup.child_verifier, SchemaRegistry::new()).await?;
    // This is an actual canonical source event, not a projected/reconstructed
    // event or a forged reader callback. Even the right agent cannot replace
    // the deterministic fork operation and canonical parent causal reference.
    child.execute(Command {
        operation_id: OperationId::from_bytes([97; 16]),
        idempotency_key: IdempotencyKey::new("hostile-original-child-binding")?,
        expected_revision: 0, scope: setup.child_scope.clone(), causal_parent: None,
        action: Action::BindConversation { agent: seed.child_agent },
    }).await?;
    assert!(child.bind_published_child_measured(&setup.parent, &seed, setup.child_scope,
        &setup.owner, budget, &cancellation).await.is_err());
    assert_eq!(setup.fixture.stream.bounds(seed.child.stream_path()?.as_str()).await?.tail, 1);
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
    let agent = owner.authenticated_binding_agent()?
        .ok_or_else(|| Error::Invalid("original ROOT fixture has no authenticated agent".into()))?;
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "original-native-root-conversation".into(),
    };
    let original_issuer = crate::core::AuthorityIssuer::new("created-owner", [80; 32], audience.clone());
    let scope = original_issuer.root_for_agent(
        agent, "original-native-root-bind", Capabilities::new(["conversation:bind".into()]),
    );
    let command = Command {
        operation_id: OperationId::from_bytes([124; 16]),
        idempotency_key: IdempotencyKey::new("original-native-root-bind")?,
        expected_revision: 0, scope, causal_parent: None,
        action: Action::BindConversation { agent },
    };
    let mut aggregate = StreamAggregate::open(
        &fixture.stream, audience.clone(), original_issuer.verifier(), SchemaRegistry::new(),
    ).await?;
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(guarded_publication_budget(), &cancellation)
        .expect("original ROOT ledger");
    let (first, first_custody) = aggregate.execute_task_command_with_native_work(
        command.clone(), &owner, &native,
    ).await?;
    assert!(matches!(&first, ApplyResult::Applied { event }
        if event.revision == 1 && event.operation_id == command.operation_id));
    assert_eq!(aggregate.reducer().conversation().and_then(|state| state.agent), Some(agent));
    assert!(first_custody.bytes() > 0);
    assert!(first_custody.work().shares_ledger(&native));
    owner.authenticate_fork_parent(&audience)?;
    let canonical = aggregate.operation_event(command.operation_id).await?
        .ok_or_else(|| Error::NotFound("original canonical ROOT event".into()))?;
    assert!(matches!(&first, ApplyResult::Applied { event } if event == &canonical));
    let record = operations::one_record_with_work(
        &fixture.stream, aggregate.stream.path(), 0, &mut None,
    ).await?.ok_or_else(|| Error::NotFound("original canonical ROOT record".into()))?;
    let raw_bytes = record.value.len() as u64;
    let dto_owned = crate::core::event_clone_reservation(&canonical)?;
    assert_eq!(native.receipt().output_bytes, raw_bytes);
    assert_eq!(native.receipt().items_returned, 1);
    assert_eq!(first_custody.bytes(), dto_owned);
    let prefix = native.receipt();
    let (retry, retry_custody) = aggregate.execute_task_command_with_native_work(
        command.clone(), &owner, &native,
    ).await?;
    assert!(matches!(&retry, ApplyResult::Replayed { event } if event == &canonical));
    assert!(native.receipt().backend_read_operations > prefix.backend_read_operations);
    assert!(retry_custody.work().shares_ledger(&native));
    assert_eq!(retry_custody.bytes(), dto_owned);
    assert_eq!(native.receipt().output_bytes - prefix.output_bytes, raw_bytes);
    assert_eq!(native.receipt().items_returned - prefix.items_returned, 1);
    assert_eq!(fixture.stream.bounds(audience.stream_path()?.as_str()).await?.tail, 1);

    // A new query ledger authenticates the same immutable retry without
    // rebinding the original owner proof or the aggregate's existing state.
    let independent = NativeMaterializationWork::new(guarded_publication_budget(), &cancellation)
        .expect("independent original query ledger");
    let (independent_retry, independent_custody) = aggregate.execute_task_command_with_native_work(
        command.clone(), &owner, &independent,
    ).await?;
    assert!(matches!(&independent_retry, ApplyResult::Replayed { event } if event == &canonical));
    assert!(independent_custody.work().shares_ledger(&independent));
    assert_eq!(independent_custody.bytes(), dto_owned);
    assert_eq!(independent.receipt().output_bytes, raw_bytes);
    assert_eq!(independent.receipt().items_returned, 1);

    let mut wrong_actor = command.clone();
    wrong_actor.action = Action::BindConversation { agent: AgentId::from_bytes([125; 16]) };
    let mut missing_grant = command.clone();
    missing_grant.scope = original_issuer.root_for_agent(
        agent, "original-native-root-bind", Capabilities::new([]),
    );
    let mut causal_binding = command.clone();
    causal_binding.causal_parent = Some(EventReference { authority: audience.clone(), revision: 1 });
    for invalid in [wrong_actor, missing_grant, causal_binding] {
        let error = aggregate.execute_task_command_with_native_work(invalid, &owner, &native)
            .await.err().expect("original ROOT actor, grant and no-parent policy remain required");
        assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
    }

    let wrong_issuer = crate::core::AuthorityIssuer::new("created-owner", [81; 32], audience.clone());
    let mut wrong_scope = command.clone();
    wrong_scope.scope = wrong_issuer.root_for_agent(
        agent, "original-native-root-bind", Capabilities::new(["conversation:bind".into()]),
    );
    let wrong_scope_error = aggregate.execute_task_command_with_native_work(
        wrong_scope, &owner, &native,
    ).await.err().expect("original issuer key must authenticate the binding scope");
    assert_eq!(wrong_scope_error.code(), crate::wire::ErrorCode::Unauthorized);
    let denied = NativeMaterializationWork::new(
        WorkBudget { bytes_copied: 0, ..guarded_publication_budget() }, &cancellation,
    ).expect("zero-copy original ledger");
    assert!(matches!(aggregate.execute_task_command_with_native_work(
        command.clone(), &owner, &denied,
    ).await, Err(Error::NativeWork {
        cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied", ..
        }), ..
    })));
    let cancelled_token = CancellationToken::new();
    let cancelled = NativeMaterializationWork::new(guarded_publication_budget(), &cancelled_token)
        .expect("original ledger before cancellation");
    cancelled_token.cancel();
    assert!(matches!(aggregate.execute_task_command_with_native_work(
        command, &owner, &cancelled,
    ).await, Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(fixture.stream.bounds(audience.stream_path()?.as_str()).await?.tail, 1);
    drop(first);
    drop(first_custody);
    drop(retry);
    drop(retry_custody);
    drop(independent_retry);
    drop(independent_custody);
    // The intrinsic reducer and immutable owner proof remain usable after
    // every caller-owned result payload and its query custody have dropped.
    assert_eq!(aggregate.reducer().revision(), 1);
    owner.authenticate_fork_parent(&audience)?;
    drop(aggregate);
    drop(owner);
    Ok(())
}
