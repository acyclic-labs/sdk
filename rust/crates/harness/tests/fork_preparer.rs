//! End-to-end preparation, lost-reply reconciliation, and parent publication.
#![cfg(feature = "filesystem")]

use acyclic_fs::Fs;
use acyclic_harness::filesystem::{
    FilesystemContentVerifier, FilesystemForkPreparer, FilesystemForkVerifier, FilesystemHost,
    workspace_ref,
};
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result,
    conversation::{
        Attachment, ContentGrant, ContentResidencyVerifier, ConversationMessage, Limits,
        MessageKind, ReferencedAttachments, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry},
    fork::{
        Capture, CapturedResource, CompositeForkVerifier, ForkCaptureProvider, ForkHistoryPolicy,
        ForkPreparation, ForkRequest, ForkSeedVerifier, ForkSelection, ResourceRevision,
        StreamHistoryForkVerifier,
    },
    resources::{ArtifactRef, ProviderRef, StreamRef},
    store::StreamAggregate,
};
use acyclic_stream::{MemoryStream};
use futures::future::BoxFuture;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use uuid::Uuid;

struct LostCaptureReply {
    provider: ProviderRef,
    capture_calls: AtomicUsize,
    reconcile_calls: AtomicUsize,
    observation_ready: AtomicBool,
}

impl ForkCaptureProvider for LostCaptureReply {
    fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    fn capture<'a>(
        &'a self,
        request: &'a ForkRequest,
        _selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Capture>> {
        self.capture_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Err(Error::Indeterminate(request.operation_id)) })
    }

    fn reconcile<'a>(
        &'a self,
        _request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Option<Capture>>> {
        self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
        let ready = self.observation_ready.load(Ordering::SeqCst);
        Box::pin(async move {
            Ok(ready.then(|| {
                Capture::Captured(CapturedResource {
                    source: selection.revision.clone(),
                    revision: selection.revision.clone(),
                })
            }))
        })
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each step next to the state it checks"
)]
async fn exact_fork_preparation_reconciles_without_allocating_another_child() -> Result<()> {
    let provider = ProviderRef::new("fork-e2e".into(), "filesystem".into(), "2".into(), &mut None)?;
    let stream_provider = ProviderRef::new("fork-e2e".into(), "stream".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let parent = Authority {
        kind: AggregateKind::Conversation,
        id: "parent".into(),
    };
    let child = Authority {
        kind: AggregateKind::Conversation,
        id: "child".into(),
    };
    let parent_agent = AgentId::from_bytes([1; 16]);
    let child_agent = AgentId::from_bytes([2; 16]);
    let project = VolumeRef::new(
        provider.clone(),
        "parent-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    let project_head = host.create_volume(&project).await?;
    let child_project = VolumeRef::new(
        provider.clone(),
        "child-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    let child_private = VolumeRef::new(
        provider.clone(),
        "child-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
        &mut None,
    )?;
    let parent_private = VolumeRef::new(
        provider.clone(),
        "parent-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(parent_agent),
        &mut None,
    )?;
    host.create_volume(&parent_private).await?;
    let issuer = AuthorityIssuer::new("fork-e2e", [7; 32], parent.clone());
    let scope = issuer.issue("parent", &Capabilities::new([
            "conversation:bind".to_owned(),
            "conversation:append".to_owned(),
            "fork:publish".to_owned(),
            project.capability(VolumeOperation::Read, &mut None)?,
            parent_private.capability(VolumeOperation::Read, &mut None)?,
            parent_private.capability(VolumeOperation::Write, &mut None)?,
        ]), Some(parent_agent), None, &mut None).expect("scope proof");
    let stream = Arc::new(MemoryStream::default());
    let resolver = FilesystemContentVerifier::new(host.clone(), &issuer.verifier(&mut None).expect("verifier copy"), &scope, 64 * 1_024, None)?;
    let mut aggregate = StreamAggregate::open(
        &stream,
        parent.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(), None, acyclic_harness::store::default_projection_read_limits(), acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
    .await?
    .with_content_verifier(resolver.clone())
    .with_fork_verifier(Arc::new(CompositeForkVerifier::new(&[
        FilesystemForkVerifier::new(host.clone(), 64 * 1_024, None, None)?,
        Arc::new(StreamHistoryForkVerifier::new(&stream_provider, &mut None)?),
    ], &mut None)?));
    aggregate.set_resident_event_limit(1)?;
    aggregate
        .execute(Command {
            operation_id: OperationId::from_bytes([3; 16]),
            idempotency_key: IdempotencyKey::new("bind-parent")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::BindConversation {
                agent: parent_agent,
            },
        }, #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    let parent_write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &parent_private,
        VolumeOperation::Write,
        u64::MAX, &mut None,
    )?;
    let text = host
        .put_content(
            &parent_private,
            &parent_write,
            "messages/request.txt",
            b"recursive context",
            "text/plain",
            "request.txt",
            1_024,
            &IdempotencyKey::new("parent-message")?,
         None, None)
        .await?;
    let attachment = host
        .put_content(
            &parent_private,
            &parent_write,
            "attachments/evidence.bin",
            b"fork evidence",
            "application/octet-stream",
            "evidence.bin",
            1_024,
            &IdempotencyKey::new("parent-attachment")?,
         None, None)
        .await?;
    let manifest_bytes = serde_json::to_vec(&vec![Attachment {
        file: attachment.clone(),
        label: Some("evidence".into()),
    }])
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let manifest = host
        .put_content(
            &parent_private,
            &parent_write,
            "attachments/manifest.json",
            &manifest_bytes,
            "application/vnd.acyclic.harness.attachments+json",
            "manifest.json",
            1_024,
            &IdempotencyKey::new("parent-manifest")?,
         None, None)
        .await?;
    aggregate
        .execute(Command {
            operation_id: OperationId::from_bytes([5; 16]),
            idempotency_key: IdempotencyKey::new("append-parent-message")?,
            expected_revision: 1,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::AppendConversationMessage {
                message: Box::new(ConversationMessage {
                    id: Uuid::from_bytes([6; 16]),
                    sequence: 1,
                    kind: MessageKind::User,
                    content: text.clone(),
                    attachments: ReferencedAttachments::Manifest {
                        manifest: manifest.clone(),
                        item_count: 1,
                    },
                    reply_to: None,
                    tool_call_id: None,
                    extensions: Default::default(),
                }),
            },
        }, #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    assert!(matches!(
        aggregate.reducer().events_after(0, 2),
        Err(Error::Unsupported(_))
    ));
    assert_eq!(aggregate.reducer().events_after(1, 2)?.len(), 1);
    assert_eq!(
        aggregate.read_event_at(1, &mut None).await?.0.operation_id,
        OperationId::from_bytes([3; 16])
    );
    let restored_parent = Reducer::restore(
        aggregate.reducer().snapshot(usize::MAX, &mut None)?.0,
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(), &mut None,
    )?;
    assert!(matches!(
        restored_parent.events_after(0, 2),
        Err(Error::Unsupported(_))
    ));
    assert_eq!(restored_parent.events_after(1, 2)?.len(), 1);
    let preparer = FilesystemForkPreparer::new(
        host.clone(),
        restored_parent.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        resolver.clone(),
        None, None, #[cfg(not(target_arch = "wasm32"))] None,
    )?;
    let request = ForkRequest {
        authorized_source: None,
        operation_id: OperationId::from_bytes([4; 16]),
        parent: parent.clone(),
        parent_revision: aggregate.reducer().revision(),
        child,
        child_agent,
        attached_agents: Vec::new(),
        preparation: ForkPreparation {
            child_result_volume: None,
            summary: None,
            child_project_volume: child_project.clone(),
            child_private_volume: child_private.clone(),
            inherited_through_sequence: 0,
            maximum_inherited_messages: 64,
            maximum_inherited_bytes: 64 * 1_024,
            maximum_inherited_references: 64,
        },
        selections: vec![
            ForkSelection {
                required: true,
                revision: ResourceRevision::PrivateVolume {
                    volume: parent_private.clone(),
                    generation: host
                        .resolve(&workspace_ref(
                            provider.clone(),
                            &parent_private.storage_name(&mut None)?,
                        )?)
                        .await?
                        .generation,
                    paths: Vec::new(),
                },
            },
            ForkSelection {
                required: true,
                revision: ResourceRevision::History(StreamRef::new(
                    stream_provider.clone(),
                    parent.stream_path(&mut None)?.into_bytes(),
                    Some("2".into()),
                )?),
            },
            ForkSelection {
                required: true,
                revision: ResourceRevision::Project {
                    volume: project.clone(),
                    generation: project_head.generation,
                },
            },
        ],
        boundary: None,
    };
    let request = request.with_history_policy(&restored_parent, ForkHistoryPolicy::default(), &mut None)?;
    assert_eq!(request.preparation.inherited_through_sequence, 1);
    let mut stale = request.clone();
    stale.parent_revision = 0;
    assert!(matches!(
        stale.with_history_policy(aggregate.reducer(), ForkHistoryPolicy::default(), &mut None),
        Err(Error::Conflict(_))
    ));
    let mut too_small = request.clone();
    too_small.preparation.maximum_inherited_messages = 0;
    assert!(
        too_small
            .with_history_policy(aggregate.reducer(), ForkHistoryPolicy::default(), &mut None)
            .is_err()
    );
    let mut fresh_request = request.clone();
    fresh_request.operation_id = OperationId::from_bytes([51; 16]);
    fresh_request.child.id = "fresh-child".into();
    fresh_request.preparation.child_project_volume = VolumeRef::new(
        provider.clone(),
        "fresh-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    fresh_request.preparation.child_private_volume = VolumeRef::new(
        provider.clone(),
        "fresh-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
        &mut None,
    )?;
    let fresh_request =
        fresh_request.with_history_policy(aggregate.reducer(), ForkHistoryPolicy::Fresh, &mut None)?;
    let fresh_report = aggregate
        .prepare_fork(&preparer, fresh_request.clone())
        .await?;
    assert_eq!(fresh_report.inherited_through_sequence, 0);
    assert!(fresh_report.inherited_context.is_empty());
    assert!(fresh_report.reference_grants.is_empty());
    assert_eq!(
        aggregate.reconcile_fork(&preparer, fresh_request).await?,
        Some(fresh_report),
    );
    let report = aggregate.prepare_fork(&preparer, request.clone()).await?;
    assert_eq!(report.inherited_context.len(), 1);
    assert!(
        report
            .reference_grants
            .iter()
            .any(|grant| grant.file == text && grant.reader == child_agent)
    );
    let mut unclaimed = report.clone().into_seed(&mut None)?;
    unclaimed.operation_id = OperationId::from_bytes([10; 16]);
    assert!(
        FilesystemForkVerifier::new(host.clone(), 64 * 1_024, None, None)?
            .verify(&unclaimed, &mut None)?
            .await
            .is_err()
    );
    let mut changed_seed = report.clone().into_seed(&mut None)?;
    changed_seed
        .attached_agents
        .push(AgentId::from_bytes([11; 16]));
    changed_seed.validate(&mut None)?;
    assert!(matches!(
        FilesystemForkVerifier::new(host.clone(), 64 * 1_024, None, None)?
            .verify(&changed_seed, &mut None)?
            .await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    assert_eq!(
        aggregate.reconcile_fork(&preparer, request.clone()).await?,
        Some(report.clone())
    );
    assert_eq!(
        aggregate.prepare_fork(&preparer, request.clone()).await?,
        report
    );
    let mut changed = request.clone();
    changed.preparation.child_project_volume = VolumeRef::new(
        provider.clone(),
        "another-child-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    assert!(aggregate.reconcile_fork(&preparer, changed).await.is_err());
    let mut reused_private = request.clone();
    reused_private.operation_id = OperationId::from_bytes([8; 16]);
    reused_private.child.id = "another-child".into();
    assert!(
        aggregate
            .prepare_fork(&preparer, reused_private)
            .await
            .is_err()
    );
    let mut reused_project = request.clone();
    reused_project.operation_id = OperationId::from_bytes([9; 16]);
    reused_project.child.id = "third-child".into();
    reused_project.preparation.child_private_volume = VolumeRef::new(
        provider.clone(),
        "another-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
        &mut None,
    )?;
    assert!(
        aggregate
            .prepare_fork(&preparer, reused_project)
            .await
            .is_err()
    );
    let objects_provider = ProviderRef::new("fork-e2e".into(), "objects".into(), "2".into(), &mut None)?;
    let lost_reply = Arc::new(LostCaptureReply {
        provider: objects_provider.clone(),
        capture_calls: AtomicUsize::new(0),
        reconcile_calls: AtomicUsize::new(0),
        observation_ready: AtomicBool::new(false),
    });
    let composed = FilesystemForkPreparer::new(
        host.clone(),
        aggregate.reducer().clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        resolver.clone(),
        None, None, #[cfg(not(target_arch = "wasm32"))] None,
    )?
    .with_capture_provider(lost_reply.clone())?;
    let another_agent = AgentId::from_bytes([12; 16]);
    let mut uncertain = request.clone();
    uncertain.operation_id = OperationId::from_bytes([13; 16]);
    uncertain.child.id = "uncertain-child".into();
    uncertain.child_agent = another_agent;
    uncertain.preparation.child_project_volume = VolumeRef::new(
        provider.clone(),
        "uncertain-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    uncertain.preparation.child_private_volume = VolumeRef::new(
        provider.clone(),
        "uncertain-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(another_agent),
        &mut None,
    )?;
    let attached_reader = AgentId::from_bytes([14; 16]);
    uncertain.attached_agents.push(attached_reader);
    uncertain.selections.push(ForkSelection {
        required: false,
        revision: ResourceRevision::Artifact(ArtifactRef::new(
            objects_provider,
            b"artifact".to_vec(),
            Some("immutable-v1".into()),
        )?),
    });
    assert!(matches!(
        aggregate.prepare_fork(&composed, uncertain.clone()).await,
        Err(Error::Indeterminate(_))
    ));
    assert_eq!(
        aggregate
            .reconcile_fork(&composed, uncertain.clone())
            .await?,
        None
    );
    assert!(matches!(
        aggregate.prepare_fork(&composed, uncertain.clone()).await,
        Err(Error::Indeterminate(_))
    ));
    assert_eq!(lost_reply.capture_calls.load(Ordering::SeqCst), 1);
    lost_reply.observation_ready.store(true, Ordering::SeqCst);
    let recovered = aggregate.prepare_fork(&composed, uncertain.clone()).await?;
    assert!(matches!(
        recovered.captures.get(3),
        Some(Capture::Captured(_))
    ));
    assert_eq!(recovered.attachment_manifests, vec![manifest.clone()]);
    let recovered_seed = recovered.clone().into_seed(&mut None)?;
    let readable = recovered_seed.reference_capabilities(attached_reader)?;
    assert!(readable.contains(&text.read_capability()?));
    assert!(readable.contains(&attachment.read_capability()?));
    assert!(readable.contains(&manifest.read_capability()?));
    assert!(!readable.contains(&parent_private.capability(VolumeOperation::Write, &mut None)?));
    let child_issuer = AuthorityIssuer::new("fork-e2e", [7; 32], uncertain.child.clone());
    let reader_scope = child_issuer.issue("attached-reader", &readable, Some(attached_reader), None, &mut None).expect("scope proof");
    let attached_resolver =
        FilesystemContentVerifier::new(host.clone(), &child_issuer.verifier(&mut None).expect("verifier copy"), &reader_scope, 1_024, None)?;
    attached_resolver
        .verify_manifest(&manifest, 1, &Limits::default(), None)?
        .await?;
    assert_eq!(lost_reply.capture_calls.load(Ordering::SeqCst), 1);
    assert_eq!(lost_reply.reconcile_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        aggregate
            .reconcile_fork(&composed, uncertain.clone())
            .await?,
        Some(recovered.clone())
    );
    let resumed_uncertain = FilesystemForkPreparer::new(
        host.clone(),
        aggregate.reducer().clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        resolver.clone(),
        None, None, #[cfg(not(target_arch = "wasm32"))] None,
    )?
    .with_capture_provider(lost_reply.clone())?;
    assert_eq!(
        aggregate
            .reconcile_fork(&resumed_uncertain, uncertain.clone())
            .await?,
        Some(recovered.clone()),
    );
    assert_eq!(
        host.resolve(&workspace_ref(
            provider.clone(),
            &uncertain.preparation.child_private_volume.storage_name(&mut None)?,
        )?)
        .await?
        .generation,
        recovered.child_private_generation,
    );
    assert_eq!(lost_reply.capture_calls.load(Ordering::SeqCst), 1);
    aggregate
        .publish_fork_report(report.clone(), scope.clone(), #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    let mut later_message = aggregate
        .reducer()
        .conversation()
        .and_then(|state| state.messages().first())
        .ok_or_else(|| Error::Invalid("parent message missing".into()))?
        .clone();
    later_message.id = Uuid::from_bytes([52; 16]);
    later_message.sequence = 2;
    aggregate
        .execute(Command {
            operation_id: OperationId::from_bytes([53; 16]),
            idempotency_key: IdempotencyKey::new("later-parent-message")?,
            expected_revision: aggregate.reducer().revision(),
            scope: scope.clone(),
            causal_parent: None,
            action: Action::AppendConversationMessage {
                message: Box::new(later_message),
            },
        }, #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    assert_eq!(report.inherited_through_sequence, 1);
    assert!(matches!(
        request
            .clone()
            .with_history_policy(aggregate.reducer(), ForkHistoryPolicy::default(), &mut None),
        Err(Error::Conflict(_))
    ));
    let mut bounded_later = request.clone();
    bounded_later.parent_revision = aggregate.reducer().revision();
    for selection in &mut bounded_later.selections {
        if matches!(selection.revision, ResourceRevision::History(_)) {
            selection.revision = ResourceRevision::History(StreamRef::new(
                stream_provider.clone(),
                parent.stream_path(&mut None)?.into_bytes(),
                Some(aggregate.reducer().revision().to_string()),
            )?);
        }
    }
    bounded_later.preparation.maximum_inherited_messages = 1;
    assert!(
        bounded_later
            .with_history_policy(aggregate.reducer(), ForkHistoryPolicy::default(), &mut None)
            .is_err()
    );
    let restarted = FilesystemForkPreparer::new(
        host,
        aggregate.reducer().clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        scope,
        project,
        stream_provider,
        resolver,
        None, None, #[cfg(not(target_arch = "wasm32"))] None,
    )?;
    assert_eq!(
        aggregate.reconcile_fork(&restarted, request).await?,
        Some(report)
    );
    Ok(())
}
