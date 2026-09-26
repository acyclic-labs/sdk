//! End-to-end preparation, lost-reply reconciliation, and parent publication.

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result,
    conversation::{
        Attachment, ContentGrant, ContentResidencyVerifier, ConversationMessage, Limits,
        MessageKind, ReferencedAttachments, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry},
    fork::{
        Capture, CapturedResource, CompositeForkVerifier, ForkCaptureProvider, ForkPreparation,
        ForkRequest, ForkSeedVerifier, ForkSelection, ResourceRevision, StreamHistoryForkVerifier,
    },
    resources::{ArtifactRef, ProviderRef, StreamRef},
    store::StreamAggregate,
};
use acyclic_harness_filesystem::{
    FilesystemContentVerifier, FilesystemForkPreparer, FilesystemForkVerifier, FilesystemHost,
    workspace_ref,
};
use acyclic_stream::{MemoryStream, StreamClient};
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
#[allow(clippy::too_many_lines)]
async fn exact_fork_preparation_reconciles_without_allocating_another_child() -> Result<()> {
    let provider = ProviderRef::new("fork-e2e", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("fork-e2e", "stream", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
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
        "parent-project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )?;
    let project_head = host.create_volume(&project).await?;
    let child_project = VolumeRef::new(
        provider.clone(),
        "child-project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )?;
    let child_private = VolumeRef::new(
        provider.clone(),
        "child-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
    )?;
    let parent_private = VolumeRef::new(
        provider.clone(),
        "parent-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(parent_agent),
    )?;
    host.create_volume(&parent_private).await?;
    let issuer = AuthorityIssuer::new("fork-e2e", [7; 32], parent.clone());
    let scope = issuer.root_for_agent(
        parent_agent,
        "parent",
        Capabilities::new([
            "conversation:bind".to_owned(),
            "conversation:append".to_owned(),
            "fork:publish".to_owned(),
            project.capability(VolumeOperation::Read)?,
            parent_private.capability(VolumeOperation::Read)?,
            parent_private.capability(VolumeOperation::Write)?,
        ]),
    );
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    let resolver = Arc::new(FilesystemContentVerifier::new(
        host.clone(),
        issuer.verifier(),
        scope.clone(),
        64 * 1_024,
    )?);
    let mut aggregate = StreamAggregate::open(
        &stream,
        parent.clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?
    .with_content_verifier(resolver.clone())
    .with_fork_verifier(Arc::new(CompositeForkVerifier::new(vec![
        Arc::new(FilesystemForkVerifier::new(host.clone(), 64 * 1_024)?),
        Arc::new(StreamHistoryForkVerifier::new(stream_provider.clone())?),
    ])?));
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
        })
        .await?;
    let parent_write = ContentGrant::verify(
        &issuer.verifier(),
        &scope,
        &parent_private,
        VolumeOperation::Write,
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
        )
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
        )
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
        )
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
        })
        .await?;
    let preparer = FilesystemForkPreparer::new(
        host.clone(),
        aggregate.reducer().clone(),
        issuer.verifier(),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        resolver.clone(),
    )?;
    let request = ForkRequest {
        operation_id: OperationId::from_bytes([4; 16]),
        parent: parent.clone(),
        parent_revision: aggregate.reducer().revision(),
        child,
        child_agent,
        attached_agents: Vec::new(),
        preparation: ForkPreparation {
            child_project_volume: child_project.clone(),
            child_private_volume: child_private.clone(),
            inherited_through_sequence: 1,
            maximum_inherited_messages: 64,
            maximum_inherited_bytes: 64 * 1_024,
            maximum_inherited_references: 64,
        },
        selections: vec![
            ForkSelection {
                required: true,
                revision: ResourceRevision::History(StreamRef::new(
                    stream_provider.clone(),
                    parent.stream_path()?.into_bytes(),
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
    let report = aggregate.prepare_fork(&preparer, request.clone()).await?;
    assert_eq!(report.inherited_context.len(), 1);
    assert!(
        report
            .reference_grants
            .iter()
            .any(|grant| grant.file == text && grant.reader == child_agent)
    );
    let mut unclaimed = report.clone().into_seed()?;
    unclaimed.operation_id = OperationId::from_bytes([10; 16]);
    assert!(
        FilesystemForkVerifier::new(host.clone(), 64 * 1_024)?
            .verify(&unclaimed)
            .await
            .is_err()
    );
    let mut changed_seed = report.clone().into_seed()?;
    changed_seed
        .attached_agents
        .push(AgentId::from_bytes([11; 16]));
    changed_seed.validate()?;
    assert!(matches!(
        FilesystemForkVerifier::new(host.clone(), 64 * 1_024)?
            .verify(&changed_seed)
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
        "another-child-project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
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
        "another-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
    )?;
    assert!(
        aggregate
            .prepare_fork(&preparer, reused_project)
            .await
            .is_err()
    );
    let objects_provider = ProviderRef::new("fork-e2e", "objects", "2")?;
    let lost_reply = Arc::new(LostCaptureReply {
        provider: objects_provider.clone(),
        capture_calls: AtomicUsize::new(0),
        reconcile_calls: AtomicUsize::new(0),
        observation_ready: AtomicBool::new(false),
    });
    let composed = FilesystemForkPreparer::new(
        host.clone(),
        aggregate.reducer().clone(),
        issuer.verifier(),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        resolver.clone(),
    )?
    .with_capture_provider(lost_reply.clone())?;
    let another_agent = AgentId::from_bytes([12; 16]);
    let mut uncertain = request.clone();
    uncertain.operation_id = OperationId::from_bytes([13; 16]);
    uncertain.child.id = "uncertain-child".into();
    uncertain.child_agent = another_agent;
    uncertain.preparation.child_project_volume = VolumeRef::new(
        provider.clone(),
        "uncertain-project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )?;
    uncertain.preparation.child_private_volume = VolumeRef::new(
        provider.clone(),
        "uncertain-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(another_agent),
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
        recovered.captures.get(2),
        Some(Capture::Captured(_))
    ));
    assert_eq!(recovered.attachment_manifests, vec![manifest.clone()]);
    let recovered_seed = recovered.clone().into_seed()?;
    let readable = recovered_seed.reference_capabilities(attached_reader)?;
    assert!(readable.contains(&text.read_capability()?));
    assert!(readable.contains(&attachment.read_capability()?));
    assert!(readable.contains(&manifest.read_capability()?));
    assert!(!readable.contains(&parent_private.capability(VolumeOperation::Write)?));
    let child_issuer = AuthorityIssuer::new("fork-e2e", [7; 32], uncertain.child.clone());
    let reader_scope = child_issuer.root_for_agent(attached_reader, "attached-reader", readable);
    let attached_resolver =
        FilesystemContentVerifier::new(host.clone(), child_issuer.verifier(), reader_scope, 1_024)?;
    attached_resolver
        .verify_manifest(&manifest, 1, &Limits::default())
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
        issuer.verifier(),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        resolver.clone(),
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
            &uncertain.preparation.child_private_volume.storage_name()?,
        )?)
        .await?
        .generation,
        recovered.child_private_generation,
    );
    assert_eq!(lost_reply.capture_calls.load(Ordering::SeqCst), 1);
    aggregate
        .publish_fork_report(report.clone(), scope.clone())
        .await?;
    let restarted = FilesystemForkPreparer::new(
        host,
        aggregate.reducer().clone(),
        issuer.verifier(),
        scope,
        project,
        stream_provider,
        resolver,
    )?;
    assert_eq!(
        aggregate.reconcile_fork(&restarted, request).await?,
        Some(report)
    );
    Ok(())
}
