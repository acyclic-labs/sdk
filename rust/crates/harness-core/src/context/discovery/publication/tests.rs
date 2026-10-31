use super::*;
use crate::{
    conversation::{VolumeClass, VolumeOperation, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::{ExecutionEvent, ExecutionJournal},
    filesystem::{
        ContextPublicationRequest, FilesystemContentVerifier, FilesystemExecutionJournal,
        FilesystemHost, FilesystemJournalBinding,
    },
    resources::ProviderRef,
};
use acyclic_auth::{contract::*, testing as signed};
use acyclic_fs::{CancellationToken, EmbeddedCapabilities, Fs, WorkBudget};
use acyclic_stream::session::{ResourceFollowRequest, ResourceProvider as _};
use acyclic_stream::{MemoryStream, StreamProviderExt as _};
use futures::FutureExt as _;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

// Every source record below is emitted by the real filesystem producer and
// filtered by ChangeProvider using real session admission and volume policy.
#[tokio::test]
async fn retained_source_stops_at_pending_cut_and_preserves_pins_on_bad_replacement() -> TestResult
{
    let streams = Arc::new(MemoryStream::default());
    let fs = Fs::new(
        acyclic_fs::distributed::StreamAuthorityStore::new(streams.clone()),
        acyclic_fs::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
        EmbeddedCapabilities::MEMORY,
    );
    let provider = ProviderRef::new("source".into(), "filesystem".into(), "1".into(), &mut None)?;
    let host = FilesystemHost::new(fs.clone(), &provider, None)?;
    let volume = VolumeRef::new(
        provider.clone(),
        "context".into(),
        VolumeClass::Project,
        VolumeOwner::Project("context".into()),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let cancel = CancellationToken::new();
    let workspace = fs
        .open_workspace(
            volume.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|error| error.error)?
        .value;
    let agent = crate::AgentId::new();
    let private = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&private).await?;
    let issuer = AuthorityRoot::new(
        "source",
        [41; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "source".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "source",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
            private.capability(VolumeOperation::Read, &mut None)?,
            private.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let reader = FilesystemContentVerifier::new(host.clone(), &verifier, &scope, 32_768, None)?;
    let resource = acyclic_fs::session::resource(workspace.id().volume_id(), "")?;
    let credential = signed::root(|block| {
        for certificate in block
            .certificates
            .iter_mut()
            .flat_map(|chain| chain.keys.iter_mut())
            .take(1)
        {
            certificate.key.grants = GrantSet {
                grants: vec![Grant {
                    kind: GrantKind::Resource(ResourceGrant {
                        resource: ResourcePattern {
                            name: resource.clone(),
                            prefix: true,
                        },
                        actions: vec!["fs/list".into()],
                    }),
                }],
            };
        }
    });
    let authorization =
        acyclic_stream::session::testing::resource_authorization(&credential, &resource)?;
    let changes = StreamPath::new(format!(
        ".acyclic/fs/{}",
        hex::encode(workspace.id().volume_id().into_bytes())
    ))?;
    let cursor = SourceCursor {
        next: streams.tail(changes).await?,
        preceding: None,
    };
    let request = ResourceFollowRequest {
        resource: resource.clone(),
        from: cursor.next,
        preceding: None,
        filter: Default::default(),
        authorization,
    };
    let subscription = host.change_provider(streams.clone())?.follow(request)?;
    let mut source = SkillPublicationSource::new(
        subscription,
        vec![ContextRoot {
            volume: volume.clone(),
            directory: "skills".into(),
        }],
        workspace.id().volume_id(),
        workspace.volume_config(),
        cursor,
    )?;
    let hidden = host
        .change_provider(streams.clone())?
        .follow(ResourceFollowRequest {
            resource: resource.clone(),
            from: cursor.next,
            preceding: None,
            filter: acyclic_stream::FollowFilter::new(acyclic_stream::wire::SessionFilter {
                attributes: vec![acyclic_stream::wire::AttributeMatch {
                    name: "unselected".into(),
                    any_of: vec![b"selected".as_slice().into()],
                }],
                ..Default::default()
            })?,
            authorization: acyclic_stream::session::testing::resource_authorization(
                &credential,
                &resource,
            )?,
        })?;
    let mut hidden = SkillPublicationSource::new(
        hidden,
        vec![ContextRoot {
            volume: volume.clone(),
            directory: "skills".into(),
        }],
        workspace.id().volume_id(),
        workspace.volume_config(),
        cursor,
    )?;
    let write = crate::conversation::ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Write,
        32_768,
        &mut None,
    )?;
    let original = host
        .put_content(
            &volume,
            &write,
            "skills/check/SKILL.md",
            b"---\nname: check\ndescription: Original header\n---\noriginal body\n",
            "text/markdown",
            "SKILL.md",
            32_768,
            &crate::IdempotencyKey::new("original")?,
            None,
            Default::default(),
        )
        .await?;
    let session = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(4_000_000, 4_000_000),
        &cancel,
    )?;
    source
        .prepare(reader.as_ref(), ContextDiscoveryLimits::default(), &session)
        .await?;
    let pending = source.pending()?.ok_or("no pending publication")?;
    assert_eq!(pending.from(), cursor);
    assert_eq!(
        pending.cursor().next,
        pending.member().ok_or("missing member")?.end()
    );
    assert_eq!(
        source.accepted(),
        cursor,
        "preparation advanced the admitted cursor"
    );
    let [SkillPublicationChange::Skill(metadata)] = pending.changes() else {
        panic!("one original skill expected")
    };
    assert_eq!(metadata.description, "Original header");
    assert_eq!(
        hex::encode(metadata.source.generation.as_resource().key()),
        original.version()
    );
    let retained = metadata.clone();
    let ready = pending.cursor();
    let checkpoint = pending.encode_checkpoint(&session)?;
    hidden
        .prepare(reader.as_ref(), ContextDiscoveryLimits::default(), &session)
        .await?;
    let filtered = hidden.pending()?.ok_or("filtered progress lost")?;
    assert!(
        filtered.member().is_none(),
        "hidden metadata was fabricated"
    );
    assert!(filtered.changes().is_empty());
    assert_eq!(filtered.cursor(), ready);
    assert_eq!(hidden.accepted(), cursor);
    assert_ne!(filtered.encode_checkpoint(&session)?, checkpoint);
    workspace
        .write_text("/skills/check/SKILL.md", "malformed replacement")
        .await?;
    source
        .prepare(reader.as_ref(), ContextDiscoveryLimits::default(), &session)
        .await?;
    assert_eq!(
        source
            .pending()?
            .ok_or("pending cut lost")?
            .encode_checkpoint(&session)?,
        checkpoint
    );
    assert!(
        source
            .confirm_publication(SourceCursor {
                next: ready.next + 1,
                ..ready
            })
            .is_err()
    );
    assert_eq!(source.accepted(), cursor);
    // The actual journal joins this source and epoch with its indexed observation.
    let operation = crate::OperationId::new();
    let journal = FilesystemExecutionJournal::new(
        streams.clone(),
        host.clone(),
        FilesystemJournalBinding {
            volume: &private,
            verifier: &verifier,
            scope: &scope,
            io: None,
        },
    )?
    .with_turn(operation, operation);
    journal
        .append(
            operation,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            Some(&session),
        )?
        .await?;
    let mut binding = journal
        .bind_context_source(&volume, workspace.id().volume_id(), cursor, &session)?
        .await?;
    use crate::conversation::epochs::{Epoch, Snapshot, SummaryKey};
    let epoch = Snapshot::genesis(Epoch {
        header: [3; 32],
        profile: [7; 32],
        summary: None,
        key: SummaryKey {
            source: StreamPath::new("conversation")?,
            through: 0,
            renderer: 1,
            model: [5; 32],
        },
    })?;
    assert!(
        journal
            .publish_context(
                &mut binding,
                &mut source,
                ContextPublicationRequest {
                    operation,
                    key: "wrong-profile",
                    step: 0,
                    expected_tail: 1,
                    profile: [8; 32]
                },
                &epoch,
                &session,
            )?
            .await
            .is_err()
    );
    assert!(
        journal
            .publish_context(
                &mut binding,
                &mut source,
                ContextPublicationRequest {
                    operation,
                    key: "conditional-loser",
                    step: 0,
                    expected_tail: 0,
                    profile: [7; 32]
                },
                &epoch,
                &session,
            )?
            .await?
            .is_none()
    );
    assert_eq!(source.accepted(), cursor);
    let receipt = journal
        .publish_context(
            &mut binding,
            &mut source,
            ContextPublicationRequest {
                operation,
                key: "context",
                step: 0,
                expected_tail: 1,
                profile: [7; 32],
            },
            &epoch,
            &session,
        )?
        .await?
        .ok_or("context publication lost")?;
    assert!(matches!(
        receipt.changes()?,
        [SkillPublicationChange::Skill(_)]
    ));
    // Losing the caller's acknowledgement cannot fabricate ephemeral visibility.
    drop(receipt);
    assert_eq!(source.accepted(), cursor);
    let recovered = journal
        .bind_context_source(&volume, workspace.id().volume_id(), cursor, &session)?
        .await?;
    assert_eq!(recovered.cursor(), ready);
    drop(recovered);
    let receipt = journal
        .publish_context(
            &mut binding,
            &mut source,
            ContextPublicationRequest {
                operation,
                key: "context",
                step: 0,
                expected_tail: 1,
                profile: [7; 32],
            },
            &epoch,
            &session,
        )?
        .await?
        .ok_or("original publication retry lost")?;
    receipt.confirm()?;
    let observations = journal.replay(operation, 0, 3, Some(&session))?.await?;
    assert_eq!(observations.len(), 2, "retry appended another observation");
    let ExecutionEvent::ConversationItem { envelope, .. } = &observations[1].event else {
        panic!("context observation missing");
    };
    assert!(
        journal
            .append(
                operation,
                "missing-source-cut",
                &ExecutionEvent::ConversationItem {
                    step: 0,
                    envelope: envelope.clone(),
                    publication: None,
                },
                Some(&session)
            )?
            .await
            .is_err()
    );
    let bytes = journal.load(envelope, Some(&session))?.await?;
    let (restored, hold) = crate::conversation::envelope::Envelope::decode(&bytes, &session)?;
    let crate::conversation::envelope::Payload::ContextBatch { changes, .. } = restored.payload()
    else {
        panic!("context batch missing");
    };
    assert_eq!(changes.len(), 1);
    drop(restored);
    drop(hold);
    assert!(
        source
            .prepare(reader.as_ref(), ContextDiscoveryLimits::default(), &session)
            .await
            .is_err()
    );
    assert_eq!(source.accepted(), ready);
    assert!(source.pending().is_err());
    let (_, body) = retained.source.read(reader.as_ref(), 32_768).await?;
    assert!(std::str::from_utf8(&body)?.contains("original body"));

    // A dropped original wait producer must complete even when data is quiet.
    // The live-producer negative first proves that the wait does not fabricate
    // readiness. These are real oneshot events, without a timeout or retry.
    let (producer, receiver) = tokio::sync::oneshot::channel::<StreamError>();
    let mut terminal: acyclic_stream::BoxProviderFuture<'static, StreamError> =
        Box::pin(async { receiver.await.unwrap_or(StreamError::Unavailable) });
    let waiting = gated(&mut terminal, &session, std::future::pending::<()>());
    assert!(waiting.now_or_never().is_none());
    drop(producer);
    assert!(
        gated(&mut terminal, &session, std::future::pending::<()>())
            .await
            .is_err()
    );
    Ok(())
}
