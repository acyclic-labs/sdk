//! Durable-local conversation, attachment, fork, and parent merge across restart.
#![cfg(feature = "local")]
#![allow(clippy::too_many_lines)]

use acyclic_fs::{Fs, LocalOptions};
use acyclic_harness::conversation::{
    Attachment, ContentGrant, ConversationMessage, MessageKind, ReferencedAttachments, VolumeClass,
    VolumeOperation, VolumeOwner, VolumeRef,
};
use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry,
};
use acyclic_harness::fork::{
    CompositeForkVerifier, ForkPreparation, ForkRequest, ForkSelection, ResourceRevision,
    StreamHistoryForkVerifier,
};
use acyclic_harness::merge::{ProjectJoinOutcome, ProjectWorkspaceProvider};
use acyclic_harness::resources::{ProviderRef, StreamRef};
use acyclic_harness::store::StreamAggregate;
use acyclic_harness::{AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result};
use acyclic_harness_filesystem::{
    FilesystemContentVerifier, FilesystemForkPreparer, FilesystemForkVerifier, FilesystemHost,
    FilesystemProjectMergeVerifier, FilesystemProjectWorkspaces, WorkspaceMutation, workspace_ref,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn local_reopen_preserves_ref_only_history_fork_and_parent_merge() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new("local-fork-e2e", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("local-fork-e2e", "stream", "2")?;
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
    let child_project = VolumeRef::new(
        provider.clone(),
        "child-project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )?;
    let private = VolumeRef::new(
        provider.clone(),
        "parent-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(parent_agent),
    )?;
    let child_private = VolumeRef::new(
        provider.clone(),
        "child-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
    )?;
    let issuer = AuthorityIssuer::new("local-fork-e2e", [7; 32], parent.clone());
    let scope = issuer.root_for_agent(
        parent_agent,
        "parent",
        Capabilities::new([
            "conversation:bind".to_owned(),
            "conversation:append".to_owned(),
            "fork:publish".to_owned(),
            "project:merge".to_owned(),
            project.capability(VolumeOperation::Read)?,
            project.capability(VolumeOperation::Write)?,
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
        ]),
    );
    let (message_file, attachment_file, inherited_file, merged_generation, attachment_capability) = {
        let host = Arc::new(FilesystemHost::new(
            Fs::local(fs_options.clone())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
            provider.clone(),
        )?);
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(&stream_root, LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let project_head = host.create_volume(&project).await?;
        host.create_volume(&private).await?;
        let reader = Arc::new(FilesystemContentVerifier::new(
            host.clone(),
            issuer.verifier(),
            scope.clone(),
            64 * 1_024,
        )?);
        let forks = Arc::new(CompositeForkVerifier::new(vec![
            Arc::new(FilesystemForkVerifier::new(host.clone(), 64 * 1_024)?),
            Arc::new(StreamHistoryForkVerifier::new(stream_provider.clone())?),
        ])?);
        let mut aggregate = StreamAggregate::open(
            &stream,
            parent.clone(),
            issuer.verifier(),
            SchemaRegistry::new(),
        )
        .await?
        .with_content_verifier(reader.clone())
        .with_fork_verifier(forks)
        .with_merge_verifier(Arc::new(FilesystemProjectMergeVerifier::new(host.clone())));
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([3; 16]),
                idempotency_key: IdempotencyKey::new("bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation {
                    agent: parent_agent,
                },
            })
            .await?;
        let writer =
            ContentGrant::verify(&issuer.verifier(), &scope, &private, VolumeOperation::Write)?;
        let message_file = host
            .put_content(
                &private,
                &writer,
                "messages/request.txt",
                b"parent request",
                "text/plain",
                "request.txt",
                1_024,
                &IdempotencyKey::new("message")?,
            )
            .await?;
        let attachment_file = host
            .put_content(
                &private,
                &writer,
                "attachments/image.png",
                &[137, 80, 78, 71],
                "image/png",
                "image.png",
                1_024,
                &IdempotencyKey::new("attachment")?,
            )
            .await?;
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([4; 16]),
                idempotency_key: IdempotencyKey::new("append")?,
                expected_revision: 1,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes([5; 16]),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: message_file.clone(),
                        attachments: vec![Attachment {
                            file: attachment_file.clone(),
                            label: None,
                        }]
                        .into(),
                        reply_to: None,
                        tool_call_id: None,
                        extensions: Default::default(),
                    }),
                },
            })
            .await?;
        let workspaces = FilesystemProjectWorkspaces::new(
            &host,
            aggregate.reducer(),
            &issuer.verifier(),
            &scope,
            project.clone(),
        )?;
        let preparer = FilesystemForkPreparer::new(
            host.clone(),
            aggregate.reducer().clone(),
            issuer.verifier(),
            scope.clone(),
            project.clone(),
            stream_provider.clone(),
            reader,
        )?;
        let request = ForkRequest {
            operation_id: OperationId::from_bytes([6; 16]),
            parent: parent.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: child.clone(),
            child_agent,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_project_volume: child_project.clone(),
                child_private_volume: child_private.clone(),
                inherited_through_sequence: 1,
                maximum_inherited_messages: 8,
                maximum_inherited_bytes: 64 * 1_024,
                maximum_inherited_references: 8,
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
                        generation: project_head.generation.clone(),
                    },
                },
            ],
            boundary: None,
        };
        let report = aggregate.prepare_fork(&preparer, request).await?;
        let inherited_file = report.inherited_context[0].clone();
        let attachment_capability = report
            .reference_grants
            .iter()
            .find(|grant| grant.reader == child_agent && grant.file == attachment_file)
            .ok_or_else(|| Error::Invalid("child attachment grant is missing".into()))?
            .capability()?;
        aggregate.publish_fork_report(report, scope.clone()).await?;
        let child_head = host
            .resolve(&workspace_ref(
                provider.clone(),
                &child_project.storage_name()?,
            )?)
            .await?;
        host.apply(
            &child_head.workspace,
            Some(&child_head.generation),
            &[WorkspaceMutation::PutFile {
                path: "/child.txt".into(),
                bytes: b"child project".to_vec(),
            }],
            &IdempotencyKey::new("child-project-write")?,
        )
        .await?;
        let notice_file = host
            .put_content(
                &private,
                &writer,
                "messages/merge.txt",
                b"merged child project",
                "text/plain",
                "merge.txt",
                1_024,
                &IdempotencyKey::new("merge-notice")?,
            )
            .await?;
        let notice = ConversationMessage {
            id: Uuid::from_bytes([8; 16]),
            sequence: 2,
            kind: MessageKind::Merge,
            content: notice_file,
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: Default::default(),
        };
        let plan = workspaces
            .prepare_project_merge(&scope, aggregate.reducer(), &child, &child_project)
            .await?;
        let outcome = plan
            .apply(
                &scope,
                OperationId::from_bytes([9; 16]),
                &child,
                &notice,
                &[],
            )
            .await?;
        let receipt = match outcome {
            ProjectJoinOutcome::Applied(receipt) | ProjectJoinOutcome::AlreadyApplied(receipt) => {
                receipt
            }
            _ => return Err(Error::Conflict("local child project was not merged".into())),
        };
        let merged_generation = receipt.result_generation.clone();
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([9; 16]),
                idempotency_key: IdempotencyKey::new("publish-merge")?,
                expected_revision: 3,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::PublishProjectMerge {
                    receipt: Box::new(receipt),
                },
            })
            .await?;
        (
            message_file,
            attachment_file,
            inherited_file,
            merged_generation,
            attachment_capability,
        )
    };
    let host = Arc::new(FilesystemHost::new(
        Fs::local(fs_options)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(&stream_root, LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    let restored = StreamAggregate::open(
        &stream,
        parent.clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?;
    assert_eq!(restored.reducer().revision(), 4);
    assert!(restored.reducer().fork(&child).is_some());
    let conversation = restored
        .reducer()
        .conversation()
        .ok_or_else(|| Error::Invalid("missing conversation".into()))?;
    assert_eq!(conversation.messages.len(), 2);
    assert_eq!(conversation.messages[0].content, message_file);
    assert!(matches!(&conversation.messages[0].attachments,
        ReferencedAttachments::Inline { items } if items.len() == 1 && items[0].file == attachment_file));
    let read = ContentGrant::verify(&issuer.verifier(), &scope, &private, VolumeOperation::Read)?;
    assert_eq!(
        host.read_content(&attachment_file, &read, 1_024)
            .await?
            .as_ref(),
        &[137, 80, 78, 71]
    );
    let child_issuer = AuthorityIssuer::new("local-fork-e2e", [7; 32], child);
    let child_scope = child_issuer.root_for_agent(
        child_agent,
        "child",
        Capabilities::new([child_private.capability(VolumeOperation::Read)?]),
    );
    let child_read = ContentGrant::verify(
        &child_issuer.verifier(),
        &child_scope,
        &child_private,
        VolumeOperation::Read,
    )?;
    assert!(
        !host
            .read_content(&inherited_file, &child_read, 64 * 1_024)
            .await?
            .is_empty()
    );
    assert!(
        ContentGrant::verify_read(&child_issuer.verifier(), &child_scope, &attachment_file)
            .is_err()
    );
    assert!(
        host.read_content(&attachment_file, &child_read, 1_024)
            .await
            .is_err()
    );
    assert!(
        ContentGrant::verify_read(&child_issuer.verifier(), &child_scope, &message_file).is_err()
    );
    let child_scope_with_reference = child_issuer.root_for_agent(
        child_agent,
        "child-reference-reader",
        Capabilities::new([
            child_private.capability(VolumeOperation::Read)?,
            attachment_capability.clone(),
        ]),
    );
    let inherited_reference_read = ContentGrant::verify_read(
        &child_issuer.verifier(),
        &child_scope_with_reference,
        &attachment_file,
    )?;
    assert_eq!(
        host.read_content(&attachment_file, &inherited_reference_read, 1_024)
            .await?
            .as_ref(),
        &[137, 80, 78, 71]
    );
    assert!(
        ContentGrant::verify_read(
            &child_issuer.verifier(),
            &child_scope_with_reference,
            &message_file
        )
        .is_err()
    );
    let delegated = issuer.delegate_private_file_read(
        &scope,
        child_agent,
        "fork-attachment-reader",
        &attachment_file,
    )?;
    assert!(delegated.capabilities().contains(&attachment_capability));
    let exact_read = ContentGrant::verify_read(&issuer.verifier(), &delegated, &attachment_file)?;
    assert_eq!(
        host.read_content(&attachment_file, &exact_read, 1_024)
            .await?
            .as_ref(),
        &[137, 80, 78, 71]
    );
    assert!(ContentGrant::verify_read(&issuer.verifier(), &delegated, &message_file).is_err());
    assert_eq!(
        host.resolve(&workspace_ref(provider, &project.storage_name()?)?)
            .await?
            .generation,
        merged_generation
    );
    assert!(
        ContentGrant::verify(
            &issuer.verifier(),
            &scope,
            &child_private,
            VolumeOperation::Write
        )
        .is_err()
    );
    Ok(())
}
