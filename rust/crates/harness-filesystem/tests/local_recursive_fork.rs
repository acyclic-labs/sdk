//! Bounded durable-local recursive forks across Stream and Filesystem.
#![cfg(feature = "local")]
#![allow(clippy::too_many_lines)]

use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs, LocalOptions};
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

const DEPTH: u8 = 3;

fn identity(value: u8) -> [u8; 16] {
    let mut bytes = [0xA5; 16];
    bytes[0] = value;
    bytes
}

fn volume(
    provider: &ProviderRef,
    class: VolumeClass,
    id: String,
    owner: VolumeOwner,
) -> Result<VolumeRef> {
    VolumeRef::new(provider.clone(), id, class, owner)
}

fn scope(
    issuer: &AuthorityIssuer,
    agent: AgentId,
    private: &VolumeRef,
    project: &VolumeRef,
) -> Result<acyclic_harness::core::Scope> {
    Ok(issuer.root_for_agent(
        agent,
        format!("agent-{agent:?}"),
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
    ))
}

fn command(
    id: u8,
    revision: u64,
    scope: &acyclic_harness::core::Scope,
    action: Action,
) -> Result<Command> {
    Ok(Command {
        operation_id: OperationId::from_bytes(identity(id)),
        idempotency_key: IdempotencyKey::new(format!("local-recursive-{id}-{revision}"))?,
        expected_revision: revision,
        scope: scope.clone(),
        causal_parent: None,
        action,
    })
}

async fn open_aggregate<A, O>(
    stream: &StreamClient<LocalStream>,
    authority: Authority,
    issuer: &AuthorityIssuer,
    host: Arc<FilesystemHost<A, O>>,
    scope: acyclic_harness::core::Scope,
    stream_provider: ProviderRef,
) -> Result<StreamAggregate<LocalStream>>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let reader = Arc::new(FilesystemContentVerifier::new(
        host.clone(),
        issuer.verifier(),
        scope,
        64 * 1_024,
    )?);
    let forks = Arc::new(CompositeForkVerifier::new(vec![
        Arc::new(FilesystemForkVerifier::new(host.clone(), 64 * 1_024)?),
        Arc::new(StreamHistoryForkVerifier::new(stream_provider)?),
    ])?);
    Ok(
        StreamAggregate::open(stream, authority, issuer.verifier(), SchemaRegistry::new())
            .await?
            .with_content_verifier(reader)
            .with_fork_verifier(forks)
            .with_merge_verifier(Arc::new(FilesystemProjectMergeVerifier::new(host))),
    )
}

#[tokio::test]
async fn local_recursive_parent_forks_reopen_and_merge_project_only() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new("local-recursive-e2e", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("local-recursive-e2e", "stream", "2")?;
    let mut host = Arc::new(FilesystemHost::new(
        Fs::local(fs_options.clone())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let mut stream = StreamClient::new(Arc::new(
        LocalStream::open(&stream_root, LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    ));

    let root_authority = Authority {
        kind: AggregateKind::Conversation,
        id: "local-root".into(),
    };
    let root_agent = AgentId::from_bytes([1; 16]);
    let root_issuer = AuthorityIssuer::new("local-recursive-e2e", [7; 32], root_authority.clone());
    let root_private = volume(
        &provider,
        VolumeClass::AgentPrivate,
        "private-0".into(),
        VolumeOwner::Agent(root_agent),
    )?;
    let root_project = volume(
        &provider,
        VolumeClass::Project,
        "project-0".into(),
        VolumeOwner::Project("local-project".into()),
    )?;
    let root_scope = scope(&root_issuer, root_agent, &root_private, &root_project)?;
    let mut project = root_project.clone();
    let private = root_private.clone();
    let mut authority = root_authority.clone();
    let mut issuer = root_issuer.clone();
    let mut grant_scope = root_scope.clone();
    let mut project_head = host.create_volume(&root_project).await?;
    host.create_volume(&root_private).await?;
    let mut aggregate = open_aggregate(
        &stream,
        authority.clone(),
        &issuer,
        host.clone(),
        grant_scope.clone(),
        stream_provider.clone(),
    )
    .await?;
    aggregate
        .execute(command(
            1,
            0,
            &grant_scope,
            Action::BindConversation { agent: root_agent },
        )?)
        .await?;
    let root_write = ContentGrant::verify(
        &issuer.verifier(),
        &grant_scope,
        &private,
        VolumeOperation::Write,
    )?;
    let root_file = host
        .put_content(
            &private,
            &root_write,
            "messages/root.txt",
            b"root message",
            "text/plain",
            "root.txt",
            1_024,
            &IdempotencyKey::new("root-message")?,
        )
        .await?;
    let root_attachment = host
        .put_content(
            &private,
            &root_write,
            "attachments/root.bin",
            b"root attachment",
            "application/octet-stream",
            "root.bin",
            1_024,
            &IdempotencyKey::new("root-attachment")?,
        )
        .await?;
    let mut previous_attachment = root_attachment.clone();
    aggregate
        .execute(command(
            2,
            1,
            &grant_scope,
            Action::AppendConversationMessage {
                message: Box::new(ConversationMessage {
                    id: Uuid::from_bytes([2; 16]),
                    sequence: 1,
                    kind: MessageKind::User,
                    content: root_file.clone(),
                    attachments: vec![Attachment {
                        file: root_attachment.clone(),
                        label: None,
                    }]
                    .into(),
                    reply_to: None,
                    tool_call_id: None,
                    extensions: Default::default(),
                }),
            },
        )?)
        .await?;

    let mut final_child_issuer = None;
    let mut final_child_scope = None;
    let mut final_child_private = None;
    let mut final_child_file = None;
    let mut final_child_attachment = None;
    let mut merge_parent_authority = None;
    let mut merge_parent_issuer = None;
    let mut merge_parent_scope = None;
    let mut merge_parent_project = None;
    let mut merge_child_authority = None;

    for level in 1..=DEPTH {
        let child_agent = AgentId::from_bytes([level + 1; 16]);
        let child_authority = Authority {
            kind: AggregateKind::Conversation,
            id: format!("local-child-{level}"),
        };
        let child_issuer =
            AuthorityIssuer::new("local-recursive-e2e", [7; 32], child_authority.clone());
        let child_private = volume(
            &provider,
            VolumeClass::AgentPrivate,
            format!("private-{level}"),
            VolumeOwner::Agent(child_agent),
        )?;
        let child_project = volume(
            &provider,
            VolumeClass::Project,
            format!("project-{level}"),
            VolumeOwner::Project("local-project".into()),
        )?;
        let child_scope = scope(&child_issuer, child_agent, &child_private, &child_project)?;
        let parent_reader = Arc::new(FilesystemContentVerifier::new(
            host.clone(),
            issuer.verifier(),
            grant_scope.clone(),
            64 * 1_024,
        )?);
        let preparer = FilesystemForkPreparer::new(
            host.clone(),
            aggregate.reducer().clone(),
            issuer.verifier(),
            grant_scope.clone(),
            project.clone(),
            stream_provider.clone(),
            parent_reader,
        )?;
        let parent_workspaces = (level == DEPTH)
            .then(|| {
                FilesystemProjectWorkspaces::new(
                    &host,
                    aggregate.reducer(),
                    &issuer.verifier(),
                    &grant_scope,
                    project.clone(),
                )
            })
            .transpose()?;
        let history = ResourceRevision::History(StreamRef::new(
            stream_provider.clone(),
            authority.stream_path()?.into_bytes(),
            Some(aggregate.reducer().revision().to_string()),
        )?);
        let request = ForkRequest {
            operation_id: OperationId::from_bytes(identity(20 + level)),
            parent: authority.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: child_authority.clone(),
            child_agent,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_project_volume: child_project.clone(),
                child_private_volume: child_private.clone(),
                inherited_through_sequence: 1,
                maximum_inherited_messages: 4,
                maximum_inherited_bytes: 16 * 1_024,
                maximum_inherited_references: 8,
            },
            selections: vec![
                ForkSelection {
                    required: true,
                    revision: history.clone(),
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
        let child_attachment_capability = report
            .reference_grants
            .iter()
            .find(|grant| grant.reader == child_agent && grant.file == previous_attachment)
            .map(|grant| grant.capability())
            .transpose()?
            .ok_or_else(|| Error::Invalid("recursive child attachment grant is missing".into()))?;
        let inherited_file = report
            .inherited_context
            .first()
            .cloned()
            .ok_or_else(|| Error::Invalid("recursive inherited context is missing".into()))?;
        let mut child_aggregate = open_aggregate(
            &stream,
            child_authority.clone(),
            &child_issuer,
            host.clone(),
            child_scope.clone(),
            stream_provider.clone(),
        )
        .await?;
        let seed = child_aggregate
            .spawn_from_report(
                &mut aggregate,
                report,
                grant_scope.clone(),
                child_scope.clone(),
            )
            .await?;
        assert_eq!(seed.child, child_authority);
        if level == 1 {
            // Close the live providers, reopen their on-disk roots, and then
            // recover both durable Stream aggregates before the child appends.
            drop(preparer);
            drop(aggregate);
            drop(child_aggregate);
            drop(stream);
            drop(host);
            host = Arc::new(FilesystemHost::new(
                Fs::local(fs_options.clone())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
                provider.clone(),
            )?);
            stream = StreamClient::new(Arc::new(
                LocalStream::open(&stream_root, LocalStreamLimits::default())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
            ));
            aggregate = open_aggregate(
                &stream,
                authority.clone(),
                &issuer,
                host.clone(),
                grant_scope.clone(),
                stream_provider.clone(),
            )
            .await?;
            child_aggregate = open_aggregate(
                &stream,
                child_authority.clone(),
                &child_issuer,
                host.clone(),
                child_scope.clone(),
                stream_provider.clone(),
            )
            .await?;
            assert!(aggregate.reducer().fork(&child_authority).is_some());
            assert_eq!(child_aggregate.reducer().revision(), 1);
        }
        let child_write = ContentGrant::verify(
            &child_issuer.verifier(),
            &child_scope,
            &child_private,
            VolumeOperation::Write,
        )?;
        let child_file = host
            .put_content(
                &child_private,
                &child_write,
                &format!("messages/child-{level}.txt"),
                format!("child message {level}").as_bytes(),
                "text/plain",
                &format!("child-{level}.txt"),
                1_024,
                &IdempotencyKey::new(format!("child-message-{level}"))?,
            )
            .await?;
        let child_attachment = host
            .put_content(
                &child_private,
                &child_write,
                &format!("attachments/child-{level}.bin"),
                format!("child attachment {level}").as_bytes(),
                "application/octet-stream",
                &format!("child-{level}.bin"),
                1_024,
                &IdempotencyKey::new(format!("child-attachment-{level}"))?,
            )
            .await?;
        let child_read = ContentGrant::verify(
            &child_issuer.verifier(),
            &child_scope,
            &child_private,
            VolumeOperation::Read,
        )?;
        assert!(
            !host
                .read_content(&inherited_file, &child_read, 16 * 1_024)
                .await?
                .is_empty()
        );
        assert!(
            host.read_content(&root_file, &child_read, 1_024)
                .await
                .is_err()
        );
        assert_eq!(
            host.read_content(&child_attachment, &child_read, 1_024)
                .await?
                .as_ref(),
            format!("child attachment {level}").as_bytes(),
        );
        child_aggregate
            .execute(command(
                40 + level,
                1,
                &child_scope,
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes([40 + level; 16]),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: child_file.clone(),
                        attachments: vec![Attachment {
                            file: child_attachment.clone(),
                            label: None,
                        }]
                        .into(),
                        reply_to: None,
                        tool_call_id: None,
                        extensions: Default::default(),
                    }),
                },
            )?)
            .await?;
        let child_observation = host
            .resolve(&workspace_ref(
                provider.clone(),
                &child_project.storage_name()?,
            )?)
            .await?;
        let child_generation = host
            .apply(
                &child_observation.workspace,
                Some(&child_observation.generation),
                &[WorkspaceMutation::PutFile {
                    path: format!("/level-{level}.txt"),
                    bytes: format!("project child {level}").into_bytes(),
                }],
                &IdempotencyKey::new(format!("project-write-{level}"))?,
            )
            .await?;

        if level == DEPTH {
            merge_parent_authority = Some(authority.clone());
            merge_parent_issuer = Some(issuer.clone());
            merge_parent_scope = Some(grant_scope.clone());
            merge_parent_project = Some(project.clone());
            merge_child_authority = Some(child_authority.clone());
            let parent_workspaces =
                parent_workspaces.ok_or_else(|| Error::Invalid("missing merge binding".into()))?;
            let notice = host
                .put_content(
                    &project,
                    &ContentGrant::verify(
                        &issuer.verifier(),
                        &grant_scope,
                        &project,
                        VolumeOperation::Write,
                    )?,
                    "notices/final-merge.txt",
                    b"final project-only merge",
                    "text/plain",
                    "final-merge.txt",
                    1_024,
                    &IdempotencyKey::new("final-merge-notice")?,
                )
                .await?;
            let merge_message = ConversationMessage {
                id: Uuid::from_bytes([90; 16]),
                sequence: 2,
                kind: MessageKind::Merge,
                content: notice,
                attachments: ReferencedAttachments::Inline { items: Vec::new() },
                reply_to: None,
                tool_call_id: None,
                extensions: Default::default(),
            };
            let plan = parent_workspaces
                .prepare_project_merge(
                    &grant_scope,
                    aggregate.reducer(),
                    &child_authority,
                    &child_project,
                )
                .await?;
            let (ProjectJoinOutcome::Applied(receipt)
            | ProjectJoinOutcome::AlreadyApplied(receipt)) = plan
                .apply(
                    &grant_scope,
                    OperationId::from_bytes([91; 16]),
                    &child_authority,
                    &merge_message,
                    &[],
                )
                .await?
            else {
                return Err(Error::Conflict(
                    "final local project merge was not applied".into(),
                ));
            };
            aggregate
                .execute(Command {
                    operation_id: OperationId::from_bytes([91; 16]),
                    idempotency_key: IdempotencyKey::new("publish-final-merge")?,
                    expected_revision: aggregate.reducer().revision(),
                    scope: grant_scope.clone(),
                    causal_parent: None,
                    action: Action::PublishProjectMerge {
                        receipt: Box::new(receipt),
                    },
                })
                .await?;
            assert_eq!(
                host.read(
                    &project_head.workspace,
                    None,
                    &format!("/level-{level}.txt"),
                    1_024
                )
                .await?
                .as_ref(),
                format!("project child {level}").as_bytes(),
            );
        }

        final_child_issuer = Some(child_issuer.clone());
        final_child_scope = Some(child_scope.clone());
        final_child_private = Some(child_private.clone());
        final_child_file = Some(child_file);
        previous_attachment = child_attachment.clone();
        final_child_attachment = Some(child_attachment);
        project = child_project;
        authority = child_authority;
        issuer = child_issuer.clone();
        grant_scope = child_scope;
        project_head = acyclic_harness_filesystem::WorkspaceObservation {
            workspace: child_observation.workspace,
            generation: child_generation,
        };
        aggregate = child_aggregate;

        // Preserve the exact attachment grant at the first boundary for the
        // owner-controlled delegation assertion below.
        if level == 1 {
            let reference_issuer = child_issuer.clone();
            let child_scope_with_reference = reference_issuer.root_for_agent(
                child_agent,
                "child-root-reference-reader",
                Capabilities::new([
                    child_private.capability(VolumeOperation::Read)?,
                    child_attachment_capability,
                ]),
            );
            let exact = ContentGrant::verify_read(
                &reference_issuer.verifier(),
                &child_scope_with_reference,
                &root_attachment,
            )?;
            assert_eq!(
                host.read_content(&root_attachment, &exact, 1_024)
                    .await?
                    .as_ref(),
                b"root attachment"
            );
            assert!(
                ContentGrant::verify_read(
                    &reference_issuer.verifier(),
                    &child_scope_with_reference,
                    &root_file,
                )
                .is_err()
            );
        }
    }

    let final_agent = final_child_scope
        .as_ref()
        .and_then(|scope| scope.agent())
        .ok_or_else(|| Error::Invalid("missing final child agent".into()))?;
    let final_issuer =
        final_child_issuer.ok_or_else(|| Error::Invalid("missing final child issuer".into()))?;
    let final_scope =
        final_child_scope.ok_or_else(|| Error::Invalid("missing final child scope".into()))?;
    let final_private = final_child_private
        .ok_or_else(|| Error::Invalid("missing final child private volume".into()))?;
    let final_file =
        final_child_file.ok_or_else(|| Error::Invalid("missing final child file".into()))?;
    let final_attachment = final_child_attachment
        .ok_or_else(|| Error::Invalid("missing final child attachment".into()))?;
    let final_read = ContentGrant::verify(
        &final_issuer.verifier(),
        &final_scope,
        &final_private,
        VolumeOperation::Read,
    )?;
    assert_eq!(
        host.read_content(&final_file, &final_read, 1_024)
            .await?
            .as_ref(),
        b"child message 3",
    );
    assert!(
        host.read_content(&root_file, &final_read, 1_024)
            .await
            .is_err()
    );
    assert_eq!(
        host.read_content(&final_attachment, &final_read, 1_024)
            .await?
            .as_ref(),
        b"child attachment 3",
    );
    let delegated = root_issuer.delegate_private_file_read(
        &root_scope,
        final_agent,
        "final-exact-root-attachment",
        &root_attachment,
    )?;
    let exact_read =
        ContentGrant::verify_read(&root_issuer.verifier(), &delegated, &root_attachment)?;
    assert_eq!(
        host.read_content(&root_attachment, &exact_read, 1_024)
            .await?
            .as_ref(),
        b"root attachment"
    );
    assert!(ContentGrant::verify_read(&root_issuer.verifier(), &delegated, &root_file).is_err());

    let merge_parent_authority = merge_parent_authority
        .ok_or_else(|| Error::Invalid("missing merge parent authority".into()))?;
    let merge_parent_issuer =
        merge_parent_issuer.ok_or_else(|| Error::Invalid("missing merge parent issuer".into()))?;
    let merge_parent_scope =
        merge_parent_scope.ok_or_else(|| Error::Invalid("missing merge parent scope".into()))?;
    let merge_parent_project = merge_parent_project
        .ok_or_else(|| Error::Invalid("missing merge parent project".into()))?;
    let merge_child_authority = merge_child_authority
        .ok_or_else(|| Error::Invalid("missing merge child authority".into()))?;
    let reopened_parent = open_aggregate(
        &stream,
        merge_parent_authority,
        &merge_parent_issuer,
        host.clone(),
        merge_parent_scope,
        stream_provider,
    )
    .await?;
    assert_eq!(reopened_parent.reducer().revision(), 4);
    assert!(
        reopened_parent
            .reducer()
            .fork(&merge_child_authority)
            .is_some()
    );
    let merged_project = host
        .resolve(&workspace_ref(
            provider,
            &merge_parent_project.storage_name()?,
        )?)
        .await?;
    assert_eq!(
        host.read(&merged_project.workspace, None, "/level-3.txt", 1_024)
            .await?
            .as_ref(),
        b"project child 3",
    );
    assert!(
        host.read(
            &merged_project.workspace,
            None,
            "/messages/child-3.txt",
            1_024
        )
        .await
        .is_err(),
        "private child conversation files stay outside the project merge"
    );
    Ok(())
}

/// Keep this chain deliberately small enough for regular CI while exercising
/// the same-path allocation and reference rules far beyond a shallow fork.
#[tokio::test]
async fn local_deep_same_path_recursive_forks_keep_parent_controls() -> Result<()> {
    const DEPTH: u8 = 64;
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new("local-deep-e2e", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("local-deep-e2e", "stream", "2")?;
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

    let mut authority = Authority {
        kind: AggregateKind::Conversation,
        id: "deep-root".into(),
    };
    let mut agent = AgentId::from_bytes([0; 16]);
    let mut issuer = AuthorityIssuer::new("local-deep-e2e", [9; 32], authority.clone());
    let mut private = volume(
        &provider,
        VolumeClass::AgentPrivate,
        "scratch".into(),
        VolumeOwner::Agent(agent),
    )?;
    let mut project = volume(
        &provider,
        VolumeClass::Project,
        "deep-project-0".into(),
        VolumeOwner::Project("deep-project".into()),
    )?;
    let mut grant_scope = scope(&issuer, agent, &private, &project)?;
    let mut project_head = host.create_volume(&project).await?;
    host.create_volume(&private).await?;
    let mut aggregate = open_aggregate(
        &stream,
        authority.clone(),
        &issuer,
        host.clone(),
        grant_scope.clone(),
        stream_provider.clone(),
    )
    .await?;
    aggregate
        .execute(command(
            1,
            0,
            &grant_scope,
            Action::BindConversation { agent },
        )?)
        .await?;
    let root_write = ContentGrant::verify(
        &issuer.verifier(),
        &grant_scope,
        &private,
        VolumeOperation::Write,
    )?;
    let mut previous_file = host
        .put_content(
            &private,
            &root_write,
            "messages/current.txt",
            b"deep level 0",
            "text/plain",
            "current.txt",
            1_024,
            &IdempotencyKey::new("deep-message-0")?,
        )
        .await?;
    let mut previous_attachment = host
        .put_content(
            &private,
            &root_write,
            "attachments/evidence.bin",
            b"deep attachment 0",
            "application/octet-stream",
            "evidence.bin",
            1_024,
            &IdempotencyKey::new("deep-attachment-0")?,
        )
        .await?;
    aggregate
        .execute(command(
            2,
            1,
            &grant_scope,
            Action::AppendConversationMessage {
                message: Box::new(ConversationMessage {
                    id: Uuid::from_bytes([2; 16]),
                    sequence: 1,
                    kind: MessageKind::User,
                    content: previous_file.clone(),
                    attachments: vec![Attachment {
                        file: previous_attachment.clone(),
                        label: None,
                    }]
                    .into(),
                    reply_to: None,
                    tool_call_id: None,
                    extensions: Default::default(),
                }),
            },
        )?)
        .await?;

    for level in 1..=DEPTH {
        let child_agent = AgentId::from_bytes([level; 16]);
        let child_authority = Authority {
            kind: AggregateKind::Conversation,
            id: format!("deep-child-{level}"),
        };
        let child_issuer = AuthorityIssuer::new("local-deep-e2e", [9; 32], child_authority.clone());
        // Deliberately reuse both storage paths. The owner identity must keep
        // each private volume distinct despite identical logical names.
        let child_private = volume(
            &provider,
            VolumeClass::AgentPrivate,
            "scratch".into(),
            VolumeOwner::Agent(child_agent),
        )?;
        assert_ne!(private.storage_name()?, child_private.storage_name()?);
        let child_project = volume(
            &provider,
            VolumeClass::Project,
            format!("deep-project-{level}"),
            VolumeOwner::Project("deep-project".into()),
        )?;
        let child_scope = scope(&child_issuer, child_agent, &child_private, &child_project)?;
        let reader = Arc::new(FilesystemContentVerifier::new(
            host.clone(),
            issuer.verifier(),
            grant_scope.clone(),
            8 * 1_024,
        )?);
        let preparer = FilesystemForkPreparer::new(
            host.clone(),
            aggregate.reducer().clone(),
            issuer.verifier(),
            grant_scope.clone(),
            project.clone(),
            stream_provider.clone(),
            reader,
        )?;
        let request = ForkRequest {
            operation_id: OperationId::from_bytes(identity(20 + level)),
            parent: authority.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: child_authority.clone(),
            child_agent,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_project_volume: child_project.clone(),
                child_private_volume: child_private.clone(),
                inherited_through_sequence: 1,
                maximum_inherited_messages: 2,
                maximum_inherited_bytes: 8 * 1_024,
                maximum_inherited_references: 4,
            },
            selections: vec![
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::History(StreamRef::new(
                        stream_provider.clone(),
                        authority.stream_path()?.into_bytes(),
                        Some(aggregate.reducer().revision().to_string()),
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
        let reference_capability = report
            .reference_grants
            .iter()
            .find(|grant| grant.reader == child_agent && grant.file == previous_attachment)
            .map(|grant| grant.capability())
            .transpose()?
            .ok_or_else(|| Error::Invalid("deep child attachment grant is missing".into()))?;
        let inherited_file = report
            .inherited_context
            .first()
            .cloned()
            .ok_or_else(|| Error::Invalid("deep inherited context is missing".into()))?;
        let mut child_aggregate = open_aggregate(
            &stream,
            child_authority.clone(),
            &child_issuer,
            host.clone(),
            child_scope.clone(),
            stream_provider.clone(),
        )
        .await?;
        child_aggregate
            .spawn_from_report(
                &mut aggregate,
                report,
                grant_scope.clone(),
                child_scope.clone(),
            )
            .await?;

        let delegated = issuer.delegate_private_file_read(
            &grant_scope,
            child_agent,
            format!("deep-parent-attachment-{level}"),
            &previous_attachment,
        )?;
        let exact_parent_read =
            ContentGrant::verify_read(&issuer.verifier(), &delegated, &previous_attachment)?;
        assert_eq!(
            host.read_content(&previous_attachment, &exact_parent_read, 1_024)
                .await?
                .as_ref(),
            format!("deep attachment {}", level - 1).as_bytes(),
        );
        assert!(ContentGrant::verify_read(&issuer.verifier(), &delegated, &previous_file).is_err());

        let child_read = ContentGrant::verify(
            &child_issuer.verifier(),
            &child_scope,
            &child_private,
            VolumeOperation::Read,
        )?;
        assert!(
            !host
                .read_content(&inherited_file, &child_read, 8 * 1_024)
                .await?
                .is_empty()
        );
        assert!(
            host.read_content(&previous_file, &child_read, 1_024)
                .await
                .is_err()
        );
        assert!(
            host.read_content(&previous_attachment, &child_read, 1_024)
                .await
                .is_err()
        );
        let reference_scope = child_issuer.root_for_agent(
            child_agent,
            format!("deep-reference-reader-{level}"),
            Capabilities::new([
                child_private.capability(VolumeOperation::Read)?,
                reference_capability,
            ]),
        );
        let child_reference_read = ContentGrant::verify_read(
            &child_issuer.verifier(),
            &reference_scope,
            &previous_attachment,
        )?;
        assert_eq!(
            host.read_content(&previous_attachment, &child_reference_read, 1_024)
                .await?
                .as_ref(),
            format!("deep attachment {}", level - 1).as_bytes(),
        );
        assert!(
            ContentGrant::verify_read(&child_issuer.verifier(), &reference_scope, &previous_file)
                .is_err()
        );

        let child_write = ContentGrant::verify(
            &child_issuer.verifier(),
            &child_scope,
            &child_private,
            VolumeOperation::Write,
        )?;
        let child_file = host
            .put_content(
                &child_private,
                &child_write,
                "messages/current.txt",
                format!("deep level {level}").as_bytes(),
                "text/plain",
                "current.txt",
                1_024,
                &IdempotencyKey::new(format!("deep-message-{level}"))?,
            )
            .await?;
        let child_attachment = host
            .put_content(
                &child_private,
                &child_write,
                "attachments/evidence.bin",
                format!("deep attachment {level}").as_bytes(),
                "application/octet-stream",
                "evidence.bin",
                1_024,
                &IdempotencyKey::new(format!("deep-attachment-{level}"))?,
            )
            .await?;
        child_aggregate
            .execute(command(
                100 + level,
                1,
                &child_scope,
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes([100 + level; 16]),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: child_file.clone(),
                        attachments: vec![Attachment {
                            file: child_attachment.clone(),
                            label: None,
                        }]
                        .into(),
                        reply_to: None,
                        tool_call_id: None,
                        extensions: Default::default(),
                    }),
                },
            )?)
            .await?;
        project_head = host
            .resolve(&workspace_ref(
                provider.clone(),
                &child_project.storage_name()?,
            )?)
            .await?;
        previous_file = child_file;
        previous_attachment = child_attachment;
        private = child_private;
        project = child_project;
        agent = child_agent;
        authority = child_authority;
        issuer = child_issuer;
        grant_scope = child_scope;
        aggregate = child_aggregate;
    }

    assert_eq!(aggregate.reducer().revision(), 2);
    assert_eq!(agent, AgentId::from_bytes([DEPTH; 16]));
    assert_eq!(project.id(), "deep-project-64");
    Ok(())
}
