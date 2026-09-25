//! End-to-end recursive fork qualification across Stream and Filesystem.
#![allow(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::indexing_slicing
)]

use acyclic_fs::{ConflictSide, Fs, JoinOutcome};
use acyclic_harness::conversation::{
    Attachment, ContentGrant, ContentResidencyVerifier, ConversationMessage, FileDescriptor,
    FileRef, Limits, MessageKind, ReferencedAttachments, VolumeClass, VolumeOperation, VolumeOwner,
    VolumeRef,
};
use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry, Scope,
};
use acyclic_harness::fork::{
    CapturedResource, CompositeForkVerifier, ForkSeed, ResourceRevision, SharedGrant,
    StreamHistoryForkVerifier,
};
use acyclic_harness::merge::ProjectMergeVerifier;
use acyclic_harness::model::{FileProjectionPolicy, ModelContent, ModelContentPart};
use acyclic_harness::projection::{ModelContextSelection, select_model_context};
use acyclic_harness::resources::{ProviderRef, StreamRef};
use acyclic_harness::store::StreamAggregate;
use acyclic_harness::{AgentId, Capabilities, IdempotencyKey, OperationId, Result};
use acyclic_harness_filesystem::{
    FilesystemContentVerifier, FilesystemForkVerifier, FilesystemHost,
    FilesystemProjectMergeVerifier, ParentProjectController, WorkspaceMutation,
};
use acyclic_stream::{MemoryStream, StreamClient};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;

const DEPTH: u16 = 1_024;

fn identity(value: u16) -> [u8; 16] {
    let mut bytes = [0xA5; 16];
    bytes[..2].copy_from_slice(&value.to_le_bytes());
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
    private: &VolumeRef,
    shared: &VolumeRef,
    project: &VolumeRef,
) -> Result<Scope> {
    let VolumeOwner::Agent(agent) = private.owner() else {
        unreachable!("private volume has an agent owner")
    };
    Ok(issuer.root_for_agent(
        *agent,
        "agent",
        Capabilities::new([
            "conversation:bind".to_owned(),
            "conversation:append".to_owned(),
            "fork:publish".to_owned(),
            "project:merge".to_owned(),
            project.capability(VolumeOperation::Read)?,
            project.capability(VolumeOperation::Write)?,
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
            shared.capability(VolumeOperation::Read)?,
        ]),
    ))
}

fn command(id: u16, revision: u64, scope: &Scope, action: Action) -> Result<Command> {
    Ok(Command {
        operation_id: OperationId::from_bytes(identity(id)),
        idempotency_key: IdempotencyKey::new(format!("recursive-operation-{id}-{revision}"))?,
        expected_revision: revision,
        scope: scope.clone(),
        causal_parent: None,
        action,
    })
}

fn fork_command(id: u16, revision: u64, scope: &Scope, seed: ForkSeed) -> Result<Command> {
    let operation_id = seed.operation_id;
    let mut prepared = command(
        id,
        revision,
        scope,
        Action::PublishFork {
            seed: Box::new(seed),
        },
    )?;
    prepared.operation_id = operation_id;
    Ok(prepared)
}

#[test]
fn thousand_twenty_four_recursive_forks_keep_files_private_and_merge_only_project() -> Result<()> {
    // The broad, instrumented E2E future has a larger stack footprint than a
    // production task. Run it on a dedicated stack without relaxing fork depth.
    std::thread::Builder::new()
        .name("recursive-fork-e2e".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
            runtime.block_on(run_thousand_twenty_four_recursive_forks())
        })
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?
        .join()
        .map_err(|_| acyclic_harness::Error::Invalid("recursive E2E thread panicked".into()))?
}

async fn run_thousand_twenty_four_recursive_forks() -> Result<()> {
    let filesystem = Fs::memory();
    let provider = ProviderRef::new("qualification", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(filesystem, provider.clone())?);
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    let stream_provider = ProviderRef::new("qualification", "stream", "2")?;

    let shared = volume(
        &provider,
        VolumeClass::SessionShared,
        "shared".into(),
        VolumeOwner::Session("session".into()),
    )?;
    host.create_volume(&shared).await?;
    let root_agent = AgentId::from_bytes([0; 16]);
    let mut private = volume(
        &provider,
        VolumeClass::AgentPrivate,
        "scratch".into(),
        VolumeOwner::Agent(root_agent),
    )?;
    host.create_volume(&private).await?;
    let mut project = volume(
        &provider,
        VolumeClass::Project,
        "project-root".into(),
        VolumeOwner::Project("project".into()),
    )?;
    let mut project_head = host.create_volume(&project).await?;

    let mut authority = Authority {
        kind: AggregateKind::Conversation,
        id: "recursive-root".into(),
    };
    let mut issuer = AuthorityIssuer::new("qualification", [17; 32], authority.clone());
    let mut grant_scope = scope(&issuer, &private, &shared, &project)?;
    let mut aggregate = StreamAggregate::open(
        &stream,
        authority.clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?
    .with_limits(Limits::default())?
    .with_content_verifier(Arc::new(FilesystemContentVerifier::new(
        host.clone(),
        issuer.verifier(),
        grant_scope.clone(),
        1_024,
    )?))
    .with_fork_verifier(Arc::new(CompositeForkVerifier::new(vec![
        Arc::new(FilesystemForkVerifier::new(host.clone(), 64 * 1_024)?),
        Arc::new(StreamHistoryForkVerifier::new(stream_provider.clone())?),
    ])?));
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
    let mut previous_file = host
        .put_content(
            &private,
            &root_write,
            "messages/current.txt",
            b"root",
            "text/plain",
            "current.txt",
            1_024,
            &IdempotencyKey::new("root-message")?,
        )
        .await?;
    let root_attachment = host
        .put_content(
            &private,
            &root_write,
            "attachments/evidence.bin",
            b"root evidence",
            "application/octet-stream",
            "evidence.bin",
            1_024,
            &IdempotencyKey::new("root-attachment")?,
        )
        .await?;
    let root_manifest_bytes = serde_json::to_vec(&vec![Attachment {
        file: root_attachment.clone(),
        label: Some("root evidence".into()),
    }])
    .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let root_manifest = host
        .put_content(
            &private,
            &root_write,
            "attachments/manifest.json",
            &root_manifest_bytes,
            "application/vnd.acyclic.harness.attachments+json",
            "manifest.json",
            1_024,
            &IdempotencyKey::new("root-manifest")?,
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
                    id: Uuid::from_bytes([1; 16]),
                    sequence: 1,
                    kind: MessageKind::User,
                    content: previous_file.clone(),
                    attachments: ReferencedAttachments::Manifest {
                        manifest: root_manifest.clone(),
                        item_count: 1,
                    },
                    reply_to: None,
                    tool_call_id: None,
                    extensions: BTreeMap::new(),
                }),
            },
        )?)
        .await?;
    let project_write = ContentGrant::verify(
        &issuer.verifier(),
        &grant_scope,
        &project,
        VolumeOperation::Write,
    )?;
    let project_reference = host
        .put_content(
            &project,
            &project_write,
            "references/project.txt",
            b"project reference",
            "text/plain",
            "project.txt",
            1_024,
            &IdempotencyKey::new("root-project-reference")?,
        )
        .await?;
    project_head = host.resolve(&project_head.workspace).await?;
    let shared_writer = issuer.root(
        "shared-writer",
        Capabilities::new([shared.capability(VolumeOperation::Write)?]),
    );
    let shared_write = ContentGrant::verify(
        &issuer.verifier(),
        &shared_writer,
        &shared,
        VolumeOperation::Write,
    )?;
    let shared_reference = host
        .put_content(
            &shared,
            &shared_write,
            "references/shared.txt",
            b"shared reference",
            "text/plain",
            "shared.txt",
            1_024,
            &IdempotencyKey::new("root-shared-reference")?,
        )
        .await?;
    aggregate
        .execute(command(
            3,
            2,
            &grant_scope,
            Action::AppendConversationMessage {
                message: Box::new(ConversationMessage {
                    id: Uuid::from_bytes([0x11; 16]),
                    sequence: 2,
                    kind: MessageKind::User,
                    content: project_reference.clone(),
                    attachments: vec![Attachment {
                        file: shared_reference.clone(),
                        label: None,
                    }]
                    .into(),
                    reply_to: None,
                    tool_call_id: None,
                    extensions: BTreeMap::new(),
                }),
            },
        )?)
        .await?;
    let mut previous_project_reference = Some(project_reference);
    let mut previous_shared_reference = Some(shared_reference);

    let mut final_parent_project = project.clone();
    let mut final_parent_authority = authority.clone();
    let mut final_parent_issuer = issuer.clone();
    let mut final_parent_scope = grant_scope.clone();
    for level in 1..=DEPTH {
        let child_agent = AgentId::from_bytes(identity(level));
        let child_private = volume(
            &provider,
            VolumeClass::AgentPrivate,
            "scratch".into(),
            VolumeOwner::Agent(child_agent),
        )?;
        host.create_volume(&child_private).await?;
        assert_ne!(private.storage_name()?, child_private.storage_name()?);
        let child_project = volume(
            &provider,
            VolumeClass::Project,
            format!("project-{level}"),
            VolumeOwner::Project("project".into()),
        )?;
        let controller = ParentProjectController::new(
            &host,
            &authority,
            &issuer.verifier(),
            &grant_scope,
            project.clone(),
        )?;
        let forked = controller
            .fork_project(
                &project_head.generation,
                &child_project,
                &IdempotencyKey::new(format!("project-fork-{level}"))?,
            )
            .await?;
        let child_authority = Authority {
            kind: AggregateKind::Conversation,
            id: format!("recursive-{level}"),
        };
        let child_issuer = AuthorityIssuer::new("qualification", [17; 32], child_authority.clone());
        let child_scope = scope(&child_issuer, &child_private, &shared, &child_project)?;
        let shared_read = ContentGrant::verify(
            &child_issuer.verifier(),
            &child_scope,
            &shared,
            VolumeOperation::Read,
        )?;
        assert!(shared_read.require(&shared, VolumeOperation::Read).is_ok());
        assert!(
            ContentGrant::verify(
                &child_issuer.verifier(),
                &child_scope,
                &private,
                VolumeOperation::Read,
            )
            .is_err()
        );
        let child_write = ContentGrant::verify(
            &child_issuer.verifier(),
            &child_scope,
            &child_private,
            VolumeOperation::Write,
        )?;
        let controller = ParentProjectController::new(
            &host,
            &authority,
            &issuer.verifier(),
            &grant_scope,
            project.clone(),
        )?;
        let attached_agents = if level == 1 {
            vec![AgentId::from_bytes([201; 16]), root_agent]
        } else {
            Vec::new()
        };
        let parent_resolver = FilesystemContentVerifier::new(
            host.clone(),
            issuer.verifier(),
            grant_scope.clone(),
            64 * 1_024,
        )?;
        if level == 1 {
            let dirty_private = volume(
                &provider,
                VolumeClass::AgentPrivate,
                "dirty-scratch".into(),
                VolumeOwner::Agent(child_agent),
            )?;
            let dirty = host.create_volume(&dirty_private).await?;
            host.apply(
                &dirty.workspace,
                Some(&dirty.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/scratch.txt".into(),
                    bytes: b"unselected private state".to_vec(),
                }],
                &IdempotencyKey::new("dirty-private-write")?,
            )
            .await?;
            assert!(
                controller
                    .materialize_inherited_conversation(
                        aggregate.reducer(),
                        &dirty_private,
                        child_agent,
                        &[],
                        2,
                        2,
                        64 * 1_024,
                        4_096,
                        &parent_resolver,
                        &IdempotencyKey::new("dirty-private-prefix")?,
                    )
                    .await
                    .is_err(),
                "parent cannot adopt an already-used child private volume"
            );
        }
        let captured = controller
            .materialize_inherited_conversation(
                aggregate.reducer(),
                &child_private,
                child_agent,
                &attached_agents,
                if level == 1 { 2 } else { 1 },
                2,
                64 * 1_024,
                4_096,
                &parent_resolver,
                &IdempotencyKey::new(format!("inherited-{level}"))?,
            )
            .await?;
        if level == 1 {
            assert!(
                controller
                    .materialize_inherited_conversation(
                        aggregate.reducer(),
                        &child_private,
                        child_agent,
                        &[],
                        2,
                        2,
                        64 * 1_024,
                        4_096,
                        &parent_resolver,
                        &IdempotencyKey::new("inherited-1")?,
                    )
                    .await
                    .is_err(),
                "one retry identity cannot change attached readers"
            );
        }
        let inherited = captured.file.clone();
        let seed = ForkSeed {
            operation_id: OperationId::from_bytes(identity(level + 80)),
            parent: authority.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: child_authority.clone(),
            child_agent,
            resources: vec![
                CapturedResource {
                    source: ResourceRevision::History(StreamRef::new(
                        stream_provider.clone(),
                        authority.stream_path()?.into_bytes(),
                        Some(aggregate.reducer().revision().to_string()),
                    )?),
                    revision: ResourceRevision::History(StreamRef::new(
                        stream_provider.clone(),
                        authority.stream_path()?.into_bytes(),
                        Some(aggregate.reducer().revision().to_string()),
                    )?),
                },
                CapturedResource {
                    source: ResourceRevision::Project {
                        volume: project.clone(),
                        generation: project_head.generation.clone(),
                    },
                    revision: ResourceRevision::Project {
                        volume: child_project.clone(),
                        generation: forked.generation.clone(),
                    },
                },
                CapturedResource {
                    source: ResourceRevision::SharedVolume(shared.clone()),
                    revision: ResourceRevision::SharedVolume(shared.clone()),
                },
            ],
            omissions: Vec::new(),
            child_private_volume: child_private.clone(),
            child_private_generation: captured.generation.clone(),
            inherited_context: vec![inherited.clone()],
            shared_grants: vec![SharedGrant {
                volume: shared.clone(),
                child_agent,
                operations: BTreeSet::from([VolumeOperation::Read]),
            }]
            .into_iter()
            .chain((level == 1).then_some(SharedGrant {
                volume: shared.clone(),
                child_agent: AgentId::from_bytes([201; 16]),
                operations: BTreeSet::from([VolumeOperation::Read]),
            }))
            .collect(),
            attached_agents,
            reference_grants: captured.reference_grants,
            attachment_manifests: captured.attachment_manifests,
            inherited_through_sequence: if level == 1 { 2 } else { 1 },
            boundary: None,
        };
        seed.validate()?;
        if level == 1 {
            let mut forged_prefix = seed.clone();
            forged_prefix.inherited_context[0] = FileRef::new(
                child_private.clone(),
                inherited.path(),
                inherited.version(),
                FileDescriptor::from_bytes(
                    b"forged parent transcript",
                    "application/vnd.acyclic.harness.inherited-conversation+json",
                )?,
                inherited.display_name(),
            )?;
            assert!(
                aggregate
                    .reducer()
                    .plan(&fork_command(
                        level + 80,
                        aggregate.reducer().revision(),
                        &grant_scope,
                        forged_prefix,
                    )?)
                    .is_err(),
                "fork must bind the exact authoritative parent prefix"
            );
            let mut renamed_prefix = seed.clone();
            renamed_prefix.inherited_context[0] = FileRef::new(
                child_private.clone(),
                inherited.path(),
                inherited.version(),
                inherited.descriptor().clone(),
                "misleading-name.json",
            )?;
            assert!(
                aggregate
                    .reducer()
                    .plan(&fork_command(
                        level + 81,
                        aggregate.reducer().revision(),
                        &grant_scope,
                        renamed_prefix,
                    )?)
                    .is_err(),
                "fork must bind inherited prefix display metadata"
            );
            let child_reference_scope = child_issuer.root(
                "child-reference-reader",
                seed.reference_capabilities(child_agent)?,
            );
            let child_reference_resolver = FilesystemContentVerifier::new(
                host.clone(),
                child_issuer.verifier(),
                child_reference_scope,
                1_024,
            )?;
            child_reference_resolver
                .verify_manifest(&root_manifest, 1, &Limits::default())
                .await?;
            let inherited_reader = AgentId::from_bytes([201; 16]);
            let capabilities = seed.reference_capabilities(inherited_reader)?;
            assert!(capabilities.contains(&previous_file.read_capability()?));
            assert!(capabilities.contains(&inherited.read_capability()?));
            assert!(capabilities.contains(&root_attachment.read_capability()?));
            assert!(
                seed.shared_capabilities_for(inherited_reader)?
                    .contains(&shared.capability(VolumeOperation::Read)?)
            );
            let reader_scope = child_issuer.root("attached-reader", capabilities);
            let exact = ContentGrant::verify_file_read(
                &child_issuer.verifier(),
                &reader_scope,
                &previous_file,
            )?;
            assert_eq!(
                host.read_content(&previous_file, &exact, 1_024)
                    .await?
                    .as_ref(),
                b"root"
            );
            let inherited_resolver = FilesystemContentVerifier::new(
                host.clone(),
                child_issuer.verifier(),
                reader_scope,
                1_024,
            )?;
            inherited_resolver.verify(&previous_file).await?;
            inherited_resolver.verify(&root_attachment).await?;
            inherited_resolver
                .verify_manifest(&root_manifest, 1, &Limits::default())
                .await?;
            inherited_resolver
                .verify(
                    previous_project_reference.as_ref().ok_or_else(|| {
                        acyclic_harness::Error::Invalid("project ref missing".into())
                    })?,
                )
                .await?;
            inherited_resolver
                .verify(
                    previous_shared_reference.as_ref().ok_or_else(|| {
                        acyclic_harness::Error::Invalid("shared ref missing".into())
                    })?,
                )
                .await?;
            assert!(exact.require_file_read(&inherited).is_err());
            assert!(exact.require(&private, VolumeOperation::Write).is_err());
        }
        if level == 1 {
            let mut missing_member = seed.clone();
            missing_member.reference_grants.retain(|grant| {
                !(grant.reader == child_agent
                    && grant.file == previous_attachment
                    && grant.attachment_manifest.is_some())
            });
            assert!(missing_member.validate().is_ok());
            assert!(
                aggregate
                    .execute(fork_command(
                        217,
                        seed.parent_revision,
                        &grant_scope,
                        missing_member,
                    )?)
                    .await
                    .is_err()
            );
            assert_eq!(aggregate.reducer().revision(), seed.parent_revision);
            let mut over_budget = StreamAggregate::open(
                &stream,
                authority.clone(),
                issuer.verifier(),
                SchemaRegistry::new(),
            )
            .await?
            .with_fork_verifier(Arc::new(CompositeForkVerifier::new(vec![
                Arc::new(
                    FilesystemForkVerifier::new(host.clone(), 64 * 1_024)?
                        .with_total_reference_bytes(1)?,
                ),
                Arc::new(StreamHistoryForkVerifier::new(stream_provider.clone())?),
            ])?));
            assert!(
                over_budget
                    .execute(fork_command(
                        216,
                        seed.parent_revision,
                        &grant_scope,
                        seed.clone(),
                    )?)
                    .await
                    .is_err()
            );
            assert_eq!(over_budget.reducer().revision(), seed.parent_revision);
            let mut incomplete = StreamAggregate::open(
                &stream,
                authority.clone(),
                issuer.verifier(),
                SchemaRegistry::new(),
            )
            .await?
            .with_fork_verifier(Arc::new(CompositeForkVerifier::new(vec![Arc::new(
                FilesystemForkVerifier::new(host.clone(), 64 * 1_024)?,
            )])?));
            assert!(
                incomplete
                    .execute(fork_command(
                        219,
                        seed.parent_revision,
                        &grant_scope,
                        seed.clone(),
                    )?)
                    .await
                    .is_err()
            );
            assert_eq!(incomplete.reducer().revision(), seed.parent_revision);
            let mut foreign_history = seed.clone();
            let foreign = ProviderRef::new("foreign", "stream", "2")?;
            let reference = ResourceRevision::History(StreamRef::new(
                foreign,
                authority.stream_path()?.into_bytes(),
                Some(seed.parent_revision.to_string()),
            )?);
            foreign_history.resources[0].source = reference.clone();
            foreign_history.resources[0].revision = reference;
            foreign_history.validate()?;
            assert!(
                aggregate
                    .execute(fork_command(
                        218,
                        seed.parent_revision,
                        &grant_scope,
                        foreign_history,
                    )?)
                    .await
                    .is_err()
            );
            assert_eq!(aggregate.reducer().revision(), seed.parent_revision);
            let mut missing = seed.clone();
            missing.inherited_context[0] = FileRef::new(
                child_private.clone(),
                ".system/inherited-conversation/missing.txt",
                missing.inherited_context[0].version(),
                missing.inherited_context[0].descriptor().clone(),
                "missing.txt",
            )?;
            for grant in &mut missing.reference_grants {
                if grant.file == inherited {
                    grant.file = missing.inherited_context[0].clone();
                }
            }
            assert!(
                aggregate
                    .execute(fork_command(
                        220,
                        aggregate.reducer().revision(),
                        &grant_scope,
                        missing,
                    )?)
                    .await
                    .is_err()
            );
            assert_eq!(aggregate.reducer().revision(), seed.parent_revision);
            let rogue_agent = AgentId::from_bytes([200; 16]);
            let rogue_private = volume(
                &provider,
                VolumeClass::AgentPrivate,
                "scratch".into(),
                VolumeOwner::Agent(rogue_agent),
            )?;
            let rogue_head = host.create_volume(&rogue_private).await?;
            let rogue_authority = Authority {
                kind: AggregateKind::Conversation,
                id: "rogue-child".into(),
            };
            let rogue_issuer =
                AuthorityIssuer::new("qualification", [17; 32], rogue_authority.clone());
            let rogue_scope = scope(&rogue_issuer, &rogue_private, &shared, &child_project)?;
            let rogue_write = ContentGrant::verify(
                &rogue_issuer.verifier(),
                &rogue_scope,
                &rogue_private,
                VolumeOperation::Write,
            )?;
            host.put_content(
                &rogue_private,
                &rogue_write,
                "secret.txt",
                b"preexisting scratch",
                "text/plain",
                "secret.txt",
                1_024,
                &IdempotencyKey::new("rogue-scratch")?,
            )
            .await?;
            let mut rogue_seed = seed.clone();
            rogue_seed.child = rogue_authority;
            rogue_seed.child_agent = rogue_agent;
            rogue_seed.shared_grants[0].child_agent = rogue_agent;
            rogue_seed.shared_grants.truncate(1);
            rogue_seed.child_private_volume = rogue_private;
            rogue_seed.child_private_generation = rogue_head.generation;
            rogue_seed.inherited_context.clear();
            rogue_seed.attached_agents.clear();
            rogue_seed
                .reference_grants
                .retain(|grant| grant.reader == child_agent);
            for grant in &mut rogue_seed.reference_grants {
                grant.reader = rogue_agent;
            }
            rogue_seed.validate()?;
            assert!(
                aggregate
                    .execute(fork_command(
                        221,
                        aggregate.reducer().revision(),
                        &grant_scope,
                        rogue_seed,
                    )?)
                    .await
                    .is_err()
            );
            assert_eq!(aggregate.reducer().revision(), seed.parent_revision);
        }
        let publish = fork_command(
            level + 80,
            aggregate.reducer().revision(),
            &grant_scope,
            seed.clone(),
        )?;
        if level == 1 {
            let verifier = CompositeForkVerifier::new(vec![
                Arc::new(FilesystemForkVerifier::new(host.clone(), 64 * 1_024)?),
                Arc::new(StreamHistoryForkVerifier::new(stream_provider.clone())?),
            ])?;
            let fence = verifier.activate(&seed).await?;
            assert!(
                host.put_content(
                    &child_private,
                    &child_write,
                    "scratch/fenced.txt",
                    b"late mutation",
                    "text/plain",
                    "fenced.txt",
                    1_024,
                    &IdempotencyKey::new("fenced-private-write")?,
                )
                .await
                .is_err(),
                "private writes cannot race fork publication"
            );
            fence.release().await?;
        }
        aggregate.execute(publish).await?;
        assert_eq!(aggregate.reducer().fork(&child_authority), Some(&seed));
        if level == 1 {
            let prebound_stream = StreamClient::new(Arc::new(MemoryStream::default()));
            let mut prebound_child = StreamAggregate::open(
                &prebound_stream,
                child_authority.clone(),
                child_issuer.verifier(),
                SchemaRegistry::new(),
            )
            .await?;
            prebound_child
                .execute(command(
                    1,
                    0,
                    &child_scope,
                    Action::BindConversation { agent: child_agent },
                )?)
                .await?;
            assert!(
                matches!(
                    prebound_child
                        .bind_published_child(&aggregate, &seed, child_scope.clone())
                        .await,
                    Err(acyclic_harness::Error::Conflict(_))
                ),
                "an independently bound child cannot impersonate the published fork"
            );
        }

        let mut child_aggregate = StreamAggregate::open(
            &stream,
            child_authority.clone(),
            child_issuer.verifier(),
            SchemaRegistry::new(),
        )
        .await?
        .with_content_verifier(Arc::new(FilesystemContentVerifier::new(
            host.clone(),
            child_issuer.verifier(),
            child_scope.clone(),
            1_024,
        )?))
        .with_fork_verifier(Arc::new(CompositeForkVerifier::new(vec![
            Arc::new(FilesystemForkVerifier::new(host.clone(), 64 * 1_024)?),
            Arc::new(StreamHistoryForkVerifier::new(stream_provider.clone())?),
        ])?));
        child_aggregate
            .bind_published_child(&aggregate, &seed, child_scope.clone())
            .await?;
        child_aggregate
            .bind_published_child(&aggregate, &seed, child_scope.clone())
            .await?;
        let binding = child_aggregate
            .reducer()
            .events_after(0, 1)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                acyclic_harness::Error::Storage("child binding event is missing".into())
            })?;
        assert_eq!(
            binding.causal_parent,
            Some(acyclic_harness::core::EventReference {
                authority: authority.clone(),
                revision: seed.parent_revision + 1,
            })
        );
        let text = format!("level {level}");
        let child_file = host
            .put_content(
                &child_private,
                &child_write,
                "messages/current.txt",
                text.as_bytes(),
                "text/plain",
                "current.txt",
                1_024,
                &IdempotencyKey::new(format!("message-{level}"))?,
            )
            .await?;
        let attachment_bytes = format!("evidence at depth {level}");
        let child_attachment = host
            .put_content(
                &child_private,
                &child_write,
                "attachments/evidence.bin",
                attachment_bytes.as_bytes(),
                "application/octet-stream",
                "evidence.bin",
                1_024,
                &IdempotencyKey::new(format!("attachment-{level}"))?,
            )
            .await?;
        let child_read = ContentGrant::verify(
            &child_issuer.verifier(),
            &child_scope,
            &child_private,
            VolumeOperation::Read,
        )?;
        assert_eq!(
            host.read_content(&child_attachment, &child_read, 1_024)
                .await?,
            attachment_bytes.as_bytes()
        );
        assert!(
            host.read_content(&previous_file, &child_read, 1_024)
                .await
                .is_err()
        );
        child_aggregate
            .execute(command(
                2,
                1,
                &child_scope,
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes(identity(level)),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: child_file.clone(),
                        attachments: vec![Attachment {
                            file: child_attachment.clone(),
                            label: Some(format!("depth {level}")),
                        }]
                        .into(),
                        reply_to: None,
                        tool_call_id: None,
                        extensions: BTreeMap::new(),
                    }),
                },
            )?)
            .await?;
        let updated = host
            .apply(
                &forked.workspace,
                Some(&forked.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/marker.txt".into(),
                    bytes: text.into_bytes(),
                }],
                &IdempotencyKey::new(format!("project-write-{level}"))?,
            )
            .await?;
        final_parent_project = project;
        final_parent_authority = authority.clone();
        final_parent_issuer = issuer.clone();
        final_parent_scope = grant_scope.clone();
        project = child_project;
        project_head = acyclic_harness_filesystem::WorkspaceObservation {
            workspace: forked.workspace,
            generation: updated,
        };
        private = child_private;
        previous_file = child_file;
        previous_attachment = child_attachment;
        previous_project_reference = None;
        previous_shared_reference = None;
        authority = child_authority;
        issuer = child_issuer;
        grant_scope = child_scope;
        aggregate = child_aggregate;
    }
    let last_write = ContentGrant::verify(
        &issuer.verifier(),
        &grant_scope,
        &private,
        VolumeOperation::Write,
    )?;
    let image = host
        .put_content(
            &private,
            &last_write,
            "attachments/final.png",
            &[137, 80, 78, 71],
            "image/png",
            "final.png",
            1_024,
            &IdempotencyKey::new("deep-final-image")?,
        )
        .await?;
    let manifest = host
        .put_attachment_manifest(
            &private,
            &last_write,
            "manifests/final.json",
            &[Attachment {
                file: image.clone(),
                label: None,
            }],
            1_024,
            &IdempotencyKey::new("deep-final-manifest")?,
        )
        .await?;
    aggregate
        .execute(command(
            3,
            2,
            &grant_scope,
            Action::AppendConversationMessage {
                message: Box::new(ConversationMessage {
                    id: Uuid::from_bytes([0xF0; 16]),
                    sequence: 2,
                    kind: MessageKind::User,
                    content: previous_file.clone(),
                    attachments: manifest,
                    reply_to: None,
                    tool_call_id: None,
                    extensions: BTreeMap::new(),
                }),
            },
        )?)
        .await?;
    let state = aggregate
        .reducer()
        .conversation()
        .ok_or_else(|| acyclic_harness::Error::Invalid("conversation not bound".into()))?;
    let resolver = FilesystemContentVerifier::new(
        host.clone(),
        issuer.verifier(),
        grant_scope.clone(),
        1_024,
    )?;
    let selected = select_model_context(
        state,
        ModelContextSelection {
            conversation_revision: 2,
            message_ids: vec![Uuid::from_bytes([0xF0; 16])],
        },
        &resolver,
        256,
        2,
        128 * 1024,
    )
    .await?;
    assert!(
        matches!(selected.messages[0].content, ModelContent::Parts(ref parts)
        if matches!(parts.get(1), Some(ModelContentPart::File { file, policy: FileProjectionPolicy::Native }) if file == &image))
    );
    assert_eq!(state.messages.len(), 2);
    assert!(matches!(
        aggregate.reducer().conversation().map(|state| &state.messages[0].attachments),
        Some(ReferencedAttachments::Inline { items }) if items.len() == 1,
    ));
    assert!(
        ParentProjectController::new(
            &host,
            &final_parent_authority,
            &issuer.verifier(),
            &grant_scope,
            final_parent_project.clone(),
        )
        .is_err(),
        "child scope must not authorize its parent's project"
    );
    let parent_controller = ParentProjectController::new(
        &host,
        &final_parent_authority,
        &final_parent_issuer.verifier(),
        &final_parent_scope,
        final_parent_project.clone(),
    )?;
    let plan = parent_controller.prepare_project_merge(&project).await?;
    let merge_key = acyclic_fs::IdempotencyKey::from_bytes([92; 16]);
    let outcome = parent_controller
        .apply_project_merge(&plan, merge_key)
        .await?;
    assert!(matches!(
        outcome,
        JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_)
    ));
    let parent_project_write = ContentGrant::verify(
        &final_parent_issuer.verifier(),
        &final_parent_scope,
        &final_parent_project,
        VolumeOperation::Write,
    )?;
    let notice_file = host
        .put_content(
            &final_parent_project,
            &parent_project_write,
            "notices/merge.txt",
            b"project merge",
            "text/plain",
            "merge.txt",
            1_024,
            &IdempotencyKey::new("deep-merge-notice")?,
        )
        .await?;
    let receipt = parent_controller.merge_receipt(
        &plan,
        &outcome,
        authority.clone(),
        OperationId::from_bytes([93; 16]),
        merge_key,
        ConversationMessage {
            id: Uuid::from_bytes([94; 16]),
            sequence: 2,
            kind: MessageKind::Merge,
            content: notice_file,
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        },
    )?;
    let merge_verifier = FilesystemProjectMergeVerifier::new(host.clone());
    merge_verifier.verify(&receipt).await?;
    let mut forged_source = receipt.clone();
    forged_source.source_generation = receipt.expected_target_generation.clone();
    assert!(merge_verifier.verify(&forged_source).await.is_err());
    let mut forged_operation = receipt.clone();
    forged_operation.filesystem_operation_id = [95; 16];
    assert!(merge_verifier.verify(&forged_operation).await.is_err());
    let mut forged_proof = receipt.clone();
    forged_proof.provider_proof.statement["source_generation"] =
        serde_json::to_value([0_u8; 32])
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    assert!(merge_verifier.verify(&forged_proof).await.is_err());
    Ok(())
}

#[tokio::test]
async fn thirty_two_sibling_forks_reject_stale_and_conflicting_merges() -> Result<()> {
    let provider = ProviderRef::new("qualification", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let root = volume(
        &provider,
        VolumeClass::Project,
        "wide-root".into(),
        VolumeOwner::Project("wide".into()),
    )?;
    let root_head = host.create_volume(&root).await?;
    let parent_authority = Authority {
        kind: AggregateKind::Conversation,
        id: "wide-parent".into(),
    };
    let parent_issuer = AuthorityIssuer::new("qualification", [19; 32], parent_authority.clone());
    let parent_scope = parent_issuer.root(
        "parent",
        Capabilities::new([
            "fork:publish".to_owned(),
            "project:merge".to_owned(),
            root.capability(VolumeOperation::Read)?,
            root.capability(VolumeOperation::Write)?,
        ]),
    );
    let controller = ParentProjectController::new(
        &host,
        &parent_authority,
        &parent_issuer.verifier(),
        &parent_scope,
        root.clone(),
    )?;
    let ungranted = parent_issuer.root(
        "no-promotion",
        Capabilities::new([
            "fork:publish".to_owned(),
            root.capability(VolumeOperation::Read)?,
        ]),
    );
    let ungranted_controller = ParentProjectController::new(
        &host,
        &parent_authority,
        &parent_issuer.verifier(),
        &ungranted,
        root.clone(),
    )?;
    let foreign_project = volume(
        &provider,
        VolumeClass::Project,
        "foreign-child".into(),
        VolumeOwner::Project("other-project".into()),
    )?;
    assert!(
        controller
            .fork_project(
                &root_head.generation,
                &foreign_project,
                &IdempotencyKey::new("foreign-fork")?,
            )
            .await
            .is_err()
    );
    let mut siblings = Vec::new();
    for index in 1..=32_u8 {
        let child = volume(
            &provider,
            VolumeClass::Project,
            format!("wide-child-{index}"),
            VolumeOwner::Project("wide".into()),
        )?;
        if index == 1 {
            assert!(
                ungranted_controller
                    .prepare_project_merge(&child)
                    .await
                    .is_err()
            );
            let child_authority = Authority {
                kind: AggregateKind::Conversation,
                id: "wide-child".into(),
            };
            let child_issuer = AuthorityIssuer::new("qualification", [19; 32], child_authority);
            let child_scope = child_issuer.root("child", parent_scope.capabilities().clone());
            assert!(
                ParentProjectController::new(
                    &host,
                    &parent_authority,
                    &parent_issuer.verifier(),
                    &child_scope,
                    root.clone(),
                )
                .is_err()
            );
        }
        let fork = controller
            .fork_project(
                &root_head.generation,
                &child,
                &IdempotencyKey::new(format!("wide-fork-{index}"))?,
            )
            .await?;
        host.apply(
            &fork.workspace,
            Some(&fork.generation),
            &[WorkspaceMutation::PutFile {
                path: "/shared.txt".into(),
                bytes: format!("sibling {index}").into_bytes(),
            }],
            &IdempotencyKey::new(format!("wide-write-{index}"))?,
        )
        .await?;
        siblings.push(child);
    }
    assert_eq!(
        host.resolve(&root_head.workspace).await?.generation,
        root_head.generation
    );
    let first = controller.prepare_project_merge(&siblings[0]).await?;
    let stale = controller.prepare_project_merge(&siblings[1]).await?;
    assert!(
        ungranted_controller
            .apply_project_merge(&first, acyclic_fs::IdempotencyKey::from_bytes([100; 16]))
            .await
            .is_err()
    );
    let first_outcome = controller
        .apply_project_merge(&first, acyclic_fs::IdempotencyKey::from_bytes([101; 16]))
        .await?;
    assert!(matches!(first_outcome, JoinOutcome::Applied(_)));
    let stale_outcome = controller
        .apply_project_merge(&stale, acyclic_fs::IdempotencyKey::from_bytes([102; 16]))
        .await?;
    assert!(matches!(stale_outcome, JoinOutcome::StaleTarget(_)));
    let inspected = controller.prepare_project_merge(&siblings[1]).await?;
    let conflict = controller
        .apply_project_merge(
            &inspected,
            acyclic_fs::IdempotencyKey::from_bytes([103; 16]),
        )
        .await?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = conflict
    else {
        return Err(acyclic_harness::Error::Invalid(
            "divergent sibling writes did not conflict".into(),
        ));
    };
    assert!(!truncated);
    let described = controller
        .describe_project_merge_conflicts(&inspected, &conflicts, truncated)
        .await?;
    assert_eq!(described.conflicts.len(), conflicts.len());
    assert!(
        ungranted_controller
            .describe_project_merge_conflicts(&inspected, &conflicts, truncated)
            .await
            .is_err()
    );
    assert_eq!(
        host.read(&root_head.workspace, None, "/shared.txt", 32)
            .await?,
        bytes::Bytes::from_static(b"sibling 1"),
    );
    let selections: BTreeMap<_, _> = conflicts
        .into_iter()
        .map(|conflict| (conflict, ConflictSide::Theirs))
        .collect();
    assert!(
        ungranted_controller
            .apply_project_merge_sides(
                &inspected,
                acyclic_fs::IdempotencyKey::from_bytes([104; 16]),
                selections.clone(),
            )
            .await
            .is_err()
    );
    let resolved_key = acyclic_fs::IdempotencyKey::from_bytes([105; 16]);
    let resolved = controller
        .apply_project_merge_sides(&inspected, resolved_key, selections)
        .await?;
    assert!(matches!(&resolved, JoinOutcome::Applied(_)));
    let write = ContentGrant::verify(
        &parent_issuer.verifier(),
        &parent_scope,
        &root,
        VolumeOperation::Write,
    )?;
    let notice_file = host
        .put_content(
            &root,
            &write,
            "notices/resolved-merge.txt",
            b"resolved merge",
            "text/plain",
            "resolved-merge.txt",
            1_024,
            &IdempotencyKey::new("wide-resolved-notice")?,
        )
        .await?;
    let receipt = controller.merge_receipt(
        &inspected,
        &resolved,
        Authority {
            kind: AggregateKind::Conversation,
            id: "wide-child-2".into(),
        },
        OperationId::from_bytes([106; 16]),
        resolved_key,
        ConversationMessage {
            id: Uuid::from_bytes([107; 16]),
            sequence: 1,
            kind: MessageKind::Merge,
            content: notice_file,
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        },
    )?;
    FilesystemProjectMergeVerifier::new(host.clone())
        .verify(&receipt)
        .await?;
    assert_eq!(
        host.read(&root_head.workspace, None, "/shared.txt", 32)
            .await?,
        bytes::Bytes::from_static(b"sibling 2"),
    );
    Ok(())
}
