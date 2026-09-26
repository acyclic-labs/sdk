//! Project-workspace binding and parent-fork publication coverage.

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, OperationId, Result,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry},
    fork::{CapturedResource, ForkSeed, ResourceRevision},
    merge::ProjectWorkspaceProvider,
    resources::{ProviderRef, StreamRef},
};
use acyclic_harness_filesystem::{FilesystemHost, FilesystemProjectWorkspaces};
use std::sync::Arc;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn project_workspace_binding_uses_parent_forks_published_after_construction() -> Result<()> {
    let provider = ProviderRef::new("project-workspaces", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("project-workspaces", "stream", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let parent = Authority {
        kind: AggregateKind::Conversation,
        id: "parent".into(),
    };
    let child = Authority {
        kind: AggregateKind::Conversation,
        id: "published-child".into(),
    };
    let unrelated = Authority {
        kind: AggregateKind::Conversation,
        id: "unrelated-child".into(),
    };
    let parent_agent = AgentId::from_bytes([1; 16]);
    let child_agent = AgentId::from_bytes([2; 16]);
    let project = VolumeRef::new(
        provider.clone(),
        "parent-project",
        VolumeClass::Project,
        VolumeOwner::Project("project-owner".into()),
    )?;
    let child_project = VolumeRef::new(
        provider.clone(),
        "published-child-project",
        VolumeClass::Project,
        VolumeOwner::Project("project-owner".into()),
    )?;
    let child_private = VolumeRef::new(
        provider.clone(),
        "published-child-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
    )?;
    let issuer = AuthorityIssuer::new("project-workspaces", [7; 32], parent.clone());
    let scope = issuer.root_for_agent(
        parent_agent,
        "parent",
        Capabilities::new([
            "conversation:bind".to_owned(),
            "fork:publish".to_owned(),
            "project:merge".to_owned(),
            project.capability(VolumeOperation::Read)?,
            project.capability(VolumeOperation::Write)?,
        ]),
    );

    let parent_head = host.create_volume(&project).await?;
    let child_private_head = host.create_volume(&child_private).await?;
    let mut reducer = Reducer::new(parent.clone(), issuer.verifier(), SchemaRegistry::new());
    reducer.apply(Command {
        operation_id: OperationId::from_bytes([1; 16]),
        idempotency_key: IdempotencyKey::new("bind-parent")?,
        expected_revision: 0,
        scope: scope.clone(),
        causal_parent: None,
        action: Action::BindConversation {
            agent: parent_agent,
        },
    })?;

    // Bind the provider before the parent has published any fork.
    let workspaces = FilesystemProjectWorkspaces::new(
        &host,
        &reducer,
        &issuer.verifier(),
        &scope,
        project.clone(),
    )?;

    // Allocate the child project after binding, but publish its Harness fork
    // only after the provider has been constructed.
    let child_head = workspaces
        .fork_project(
            &scope,
            &parent_head.generation,
            &child_project,
            &IdempotencyKey::new("fork-project")?,
        )
        .await?;
    let history = StreamRef::new(
        stream_provider.clone(),
        parent.stream_path()?.into_bytes(),
        Some(reducer.revision().to_string()),
    )?;
    let seed = ForkSeed {
        operation_id: OperationId::from_bytes([2; 16]),
        parent: parent.clone(),
        parent_revision: reducer.revision(),
        child: child.clone(),
        child_agent,
        attached_agents: Vec::new(),
        resources: vec![
            CapturedResource {
                source: ResourceRevision::History(history.clone()),
                revision: ResourceRevision::History(history),
            },
            CapturedResource {
                source: ResourceRevision::Project {
                    volume: project.clone(),
                    generation: parent_head.generation.clone(),
                },
                revision: ResourceRevision::Project {
                    volume: child_project.clone(),
                    generation: child_head,
                },
            },
        ],
        omissions: Vec::new(),
        child_private_volume: child_private,
        child_private_generation: child_private_head.generation,
        inherited_context: Vec::new(),
        inherited_through_sequence: 0,
        shared_grants: Vec::new(),
        reference_grants: Vec::new(),
        attachment_manifests: Vec::new(),
        boundary: None,
    };
    seed.validate()?;
    reducer.apply(Command {
        operation_id: seed.operation_id,
        idempotency_key: IdempotencyKey::new("publish-fork")?,
        expected_revision: seed.parent_revision,
        scope: scope.clone(),
        causal_parent: None,
        action: Action::PublishFork {
            seed: Box::new(seed),
        },
    })?;

    assert!(
        workspaces
            .prepare_project_merge(&scope, &reducer, &child, &child_project)
            .await
            .is_ok()
    );
    assert!(
        workspaces
            .prepare_project_merge(&scope, &reducer, &unrelated, &child_project)
            .await
            .is_err()
    );
    Ok(())
}
