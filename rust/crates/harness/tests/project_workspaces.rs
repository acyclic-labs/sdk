//! Project-workspace binding and parent-fork publication coverage.
#![cfg(feature = "filesystem")]

use acyclic_fs::Fs;
use acyclic_harness::filesystem::workspace_ref;
use acyclic_harness::filesystem::{FilesystemHost, FilesystemProjectWorkspaces};
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, OperationId, Result,
    conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry},
    fork::{CapturedResource, ForkSeed, ResourceRevision},
    merge::ProjectWorkspaceProvider,
    resources::{ProviderRef, StreamRef},
};
use std::sync::Arc;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each step next to the state it checks"
)]
async fn project_workspace_binding_uses_parent_forks_published_after_construction() -> Result<()> {
    let provider = ProviderRef::new("project-workspaces".into(), "filesystem".into(), "2".into(), &mut None)?;
    let stream_provider = ProviderRef::new("project-workspaces".into(), "stream".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
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
        "parent-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project-owner".into()),
        &mut None,
    )?;
    let child_project = VolumeRef::new(
        provider.clone(),
        "published-child-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project-owner".into()),
        &mut None,
    )?;
    let child_private = VolumeRef::new(
        provider.clone(),
        "published-child-private".into(),
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
    let issuer = AuthorityIssuer::new("project-workspaces", [7; 32], parent.clone());
    let scope = issuer.issue("parent", &Capabilities::new([
            "conversation:bind".to_owned(),
            "fork:publish".to_owned(),
            "project:merge".to_owned(),
            project.capability(VolumeOperation::Read, &mut None)?,
            project.capability(VolumeOperation::Write, &mut None)?,
            child_project.capability(VolumeOperation::Read, &mut None)?,
            parent_private.capability(VolumeOperation::Read, &mut None)?,
        ]), Some(parent_agent), None, &mut None).expect("scope proof");

    let parent_head = host.create_volume(&project).await?;
    let parent_private_head = host.create_volume(&parent_private).await?;
    let child_scope = issuer.issue("child", &Capabilities::new([child_private.capability(VolumeOperation::Write, &mut None)?]), Some(child_agent), None, &mut None).expect("scope proof");
    let child_private_head = host
        .fork_volume(
            &parent_private,
            &ContentGrant::verify(
                &issuer.verifier(&mut None).expect("verifier copy"),
                &scope,
                &parent_private,
                VolumeOperation::Read,
                u64::MAX, &mut None,
            )?,
            &parent_private_head.generation,
            &child_private,
            &ContentGrant::verify(
                &issuer.verifier(&mut None).expect("verifier copy"),
                &child_scope,
                &child_private,
                VolumeOperation::Write,
                u64::MAX, &mut None,
            )?,
            Some(Vec::new()),
            &IdempotencyKey::new("fork-private")?,
        )
        .await?;
    let mut reducer = Reducer::new(parent.clone(), issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new());
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

    let workspaces = FilesystemProjectWorkspaces::new(
        &host,
        &reducer,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        project.clone(),
    )?;

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
        parent.stream_path(&mut None)?.into_bytes(),
        Some(reducer.revision().to_string()),
    )?;
    let seed = ForkSeed { history_witness: None, summary: None,
    authorized_source: None,
    child_result_volume: None,
    child_result_generation: None,
    operation_id: OperationId::from_bytes([2; 16]),
    parent: parent.clone(),
    parent_revision: reducer.revision(),
    child: child.clone(),
    child_agent,
    attached_agents: Vec::new(),
    resources: vec![
        CapturedResource {
            source: ResourceRevision::PrivateVolume {
                volume: parent_private,
                generation: parent_private_head.generation,
                paths: Vec::new(),
            },
            revision: ResourceRevision::PrivateVolume {
                volume: child_private.clone(),
                generation: child_private_head.generation.clone(),
                paths: Vec::new(),
            },
        },
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
                generation: child_head.clone(),
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
    boundary: None, };
    seed.validate(&mut None)?;
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
            .prepare_project_import(
                &scope,
                &reducer,
                &child,
                &child_project,
                &host
                    .resolve(&workspace_ref(
                        provider.clone(),
                        &(child_project).storage_name(&mut None)?
                    )?)
                    .await?
                    .generation
            )
            .await
            .is_ok()
    );
    assert!(
        workspaces
            .prepare_project_import(
                &scope,
                &reducer,
                &unrelated,
                &child_project,
                &host
                    .resolve(&workspace_ref(
                        provider.clone(),
                        &(child_project).storage_name(&mut None)?
                    )?)
                    .await?
                    .generation
            )
            .await
            .is_ok()
    );
    Ok(())
}
