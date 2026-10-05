//! Typed Filesystem merge publication recovery through a cold reopen.
#![cfg(feature = "filesystem-local")]

use acyclic_fs::{JoinOutcome, LocalCoreStateStore, LocalFs, LocalOptions};
use acyclic_harness::conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry,
};
use acyclic_harness::filesystem::{
    FilesystemGitFacade, FilesystemHost, ParentProjectController, WorkspaceMutation, workspace_ref,
};
use acyclic_harness::resources::ProviderRef;
use acyclic_harness::{AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result};
use std::sync::Arc;
use tempfile::tempdir;

fn bind_parent(
    authority: Authority,
    issuer: &AuthorityIssuer,
    agent: AgentId,
    scope: &acyclic_harness::core::Scope,
) -> Result<Reducer> {
    let mut reducer = Reducer::new(authority, issuer.verifier(), SchemaRegistry::new());
    reducer.apply(Command {
        operation_id: OperationId::from_bytes([24; 16]),
        idempotency_key: IdempotencyKey::new("bind-merge-parent")?,
        expected_revision: 0,
        scope: scope.clone(),
        causal_parent: None,
        action: Action::BindConversation { agent },
    })?;
    Ok(reducer)
}

#[tokio::test]
async fn typed_merge_publication_reopens_and_replays_exact_operation() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let filesystem_root = directory.path().join("filesystem");
    let provider = ProviderRef::new("typed-merge-recovery", "filesystem", "2")?;
    let project = VolumeRef::new(
        provider.clone(),
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("typed-merge-recovery".into()),
    )?;
    let child = VolumeRef::new(
        provider.clone(),
        "child-project",
        VolumeClass::Project,
        VolumeOwner::Project("typed-merge-recovery".into()),
    )?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "typed-merge-parent".into(),
    };
    let agent = AgentId::from_bytes([22; 16]);
    let issuer = AuthorityIssuer::new("typed-merge-recovery", [21; 32], authority.clone());
    let scope = issuer.root_for_agent(
        agent,
        "typed-merge-parent",
        Capabilities::new([
            "conversation:bind".into(),
            "fork:publish".into(),
            "project:merge".into(),
            project.capability(VolumeOperation::Read)?,
            project.capability(VolumeOperation::Write)?,
        ]),
    );
    let reducer = bind_parent(authority.clone(), &issuer, agent, &scope)?;

    let host = Arc::new(FilesystemHost::new(
        LocalFs::local(LocalOptions::new(filesystem_root.clone()))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let parent_head = host.create_volume(&project).await?;
    let facade = FilesystemGitFacade::new(
        acyclic_fs::WorkspaceId::from_bytes([23; 16]),
        LocalCoreStateStore::new(directory.path().join("control-plane")),
        project.clone(),
        issuer.verifier(),
        scope.clone(),
    )?;
    let child_head = facade
        .fork_project(
            host.as_ref(),
            &reducer,
            &parent_head.generation,
            &child,
            &IdempotencyKey::new("typed-merge-fork")?,
        )
        .await?;
    host.apply(
        &child_head.workspace,
        Some(&child_head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/merge-publication.txt".into(),
            bytes: b"published by the typed workspace join".to_vec(),
        }],
        &IdempotencyKey::new("typed-merge-child-edit")?,
    )
    .await?;

    let operation_id = OperationId::from_bytes([43; 16]);
    let (source_generation, target_generation, target_authority_head) = {
        let controller = ParentProjectController::new(
            host.as_ref(),
            &reducer,
            &issuer.verifier(),
            &scope,
            project.clone(),
        )?;
        let plan = controller.prepare_project_merge(&child).await?;
        let source_generation = host.generation_ref_id(plan.source_head())?;
        let target_generation = host.generation_ref_id(plan.target_head())?;
        let target_authority_head = plan.target_authority_head();
        let outcome = controller.apply_project_merge(&plan, operation_id).await?;
        assert!(matches!(outcome, JoinOutcome::Applied(_)));
        // Model the crash window after the provider has committed the join but
        // before the caller durably records the returned result.
        drop(outcome);
        (source_generation, target_generation, target_authority_head)
    };
    assert_eq!(
        host.read(&parent_head.workspace, None, "/merge-publication.txt", 256,)
            .await?
            .as_ref(),
        b"published by the typed workspace join"
    );
    let first_result_generation = host.resolve(&parent_head.workspace).await?.generation;

    drop(facade);
    drop(reducer);
    drop(host);

    let reopened_host = Arc::new(FilesystemHost::new(
        LocalFs::local(LocalOptions::new(filesystem_root))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let reopened_reducer = bind_parent(authority, &issuer, agent, &scope)?;
    let reopened_controller = ParentProjectController::new(
        reopened_host.as_ref(),
        &reopened_reducer,
        &issuer.verifier(),
        &scope,
        project.clone(),
    )?;
    let reopened_plan = reopened_controller
        .prepare_project_merge_at(
            &child,
            &source_generation,
            &target_generation,
            target_authority_head,
        )
        .await?;
    let replay = reopened_controller
        .apply_project_merge(&reopened_plan, operation_id)
        .await?;
    assert!(matches!(replay, JoinOutcome::AlreadyApplied(_)));
    let replayed_generation = reopened_host
        .resolve(&workspace_ref(provider.clone(), &project.storage_name()?)?)
        .await?
        .generation;
    assert_eq!(replayed_generation, first_result_generation);
    assert_eq!(
        reopened_host
            .read(
                &workspace_ref(provider.clone(), &project.storage_name()?)?,
                None,
                "/merge-publication.txt",
                256,
            )
            .await
            .err(),
        None,
        "the recovered target must remain readable after replay"
    );
    Ok(())
}
