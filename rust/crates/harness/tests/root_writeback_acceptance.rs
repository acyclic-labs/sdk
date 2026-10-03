//! Public typed root-writeback acceptance coverage against the local provider.
#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

use acyclic_fs::{
    Fs, JoinOutcome, LocalAuthorityBackend, LocalObjectBackend, LocalOptions, MemoryGitCompatStore,
    WorkspaceId,
};
use acyclic_harness::conversation::{
    ConversationMessage, MessageKind, ReferencedAttachments, VolumeClass, VolumeOperation,
    VolumeOwner, VolumeRef,
};
use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry,
};
use acyclic_harness::filesystem::{
    FilesystemGitFacade, FilesystemHost, RootWritebackApproval, RootWritebackRequest, workspace_ref,
};
use acyclic_harness::fork::{CapturedResource, ForkSeed, ResourceRevision};
use acyclic_harness::resources::{GenerationRef, ProviderRef, StreamRef};
use acyclic_harness::{AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result};
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

type LocalHost = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

struct Fixture {
    _directory: TempDir,
    host: Arc<LocalHost>,
    provider: ProviderRef,
    root_project: VolumeRef,
    child_project: VolumeRef,
    root_authority: Authority,
    child_authority: Authority,
    root_scope: acyclic_harness::core::Scope,
    child_scope: acyclic_harness::core::Scope,
    facade: FilesystemGitFacade<MemoryGitCompatStore>,
    reducer: Reducer,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = ProviderRef::new("writeback-acceptance", "filesystem", "2")?;
        let stream_provider = ProviderRef::new("writeback-acceptance", "stream", "2")?;
        let host = Arc::new(FilesystemHost::new(
            Fs::local(LocalOptions::new(directory.path().join("filesystem")))
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
            provider.clone(),
        )?);

        let root_project = VolumeRef::new(
            provider.clone(),
            "root-project",
            VolumeClass::Project,
            VolumeOwner::Project("acceptance".into()),
        )?;
        let child_project = VolumeRef::new(
            provider.clone(),
            "child-project",
            VolumeClass::Project,
            VolumeOwner::Project("acceptance".into()),
        )?;
        let child_private = VolumeRef::new(
            provider.clone(),
            "child-private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
        )?;
        let root_authority = Authority {
            kind: AggregateKind::Conversation,
            id: "root-writeback".into(),
        };
        let child_authority = Authority {
            kind: AggregateKind::Conversation,
            id: "child-writeback".into(),
        };
        let root_agent = AgentId::from_bytes([1; 16]);
        let child_agent = AgentId::from_bytes([2; 16]);
        let root_issuer =
            AuthorityIssuer::new("writeback-acceptance", [7; 32], root_authority.clone());
        let child_issuer =
            AuthorityIssuer::new("writeback-acceptance", [8; 32], child_authority.clone());
        let root_scope = root_issuer.root_for_agent(
            root_agent,
            "root",
            Capabilities::new([
                "conversation:bind".into(),
                "fork:publish".into(),
                "project:merge".into(),
                "project:writeback".into(),
                root_project.capability(VolumeOperation::Read)?,
                root_project.capability(VolumeOperation::Write)?,
                child_project.capability(VolumeOperation::Read)?,
            ]),
        );
        let child_scope = child_issuer.root_for_agent(
            child_agent,
            "child",
            Capabilities::new([
                "conversation:bind".into(),
                child_project.capability(VolumeOperation::Read)?,
                child_project.capability(VolumeOperation::Write)?,
            ]),
        );
        let mut reducer = Reducer::new(
            root_authority.clone(),
            root_issuer.verifier(),
            SchemaRegistry::new(),
        );
        reducer.apply(Command {
            operation_id: OperationId::from_bytes([1; 16]),
            idempotency_key: IdempotencyKey::new("bind-root")?,
            expected_revision: 0,
            scope: root_scope.clone(),
            causal_parent: None,
            action: Action::BindConversation { agent: root_agent },
        })?;

        let root_head = host.create_volume(&root_project).await?;
        let root_write = acyclic_harness::conversation::ContentGrant::verify(
            &root_issuer.verifier(),
            &root_scope,
            &root_project,
            VolumeOperation::Write,
        )?;
        host.put_content(
            &root_project,
            &root_write,
            "shared.txt",
            b"base",
            "text/plain",
            "shared.txt",
            1_024,
            &IdempotencyKey::new("root-base")?,
        )
        .await?;
        let root_head = host.resolve(&root_head.workspace).await?;
        let facade = FilesystemGitFacade::new(
            WorkspaceId::from_bytes([9; 16]),
            MemoryGitCompatStore::new(),
            root_project.clone(),
            root_issuer.verifier(),
            root_scope.clone(),
        )?;
        let child_head = facade
            .fork_project(
                host.as_ref(),
                &reducer,
                &root_head.generation,
                &child_project,
                &IdempotencyKey::new("fork-child")?,
            )
            .await?;
        let child_private_head = host.create_volume(&child_private).await?;

        let history = ResourceRevision::History(StreamRef::new(
            stream_provider,
            root_authority.stream_path()?.into_bytes(),
            Some("1".into()),
        )?);
        let source_project = ResourceRevision::Project {
            volume: root_project.clone(),
            generation: root_head.generation.clone(),
        };
        let child_project_revision = ResourceRevision::Project {
            volume: child_project.clone(),
            generation: child_head.generation.clone(),
        };
        let seed = ForkSeed {
            operation_id: OperationId::from_bytes([3; 16]),
            parent: root_authority.clone(),
            parent_revision: 1,
            inherited_parent_revision: 1,
            child: child_authority.clone(),
            child_agent,
            attached_agents: Vec::new(),
            resources: vec![
                CapturedResource {
                    source: history.clone(),
                    revision: history,
                },
                CapturedResource {
                    source: source_project,
                    revision: child_project_revision,
                },
            ],
            omissions: Vec::new(),
            child_private_volume: child_private,
            child_private_generation: child_private_head.generation,
            inherited_context: Vec::new(),
            inherited_through_sequence: 0,
            shared_grants: Vec::new(),
            reference_grants: Vec::new(),
            model_boundary: None,
            attachment_manifests: Vec::new(),
            boundary: None,
        };
        reducer.apply(Command {
            operation_id: seed.operation_id,
            idempotency_key: IdempotencyKey::new("publish-child")?,
            expected_revision: 1,
            scope: root_scope.clone(),
            causal_parent: None,
            action: Action::PublishFork {
                seed: Box::new(seed),
            },
        })?;

        Ok(Self {
            _directory: directory,
            host,
            provider,
            root_project,
            child_project,
            root_authority,
            child_authority,
            root_scope,
            child_scope,
            facade,
            reducer,
        })
    }

    async fn child_change(
        &self,
        path: &str,
        bytes: &[u8],
        key: &str,
    ) -> Result<ConversationMessage> {
        let child_issuer = AuthorityIssuer::new(
            "writeback-acceptance",
            [8; 32],
            self.child_authority.clone(),
        );
        let grant = acyclic_harness::conversation::ContentGrant::verify(
            &child_issuer.verifier(),
            &self.child_scope,
            &self.child_project,
            VolumeOperation::Write,
        )?;
        let file = self
            .host
            .put_content(
                &self.child_project,
                &grant,
                path,
                bytes,
                "text/plain",
                path,
                1_024,
                &IdempotencyKey::new(key)?,
            )
            .await?;
        Ok(ConversationMessage {
            id: Uuid::from_bytes([11; 16]),
            sequence: 1,
            kind: MessageKind::Merge,
            content: file,
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: Default::default(),
        })
    }

    async fn read_root(&self, path: &str) -> Option<Vec<u8>> {
        let workspace = workspace_ref(
            self.provider.clone(),
            &self.root_project.storage_name().ok()?,
        )
        .ok()?;
        self.host
            .read(&workspace, None, path, 1_024)
            .await
            .ok()
            .map(|bytes| bytes.to_vec())
    }
}

#[tokio::test]
async fn approved_root_writeback_mutates_real_root_only_after_exact_approval() -> Result<()> {
    let fixture = Fixture::new().await?;
    let notice = fixture
        .child_change("child.txt", b"child", "child-add")
        .await?;
    let plan = fixture
        .facade
        .prepare_project_merge_for_child(
            fixture.host.as_ref(),
            &fixture.reducer,
            &fixture.child_authority,
            &fixture.child_project,
        )
        .await?;
    let source = fixture.host.generation_ref_id(plan.source_head())?;
    let target = fixture.host.generation_ref_id(plan.target_head())?;
    let stale_target = GenerationRef::new(fixture.provider.clone(), [0xD1; 32], None)?;
    let stale = RootWritebackApproval::issue(
        &AuthorityIssuer::new(
            "writeback-acceptance",
            [7; 32],
            fixture.root_authority.clone(),
        )
        .verifier(),
        &fixture.root_scope,
        fixture.root_project.clone(),
        OperationId::from_bytes([21; 16]),
        source.clone(),
        stale_target,
    )?;
    let stale_error = fixture
        .facade
        .apply_root_writeback_plan_for_child_with_notice(
            &RootWritebackRequest::new(stale, fixture.root_scope.clone()),
            fixture.host.as_ref(),
            &fixture.reducer,
            &fixture.child_authority,
            &fixture.child_project,
            &plan,
            BTreeMap::new(),
            &notice,
        )
        .await
        .err()
        .expect("stale approval must be rejected before provider mutation");
    assert!(matches!(stale_error, Error::Conflict(_)));
    assert_eq!(fixture.read_root("/child.txt").await, None);

    let approval = RootWritebackApproval::issue(
        &AuthorityIssuer::new(
            "writeback-acceptance",
            [7; 32],
            fixture.root_authority.clone(),
        )
        .verifier(),
        &fixture.root_scope,
        fixture.root_project.clone(),
        OperationId::from_bytes([22; 16]),
        source,
        target,
    )?;
    let outcome = fixture
        .facade
        .apply_root_writeback_plan_for_child_with_notice(
            &RootWritebackRequest::new(approval, fixture.root_scope.clone()),
            fixture.host.as_ref(),
            &fixture.reducer,
            &fixture.child_authority,
            &fixture.child_project,
            &plan,
            BTreeMap::new(),
            &notice,
        )
        .await?;
    assert!(matches!(
        outcome,
        JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_)
    ));
    assert_eq!(
        fixture.read_root("/child.txt").await.as_deref(),
        Some(b"child".as_slice())
    );
    assert_eq!(
        fixture.read_root("/shared.txt").await.as_deref(),
        Some(b"base".as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_user_edit_returns_conflict_without_overwriting_the_root() -> Result<()> {
    let fixture = Fixture::new().await?;
    let notice = fixture
        .child_change("shared.txt", b"child", "child-edit")
        .await?;
    let initial_plan = fixture
        .facade
        .prepare_project_merge_for_child(
            fixture.host.as_ref(),
            &fixture.reducer,
            &fixture.child_authority,
            &fixture.child_project,
        )
        .await?;
    let root_issuer = AuthorityIssuer::new(
        "writeback-acceptance",
        [7; 32],
        fixture.root_authority.clone(),
    );
    let root_write = acyclic_harness::conversation::ContentGrant::verify(
        &root_issuer.verifier(),
        &fixture.root_scope,
        &fixture.root_project,
        VolumeOperation::Write,
    )?;
    fixture
        .host
        .put_content(
            &fixture.root_project,
            &root_write,
            "shared.txt",
            b"user",
            "text/plain",
            "shared.txt",
            1_024,
            &IdempotencyKey::new("user-edit")?,
        )
        .await?;

    // Reinspect after the user edit. The refreshed plan has a current CAS
    // target, so the provider must surface the actual three-way conflict.
    let plan = fixture
        .facade
        .prepare_project_merge_for_child(
            fixture.host.as_ref(),
            &fixture.reducer,
            &fixture.child_authority,
            &fixture.child_project,
        )
        .await?;
    assert_ne!(plan.target_head(), initial_plan.target_head());
    let source = fixture.host.generation_ref_id(plan.source_head())?;
    let target = fixture.host.generation_ref_id(plan.target_head())?;

    let approval = RootWritebackApproval::issue(
        &root_issuer.verifier(),
        &fixture.root_scope,
        fixture.root_project.clone(),
        OperationId::from_bytes([23; 16]),
        source,
        target,
    )?;
    let outcome = fixture
        .facade
        .apply_root_writeback_plan_for_child_with_notice(
            &RootWritebackRequest::new(approval, fixture.root_scope.clone()),
            fixture.host.as_ref(),
            &fixture.reducer,
            &fixture.child_authority,
            &fixture.child_project,
            &plan,
            BTreeMap::new(),
            &notice,
        )
        .await?;
    assert!(matches!(
        outcome,
        JoinOutcome::StaleTarget(_) | JoinOutcome::Conflicted { .. }
    ));
    assert_eq!(
        fixture.read_root("/shared.txt").await.as_deref(),
        Some(b"user".as_slice())
    );
    Ok(())
}
