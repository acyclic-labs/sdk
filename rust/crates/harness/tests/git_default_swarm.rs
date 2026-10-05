//! Black-box smoke coverage for the default local Git-capable composition.
#![cfg(feature = "filesystem-local")]

use acyclic_fs::{
    ConflictSide, Digest, Fs, GenerationId, GitCommand, GitFilesystemAction, GitFilesystemExecutor,
    GitFilesystemResult, GitObjectName, GitResetMode, GitTreeRef, LocalAuthorityBackend,
    LocalObjectBackend, LocalOptions, MemoryGitCompatStore, MergeConflict,
    OperationId as FsOperationId, WorkspaceId,
};
use acyclic_harness::conversation::{
    ContentGrant, MessageKind, ReferencedAttachments, VolumeClass, VolumeOperation, VolumeOwner,
    VolumeRef,
};
use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry,
};
use acyclic_harness::filesystem::PersistentLocalSwarm;
use acyclic_harness::filesystem::{
    FilesystemGitFacade, FilesystemHost, WorkspaceMutation, workspace_ref,
};
use acyclic_harness::fork::{CapturedResource, ForkSeed, ResourceRevision};
use acyclic_harness::model::{Model, ModelContent, ModelContentPart, ModelEvent, ModelProvider};
use acyclic_harness::resources::{GenerationRef, ProviderRef, StreamRef};
use acyclic_harness::{Error, Limits, OperationId, Result};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::stream;
use futures::stream::BoxStream;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tempfile::tempdir;
use uuid::Uuid;

type TestHost = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

struct LifecycleExecutor {
    workspace_id: WorkspaceId,
    fail_next: AtomicBool,
    operations: Mutex<Vec<FsOperationId>>,
}

impl LifecycleExecutor {
    fn new(workspace_id: WorkspaceId) -> Self {
        Self {
            workspace_id,
            fail_next: AtomicBool::new(false),
            operations: Mutex::new(Vec::new()),
        }
    }

    fn fail_once(&self) {
        self.fail_next.store(true, Ordering::SeqCst);
    }
}

impl GitFilesystemExecutor for LifecycleExecutor {
    type Error = Error;

    async fn validate_workspace_tree(&self, _workspace_tree: GitTreeRef) -> Result<()> {
        Ok(())
    }

    async fn execute(
        &self,
        operation_id: FsOperationId,
        action: &GitFilesystemAction,
    ) -> std::result::Result<GitFilesystemResult, Self::Error> {
        self.operations
            .lock()
            .map_err(|_| Error::Storage("lifecycle operation lock poisoned".into()))?
            .push(operation_id);
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(Error::Storage("injected lifecycle interruption".into()));
        }
        Ok(match action {
            GitFilesystemAction::CaptureCommit { workspace_tree, .. } => {
                GitFilesystemResult::Captured {
                    tree: *workspace_tree,
                    tracked_paths: Default::default(),
                    proof: None,
                }
            }
            GitFilesystemAction::ForkBranch { .. } => GitFilesystemResult::Forked {
                workspace_id: self.workspace_id,
            },
            _ => GitFilesystemResult::Applied {
                tree: Some(live_tree(self.workspace_id)),
                tracked_paths: Some(Default::default()),
            },
        })
    }
}

fn live_tree(workspace_id: WorkspaceId) -> GitTreeRef {
    GitTreeRef::exact(workspace_id, GenerationId::new(Digest::from_bytes([1; 32])))
}

fn transition_fixture() -> Result<(FilesystemGitFacade<MemoryGitCompatStore>, WorkspaceId)> {
    let provider = ProviderRef::new("git-default-lifecycle", "filesystem", "2")?;
    let project = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("root".into()),
    )?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "root".into(),
    };
    let issuer = AuthorityIssuer::new("git-default-lifecycle", [51; 32], authority);
    let scope = issuer.root_for_agent(
        acyclic_harness::AgentId::from_bytes([52; 16]),
        "root",
        acyclic_harness::Capabilities::new([
            project.capability(VolumeOperation::Read)?,
            project.capability(VolumeOperation::Write)?,
            "fork:publish".to_owned(),
            "project:merge".to_owned(),
        ]),
    );
    let workspace_id = WorkspaceId::from_bytes([53; 16]);
    Ok((
        FilesystemGitFacade::new(
            workspace_id,
            MemoryGitCompatStore::new(),
            project,
            issuer.verifier(),
            scope,
        )?,
        workspace_id,
    ))
}

struct ProjectNode {
    authority: Authority,
    agent: acyclic_harness::AgentId,
    issuer: AuthorityIssuer,
    project: VolumeRef,
    private: VolumeRef,
    project_generation: GenerationRef,
    private_generation: GenerationRef,
    scope: acyclic_harness::core::Scope,
}

async fn project_node(
    host: &TestHost,
    provider: &ProviderRef,
    label: &str,
    authority_id: &str,
    identity: u8,
    create_project: bool,
) -> Result<ProjectNode> {
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: authority_id.into(),
    };
    let agent = acyclic_harness::AgentId::from_bytes([identity; 16]);
    let issuer = AuthorityIssuer::new("git-default-swarm", [identity; 32], authority.clone());
    let project = VolumeRef::new(
        provider.clone(),
        format!("{label}-project"),
        VolumeClass::Project,
        VolumeOwner::Project("git-default-swarm".into()),
    )?;
    let private = VolumeRef::new(
        provider.clone(),
        format!("{label}-private"),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let project_generation = if create_project {
        let project_head = host.create_volume(&project).await?;
        host.generation_ref_id(project_head.generation.id())?
    } else {
        GenerationRef::new(provider.clone(), [identity; 32], Some("pending".into()))?
    };
    let private_head = host.create_volume(&private).await?;
    let scope = issuer.root_for_agent(
        agent,
        format!("{label}-scope"),
        acyclic_harness::Capabilities::new([
            project.capability(VolumeOperation::Read)?,
            project.capability(VolumeOperation::Write)?,
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
            "conversation:bind".to_owned(),
            "fork:publish".to_owned(),
            "project:merge".to_owned(),
        ]),
    );
    Ok(ProjectNode {
        authority,
        agent,
        issuer,
        project,
        private,
        project_generation,
        private_generation: host.generation_ref_id(private_head.generation.id())?,
        scope,
    })
}

fn bind_reducer(reducer: &mut Reducer, node: &ProjectNode, operation: u8) -> Result<()> {
    reducer.apply(Command {
        operation_id: OperationId::from_bytes([operation; 16]),
        idempotency_key: acyclic_harness::IdempotencyKey::new(format!("bind-{operation}"))?,
        expected_revision: reducer.revision(),
        scope: node.scope.clone(),
        causal_parent: None,
        action: Action::BindConversation { agent: node.agent },
    })?;
    Ok(())
}

async fn publish_direct_fork(
    parent: &mut Reducer,
    parent_node: &ProjectNode,
    child_node: &ProjectNode,
    stream_provider: &ProviderRef,
    operation: u8,
) -> Result<()> {
    let history = StreamRef::new(
        stream_provider.clone(),
        parent_node.authority.stream_path()?.into_bytes(),
        Some("1".into()),
    )?;
    let seed = ForkSeed {
        operation_id: OperationId::from_bytes([operation; 16]),
        parent: parent_node.authority.clone(),
        parent_revision: 1,
        child: child_node.authority.clone(),
        child_agent: child_node.agent,
        attached_agents: Vec::new(),
        resources: vec![
            CapturedResource {
                source: ResourceRevision::History(history.clone()),
                revision: ResourceRevision::History(history),
            },
            CapturedResource {
                source: ResourceRevision::Project {
                    volume: parent_node.project.clone(),
                    generation: parent_node.project_generation.clone(),
                },
                revision: ResourceRevision::Project {
                    volume: child_node.project.clone(),
                    generation: child_node.project_generation.clone(),
                },
            },
        ],
        omissions: Vec::new(),
        child_private_volume: child_node.private.clone(),
        child_private_generation: child_node.private_generation.clone(),
        inherited_context: Vec::new(),
        inherited_through_sequence: 0,
        shared_grants: Vec::new(),
        reference_grants: Vec::new(),
        model_boundary: None,
        attachment_manifests: Vec::new(),
        boundary: None,
    };
    parent
        .execute(Command {
            operation_id: OperationId::from_bytes([operation.wrapping_add(1); 16]),
            idempotency_key: acyclic_harness::IdempotencyKey::new(format!("publish-{operation}"))?,
            expected_revision: parent.revision(),
            scope: parent_node.scope.clone(),
            causal_parent: None,
            action: Action::PublishFork {
                seed: Box::new(seed),
            },
        })
        .await?;
    Ok(())
}

fn root_facade(
    node: &ProjectNode,
    workspace: u8,
) -> Result<FilesystemGitFacade<MemoryGitCompatStore>> {
    FilesystemGitFacade::new(
        WorkspaceId::from_bytes([workspace; 16]),
        MemoryGitCompatStore::new(),
        node.project.clone(),
        node.issuer.verifier(),
        node.scope.clone(),
    )
}

async fn merge_notice(
    host: &TestHost,
    node: &ProjectNode,
    path: &str,
) -> Result<acyclic_harness::conversation::ConversationMessage> {
    let grant = ContentGrant::verify(
        &node.issuer.verifier(),
        &node.scope,
        &node.private,
        VolumeOperation::Write,
    )?;
    let content = host
        .put_content(
            &node.private,
            &grant,
            path,
            b"merge notice",
            "text/plain",
            "merge-notice.txt",
            1_024,
            &acyclic_harness::IdempotencyKey::new(format!("notice-{path}"))?,
        )
        .await?;
    Ok(acyclic_harness::conversation::ConversationMessage {
        id: Uuid::from_bytes([path.len() as u8; 16]),
        sequence: 1,
        kind: MessageKind::Merge,
        content,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: None,
        extensions: Default::default(),
    })
}

struct CompletionProvider;

impl ModelProvider for CompletionProvider {
    fn generate<'a>(
        &'a self,
        _prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        Box::pin(stream::iter([
            Ok(ModelEvent::Content {
                delta: "ready".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]))
    }
}

struct GitStatusProvider {
    calls: AtomicUsize,
    result_seen: AtomicBool,
}

impl GitStatusProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            result_seen: AtomicBool::new(false),
        })
    }
}

impl ModelProvider for GitStatusProvider {
    fn generate<'a>(
        &'a self,
        prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let request = prepared.request();
        let has_result = request.messages.iter().any(|message| {
            matches!(
                &message.content,
                ModelContent::Part(ModelContentPart::ToolResult { name, .. })
                    if name == "acyclic.git"
            )
        });
        if has_result {
            self.result_seen.store(true, Ordering::SeqCst);
        }
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let events = if first {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "default-git-status".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["status"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else {
            vec![
                Ok(ModelEvent::Content {
                    delta: "git-ready".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        };
        Box::pin(stream::iter(events))
    }
}

#[tokio::test]
async fn default_local_swarm_opens_with_the_git_facade_bound() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let model = Model::new("mock", "git-default-smoke", "1", json!({}))?;
    let swarm = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        directory.path(),
        model,
        Arc::new(CompletionProvider),
        Limits::default(),
    )
    .await?;

    let output = swarm
        .run_root(
            OperationId::from_bytes([0xD1; 16]),
            "verify default git composition",
        )
        .await?;
    assert_eq!(output.text, "ready");
    assert_eq!(swarm.sessions().await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn default_local_swarm_executes_git_status_and_reopens() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let model = Model::new("mock", "git-default-status", "1", json!({}))?;
    let provider = GitStatusProvider::new();
    let swarm = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        directory.path(),
        model.clone(),
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let output = swarm
        .run_root(
            OperationId::from_bytes([0xD2; 16]),
            "run the default git status check",
        )
        .await?;
    assert_eq!(output.text, "git-ready");
    assert!(provider.result_seen.load(Ordering::SeqCst));
    drop(swarm);

    let reopened_provider = GitStatusProvider::new();
    let reopened = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        directory.path(),
        model,
        reopened_provider.clone(),
        Limits::default(),
    )
    .await?;
    let output = reopened
        .run_root(
            OperationId::from_bytes([0xD3; 16]),
            "repeat the default git status check after restart",
        )
        .await?;
    assert_eq!(output.text, "git-ready");
    assert!(reopened_provider.result_seen.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn recursive_project_merges_use_exact_workspaces_and_reject_cross_lineage() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new("git-default-recursive", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("git-default-recursive", "stream", "2")?;
    let host = Arc::new(FilesystemHost::new(
        Fs::local(LocalOptions::new(directory.path().join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(
            directory.path().join("streams"),
            LocalStreamLimits::default(),
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    let root = project_node(host.as_ref(), &provider, "root", "root", 1, true).await?;
    let mut child = project_node(host.as_ref(), &provider, "child", "child", 2, false).await?;
    let mut grandchild = project_node(
        host.as_ref(),
        &provider,
        "grandchild",
        "grandchild",
        3,
        false,
    )
    .await?;
    let mut sibling =
        project_node(host.as_ref(), &provider, "sibling", "sibling", 4, false).await?;
    let mut root_reducer = Reducer::new(
        root.authority.clone(),
        root.issuer.verifier(),
        SchemaRegistry::new(),
    );
    let mut child_reducer = Reducer::new(
        child.authority.clone(),
        child.issuer.verifier(),
        SchemaRegistry::new(),
    );
    bind_reducer(&mut root_reducer, &root, 1)?;
    bind_reducer(&mut child_reducer, &child, 2)?;
    let root_git = root_facade(&root, 21)?;
    let child_git = root_facade(&child, 22)?;
    child.project_generation = root_git
        .fork_project(
            host.as_ref(),
            &root_reducer,
            &root.project_generation,
            &child.project,
            &acyclic_harness::IdempotencyKey::new("root-child-project-fork")?,
        )
        .await?
        .generation;
    grandchild.project_generation = child_git
        .fork_project(
            host.as_ref(),
            &child_reducer,
            &child.project_generation,
            &grandchild.project,
            &acyclic_harness::IdempotencyKey::new("child-grandchild-project-fork")?,
        )
        .await?
        .generation;
    sibling.project_generation = root_git
        .fork_project(
            host.as_ref(),
            &root_reducer,
            &root.project_generation,
            &sibling.project,
            &acyclic_harness::IdempotencyKey::new("root-sibling-project-fork")?,
        )
        .await?
        .generation;
    publish_direct_fork(&mut root_reducer, &root, &child, &stream_provider, 10).await?;
    publish_direct_fork(
        &mut child_reducer,
        &child,
        &grandchild,
        &stream_provider,
        20,
    )
    .await?;
    publish_direct_fork(&mut root_reducer, &root, &sibling, &stream_provider, 30).await?;

    let grandchild_workspace = host
        .resolve(&workspace_ref(
            provider.clone(),
            &grandchild.project.storage_name()?,
        )?)
        .await?;
    host.apply(
        &grandchild_workspace.workspace,
        Some(&grandchild_workspace.generation),
        &[WorkspaceMutation::PutFile {
            path: "/recursive.txt".into(),
            bytes: b"grandchild exact content".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("grandchild-recursive-write")?,
    )
    .await?;

    let child_facade = root_facade(&child, 22)?;
    let child_plan = child_facade
        .prepare_project_merge_for_child(
            host.as_ref(),
            &child_reducer,
            &grandchild.authority,
            &grandchild.project,
        )
        .await?;
    let child_notice = merge_notice(host.as_ref(), &child, "notices/child-merge.txt").await?;
    let child_outcome = child_facade
        .apply_project_merge_for_child_with_notice(
            host.as_ref(),
            &child_reducer,
            &grandchild.authority,
            &grandchild.project,
            &child_plan,
            OperationId::from_bytes([40; 16]),
            &child_notice,
        )
        .await?;
    assert!(matches!(
        child_outcome,
        acyclic_fs::JoinOutcome::Applied(_)
            | acyclic_fs::JoinOutcome::AlreadyApplied(_)
            | acyclic_fs::JoinOutcome::NoChanges(_)
    ));
    let child_after = host
        .resolve(&workspace_ref(
            provider.clone(),
            &child.project.storage_name()?,
        )?)
        .await?;
    assert_eq!(
        host.read(&child_after.workspace, None, "/recursive.txt", 1_024,)
            .await?
            .as_ref(),
        b"grandchild exact content",
    );

    let root_facade = root_facade(&root, 21)?;
    let root_plan = root_facade
        .prepare_project_merge_for_child(
            host.as_ref(),
            &root_reducer,
            &child.authority,
            &child.project,
        )
        .await?;
    let root_notice = merge_notice(host.as_ref(), &root, "notices/root-merge.txt").await?;
    let root_outcome = root_facade
        .apply_project_merge_for_child_with_notice(
            host.as_ref(),
            &root_reducer,
            &child.authority,
            &child.project,
            &root_plan,
            OperationId::from_bytes([41; 16]),
            &root_notice,
        )
        .await?;
    assert!(matches!(
        root_outcome,
        acyclic_fs::JoinOutcome::Applied(_)
            | acyclic_fs::JoinOutcome::AlreadyApplied(_)
            | acyclic_fs::JoinOutcome::NoChanges(_)
    ));
    let root_after = host
        .resolve(&workspace_ref(
            provider.clone(),
            &root.project.storage_name()?,
        )?)
        .await?;
    assert_eq!(
        host.read(&root_after.workspace, None, "/recursive.txt", 1_024)
            .await?
            .as_ref(),
        b"grandchild exact content",
    );

    assert!(
        root_facade
            .prepare_project_merge_for_child(
                host.as_ref(),
                &root_reducer,
                &grandchild.authority,
                &grandchild.project,
            )
            .await
            .is_err()
    );
    assert!(
        child_facade
            .prepare_project_merge_for_child(
                host.as_ref(),
                &child_reducer,
                &sibling.authority,
                &sibling.project,
            )
            .await
            .is_err()
    );
    assert!(
        root_facade
            .prepare_project_merge_for_child(
                host.as_ref(),
                &root_reducer,
                &root.authority,
                &root.project,
            )
            .await
            .is_err()
    );

    let child_workspace = host
        .resolve(&workspace_ref(
            provider.clone(),
            &child.project.storage_name()?,
        )?)
        .await?;
    host.apply(
        &child_workspace.workspace,
        Some(&child_workspace.generation),
        &[WorkspaceMutation::PutFile {
            path: "/conflict.txt".into(),
            bytes: b"child side".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("child-conflict-write")?,
    )
    .await?;
    let root_workspace = host
        .resolve(&workspace_ref(
            provider.clone(),
            &root.project.storage_name()?,
        )?)
        .await?;
    host.apply(
        &root_workspace.workspace,
        Some(&root_workspace.generation),
        &[WorkspaceMutation::PutFile {
            path: "/conflict.txt".into(),
            bytes: b"root side".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("root-conflict-write")?,
    )
    .await?;
    let conflict_plan = root_facade
        .prepare_project_merge_for_child(
            host.as_ref(),
            &root_reducer,
            &child.authority,
            &child.project,
        )
        .await?;
    let conflict_notice = merge_notice(host.as_ref(), &root, "notices/conflict.txt").await?;
    let conflict_outcome = root_facade
        .apply_project_merge_for_child_with_notice(
            host.as_ref(),
            &root_reducer,
            &child.authority,
            &child.project,
            &conflict_plan,
            OperationId::from_bytes([42; 16]),
            &conflict_notice,
        )
        .await?;
    let conflicts = match conflict_outcome {
        acyclic_fs::JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } => {
            assert!(!truncated, "fixture must retain the complete conflict set");
            conflicts
        }
        _ => return Err(Error::Invalid("expected a direct-child conflict".into())),
    };
    assert!(!conflicts.is_empty());
    let selections: BTreeMap<MergeConflict, ConflictSide> = conflicts
        .into_iter()
        .map(|conflict| (conflict, ConflictSide::Theirs))
        .collect();
    let resolved = root_facade
        .apply_project_merge_sides_for_child(
            host.as_ref(),
            &root_reducer,
            &child.authority,
            &child.project,
            &conflict_plan,
            OperationId::from_bytes([42; 16]),
            selections,
        )
        .await?;
    assert!(matches!(
        resolved,
        acyclic_fs::JoinOutcome::Applied(_)
            | acyclic_fs::JoinOutcome::AlreadyApplied(_)
            | acyclic_fs::JoinOutcome::NoChanges(_)
    ));

    let child_workspace = host
        .resolve(&workspace_ref(
            provider.clone(),
            &child.project.storage_name()?,
        )?)
        .await?;
    host.apply(
        &child_workspace.workspace,
        Some(&child_workspace.generation),
        &[WorkspaceMutation::PutFile {
            path: "/stale-source.txt".into(),
            bytes: b"source before stale target".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("stale-source-write")?,
    )
    .await?;
    let stale_plan = root_facade
        .prepare_project_merge_for_child(
            host.as_ref(),
            &root_reducer,
            &child.authority,
            &child.project,
        )
        .await?;
    let root_workspace = host
        .resolve(&workspace_ref(
            provider.clone(),
            &root.project.storage_name()?,
        )?)
        .await?;
    host.apply(
        &root_workspace.workspace,
        Some(&root_workspace.generation),
        &[WorkspaceMutation::PutFile {
            path: "/stale-target.txt".into(),
            bytes: b"target moved after planning".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("stale-target-write")?,
    )
    .await?;
    let stale_notice = merge_notice(host.as_ref(), &root, "notices/stale.txt").await?;
    let stale_outcome = root_facade
        .apply_project_merge_for_child_with_notice(
            host.as_ref(),
            &root_reducer,
            &child.authority,
            &child.project,
            &stale_plan,
            OperationId::from_bytes([43; 16]),
            &stale_notice,
        )
        .await?;
    assert!(matches!(
        stale_outcome,
        acyclic_fs::JoinOutcome::StaleTarget(_)
    ));
    drop(stream);
    Ok(())
}

#[tokio::test]
async fn git_lifecycle_recovery_covers_abort_rebase_and_discard() -> Result<()> {
    let (facade, workspace_id) = transition_fixture()?;
    let executor = Arc::new(LifecycleExecutor::new(workspace_id));
    facade
        .run(
            GitCommand::Branch {
                create: Some("feature".into()),
            },
            live_tree(workspace_id),
            executor.as_ref(),
        )
        .await?;
    executor.fail_once();
    assert!(
        facade
            .run(
                GitCommand::Merge {
                    branch: "feature".into(),
                },
                live_tree(workspace_id),
                executor.as_ref(),
            )
            .await
            .is_err()
    );
    let resumed = facade
        .resume(executor.as_ref())
        .await?
        .ok_or_else(|| Error::Invalid("pending merge was not recoverable".into()))?;
    assert!(matches!(
        resumed,
        acyclic_fs::GitCommandOutput::Committed(_)
    ));
    assert!(facade.resume(executor.as_ref()).await?.is_none());

    let (facade, workspace_id) = transition_fixture()?;
    let executor = Arc::new(LifecycleExecutor::new(workspace_id));
    facade
        .run(
            GitCommand::Branch {
                create: Some("feature".into()),
            },
            live_tree(workspace_id),
            executor.as_ref(),
        )
        .await?;
    executor.fail_once();
    assert!(
        facade
            .run(
                GitCommand::Merge {
                    branch: "feature".into(),
                },
                live_tree(workspace_id),
                executor.as_ref(),
            )
            .await
            .is_err()
    );
    let aborted = facade
        .run(
            GitCommand::MergeAbort,
            live_tree(workspace_id),
            executor.as_ref(),
        )
        .await?;
    assert!(matches!(
        aborted,
        acyclic_fs::GitCommandOutput::Filesystem(GitFilesystemResult::Applied { .. })
    ));
    assert!(facade.resume(executor.as_ref()).await?.is_none());

    let (facade, workspace_id) = transition_fixture()?;
    let executor = Arc::new(LifecycleExecutor::new(workspace_id));
    facade
        .run(
            GitCommand::Commit {
                message: "baseline".into(),
                author: "root".into(),
                authored_at_seconds: 100,
            },
            live_tree(workspace_id),
            executor.as_ref(),
        )
        .await?;
    facade
        .run(
            GitCommand::Branch {
                create: Some("feature".into()),
            },
            live_tree(workspace_id),
            executor.as_ref(),
        )
        .await?;
    executor.fail_once();
    assert!(
        facade
            .run(
                GitCommand::Rebase {
                    branch: "feature".into(),
                },
                live_tree(workspace_id),
                executor.as_ref(),
            )
            .await
            .is_err()
    );
    let resumed = facade
        .resume(executor.as_ref())
        .await?
        .ok_or_else(|| Error::Invalid("pending rebase was not recoverable".into()))?;
    assert!(matches!(
        resumed,
        acyclic_fs::GitCommandOutput::Committed(_)
    ));
    assert!(facade.resume(executor.as_ref()).await?.is_none());

    executor.fail_once();
    assert!(
        facade
            .run(
                GitCommand::Reset {
                    target: GitObjectName("HEAD".into()),
                    mode: GitResetMode::Hard,
                },
                live_tree(workspace_id),
                executor.as_ref(),
            )
            .await
            .is_err()
    );
    let resumed = facade
        .resume(executor.as_ref())
        .await?
        .ok_or_else(|| Error::Invalid("pending hard reset was not recoverable".into()))?;
    assert!(matches!(
        resumed,
        acyclic_fs::GitCommandOutput::Filesystem(GitFilesystemResult::Applied { .. })
    ));
    assert!(facade.resume(executor.as_ref()).await?.is_none());
    Ok(())
}
