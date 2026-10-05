//! Black-box smoke coverage for the default local Git-capable composition.
#![cfg(feature = "filesystem-local")]

use acyclic_fs::{
    ConflictSide, Digest, Fs, GenerationId, GitBranch, GitCommand, GitCompatState, GitCompatStore,
    GitFilesystemAction, GitFilesystemExecutor, GitFilesystemResult, GitObjectName, GitResetMode,
    GitTreeRef, LocalAuthorityBackend, LocalCoreStateStore, LocalObjectBackend, LocalOptions,
    MemoryGitCompatStore, MergeConflict, OperationId as FsOperationId, WorkspaceId,
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
    FilesystemGitFacade, FilesystemHost, ProjectWorkspaceTree, WorkspaceMutation, workspace_ref,
};
use acyclic_harness::fork::{CapturedResource, ForkSeed, ResourceRevision};
use acyclic_harness::model::{
    Model, ModelContent, ModelContentPart, ModelEvent, ModelProvider, ModelRequest,
};
use acyclic_harness::resources::{GenerationRef, ProviderRef, StreamRef};
use acyclic_harness::tool::{ToolExecutor, ToolInvocation};
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

async fn model_git_call(
    host: Arc<TestHost>,
    node: &ProjectNode,
    operation: u8,
    argv: &[&str],
) -> Result<serde_json::Value> {
    let workspace_id = host
        .workspace_id(node.project.storage_name()?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let workspace = Arc::new(acyclic_harness::filesystem::LocalProjectWorkspaceTree::new(
        host,
        node.project.clone(),
        &node.issuer.verifier(),
        node.scope.clone(),
    )?);
    let tool = Arc::new(acyclic_harness::filesystem::FilesystemGitTool::new(
        Arc::new(root_facade(node, workspace_id)?),
        workspace,
        node.authority.id.clone(),
        || 100,
    )?)
    .into_tool();
    let invocation = ToolInvocation::for_model_call(
        OperationId::from_bytes([operation; 16]),
        0,
        format!("git-call-{operation}"),
        acyclic_harness::filesystem::GIT_FACADE_TOOL_NAME.into(),
        json!({"argv": argv}),
    );
    let serialized = serde_json::to_vec(&invocation)
        .map_err(|error| Error::Storage(format!("Git invocation encoding failed: {error}")))?;
    let decoded: ToolInvocation = serde_json::from_slice(&serialized)
        .map_err(|error| Error::Invalid(format!("Git invocation decoding failed: {error}")))?;
    tool.executor
        .execute(decoded)
        .await
        .map(|result| result.value)
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

struct RuntimeGitSwarmProvider {
    child_operation: OperationId,
    grandchild_operation: OperationId,
    root_fork_sent: AtomicBool,
    child_fork_sent: AtomicBool,
    merge_child: AtomicBool,
    merge_root: AtomicBool,
    commit_root: AtomicBool,
    rebase_root: AtomicBool,
    reset_root: AtomicBool,
    continue_root: AtomicBool,
    abort_root: AtomicBool,
    reject_root_merge: AtomicBool,
    reject_child_sibling: AtomicBool,
    merge_child_sent: AtomicBool,
    merge_root_sent: AtomicBool,
    commit_root_sent: AtomicBool,
    rebase_root_sent: AtomicBool,
    reset_root_sent: AtomicBool,
    abort_merge_sent: AtomicBool,
    continue_sent: AtomicBool,
    abort_sent: AtomicBool,
    reject_root_sent: AtomicBool,
    reject_child_sibling_sent: AtomicBool,
}

impl RuntimeGitSwarmProvider {
    fn new(child_operation: OperationId, grandchild_operation: OperationId) -> Arc<Self> {
        Arc::new(Self {
            child_operation,
            grandchild_operation,
            root_fork_sent: AtomicBool::new(false),
            child_fork_sent: AtomicBool::new(false),
            merge_child: AtomicBool::new(false),
            merge_root: AtomicBool::new(false),
            commit_root: AtomicBool::new(false),
            rebase_root: AtomicBool::new(false),
            reset_root: AtomicBool::new(false),
            continue_root: AtomicBool::new(false),
            abort_root: AtomicBool::new(false),
            reject_root_merge: AtomicBool::new(false),
            reject_child_sibling: AtomicBool::new(false),
            merge_child_sent: AtomicBool::new(false),
            merge_root_sent: AtomicBool::new(false),
            commit_root_sent: AtomicBool::new(false),
            rebase_root_sent: AtomicBool::new(false),
            reset_root_sent: AtomicBool::new(false),
            abort_merge_sent: AtomicBool::new(false),
            continue_sent: AtomicBool::new(false),
            abort_sent: AtomicBool::new(false),
            reject_root_sent: AtomicBool::new(false),
            reject_child_sibling_sent: AtomicBool::new(false),
        })
    }

    fn task(request: &ModelRequest) -> Option<String> {
        request.messages.iter().find_map(|message| {
            let ModelContent::Text(text) = &message.content else {
                return None;
            };
            let suffix = text.strip_prefix("child task: ")?;
            Some(suffix.split(';').next()?.to_owned())
        })
    }
}

impl ModelProvider for RuntimeGitSwarmProvider {
    fn generate<'a>(
        &'a self,
        prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let request = prepared.request();
        let task = Self::task(request);
        let root = task.is_none();
        let events = if root && !self.root_fork_sent.swap(true, Ordering::SeqCst) {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-fork-child".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": self.child_operation.to_string(),
                        "task": "runtime-child",
                        "prompt": "integrate the runtime grandchild"
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if task.as_deref() == Some("runtime-child")
            && !self.child_fork_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-fork-grandchild".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": self.grandchild_operation.to_string(),
                        "task": "runtime-grandchild",
                        "prompt": "write the final runtime file"
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if task.as_deref() == Some("runtime-child")
            && self.reject_child_sibling.load(Ordering::SeqCst)
            && !self.reject_child_sibling_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-reject-child-sibling".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["merge", "sibling"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if task.as_deref() == Some("runtime-child")
            && self.merge_child.load(Ordering::SeqCst)
            && !self.merge_child_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-merge-grandchild".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["merge", "grandchild"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if root
            && self.commit_root.load(Ordering::SeqCst)
            && !self.commit_root_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-commit-root".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["commit", "-m", "runtime baseline"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if root
            && self.rebase_root.load(Ordering::SeqCst)
            && !self.rebase_root_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-rebase-root".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["rebase", "child"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if root
            && self.reset_root.load(Ordering::SeqCst)
            && !self.reset_root_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-reset-root".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["reset", "--hard", "HEAD"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if root
            && self.abort_root.load(Ordering::SeqCst)
            && !self.abort_merge_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-merge-child-for-abort".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["merge", "child"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if root
            && self.continue_root.load(Ordering::SeqCst)
            && !self.continue_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-merge-continue".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["merge", "--continue"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if root
            && self.abort_root.load(Ordering::SeqCst)
            && !self.abort_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-merge-abort".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["merge", "--abort"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if root
            && self.merge_root.load(Ordering::SeqCst)
            && !self.merge_root_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-merge-child".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["merge", "child"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else if root
            && self.reject_root_merge.load(Ordering::SeqCst)
            && !self.reject_root_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "runtime-reject-grandchild".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["merge", "grandchild"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        } else {
            vec![
                Ok(ModelEvent::Content {
                    delta: "runtime-ready".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]
        };
        Box::pin(stream::iter(events))
    }
}

fn project_from_seed(seed: &ForkSeed) -> Result<VolumeRef> {
    seed.resources
        .iter()
        .find_map(|resource| match &resource.revision {
            ResourceRevision::Project { volume, .. } => Some(volume.clone()),
            _ => None,
        })
        .ok_or_else(|| Error::Invalid("runtime fork seed has no project revision".into()))
}

async fn install_git_branch(
    root: &std::path::Path,
    parent: WorkspaceId,
    name: &str,
    source: WorkspaceId,
) -> Result<()> {
    let store = LocalCoreStateStore::new(root.join("git"));
    let current = store
        .load(parent)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let (expected, mut state) = current
        .map(|state| (state.revision, state))
        .unwrap_or_else(|| (0, GitCompatState::new("main", parent)));
    state.branches.insert(
        name.to_owned(),
        GitBranch {
            name: name.to_owned(),
            workspace_id: source,
            head: None,
            tracked_paths: Default::default(),
        },
    );
    if expected != 0 {
        state.revision = expected + 1;
    }
    let replaced = store
        .compare_and_swap(parent, expected, state)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    if !replaced {
        return Err(Error::Conflict("runtime Git branch state raced".into()));
    }
    Ok(())
}

async fn mark_git_tracked_paths(
    root: &std::path::Path,
    workspace: WorkspaceId,
    branch: &str,
    paths: &[&str],
) -> Result<()> {
    let store = LocalCoreStateStore::new(root.join("git"));
    let current = store
        .load(workspace)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let (expected, mut state) = current
        .map(|state| (state.revision, state))
        .ok_or_else(|| Error::NotFound(format!("runtime Git workspace {workspace}")))?;
    let tracked = state
        .branches
        .get_mut(branch)
        .ok_or_else(|| Error::NotFound(format!("runtime Git branch {branch}")))?;
    tracked
        .tracked_paths
        .extend(paths.iter().map(|path| (*path).to_owned()));
    if expected != 0 {
        state.revision = expected + 1;
    }
    let replaced = store
        .compare_and_swap(workspace, expected, state)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    if !replaced {
        return Err(Error::Conflict("runtime Git tracked paths raced".into()));
    }
    Ok(())
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
async fn local_git_adapter_uses_active_workspace_after_branch_switch() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new("git-active-branch", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(
        Fs::local(LocalOptions::new(directory.path().join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let root = project_node(host.as_ref(), &provider, "root", "root", 41, true).await?;
    let tree = acyclic_harness::filesystem::LocalProjectWorkspaceTree::new(
        host,
        root.project,
        &root.issuer.verifier(),
        root.scope,
    )?;
    let source_tree = tree.current_tree().await?;
    let branch = tree
        .execute(
            FsOperationId::from_bytes([0xA1; 16]),
            &GitFilesystemAction::ForkBranch {
                branch: "feature".into(),
                source_tree,
                head: None,
                switch: true,
            },
        )
        .await?;
    let branch_workspace = match branch {
        GitFilesystemResult::Forked { workspace_id } => workspace_id,
        other => {
            return Err(Error::Invalid(format!(
                "branch fork returned unexpected result: {other:?}"
            )));
        }
    };
    let branch_tree = tree.current_tree().await?;
    assert_eq!(branch_tree.workspace_id(), branch_workspace);
    let result = tree
        .execute(
            FsOperationId::from_bytes([0xA2; 16]),
            &GitFilesystemAction::ApplyPatch {
                patch: b"--- /dev/null\n+++ b/feature.txt\n@@ -0,0 +1 @@\n+feature\n".to_vec(),
                expected_workspace_tree: Some(branch_tree),
            },
        )
        .await?;
    match result {
        GitFilesystemResult::Applied {
            tree: Some(tree), ..
        } => {
            assert_eq!(tree.workspace_id(), branch_workspace);
        }
        other => {
            return Err(Error::Invalid(format!(
                "active branch patch returned unexpected result: {other:?}"
            )));
        }
    }
    Ok(())
}

#[tokio::test]
async fn model_git_boundary_rejects_bare_git_prefix() -> Result<()> {
    let (facade, workspace_id) = transition_fixture()?;
    let executor = LifecycleExecutor::new(workspace_id);
    let error = facade
        .run_argv(
            &["git".into(), "status".into()],
            live_tree(workspace_id),
            "test-agent",
            100,
            &executor,
        )
        .await
        .expect_err("bare system Git must remain outside the acyclic facade");
    assert!(
        matches!(error, Error::Invalid(message) if message.contains("unsupported Git command 'git'"))
    );
    assert_eq!(
        acyclic_harness::filesystem::FilesystemGitTool::<MemoryGitCompatStore>::definition().name,
        acyclic_harness::filesystem::GIT_FACADE_TOOL_NAME
    );
    Ok(())
}

#[tokio::test]
async fn default_runtime_git_merges_only_through_explicit_authenticated_commands() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let child_operation = OperationId::from_bytes([0xE1; 16]);
    let grandchild_operation = OperationId::from_bytes([0xE2; 16]);
    let provider = RuntimeGitSwarmProvider::new(child_operation, grandchild_operation);
    let model = Model::new("mock", "git-default-runtime", "1", json!({}))?;
    let swarm = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        directory.path(),
        model.clone(),
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let root_output = swarm
        .run_root(
            OperationId::from_bytes([0xE0; 16]),
            "prepare a recursive project change",
        )
        .await?;
    assert_eq!(root_output.text, "runtime-ready");
    let child_task = acyclic_harness::TaskId::from_bytes(child_operation.into_bytes());
    let grandchild_task = acyclic_harness::TaskId::from_bytes(grandchild_operation.into_bytes());
    assert_eq!(swarm.sessions().await?.len(), 3);
    assert_eq!(swarm.outcome(child_task).await?.text, "runtime-ready");
    assert_eq!(swarm.outcome(grandchild_task).await?.text, "runtime-ready");

    let filesystem_provider = ProviderRef::new("local", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(
        Fs::local(LocalOptions::new(directory.path().join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        filesystem_provider,
    )?);
    let child_project = project_from_seed(&swarm.published_seed(child_task).await?)?;
    let grandchild_project = project_from_seed(&swarm.published_seed(grandchild_task).await?)?;
    let root_project = VolumeRef::new(
        host.provider().clone(),
        "local-project",
        VolumeClass::Project,
        VolumeOwner::Project("local-swarm".into()),
    )?;
    let grandchild_workspace =
        workspace_ref(host.provider().clone(), &grandchild_project.storage_name()?)?;
    let grandchild_head = host.resolve(&grandchild_workspace).await?;
    host.apply(
        &grandchild_workspace,
        Some(&grandchild_head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/runtime-final.txt".into(),
            bytes: b"grandchild authored this exact file".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("runtime-grandchild-edit")?,
    )
    .await?;
    let root_workspace = workspace_ref(host.provider().clone(), &root_project.storage_name()?)?;
    assert!(
        host.read(&root_workspace, None, "/runtime-final.txt", 1_024)
            .await
            .is_err()
    );

    let child_workspace_id = host
        .workspace_id(child_project.storage_name()?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let grandchild_workspace_id = host
        .workspace_id(grandchild_project.storage_name()?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let root_workspace_id = host
        .workspace_id(root_project.storage_name()?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    install_git_branch(
        directory.path(),
        child_workspace_id,
        "grandchild",
        grandchild_workspace_id,
    )
    .await?;
    // A child must not be able to route a branch pointing at an unrelated
    // sibling or ancestor workspace merely because its Git state names it.
    // The model-visible argv path must reject this before any filesystem join.
    install_git_branch(
        directory.path(),
        child_workspace_id,
        "sibling",
        root_workspace_id,
    )
    .await?;
    provider.reject_child_sibling.store(true, Ordering::SeqCst);
    assert!(
        swarm
            .run(
                child_task,
                OperationId::from_bytes([0xEC; 16]),
                "attempt a forbidden child to sibling merge",
            )
            .await
            .is_err()
    );
    provider.reject_child_sibling.store(false, Ordering::SeqCst);
    provider.merge_child.store(true, Ordering::SeqCst);
    swarm
        .run(
            child_task,
            OperationId::from_bytes([0xE3; 16]),
            "merge the completed grandchild project",
        )
        .await?;
    assert_eq!(
        host.read(
            &workspace_ref(host.provider().clone(), &child_project.storage_name()?)?,
            None,
            "/runtime-final.txt",
            1_024,
        )
        .await?,
        b"grandchild authored this exact file"[..]
    );
    assert!(
        host.read(&root_workspace, None, "/runtime-final.txt", 1_024)
            .await
            .is_err()
    );

    install_git_branch(
        directory.path(),
        root_workspace_id,
        "child",
        child_workspace_id,
    )
    .await?;

    let child_workspace = workspace_ref(host.provider().clone(), &child_project.storage_name()?)?;

    // Exercise the default model-facing commit and rebase routes before
    // creating the conflict fixture. These commands still resolve to the
    // authenticated direct-parent Filesystem join.
    let root_head = host.resolve(&root_workspace).await?;
    host.apply(
        &root_workspace,
        Some(&root_head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/runtime-baseline.txt".into(),
            bytes: b"baseline".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("runtime-root-baseline")?,
    )
    .await?;
    mark_git_tracked_paths(
        directory.path(),
        root_workspace_id,
        "main",
        &["runtime-baseline.txt"],
    )
    .await?;
    provider.commit_root.store(true, Ordering::SeqCst);
    swarm
        .run_root(
            OperationId::from_bytes([0xE9; 16]),
            "commit the runtime baseline",
        )
        .await?;
    let child_head = host.resolve(&child_workspace).await?;
    host.apply(
        &child_workspace,
        Some(&child_head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/runtime-rebase.txt".into(),
            bytes: b"rebased child change".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("runtime-child-rebase-edit")?,
    )
    .await?;
    mark_git_tracked_paths(
        directory.path(),
        root_workspace_id,
        "child",
        &["runtime-rebase.txt"],
    )
    .await?;
    provider.rebase_root.store(true, Ordering::SeqCst);
    swarm
        .run_root(
            OperationId::from_bytes([0xEA; 16]),
            "rebase the root onto the child workspace",
        )
        .await?;
    assert_eq!(
        host.read(&root_workspace, None, "/runtime-rebase.txt", 1_024)
            .await?,
        b"rebased child change"[..]
    );

    // A hard reset is the explicit discard operation. Mutating a tracked
    // path after the rebase must be undone by the typed reset action.
    let root_head = host.resolve(&root_workspace).await?;
    host.apply(
        &root_workspace,
        Some(&root_head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/runtime-baseline.txt".into(),
            bytes: b"dirty baseline".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("runtime-root-dirty-edit")?,
    )
    .await?;
    provider.reset_root.store(true, Ordering::SeqCst);
    swarm
        .run_root(
            OperationId::from_bytes([0xEB; 16]),
            "discard the dirty tracked workspace edit",
        )
        .await?;
    assert_eq!(
        host.read(&root_workspace, None, "/runtime-baseline.txt", 1_024)
            .await?,
        b"baseline"[..]
    );

    // Make the direct child/root merge genuinely conflicted. Both workspaces
    // share the original project base for this path, so the existing
    // Filesystem three-way join must leave a durable pending transition.
    let child_head = host.resolve(&child_workspace).await?;
    host.apply(
        &child_workspace,
        Some(&child_head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/runtime-conflict.txt".into(),
            bytes: b"child side".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("runtime-child-conflict-edit")?,
    )
    .await?;
    let root_head = host.resolve(&root_workspace).await?;
    host.apply(
        &root_workspace,
        Some(&root_head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/runtime-conflict.txt".into(),
            bytes: b"root side".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("runtime-root-conflict-edit")?,
    )
    .await?;
    provider.merge_root.store(true, Ordering::SeqCst);
    assert!(
        swarm
            .run_root(
                OperationId::from_bytes([0xE4; 16]),
                "attempt the conflicted child project merge",
            )
            .await
            .is_err()
    );
    assert_eq!(
        host.read(&root_workspace, None, "/runtime-conflict.txt", 1_024)
            .await?,
        b"root side"[..]
    );
    assert!(
        host.read(&root_workspace, None, "/runtime-final.txt", 1_024)
            .await
            .is_err()
    );

    // The failed model turn leaves the compatibility transition durable. Drop
    // the swarm and reopen it before continuing, proving that recovery uses
    // the persisted transition rather than replaying the failed join.
    let resumed_provider = RuntimeGitSwarmProvider::new(child_operation, grandchild_operation);
    resumed_provider
        .root_fork_sent
        .store(true, Ordering::SeqCst);
    resumed_provider
        .child_fork_sent
        .store(true, Ordering::SeqCst);
    resumed_provider
        .merge_root_sent
        .store(true, Ordering::SeqCst);
    resumed_provider.continue_root.store(true, Ordering::SeqCst);
    drop(swarm);
    let provider = resumed_provider;
    let swarm = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        directory.path(),
        model.clone(),
        provider.clone(),
        Limits::default(),
    )
    .await?;

    let root_head = host.resolve(&root_workspace).await?;
    host.apply(
        &root_workspace,
        Some(&root_head.generation),
        &[
            WorkspaceMutation::PutFile {
                path: "/runtime-conflict.txt".into(),
                bytes: b"child side".to_vec(),
            },
            // Continue captures the current target generation. Resolve the
            // conflict and stage the non-conflicting child file in the same
            // explicit workspace edit before admitting that continuation.
            WorkspaceMutation::PutFile {
                path: "/runtime-final.txt".into(),
                bytes: b"grandchild authored this exact file".to_vec(),
            },
        ],
        &acyclic_harness::IdempotencyKey::new("runtime-root-conflict-resolve")?,
    )
    .await?;
    provider.continue_root.store(true, Ordering::SeqCst);
    swarm
        .run_root(
            OperationId::from_bytes([0xE6; 16]),
            "continue the resolved child project merge",
        )
        .await?;
    assert_eq!(
        host.read(&root_workspace, None, "/runtime-conflict.txt", 1_024)
            .await?,
        b"child side"[..]
    );

    // A second conflict exercises the explicit abort path. The durable
    // compatibility layer restores the exact target generation captured by
    // the pending merge, leaving the child workspace untouched.
    let child_head = host.resolve(&child_workspace).await?;
    host.apply(
        &child_workspace,
        Some(&child_head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/runtime-abort.txt".into(),
            bytes: b"child abort side".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("runtime-child-abort-edit")?,
    )
    .await?;
    let root_head = host.resolve(&root_workspace).await?;
    host.apply(
        &root_workspace,
        Some(&root_head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/runtime-abort.txt".into(),
            bytes: b"root abort side".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("runtime-root-abort-edit")?,
    )
    .await?;
    provider.merge_root_sent.store(false, Ordering::SeqCst);
    provider.abort_root.store(true, Ordering::SeqCst);
    assert!(
        swarm
            .run_root(
                OperationId::from_bytes([0xE7; 16]),
                "attempt a merge that will be aborted",
            )
            .await
            .is_err()
    );
    swarm
        .run_root(
            OperationId::from_bytes([0xE8; 16]),
            "abort the conflicted child project merge",
        )
        .await?;
    assert!(
        host.read(&root_workspace, None, "/runtime-abort.txt", 1_024)
            .await
            .is_err()
    );
    assert_eq!(
        host.read(&child_workspace, None, "/runtime-abort.txt", 1_024)
            .await?,
        b"child abort side"[..]
    );

    // The successful, non-conflicted path remains covered after recovery.
    provider.merge_root.store(false, Ordering::SeqCst);
    provider.abort_root.store(false, Ordering::SeqCst);
    provider.continue_root.store(false, Ordering::SeqCst);
    provider.merge_root_sent.store(true, Ordering::SeqCst);
    assert_eq!(
        host.read(&root_workspace, None, "/runtime-final.txt", 1_024)
            .await?,
        b"grandchild authored this exact file"[..]
    );

    install_git_branch(
        directory.path(),
        root_workspace_id,
        "grandchild",
        grandchild_workspace_id,
    )
    .await?;
    provider.reject_root_merge.store(true, Ordering::SeqCst);
    assert!(
        swarm
            .run_root(
                OperationId::from_bytes([0xE5; 16]),
                "attempt a forbidden grandchild to root merge",
            )
            .await
            .is_err()
    );
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
    for (node, operation) in [(&root, 60), (&child, 61), (&grandchild, 62)] {
        let status = model_git_call(host.clone(), node, operation, &["status"]).await?;
        assert!(status.get("output").is_some());
        let diff = model_git_call(host.clone(), node, operation + 10, &["diff"]).await?;
        assert!(diff.get("output").is_some());
    }
    let read_only_scope = root.issuer.root_for_agent(
        root.agent,
        "read-only-git",
        acyclic_harness::Capabilities::new([root.project.capability(VolumeOperation::Read)?]),
    );
    assert!(
        acyclic_harness::filesystem::LocalProjectWorkspaceTree::new(
            host.clone(),
            root.project.clone(),
            &root.issuer.verifier(),
            read_only_scope,
        )
        .is_err(),
        "model-visible Git binding must reject a scope without project write"
    );
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
