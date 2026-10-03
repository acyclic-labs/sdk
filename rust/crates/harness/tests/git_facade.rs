//! Black-box authority and delegation coverage for the Harness Git facade.
#![cfg(feature = "filesystem")]

use acyclic_fs::{
    Digest, Fs, GenerationId, GitCommand, GitFilesystemAction, GitFilesystemExecutor,
    GitFilesystemResult, GitTreeRef, MemoryGitCompatStore, OperationId as FsOperationId,
    WorkspaceId,
};
use acyclic_harness::filesystem::{RootWritebackApproval, RootWritebackRequest};
use acyclic_harness::model::ModelToolContext;
use acyclic_harness::resources::GenerationRef;
use acyclic_harness::tool::{
    Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry, ToolResult,
};
use acyclic_harness::{
    AgentId, Capabilities, Error, Result,
    conversation::{
        ConversationMessage, FileDescriptor, FileRef, MessageKind, ReferencedAttachments,
        VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry},
    filesystem::{FilesystemGitFacade, FilesystemHost},
    merge::{ProjectConflictSelection, ProjectJoinOutcome, ProjectJoinPlan},
    resources::ProviderRef,
};
use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, thiserror::Error)]
#[error("test executor failed")]
struct TestExecutorError;

struct NoopExecutor;

impl GitFilesystemExecutor for NoopExecutor {
    type Error = TestExecutorError;

    async fn validate_workspace_tree(
        &self,
        _workspace_tree: GitTreeRef,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn execute(
        &self,
        _operation_id: FsOperationId,
        _action: &GitFilesystemAction,
    ) -> std::result::Result<GitFilesystemResult, Self::Error> {
        Ok(GitFilesystemResult::Applied {
            tree: None,
            tracked_paths: None,
        })
    }
}

/// Minimal model-facing adapter used by the black-box facade tests.
///
/// Production composition owns registration and policy, but every adapter must
/// keep the model-facing surface small: argv is schema-bound, the live tree is
/// supplied by the host, and execution is delegated to the authenticated
/// facade. No shell or process boundary is introduced here.
struct ModelGitFacadeTool {
    facade: FilesystemGitFacade<MemoryGitCompatStore>,
    workspace_id: WorkspaceId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelGitArguments {
    argv: Vec<String>,
}

impl ToolExecutor for ModelGitFacadeTool {
    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let arguments: ModelGitArguments = serde_json::from_value(invocation.arguments)
                .map_err(|error| Error::Invalid(format!("invalid git tool arguments: {error}")))?;
            let output = self
                .facade
                .run_argv(
                    &arguments.argv,
                    live_tree(self.workspace_id),
                    "root",
                    100,
                    &NoopExecutor,
                )
                .await?;
            Ok(ToolResult {
                value: serde_json::to_value(output).map_err(|error| {
                    Error::Storage(format!("git output encoding failed: {error}"))
                })?,
            })
        })
    }

    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

impl ToolProjection for ModelGitFacadeTool {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(result.value.clone())
    }
}

fn model_git_definition() -> ToolDefinition {
    ToolDefinition {
        name: "filesystem.git".into(),
        revision: "1".into(),
        description: "Run one typed acyclic git command in the caller's workspace.".into(),
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["argv"],
            "properties": {
                "argv": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 32,
                    "items": {"type": "string", "maxLength": 255}
                }
            }
        }),
        output_schema: json!({"type": "object"}),
    }
}

struct FaultExecutor {
    workspace_id: WorkspaceId,
    fail_next: AtomicBool,
    operations: Mutex<Vec<FsOperationId>>,
}

impl FaultExecutor {
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

    fn operations(&self) -> Vec<FsOperationId> {
        self.operations.lock().expect("operation lock").clone()
    }
}

impl GitFilesystemExecutor for FaultExecutor {
    type Error = TestExecutorError;

    async fn validate_workspace_tree(
        &self,
        _workspace_tree: GitTreeRef,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn execute(
        &self,
        operation_id: FsOperationId,
        action: &GitFilesystemAction,
    ) -> std::result::Result<GitFilesystemResult, Self::Error> {
        self.operations
            .lock()
            .expect("operation lock")
            .push(operation_id);
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(TestExecutorError);
        }
        let result = match action {
            GitFilesystemAction::CaptureCommit { workspace_tree, .. } => {
                GitFilesystemResult::Captured {
                    tree: *workspace_tree,
                    tracked_paths: BTreeSet::new(),
                    proof: None,
                }
            }
            GitFilesystemAction::ForkBranch { .. } => GitFilesystemResult::Forked {
                workspace_id: WorkspaceId::from_bytes([31; 16]),
            },
            _ => GitFilesystemResult::Applied {
                tree: Some(live_tree(self.workspace_id)),
                tracked_paths: None,
            },
        };
        Ok(result)
    }
}

struct ApprovalPlan {
    source: GenerationRef,
    target: GenerationRef,
    project: VolumeRef,
}

impl ProjectJoinPlan for ApprovalPlan {
    fn source_generation(&self) -> &GenerationRef {
        &self.source
    }

    fn expected_target_generation(&self) -> &GenerationRef {
        &self.target
    }

    fn target_project(&self) -> Option<&VolumeRef> {
        Some(&self.project)
    }

    fn apply<'a>(
        &'a self,
        _scope: &'a acyclic_harness::core::Scope,
        _operation_id: acyclic_harness::OperationId,
        _child: &'a Authority,
        _notice: &'a ConversationMessage,
        _selections: &'a [ProjectConflictSelection],
    ) -> BoxFuture<'a, Result<ProjectJoinOutcome>> {
        Box::pin(async { Err(Error::Conflict("approval guard was bypassed".into())) })
    }
}

fn fixture(
    writable: bool,
) -> Result<(
    FilesystemGitFacade<MemoryGitCompatStore>,
    WorkspaceId,
    AuthorityIssuer,
)> {
    let provider = ProviderRef::new("acyclic", "filesystem", "2")?;
    let volume = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("root".into()),
    )?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "root".into(),
    };
    let issuer = AuthorityIssuer::new("git-facade", [7; 32], authority);
    let mut grants = vec![volume.capability(VolumeOperation::Read)?];
    if writable {
        grants.push(volume.capability(VolumeOperation::Write)?);
        grants.push("project:merge".into());
    }
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([8; 16]),
        "root",
        Capabilities::new(grants),
    );
    let workspace_id = WorkspaceId::from_bytes([9; 16]);
    let facade = FilesystemGitFacade::new(
        workspace_id,
        MemoryGitCompatStore::new(),
        volume,
        issuer.verifier(),
        scope,
    )?;
    Ok((facade, workspace_id, issuer))
}

fn writer_without_merge_fixture() -> Result<(FilesystemGitFacade<MemoryGitCompatStore>, WorkspaceId)>
{
    let provider = ProviderRef::new("git-facade-authority", "filesystem", "2")?;
    let volume = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("root".into()),
    )?;
    let issuer = AuthorityIssuer::new(
        "git-facade-authority",
        [63; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "root".into(),
        },
    );
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([64; 16]),
        "root",
        Capabilities::new([
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let workspace_id = WorkspaceId::from_bytes([65; 16]);
    Ok((
        FilesystemGitFacade::new(
            workspace_id,
            MemoryGitCompatStore::new(),
            volume,
            issuer.verifier(),
            scope,
        )?,
        workspace_id,
    ))
}

fn live_tree(workspace_id: WorkspaceId) -> GitTreeRef {
    GitTreeRef::exact(workspace_id, GenerationId::new(Digest::from_bytes([1; 32])))
}

fn transition_facade() -> Result<(FilesystemGitFacade<MemoryGitCompatStore>, WorkspaceId)> {
    let provider = ProviderRef::new("git-facade-recovery", "filesystem", "2")?;
    let volume = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("root".into()),
    )?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "root".into(),
    };
    let issuer = AuthorityIssuer::new("git-facade-recovery", [41; 32], authority);
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([42; 16]),
        "root",
        Capabilities::new([
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
            "fork:publish".to_owned(),
            "project:merge".to_owned(),
        ]),
    );
    let workspace_id = WorkspaceId::from_bytes([43; 16]);
    Ok((
        FilesystemGitFacade::new(
            workspace_id,
            MemoryGitCompatStore::new(),
            volume,
            issuer.verifier(),
            scope,
        )?,
        workspace_id,
    ))
}

#[tokio::test]
async fn read_commands_delegate_to_durable_compat_repository() -> Result<()> {
    let (facade, workspace_id, _) = fixture(false)?;
    let output = facade
        .run_argv(
            &["status".into()],
            live_tree(workspace_id),
            "root",
            100,
            &NoopExecutor,
        )
        .await?;
    assert!(matches!(output, acyclic_fs::GitCommandOutput::Status(_)));
    Ok(())
}

#[tokio::test]
async fn model_facing_git_tool_routes_through_pinned_facade_and_provenance() -> Result<()> {
    let (facade, workspace_id, _) = fixture(false)?;
    let tool = Arc::new(ModelGitFacadeTool {
        facade,
        workspace_id,
    });
    let definition = model_git_definition();
    definition.validate()?;
    let definition_digest = definition.digest()?;
    assert_eq!(definition_digest, definition.digest()?);
    let mut registry = ToolRegistry::new();
    registry.register(Tool {
        definition: definition.clone(),
        executor: tool.clone(),
        projection: tool.clone(),
    })?;
    assert_eq!(registry.definitions()?, vec![definition.clone()]);

    let parent_operation = acyclic_harness::OperationId::from_bytes([61; 16]);
    let context = ModelToolContext {
        parent_operation,
        step: 0,
    };
    let invocation = ToolInvocation::for_model_call(
        parent_operation,
        0,
        "git-status-1".into(),
        definition.name.clone(),
        json!({"argv": ["status"]}),
    );
    context.validate_invocation(&invocation)?;
    let registered = registry
        .get(&definition.name)
        .ok_or_else(|| Error::NotFound("filesystem.git".into()))?;
    let result = registered
        .executor
        .execute_in_model_batch(context, invocation.clone())
        .await?;
    let projected = registered.projection.project(&invocation, &result)?;
    assert!(projected.get("Status").is_some());

    // Model provenance is part of the admission boundary. A forged operation
    // identity must fail before the facade sees a command.
    let forged = ToolInvocation {
        operation_id: acyclic_harness::OperationId::from_bytes([62; 16]),
        ..invocation
    };
    assert!(
        registered
            .executor
            .execute_in_model_batch(context, forged)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn mutating_commands_require_the_exact_volume_write_capability() -> Result<()> {
    let (facade, workspace_id, _) = fixture(false)?;
    let error = facade
        .run(
            GitCommand::Commit {
                message: "should be denied".into(),
                author: "root".into(),
                authored_at_seconds: 100,
            },
            live_tree(workspace_id),
            &NoopExecutor,
        )
        .await
        .err()
        .ok_or_else(|| Error::Invalid("read-only scope executed a commit".into()))?;
    assert!(matches!(error, Error::Unauthorized(_)));
    Ok(())
}

#[tokio::test]
async fn merge_lifecycle_commands_require_parent_merge_authority() -> Result<()> {
    let (facade, workspace_id) = writer_without_merge_fixture()?;
    for command in [
        GitCommand::Merge {
            branch: "feature".into(),
        },
        GitCommand::MergeContinue,
        GitCommand::MergeAbort,
        GitCommand::Rebase {
            branch: "feature".into(),
        },
    ] {
        let error = facade
            .run(command, live_tree(workspace_id), &NoopExecutor)
            .await
            .err()
            .ok_or_else(|| Error::Invalid("merge lifecycle command bypassed authority".into()))?;
        assert!(matches!(error, Error::Unsupported(value) if value == "project:merge"));
    }
    Ok(())
}

#[tokio::test]
async fn branch_workspace_transitions_require_parent_fork_authority() -> Result<()> {
    let (facade, workspace_id, _) = fixture(true)?;
    let error = facade
        .run(
            GitCommand::Switch {
                branch: "child".into(),
                create: false,
            },
            live_tree(workspace_id),
            &NoopExecutor,
        )
        .await
        .err()
        .ok_or_else(|| Error::Invalid("project writer switched without fork authority".into()))?;
    assert!(matches!(error, Error::Unsupported(value) if value == "fork:publish"));
    Ok(())
}

#[tokio::test]
async fn root_writeback_requires_authenticated_scope_binding() -> Result<()> {
    let provider = ProviderRef::new("git-facade-approval", "filesystem", "2")?;
    let volume = VolumeRef::new(
        provider.clone(),
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("root".into()),
    )?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "root".into(),
    };
    let issuer = AuthorityIssuer::new("git-facade-approval", [11; 32], authority.clone());
    let agent = AgentId::from_bytes([12; 16]);
    let capabilities = Capabilities::new([
        volume.capability(VolumeOperation::Read)?,
        volume.capability(VolumeOperation::Write)?,
        "project:writeback".into(),
    ]);
    let scope = issuer.root_for_agent(agent, "root", capabilities.clone());
    let facade = FilesystemGitFacade::new(
        WorkspaceId::from_bytes([13; 16]),
        MemoryGitCompatStore::new(),
        volume.clone(),
        issuer.verifier(),
        scope.clone(),
    )?;
    let source = GenerationRef::new(provider.clone(), [1; 32], Some("source".into()))?;
    let target = GenerationRef::new(provider, [2; 32], Some("target".into()))?;
    let operation_id = acyclic_harness::OperationId::from_bytes([14; 16]);
    let approval = RootWritebackApproval::issue(
        &issuer.verifier(),
        &scope,
        volume.clone(),
        operation_id,
        source.clone(),
        target.clone(),
    )?;
    let other_scope = issuer.root_for_agent(agent, "different-approval", capabilities);
    let request = RootWritebackRequest::new(approval, other_scope);
    let notice_file = FileRef::new(
        volume.clone(),
        "notice.txt",
        "notice",
        FileDescriptor::from_bytes(b"notice", "text/plain")?,
        "notice.txt",
    )?;
    let notice = ConversationMessage {
        id: uuid::Uuid::from_bytes([15; 16]),
        sequence: 1,
        kind: MessageKind::Merge,
        content: notice_file,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: None,
        extensions: Default::default(),
    };
    let plan = ApprovalPlan {
        source,
        target,
        project: volume.clone(),
    };
    let mut malformed_notice = notice.clone();
    malformed_notice.kind = MessageKind::User;
    let valid_request = RootWritebackRequest::new(approval.clone(), scope.clone());
    let error = facade
        .apply_root_writeback(&valid_request, &plan, &authority, &malformed_notice, &[])
        .await
        .err()
        .ok_or_else(|| Error::Invalid("malformed writeback notice was accepted".into()))?;
    assert!(matches!(error, Error::Invalid(_)));
    let error = facade
        .apply_root_writeback(&valid_request, &plan, &authority, &notice, &[])
        .await
        .err()
        .ok_or_else(|| Error::Invalid("unbound writeback plan was accepted".into()))?;
    assert!(matches!(error, Error::Unauthorized(_)));
    let error = facade
        .apply_root_writeback(&request, &plan, &authority, &notice, &[])
        .await
        .err()
        .ok_or_else(|| Error::Invalid("approval data authorized a different scope".into()))?;
    assert!(matches!(error, Error::Unauthorized(_)));

    let no_writeback = issuer.root_for_agent(
        agent,
        "without-writeback",
        Capabilities::new([
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let error = RootWritebackApproval::issue(
        &issuer.verifier(),
        &no_writeback,
        volume.clone(),
        operation_id,
        GenerationRef::new(
            ProviderRef::new("git-facade-approval", "filesystem", "2")?,
            [3; 32],
            Some("source".into()),
        )?,
        GenerationRef::new(
            ProviderRef::new("git-facade-approval", "filesystem", "2")?,
            [4; 32],
            Some("target".into()),
        )?,
    )
    .err()
    .ok_or_else(|| Error::Invalid("writeback approval omitted explicit capability".into()))?;
    assert!(matches!(error, Error::Unsupported(value) if value == "project:writeback"));
    Ok(())
}

#[tokio::test]
async fn lifecycle_fork_uses_parent_controller_and_real_filesystem_state() -> Result<()> {
    let provider = ProviderRef::new("git-facade-native", "filesystem", "2")?;
    let host = FilesystemHost::new(Fs::memory(), provider.clone())?;
    let project = VolumeRef::new(
        provider.clone(),
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("root".into()),
    )?;
    let child = VolumeRef::new(
        provider,
        "child-project",
        VolumeClass::Project,
        VolumeOwner::Project("root".into()),
    )?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "root".into(),
    };
    let issuer = AuthorityIssuer::new("git-facade-native", [21; 32], authority.clone());
    let agent = AgentId::from_bytes([22; 16]);
    let scope = issuer.root_for_agent(
        agent,
        "root",
        Capabilities::new([
            "conversation:bind".into(),
            "fork:publish".into(),
            project.capability(VolumeOperation::Read)?,
            project.capability(VolumeOperation::Write)?,
        ]),
    );
    let facade = FilesystemGitFacade::new(
        WorkspaceId::from_bytes([23; 16]),
        MemoryGitCompatStore::new(),
        project.clone(),
        issuer.verifier(),
        scope.clone(),
    )?;
    let parent_head = host.create_volume(&project).await?;
    let mut reducer = Reducer::new(authority, issuer.verifier(), SchemaRegistry::new());
    reducer.apply(Command {
        operation_id: acyclic_harness::OperationId::from_bytes([24; 16]),
        idempotency_key: acyclic_harness::IdempotencyKey::new("bind")?,
        expected_revision: 0,
        scope,
        causal_parent: None,
        action: Action::BindConversation { agent },
    })?;
    let child_head = facade
        .fork_project(
            &host,
            &reducer,
            &parent_head.generation,
            &child,
            &acyclic_harness::IdempotencyKey::new("fork")?,
        )
        .await?;
    assert_eq!(
        host.resolve(&child_head.workspace).await?.generation,
        child_head.generation
    );
    Ok(())
}

#[tokio::test]
async fn recovery_continuation_and_abort_use_the_facade_sequencer() -> Result<()> {
    let (facade, workspace_id) = transition_facade()?;
    let executor = Arc::new(FaultExecutor::new(workspace_id));
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
    let merge_error = facade
        .run(
            GitCommand::Merge {
                branch: "feature".into(),
            },
            live_tree(workspace_id),
            executor.as_ref(),
        )
        .await
        .err()
        .ok_or_else(|| Error::Invalid("failed merge was reported as complete".into()))?;
    assert!(matches!(merge_error, Error::Storage(_)));
    let resumed = facade
        .resume(executor.as_ref())
        .await?
        .ok_or_else(|| Error::Invalid("pending merge was not recoverable".into()))?;
    assert!(matches!(
        resumed,
        acyclic_fs::GitCommandOutput::Committed(_)
    ));
    let operations = executor.operations();
    assert_eq!(operations.len(), 3);
    assert_eq!(operations[1], operations[2]);

    let (facade, workspace_id) = transition_facade()?;
    let executor = Arc::new(FaultExecutor::new(workspace_id));
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
    assert_eq!(executor.operations().len(), 3);
    Ok(())
}

#[tokio::test]
async fn recovery_rebase_and_hard_reset_use_the_facade_identity() -> Result<()> {
    let (facade, workspace_id) = transition_facade()?;
    let executor = Arc::new(FaultExecutor::new(workspace_id));
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

    executor.fail_once();
    assert!(
        facade
            .run(
                GitCommand::Reset {
                    target: acyclic_fs::GitObjectName("HEAD".into()),
                    mode: acyclic_fs::GitResetMode::Hard,
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
    assert_eq!(executor.operations().len(), 6);
    Ok(())
}
