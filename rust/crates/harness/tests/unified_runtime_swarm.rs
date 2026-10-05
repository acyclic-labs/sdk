#![cfg(feature = "filesystem-local")]

//! One production-composition black-box scenario for the local swarm.
//!
//! The provider is deterministic, but every event is sent through the normal
//! model executor. The test deliberately observes the actual serialized
//! requests, tool-result pairing, durable communication, and real per-child
//! project volumes after the recursive run.

use acyclic_fs::{Fs, GitBranch, GitCompatState, LocalCoreStateStore, LocalOptions, WorkspaceId};
use acyclic_harness::conversation::{VolumeClass, VolumeOwner, VolumeRef};
use acyclic_harness::filesystem::PersistentLocalSwarm;
use acyclic_harness::fork::ResourceRevision;
use acyclic_harness::model::{
    Model, ModelContent, ModelContentPart, ModelEvent, ModelProvider, ModelRequest,
};
use acyclic_harness::resources::ProviderRef;
use acyclic_harness::{Error, Limits, OperationId, Result};
use futures::{stream, stream::BoxStream};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tempfile::tempdir;

#[path = "swarm_provider_support.rs"]
mod swarm_provider_support;
use swarm_provider_support::FixtureUsage;

fn id(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn task_name(request: &ModelRequest) -> Option<String> {
    request.messages.iter().rev().find_map(|message| {
        let ModelContent::Text(text) = &message.content else {
            return None;
        };
        let text = text.strip_prefix("child task: ")?;
        Some(text.split_once(";")?.0.to_owned())
    })
}

fn staged_file(request: &ModelRequest) -> Option<Value> {
    request.messages.iter().find_map(|message| {
        let ModelContent::Part(ModelContentPart::ToolResult { name, value, .. }) = &message.content
        else {
            return None;
        };
        (name == "acyclic.stage_file")
            .then(|| value.get("file").cloned())
            .flatten()
    })
}

fn read_generation(request: &ModelRequest) -> Value {
    request
        .messages
        .iter()
        .find_map(|message| {
            let ModelContent::Part(ModelContentPart::ToolResult { name, value, .. }) =
                &message.content
            else {
                return None;
            };
            (name == "acyclic.read")
                .then(|| value.get("generation").cloned())
                .flatten()
        })
        .expect("the model-driven edit must use the pinned read generation")
}

fn has_tool_result(request: &ModelRequest, name: &str) -> bool {
    request.messages.iter().any(|message| {
        matches!(
            &message.content,
            ModelContent::Part(ModelContentPart::ToolResult { name: result, .. }) if result == name
        )
    })
}

fn has_tool_result_call(request: &ModelRequest, name: &str, call_id: &str) -> bool {
    request.messages.iter().any(|message| {
        matches!(
            &message.content,
            ModelContent::Part(ModelContentPart::ToolResult {
                call_id: result_call,
                name: result_name,
                ..
            }) if result_name == name && result_call == call_id
        )
    })
}

struct UnifiedProvider {
    requests: Mutex<Vec<ModelRequest>>,
    root_started: AtomicBool,
    root_edit_sent: AtomicBool,
    child_a_forked: AtomicBool,
    child_a_edit_sent: AtomicBool,
    child_a_status_sent: AtomicBool,
    merge_child: AtomicBool,
    merge_child_sent: AtomicBool,
    grandchild_status_sent: AtomicBool,
    child_b_status_sent: AtomicBool,
    child_b_edit_sent: AtomicBool,
    grandchild_edit_sent: AtomicBool,
    root_exchange_sent: AtomicBool,
    merge_root: AtomicBool,
    merge_root_sent: AtomicBool,
    child_a: OperationId,
    child_b: OperationId,
    grandchild: OperationId,
    usage: Arc<FixtureUsage>,
}

impl UnifiedProvider {
    fn new(child_a: OperationId, child_b: OperationId, grandchild: OperationId) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            root_started: AtomicBool::new(false),
            root_edit_sent: AtomicBool::new(false),
            child_a_forked: AtomicBool::new(false),
            child_a_edit_sent: AtomicBool::new(false),
            child_a_status_sent: AtomicBool::new(false),
            merge_child: AtomicBool::new(false),
            merge_child_sent: AtomicBool::new(false),
            grandchild_status_sent: AtomicBool::new(false),
            child_b_status_sent: AtomicBool::new(false),
            child_b_edit_sent: AtomicBool::new(false),
            grandchild_edit_sent: AtomicBool::new(false),
            root_exchange_sent: AtomicBool::new(false),
            merge_root: AtomicBool::new(false),
            merge_root_sent: AtomicBool::new(false),
            child_a,
            child_b,
            grandchild,
            usage: FixtureUsage::new("harness.test.unified-runtime-swarm"),
        })
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().expect("request lock").clone()
    }
}

impl ModelProvider for UnifiedProvider {
    fixture_budget_methods!();

    fn generate<'a>(
        &'a self,
        prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let request = prepared.request().clone();
        self.requests
            .lock()
            .expect("request lock")
            .push(request.clone());
        let task = task_name(&request);
        let root = task.is_none();
        let events = if root && !self.root_started.swap(true, Ordering::SeqCst) {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "root-read-seed".into(),
                    name: "acyclic.read".into(),
                    arguments: json!({"path": "/seed.txt"}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]
        } else if root && !self.root_edit_sent.swap(true, Ordering::SeqCst) {
            let generation = read_generation(&request);
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "root-edit".into(),
                    name: "acyclic.edit".into(),
                    arguments: json!({
                        "path": "/root-note.txt",
                        "content": "root authored this exact note",
                        "expected_generation": generation,
                    }),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "root-git-add".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["add", "--all"]}),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "root-git-commit".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["commit", "-m", "root authored note"]}),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "root-stage-message".into(),
                    name: "acyclic.stage_file".into(),
                    arguments: json!({
                        "path": "root-note.txt",
                        "text": "root authored this exact note",
                        "media_type": "text/plain",
                        "display_name": "root-note.txt"
                    }),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "root-fork-a".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": self.child_a.to_string(),
                        "task": "runtime-child-a",
                        "prompt": "fork your grandchild and inspect the project"
                    }),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "root-fork-b".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": self.child_b.to_string(),
                        "task": "runtime-child-b",
                        "prompt": "inspect the sibling project"
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]
        } else if task.as_deref() == Some("runtime-child-a")
            && self.merge_child.load(Ordering::SeqCst)
            && !self.merge_child_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "child-a-merge-grandchild".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["merge", "grandchild"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]
        } else if task.as_deref() == Some("runtime-child-a")
            && !self.child_a_forked.load(Ordering::SeqCst)
        {
            if !has_tool_result(&request, "acyclic.read") {
                return Box::pin(stream::iter(vec![
                    Ok(ModelEvent::ToolCall {
                        call_id: "child-a-read-seed".into(),
                        name: "acyclic.read".into(),
                        arguments: json!({"path": "/seed.txt"}),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]));
            }
            self.child_a_edit_sent.store(true, Ordering::SeqCst);
            self.child_a_forked.store(true, Ordering::SeqCst);
            let generation = read_generation(&request);
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "child-a-edit".into(),
                    name: "acyclic.edit".into(),
                    arguments: json!({
                        "path": "/child-a-note.txt",
                        "content": "child A authored this exact note",
                        "expected_generation": generation,
                    }),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "child-a-git-add".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["add", "--all"]}),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "child-a-git-commit".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["commit", "-m", "child A authored note"]}),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "child-a-fork-grandchild".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": self.grandchild.to_string(),
                        "task": "runtime-grandchild",
                        "prompt": "inspect and complete the recursive project"
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]
        } else if task.as_deref() == Some("runtime-child-a")
            && !self.child_a_status_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "child-a-status".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["status"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]
        } else if task.as_deref() == Some("runtime-child-b")
            && !self.child_b_status_sent.load(Ordering::SeqCst)
        {
            if !has_tool_result(&request, "acyclic.read") {
                return Box::pin(stream::iter(vec![
                    Ok(ModelEvent::ToolCall {
                        call_id: "child-b-read-seed".into(),
                        name: "acyclic.read".into(),
                        arguments: json!({"path": "/seed.txt"}),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]));
            }
            self.child_b_edit_sent.store(true, Ordering::SeqCst);
            self.child_b_status_sent.store(true, Ordering::SeqCst);
            let generation = read_generation(&request);
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "child-b-edit".into(),
                    name: "acyclic.edit".into(),
                    arguments: json!({
                        "path": "/child-b-note.txt",
                        "content": "child B authored this exact note",
                        "expected_generation": generation,
                    }),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "child-b-git-add".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["add", "--all"]}),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "child-b-git-commit".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["commit", "-m", "child B authored note"]}),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "child-b-status".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["status"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]
        } else if task.as_deref() == Some("runtime-grandchild")
            && !self.grandchild_status_sent.load(Ordering::SeqCst)
        {
            if !has_tool_result(&request, "acyclic.read") {
                return Box::pin(stream::iter(vec![
                    Ok(ModelEvent::ToolCall {
                        call_id: "grandchild-read-seed".into(),
                        name: "acyclic.read".into(),
                        arguments: json!({"path": "/seed.txt"}),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]));
            }
            self.grandchild_edit_sent.store(true, Ordering::SeqCst);
            self.grandchild_status_sent.store(true, Ordering::SeqCst);
            let generation = read_generation(&request);
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "grandchild-edit".into(),
                    name: "acyclic.edit".into(),
                    arguments: json!({
                        "path": "/grandchild-note.txt",
                        "content": "grandchild authored this exact note",
                        "expected_generation": generation,
                    }),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "grandchild-git-add".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["add", "--all"]}),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "grandchild-git-commit".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["commit", "-m", "grandchild authored note"]}),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "grandchild-status".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["status"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]
        } else if root && !self.root_exchange_sent.swap(true, Ordering::SeqCst) {
            let payload = staged_file(&request).expect("root stage result must be retained");
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "root-message-a".into(),
                    name: "swarm.message".into(),
                    arguments: json!({
                        "recipient": self.child_a.to_string(),
                        "target": "child",
                        "payload": payload
                    }),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "root-wait-children".into(),
                    name: "swarm.wait".into(),
                    arguments: json!({
                        "kind": "tasks",
                        "task_ids": [self.child_a.to_string(), self.child_b.to_string()]
                    }),
                }),
                Ok(ModelEvent::Content {
                    delta: "unified recursive swarm complete".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]
        } else if root
            && self.merge_root.load(Ordering::SeqCst)
            && !self.merge_root_sent.swap(true, Ordering::SeqCst)
        {
            vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "root-merge-child-a".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["merge", "child-a"]}),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "root-merge-child-b".into(),
                    name: "acyclic.git".into(),
                    arguments: json!({"argv": ["merge", "child-b"]}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]
        } else {
            vec![
                Ok(ModelEvent::Content {
                    delta: "completed".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]
        };
        Box::pin(stream::iter(events))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: acyclic_harness::model::ModelAttempt,
    ) -> futures::future::BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

fn project_from_seed(seed: &acyclic_harness::fork::ForkSeed) -> Result<VolumeRef> {
    seed.resources
        .iter()
        .find_map(|resource| match &resource.revision {
            ResourceRevision::Project { volume, .. } => Some(volume.clone()),
            _ => None,
        })
        .ok_or_else(|| Error::Invalid("recursive model fork has no project volume".into()))
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
        return Err(Error::Conflict("unified Git branch state raced".into()));
    }
    Ok(())
}

#[tokio::test]
async fn default_local_runtime_executes_two_children_grandchild_and_communication() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let child_a = id(0xA1);
    let child_b = id(0xB1);
    let grandchild = id(0xC1);
    let provider = UnifiedProvider::new(child_a, child_b, grandchild);
    let model = Model::new("mock", "unified-runtime-swarm", "1", json!({}))?;
    let filesystem_provider = ProviderRef::new("local", "filesystem", "2")?;
    let host = Arc::new(acyclic_harness::filesystem::FilesystemHost::new(
        Fs::local(LocalOptions::new(directory.path().join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        filesystem_provider.clone(),
    )?);
    let root_project = VolumeRef::new(
        filesystem_provider.clone(),
        "local-project",
        VolumeClass::Project,
        VolumeOwner::Project("local-swarm".into()),
    )?;
    host.create_volume(&root_project).await?;
    let root_workspace = acyclic_harness::filesystem::workspace_ref(
        filesystem_provider.clone(),
        &root_project.storage_name()?,
    )?;
    let root_head = host.resolve(&root_workspace).await?;
    host.apply(
        &root_workspace,
        Some(&root_head.generation),
        &[acyclic_harness::filesystem::WorkspaceMutation::PutFile {
            path: "/seed.txt".into(),
            bytes: b"seed for pinned model reads".to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("unified-seed")?,
    )
    .await?;
    let swarm = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
        directory.path(),
        model,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let output = swarm
        .run_root(id(0xA0), "run the complete recursive local swarm")
        .await?;
    assert_eq!(output.text, "unified recursive swarm complete");
    assert_eq!(swarm.sessions().await?.len(), 4);
    for operation in [child_a, child_b, grandchild] {
        let task = acyclic_harness::TaskId::from_bytes(operation.into_bytes());
        assert_eq!(swarm.outcome(task).await?.text, "completed");
    }

    let requests = provider.requests();
    assert!(requests.iter().any(|request| {
        task_name(request).as_deref() == Some("runtime-grandchild")
            && has_tool_result(request, "acyclic.git")
    }));
    let root_request = requests
        .iter()
        .rev()
        .find(|request| task_name(request).is_none() && has_tool_result(request, "swarm.wait"))
        .ok_or_else(|| Error::Conflict("root communication exchange was not captured".into()))?;
    let wait = root_request.messages.iter().any(|message| {
        matches!(
            &message.content,
            ModelContent::Part(ModelContentPart::ToolResult { name, value, .. })
                if name == "swarm.wait" && value["kind"] == "tasks"
        )
    });
    assert!(
        wait,
        "root wait must be paired with the durable task outcomes"
    );
    assert!(root_request.messages.iter().any(|message| {
        matches!(
            &message.content,
            ModelContent::Part(ModelContentPart::ToolResult { name, value, .. })
                if name == "swarm.message" && value["delivered"] == true
        )
    }));

    // Every project edit and commit above came from an admitted model tool
    // call. Verify the resulting child generations directly through the typed
    // host; no host-side content mutation is performed after the model run.
    let child_a_project = project_from_seed(
        &swarm
            .published_seed(acyclic_harness::TaskId::from_bytes(child_a.into_bytes()))
            .await?,
    )?;
    let child_b_project = project_from_seed(
        &swarm
            .published_seed(acyclic_harness::TaskId::from_bytes(child_b.into_bytes()))
            .await?,
    )?;
    let grandchild_project = project_from_seed(
        &swarm
            .published_seed(acyclic_harness::TaskId::from_bytes(grandchild.into_bytes()))
            .await?,
    )?;

    // The branch records are test fixture metadata only: content and all
    // commits below are produced by model-facing Git calls.  The subsequent
    // merge calls are issued by the child and root model turns, so the typed
    // facade remains the sole publication path.
    let child_a_workspace_id = host
        .workspace_id(child_a_project.storage_name()?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let child_b_workspace_id = host
        .workspace_id(child_b_project.storage_name()?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let grandchild_workspace_id = host
        .workspace_id(grandchild_project.storage_name()?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let root_workspace_id = host
        .workspace_id(root_project.storage_name()?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    install_git_branch(
        directory.path(),
        child_a_workspace_id,
        "grandchild",
        grandchild_workspace_id,
    )
    .await?;
    install_git_branch(
        directory.path(),
        root_workspace_id,
        "child-a",
        child_a_workspace_id,
    )
    .await?;
    install_git_branch(
        directory.path(),
        root_workspace_id,
        "child-b",
        child_b_workspace_id,
    )
    .await?;
    provider.merge_child.store(true, Ordering::SeqCst);
    swarm
        .run(
            acyclic_harness::TaskId::from_bytes(child_a.into_bytes()),
            id(0xA2),
            "merge the completed grandchild through the model Git facade",
        )
        .await?;
    provider.merge_root.store(true, Ordering::SeqCst);
    swarm
        .run_root(
            id(0xA3),
            "publish both direct child projects through the model Git facade",
        )
        .await?;

    // The production provider boundary must settle measured usage for every
    // dispatch and release every child reservation, including after the
    // explicit merge turns.  The root lease remains alive, while the three
    // completed descendants leave no reserved budget behind.
    let budget = swarm.budget_journal().lock().await.usage()?;
    assert_eq!(budget.total_agents, 4);
    assert_eq!(budget.active_agents, 1);
    assert!(budget.consumed.model_steps > 0);
    assert!(budget.consumed.output_bytes > 0);
    assert_eq!(budget.reserved.model_steps, 0);
    assert_eq!(budget.reserved.output_bytes, 0);
    assert_eq!(budget.reserved.execution_time_ms, 0);

    let requests = provider.requests();
    for (task, call_id) in [
        (None, "root-git-commit"),
        (Some("runtime-child-a"), "child-a-git-commit"),
        (Some("runtime-child-b"), "child-b-git-commit"),
        (Some("runtime-grandchild"), "grandchild-git-commit"),
        (Some("runtime-child-a"), "child-a-merge-grandchild"),
        (None, "root-merge-child-a"),
        (None, "root-merge-child-b"),
    ] {
        assert!(
            requests.iter().any(|request| {
                task_name(request).as_deref() == task
                    && has_tool_result_call(request, "acyclic.git", call_id)
            }),
            "model Git call {call_id} must have a durable paired result"
        );
    }
    let child_a_workspace = acyclic_harness::filesystem::workspace_ref(
        filesystem_provider.clone(),
        &child_a_project.storage_name()?,
    )?;
    let child_b_workspace = acyclic_harness::filesystem::workspace_ref(
        filesystem_provider.clone(),
        &child_b_project.storage_name()?,
    )?;
    let grandchild_workspace = acyclic_harness::filesystem::workspace_ref(
        filesystem_provider.clone(),
        &grandchild_project.storage_name()?,
    )?;
    for (workspace, path, expected) in [
        (
            &child_a_workspace,
            "/child-a-note.txt",
            "child A authored this exact note",
        ),
        (
            &child_b_workspace,
            "/child-b-note.txt",
            "child B authored this exact note",
        ),
        (
            &grandchild_workspace,
            "/grandchild-note.txt",
            "grandchild authored this exact note",
        ),
    ] {
        assert_eq!(
            host.read(workspace, None, path, 1_024).await?,
            expected.as_bytes()
        );
    }
    assert_eq!(
        host.read(&child_a_workspace, None, "/grandchild-note.txt", 1_024)
            .await?,
        b"grandchild authored this exact note"
    );
    assert!(host
        .read(&child_b_workspace, None, "/child-a-note.txt", 1_024)
        .await
        .is_err());
    assert!(host
        .read(&child_b_workspace, None, "/grandchild-note.txt", 1_024)
        .await
        .is_err());
    assert!(host
        .read(&grandchild_workspace, None, "/child-b-note.txt", 1_024)
        .await
        .is_err());
    for (path, expected) in [
        ("/root-note.txt", "root authored this exact note"),
        ("/child-a-note.txt", "child A authored this exact note"),
        ("/child-b-note.txt", "child B authored this exact note"),
        (
            "/grandchild-note.txt",
            "grandchild authored this exact note",
        ),
    ] {
        assert_eq!(
            host.read(&root_workspace, None, path, 1_024).await?,
            expected.as_bytes()
        );
    }
    swarm.shutdown_workers().await;
    Ok(())
}
