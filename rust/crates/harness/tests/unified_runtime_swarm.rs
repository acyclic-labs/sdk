#![cfg(feature = "filesystem-local")]

//! One production-composition black-box scenario for the local swarm.
//!
//! The provider is deterministic, but every event is sent through the normal
//! model executor. The test deliberately observes the actual serialized
//! requests, tool-result pairing, durable communication, and real per-child
//! project volumes after the recursive run.

use acyclic_fs::{Fs, LocalOptions};
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

struct UnifiedProvider {
    requests: Mutex<Vec<ModelRequest>>,
    root_started: AtomicBool,
    root_edit_sent: AtomicBool,
    child_a_forked: AtomicBool,
    child_a_edit_sent: AtomicBool,
    child_a_status_sent: AtomicBool,
    grandchild_status_sent: AtomicBool,
    child_b_status_sent: AtomicBool,
    child_b_edit_sent: AtomicBool,
    grandchild_edit_sent: AtomicBool,
    root_exchange_sent: AtomicBool,
    child_a: OperationId,
    child_b: OperationId,
    grandchild: OperationId,
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
            grandchild_status_sent: AtomicBool::new(false),
            child_b_status_sent: AtomicBool::new(false),
            child_b_edit_sent: AtomicBool::new(false),
            grandchild_edit_sent: AtomicBool::new(false),
            root_exchange_sent: AtomicBool::new(false),
            child_a,
            child_b,
            grandchild,
        })
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().expect("request lock").clone()
    }
}

impl ModelProvider for UnifiedProvider {
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

    // Every project edit above came from an admitted model tool call. Verify
    // the resulting child generations directly through the typed host; no
    // host-side mutation is performed after the model run.
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
    for (project, path, expected) in [
        (
            &child_a_project,
            "/child-a-note.txt",
            "child A authored this exact note",
        ),
        (
            &child_b_project,
            "/child-b-note.txt",
            "child B authored this exact note",
        ),
        (
            &grandchild_project,
            "/grandchild-note.txt",
            "grandchild authored this exact note",
        ),
    ] {
        let workspace = acyclic_harness::filesystem::workspace_ref(
            filesystem_provider.clone(),
            &project.storage_name()?,
        )?;
        assert_eq!(
            host.read(&workspace, None, path, 1_024).await?,
            expected.as_bytes()
        );
    }
    assert!(host
        .read(&root_workspace, None, "/child-a-note.txt", 1_024)
        .await
        .is_err());
    assert!(host
        .read(&root_workspace, None, "/child-b-note.txt", 1_024)
        .await
        .is_err());
    assert!(host
        .read(&root_workspace, None, "/grandchild-note.txt", 1_024)
        .await
        .is_err());
    swarm.shutdown_workers().await;
    Ok(())
}
