//! Rust and browser reads share the actual followed ClientViews handle.
#![cfg(target_arch = "wasm32")]
use acyclic_harness::{
    client::{
        ClientViewHandle, ClientViewLimits, QueryViewLimits, SwarmClient, SwarmClientLimits,
        WasmClientViews,
    },
    task_volume::{Channel, Cursor, FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{
    MemoryStream, StreamPath, StreamProvider,
    session::{Client, runtime::PlatformExecutor},
    wire,
};
use futures::{FutureExt as _, channel::oneshot};
use js_sys::{Array, Uint8Array};
use prost::Message as _;
use serde::Serialize as _;
use std::sync::Arc;
use wasm_bindgen_test::wasm_bindgen_test;

fn path(s: &str) -> StreamPath {
    StreamPath::new(s).unwrap()
}
fn client() -> SwarmClient {
    SwarmClient::new(
        path("swarm"),
        SwarmClientLimits {
            agents: 2,
            selected: 1,
            layout_bytes: 1024,
            config_files: 4,
            snapshot_files: 16,
        },
        ClientViewLimits {
            agents: 2,
            selected: 1,
            overview_values: 4,
            overview_bytes: 4096,
            query: QueryViewLimits {
                files: 16,
                segments: 16,
                bytes: 4096,
            },
        },
    )
    .unwrap()
}
#[derive(serde::Deserialize, Debug, PartialEq)]
struct Status {
    next: Option<u64>,
    accepted: u64,
    durable: u64,
    values: Vec<StatusValue>,
}
#[derive(serde::Deserialize, Debug, PartialEq)]
struct StatusValue {
    key: Vec<u8>,
    value: Vec<u8>,
    attention: bool,
}
fn same(
    native: &ClientViewHandle,
    browser: &WasmClientViews,
    drop_projection: bool,
    drop_task: bool,
    drop_notification: bool,
) -> Result<(), String> {
    let agents: Vec<String> = serde_wasm_bindgen::from_value(browser.agents().unwrap()).unwrap();
    let expected = native
        .read(|v| {
            v.agents()
                .map(|p| p.as_str().to_owned())
                .collect::<Vec<_>>()
        })
        .unwrap();
    if agents != expected {
        return Err("view parity: discovered agents differ".into());
    }
    let models: serde_json::Value =
        serde_wasm_bindgen::from_value(browser.models().unwrap()).unwrap();
    let actual_models = if drop_projection {
        serde_json::json!([])
    } else {
        models
    };
    let expected_models = native
        .read(|v| serde_json::to_value(v.models().collect::<Vec<_>>()).unwrap())
        .unwrap();
    if actual_models != expected_models {
        return Err("view parity: model projections differ".into());
    }
    let actual_notifications: serde_json::Value = if drop_notification {
        serde_json::json!([])
    } else {
        serde_wasm_bindgen::from_value(browser.notifications().unwrap()).unwrap()
    };
    let expected_notifications = native
        .read(|v| serde_json::to_value(v.notifications().collect::<Vec<_>>()).unwrap())
        .unwrap();
    if actual_notifications != expected_notifications {
        return Err("view parity: notification projections differ".into());
    }
    let actual_goals: serde_json::Value =
        serde_wasm_bindgen::from_value(browser.goals().unwrap()).unwrap();
    let expected_goals = native
        .read(|v| serde_json::to_value(v.goals().collect::<Vec<_>>()).unwrap())
        .unwrap();
    if actual_goals != expected_goals {
        return Err("view parity: goal projections differ".into());
    }
    for agent in expected {
        let path = path(&agent);
        let mut actual_task: serde_json::Value =
            serde_wasm_bindgen::from_value(browser.task(&agent, "t").unwrap()).unwrap();
        if drop_task && actual_task.is_object() {
            actual_task["tools"] = serde_json::json!([]);
        }
        let expected_task=native.read(|views| {
            let cursor=views.cursor(&path)?;
            let task=acyclic_harness::task_volume::Component::new("t").unwrap();
            let task=views.task(&(cursor.volume,cursor.query),&task)?;
            Some(serde_json::json!({
                "task":task.task,"cursor":task.cursor,
                "text":task.text().map(|text|serde_json::json!({
                    "turn":text.turn,"channel":text.channel,"file":file_info(text.file),
                })).collect::<Vec<_>>(),
                "tools":task.tools().map(|tool|serde_json::json!({
                    "turn":tool.turn,"call":tool.call,
                    "request":tool.request().map(file_info),"result":tool.result().map(file_info),
                    "arguments":tool.arguments().map(file_info).collect::<Vec<_>>(),
                    "stdout":tool.stdout().map(file_info).collect::<Vec<_>>(),
                    "stderr":tool.stderr().map(file_info).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
                "completions":task.completions().map(|(turn,file)|serde_json::json!({
                    "turn":turn,"file":file_info(file),
                })).collect::<Vec<_>>(),
            }))
        }).unwrap().unwrap_or(serde_json::Value::Null);
        if actual_task != expected_task {
            return Err("view parity: task folders differ".into());
        }
        let status: Option<Status> =
            serde_wasm_bindgen::from_value(browser.status(&agent).unwrap()).unwrap();
        let owner = acyclic_harness::client::OutputOwner::Task(
            acyclic_harness::task_volume::Component::new("t").unwrap(),
        );
        let actual_outcomes: serde_json::Value = serde_wasm_bindgen::from_value(
            browser
                .outcomes(&agent, serde_wasm_bindgen::to_value(&owner).unwrap(), 0)
                .unwrap(),
        )
        .unwrap();
        let expected_outcomes = native
            .read(|views| {
                let Some(cursor) = views.cursor(&path) else {
                    return serde_json::json!([]);
                };
                let files = views.selection(&(cursor.volume, cursor.query)).unwrap();
                serde_json::to_value(files.outputs(&owner, 0).collect::<Vec<_>>()).unwrap()
            })
            .unwrap();
        if actual_outcomes != expected_outcomes {
            return Err("view parity: command outcomes differ".into());
        }
        let expected_status = native
            .read(|v| {
                v.status(&path).map(|s| Status {
                    next: s.cursor().map(|c| c.next),
                    accepted: s.watermarks().accepted,
                    durable: s.watermarks().durable,
                    values: s
                        .values()
                        .map(|v| StatusValue {
                            key: v.key.to_vec(),
                            value: v.value.to_vec(),
                            attention: v.attention,
                        })
                        .collect(),
                })
            })
            .unwrap();
        if status != expected_status {
            return Err("view parity: status or attention differs".into());
        }
        let actual: Option<Cursor> =
            serde_wasm_bindgen::from_value(browser.cursor(&agent).unwrap()).unwrap();
        let expected = native.read(|v| v.cursor(&path).cloned()).unwrap();
        if actual != expected {
            return Err("view parity: cursors differ".into());
        }
        let actual_plans: Vec<String> =
            serde_wasm_bindgen::from_value(browser.plans(&agent).unwrap()).unwrap();
        let expected_plans = native
            .read(|v| v.plans(&path).map(str::to_owned).collect::<Vec<_>>())
            .unwrap();
        if actual_plans != expected_plans {
            return Err("view parity: plan projections differ".into());
        }
        let keys = native
            .read(|v| v.files(&path).map(|f| f.key.clone()).collect::<Vec<_>>())
            .unwrap();
        let files = Array::from(&browser.files(&agent).unwrap());
        if files.length() as usize != keys.len() {
            return Err("view parity: files differ".into());
        }
        for key in keys {
            let actual = Array::from(
                &browser
                    .segments(
                        &agent,
                        key.serialize(
                            &serde_wasm_bindgen::Serializer::new()
                                .serialize_large_number_types_as_bigints(true),
                        )
                        .unwrap(),
                    )
                    .unwrap(),
            );
            let actual: Vec<Vec<u8>> = actual
                .iter()
                .map(|a| Uint8Array::new(&a).to_vec())
                .collect();
            let expected = native
                .read(|v| {
                    v.file(&path, &key)
                        .unwrap()
                        .segments
                        .iter()
                        .map(|s| s.to_vec())
                        .collect::<Vec<_>>()
                })
                .unwrap();
            if actual != expected {
                return Err("view parity: text or tool segments differ".into());
            }
        }
    }
    Ok(())
}
fn file_info(file: &acyclic_harness::task_volume::HeadFile) -> serde_json::Value {
    serde_json::json!({"key":file.key,"length":file.length,
        "through_sequence":file.through_sequence,"segments":file.segments.len()})
}

async fn exercise(
    wrong_handle: bool,
    drop_projection: bool,
    drop_task: bool,
    drop_notification: bool,
) -> Result<(), String> {
    let provider = Arc::new(MemoryStream::default());
    let volume = TaskVolume::open(provider.clone(), path("swarm/a/files"))
        .await
        .unwrap();
    let parent = TaskVolume::open(provider.clone(), path("swarm/files"))
        .await
        .unwrap();
    let member = acyclic_harness::task_volume::ChildStatus {
        task: acyclic_harness::TaskId::from_bytes([1; 16]),
        state: acyclic_harness::task_volume::ChildState::Running,
        settled: false,
        inputs: 1,
    };
    parent
        .append(
            parent.head().await.unwrap(),
            &Write::new(
                FileKey::parse("agents/a/status.json").unwrap(),
                serde_json::to_vec(&member).unwrap(),
            ),
        )
        .await
        .unwrap();
    put(
        &volume,
        ".tasks/t/notifications/settled.json",
        br#"{"task":"t","sequence":9007199254740993,"event":"needs_approval","detail":"approval requested"}"#,
    )
    .await;
    put(&volume, "GOAL.md", b"initial goal").await;
    put(&volume, "plans/main.md", b"- [ ] initial plan").await;
    put(
        &volume,
        ".tasks/t/turns/9007199254740993/request.json",
        &request("initial"),
    )
    .await;
    put(
        &volume,
        ".tasks/t/executions/reply/result.json",
        br#"{"sequence":9007199254740993,"ok":{"text":"completed answer"}}"#,
    )
    .await;
    let mut chunk = Write::new(
        FileKey::parse(".tasks/t/turns/9007199254740993/chunks/1").unwrap(),
        "\u{8abf}\u{67fb}".as_bytes(),
    );
    chunk.channel = Some(Channel::Text);
    volume
        .append(volume.head().await.unwrap(), &chunk)
        .await
        .unwrap();
    for (leaf, bytes) in [
        ("request.json", b"request".as_slice()),
        ("arguments/1", b"args"),
        ("stdout/2", b"first"),
        ("stdout/10", b"second"),
        ("stderr/1", b"err"),
    ] {
        put(
            &volume,
            &format!(".tasks/t/turns/9007199254740993/tools/call/{leaf}"),
            bytes,
        )
        .await;
    }
    put(
        &volume,
        ".tasks/t/turns/9007199254740993/completion.json",
        b"completion",
    )
    .await;
    provider
        .append(acyclic_stream::AppendRequest {
            path: path("swarm/a/status"),
            records: vec![
                wire::SessionLatestValue {
                    key: b"status".to_vec().into(),
                    value: b"blocked".to_vec().into(),
                    attention: true,
                }
                .encode_to_vec()
                .into(),
            ],
            if_tail: None,
            idempotency_key: None,
        })
        .await
        .unwrap();
    let config = TaskVolume::open(Arc::new(MemoryStream::default()), path("config"))
        .await
        .unwrap();
    config
        .append(
            config.head().await.unwrap(),
            &Write::new(
                FileKey::parse("workspace/layout.json").unwrap(),
                br#"{"in_view":["swarm/a"]}"#.as_slice(),
            ),
        )
        .await
        .unwrap();
    let client = client();
    let native = client.views();
    let browser = WasmClientViews::from(if wrong_handle {
        self::client().views()
    } else {
        native.clone()
    });
    let session = Client::in_process(provider, Arc::new(PlatformExecutor));
    let (done, completed) = oneshot::channel();
    let mut done = Some(done);
    let target = std::cell::Cell::new(None);
    let followed = client
        .follow(
            &config,
            &parent,
            &session,
            |agent, maximum| {
                let volume = &volume;
                async move {
                    assert_eq!(agent, path("swarm/a"));
                    volume.heads(&Query::Root, None, maximum).await
                }
            },
            async |change| {
                if matches!(change, acyclic_harness::client::SwarmChange::Snapshot { .. }) {
                    put(&volume, ".tasks/t/notifications/blocked.json", br#"{"task":"t","sequence":9007199254740994,"event":"goal_blocked","detail":"input needed"}"#).await;
                    put(&volume, "GOAL.md", b"---\nstatus: done\n---\nfinal goal").await;
                    put(&volume, "plans/main.md", b"- [x] final plan").await;
                    put(&volume, ".tasks/t/turns/9007199254740994/request.json", &request("final")).await;
                    put(&volume,".tasks/t/turns/9007199254740993/tools/call/stdout/11",b"live").await;
                    put(&volume,".tasks/t/turns/9007199254740993/tools/call/result.json",b"result").await;
                    target.set(Some(volume.head().await.unwrap().position));
                }
                same(&native, &browser, drop_projection, drop_task, drop_notification).map_err(acyclic_harness::Error::Invalid)?;
                let ready = native
                    .read(|v| {
                        target.get().is_some_and(|target| v.cursor(&path("swarm/a")).is_some_and(|c| c.position == target))
                            && v.status(&path("swarm/a"))
                                .is_some_and(|s| s.values().any(|v| v.attention))
                    })
                    .unwrap();
                if ready {
                    native.read(|v| {
                        assert_eq!(v.models().count(), 2, "followed requests must project model identities");
                        assert!(v.models().any(|m| matches!(m.model, acyclic_harness::client::ModelFileView::Selected { name, .. } if name == "final")));
                        assert!(v.goals().any(|g| matches!(g.goal, acyclic_harness::client::GoalFileView::Present { goal } if goal.status == acyclic_harness::goal::GoalStatus::Done && goal.markdown == "final goal")));
                        assert!(v.plans(&path("swarm/a")).any(|line| line.contains("final plan")));
                    }).unwrap();
                }
                if ready && let Some(done) = done.take() {
                    let _ = done.send(());
                }
                Ok(())
            },
        )
        .fuse();
    let completed = completed.fuse();
    futures::pin_mut!(followed, completed);
    futures::select! { result = followed => result.map_err(|e| e.to_string()), result = completed => result.map_err(|e| e.to_string()) }
}
#[wasm_bindgen_test]
async fn native_and_wasm_render_the_same_followed_files_and_full_width_cursor() {
    exercise(false, false, false, false).await.unwrap();
}
#[wasm_bindgen_test]
async fn negative_control_separate_browser_reducer_is_detected() {
    assert!(
        exercise(true, false, false, false)
            .await
            .unwrap_err()
            .contains("view parity")
    );
}

#[wasm_bindgen_test]
async fn negative_control_omitting_followed_model_projections_is_detected() {
    assert!(
        exercise(false, true, false, false)
            .await
            .unwrap_err()
            .contains("model projections")
    );
}
async fn put(volume: &TaskVolume<MemoryStream>, path: &str, bytes: &[u8]) {
    volume
        .append(
            volume.head().await.unwrap(),
            &Write::new(FileKey::parse(path).unwrap(), bytes.to_vec()),
        )
        .await
        .unwrap();
}
fn request(name: &str) -> Vec<u8> {
    use acyclic_harness::model::{Model, ModelContent, ModelMessage, ModelRequest, ModelRole};
    serde_json::to_vec(&ModelRequest {
        model: Model::new(
            "scripted".into(),
            name.into(),
            "1".into(),
            serde_json::json!({}),
        )
        .unwrap(),
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("work".into()),
        }],
        tools: vec![],
        max_output_tokens: Some(32),
    })
    .unwrap()
}

#[wasm_bindgen_test]
async fn negative_control_omitting_followed_notifications_is_detected() {
    assert!(
        exercise(false, false, false, true)
            .await
            .unwrap_err()
            .contains("notification projections")
    );
}

#[wasm_bindgen_test]
async fn negative_control_omitting_followed_task_tools_is_detected() {
    assert!(exercise(false, false, true, false).await.unwrap_err().contains("task folders differ"));
}
