//! Native children run under the existing owner while their hook slot stays free.
#[path = "support/native_actions.rs"]
mod native_actions;
#[path = "support/native_hooks.rs"]
mod native_hooks;
use futures::FutureExt as _;
use native_actions::Tools;
use native_hooks::*;
use serde_json::{Value, json};
use std::{path::Path, process::Stdio};

fn native_spawn(id: &str, helper: &str) -> Value {
    let session = uuid::Uuid::new_v4().to_string();
    json!({"id":id,"prompt":helper,"executor":{
        "session":{"host":"pi","session_id":session,"caller":null},
        "executable":std::env::current_exe().unwrap(),
        "arguments":[{"kind":"text","value":"--ignored"},{"kind":"text","value":"--exact"},
            {"kind":"prompt"},{"kind":"text","value":"--nocapture"}],
        "session_bytes":json!({"type":"session","version":3,"id":session,
            "timestamp":"2026-10-17T00:00:00Z","cwd":"/project"}).to_string(),
        "timeout_ms":8000
    }})
}

#[tokio::test]
async fn native_process_runs_nested_hooks_in_isolated_children_and_replays_without_launching()
-> Result {
    let fixture = Fixture::new()?;
    std::fs::write(fixture.path().join("preserved.txt"), "parent contents")?;
    let mut owner = Some(fixture.start_owner().await?);
    let result = std::panic::AssertUnwindSafe(async {
        let cwd = fixture.path();
        let mut parent = Tools::open(
            cwd,
            acyclic_harness::external_executor::ExternalHost::Pi,
            None,
        )
        .await?;
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            let args = native_spawn("child", "native_child_process");
            let child = parent.call("spawn", "acyclic_spawn", args.clone()).await?;
            let child = child.as_str().ok_or("missing child task")?;
            let outcome = parent
                .call("wait", "acyclic_wait", json!({"child":child}))
                .await?;
            assert_eq!(outcome["success"], true, "{outcome}");
            assert!(!cwd.join("child.txt").exists());
            assert!(!cwd.join("leaf.txt").exists());
            let inspected = parent
                .call("inspect", "acyclic_inspect", json!({"task":child}))
                .await?;
            assert_eq!(inspected["admission"]["parent"], json!(parent.task));
            let diff = parent
                .call(
                    "diff",
                    "fs",
                    json!({"command":format!("diff agents/{child}")}),
                )
                .await?;
            assert_eq!(
                diff["result"]["Filesystem"]["Data"]["kind"], "diff",
                "{diff}"
            );
            assert_eq!(
                diff["result"]["Filesystem"]["Data"]["value"]["fileChanges"], 2,
                "{diff}"
            );
            parent
                .call(
                    "selected-import",
                    "fs",
                    json!({"command":format!("restore --source agents/{child} -- child.txt")}),
                )
                .await?;
            assert_eq!(
                std::fs::read_to_string(cwd.join("child.txt"))?,
                "isolated native effect"
            );
            assert!(!cwd.join("leaf.txt").exists());
            assert_eq!(
                std::fs::read_to_string(cwd.join("preserved.txt"))?,
                "parent contents"
            );
            fixture.close_owner(owner.take().unwrap()).await?;
            owner = Some(fixture.start_owner().await?);
            parent.reconnect().await?;
            let replay = parent.call("spawn", "acyclic_spawn", args).await?;
            assert_eq!(replay, child);
            assert_eq!(
                parent
                    .call("wait", "acyclic_wait", json!({"child":child}))
                    .await?,
                outcome
            );
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await?
    })
    .catch_unwind()
    .await;
    if matches!(&result, Ok(Err(_))) {
        let _ = retained_diagnostics(fixture.path()).await;
    }
    let stopped = if let Some(owner) = owner {
        fixture.close_owner(owner).await
    } else {
        Ok(())
    };
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    stopped
}

// These are the actual native executables launched by the production custody
// task. They use the installed CLI and inherited authenticated client binding.
async fn owned_hook(event: &str, input: Value) -> Result<Value> {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_acyclic"));
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    hook_using(command, "pi", event, input).await
}

async fn native_child(nested: bool) -> Result {
    let cwd = std::env::current_dir()?;
    // create_new proves the custody command did not execute twice after reopen.
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(cwd.join("executed"))?;
    let bytes = std::fs::read(cwd.join("session.jsonl"))?;
    let header: Value = serde_json::from_slice(
        bytes
            .split(|byte| *byte == b'\n')
            .next()
            .ok_or("missing session header")?,
    )?;
    let session = header["id"]
        .as_str()
        .ok_or("missing native session identity")?;
    owned_hook("session_start", json!({"sessionId":session,"cwd":cwd})).await?;
    let binding = acyclic_harness::external_executor::HookBinding::from_bytes(&std::fs::read(
        std::env::var_os("ACYCLIC_HARNESS_BINDING").ok_or("missing native binding")?,
    )?)?;
    let acyclic_harness::external_executor::HookBinding::Local { workspace, task } = binding else {
        return Err("native fixture requires its admitted local binding".into());
    };
    let root = std::env::var_os("ACYCLIC_LOCAL_OWNER_DIR").ok_or("missing owner directory")?;
    let provider = native_hooks::sdk_client::workspace(Path::new(&root), &workspace).await?;
    let tools = Tools::attached(
        &cwd,
        acyclic_harness::external_executor::ExternalHost::Pi,
        session.into(),
        std::sync::Arc::new(provider),
    )
    .await?;
    assert_eq!(task, Some(tools.task));
    tools.call("write","write",json!({"path":if nested {"child.txt"} else {"leaf.txt"},"content":"isolated native effect"})).await?;
    if nested {
        let leaf = tools
            .call(
                "spawn-leaf",
                "acyclic_spawn",
                native_spawn("leaf", "native_leaf_process"),
            )
            .await?;
        let outcome = tools
            .call("wait-leaf", "acyclic_wait", json!({"child":leaf}))
            .await?;
        assert_eq!(outcome["success"], true, "{outcome}");
        tools.call("import-leaf","fs",json!({"command":format!("restore --source agents/{} -- leaf.txt",leaf.as_str().ok_or("missing leaf task")?)})).await?;
    }
    owned_hook("session_shutdown", json!({"sessionId":session,"cwd":cwd})).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "launched as the native child by the custody regression"]
async fn native_child_process() -> Result {
    native_child(true).await
}

#[tokio::test]
#[ignore = "launched as the native descendant by the custody regression"]
async fn native_leaf_process() -> Result {
    native_child(false).await
}

#[tokio::test]
#[ignore = "launched with selected native context by the custody regression"]
async fn selected_native_process() -> Result {
    let bytes = std::fs::read("session.jsonl")?;
    let rows = std::str::from_utf8(&bytes)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(
        rows.len(),
        4,
        "the invoking assistant or selected history was changed"
    );
    assert_eq!(rows[1]["id"], "source-user");
    assert_eq!(rows[1]["message"]["content"], "CALLER_SELECTED_CONTEXT");
    assert_eq!(rows[2]["id"], "completed-call");
    assert_eq!(rows[3]["id"], "completed-result");
    assert_eq!(rows[3]["parentId"], "completed-call");
    native_child(false).await
}

#[tokio::test]
async fn selected_task_files_seed_the_native_child_and_pin_provenance_without_parent_effects()
-> Result {
    use acyclic_harness::task_volume::{FileKey, FileValue, Query, TaskFile, TaskVolume};
    use acyclic_stream::StreamPath;
    use sha2::{Digest as _, Sha256};
    let fixture = Fixture::new()?;
    let owner = fixture.start_owner().await?;
    let result=std::panic::AssertUnwindSafe(async {
        let parent=Tools::open(fixture.path(),acyclic_harness::external_executor::ExternalHost::Pi,None).await?;
        let rows=[json!({"type":"session","version":3,"id":parent.session,"cwd":fixture.path(),"timestamp":"2026-10-17T00:00:00Z"}),
            json!({"type":"message","id":"source-user","parentId":null,"timestamp":"now","message":{"role":"user","content":"CALLER_SELECTED_CONTEXT","timestamp":1}}),
            json!({"type":"message","id":"completed-call","parentId":"source-user","timestamp":"now","message":{"role":"assistant","content":[{"type":"toolCall","id":"read","name":"read","arguments":{}}],"timestamp":2}}),
            json!({"type":"message","id":"completed-result","parentId":"completed-call","timestamp":"now","message":{"role":"toolResult","toolCallId":"read","toolName":"read","content":[{"type":"text","text":"retained result"}],"isError":false,"timestamp":3}}),
            json!({"type":"message","id":"invoking","parentId":"completed-result","timestamp":"now","message":{"role":"assistant","content":[{"type":"toolCall","id":"selected-spawn","name":"acyclic_spawn","arguments":{}}],"timestamp":4}})];
        let bytes=rows.iter().map(Value::to_string).collect::<Vec<_>>().join("\n")+"\n";
        let digest:[u8;32]=Sha256::digest(bytes.as_bytes()).into();
        let prefix=format!("workspace/native/{}/{}",parent.session,hex::encode(digest));
        let files=TaskVolume::open_existing(parent.provider.clone(),StreamPath::new(format!("external/requests/{}",parent.task))?).await?;
        for (index,chunk) in bytes.as_bytes().chunks(8192).enumerate() {
            files.append(files.head().await?,&FileValue::new(FileKey::parse(&format!("{prefix}/chunk-{index}"))?,chunk.to_vec())).await?;
        }
        let path=format!("{prefix}/manifest.json");
        files.append(files.head().await?,&FileValue::new(FileKey::parse(&path)?,serde_json::to_vec(&json!({"chunks":bytes.len().div_ceil(8192),"length":bytes.len()}))?)).await?;
        let mut args=native_spawn("selected-child","selected_native_process");
        args["context"]=json!("native_cut");
        args["executor"]["context"]=json!({"manifest":path,"head":"invoking","sha256":digest});
        let mut rejected=args.clone();
        rejected["executor"]["context"]["sha256"]=serde_json::to_value([0_u8;32])?;
        let refused=parent.reject("invalid-native-source","acyclic_spawn",rejected).await?;
        assert!(refused["Failed"]["message"].as_str().unwrap().contains("native context"));
        let operation=acyclic_harness::external_executor::request(acyclic_harness::external_executor::ExternalHost::Pi,"tool_call",
            json!({"sessionId":parent.session,"toolCallId":"selected-spawn","toolName":"acyclic_spawn","input":args.clone()}))?.operation_id;
        let child=parent.call("selected-spawn","acyclic_spawn",args.clone()).await?;
        let outcome=parent.call("selected-wait","acyclic_wait",json!({"child":child})).await?;
        assert_eq!(outcome["success"],true,"{outcome}");
        assert!(!fixture.path().join("leaf.txt").exists());
        let results=TaskVolume::open_existing(parent.provider.clone(),StreamPath::new(format!("external/results/{}",parent.task))?).await?;
        let key=FileKey::Task {task:acyclic_harness::task_volume::Component::new(parent.task.to_string())?,file:TaskFile::Context(acyclic_harness::task_volume::Component::new(format!("native-{operation}"))?)};
        let mut selected=results.snapshot(&Query::File(key),1).await?.files;
        assert_eq!(selected.len(),1,"selected context manifest missing");
        let manifest=selected.pop().unwrap();
        let manifest:Value=serde_json::from_slice(&manifest.bytes)?;
        assert_eq!(manifest["cut"]["session"],parent.session);
        assert_eq!(manifest["cut"]["through"],"completed-result");
        assert_eq!(manifest["cut"]["workspace"],json!(fixture.path()));
        // Acceptance pins the seed. A later host-file edit cannot reseed a
        // retry, change its provenance, or launch the original child again.
        let changed=FileValue::new(FileKey::parse(&format!("{prefix}/chunk-0"))?,b"later source mutation".to_vec());
        files.append(files.head().await?,&changed).await?;
        assert_eq!(parent.call("selected-spawn","acyclic_spawn",args).await?,child);
        let retained=results.snapshot(&Query::File(FileKey::Task {
            task:acyclic_harness::task_volume::Component::new(parent.task.to_string())?,
            file:TaskFile::Context(acyclic_harness::task_volume::Component::new(format!("native-{operation}"))?),
        }),1).await?;
        assert_eq!(retained.files.len(),1);
        assert_eq!(serde_json::from_slice::<Value>(&retained.files[0].bytes)?,manifest);
        Ok::<_,Box<dyn std::error::Error>>(())
    }).catch_unwind().await;
    let stopped = fixture.close_owner(owner).await;
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    stopped
}

// Read the accepted task files on failure before closing their provider. This
// distinguishes an unlaunched child, a running effect and a stranded result.
async fn retained_diagnostics(cwd: &Path) -> Result {
    use acyclic_harness::task_volume::{Query, TaskVolume};
    use acyclic_stream::{ChildrenPageRequest, StreamPath, StreamProvider as _};
    let client = std::sync::Arc::new(client(cwd).await?);
    for parent in ["external/requests", "external/results"] {
        let page = client
            .children_page(ChildrenPageRequest {
                parent: Some(StreamPath::new(parent)?),
                after: None,
                hierarchy_version: None,
                limit: 64,
            })
            .await?;
        assert!(
            page.next_after.is_none(),
            "native regression exceeded its task bound"
        );
        for child in page.children {
            let files = TaskVolume::open_existing(client.clone(), child.path.clone()).await?;
            for file in files.snapshot(&Query::Root, 128).await?.files {
                let preview = &file.bytes[..file.bytes.len().min(2048)];
                eprintln!(
                    "{} {}: {}",
                    child.path.as_str(),
                    file.key.path(),
                    String::from_utf8_lossy(preview)
                );
            }
        }
    }
    Ok(())
}
