//! F9 through the shared shipping entrypoint, native hooks and real HTTP.
#![cfg(all(feature = "filesystem-local", feature = "native-execution", unix))]
use acyclic_harness::{
    Admission, LocalHarness, LocalHarnessConfig, OperationId, Result,
    context::{CompactionPolicy, ModelContextCapacity},
    native_settings::SettingsConfig,
    runtime::RuntimeTask,
};
use acyclic_harness_providers::{
    Adapter, Authentication, Credentials, ModelFeatures, ModelSpec, ProviderSpec, Registry,
    TokenCountField, TokenCounterSpec,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing_subscriber::{Layer, layer::SubscriberExt};

const EVENTS: [&str; 5] = [
    "SessionStart",
    "UserPromptSubmit",
    "PreToolUse",
    "PostToolUse",
    "Stop",
];

// A local HTTP peer, using the provider's actual Reqwest transport. Each reply
// closes its connection; the recorder channel has no locks or timing waits.
async fn serve(
    listener: tokio::net::TcpListener,
    sent: tokio::sync::mpsc::UnboundedSender<Value>,
) -> std::io::Result<()> {
    let mut step = 0;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let mut bytes = Vec::new();
        let (header, length) = loop {
            let mut chunk = [0; 4096];
            let count = socket.read(&mut chunk).await?;
            if count == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < header + length {
            let mut chunk = [0; 4096];
            let count = socket.read(&mut chunk).await?;
            if count == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            bytes.extend_from_slice(&chunk[..count]);
        }
        let body: Value = serde_json::from_slice(&bytes[header..header + length]).unwrap();
        if bytes.starts_with(b"POST /count ") {
            let body = r#"{"input_tokens":1}"#;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
            continue;
        }
        sent.send(body).unwrap();
        let event = if step == 0 {
            json!({"type":"response.output_item.done","output_index":0,"item":{
                "type":"function_call","call_id":"bash-call","name":"bash",
                "arguments":"{\"command\":\"printf original > executed\"}"}})
        } else {
            json!({"type":"response.output_text.delta","delta":if step == 1 {"provisional"} else {"done"}})
        };
        step += 1;
        let body = format!(
            "data: {event}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"status\":\"completed\",\"usage\":{{\"input_tokens\":1,\"output_tokens\":1}}}}}}\n\n"
        );
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
    }
}

/// The same detector is used for the candidate and each omitted-dispatch
/// control. This proves the shipping-path oracle observes every event, the
/// effective invocation, continuation context and terminal replay.
async fn lifecycle(
    omitted: Option<&str>,
    denied: bool,
) -> std::result::Result<bool, Box<dyn std::error::Error>> {
    // Keep the command's original cause visible if the host's existing
    // dispatch boundary collapses it to Indeterminate (tracked by #1020).
    let _trace = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_test_writer()
                .with_filter(tracing_subscriber::filter::Targets::new().with_target(
                    "acyclic_harness::filesystem::task_runtime::dispatch",
                    tracing::Level::TRACE,
                )),
        ),
    );
    let root = tempfile::tempdir()?;
    let workspace = root.path().join("workspace");
    let home = root.path().join("home");
    std::fs::create_dir(&workspace)?;
    std::fs::create_dir_all(home.join(".claude"))?;
    let script = root.path().join("hook.sh");
    std::fs::write(
        &script,
        r#"set -eu
input=$(cat)
printf '%s\n' "$input" >> hook-inputs.jsonl
case "$1" in
SessionStart|UserPromptSubmit) printf '{"hookSpecificOutput":{"additionalContext":"%s-context"}}' "$1";;
PreToolUse) if [ "$2" = true ]; then printf '%s' '{"decision":"block","reason":"denied-by-hook"}'; else printf '%s' '{"hookSpecificOutput":{"updatedInput":{"command":"printf rewritten > executed"}}}'; fi;;
PostToolUse) printf '%s' '{"hookSpecificOutput":{"additionalContext":"PostToolUse-context"}}';;
Stop) case "$input" in *'"stop_hook_active":true'*) ;; *) printf '%s' '{"decision":"block","reason":"Stop-context"}';; esac;;
esac
"#,
    )?;
    let hooks: serde_json::Map<String, Value> = EVENTS
        .into_iter()
        .filter(|event| Some(*event) != omitted)
        .map(|event| {
            (
                event.into(),
                json!([{"hooks":[{
                    "type":"command","command":"sh","args":[script,event,denied.to_string()],"onFailure":"block"
                }]}]),
            )
        })
        .collect();
    std::fs::write(
        home.join(".claude/settings.json"),
        json!({"hooks":hooks}).to_string(),
    )?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (sent, mut requests) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(serve(listener, sent));
    // Aborting on every return also keeps failed assertions from leaking a peer.
    struct Server(tokio::task::JoinHandle<std::io::Result<()>>);
    impl Drop for Server {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _server = Server(server);
    let mut registry = Registry::new(Credentials::explicit(BTreeMap::new()))?;
    registry.register(
        "fixture".into(),
        ProviderSpec {
            adapter: Adapter::Responses,
            authentication: Authentication::Optional,
            base_url: endpoint,
            headers: BTreeMap::new(),
            models: BTreeMap::from([(
                "model".into(),
                ModelSpec {
                    max_context_tokens: None,
                    capacity: ModelContextCapacity {
                        context_tokens: 32_768,
                        output_tokens: Some(4096),
                    },
                    features: ModelFeatures {
                        tools: true,
                        images: false,
                    },
                },
            )]),
            token_counter: Some(TokenCounterSpec {
                path: "count".into(),
                adapter: Adapter::Responses,
                response_field: TokenCountField::InputTokens,
                framing_tokens: 0,
            }),
            max_event_bytes: 4096,
            retry: Default::default(),
            timeouts: Default::default(),
        },
    )?;
    let provider = Arc::new(registry.resolve("fixture/model")?);
    let config = || -> Result<LocalHarnessConfig> {
        Ok(acyclic_harness::test_support::local_harness_config(
            &workspace,
            (provider.model().clone(), provider.clone()),
        )?
        .state_directory(root.path().join("state"))
        .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex")))
        .compaction(CompactionPolicy::Disabled))
    };
    let harness = LocalHarness::open(config()?).await?;
    let Admission::Accepted(RuntimeTask::Durable { task_id: task, .. }) =
        harness.submit(OperationId::new(), "inspect hooks").await?
    else {
        return Err("hook fixture was not durably admitted".into());
    };
    let output = match harness.resume(task).await {
        Ok(output) => output,
        Err(error) => {
            eprintln!(
                "omitted={omitted:?}, denied={denied}, hook inputs={:?}",
                std::fs::read_to_string(workspace.join("hook-inputs.jsonl"))
            );
            while let Ok(body) = requests.try_recv() {
                eprintln!("request={body}");
            }
            return Err(error.into());
        }
    };
    drop(harness);
    // Reopen after removing the executable and settings: retained outcomes and
    // terminal output must stand alone, without another hook or HTTP exchange.
    std::fs::remove_file(&script)?;
    std::fs::remove_file(home.join(".claude/settings.json"))?;
    let harness = LocalHarness::open(config()?).await?;
    let replay = harness.resume(task).await?;
    let mut bodies = Vec::new();
    while let Ok(body) = requests.try_recv() {
        bodies.push(body);
    }
    let inputs = std::fs::read_to_string(workspace.join("hook-inputs.jsonl"))?;
    let inputs: Vec<Value> = inputs
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let events: Vec<_> = inputs
        .iter()
        .map(|input| input["hook_event_name"].as_str().unwrap())
        .collect();
    let mut expected = vec![
        "SessionStart",
        "UserPromptSubmit",
        "PreToolUse",
        "PostToolUse",
        "Stop",
        "Stop",
    ];
    if denied {
        expected.retain(|event| *event != "PostToolUse");
    }
    let has = |index: usize, text: &str| {
        bodies
            .get(index)
            .is_some_and(|body| body.to_string().contains(text))
    };
    let passed = output == replay
        && output.text == "done"
        && bodies.len() == 3
        && events == expected
        && inputs[2]["tool_input"]["command"] == "printf original > executed"
        && if denied {
            !workspace.join("executed").exists() && has(1, "denied-by-hook")
        } else {
            std::fs::read(workspace.join("executed"))? == b"rewritten"
                && inputs[3]["tool_input"]["command"] == "printf rewritten > executed"
                && inputs[3]["tool_response"]["success"] == true
                && has(1, "PostToolUse-context")
        }
        && has(0, "SessionStart-context")
        && has(0, "UserPromptSubmit-context")
        && has(2, "Stop-context")
        && has(2, "provisional");
    if !passed && omitted.is_none() {
        eprintln!("output={output:?}, replay={replay:?}, events={events:?}, requests={bodies:?}");
    }
    Ok(passed)
}

#[tokio::test]
async fn lifecycle_http_detects_each_missing_dispatch_and_replays_without_effects()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    assert!(
        lifecycle(None, false).await?,
        "healthy lifecycle failed its shipping-path oracle"
    );
    for event in EVENTS {
        assert!(
            !lifecycle(Some(event), false).await?,
            "missing {event} escaped the shipping-path oracle"
        );
    }
    assert!(lifecycle(None, true).await?, "denied tool was not refused");
    Ok(())
}
