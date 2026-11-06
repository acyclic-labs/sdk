//! One shared real process, three concurrent HTTP adapter conversations.
use serde_json::{Value, json};
use std::{
    io::{BufRead as _, BufReader},
    process::{Command, Stdio},
};

struct Server(std::process::Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn native_capability_authenticates_before_body_or_script_consumption() {
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("script.json");
    let record = directory.path().join("requests.jsonl");
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../scripts/graphcoder-e2e/cases/installed-pi-recursive.json"
    ))
    .unwrap();
    std::fs::write(
        &script,
        json!({"cases":{"native-installed-pi-recursive":fixture["model"]}}).to_string(),
    )
    .unwrap();
    // This is the server's actual issued credential, never a provider key or
    // a placeholder bypassing the native SDK's authentication requirement.
    let capability = "1234567890abcdef".repeat(4);
    let mut server = Server(
        Command::new(acyclic_test_support::cargo_binary!("harness-mock-model"))
            .args([
                "--script",
                script.to_str().unwrap(),
                "--record",
                record.to_str().unwrap(),
            ])
            .env("ACYCLIC_E2E_MODEL_CAPABILITY", &capability)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut lines = BufReader::new(server.0.stdout.take().unwrap()).lines();
    let ready = lines.next().unwrap().unwrap();
    assert!(!ready.contains(&capability));
    let ready: Value = serde_json::from_str(&ready).unwrap();
    assert_eq!(ready["event"], "ready");
    let root = format!("http://127.0.0.1:{}", ready["port"]);
    let url = format!("{root}/native-installed-pi-recursive/v1/responses");
    let client = reqwest::Client::new();
    let body =
        include_str!("../../../../plugin/tests/hosts/fixtures/native-pi-fixture-request.json");
    let authorization = format!("Bearer {capability}");
    for supplied in [
        None,
        Some(format!("Bearer {}0", capability)),
        Some(format!("Bearer {}0", &capability[..63])),
        Some(format!("bearer {capability}")),
        Some(capability.clone()),
    ] {
        let mut request = client.post(&url).body("{");
        if let Some(supplied) = supplied {
            request = request.header("authorization", supplied);
        }
        let denied = request.send().await.unwrap();
        assert_eq!(denied.status(), 401);
        assert!(denied.bytes().await.unwrap().is_empty());
    }
    assert!(std::fs::read(&record).unwrap().is_empty());
    let alternate = client
        .post(format!(
            "{root}//native-installed-pi-recursive/v1/responses"
        ))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(alternate.status(), 401);
    assert!(std::fs::read(&record).unwrap().is_empty());
    // Removing the protected prefix cannot select the same script anonymously.
    let anonymous = client
        .post(format!("{root}/installed-pi-recursive/v1/responses"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 409);
    let admitted = client
        .post(&url)
        .header("authorization", authorization)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(admitted.status(), 200);
    assert!(admitted.text().await.unwrap().contains("native_parent"));
    for _ in 0..2 {
        let event = lines.next().unwrap().unwrap();
        assert!(!event.contains(&capability));
        assert!(event.contains("\"event\":\"request\""));
    }
    let recorded = std::fs::read_to_string(&record).unwrap();
    assert!(!recorded.contains(&capability));
    let records: Vec<Value> = recorded
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["step"], Value::Null);
    assert_eq!(records[1]["case"], "installed-pi-recursive");
    assert_eq!(records[1]["step"], 0);
    assert_eq!(
        records[1]["body"],
        serde_json::from_str::<Value>(body).unwrap()
    );
    // The original process is the response producer. Retirement closes its
    // listener; no readiness loop or replacement server can satisfy this call.
    server.0.kill().unwrap();
    server.0.wait().unwrap();
    assert!(client.post(&url).body(body).send().await.is_err());
}

#[tokio::test]
async fn real_server_records_all_protocols_and_isolates_parallel_cases() {
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("script.json");
    let record = directory.path().join("requests.jsonl");
    let mut cases = serde_json::Map::new();
    for namespace in ["responses", "chat", "anthropic"] {
        cases.insert(namespace.into(),json!({"bytes_per_token":4,"steps":[
            {"when":{"user":"hello"},"text":["hé","\n**world**"],"tools":[{"id":"call_1","name":"read","arguments":{"path":"a.txt"}}]},
            {"when":{"tool_result":"tool-result"},"text":["done"]}
        ]}));
    }
    cases.insert("errors".into(), json!({"steps":[{"tools":[{"id":"missing","name":"read","arguments":{"file":{"$tool_result":"/file"}}}]}]}));
    cases.insert("codex".into(),json!({"steps":[
        {"tools":[{"id":"spawn","namespace":"collaboration","name":"spawn_agent","arguments":{"message":{"$request":"/metadata/prompt"}}}]},
        {"when":{"tool_result":"child-id"},"custom_tools":[{"id":"patch","name":"apply_patch","input":"*** Begin Patch\n*** Add File: {{tool_result:/mount}}/child.txt\n+{{tool_result:/id}}\n*** End Patch"}]},
        {"when":{"tool_result":"applied"},"json":{"echo":{"$request":"/input/0/output"}}},
        {"json":{"$request":"/value"}}
    ]}));
    cases.insert(
        "bad-template".into(),
        json!({"steps":[{"text":["{{request:/absent}}"]}]}),
    );
    cases.insert(
        "mixed".into(),
        json!({"steps":[{
            "when":{"user":"latest user","tool_result":"last-file"},
            "tools":[{"id":"bound","name":"edit","arguments":{"file":{"$tool_result":"/file"}}}]
        }]}),
    );
    std::fs::write(&script, json!({"cases":cases}).to_string()).unwrap();
    let mut server = Server(
        Command::new(acyclic_test_support::cargo_binary!("harness-mock-model"))
            .args([
                "--script",
                script.to_str().unwrap(),
                "--record",
                record.to_str().unwrap(),
            ])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut lines = BufReader::new(server.0.stdout.take().unwrap()).lines();
    let ready: Value = serde_json::from_str(&lines.next().unwrap().unwrap()).unwrap();
    assert_eq!(ready["event"], "ready");
    let root = format!("http://127.0.0.1:{}", ready["port"]);
    let client = reqwest::Client::new();
    let run = |namespace: &'static str, path: &'static str| {
        let client = client.clone();
        let root = root.clone();
        async move {
            let url = format!("{root}/{namespace}/v1/{path}");
            let wrong = client
                .post(&url)
                .json(&json!({"messages":[{"role":"user","content":"wrong"}]}))
                .send()
                .await
                .unwrap();
            assert_eq!(wrong.status(), 409);
            let first = client
                .post(&url)
                .json(&json!({"messages":[{"role":"user","content":"hello"}]}))
                .send()
                .await
                .unwrap();
            assert!(first.status().is_success());
            assert_eq!(first.headers()["content-type"], "text/event-stream");
            let stream = first.text().await.unwrap();
            assert!(stream.contains("hé"));
            assert!(stream.contains("call_1"));
            assert!(stream.contains("a.txt"));
            let expected = 64
                + (json!({"messages":[{"role":"user","content":"hello"}]})
                    .to_string()
                    .len() as u64)
                    .div_ceil(4);
            let events: Vec<Value> = stream
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect();
            let usage = events
                .iter()
                .find_map(|event| {
                    event
                        .pointer("/response/usage")
                        .or_else(|| event.pointer("/message/usage"))
                        .or_else(|| event.get("usage"))
                })
                .unwrap();
            assert_eq!(
                usage
                    .get("input_tokens")
                    .or_else(|| usage.get("prompt_tokens"))
                    .unwrap(),
                expected
            );
            let count = client
                .post(format!("{root}/{namespace}/v1/count"))
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            assert_eq!(count["input_tokens"], 65);
            let next = client
                .post(&url)
                .json(&json!({"messages":[{"role":"tool","content":"tool-result"}]}))
                .send()
                .await
                .unwrap();
            assert!(next.text().await.unwrap().contains("done"));
            assert_eq!(
                client
                    .post(&url)
                    .json(&json!({"messages":[{"role":"user","content":"hello"}]}))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                409
            );
        }
    };
    tokio::join!(
        run("responses", "responses"),
        run("chat", "chat/completions"),
        run("anthropic", "messages")
    );
    let url = format!("{root}/codex/v1/responses");
    let spawn = client
        .post(&url)
        .json(&json!({"metadata":{"prompt":"child task"}}))
        .send()
        .await
        .unwrap();
    assert!(spawn.status().is_success());
    let frames = |wire: String| -> Vec<Value> {
        wire.lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    };
    let spawn = frames(spawn.text().await.unwrap());
    let item = &spawn
        .iter()
        .find(|event| event["type"] == "response.output_item.done")
        .unwrap()["item"];
    assert_eq!(item["namespace"], "collaboration");
    assert_eq!(item["name"], "spawn_agent");
    assert_eq!(
        serde_json::from_str::<Value>(item["arguments"].as_str().unwrap()).unwrap(),
        json!({"message":"child task"})
    );
    let usage = &spawn.last().unwrap()["response"]["usage"];
    assert_eq!(
        usage,
        &json!({"input_tokens":64,"output_tokens":2,"total_tokens":66})
    );
    let patch = client.post(&url).json(&json!({"input":[{"type":"function_call_output","call_id":"spawn","output":"{\"id\":\"child-id\",\"mount\":\"/real/mount\"}"}]})).send().await.unwrap();
    assert!(patch.status().is_success());
    let patch = frames(patch.text().await.unwrap());
    let expected = "*** Begin Patch\n*** Add File: /real/mount/child.txt\n+child-id\n*** End Patch";
    let item = &patch
        .iter()
        .find(|event| event["type"] == "response.output_item.done")
        .unwrap()["item"];
    assert_eq!(item["type"], "custom_tool_call");
    assert_eq!(item["call_id"], "patch");
    assert_eq!(item["input"], expected);
    assert!(patch.iter().any(
        |event| event["type"] == "response.custom_tool_call_input.delta"
            && event["delta"] == expected
    ));
    let echoed = client.post(&url).json(&json!({"input":[{"type":"custom_tool_call_output","call_id":"patch","output":"applied"}]})).send().await.unwrap();
    assert!(echoed.status().is_success());
    assert_eq!(
        echoed.json::<Value>().await.unwrap(),
        json!({"echo":"applied"})
    );
    let null = client
        .post(&url)
        .json(&json!({"value":null}))
        .send()
        .await
        .unwrap();
    assert!(null.status().is_success());
    assert_eq!(null.headers()["content-type"], "application/json");
    assert_eq!(null.text().await.unwrap(), "null");
    assert_eq!(
        client
            .post(format!("{root}/bad-template/v1/responses"))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        500
    );
    let mixed = client
        .post(format!("{root}/mixed/v1/messages"))
        .json(&json!({"messages":[
            {"role":"user","content":"older user"},
            {"role":"user","content":[
                {"type":"tool_result","tool_use_id":"first","content":"{\"file\":\"first-file\"}"},
                {"type":"text","text":"latest user"},
                {"type":"tool_result","tool_use_id":"last","content":"{\"file\":\"last-file\"}"}
            ]}
        ]}))
        .send()
        .await
        .unwrap();
    assert!(mixed.status().is_success());
    let mixed = mixed.text().await.unwrap();
    assert!(mixed.contains("last-file"));
    assert!(!mixed.contains("first-file"));
    assert_eq!(
        client
            .post(format!("{root}/errors/v1/responses"))
            .body("{")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        client
            .get(format!("{root}/errors/v1/responses"))
            .send()
            .await
            .unwrap()
            .status(),
        405
    );
    assert_eq!(
        client
            .post(format!("{root}/errors/v1/responses"))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        500
    );
    for _ in 0..24 {
        assert!(
            lines
                .next()
                .unwrap()
                .unwrap()
                .contains("\"event\":\"request\"")
        );
    }
    let records: Vec<Value> = std::fs::read_to_string(record)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 24);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record["sequence"], index + 1);
    }
    for namespace in ["responses", "chat", "anthropic"] {
        let records: Vec<_> = records
            .iter()
            .filter(|record| record["case"] == namespace)
            .collect();
        assert_eq!(records.iter().filter(|r| r["step"] == 0).count(), 1);
        assert_eq!(records.iter().filter(|r| r["step"] == 1).count(), 1);
        assert_eq!(records.iter().filter(|r| r["status"] == 409).count(), 2);
    }
}
