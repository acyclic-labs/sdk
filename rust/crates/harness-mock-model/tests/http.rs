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
async fn real_server_records_all_protocols_and_isolates_parallel_cases() {
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("script.json");
    let record = directory.path().join("requests.jsonl");
    let mut cases = serde_json::Map::new();
    for namespace in ["responses", "chat", "anthropic"] {
        cases.insert(namespace.into(),json!({"count_bytes":true,"steps":[
            {"when":{"user":"hello"},"text":["hé","\n**world**"],"tools":[{"id":"call_1","name":"read","arguments":{"path":"a.txt"}}]},
            {"when":{"tool_result":"tool-result"},"text":["done"]}
        ]}));
    }
    cases.insert("errors".into(), json!({"steps":[{"tools":[{"id":"missing","name":"read","arguments":{"file":{"$tool_result":"/file"}}}]}]}));
    cases.insert("codex".into(),json!({"steps":[
        {"tools":[{"id":"spawn","namespace":"collaboration","name":"spawn_agent","arguments":{"message":{"$request":"/metadata/prompt"}}}]},
        {"when":{"tool_result":"child-id"},"custom_tools":[{"id":"patch","name":"apply_patch","input":"*** Begin Patch\n*** Add File: {{tool_result:/mount}}/child.txt\n+{{tool_result:/id}}\n*** End Patch"}]},
        {"when":{"tool_result":"applied"},"json":{"echo":{"$request":"/input/0/output"}}}
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
        Command::new(env!("CARGO_BIN_EXE_harness-mock-model"))
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
                + json!({"messages":[{"role":"user","content":"hello"}]})
                    .to_string()
                    .len() as u64;
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
            assert_eq!(count["input_tokens"], 66);
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
    for _ in 0..23 {
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
    assert_eq!(records.len(), 23);
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
