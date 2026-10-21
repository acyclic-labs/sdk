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
        cases.insert(namespace.into(),json!({"steps":[
            {"when":{"user":"hello"},"text":["hé","\n**world**"],"tools":[{"id":"call_1","name":"read","arguments":{"path":"a.txt"}}]},
            {"when":{"tool_result":"tool-result"},"text":["done"]}
        ]}));
    }
    cases.insert("errors".into(), json!({"steps":[{"tools":[{"id":"missing","name":"read","arguments":{"file":{"$tool_result":"/file"}}}]}]}));
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
            let count = client
                .post(format!("{root}/{namespace}/v1/count"))
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            assert_eq!(count["input_tokens"], 64);
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
    for _ in 0..18 {
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
    assert_eq!(records.len(), 18);
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
