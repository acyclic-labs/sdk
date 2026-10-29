use super::*;

/// Echoes `echo`, parks `hold` until released, and records cancellations.
#[derive(Default)]
struct Fixture {
    released: bool,
    cancelled: Vec<String>,
}

impl ToolProvider for Fixture {
    fn tools(&self) -> Value {
        json!({"tools":[{"name":"echo"},{"name":"hold"},{"name":"strict"}]})
    }

    fn call(&mut self, name: &str, arguments: &Value) -> Result<ToolCall> {
        Ok(match name {
            "echo" => ToolCall::Answer(arguments.clone()),
            "strict" => ToolCall::Invalid("strict takes no arguments".into()),
            _ => ToolCall::Park("held".into()),
        })
    }

    fn cancel(&mut self, key: &str) {
        self.cancelled.push(key.into());
    }

    fn progress(
        &mut self,
        parked: &BTreeSet<String>,
        settled: &mut Vec<(String, Settled)>,
    ) -> Result<Option<Wake<'_>>> {
        if self.released && parked.contains("held") {
            settled.push(("held".into(), Err(json!({"code":"released"}))));
        }
        Ok(None)
    }
}

/// Output the test reads back once the server returns.
#[derive(Clone, Default)]
struct Written(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl Write for Written {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serve(provider: &mut Fixture, input: &str) -> (Result<()>, Vec<Value>) {
    let output = Written::default();
    let served = ToolServer {
        name: "fixture",
        version: "1",
        parked: 1,
        frame_bytes: 256,
    }
    .serve(provider, io::Cursor::new(input.to_owned()), output.clone());
    let output = output
        .0
        .lock()
        .map(|bytes| bytes.clone())
        .unwrap_or_default();
    let replies = String::from_utf8_lossy(&output)
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    (served, replies)
}

fn code(reply: &Value) -> Option<i64> {
    reply.pointer("/error/code").and_then(Value::as_i64)
}

#[test]
fn protocol_is_validated_negotiated_and_initialized_first() {
    let mut provider = Fixture::default();
    let (served, replies) = serve(
        &mut provider,
        concat!(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":2,"method":"initialize","params":{}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":4,"method":"initialize","params":{"protocolVersion":"1999-01-01"}}"#,
            "\n",
            r#"{"id":5,"method":"ping"}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":{},"method":"ping"}"#,
            "\n",
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"absent"}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"strict"}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"echo","arguments":{"value":1}}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":9,"method":"other"}"#,
            "\n",
        ),
    );
    assert!(served.is_ok());
    let codes = replies.iter().map(code).collect::<Vec<_>>();
    assert_eq!(
        codes,
        [
            Some(-32000),
            Some(-32602),
            None,
            None,
            Some(-32600),
            Some(-32600),
            Some(-32602),
            Some(-32602),
            None,
            Some(-32601)
        ]
    );
    assert_eq!(at(&replies, 2)["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(
        at(&replies, 3)["result"]["protocolVersion"],
        PROTOCOL_VERSION
    );
    assert_eq!(
        at(&replies, 8)["result"],
        json!({"content":[{"type":"text","text":"{\"value\":1}"}],
            "structuredContent":{"value":1},"isError":false})
    );
}

#[test]
fn parked_calls_cancel_once_and_bound_their_count() {
    let mut provider = Fixture::default();
    let (served, replies) = serve(
        &mut provider,
        concat!(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"hold"}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"hold"}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":4,"method":"ping"}"#,
            "\n",
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}"#,
            "\n",
        ),
    );
    assert!(served.is_ok());
    let ids = replies
        .iter()
        .map(|reply| reply["id"].clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, [json!(1), json!(3), json!(4), json!(2)]);
    assert_eq!(at(&replies, 1)["result"]["isError"], true);
    assert_eq!(code(&at(&replies, 3)), Some(-32800));
    assert_eq!(provider.cancelled, ["held", "held"]);
}

#[test]
fn settled_failures_answer_their_parked_call() {
    let mut provider = Fixture {
        released: true,
        ..Fixture::default()
    };
    let (_, replies) = serve(
        &mut provider,
        concat!(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"hold"}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":3,"method":"ping"}"#,
            "\n",
        ),
    );
    assert_eq!(at(&replies, 1)["id"], 2);
    assert_eq!(at(&replies, 1)["result"]["isError"], true);
    assert_eq!(
        at(&replies, 1)["result"]["structuredContent"],
        json!({"code":"released"})
    );
}

#[test]
fn undecodable_input_answers_a_parse_error_and_ends_the_server() {
    let mut provider = Fixture::default();
    let oversized = format!("{}\n", "x".repeat(512));
    for input in ["not json\n", oversized.as_str()] {
        let (served, replies) = serve(&mut provider, input);
        assert!(served.is_err());
        assert_eq!(replies.iter().map(code).collect::<Vec<_>>(), [Some(-32700)]);
    }
}

/// A client that reads slower than requests arrive.
struct Slow(Written);

impl Write for Slow {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        std::thread::sleep(std::time::Duration::from_millis(1));
        self.0.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn request_bursts_wait_for_a_slow_reader() {
    let mut input =
        r#"{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}"#
            .to_owned();
    input.push('\n');
    for id in 1..=100 {
        input.push_str(&format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"ping"}}"#));
        input.push('\n');
    }
    let output = Written::default();
    let served = ToolServer {
        name: "fixture",
        version: "1",
        parked: 1,
        frame_bytes: 256,
    }
    .serve(
        &mut Fixture::default(),
        io::Cursor::new(input),
        Slow(output.clone()),
    );
    assert!(served.is_ok());
    let output = output
        .0
        .lock()
        .map(|bytes| bytes.clone())
        .unwrap_or_default();
    let ids = String::from_utf8_lossy(&output)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|reply| reply["id"].as_u64())
        .collect::<Vec<_>>();
    assert_eq!(ids, (0..=100).collect::<Vec<_>>());
}

fn at(replies: &[Value], index: usize) -> &Value {
    replies.get(index).unwrap_or(&Value::Null)
}
