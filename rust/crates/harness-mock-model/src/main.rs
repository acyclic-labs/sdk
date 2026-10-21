//! Real loopback HTTP server. Stdout is a flushed, line-oriented event stream.
use axum::body::{Body, Bytes};
use futures::{StreamExt as _, stream};
use harness_mock_model::{Engine, Script, bind_result, events_with_usage};
use http_body_util::BodyExt as _;
use hyper::{Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::{cell::RefCell, convert::Infallible, fs::File, io::Write as _, rc::Rc};

struct Host {
    engine: Engine,
    record: recorder::Recorder,
    sequence: u64,
}

mod recorder;

struct RecordedReply {
    reply: Response<Body>,
    committed: tokio::sync::oneshot::Receiver<std::io::Result<()>>,
}

fn response(status: u16, kind: &str, body: Body) -> Response<Body> {
    let mut response = Response::new(body);
    *response.status_mut() =
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    response.headers_mut().insert(
        "content-type",
        if kind == "sse" {
            hyper::header::HeaderValue::from_static("text/event-stream")
        } else {
            hyper::header::HeaderValue::from_static("application/json")
        },
    );
    response
}

fn answer(
    host: &mut Host,
    method: &str,
    path: &str,
    body: Value,
) -> std::io::Result<RecordedReply> {
    if method != "POST" {
        return record(
            host,
            method,
            path,
            body,
            None,
            response(405, "json", Body::from("{\"error\":\"POST required\"}")),
        );
    }
    let (namespace, endpoint) = path
        .trim_start_matches('/')
        .split_once('/')
        .unwrap_or(("", ""));
    let endpoint = endpoint.strip_prefix("v1/").unwrap_or(endpoint);
    let count = matches!(
        endpoint,
        "count" | "responses/input_tokens" | "messages/count_tokens"
    );
    let tokens = host.engine.input_tokens(namespace, &body).unwrap_or(0);
    let (step, reply) = if let Some((index, step)) = host.engine.take(namespace, endpoint, &body) {
        let status = step.status.unwrap_or(200);
        let bound = bind_result(step, &body);
        let reply = if let Err(error) = &bound {
            response(500, "json", Body::from(json!({"error":error}).to_string()))
        } else if let Some(json) = bound.as_ref().ok().and_then(|step| step.json.as_ref()) {
            let body = if step.hold {
                Body::from_stream(stream::pending::<Result<Bytes, Infallible>>())
            } else {
                Body::from(json.to_string())
            };
            response(status, "json", body)
        } else if let Some(events) = bound
            .as_ref()
            .ok()
            .and_then(|bound| events_with_usage(endpoint, bound, tokens))
        {
            let chunks = stream::iter(
                events
                    .into_iter()
                    .map(|text| Ok::<_, Infallible>(Bytes::from(text))),
            );
            let hold = step.hold;
            let tail = stream::once(async move {
                if hold {
                    std::future::pending::<()>().await;
                }
                Ok::<_, Infallible>(Bytes::new())
            });
            response(status, "sse", Body::from_stream(chunks.chain(tail)))
        } else {
            response(
                404,
                "json",
                Body::from("{\"error\":\"unsupported endpoint\"}"),
            )
        };
        (Some(index), reply)
    } else if count {
        match host.engine.input_tokens(namespace, &body) {
            Some(tokens) => (
                None,
                response(
                    200,
                    "json",
                    Body::from(json!({"input_tokens":tokens}).to_string()),
                ),
            ),
            None => (
                None,
                response(404, "json", Body::from("{\"error\":\"unknown namespace\"}")),
            ),
        }
    } else {
        (None, response(409,"json",Body::from(json!({"error":"no unused script step matches","case":namespace,"path":endpoint}).to_string())))
    };
    record(host, method, path, body, step, reply)
}

fn record(
    host: &mut Host,
    method: &str,
    path: &str,
    body: Value,
    step: Option<usize>,
    reply: Response<Body>,
) -> std::io::Result<RecordedReply> {
    let (namespace, endpoint) = path
        .trim_start_matches('/')
        .split_once('/')
        .unwrap_or(("", ""));
    let endpoint = endpoint.strip_prefix("v1/").unwrap_or(endpoint);
    host.sequence += 1;
    let record = json!({"sequence":host.sequence,"case":namespace,"method":method,"path":endpoint,"body":body,"step":step,"status":reply.status().as_u16()});
    // Consumption and enqueue share one executor turn. The ordered writer
    // owns append -> flush -> event; the HTTP reply waits for that boundary.
    let event = json!({"event":"request","sequence":host.sequence,"case":namespace,"path":endpoint,"step":step});
    Ok(RecordedReply {
        reply,
        committed: host
            .record
            .enqueue(json_line(&record)?, json_line(&event)?)?,
    })
}

fn json_line(record: &Value) -> std::io::Result<Vec<u8>> {
    // Value's Display streams JSON fragments. Writing it directly to File
    // turns every fragment into a syscall on the one HTTP executor, holding
    // up every concurrent client. Assemble one complete JSONL record first.
    let mut bytes = serde_json::to_vec(record).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    Ok(bytes)
}

async fn serve(
    host: Rc<RefCell<Host>>,
    request: Request<Incoming>,
) -> Result<Response<Body>, Infallible> {
    let method = request.method().to_string();
    let path = request.uri().path().to_owned();
    let received = async {
        let bytes = request
            .into_body()
            .collect()
            .await
            .map_err(std::io::Error::other)?
            .to_bytes();
        if bytes.is_empty() {
            Ok(Value::Null)
        } else {
            serde_json::from_slice(&bytes).map_err(std::io::Error::other)
        }
    }
    .await;
    let result = match received {
        Ok(body) => answer(&mut host.borrow_mut(), &method, &path, body),
        Err(error) => record(
            &mut host.borrow_mut(),
            &method,
            &path,
            json!({"invalid_body":error.to_string()}),
            None,
            response(
                400,
                "json",
                Body::from("{\"error\":\"invalid request body\"}"),
            ),
        ),
    };
    let result = match result {
        Ok(recorded) => match recorded.committed.await {
            Ok(Ok(())) => Ok(recorded.reply),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(std::io::Error::other(error)),
        },
        Err(error) => Err(error),
    };
    Ok(result.unwrap_or_else(|error| {
        response(
            500,
            "json",
            Body::from(json!({"error":error.to_string()}).to_string()),
        )
    }))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [flag, script, record_flag, record] = args.as_slice() else {
        return Err(
            "usage: harness-mock-model --script SCRIPT.json --record REQUESTS.jsonl".into(),
        );
    };
    if flag != "--script" || record_flag != "--record" {
        return Err("expected --script and --record".into());
    }
    let script: Script = serde_json::from_slice(&std::fs::read(script)?)?;
    let host = Rc::new(RefCell::new(Host {
        engine: Engine::new(script),
        record: recorder::Recorder::open(File::create(record)?)?,
        sequence: 0,
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    println!(
        "{}",
        json!({"event":"ready","port":listener.local_addr()?.port()})
    );
    std::io::stdout().flush()?;
    tokio::task::LocalSet::new()
        .run_until(async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(connection) => connection,
                    Err(error) => return Err(error),
                };
                let host = host.clone();
                tokio::task::spawn_local(async move {
                    let service = service_fn(move |request| serve(host.clone(), request));
                    if let Err(error) = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(socket), service)
                        .await
                    {
                        eprintln!("connection: {error}");
                    }
                });
            }
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Writes {
        bytes: Vec<u8>,
        calls: usize,
        flushed: usize,
    }

    impl std::io::Write for Writes {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.calls += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushed = self.bytes.len();
            Ok(())
        }
    }

    #[test]
    fn request_record_is_one_complete_append_before_its_event() -> std::io::Result<()> {
        let record = json!({"body": {"messages": [{"text": "hé λ 🙂\n"}], "tools": [1, 2, 3]}});
        let complete = |writes: &Writes| writes.calls == 1 && writes.flushed == writes.bytes.len();
        let mut writes = Writes::default();
        writes.write_all(&json_line(&record)?)?;
        writes.flush()?;
        assert!(complete(&writes));
        assert_eq!(writes.bytes.last(), Some(&b'\n'));
        assert_eq!(serde_json::from_slice::<Value>(&writes.bytes)?, record);

        // Negative control: the old formatting path has identical bytes but
        // performs one write per serializer fragment on the HTTP executor.
        let mut old = Writes::default();
        writeln!(old, "{record}")?;
        old.flush()?;
        assert_eq!(old.bytes, writes.bytes);
        assert!(!complete(&old));
        Ok(())
    }
}
