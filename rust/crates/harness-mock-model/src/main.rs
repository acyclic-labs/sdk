//! Real loopback HTTP server. Stdout is a flushed, line-oriented event stream.
use axum::body::{Body, Bytes};
use futures::{StreamExt as _, stream};
use harness_mock_model::{Engine, Script, bind_result, events};
use http_body_util::BodyExt as _;
use hyper::{Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::{cell::RefCell, convert::Infallible, fs::File, io::Write as _, rc::Rc};

struct Host {
    engine: Engine,
    record: File,
    sequence: u64,
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
) -> std::io::Result<Response<Body>> {
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
    let (step, reply) = if let Some((index, step)) = host.engine.take(namespace, endpoint, &body) {
        let status = step.status.unwrap_or(200);
        let reply = if let Some(json) = &step.json {
            let body = if step.hold {
                Body::from_stream(stream::pending::<Result<Bytes, Infallible>>())
            } else {
                Body::from(json.to_string())
            };
            response(status, "json", body)
        } else if let Err(error) = bind_result(step, &body) {
            response(500, "json", Body::from(json!({"error":error}).to_string()))
        } else if let Some(events) = bind_result(step, &body)
            .ok()
            .and_then(|bound| events(endpoint, &bound))
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
) -> std::io::Result<Response<Body>> {
    let (namespace, endpoint) = path
        .trim_start_matches('/')
        .split_once('/')
        .unwrap_or(("", ""));
    let endpoint = endpoint.strip_prefix("v1/").unwrap_or(endpoint);
    host.sequence += 1;
    let record = json!({"sequence":host.sequence,"case":namespace,"method":method,"path":endpoint,"body":body,"step":step,"status":reply.status().as_u16()});
    // One local-executor turn owns consume -> append -> flush -> event. A caller
    // observing the event can immediately read the complete JSONL record.
    writeln!(host.record, "{record}")?;
    host.record.flush()?;
    println!(
        "{}",
        json!({"event":"request","sequence":host.sequence,"case":namespace,"path":endpoint,"step":step})
    );
    std::io::stdout().flush()?;
    Ok(reply)
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
        record: File::create(record)?,
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
