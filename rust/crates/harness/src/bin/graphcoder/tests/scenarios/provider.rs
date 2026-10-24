use super::{TestResult, faults::Fault, wire};
use axum::{Json, Router, body::Body, extract::OriginalUri, http::Response, routing::post};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

struct Request {
    body: Value,
    endpoint: String,
    reply: oneshot::Sender<TestResult<Reply>>,
}

enum Reply {
    Model(Vec<String>, bool),
    Fault(Fault),
}

pub struct Recording {
    pub requests: BTreeMap<String, Vec<Value>>,
    pub endpoints: BTreeMap<String, Vec<String>>,
    pub remaining: BTreeMap<String, usize>,
    pub audits: BTreeMap<String, Vec<Value>>,
    pub attempts: BTreeMap<String, usize>,
    pub actions_applied: usize,
    pub actions_remaining: Vec<(String, usize)>,
    pub injected: Vec<Fault>,
    pub remaining_faults: usize,
}

pub struct Provider {
    pub url: String,
    server: JoinHandle<()>,
    actor: JoinHandle<TestResult<Recording>>,
    stop: Option<oneshot::Sender<()>>,
}

struct Script {
    turns: VecDeque<Value>,
    requests: Vec<Value>,
    endpoints: Vec<String>,
    pins: Vec<String>,
    prepared: bool,
    audit: Value,
    audit_sent: bool,
    list_active: bool,
    audit_requests: Vec<Value>,
    attempts: usize,
}

impl Script {
    fn new(turns: Vec<Value>) -> Self {
        let pins: BTreeSet<_> = turns
            .iter()
            .filter_map(Value::as_array)
            .flatten()
            .filter(|event| {
                matches!(
                    event["name"]
                        .as_str()
                        .map(|name| name.split('@').next().unwrap_or(name)),
                    Some("read" | "edit")
                )
            })
            .filter_map(|event| event["arguments"]["file"].as_str().map(str::to_owned))
            .collect();
        Self {
            turns: turns.into(),
            requests: Vec::new(),
            endpoints: Vec::new(),
            pins: pins.into_iter().collect(),
            prepared: false,
            audit: Value::Null,
            audit_sent: false,
            list_active: false,
            audit_requests: Vec::new(),
            attempts: 0,
        }
    }

    fn reply(
        &mut self,
        body: Value,
        seed: &Value,
        protocol: &str,
        raw: bool,
        bindings: &Value,
    ) -> TestResult<(Vec<String>, bool)> {
        self.attempts += 1;
        if !self.pins.is_empty() && !self.prepared {
            self.prepared = true;
            let mut events = Vec::new();
            for (index, path) in self.pins.iter().enumerate() {
                events.push(json!({"type":"event","kind":"tool_call","call_id":format!("binary_seed_{index}"),
                    "name":"write","arguments":{"path":path,"text":seed[path].as_str().ok_or("seed file missing")?,
                    "media_type":"text/plain","display_name":path.rsplit('/').next().ok_or("seed name missing")?}}));
            }
            events.push(json!({"type":"event","kind":"completed","metadata":null}));
            return wire::encode(&json!(events), protocol);
        }
        if self.turns.len() == 1
            && self.turns[0].is_string()
            && (self.list_active
                || self
                    .audit
                    .as_object()
                    .is_some_and(|files| !files.is_empty()))
        {
            if !self.audit_sent {
                self.audit_sent = true;
                self.requests.push(body);
                let mut events: Vec<_> = self.audit.as_object().ok_or("audit files missing")?.keys().enumerate()
                    .map(|(index, path)| json!({"type":"event","kind":"tool_call","call_id":format!("binary_audit_{index}"),
                        "name":"fs","arguments":{"command":format!("cat .volumes/{path}")}})).collect();
                if self.list_active {
                    events.push(
                        json!({"type":"event","kind":"tool_call","call_id":"binary_active_list",
                        "name":"fs","arguments":{"command":"ls .volumes/agents"}}),
                    );
                }
                events.push(json!({"type":"event","kind":"completed","metadata":null}));
                return wire::encode(&json!(events), protocol);
            }
            self.audit_requests.push(body.clone());
        } else {
            self.requests.push(body.clone());
        }
        let mut turn = self
            .turns
            .pop_front()
            .ok_or("HTTP provider script exhausted")?;
        if !raw && let Some(events) = turn.as_array_mut() {
            let outputs = wire::outputs(&body, protocol)?;
            for event in events {
                if matches!(
                    event["name"]
                        .as_str()
                        .map(|name| name.split('@').next().unwrap_or(name)),
                    Some("read" | "edit")
                ) && let Some(path) = event["arguments"]["file"].as_str()
                {
                    let index = self
                        .pins
                        .iter()
                        .position(|pin| pin == path)
                        .ok_or("unbound seed file")?;
                    let id = format!("binary_seed_{index}");
                    let (_, result) = outputs
                        .iter()
                        .find(|(call, _)| call == &id)
                        .ok_or("seed publication result missing")?;
                    if result["file"].is_null() {
                        return Err(format!("seed write returned no FileRef: {result}").into());
                    }
                    event["arguments"]["file"] = result["file"].clone();
                }
                if let Some(name) = event["name"].as_str() {
                    let alias = if let Some(description) = bindings.get(name) {
                        Some(super::bindings::tool(
                            &body,
                            description
                                .as_str()
                                .ok_or("wire tool description must be text")?,
                        )?)
                    } else if name.contains('@') {
                        Some(super::bindings::pinned(&body, name)?)
                    } else {
                        None
                    };
                    if let Some(alias) = alias {
                        event["name"] = alias.into();
                    }
                }
            }
        }
        if raw {
            turn.as_array()
                .ok_or("raw response needs fragments")?
                .iter()
                .map(|part| {
                    part.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| "raw fragment must be text".into())
                })
                .collect::<TestResult<Vec<_>>>()
                .map(|parts| (parts, false))
        } else {
            wire::encode(&turn, protocol)
        }
    }
}

pub async fn start(case: &Value, protocol: &str, workspace: &Path) -> TestResult<Provider> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let (send, mut receive) = mpsc::unbounded_channel::<Request>();
    let (stop, mut stopped) = oneshot::channel();
    let seed = case["seed"].clone();
    let bindings = case["wire_tools"].clone();
    let routes = case["routes"].as_object().cloned().unwrap_or_default();
    let protocol = protocol.to_owned();
    let raw = case.get("responses").is_some();
    let mut faults: VecDeque<Fault> =
        serde_json::from_value(case.get("faults").cloned().unwrap_or_else(|| json!([])))?;
    for fault in &faults {
        fault.validate()?;
    }
    let mut scripts = BTreeMap::new();
    if let Some(models) = case["models"].as_object() {
        for (name, turns) in models {
            scripts.insert(
                name.clone(),
                Script::new(turns.as_array().ok_or("model turns missing")?.clone()),
            );
        }
    } else {
        let turns = case
            .get("responses")
            .or_else(|| case.get("turns"))
            .and_then(Value::as_array)
            .ok_or("binary scenario needs turns, models or provider responses")?;
        scripts.insert("parent".into(), Script::new(turns.clone()));
    }
    if let Some(script) = scripts.get_mut("parent") {
        script.audit = case["expect"]
            .get("task_files")
            .cloned()
            .unwrap_or_else(|| json!({}));
        script.list_active = case["expect"].get("active").is_some();
    }
    let mut actions = BTreeMap::new();
    for action in serde_json::from_value::<Vec<super::scenario_actions::Action>>(
        case.get("actions").cloned().unwrap_or_else(|| json!([])),
    )? {
        super::scenario_actions::file(&action)?;
        let script = scripts
            .get(&action.model)
            .ok_or("action selected an unknown model")?;
        if action.request > script.turns.len() {
            return Err("action selected an unscripted model request".into());
        }
        let key = (action.model.clone(), action.request);
        if actions.insert(key, action).is_some() {
            return Err("scenario actions repeat a model request event".into());
        }
    }
    let workspace = workspace.to_owned();
    let actor = tokio::spawn(async move {
        let mut errors = Vec::new();
        let mut actions_applied = 0;
        let mut injected = Vec::new();
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                request = receive.recv() => {
                    let Some(request) = request else { break };
                    let route = route(&request.body, &protocol, &routes);
                    let reply = (|| {
                        let script = scripts.get_mut(route).ok_or_else(|| format!("unknown scripted route: {route}"))?;
                        script.endpoints.push(request.endpoint);
                        if let Some(fault) = faults.pop_front() {
                            script.attempts += 1;
                            script.requests.push(request.body);
                            injected.push(fault.clone());
                            return Ok(Reply::Fault(fault));
                        }
                        if (script.pins.is_empty() || script.prepared)
                            && let Some(action) = actions.remove(&(route.to_owned(), script.requests.len() + 1))
                        {
                            publish_action(&workspace, &action)?;
                            actions_applied += 1;
                        }
                        script.reply(request.body, &seed, &protocol, raw, &bindings)
                            .map(|(parts, pending)| Reply::Model(parts, pending))
                    })();
                    if let Err(error) = &reply {
                        errors.push(format!("{route}: {error}"));
                    }
                    let _ = request.reply.send(reply);
                }
            }
        }
        if !errors.is_empty() {
            return Err(errors.join("\n").into());
        }
        let remaining = scripts
            .iter()
            .map(|(name, script)| (name.clone(), script.turns.len()))
            .collect();
        let attempts = scripts
            .iter()
            .map(|(name, script)| (name.clone(), script.attempts))
            .collect();
        let endpoints = scripts
            .iter_mut()
            .map(|(name, script)| (name.clone(), std::mem::take(&mut script.endpoints)))
            .collect();
        let audits = scripts
            .iter_mut()
            .map(|(name, script)| (name.clone(), std::mem::take(&mut script.audit_requests)))
            .collect();
        let requests = scripts
            .into_iter()
            .map(|(name, script)| (name, script.requests))
            .collect();
        Ok(Recording {
            requests,
            endpoints,
            remaining,
            attempts,
            audits,
            actions_applied,
            actions_remaining: actions.into_keys().collect(),
            injected,
            remaining_faults: faults.len(),
        })
    });
    let width = case["chunk_bytes"].as_u64().unwrap_or(13) as usize;
    if width == 0 {
        return Err("chunk_bytes must be positive".into());
    }
    let generate = move |OriginalUri(uri): OriginalUri, Json(body): Json<Value>| {
        serve(body, uri.to_string(), send.clone(), width)
    };
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":32})) }),
        )
        .route("/responses", post(generate.clone()))
        .route("/messages", post(generate.clone()))
        .route("/chat/completions", post(generate.clone()))
        .route("/models/{model}", post(generate));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Ok(Provider {
        url,
        server,
        actor,
        stop: Some(stop),
    })
}

fn publish_action(workspace: &Path, action: &super::scenario_actions::Action) -> TestResult {
    use acyclic_harness::task_volume::{ChildFile, FileKey};
    use std::io::Write as _;
    let value = super::scenario_actions::file(action)?;
    let FileKey::Child { reference, file } = &value.key else {
        return Err("action escaped the child namespace".into());
    };
    let child = workspace.join(".volumes/agents").join(reference.as_str());
    if !child.is_dir() {
        return Err(format!(
            "shipped child filesystem view is absent: {}",
            child.display()
        )
        .into());
    }
    let path = workspace.join(".volumes").join(value.key.path());
    let parent = path.parent().ok_or("action has no child directory")?;
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(&value.bytes)?;
    temporary.as_file().sync_all()?;
    match file {
        ChildFile::Control => {
            temporary.persist(path).map_err(|error| error.error)?;
        }
        ChildFile::Inbox(_) => {
            temporary
                .persist_noclobber(path)
                .map_err(|error| error.error)?;
        }
        _ => return Err("action selected a harness-owned child file".into()),
    }
    Ok(())
}

fn route<'a>(body: &Value, protocol: &str, routes: &'a serde_json::Map<String, Value>) -> &'a str {
    let field = if protocol == "responses" {
        "input"
    } else {
        "messages"
    };
    let mut selected = "parent";
    for message in body[field]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|message| message["role"] == "user")
    {
        if let Some(text) = message["content"].as_str()
            && let Some(model) = routes.get(text).and_then(Value::as_str)
        {
            selected = model;
        }
        for part in message["content"].as_array().into_iter().flatten() {
            if matches!(part["type"].as_str(), Some("input_text" | "text"))
                && let Some(model) = part["text"]
                    .as_str()
                    .and_then(|text| routes.get(text))
                    .and_then(Value::as_str)
            {
                selected = model;
            }
        }
    }
    selected
}

async fn serve(
    body: Value,
    endpoint: String,
    send: mpsc::UnboundedSender<Request>,
    width: usize,
) -> Response<Body> {
    let (reply, result) = oneshot::channel();
    send.send(Request {
        body,
        endpoint,
        reply,
    })
    .unwrap();
    match result.await.unwrap() {
        Ok(Reply::Fault(fault)) => fault_response(fault, width),
        Ok(Reply::Model(parts, pending)) => {
            let chunks: Vec<_> = parts
                .iter()
                .flat_map(|part| part.as_bytes().chunks(width))
                .map(|chunk| Ok::<_, std::io::Error>(chunk.to_vec()))
                .collect();
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(futures::StreamExt::chain(
                    futures::stream::iter(chunks),
                    futures::stream::poll_fn(move |_| {
                        if pending {
                            std::task::Poll::Pending
                        } else {
                            std::task::Poll::Ready(None)
                        }
                    }),
                )))
                .unwrap()
        }
        Err(error) => Response::builder()
            .status(500)
            .body(Body::from(error.to_string()))
            .unwrap(),
    }
}

fn fault_response(fault: Fault, width: usize) -> Response<Body> {
    match fault {
        Fault::Status { code, retry_after } => {
            let mut response = Response::builder().status(code);
            if let Some(seconds) = retry_after {
                response = response.header("retry-after", seconds);
            }
            response
                .body(Body::from("SCRIPTED_HTTP_ERROR_BODY"))
                .unwrap()
        }
        Fault::Malformed { body } => Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from(body))
            .unwrap(),
        Fault::Reset { prefix } => {
            // A body error terminates the actual HTTP response stream. It is
            // deliberately distinct from a complete response with invalid SSE.
            let chunks: Vec<_> = prefix
                .iter()
                .flat_map(|part| part.as_bytes().chunks(width))
                .map(|chunk| Ok::<_, std::io::Error>(chunk.to_vec()))
                .chain(std::iter::once(Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "scripted HTTP body reset",
                ))))
                .collect();
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(futures::stream::iter(chunks)))
                .unwrap()
        }
    }
}

impl Provider {
    pub async fn finish(mut self) -> TestResult<Recording> {
        self.server.abort();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        (&mut self.actor).await?
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.server.abort();
        self.actor.abort();
    }
}

#[cfg(test)]
#[path = "provider_fault_tests.rs"]
mod fault_tests;

#[cfg(test)]
#[path = "inventory.rs"]
mod inventory;

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn enabled_binary_cases_cover_the_current_tool_catalog() -> TestResult {
        super::inventory::check_catalog()
    }

    proptest! {
        #[test]
        fn binary_inventory_requires_a_real_scripted_call(name in "[a-z]{1,16}", text in ".{0,64}") {
            super::inventory::check_generated_call(&name, &text);
        }

        // Routing uses the exact user bytes. A user-role tool result, JSON
        // escaping or an incidental substring cannot redirect the parent.
        #[test]
        fn routing_preserves_unicode_and_excludes_tool_results(text in ".{1,64}") {
            let routes = serde_json::Map::from_iter([(text.clone(), json!("worker"))]);
            for (protocol, field, kind) in [("responses", "input", "input_text"), ("anthropic", "messages", "text")] {
                let request = json!({(field):[{"role":"user","content":[{"type":kind,"text":text}]}]});
                let bytes = serde_json::to_vec(&request)?;
                let decoded: Value = serde_json::from_slice(&bytes)?;
                prop_assert_eq!(route(&decoded, protocol, &routes), "worker");
                let tool = json!({(field):[{"role":"user","content":[{"type":"tool_result","content":text,"text":text}]}]});
                prop_assert_eq!(route(&tool, protocol, &routes), "parent");
                let substring = json!({(field):[{"role":"user","content":[{"type":kind,"text":format!("prefix{text}suffix")}]}]});
                prop_assert_eq!(route(&substring, protocol, &routes), "parent");
            }
        }

        #[test]
        fn both_wires_preserve_pinned_tool_names_and_argument_bytes(
            base in "[a-z]{1,16}", revision in "[a-z0-9-]{1,12}", text in ".{0,64}",
        ) {
            let name = format!("{base}@{revision}");
            let arguments = json!({"text":text});
            let turn = json!([
                {"type":"event","kind":"tool_call","call_id":"call","name":name,"arguments":arguments},
                {"type":"event","kind":"completed","metadata":null}
            ]);
            for protocol in ["responses", "anthropic"] {
                let (parts, pending) = wire::encode(&turn, protocol).map_err(|error| TestCaseError::fail(error.to_string()))?;
                prop_assert!(!pending);
                let frames: Vec<Value> = parts.iter().map(|part| serde_json::from_str(part.strip_prefix("data: ").unwrap().trim())).collect::<Result<_, _>>()?;
                let (actual_name, actual_arguments) = if protocol == "responses" {
                    let item = frames.iter().find(|frame| frame["type"] == "response.output_item.added").unwrap();
                    let args: String = frames.iter().filter(|frame| frame["type"] == "response.function_call_arguments.delta")
                        .filter_map(|frame| frame["delta"].as_str()).collect();
                    (item["item"]["name"].clone(), args)
                } else {
                    let block = frames.iter().find(|frame| frame["type"] == "content_block_start").unwrap();
                    let args: String = frames.iter().filter(|frame| frame["type"] == "content_block_delta")
                        .filter_map(|frame| frame["delta"]["partial_json"].as_str()).collect();
                    (block["content_block"]["name"].clone(), args)
                };
                prop_assert_eq!(&actual_name, &json!(name));
                prop_assert_eq!(serde_json::from_str::<Value>(&actual_arguments)?, arguments.clone());
            }
        }
    }
}
