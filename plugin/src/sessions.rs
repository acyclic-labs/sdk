//! MCP coordination shim. Native CLIs own agent loops, tools and policy.
use super::*;
use acyclic_harness::{
    local::{
        host_owner::SessionAction,
        host_process::{HostProcessLease, HostSessionProcess},
    },
    task_volume::{ChildTurnClaim, Component, Control},
};
use acyclic_native_runtime::{FileLock, FileLockMode};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
    sync::mpsc,
    time::{Duration, Instant},
};

struct Running {
    process: HostSessionProcess,
    claim: ChildTurnClaim,
    failure: Option<String>,
    events: EventLines,
    answer: String,
    session: Option<String>,
}
impl Running {
    fn finish_input(&mut self) -> Result<()> {
        self.process.require_stdin_complete().map_err(Into::into)
    }
    fn check_input(&mut self) -> Result<()> {
        self.process.pump_stdin().map(|_| ()).map_err(Into::into)
    }
}
struct EventLines(acyclic_native_runtime::lines::LineBuffer);
impl Default for EventLines {
    fn default() -> Self {
        Self(acyclic_native_runtime::lines::LineBuffer::new(65_536))
    }
}
impl EventLines {
    fn push(&mut self, chunk: &[u8]) -> Vec<io::Result<Value>> {
        self.0
            .push(chunk)
            .into_iter()
            .filter_map(|line| match line {
                Ok(line) => serde_json::from_str(&line).ok().map(Ok),
                Err(error) => Some(Err(error)),
            })
            .collect()
    }
}
struct Sessions {
    host: String,
    cwd: PathBuf,
    parent: TaskId,
    flags: Vec<String>,
    children: BTreeMap<String, Running>,
    after: Option<Component>,
    _ownership: FileLock,
    _root: acyclic_fs::HeldRoot,
}

impl Sessions {
    fn open(host: &str) -> Result<Self> {
        let flags: Vec<String> = serde_json::from_str(&env::var("ACYCLIC_HOST_ARGS").map_err(
            |_| "launch the native parent with acyclic host HOST -- POLICY_ARGS before spawning",
        )?)?;
        codex::validate(host, &flags)?;
        let cwd = PathBuf::from(
            env::var_os("ACYCLIC_WORKSPACE").ok_or("native workspace context absent")?,
        )
        .canonicalize()?;
        let parent = parse_task(&env::var("ACYCLIC_TASK")?)?;
        let root = acyclic_fs::hold_root(&host_files::workspace_root(&cwd)?)?.create_directories(
            &PathBuf::from(".acyclic-sdk/host-processes").join(parent.to_string()),
        )?;
        let ownership = FileLock::try_acquire(
            root.open_file(Path::new("manager.lock"), true)?,
            FileLockMode::Exclusive,
        )?;
        Ok(Self {
            host: host.into(),
            cwd,
            parent,
            flags,
            children: BTreeMap::new(),
            after: None,
            _ownership: ownership,
            _root: root,
        })
    }
    fn request(&self, reference: &str, action: SessionAction) -> Result<Value> {
        Ok(host_files::request(
            &self.cwd,
            HostAction::Session {
                parent: self.parent,
                reference: reference.into(),
                action,
            },
        )?)
    }
    fn start(&mut self, reference: &str, observed: &Value) -> Result<()> {
        if self.children.contains_key(reference) {
            return Ok(());
        }
        // Retained references include completed sessions. Observing an empty,
        // settled mailbox must not publish recovery or execution commands.
        if settled(observed) {
            return Ok(());
        }
        if observed.pointer("/status/settled").and_then(Value::as_bool) == Some(false) {
            self.recover(reference)?;
        }
        let Some((lease, turn)) = HostProcessLease::claim_child(&self.cwd, self.parent, reference)?
        else {
            return Ok(());
        };
        let task: TaskId =
            serde_json::from_value(turn.get("task").cloned().ok_or("child task absent")?)?;
        let claim: ChildTurnClaim =
            serde_json::from_value(turn.get("claim").cloned().ok_or("child claim absent")?)?;
        let cwd = PathBuf::from(
            turn.get("cwd")
                .and_then(Value::as_str)
                .ok_or("child cwd absent")?,
        );
        let prompt = turn
            .get("prompt")
            .and_then(Value::as_str)
            .ok_or("native prompt absent")?
            .as_bytes()
            .to_vec();
        let resume = if self.host == "pi" {
            turn.pointer("/session/file")
                .or_else(|| turn.pointer("/session/id"))
        } else {
            turn.pointer("/session/id")
        };
        let mut command = codex::child(
            &self.host,
            &self.flags,
            &cwd,
            resume.and_then(Value::as_str),
        )?;
        command
            .env("ACYCLIC_WORKSPACE", &cwd)
            .env("ACYCLIC_TASK", task.to_string());
        let process = lease.funded_process()?;
        // Install the stable owner before spawn can acquire any physical work.
        // Even a partial spawn or stdin failure stays here until it is reaped.
        let running = self.children.entry(reference.into()).or_insert(Running {
            process,
            claim,
            failure: None,
            events: EventLines::default(),
            answer: String::new(),
            session: None,
        });
        if let Err(error) = running
            .process
            .spawn(&mut command)
            .and_then(|()| running.process.queue_stdin(prompt))
        {
            running.failure = Some(error.to_string());
        }
        Ok(())
    }
    fn recover(&mut self, reference: &str) -> Result<()> {
        if !self.children.contains_key(reference) {
            self.request(reference, SessionAction::Recover)?;
        }
        Ok(())
    }
    fn output(&self, reference: &str, running: &mut Running) -> Result<()> {
        running.process.drain_stderr()?;
        let mut chunk = [0u8; 8192];
        // Bound each pass so a busy child cannot starve cancellation or peers.
        for _ in 0..16 {
            let Some(count) = running.process.read_stdout(&mut chunk)? else {
                break;
            };
            if count == 0 {
                return Ok(());
            }
            for event in running
                .events
                .push(chunk.get(..count).ok_or("native output count invalid")?)
            {
                let event = match event {
                    Ok(event) => event,
                    Err(_) => {
                        // A dropped answer must replace an earlier preview.
                        // Keep draining through the same bounded framer and
                        // process owner; later complete answers remain usable.
                        running.answer = "[Native output omitted: an event exceeded the 65,536-byte limit or contained invalid UTF-8.]".into();
                        continue;
                    }
                };
                let id = event
                    .get("thread_id")
                    .or_else(|| event.get("session_id"))
                    .or_else(|| {
                        (event.get("type").and_then(Value::as_str) == Some("session"))
                            .then(|| event.get("id"))
                            .flatten()
                    });
                if let Some(id) = id.and_then(Value::as_str)
                    && running.session.as_deref() != Some(id)
                {
                    self.request(reference, SessionAction::SaveSession { id: id.into() })?;
                    running.session = Some(id.into());
                }
                let text = if event.pointer("/item/type").and_then(Value::as_str)
                    == Some("agent_message")
                {
                    event.pointer("/item/text")
                } else {
                    event.get("result")
                };
                if let Some(text) = text.and_then(Value::as_str) {
                    running.answer = bounded_text(text);
                }
                if event.pointer("/message/role").and_then(Value::as_str) == Some("assistant")
                    && event.get("type").and_then(Value::as_str) == Some("message_end")
                    && let Some(parts) = event.pointer("/message/content").and_then(Value::as_array)
                {
                    running.answer = bounded_text(
                        &parts
                            .iter()
                            .filter_map(|part| part.get("text").and_then(Value::as_str))
                            .collect::<String>(),
                    );
                }
            }
        }
        Ok(())
    }
    fn refresh(&mut self, waiting: &mut Vec<(Value, String)>) -> Result<bool> {
        let mut progressed = false;
        let page: Option<Vec<Component>> = serde_json::from_value(host_files::request(
            &self.cwd,
            HostAction::Sessions {
                parent: self.parent,
                after: self.after.clone(),
            },
        )?)?;
        let mut references: BTreeSet<String> = self.children.keys().cloned().collect();
        references.extend(waiting.iter().map(|(_, reference)| reference.clone()));
        if let Some(page) = page {
            self.after = page.last().cloned();
            references.extend(
                page.into_iter()
                    .map(|reference| reference.as_str().to_owned()),
            );
        }
        for reference in references {
            let observed = self.request(&reference, SessionAction::Observe)?;
            let observed = if let Some(mut running) = self.children.remove(&reference) {
                let refreshed = self.refresh_running(&reference, &observed, &mut running);
                progressed |= running.process.take_output_progress();
                match refreshed {
                    Ok(Some(observed)) => {
                        self.start(&reference, &observed)?;
                        observed
                    }
                    Ok(None) => {
                        self.children.insert(reference.clone(), running);
                        observed
                    }
                    Err(error) => {
                        self.children.insert(reference, running);
                        return Err(error);
                    }
                }
            } else {
                self.start(&reference, &observed)?;
                observed
            };
            if settled(&observed) {
                for (id, _) in waiting.extract_if(.., |(_, pending)| pending == &reference) {
                    reply(&id, Ok(observed.clone()))?;
                }
            }
        }
        Ok(progressed)
    }
    fn refresh_running(
        &self,
        reference: &str,
        observed: &Value,
        running: &mut Running,
    ) -> Result<Option<Value>> {
        if observed
            .get("control")
            .and_then(Value::as_str)
            .is_some_and(|state| state != "resume")
        {
            running.process.terminate_descendants()?;
            let _ = self.output(reference, running);
            return self
                .complete(reference, running, acyclic_harness::Outcome::Cancelled)
                .map(Some);
        }
        let mut exited = if let Some(error) = &running.failure {
            Err(error.clone().into())
        } else {
            running
                .check_input()
                .and_then(|()| self.output(reference, running))
                .and_then(|_| running.process.try_wait().map_err(Into::into))
        };
        if matches!(&exited, Ok(Some(_))) {
            running.process.terminate_descendants()?;
            match running.process.pump_stdin() {
                Ok(false) => return Ok(None),
                Ok(true) => {}
                Err(error) => exited = Err(error.into()),
            }
            // Exit is the capture boundary. A detached process can retain a
            // pipe writer after containment stops; EOF cannot own settlement.
            if let Err(error) = self.output(reference, running) {
                exited = Err(error);
            }
        }
        let outcome = match exited {
            Err(error) => acyclic_harness::Outcome::Failed {
                message: error.to_string(),
            },
            Ok(Some(status)) if status.success() => {
                acyclic_harness::Outcome::Succeeded(acyclic_harness::executor::TurnOutput {
                    text: running.answer.clone(),
                    metadata: Value::Null,
                    steps: 1,
                    attachments: Vec::new(),
                })
            }
            Ok(Some(status)) => acyclic_harness::Outcome::Failed {
                message: format!("native host exited {status}"),
            },
            Ok(None) => return Ok(None),
        };
        self.complete(reference, running, outcome).map(Some)
    }
    fn complete(
        &self,
        reference: &str,
        running: &mut Running,
        outcome: acyclic_harness::Outcome<acyclic_harness::executor::TurnOutput>,
    ) -> Result<Value> {
        running.process.release()?;
        let outcome = match (running.finish_input(), outcome) {
            (_, acyclic_harness::Outcome::Cancelled) => acyclic_harness::Outcome::Cancelled,
            (_, acyclic_harness::Outcome::Failed { message }) => acyclic_harness::Outcome::Failed {
                message: running.process.failure_message(&message),
            },
            (Ok(()), outcome) => outcome,
            (Err(error), _) => acyclic_harness::Outcome::Failed {
                message: running.process.failure_message(&error.to_string()),
            },
        };
        self.request(
            reference,
            SessionAction::Complete {
                claim: running.claim.clone(),
                outcome,
            },
        )
    }
    fn stop(&mut self, reference: &str, control: Control) -> Result<Value> {
        self.request(reference, SessionAction::Control { control })?;
        if let Some(mut running) = self.children.remove(reference) {
            let result = (|| -> Result<()> {
                running.process.terminate_descendants()?;
                let _ = self.output(reference, &mut running);
                self.complete(reference, &mut running, acyclic_harness::Outcome::Cancelled)
                    .map(|_| ())
            })();
            if let Err(error) = result {
                self.children.insert(reference.into(), running);
                return Err(error);
            }
        } else {
            self.recover(reference)?;
        }
        self.request(reference, SessionAction::Observe)
    }
    fn call(&mut self, name: &str, args: &Value) -> Result<Value> {
        let object = args.as_object().ok_or("tool arguments must be an object")?;
        if object
            .keys()
            .any(|key| !matches!(key.as_str(), "reference" | "prompt"))
        {
            return Err("unsupported coordination argument".into());
        }
        let reference = object
            .get("reference")
            .and_then(Value::as_str)
            .ok_or("reference is required")?;
        let prompt = || {
            object
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or("prompt is required")
        };
        match name {
            "spawn" => {
                let observed =
                    self.request(reference, SessionAction::Spawn { prompt: prompt()? })?;
                self.start(reference, &observed)?;
            }
            "send" => {
                let observed =
                    self.request(reference, SessionAction::Send { prompt: prompt()? })?;
                self.start(reference, &observed)?;
            }
            "resume" => {
                if self.children.contains_key(reference) {
                    return Err("native session is still running".into());
                }
                self.recover(reference)?;
                self.request(
                    reference,
                    SessionAction::Control {
                        control: Control::Resume,
                    },
                )?;
                let observed =
                    self.request(reference, SessionAction::Send { prompt: prompt()? })?;
                self.start(reference, &observed)?;
            }
            "archive" => return self.stop(reference, Control::Archive),
            "cancel" => return self.stop(reference, Control::Interrupt),
            "wait" => {}
            _ => return Err("unknown coordination tool".into()),
        }
        self.request(reference, SessionAction::Observe)
    }
}

fn bounded_text(text: &str) -> String {
    text.chars()
        .scan(0, |bytes, ch| {
            *bytes += ch.len_utf8();
            (*bytes <= 4096).then_some(ch)
        })
        .collect()
}
fn settled(value: &Value) -> bool {
    let stopped = value
        .get("control")
        .and_then(Value::as_str)
        .is_some_and(|state| state != "resume");
    value
        .pointer("/status/settled")
        .and_then(Value::as_bool)
        .unwrap_or(stopped)
        && (stopped || value.get("pending").and_then(Value::as_u64) == Some(0))
}
fn reply(id: &Value, result: Result<Value>) -> Result<()> {
    let result = match result {
        Ok(value) => json!({"content":[{"type":"text","text":value.to_string()}]}),
        Err(error) => json!({"isError":true,"content":[{"type":"text","text":error.to_string()}]}),
    };
    emit(&json!({"jsonrpc":"2.0","id":id,"result":result}))
}
fn emit(value: &Value) -> Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, value)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn input_messages() -> mpsc::Receiver<io::Result<Value>> {
    let (send, receive) = mpsc::sync_channel(16);
    std::thread::spawn(move || {
        let input = io::stdin();
        for message in acyclic_harness::mcp::stdio::JsonLineReader::new(input.lock(), 65_536) {
            if send.send(message).is_err() {
                break;
            }
        }
    });
    receive
}

pub(super) fn serve(host: &str) -> Result<()> {
    let receive = input_messages();
    let mut sessions: Option<Sessions> = None;
    let mut waiting: Vec<(Value, String)> = Vec::new();
    let mut next_refresh = Instant::now();
    let serving = (|| -> Result<()> {
        loop {
            let mut progressed = false;
            if Instant::now() >= next_refresh {
                if let Some(state) = sessions.as_mut() {
                    progressed = state.refresh(&mut waiting)?;
                }
                next_refresh = Instant::now()
                    + if progressed {
                        Duration::ZERO
                    } else {
                        Duration::from_millis(100)
                    };
            }
            // Ready output schedules another bounded turn. Only a quiescent
            // pass may park; idle waiting cannot throttle a backpressured pipe.
            let message = match receive.recv_timeout(if progressed {
                Duration::ZERO
            } else {
                Duration::from_millis(20)
            }) {
                Ok(Ok(value)) => value,
                Ok(Err(_)) => {
                    emit(
                        &json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"invalid JSON"}}),
                    )?;
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(_) => break,
            };
            let params = message.get("params").unwrap_or(&Value::Null);
            if message.get("method").and_then(Value::as_str) == Some("notifications/cancelled") {
                waiting.retain(|(id, _)| Some(id) != params.get("requestId"));
                continue;
            }
            let Some(id) = message.get("id") else {
                continue;
            };
            match message.get("method").and_then(Value::as_str) {
                Some("initialize") => emit(
                    &json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":params.get("protocolVersion").and_then(Value::as_str).unwrap_or("2025-06-18"),"capabilities":{"tools":{}},"serverInfo":{"name":"acyclic-sessions","version":env!("CARGO_PKG_VERSION")}}}),
                )?,
                Some("ping") => emit(&json!({"jsonrpc":"2.0","id":id,"result":{}}))?,
                Some("tools/list") => emit(&json!({"jsonrpc":"2.0","id":id,"result":catalog()}))?,
                Some("tools/call") => {
                    let name = params
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let args = params.get("arguments").unwrap_or(&Value::Null);
                    let result = (|| -> Result<Value> {
                        if sessions.is_none() {
                            sessions = Some(Sessions::open(host)?);
                        }
                        sessions
                            .as_mut()
                            .ok_or("session owner absent")?
                            .call(name, args)
                    })();
                    if name == "wait" && result.as_ref().is_ok_and(|value| !settled(value)) {
                        if waiting.len() >= 16 {
                            reply(id, Err("native wait concurrency bound reached".into()))?;
                            continue;
                        }
                        waiting.push((
                            id.clone(),
                            args.get("reference")
                                .and_then(Value::as_str)
                                .ok_or("reference absent")?
                                .into(),
                        ));
                    } else {
                        reply(id, result)?;
                    }
                }
                _ => emit(
                    &json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}}),
                )?,
            }
        }
        Ok(())
    })();
    if let Some(mut sessions) = sessions {
        let mut failure = None;
        for reference in sessions.children.keys().cloned().collect::<Vec<_>>() {
            if let Err(error) = sessions.stop(&reference, Control::Interrupt) {
                failure = Some(error);
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
    }
    serving
}

fn catalog() -> Value {
    json!({"tools":(["spawn","send","wait","archive","cancel","resume"].map(|name| {
        let prompt=matches!(name,"spawn"|"send"|"resume");
        json!({"name":name,"description":format!("{name} a separate native session in an admitted harness view. Native tools and policy are preserved. Native subagents are unmanaged."),"inputSchema":{"type":"object","properties":{"reference":{"type":"string"},"prompt":{"type":"string"}},"required":if prompt {vec!["reference","prompt"]} else {vec!["reference"]},"additionalProperties":false}})
    }))})
}

#[cfg(all(test, target_os = "linux"))]
#[path = "../tests/held-stderr.rs"]
mod exit_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oversized_native_tool_output_does_not_kill_or_wedge_the_host() {
        let mut events = EventLines::default();
        let mut omitted = 0;
        for _ in 0..16 {
            for event in events.push(&[b'x'; 8192]) {
                assert!(event.is_err());
                omitted += 1;
            }
            assert!(events.0.buffered_bytes() <= 65_536);
        }
        assert_eq!(omitted, 1, "oversized output must be reported once");
        assert_eq!(
            events
                .push(b"\n{\"probe\":true}\n")
                .into_iter()
                .collect::<io::Result<Vec<_>>>()
                .expect("valid frame after omitted event"),
            vec![json!({"probe":true})]
        );
    }
    #[test]
    fn waits_for_quiescence_and_all_accepted_inputs_without_a_tool_ledger() {
        assert!(!settled(&json!({"status":{"settled":false},"pending":0})));
        assert!(!settled(&json!({"status":{"settled":true},"pending":1})));
        assert!(settled(&json!({"status":{"settled":true},"pending":0})));
        assert!(settled(
            &json!({"status":{"settled":true},"pending":1,"control":"interrupt"})
        ));
        assert!(bounded_text(&"😀".repeat(4096)).len() <= 4096);
    }
}
