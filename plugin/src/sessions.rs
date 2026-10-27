//! MCP coordination shim. Native CLIs own agent loops, tools and policy.
use super::*;
use acyclic_harness::local::host_owner::{Control, TurnClaim};
use acyclic_harness::{
    agent::AgentPath,
    local::{
        host_owner::SessionAction,
        host_process::{HostProcessLease, HostSessionProcess},
    },
};
use acyclic_native_runtime::{FileLock, FileLockMode};
use std::{collections::BTreeMap, io::Write as _, sync::mpsc, time::Duration};

struct Running {
    process: HostSessionProcess,
    claim: TurnClaim,
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
    parent: AgentPath,
    flags: Vec<String>,
    /// Native processes this shim runs, by parent-local reference.
    children: BTreeMap<String, Running>,
    /// The last prompt accepted per reference, as its committed `id` and
    /// `inbox`: what `wait` awaits through the shared reply.
    prompts: BTreeMap<String, Value>,
    _ownership: FileLock,
    _root: acyclic_fs::HeldRoot,
}

impl Sessions {
    /// Takes exclusive ownership of the parent's children. Every input reaches
    /// them through this owner, so it settles their orphaned turns once and
    /// runs their queued inputs; it never polls the harness afterwards.
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
            &PathBuf::from(".acyclic-sdk/host-processes").join(parent.id().as_str()),
        )?;
        let ownership = FileLock::try_acquire(
            root.open_file(Path::new("manager.lock"), true)?,
            FileLockMode::Exclusive,
        )?;
        let mut sessions = Self {
            host: host.into(),
            cwd,
            parent,
            flags,
            children: BTreeMap::new(),
            prompts: BTreeMap::new(),
            _ownership: ownership,
            _root: root,
        };
        let open = host_files::request(
            &sessions.cwd,
            HostAction::Sessions {
                parent: sessions.parent.clone(),
            },
        )?;
        for reference in serde_json::from_value::<Vec<String>>(
            open.get("references").cloned().unwrap_or_default(),
        )? {
            sessions.request(&reference, SessionAction::Recover)?;
            sessions.start(&reference)?;
        }
        Ok(sessions)
    }
    fn request(&self, reference: &str, action: SessionAction) -> Result<Value> {
        Ok(host_files::request(
            &self.cwd,
            HostAction::Session {
                parent: self.parent.clone(),
                reference: reference.into(),
                action,
            },
        )?)
    }
    /// Records the prompt a Spawn or Send accepted, then runs its input.
    fn accept(&mut self, reference: &str, action: SessionAction) -> Result<Value> {
        let accepted = self.request(reference, action)?;
        self.prompts.insert(
            reference.into(),
            json!({"id": accepted.get("id").ok_or("accepted prompt has no id")?,
                "inbox": accepted.get("inbox").ok_or("accepted prompt has no inbox")?}),
        );
        self.start(reference)?;
        Ok(accepted)
    }
    /// Claims and launches the child's next input, unless one already runs.
    fn start(&mut self, reference: &str) -> Result<()> {
        if self.children.contains_key(reference) {
            return Ok(());
        }
        let Some((lease, turn)) =
            HostProcessLease::claim_child(&self.cwd, &self.parent, reference)?
        else {
            return Ok(());
        };
        let task: AgentPath =
            serde_json::from_value(turn.get("task").cloned().ok_or("child task absent")?)?;
        let claim: TurnClaim =
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
    /// Advances every running process by one bounded pass. A settled turn
    /// starts the child's next input and answers the waits it resolves.
    fn pump(&mut self, waiting: &mut Vec<(Value, String)>) -> Result<bool> {
        let mut progressed = false;
        for reference in self.children.keys().cloned().collect::<Vec<_>>() {
            let Some(mut running) = self.children.remove(&reference) else {
                continue;
            };
            let refreshed = self.refresh_running(&reference, &mut running);
            progressed |= running.process.take_output_progress();
            match refreshed {
                Ok(true) => {
                    self.start(&reference)?;
                    self.resolve(&reference, waiting)?;
                }
                Ok(false) => {
                    self.children.insert(reference, running);
                }
                Err(error) => {
                    self.children.insert(reference, running);
                    return Err(error);
                }
            }
        }
        Ok(progressed)
    }
    /// The reply to `reference`'s last accepted prompt, once nothing running
    /// can still settle it: answered, dropped, or pending behind a stop.
    fn wait(&self, reference: &str) -> Result<Option<Value>> {
        let Some(prompt) = self.prompts.get(reference) else {
            // This owner accepted no prompt for it: nothing is awaited.
            return self.request(reference, SessionAction::Observe).map(Some);
        };
        let value = self.request(
            reference,
            SessionAction::Reply {
                id: serde_json::from_value(prompt["id"].clone())?,
                inbox: prompt["inbox"].as_u64().ok_or("prompt inbox absent")?,
            },
        )?;
        let pending = value.pointer("/reply/kind").and_then(Value::as_str) == Some("pending");
        Ok((!pending || !self.children.contains_key(reference)).then_some(value))
    }
    fn resolve(&self, reference: &str, waiting: &mut Vec<(Value, String)>) -> Result<()> {
        if !waiting.iter().any(|(_, pending)| pending == reference) {
            return Ok(());
        }
        if let Some(value) = self.wait(reference)? {
            for (id, _) in waiting.extract_if(.., |(_, pending)| pending == reference) {
                reply(&id, Ok(value.clone()))?;
            }
        }
        Ok(())
    }
    /// Whether the process exited and its turn settled.
    fn refresh_running(&self, reference: &str, running: &mut Running) -> Result<bool> {
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
                Ok(false) => return Ok(false),
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
            Ok(None) => return Ok(false),
        };
        self.complete(reference, running, outcome).map(|_| true)
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
    fn stop(
        &mut self,
        reference: &str,
        control: Control,
        waiting: &mut Vec<(Value, String)>,
    ) -> Result<Value> {
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
        }
        self.resolve(reference, waiting)?;
        if control == Control::Archive {
            self.prompts.remove(reference);
        }
        self.request(reference, SessionAction::Observe)
    }
    /// One coordination tool call; `None` parks a wait until its reply.
    fn call(
        &mut self,
        name: &str,
        args: &Value,
        waiting: &mut Vec<(Value, String)>,
    ) -> Result<Option<Value>> {
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
            "spawn" => self
                .accept(reference, SessionAction::Spawn { prompt: prompt()? })
                .map(Some),
            "send" => self
                .accept(reference, SessionAction::Send { prompt: prompt()? })
                .map(Some),
            "resume" => {
                if self.children.contains_key(reference) {
                    return Err("native session is still running".into());
                }
                self.request(
                    reference,
                    SessionAction::Control {
                        control: Control::Resume,
                    },
                )?;
                self.accept(reference, SessionAction::Send { prompt: prompt()? })
                    .map(Some)
            }
            "archive" => self.stop(reference, Control::Archive, waiting).map(Some),
            "cancel" => self.stop(reference, Control::Interrupt, waiting).map(Some),
            "wait" => self.wait(reference),
            _ => Err("unknown coordination tool".into()),
        }
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
    let serving = (|| -> Result<()> {
        loop {
            let (mut running, mut progressed) = (false, false);
            if let Some(state) = sessions.as_mut() {
                progressed = state.pump(&mut waiting)?;
                running = !state.children.is_empty();
            }
            // Only this owner's own processes can settle a wait, so with none
            // running it blocks on its host. Ready output schedules another
            // bounded pass; idle waiting cannot throttle a backpressured pipe.
            let message = if running {
                receive.recv_timeout(if progressed {
                    Duration::ZERO
                } else {
                    Duration::from_millis(20)
                })
            } else {
                receive
                    .recv()
                    .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
            };
            let message = match message {
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
                    let result = (|| -> Result<Option<Value>> {
                        if sessions.is_none() {
                            sessions = Some(Sessions::open(host)?);
                        }
                        sessions.as_mut().ok_or("session owner absent")?.call(
                            name,
                            args,
                            &mut waiting,
                        )
                    })();
                    if let Ok(None) = result {
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
                        reply(
                            id,
                            result.and_then(|value| value.ok_or("wait parked".into())),
                        )?;
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
            if let Err(error) = sessions.stop(&reference, Control::Interrupt, &mut waiting) {
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
#[path = "sessions/held_stderr_tests.rs"]
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
    fn native_answers_are_bounded_on_character_boundaries() {
        assert!(bounded_text(&"😀".repeat(4096)).len() <= 4096);
    }
}
