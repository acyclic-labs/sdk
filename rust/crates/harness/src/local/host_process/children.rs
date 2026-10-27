//! The one owner of a native parent's managed children.
//!
//! A native host (Claude Code, Codex, Pi) runs its own loop, tools and
//! sandbox; its plugin only forwards coordination calls here. This owner
//! claims each child's next input under a physical [`HostProcessLease`], runs
//! the native child under the parent's inherited policy, records the native
//! resume reference, answer and whole event log from its event stream, and
//! settles the claim.
//! Waits resolve through the harness's shared committed reply.
use super::*;
use crate::Outcome;
use crate::agent::IntentId;
use crate::executor::TurnOutput;
use crate::local::host_files;
use crate::local::host_owner::{Control, HostAction, SessionAction, TurnClaim};
use serde_json::Value;
use std::collections::BTreeMap;

mod tools;
pub use tools::serve;

/// Byte bound of one native event line and of a retained answer's source.
const EVENT_LIMIT: usize = 65_536;
/// Byte bound of a retained answer.
const ANSWER_LIMIT: usize = 4096;

struct Running {
    process: HostSessionProcess,
    claim: TurnClaim,
    /// A spawn or stdin failure, settled as the turn's failure.
    failure: Option<String>,
    events: acyclic_native_runtime::lines::LineBuffer,
    answer: String,
    session: Option<String>,
}

/// Exclusive owner of one native parent's children, by parent-local name.
pub struct NativeChildren {
    host: String,
    cwd: PathBuf,
    parent: AgentPath,
    flags: Vec<String>,
    children: BTreeMap<String, Running>,
    /// The newest prompt accepted per reference, as its committed `id` and
    /// `inbox`: what an idle child's wait answers with.
    prompts: BTreeMap<String, (IntentId, u64)>,
    _ownership: FileLock,
    _root: acyclic_fs::HeldRoot,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

impl NativeChildren {
    /// Takes exclusive ownership of the children of the native parent this
    /// process runs under (`ACYCLIC_HOST_ARGS`, `ACYCLIC_WORKSPACE`,
    /// `ACYCLIC_TASK`, set by [`launch::launch`]). Every input reaches them
    /// through this owner, so it settles their orphaned turns once and runs
    /// their queued inputs; it never polls the harness afterwards.
    pub fn open(host: &str) -> Result<Self> {
        let flags: Vec<String> =
            serde_json::from_str(&std::env::var("ACYCLIC_HOST_ARGS").map_err(|_| {
                invalid(
                    "launch the native parent with acyclic host HOST -- POLICY_ARGS before spawning",
                )
            })?)
            .map_err(storage)?;
        let cwd = PathBuf::from(
            std::env::var_os("ACYCLIC_WORKSPACE")
                .ok_or_else(|| invalid("native workspace context absent"))?,
        );
        let parent = AgentPath::try_from(
            std::env::var("ACYCLIC_TASK").map_err(|_| invalid("native task context absent"))?,
        )
        .map_err(storage)?;
        Self::own(host, &cwd, parent, flags)
    }

    fn own(host: &str, cwd: &Path, parent: AgentPath, flags: Vec<String>) -> Result<Self> {
        launch::validate(host, &flags)?;
        let cwd = cwd.canonicalize().map_err(storage)?;
        let root = acyclic_fs::hold_root(&host_files::workspace_root(&cwd)?)
            .map_err(storage)?
            .create_directories(
                &PathBuf::from(".acyclic-sdk/host-processes").join(parent.id().as_str()),
            )
            .map_err(storage)?;
        let ownership = FileLock::try_acquire(
            root.open_file(Path::new("manager.lock"), true)
                .map_err(storage)?,
            FileLockMode::Exclusive,
        )
        .map_err(storage)?;
        let mut children = Self {
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
            &children.cwd,
            HostAction::Sessions {
                parent: children.parent.clone(),
            },
        )?;
        let references: Vec<String> =
            serde_json::from_value(open.get("references").cloned().unwrap_or_default())
                .map_err(storage)?;
        for reference in references {
            children.request(&reference, SessionAction::Recover)?;
            children.start(&reference)?;
        }
        Ok(children)
    }

    fn request(&self, reference: &str, action: SessionAction) -> Result<Value> {
        host_files::request(
            &self.cwd,
            HostAction::Session {
                parent: self.parent.clone(),
                reference: reference.into(),
                action,
            },
        )
    }

    /// Whether any child process is running.
    #[must_use]
    pub fn running(&self) -> bool {
        !self.children.is_empty()
    }

    /// One wait per running child that fires when its pipes have data or
    /// room, or it exits, all served by the shared process-event hub.
    pub fn readiness(&mut self) -> Result<Vec<acyclic_native_runtime::ProcessReadiness>> {
        self.children
            .values_mut()
            .map(|running| running.process.readiness())
            .collect()
    }

    /// Spawns `reference` with its first prompt, or names the existing child.
    pub fn spawn(&mut self, reference: &str, prompt: String) -> Result<Value> {
        self.accept(reference, SessionAction::Spawn { prompt })
    }

    /// Queues `prompt` for `reference`.
    pub fn send(&mut self, reference: &str, prompt: String) -> Result<Value> {
        self.accept(reference, SessionAction::Send { prompt })
    }

    /// Resumes a stopped `reference` with `prompt`.
    pub fn resume(&mut self, reference: &str, prompt: String) -> Result<Value> {
        if self.children.contains_key(reference) {
            return Err(Error::Conflict("native session is still running".into()));
        }
        self.request(
            reference,
            SessionAction::Control {
                control: Control::Resume,
            },
        )?;
        self.send(reference, prompt)
    }

    /// Records the prompt a Spawn or Send accepted, then runs its input. A
    /// replay names its original, earlier prompt and never displaces a newer
    /// one.
    fn accept(&mut self, reference: &str, action: SessionAction) -> Result<Value> {
        let accepted = self.request(reference, action)?;
        let inbox = accepted
            .get("inbox")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("accepted prompt has no inbox"))?;
        if self
            .prompts
            .get(reference)
            .is_none_or(|(_, latest)| *latest < inbox)
        {
            let id = serde_json::from_value(
                accepted
                    .get("id")
                    .cloned()
                    .ok_or_else(|| invalid("accepted prompt has no id"))?,
            )
            .map_err(storage)?;
            self.prompts.insert(reference.into(), (id, inbox));
        }
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
        let field = |name: &str| {
            turn.get(name)
                .cloned()
                .ok_or_else(|| invalid(format!("child {name} absent")))
        };
        let task: AgentPath = serde_json::from_value(field("task")?).map_err(storage)?;
        let claim: TurnClaim = serde_json::from_value(field("claim")?).map_err(storage)?;
        let cwd: PathBuf = serde_json::from_value(field("cwd")?).map_err(storage)?;
        let prompt = field("prompt")?
            .as_str()
            .ok_or_else(|| invalid("native prompt absent"))?
            .as_bytes()
            .to_vec();
        let resume = if self.host == "pi" {
            turn.pointer("/session/file")
                .or_else(|| turn.pointer("/session/id"))
        } else {
            turn.pointer("/session/id")
        };
        let mut command = launch::child(
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
            events: acyclic_native_runtime::lines::LineBuffer::new(EVENT_LIMIT),
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

    /// Reads one bounded pass of the child's native event stream, saving its
    /// resume reference and keeping its latest answer.
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
            let bytes = chunk
                .get(..count)
                .ok_or_else(|| invalid("native output count invalid"))?;
            for line in running.events.push(bytes) {
                let Ok(line) = line else {
                    // A dropped answer must replace an earlier preview. Keep
                    // draining through the same bounded framer and process
                    // owner; later complete answers remain usable.
                    running.answer = "[Native output omitted: an event exceeded the 65,536-byte limit or contained invalid UTF-8.]".into();
                    continue;
                };
                let Ok(event) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                // The whole event stream joins the conversation at settlement.
                running.process.record_event(line.as_bytes())?;
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
                if let Some(answer) = answer(&event) {
                    running.answer = answer;
                }
            }
        }
        Ok(())
    }

    /// Advances every running child by one bounded pass. Returns whether any
    /// output flowed; a child whose turn settled has its next input started.
    pub fn pump(&mut self) -> Result<bool> {
        let mut progressed = false;
        for reference in self.children.keys().cloned().collect::<Vec<_>>() {
            let Some(mut running) = self.children.remove(&reference) else {
                continue;
            };
            let refreshed = self.refresh(&reference, &mut running);
            progressed |= running.process.take_output_progress();
            match refreshed {
                Ok(true) => self.start(&reference)?,
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

    /// Whether the process exited and its turn settled.
    fn refresh(&self, reference: &str, running: &mut Running) -> Result<bool> {
        let mut exited = match &running.failure {
            Some(failure) => Err(failure.clone()),
            None => running
                .process
                .pump_stdin()
                .and_then(|_| self.output(reference, running))
                .and_then(|()| running.process.try_wait())
                .map_err(|error| error.to_string()),
        };
        if matches!(&exited, Ok(Some(_))) {
            running.process.terminate_descendants()?;
            match running.process.pump_stdin() {
                Ok(false) => return Ok(false),
                Ok(true) => {}
                Err(error) => exited = Err(error.to_string()),
            }
            // Exit is the capture boundary. A detached process can retain a
            // pipe writer after containment stops; EOF cannot own settlement.
            if let Err(error) = self.output(reference, running) {
                exited = Err(error.to_string());
            }
        }
        let outcome = match exited {
            Err(message) => Outcome::Failed { message },
            Ok(Some(status)) if status.success() => Outcome::Succeeded(TurnOutput {
                text: running.answer.clone(),
                metadata: Value::Null,
                steps: 1,
                attachments: Vec::new(),
            }),
            Ok(Some(status)) => Outcome::Failed {
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
        outcome: Outcome<TurnOutput>,
    ) -> Result<Value> {
        running.process.release()?;
        let outcome = match (running.process.require_stdin_complete(), outcome) {
            (_, Outcome::Cancelled) => Outcome::Cancelled,
            (_, Outcome::Failed { message }) => Outcome::Failed {
                message: running.process.failure_message(&message),
            },
            (Ok(()), outcome) => outcome,
            (Err(error), _) => Outcome::Failed {
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

    /// Interrupts `reference`: its running turn is cancelled and its queue
    /// held until a resume.
    pub fn cancel(&mut self, reference: &str) -> Result<Value> {
        self.stop(reference, Control::Interrupt)
    }

    /// Archives `reference`, cancelling its running turn and queued inputs.
    /// Its last accepted prompt stays recorded, so every wait, parked or
    /// later, answers with that prompt's reply.
    pub fn archive(&mut self, reference: &str) -> Result<Value> {
        self.stop(reference, Control::Archive)
    }

    fn stop(&mut self, reference: &str, control: Control) -> Result<Value> {
        self.request(reference, SessionAction::Control { control })?;
        if let Some(mut running) = self.children.remove(reference) {
            let result = running.process.terminate_descendants().and_then(|()| {
                let _ = self.output(reference, &mut running);
                self.complete(reference, &mut running, Outcome::Cancelled)
            });
            if let Err(error) = result {
                self.children.insert(reference.into(), running);
                return Err(error);
            }
        }
        self.request(reference, SessionAction::Observe)
    }

    /// `None` while the child has a running input. This owner is the only
    /// one that runs it and starts its next input as each settles, so an idle
    /// child has nothing left that can settle: the answer is then the reply to
    /// its last accepted prompt (answered, dropped, or pending behind a stop).
    /// After a restart, recovered work is awaited the same way, and the
    /// answer is the observed session.
    pub fn wait(&self, reference: &str) -> Result<Option<Value>> {
        if self.children.contains_key(reference) {
            return Ok(None);
        }
        let action = match self.prompts.get(reference) {
            Some((id, inbox)) => SessionAction::Reply {
                id: id.clone(),
                inbox: *inbox,
            },
            None => SessionAction::Observe,
        };
        self.request(reference, action).map(Some)
    }

    /// Interrupts every running child; the owner's host is going away.
    pub fn shutdown(&mut self) -> Result<()> {
        let mut failure = None;
        for reference in self.children.keys().cloned().collect::<Vec<_>>() {
            if let Err(error) = self.cancel(&reference) {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

/// The answer a native event carries: a Codex agent message, a Claude Code
/// result, or a Pi assistant message end.
fn answer(event: &Value) -> Option<String> {
    let text = if event.pointer("/item/type").and_then(Value::as_str) == Some("agent_message") {
        event.pointer("/item/text")
    } else {
        event.get("result")
    };
    if let Some(text) = text.and_then(Value::as_str) {
        return Some(bounded(text));
    }
    let parts = (event.pointer("/message/role").and_then(Value::as_str) == Some("assistant")
        && event.get("type").and_then(Value::as_str) == Some("message_end"))
    .then(|| event.pointer("/message/content").and_then(Value::as_array))
    .flatten()?;
    Some(bounded(
        &parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<String>(),
    ))
}

fn bounded(text: &str) -> String {
    text.chars()
        .scan(0, |bytes, ch| {
            *bytes += ch.len_utf8();
            (*bytes <= ANSWER_LIMIT).then_some(ch)
        })
        .collect()
}

#[cfg(all(test, target_os = "linux"))]
mod tests;

#[cfg(test)]
mod framing {
    use super::*;

    #[test]
    fn oversized_native_events_are_omitted_once_without_wedging_the_stream() {
        let mut events = acyclic_native_runtime::lines::LineBuffer::new(EVENT_LIMIT);
        let mut omitted = 0;
        for _ in 0..16 {
            for event in events.push(&[b'x'; 8192]) {
                assert!(event.is_err());
                omitted += 1;
            }
            assert!(events.buffered_bytes() <= EVENT_LIMIT);
        }
        assert_eq!(omitted, 1, "oversized output must be reported once");
        let lines = events.push(b"\n{\"result\":\"probe\"}\n");
        let [Ok(line)] = lines.as_slice() else {
            panic!("valid frame after omitted event: {lines:?}");
        };
        let event: Value = serde_json::from_str(line).expect("frame is JSON");
        assert_eq!(answer(&event).as_deref(), Some("probe"));
    }

    #[test]
    fn native_answers_are_bounded_on_character_boundaries() {
        assert!(bounded(&"😀".repeat(4096)).len() <= ANSWER_LIMIT);
    }
}
