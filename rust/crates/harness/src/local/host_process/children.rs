//! The one owner of a native parent's managed children.
//!
//! A native host (Claude Code, Codex, Pi) runs its own loop, tools and
//! sandbox; its plugin only forwards coordination calls here. This owner
//! launches each child's next input, which the accepting or settling exchange
//! itself carries, under a physical [`HostProcessLease`], runs the native
//! child under the parent's inherited policy, keeps its answer and whole
//! event log from its event stream, and settles the claim.
//!
//! Each running child registers its readiness once per armed wait with the
//! shared process-event hub; the hub wakes that child's key alone, and a pass
//! pumps only the children whose keys woke. A quiet child is never probed.
//! Waits resolve through the harness's shared committed reply.
use super::*;
use crate::Outcome;
use crate::agent::IntentId;
use crate::executor::TurnOutput;
use crate::local::host_files;
use crate::local::host_owner::{Control, HostAction, SessionAction, TurnClaim};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future as _;
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

mod tools;
pub use tools::serve;

/// Byte bound of one native event line and of a retained answer's source.
const EVENT_LIMIT: usize = 65_536;
/// The saved log's line where the bounded framer dropped one native event.
const EVENT_OMITTED: &[u8] = b"{\"type\":\"acyclic.event_omitted\"}";
/// Byte bound of a retained answer.
const ANSWER_LIMIT: usize = 4096;

struct Running {
    process: HostSessionProcess,
    claim: TurnClaim,
    /// A spawn or stdin failure, settled as the turn's failure.
    failure: Option<String>,
    events: acyclic_native_runtime::lines::LineBuffer,
    answer: Outcome<String>,
    /// The settlement once decided. A retry after an unknown outcome resends
    /// exactly it, under the same claim.
    settlement: Option<Outcome<TurnOutput>>,
    /// This child's key on the ready queue, registered once per lifetime.
    wake: Waker,
    /// The armed readiness wait, held until it fires or the child settles.
    armed: Option<acyclic_native_runtime::ProcessReadiness>,
}

/// One child's accepted context and newest accepted prompt.
struct Accepted {
    receipt: Receipt,
    /// The newest prompt accepted, as its committed `id` and `inbox`: what
    /// an idle child's wait answers with.
    prompt: Option<(IntentId, u64)>,
}

/// The keys of children whose readiness fired, and the server's waker.
#[derive(Default)]
struct Ready(Mutex<Queue>);

#[derive(Default)]
struct Queue {
    keys: BTreeSet<String>,
    waker: Option<Waker>,
}

impl Ready {
    fn queue(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push(&self, key: &str) {
        let waker = {
            let mut queue = self.queue();
            queue.keys.insert(key.to_owned());
            queue.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn take(&self) -> BTreeSet<String> {
        std::mem::take(&mut self.queue().keys)
    }

    fn poll(&self, context: &Context<'_>) -> Poll<()> {
        let mut queue = self.queue();
        if queue.keys.is_empty() {
            queue.waker = Some(context.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

/// One child's registration: the hub firing its wait queues its key alone.
struct ChildWake {
    key: String,
    ready: Arc<Ready>,
}

impl std::task::Wake for ChildWake {
    fn wake(self: Arc<Self>) {
        self.ready.push(&self.key);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.push(&self.key);
    }
}

/// Exclusive owner of one native parent's children, by parent-local name.
pub struct NativeChildren {
    host: String,
    cwd: PathBuf,
    parent: AgentPath,
    flags: Vec<String>,
    children: BTreeMap<String, Running>,
    accepted: BTreeMap<String, Accepted>,
    ready: Arc<Ready>,
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
        let mut children = Self::with(host, cwd, parent, flags, ownership, root);
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
            // Settles the orphaned claim, then carries the next input.
            let mut recovered = children.request(&reference, SessionAction::Recover, true)?;
            children.launch(&reference, &mut recovered)?;
        }
        Ok(children)
    }

    fn with(
        host: &str,
        cwd: PathBuf,
        parent: AgentPath,
        flags: Vec<String>,
        ownership: FileLock,
        root: acyclic_fs::HeldRoot,
    ) -> Self {
        Self {
            host: host.into(),
            cwd,
            parent,
            flags,
            children: BTreeMap::new(),
            accepted: BTreeMap::new(),
            ready: Arc::default(),
            _ownership: ownership,
            _root: root,
        }
    }

    /// One exchange about `reference`; with `next`, its reply also carries
    /// the child's next claimed input when its physical fence is free.
    fn request(&self, reference: &str, action: SessionAction, next: bool) -> Result<Value> {
        host_files::request(
            &self.cwd,
            HostAction::Session {
                parent: self.parent.clone(),
                reference: reference.into(),
                action,
                next,
            },
        )
    }

    /// Records the task a reply names: the first reply fixes it, and any
    /// later one naming another task is refused. Its view may differ: an
    /// idle child's view is released and remounted by a later exchange.
    fn record(&mut self, reference: &str, reply: &Value) -> Result<&mut Accepted> {
        let accepted = match self.accepted.entry(reference.into()) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(Accepted {
                receipt: Receipt::accepted(reply)?,
                prompt: None,
            }),
        };
        accepted.receipt.confirm(reply)?;
        Ok(accepted)
    }

    /// Whether any child process is running.
    #[must_use]
    pub fn running(&self) -> bool {
        !self.children.is_empty()
    }

    /// Completes once some child's readiness fired: its pipes have data or
    /// room, or it exited. Served by the shared process-event hub.
    fn woken(&self) -> impl std::future::Future<Output = ()> + 'static {
        let ready = Arc::clone(&self.ready);
        std::future::poll_fn(move |context| ready.poll(context))
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
        let resumed = self.request(
            reference,
            SessionAction::Control {
                control: Control::Resume,
            },
            false,
        )?;
        self.record(reference, &resumed)?;
        self.send(reference, prompt)
    }

    /// Records the prompt a Spawn or Send accepted, then launches the input
    /// the same exchange claimed. A replay names its original, earlier prompt
    /// and never displaces a newer one.
    fn accept(&mut self, reference: &str, action: SessionAction) -> Result<Value> {
        // A running child's fence is held: claim only for an idle one.
        let idle = !self.children.contains_key(reference);
        let mut accepted = self.request(reference, action, idle)?;
        let inbox = accepted
            .get("inbox")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("accepted prompt has no inbox"))?;
        let id: IntentId = serde_json::from_value(
            accepted
                .get("id")
                .cloned()
                .ok_or_else(|| invalid("accepted prompt has no id"))?,
        )
        .map_err(storage)?;
        let record = self.record(reference, &accepted)?;
        if record
            .prompt
            .as_ref()
            .is_none_or(|(_, latest)| *latest < inbox)
        {
            record.prompt = Some((id, inbox));
        }
        self.launch(reference, &mut accepted)?;
        Ok(accepted)
    }

    /// Launches the claimed input `reply` carries as `next`, removing it from
    /// the reply; a reply carrying none launches nothing.
    fn launch(&mut self, reference: &str, reply: &mut Value) -> Result<()> {
        let turn = reply
            .as_object_mut()
            .and_then(|reply| reply.remove("next"))
            .filter(Value::is_object);
        self.record(reference, reply)?;
        let Some(turn) = turn else {
            return Ok(());
        };
        if self.children.contains_key(reference) {
            return Err(Error::Conflict(
                "native session carried a second claim while running".into(),
            ));
        }
        let receipt = &self
            .accepted
            .get(reference)
            .ok_or_else(|| invalid("native child receipt absent"))?
            .receipt;
        // The view this exchange mounted the child at.
        let view = receipt.confirm(&turn)?;
        let lease = HostProcessLease::admit(&self.cwd, receipt, &turn)?;
        let field = |name: &str| {
            turn.get(name)
                .cloned()
                .ok_or_else(|| invalid(format!("child {name} absent")))
        };
        let claim: TurnClaim = serde_json::from_value(field("claim")?).map_err(storage)?;
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
            &view,
            resume.and_then(Value::as_str),
        )?;
        command
            .env("ACYCLIC_WORKSPACE", &view)
            .env("ACYCLIC_TASK", receipt.task().to_string());
        let process = lease.funded_process()?;
        let wake = Waker::from(Arc::new(ChildWake {
            key: reference.into(),
            ready: Arc::clone(&self.ready),
        }));
        // Install the stable owner before spawn can acquire any physical work.
        // Even a partial spawn or stdin failure stays here until it is reaped.
        let running = self.children.entry(reference.into()).or_insert(Running {
            process,
            claim,
            failure: None,
            events: acyclic_native_runtime::lines::LineBuffer::new(EVENT_LIMIT),
            answer: Outcome::Succeeded(String::new()),
            settlement: None,
            wake,
            armed: None,
        });
        let claim = running.claim.operation;
        if let Err(error) = running
            .process
            .spawn(&mut command, claim)
            .and_then(|()| running.process.queue_stdin(prompt))
        {
            running.failure = Some(error.to_string());
        }
        // The first pass delivers stdin, or settles the failure.
        self.ready.push(reference);
        Ok(())
    }

    /// Arms one readiness wait for a running child, under its registered key.
    fn arm(&self, reference: &str, running: &mut Running) -> Result<()> {
        let mut wait = running.process.readiness()?;
        let mut context = Context::from_waker(&running.wake);
        if std::pin::Pin::new(&mut wait).poll(&mut context).is_ready() {
            self.ready.push(reference);
        } else {
            running.armed = Some(wait);
        }
        Ok(())
    }

    /// Advances only the children whose readiness fired, by one bounded pass
    /// each. A child whose turn settled launches the next input its
    /// settlement carried; one whose pass left output unread is queued again.
    pub fn pump(&mut self) -> Result<()> {
        let mut ready = self.ready.take().into_iter();
        while let Some(reference) = ready.next() {
            let Some(mut running) = self.children.remove(&reference) else {
                continue;
            };
            running.armed = None;
            let result = match self.refresh(&reference, &mut running) {
                Ok(Some(mut settled)) => self.launch(&reference, &mut settled),
                Ok(None) => {
                    let armed = if running.process.take_output_progress() {
                        self.ready.push(&reference);
                        Ok(())
                    } else {
                        self.arm(&reference, &mut running)
                    };
                    self.children.insert(reference.clone(), running);
                    armed
                }
                Err(error) => {
                    self.children.insert(reference.clone(), running);
                    Err(error)
                }
            };
            if let Err(error) = result {
                // The failed child and every child not yet pumped stay ready.
                self.ready.push(&reference);
                for reference in ready {
                    self.ready.push(&reference);
                }
                return Err(error);
            }
        }
        Ok(())
    }

    /// The settlement reply, carrying the next input, once the process exited
    /// and its turn settled.
    fn refresh(&self, reference: &str, running: &mut Running) -> Result<Option<Value>> {
        if running.settlement.is_some() {
            // A settlement whose exchange had no known outcome is resent.
            return self
                .complete(reference, running, Outcome::Cancelled, true)
                .map(Some);
        }
        let mut exited = match &running.failure {
            Some(failure) => Err(failure.clone()),
            None => running
                .process
                .pump_stdin()
                .and_then(|_| output(running))
                .and_then(|()| running.process.try_wait())
                .map_err(|error| error.to_string()),
        };
        if matches!(&exited, Ok(Some(_))) {
            running.process.terminate_descendants()?;
            match running.process.pump_stdin() {
                Ok(false) => return Ok(None),
                Ok(true) => {}
                Err(error) => exited = Err(error.to_string()),
            }
            // Exit is the capture boundary. A detached process can retain a
            // pipe writer after containment stops; EOF cannot own settlement.
            if let Err(error) = output(running) {
                exited = Err(error.to_string());
            }
        }
        let outcome = match exited {
            Err(message) => Outcome::Failed { message },
            Ok(Some(status)) if status.success() => match &running.answer {
                Outcome::Succeeded(text) => Outcome::Succeeded(TurnOutput {
                    text: text.clone(),
                    metadata: Value::Null,
                    steps: 1,
                    attachments: Vec::new(),
                }),
                Outcome::Failed { message } => Outcome::Failed {
                    message: message.clone(),
                },
                Outcome::Cancelled => Outcome::Cancelled,
                Outcome::Indeterminate {
                    operation_id,
                    cause,
                } => Outcome::Indeterminate {
                    operation_id: *operation_id,
                    cause: cause.clone(),
                },
            },
            Ok(Some(status)) => Outcome::Failed {
                message: format!("native host exited {status}"),
            },
            Ok(None) => return Ok(None),
        };
        self.complete(reference, running, outcome, true).map(Some)
    }

    /// Settles the running claim. The first settlement decided is kept, so
    /// every retry presents the identical claim and outcome.
    fn complete(
        &self,
        reference: &str,
        running: &mut Running,
        outcome: Outcome<TurnOutput>,
        next: bool,
    ) -> Result<Value> {
        running.process.release()?;
        let settlement = match &running.settlement {
            Some(settlement) => settlement.clone(),
            None => {
                let settlement = match (running.process.require_stdin_complete(), outcome) {
                    (_, Outcome::Cancelled) => Outcome::Cancelled,
                    (_, Outcome::Failed { message }) => Outcome::Failed {
                        message: running.process.failure_message(&message),
                    },
                    (Ok(()), outcome) => outcome,
                    (Err(error), _) => Outcome::Failed {
                        message: running.process.failure_message(&error.to_string()),
                    },
                };
                running.settlement = Some(settlement.clone());
                settlement
            }
        };
        self.request(
            reference,
            SessionAction::Complete {
                claim: running.claim.clone(),
                outcome: settlement,
            },
            next,
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

    /// The control's own reply, or the settlement's when a turn was running:
    /// no separate observation follows.
    fn stop(&mut self, reference: &str, control: Control) -> Result<Value> {
        let mut reply = self.request(reference, SessionAction::Control { control }, false)?;
        self.record(reference, &reply)?;
        if let Some(mut running) = self.children.remove(reference) {
            running.armed = None;
            let result = if running.settlement.is_some() {
                self.complete(reference, &mut running, Outcome::Cancelled, false)
            } else {
                running.process.terminate_descendants().and_then(|()| {
                    let _ = output(&mut running);
                    self.complete(reference, &mut running, Outcome::Cancelled, false)
                })
            };
            match result {
                Ok(settled) => reply = settled,
                Err(error) => {
                    self.children.insert(reference.into(), running);
                    self.ready.push(reference);
                    return Err(error);
                }
            }
            self.record(reference, &reply)?;
        }
        Ok(reply)
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
        let action = match self
            .accepted
            .get(reference)
            .and_then(|accepted| accepted.prompt.as_ref())
        {
            Some((id, inbox)) => SessionAction::Reply {
                id: id.clone(),
                inbox: *inbox,
            },
            None => SessionAction::Observe,
        };
        self.request(reference, action, false).map(Some)
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

/// Reads one bounded pass of the child's native event stream into its
/// custodied event log, keeping its latest answer. The settlement derives the
/// native resume reference from that log, so a running turn sends nothing.
fn output(running: &mut Running) -> Result<()> {
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
                // A dropped answer replaces an earlier preview, preserving
                // terminal refusals. Keep draining through the same framer
                // and owner; later complete answers remain usable.
                if matches!(running.answer, Outcome::Succeeded(_)) {
                    running.answer = Outcome::Succeeded("[Native output omitted: an event exceeded the 65,536-byte limit or contained invalid UTF-8.]".into());
                }
                // The saved log marks where the event was dropped.
                running.process.record_event(EVENT_OMITTED)?;
                continue;
            };
            let Ok(event) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            // The whole event stream joins the conversation at settlement.
            running.process.record_event(line.as_bytes())?;
            if let Some(answer) = answer(&event) {
                running.answer = answer;
            }
        }
    }
    Ok(())
}

/// The answer a native event carries: a Codex agent message, a Claude Code
/// result, or a Pi assistant message end.
fn answer(event: &Value) -> Option<Outcome<String>> {
    let text = if event.pointer("/item/type").and_then(Value::as_str) == Some("agent_message") {
        event.pointer("/item/text")
    } else {
        event.get("result")
    };
    if let Some(text) = text.and_then(Value::as_str) {
        return Some(Outcome::Succeeded(bounded(text)));
    }
    // Pi can report a failed assistant turn and still exit successfully. Keep
    // its semantic outcome until process exit; a subsequent successful turn
    // replaces it if the native host retries the request itself.
    if event.get("type").and_then(Value::as_str) == Some("message_end")
        && event.pointer("/message/role").and_then(Value::as_str) == Some("assistant")
        && matches!(
            event.pointer("/message/stopReason").and_then(Value::as_str),
            Some("error" | "aborted")
        )
    {
        return Some(
            if event.pointer("/message/stopReason").and_then(Value::as_str) == Some("aborted") {
                Outcome::Cancelled
            } else {
                Outcome::Failed {
                    message: event
                        .pointer("/message/errorMessage")
                        .and_then(Value::as_str)
                        .unwrap_or("native assistant turn failed")
                        .to_owned(),
                }
            },
        );
    }
    let parts = (event.pointer("/message/role").and_then(Value::as_str) == Some("assistant")
        && event.get("type").and_then(Value::as_str) == Some("message_end"))
    .then(|| event.pointer("/message/content").and_then(Value::as_array))
    .flatten()?;
    Some(Outcome::Succeeded(bounded(
        &parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<String>(),
    )))
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
    fn native_assistant_refusal_is_not_a_successful_empty_answer() {
        let mut captured = include_str!("fixtures/pi-assistant-error.jsonl").lines();
        let event: Value = serde_json::from_str(captured.next().expect("captured assistant"))
            .expect("captured Pi event");
        assert_eq!(
            answer(&event),
            Some(Outcome::Failed {
                message: "No API key for provider: fixture".into()
            })
        );
        // The session-level settlement event has no assistant outcome. It must
        // not erase a preceding terminal error while the process exits.
        let settled: Value = serde_json::from_str(captured.next().expect("captured settlement"))
            .expect("captured Pi settlement");
        assert_eq!(answer(&settled), None);
    }

    #[test]
    fn a_receipt_follows_its_child_to_a_remounted_view() {
        let parent = AgentPath::root(&crate::agent::AgentId::from_bytes([1; 16]));
        let Ok(child) = parent.child(&crate::agent::AgentId::from_bytes([2; 16])) else {
            panic!("a child path nests under its root");
        };
        let reply = |task: &AgentPath, cwd: &str| serde_json::json!({"task": task, "cwd": cwd});
        let Ok(receipt) = Receipt::accepted(&reply(&child, "/w/.views/a")) else {
            panic!("the first reply names the child");
        };
        // An idle child's view was released; the next exchange mounted it
        // elsewhere, and its turn runs there.
        assert_eq!(
            receipt.confirm(&reply(&child, "/w/.views/b")).ok(),
            Some(PathBuf::from("/w/.views/b"))
        );
        // Negative control: a reply naming another task is still refused.
        assert!(matches!(
            receipt.confirm(&reply(&parent, "/w/.views/b")),
            Err(Error::Conflict(_))
        ));
        assert!(
            receipt
                .confirm(&serde_json::json!({"task": child}))
                .is_err()
        );
    }

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
        assert_eq!(answer(&event), Some(Outcome::Succeeded("probe".into())));
    }

    #[test]
    fn native_answers_are_bounded_on_character_boundaries() {
        assert!(bounded(&"😀".repeat(4096)).len() <= ANSWER_LIMIT);
    }

    /// The ready queue holds only children whose registration fired, and a
    /// fired key wakes the server once; a quiet key is never queued.
    #[test]
    fn only_fired_registrations_reach_the_ready_queue() {
        let ready = Arc::new(Ready::default());
        let wakes = (0..1000)
            .map(|index| {
                Waker::from(Arc::new(ChildWake {
                    key: format!("child-{index}"),
                    ready: Arc::clone(&ready),
                }))
            })
            .collect::<Vec<_>>();
        let server = Waker::noop();
        assert!(ready.poll(&Context::from_waker(server)).is_pending());
        wakes[7].wake_by_ref();
        wakes[7].wake_by_ref();
        assert!(ready.poll(&Context::from_waker(server)).is_ready());
        assert_eq!(ready.take(), BTreeSet::from(["child-7".to_owned()]));
        assert!(ready.take().is_empty(), "999 quiet children were queued");
    }
}
