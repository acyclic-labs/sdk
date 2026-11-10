//! Physical custody shared by native host adapters and their workspace owner.
//!
//! The lock precedes claiming a turn and outlives its process tree. Settlement
//! takes that same lock and fences orphaned trees before capturing any files.
//! There is no adapter journal or caller-supplied quiescence assertion.
use super::*;
use crate::agent::AgentPath;
use acyclic_native_runtime::{
    FileLock, FileLockMode, ProcessCommand, ProcessCustody, ProcessDrain, ProcessTreeStorage,
};

pub mod children;
pub mod launch;

/// The native event log of the turn `claim`, in its custody directory. Each
/// turn claim owns its own log, so a settlement never reads another turn's.
fn event_log(claim: OperationId) -> PathBuf {
    PathBuf::from(format!("events-{claim}.jsonl"))
}
/// Bound of one turn's native event log, its omission marker included.
pub const EVENT_LOG_BYTES: u64 = 4 * 1024 * 1024;
/// The line ending a log whose later events exceeded its bound.
const EVENTS_OMITTED: &[u8] = b"{\"type\":\"acyclic.events_omitted\"}\n";

/// Runs the interactive native root with its inherited console and process
/// group. Native hosts retain terminal/job control; managed children use the
/// separate, custodied session API below. Turn-end hooks capture root turns.
/// A `prompt` runs the root headless instead, in an owned process tree.
pub fn run_root_session(
    cwd: &Path,
    host: &str,
    program: &std::ffi::OsStr,
    arguments: &[String],
    prompt: Option<&str>,
) -> Result<std::process::ExitStatus> {
    use super::host_owner::HostAction;
    let native_arguments = launch::root_arguments(host, arguments, cwd, prompt)?;
    let location = super::host_files::request(
        cwd,
        HostAction::Open {
            host: host.into(),
            session: OperationId::new().to_string(),
        },
    )?;
    let task: AgentPath = serde_json::from_value(
        location
            .get("task")
            .cloned()
            .ok_or_else(|| Error::Invalid("native root task absent".into()))?,
    )
    .map_err(storage)?;
    let resolved = acyclic_native_runtime::host_command(program).map_err(storage)?;
    let host_args = serde_json::to_string(arguments).map_err(storage)?;
    let status = if prompt.is_some() {
        let mut command = ProcessCommand::new(resolved.get_program());
        command
            .args(resolved.get_args())
            .args(native_arguments)
            .current_dir(cwd)
            .env("ACYCLIC_HOST_ARGS", host_args)
            .env("ACYCLIC_WORKSPACE", cwd)
            .env("ACYCLIC_TASK", task.to_string());
        let model = launch::selected_model(host, arguments);
        let context = native_context(&task, cwd, model)?;
        run_headless(&mut command, &mut std::io::stdout().lock(), &context, model)?
    } else {
        let mut command = resolved;
        command
            .args(native_arguments)
            .current_dir(cwd)
            .env("ACYCLIC_HOST_ARGS", host_args)
            .env("ACYCLIC_WORKSPACE", cwd)
            .env("ACYCLIC_TASK", task.to_string());
        command.status().map_err(storage)
    };
    // Capture only a quiescent tree. Once retirement is proved, preserve the
    // original output/exit error while still capturing its completed writes.
    let captured = super::host_files::request(cwd, HostAction::Capture { task });
    let status = status?;
    captured?;
    Ok(status)
}

/// Runs a headless root in an owned process tree and forwards its events to
/// `output`. The whole tree, tools included, is stopped and joined before
/// the outer result succeeds. The inner result preserves output/exit failure;
/// the caller may capture only after retirement has been proved.
fn run_headless(
    command: &mut ProcessCommand,
    output: &mut impl std::io::Write,
    context: &[u8],
    model: Option<&str>,
) -> Result<Result<std::process::ExitStatus>> {
    command
        .stdin(acyclic_native_runtime::ProcessStdio::null())
        .stdout(acyclic_native_runtime::ProcessStdio::piped());
    let mut tree = acyclic_native_runtime::ProcessTree::spawn(command).map_err(storage)?;
    let forwarded = (|| {
        let events =
            acyclic_native_runtime::ProcessPipeReader::take_stdout(&mut tree).map_err(storage)?;
        let mut events = HeadlessOutput {
            events,
            tree: &mut tree,
            status: None,
        };
        output.write_all(context).map_err(storage)?;
        output.write_all(b"\n").map_err(storage)?;
        output.flush().map_err(storage)?;
        forward_events(&mut events, output, model)?;
        Ok(events.status)
    })();
    let status = match forwarded {
        Ok(Some(status)) => Ok(status),
        Ok(None) => tree.wait_exit(None).map_err(storage),
        Err(error) => Err(error),
    };
    // Descendants that outlive the root (or a failed copy) are stopped here.
    tree.terminate(None).map_err(storage)?;
    Ok(status)
}

/// The transferred pipe and the original child's exit are observed together.
/// A quiet inherited writer cannot hide the root's terminal event.
struct HeadlessOutput<'a> {
    events: acyclic_native_runtime::ProcessPipeReader,
    tree: &'a mut acyclic_native_runtime::ProcessTree,
    status: Option<std::process::ExitStatus>,
}

impl std::io::Read for HeadlessOutput<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        use tokio::io::AsyncReadExt as _;
        futures::executor::block_on(async {
            loop {
                if self.status.is_some() {
                    return self.events.read(buffer).await;
                }
                let exited = self.tree.readiness()?;
                tokio::select! {
                    read = self.events.read(buffer) => return read,
                    outcome = exited => outcome.into_result()?,
                }
                if let Some(status) = self.tree.try_wait(None)? {
                    self.tree.terminate(None)?;
                    self.status = Some(status);
                }
            }
        })
    }
}
/// Bytes of one native event buffered to attribute its model. A longer event
/// passes through verbatim in read-sized chunks, with the model it reported.
const ATTRIBUTION_WINDOW: usize = 64 * 1024;

/// Forwards native JSONL events. Events within [`ATTRIBUTION_WINDOW`] gain
/// the launch model when absent; larger events are streamed unmodified, so no
/// event size fails the run.
fn forward_events(
    mut input: impl std::io::Read,
    output: &mut impl std::io::Write,
    model: Option<&str>,
) -> Result<()> {
    let emit = |line: &[u8], output: &mut dyn std::io::Write| -> Result<()> {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            return Ok(());
        }
        output
            .write_all(&attribute_model(line, model)?)
            .map_err(storage)?;
        output.write_all(b"\n").map_err(storage)
    };
    let mut line = Vec::new();
    let mut streaming = false;
    let mut buffer = [0; 8192];
    loop {
        let count = match input.read(&mut buffer) {
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(storage(error)),
        };
        if count == 0 {
            break;
        }
        let mut bytes = &buffer[..count];
        while !bytes.is_empty() {
            let end = bytes.iter().position(|byte| *byte == b'\n');
            let (part, rest) = match end {
                Some(end) => (&bytes[..end], &bytes[end + 1..]),
                None => (bytes, &[][..]),
            };
            if !streaming && line.len() + part.len() > ATTRIBUTION_WINDOW {
                output.write_all(&line).map_err(storage)?;
                line.clear();
                streaming = true;
            }
            if streaming {
                output.write_all(part).map_err(storage)?;
            } else {
                line.extend_from_slice(part);
            }
            if end.is_some() {
                if streaming {
                    output.write_all(b"\n").map_err(storage)?;
                    streaming = false;
                } else {
                    emit(&line, output)?;
                    line.clear();
                }
            }
            bytes = rest;
        }
        output.flush().map_err(storage)?;
    }
    if streaming {
        output.write_all(b"\n").map_err(storage)?;
    } else {
        emit(&line, output)?;
    }
    output.flush().map_err(storage)
}

/// Exclusive right to start one agent's native session processes.
pub struct HostProcessLease {
    task: AgentPath,
    view: PathBuf,
    custody: Arc<ProcessCustody>,
    _ownership: FileLock,
    root: acyclic_fs::HeldRoot,
}

/// Physical proof held until capture and result publication both finish.
///
/// Its fields are private and it has no serialization or cloning operation.
pub struct QuiescentHostProcess(HostProcessLease);

/// One child's admitted task, exactly as the live harness accepted it:
/// immutable for the child's lifetime. Every later reply must name the same
/// task, so a competitor is refused, and no separate observation precedes a
/// launch. The child is its task, not a path: its view is wherever the
/// harness mounted it for each exchange, so an idle child's view can be
/// released and remounted elsewhere without invalidating the receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    task: AgentPath,
}

impl Receipt {
    /// The task a session reply names.
    pub fn accepted(reply: &serde_json::Value) -> Result<Self> {
        let task = serde_json::from_value(
            reply
                .get("task")
                .cloned()
                .ok_or_else(|| Error::Invalid("native child task absent".into()))?,
        )
        .map_err(storage)?;
        Ok(Self { task })
    }

    /// The view `reply` names for its own exchange; a reply naming another
    /// task is refused.
    pub fn confirm(&self, reply: &serde_json::Value) -> Result<PathBuf> {
        if Self::accepted(reply)? != *self {
            return Err(Error::Conflict(
                "step=claim native process: admitted task changed; recovery: acyclic agents".into(),
            ));
        }
        reply
            .get("cwd")
            .and_then(serde_json::Value::as_str)
            .map(PathBuf::from)
            .ok_or_else(|| Error::Invalid("native child view absent".into()))
    }

    /// The admitted agent.
    #[must_use]
    pub const fn task(&self) -> &AgentPath {
        &self.task
    }
}

impl HostProcessLease {
    /// Takes the physical execution lease of one claimed child input, which
    /// its accepting exchange carried. The turn must name the receipt's own
    /// task, and runs in the view that exchange mounted, inside the
    /// workspace: adapters cannot construct an execution lease from
    /// caller-supplied task/view strings. A competitor holding the lease is
    /// refused; the claim stays exact for the next fenced handoff.
    pub fn admit(cwd: &Path, receipt: &Receipt, turn: &serde_json::Value) -> Result<Self> {
        let view = receipt.confirm(turn)?;
        // This process serves no view, so it may resolve paths through one.
        Self::acquire(
            &std::fs::canonicalize(super::host_files::workspace_root(cwd)?).map_err(storage)?,
            &receipt.task,
            &std::fs::canonicalize(view).map_err(storage)?,
        )
    }

    /// Acquires the task's workspace-local custody before claiming an input.
    /// A live owner refuses acquisition; a dead owner's trees are fenced using
    /// the runtime's existing cross-platform process containment implementation.
    /// Both paths name canonical locations; Windows drive/UNC prefixes may use
    /// equivalent verbatim spelling. The owner that serves `view` passes the
    /// path it mounted and never resolves it, since resolving a served view
    /// waits on the owner's own callbacks.
    pub(super) fn acquire(workspace: &Path, task: &AgentPath, view: &Path) -> Result<Self> {
        if acyclic_native_runtime::paths::strip_native_prefix(view, workspace).is_none() {
            return Err(Error::Unauthorized(
                "step=admit native process view: outside workspace; recovery: reopen the task through acyclic host".into(),
            ));
        }
        let root = acyclic_fs::hold_root(workspace)
            .map_err(storage)?
            .create_directories(
                &PathBuf::from(".acyclic-sdk/host-processes").join(task.id().as_str()),
                acyclic_fs::NativeDurability::Durable,
            )
            .map_err(storage)?;
        let ownership = FileLock::try_acquire(
            root.open_file(Path::new("owner.lock"), true).map_err(storage)?,
            FileLockMode::Exclusive,
        )
        .map_err(|error| Error::Conflict(format!(
            "step=acquire native process custody for {task}: {error}; recovery: acyclic --task {task} agents"
        )))?;
        // The exclusive lock is retained throughout the fence and all later
        // execution. Another process cannot start while settlement captures.
        let custody = ProcessCustody::acquire(root.path()).map_err(storage)?;
        Ok(Self {
            task: task.clone(),
            view: view.to_path_buf(),
            custody,
            _ownership: ownership,
            root,
        })
    }

    /// Funds the process slot on the supplied original invocation, before any
    /// physical spawn. The lease moves with that slot and cannot escape it.
    pub fn process(
        self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<HostSessionProcess> {
        let tree = ProcessTreeStorage::new(&work.runtime_meter()).map_err(storage)?;
        Ok(HostSessionProcess {
            tree,
            lease: Some(self),
            work: work.clone(),
            output_progress: false,
            events: None,
        })
    }

    /// Admits one child's storage and retirement independently of its manager.
    pub fn funded_process(self) -> Result<HostSessionProcess> {
        self.process(&polling_work()?)
    }

    /// An unstarted lease already fenced every preceding physical owner.
    pub fn quiescent(self) -> QuiescentHostProcess {
        QuiescentHostProcess(self)
    }
}

impl QuiescentHostProcess {
    /// The native event log the process of turn `claim` recorded; `None`
    /// when that turn's process never started.
    pub(crate) fn events(&self, claim: OperationId) -> Result<Option<Vec<u8>>> {
        let file = match self.0.root.open_file(&event_log(claim), false) {
            Ok(file) => file,
            Err(acyclic_fs::demand::DemandError::Absent) => return Ok(None),
            Err(acyclic_fs::demand::DemandError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(error) => return Err(storage(error)),
        };
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(
            &mut std::io::Read::take(file, EVENT_LOG_BYTES + 1),
            &mut bytes,
        )
        .map_err(storage)?;
        if bytes.len() as u64 > EVENT_LOG_BYTES {
            return Err(Error::Invalid("native event log exceeds its bound".into()));
        }
        Ok(Some(bytes))
    }

    /// Removes the log of turn `claim` once its conversation entry holds it.
    pub(crate) fn retire_events(&self, claim: OperationId) -> Result<()> {
        remove_absent(&self.0.root, &event_log(claim))
    }

    /// Refuses applying one task's physical proof to another view or claim.
    pub fn verify(&self, task: &AgentPath, view: &Path) -> Result<()> {
        if self.0.task != *task
            || acyclic_host_platform::paths::strip_native_prefix(view, &self.0.view)
                != Some(Path::new(""))
        {
            return Err(Error::Conflict(
                "step=verify native quiescence: task or view differs; recovery: reopen the original task through acyclic host".into(),
            ));
        }
        Ok(())
    }
}

/// A native host process, with its exact workspace lease and original funding.
/// The tree retires before the lease, including a partially failed spawn.
pub struct HostSessionProcess {
    tree: ProcessTreeStorage,
    lease: Option<HostProcessLease>,
    work: acyclic_fs::NativeMaterializationWork,
    output_progress: bool,
    events: Option<EventLog>,
}

/// The running turn's native event log and the bytes it holds.
struct EventLog {
    file: std::fs::File,
    written: u64,
    omitted: bool,
    model: Option<String>,
}

fn native_context(task: &AgentPath, cwd: &Path, model: Option<&str>) -> Result<Vec<u8>> {
    let mut context = serde_json::json!({
        "type": "acyclic.host_context", "version": 1, "task": task, "cwd": cwd,
    });
    if let Some(model) = model {
        context["model"] = model.into();
    }
    serde_json::to_vec(&context).map_err(storage)
}

/// Native events keep reported models; the explicit launch selection fills an
/// absent name in the same conversation event for every host family.
fn attribute_model<'a>(line: &'a [u8], model: Option<&str>) -> Result<std::borrow::Cow<'a, [u8]>> {
    if let Some(model) = model
        && let Ok(serde_json::Value::Object(mut event)) = serde_json::from_slice(line)
        && !event.contains_key("model")
    {
        event.insert("model".into(), model.into());
        return serde_json::to_vec(&event)
            .map(std::borrow::Cow::Owned)
            .map_err(storage);
    }
    Ok(std::borrow::Cow::Borrowed(line))
}

impl EventLog {
    fn record(&mut self, line: &[u8]) -> Result<()> {
        if self.omitted {
            return Ok(());
        }
        let line = attribute_model(line, self.model.as_deref())?;
        let mut bytes = Vec::with_capacity(line.len() + 1);
        if self.written + line.len() as u64 + 1 + EVENTS_OMITTED.len() as u64 <= EVENT_LOG_BYTES {
            bytes.extend_from_slice(&line);
            bytes.push(b'\n');
        } else {
            self.omitted = true;
            bytes.extend_from_slice(EVENTS_OMITTED);
        }
        std::io::Write::write_all(&mut self.file, &bytes).map_err(storage)?;
        self.written += bytes.len() as u64;
        Ok(())
    }
}

/// Removes `leaf` from `root`; an absent file is already removed.
fn remove_absent(root: &acyclic_fs::HeldRoot, leaf: &Path) -> Result<()> {
    match root.remove_file(leaf, acyclic_fs::NativeDurability::Durable) {
        Ok(()) | Err(acyclic_fs::demand::DemandError::Absent) => Ok(()),
        Err(acyclic_fs::demand::DemandError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(())
        }
        Err(error) => Err(storage(error)),
    }
}

fn polling_work() -> Result<acyclic_fs::NativeMaterializationWork> {
    acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(65_536, 65_536),
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

impl HostSessionProcess {
    /// Starts the process of turn `claim` in the exact admitted view. Hosts
    /// retain their tools and policy.
    pub fn spawn(
        &mut self,
        command: &mut ProcessCommand,
        claim: OperationId,
        model: Option<&str>,
    ) -> Result<()> {
        let lease = self.lease.as_ref().ok_or_else(|| Error::Conflict(
            "step=spawn native session: process already released; recovery: reopen the task through acyclic host".into()
        ))?;
        if command
            .get_current_dir()
            .and_then(|cwd| acyclic_host_platform::paths::strip_native_prefix(cwd, &lease.view))
            != Some(Path::new(""))
        {
            return Err(Error::Unauthorized(
                "step=spawn native session: cwd differs from admitted view; recovery: reopen the task through acyclic host".into(),
            ));
        }
        command
            .env("ACYCLIC_WORKSPACE", &lease.view)
            .env("ACYCLIC_TASK", lease.task.to_string());
        let context = native_context(&lease.task, &lease.view, model)?;
        // A relaunch of the same claim restarts its log.
        let log = event_log(claim);
        remove_absent(&lease.root, &log)?;
        let file = lease.root.open_file(&log, true).map_err(storage)?;
        let mut events = EventLog {
            file,
            written: 0,
            omitted: false,
            model: model.map(str::to_owned),
        };
        events.record(&context)?;
        if events.omitted {
            return Err(Error::Invalid("step=record native context: metadata exceeds the native event frame bound; recovery: shorten the selected task path before launching the native host".into()));
        }
        self.events = Some(events);
        self.tree
            .spawn(command, Some(&lease.custody))
            .map_err(storage)
    }

    /// Appends one native event line to the turn's log, which its settlement
    /// appends to the conversation. Events past the bound are omitted, once
    /// marked.
    pub fn record_event(&mut self, line: &[u8]) -> Result<()> {
        let log = self
            .events
            .as_mut()
            .ok_or_else(|| Error::Conflict("native event log absent before spawn".into()))?;
        log.record(line)
    }

    /// Queues a prompt on the originally funded native stdin owner.
    pub fn queue_stdin(&mut self, bytes: Vec<u8>) -> Result<()> {
        self.live_io()?.queue_stdin(bytes).map_err(storage)
    }

    /// Advances the existing native stdin pump.
    pub fn pump_stdin(&mut self) -> Result<bool> {
        self.live_io()?.pump_stdin().map_err(storage)
    }

    /// Verifies delivery even after physical retirement.
    pub fn require_stdin_complete(&self) -> Result<()> {
        self.tree.require_stdin_complete().map_err(storage)
    }

    /// Reads through the existing bounded native output owner.
    pub fn read_stdout(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        let read = self.live_io()?.read_stdout(bytes).map_err(storage)?;
        self.output_progress |= read.is_some_and(|count| count > 0);
        Ok(read)
    }

    /// Drains bounded stderr through shared process capture, retaining its tail.
    pub fn drain_stderr(&mut self) -> Result<ProcessDrain> {
        self.require_live()?;
        let meter = self.polling_meter()?;
        let drained = self.tree.drain_stderr(&meter).map_err(storage)?;
        self.output_progress |= drained.read_bytes > 0;
        Ok(drained)
    }

    /// Stops descendants after root exit and fixes the original pipe boundary.
    /// Both operations share the terminal observation's funding window.
    pub fn seal_output(&mut self) -> Result<()> {
        let mut owner = self.live_io()?;
        owner.terminate_descendants().map_err(storage)?;
        owner.seal_output().map_err(storage)
    }

    /// Arms one wait for this child's next pipe data, writable stdin or exit,
    /// served by the shared process-event hub without a thread or timer.
    pub fn readiness(&mut self) -> Result<acyclic_native_runtime::ProcessReadiness> {
        self.live_io()?.readiness().map_err(storage)
    }

    /// Takes the actual I/O progress edge; quiet polls never manufacture one.
    pub fn take_output_progress(&mut self) -> bool {
        std::mem::take(&mut self.output_progress)
    }

    /// Formats a failed native outcome with bounded diagnostic context.
    pub fn failure_message(&self, message: &str) -> String {
        let tail = self.tree.stderr_tail();
        if tail.is_empty() {
            message.into()
        } else {
            format!("{message}\n{}", String::from_utf8_lossy(tail))
        }
    }

    /// Observes native exit without releasing descendant custody.
    pub fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>> {
        self.live_io()?.try_wait().map_err(storage)
    }

    /// Stops descendants using the one runtime containment implementation.
    pub fn terminate_descendants(&mut self) -> Result<()> {
        self.live_io()?.terminate_descendants().map_err(storage)
    }

    fn live_io(&mut self) -> Result<acyclic_native_runtime::ProcessTreeIo<'_>> {
        self.require_live()?;
        // Each bounded operation is independently admitted. Session duration
        // cannot spend another operation's credit or replace retirement funds.
        let meter = self.polling_meter()?;
        self.tree.io(&meter).map_err(storage)
    }

    fn polling_meter(&self) -> Result<Arc<dyn acyclic_native_runtime::NativeReadMeter>> {
        self.work
            .invocation_window()
            .map(|window| window.runtime_meter())
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })
    }

    fn require_live(&self) -> Result<()> {
        if self.lease.is_none() {
            return Err(Error::Conflict(
                "step=access native session: process already released; recovery: reopen the task through acyclic host".into(),
            ));
        }
        Ok(())
    }

    /// Releases execution custody only after the complete tree is reaped.
    /// Publication reacquires that lease and retains it through the commit.
    pub fn release(&mut self) -> Result<()> {
        if self.lease.is_none() {
            return Ok(());
        }
        // Capture ready diagnostics before retirement closes the native pipes,
        // including when input delivery failed before the ordinary output poll.
        self.drain_stderr()?;
        self.tree.terminate().map_err(storage)?;
        if !self.tree.is_reaped() {
            return Err(Error::Conflict(
                "step=release native process: containment remains active; recovery: reopen the original task through acyclic host".into(),
            ));
        }
        // Settlement reads the closed log under the lease this releases.
        self.events.take();
        self.lease.take();
        Ok(())
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn events_of_any_size_reach_the_caller_and_small_ones_gain_the_model() -> Result<()> {
        let large = format!(
            "{{\"type\":\"reply\",\"text\":\"{}\"}}",
            "x".repeat(4 * ATTRIBUTION_WINDOW)
        );
        let input = format!("{large}\n{{\"type\":\"step\"}}\r\n\n{{\"type\":\"end\"}}");
        let mut output = Vec::new();
        forward_events(input.as_bytes(), &mut output, Some("launched"))?;
        let lines = output.split(|byte| *byte == b'\n').collect::<Vec<_>>();
        assert_eq!(lines.len(), 4, "three events and the final newline");
        assert_eq!(
            lines[0],
            large.as_bytes(),
            "a large event is forwarded verbatim"
        );
        for (line, kind) in [(lines[1], "step"), (lines[2], "end")] {
            let event: serde_json::Value = serde_json::from_slice(line).map_err(storage)?;
            assert_eq!(event["type"], kind);
            assert_eq!(event["model"], "launched");
        }
        assert!(lines[3].is_empty());
        Ok(())
    }

    /// A caller that stops reading must not leave a running tool behind:
    /// the headless root's whole tree is stopped before capture.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_failed_headless_copy_stops_the_roots_tools() -> Result<()> {
        struct ClosedAfterContext(bool);
        impl std::io::Write for ClosedAfterContext {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.0 {
                    return Err(std::io::ErrorKind::BrokenPipe.into());
                }
                self.0 = bytes.contains(&b'\n');
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let root = tempfile::tempdir().map_err(storage)?;
        let pid_file = root.path().join("tool.pid");
        let mut command = ProcessCommand::new("/bin/sh");
        command
            .args([
                "-c",
                "sleep 1000 & echo $! > \"$1\"; echo '{}'; exec sleep 1000",
                "root",
            ])
            .arg(&pid_file)
            .current_dir(root.path());
        let error = run_headless(&mut command, &mut ClosedAfterContext(false), b"{}", None)
            .expect("the failed copy still proves tree retirement")
            .expect_err("a closed caller fails the copy");
        assert!(matches!(error, Error::Storage(_)), "{error}");
        let tool = std::fs::read_to_string(&pid_file).map_err(storage)?;
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", tool.trim())).ok();
        // The tool is gone, or at most an unreaped zombie of a foreign reaper.
        assert!(
            stat.as_deref().is_none_or(|stat| stat
                .rsplit_once(") ")
                .is_some_and(|(_, rest)| rest.starts_with(['Z', 'X']))),
            "the root's tool outlived its failed copy: {stat:?}"
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn an_exited_headless_root_stops_inherited_output_writers() -> Result<()> {
        let mut command = ProcessCommand::new("/bin/sh");
        command.args(["-c", "sleep 1000 & printf '%s\\n' '{\"type\":\"done\"}'"]);
        let mut output = Vec::new();
        let status = run_headless(&mut command, &mut output, b"{}", None)??;
        assert!(status.success());
        assert!(String::from_utf8(output).map_err(storage)?.contains("done"));
        Ok(())
    }
    #[test]
    fn launched_model_joins_native_usage_without_overwriting_reported_models() -> Result<()> {
        let root = tempfile::tempdir().map_err(storage)?;
        let path = root.path().join("events.jsonl");
        let arguments = [
            "--settings",
            "--model",
            "--sandbox",
            "--mode",
            "accept-edits",
            "--model",
            "gemini-2.5-flash",
        ];
        let arguments = arguments.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let model = launch::selected_model("agy", &arguments).map(str::to_owned);
        let mut log = EventLog {
            file: std::fs::File::create(&path).map_err(storage)?,
            written: 0,
            omitted: false,
            model,
        };
        for event in [
            br#"{"event":"result","result":{"usage":{"input_tokens":128,"output_tokens":4}}}"#
                .as_slice(),
            br#"{"model":"reported-model","usage":{"input_tokens":2}}"#.as_slice(),
        ] {
            log.record(event)?;
        }
        drop(log);
        let bytes = std::fs::read_to_string(path).map_err(storage)?;
        let events = bytes
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<serde_json::Result<Vec<_>>>()
            .map_err(storage)?;
        assert_eq!(events[0]["model"], "gemini-2.5-flash");
        assert_eq!(events[0]["result"]["usage"]["input_tokens"], 128);
        assert_eq!(events[1]["model"], "reported-model");
        Ok(())
    }

    fn test_agent() -> AgentPath {
        AgentPath::root(&crate::agent::AgentId::from_bytes(
            *uuid::Uuid::new_v4().as_bytes(),
        ))
    }

    #[test]
    fn process_polls_never_spend_the_original_lifetime_allowance() -> Result<()> {
        let root = tempfile::tempdir().map_err(storage)?;
        let view = root.path().canonicalize().map_err(storage)?;
        let work = polling_work()?;
        let mut process = HostProcessLease::acquire(&view, &test_agent(), &view)?.process(&work)?;
        let mut command = ProcessCommand::new(std::env::current_exe().map_err(storage)?);
        command
            .arg("--list")
            .current_dir(&view)
            .stdout(acyclic_native_runtime::ProcessStdio::piped())
            .stderr(acyclic_native_runtime::ProcessStdio::piped());
        process.spawn(&mut command, OperationId::new(), None)?;
        let before = work.remaining().map_err(storage)?;
        let live = work.live_allocation_bytes();
        let mut progressed = false;
        // Each poll is metered; run past one invocation budget. Pipes at EOF
        // are released, so a reaped child's polls cost one exit observation.
        for _ in 0..262_144 {
            if work.receipt().backend_read_operations > 65_536 {
                break;
            }
            process.read_stdout(&mut [0; 8192])?;
            process.drain_stderr()?;
            process.try_wait()?;
            progressed |= process.take_output_progress();
            assert!(
                !process.take_output_progress(),
                "an output edge was delivered twice"
            );
            assert_eq!(
                work.remaining().map_err(storage)?.backend_read_operations,
                before.backend_read_operations,
                "polling spent the original process allowance"
            );
            assert_eq!(work.live_allocation_bytes(), live);
        }
        assert!(
            work.receipt().backend_read_operations > 65_536,
            "polling must retain cumulative work beyond a single invocation budget"
        );
        assert!(
            progressed,
            "actual output never scheduled another ready turn"
        );
        process.release()?;
        Ok(())
    }

    #[test]
    fn failed_spawn_keeps_physical_custody_until_shared_retirement() -> Result<()> {
        let root = tempfile::tempdir().map_err(storage)?;
        let task = test_agent();
        let view = root.path().canonicalize().map_err(storage)?;
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut process = HostProcessLease::acquire(&view, &task, &view)?.process(&work)?;
        let mut command = ProcessCommand::new(view.join("absent-native-program"));
        let host_view = acyclic_host_platform::paths::strip_verbatim(&view);
        command.current_dir(host_view.as_ref());
        let error = process
            .spawn(&mut command, OperationId::new(), None)
            .expect_err("missing program started");
        assert!(
            matches!(error, Error::Storage(_)),
            "equivalent native cwd was refused before process creation: {error}"
        );
        assert!(HostProcessLease::acquire(&view, &task, &view).is_err());
        process.release()?;
        assert!(
            process
                .spawn(&mut command, OperationId::new(), None)
                .is_err(),
            "retired owner cannot spawn again"
        );
        HostProcessLease::acquire(&view, &task, &view)?
            .quiescent()
            .verify(&task, host_view.as_ref())?;
        Ok(())
    }

    #[test]
    fn native_process_admission_rejects_a_different_view() -> Result<()> {
        let root = tempfile::tempdir().map_err(storage)?;
        let outside = tempfile::tempdir().map_err(storage)?;
        let task = test_agent();
        assert!(HostProcessLease::acquire(root.path(), &task, outside.path()).is_err());
        let view = root.path().canonicalize().map_err(storage)?;
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut process = HostProcessLease::acquire(&view, &task, &view)?.process(&work)?;
        let mut command = ProcessCommand::new("must-not-execute");
        command.current_dir(outside.path());
        let error = process
            .spawn(&mut command, OperationId::new(), None)
            .expect_err("foreign cwd admitted");
        assert!(error.to_string().contains("cwd differs from admitted view"));
        process.release()?;
        let quiet = HostProcessLease::acquire(&view, &task, &view)?.quiescent();
        assert!(quiet.verify(&task, outside.path()).is_err());
        quiet.verify(&task, &view)?;
        Ok(())
    }
}
