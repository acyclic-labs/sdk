//! Native `GraphCoder` presentation over the public Harness runtime and wire API.

#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/fixture.rs"]
mod fixture;
#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/terminal.rs"]
mod terminal;
#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local"))]
#[path = "graphcoder/native.rs"]
mod native;
#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local"))]
#[path = "graphcoder/provider.rs"]
mod provider;

#[cfg(target_arch = "wasm32")]
fn main() -> std::process::ExitCode {
    eprintln!("graphcoder requires a native terminal; use the public Harness WASM APIs");
    std::process::ExitCode::FAILURE
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> std::process::ExitCode {
    let result = tokio::runtime::Builder::new_current_thread().enable_all().build()
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
        .and_then(|runtime| runtime.block_on(run()));
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("graphcoder: {}", terminal::text(&error.to_string()));
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
const HELP: &str = "graphcoder --session CONFIG.json [--headless]\n\
    Actual local Filesystem/Stream task runtime; explicit deterministic model.\n\
    Native session requires terminal,filesystem-local features.\n\
    graphcoder --fixture wire [--headless]  Explicit presentation playback only.\n\
    TEXT        Queue a stock root/selected-parent task, without starting it\n\
    /parent TASK|root  Select an actual retained task as parent, or root\n\
    /queue FILE Queue a typed WorkflowCommand JSON with fresh admission identity\n\
    /run TASK   Drive one finite ordinary worker turn for this exact task\n\
    /resume FILE Reconcile the original WorkLease JSON; never guess a new lease\n\
    /reconcile OPERATION  Observe a lost admission acknowledgement, without resend\n\
    /outcome TASK Observe the durable task result and distinct admission identity\n\
    /children FILE  Page direct children from task/revision/after JSON (max 16)\n\
    /next       Display one bounded activity delivery\n\
    /cursor FILE Select a protobuf Resume ClientFrame for bounded archival paging\n\
    /back N     Show a retained display page (0 newest; 64 pages/256 KiB max)\n\
    /file FILE  Display a granted immutable FileRef JSON (max 64 KiB)\n\
    /diff FILE  Display exact before/after FileRef JSON (max 32 KiB each)\n\
    /request FILE Dispatch one scoped protobuf ClientFrame; no automatic retries\n\
    /input FILE Dispatch a prepared conversation-message command\n\
    /approve FILE Dispatch an exact scoped interaction resolution command\n\
    /cancel FILE Dispatch an exact scoped protobuf Cancel ClientFrame\n\
    /cancel-task TASK  Request cancellation through the actual typed task host\n\
    /reopen     Drop/reopen actual local providers; do not execute queued work\n\
    /reset      Reset fixture display only (unavailable for native sessions)\n\
    /help       Display these commands\n\
    /quit       Exit without claiming pending work completed or cancelled\n\
    Live-model, native-process and optional remote capabilities require explicit adapters.\n";

#[cfg(not(target_arch = "wasm32"))]
enum Selection { Fixture, Native(std::path::PathBuf) }

#[cfg(not(target_arch = "wasm32"))]
fn options() -> acyclic_harness::Result<Option<(Selection, bool)>> {
    use acyclic_harness::Error;
    let mut selection = None;
    let mut headless = false;
    let mut arguments = std::env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--help" | "-h") => { print!("{HELP}"); return Ok(None); }
            Some("--headless") if !headless => headless = true,
            Some("--fixture") if selection.is_none() => {
                if arguments.next().as_deref() != Some(std::ffi::OsStr::new("wire")) {
                    return Err(Error::Invalid("only the explicit wire fixture is available".into()));
                }
                selection = Some(Selection::Fixture);
            }
            Some("--session") if selection.is_none() => {
                let path = arguments.next().ok_or_else(|| Error::Invalid("session config path missing".into()))?;
                selection = Some(Selection::Native(path.into()));
            }
            _ => return Err(Error::Invalid("unknown or duplicate terminal option".into())),
        }
    }
    let selection = selection.ok_or_else(|| Error::Unsupported(
        "durable terminal host is unavailable without explicit --session CONFIG; use --fixture wire for UI playback only".into()))?;
    Ok(Some((selection, headless)))
}

#[cfg(not(target_arch = "wasm32"))]
enum Backend {
    Fixture(fixture::WireFixture),
    #[cfg(feature = "filesystem-local")]
    Native(std::sync::Arc<native::NativeSession>),
}

#[cfg(not(target_arch = "wasm32"))]
impl Backend {
    fn api(&self) -> &dyn acyclic_harness::wire_api::HarnessWireApi {
        match self {
            Self::Fixture(api) => api,
            #[cfg(feature = "filesystem-local")]
            Self::Native(api) => api.as_ref(),
        }
    }

    fn cursor(&self) -> acyclic_harness::wire::ReplayCursor {
        match self {
            Self::Fixture(api) => api.start_cursor(),
            #[cfg(feature = "filesystem-local")]
            Self::Native(api) => api.start_cursor(),
        }
    }

    #[cfg(feature = "filesystem-local")]
    fn native(&self) -> acyclic_harness::Result<&std::sync::Arc<native::NativeSession>> {
        match self {
            Self::Native(api) => Ok(api),
            Self::Fixture(_) => Err(acyclic_harness::Error::Unsupported(
                "operation requires a durable product host; fixture input is not queued".into())),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn input_lines() -> acyclic_harness::Result<tokio::sync::mpsc::Receiver<acyclic_harness::Result<String>>> {
    use std::io::{BufRead as _, Read as _};
    let (send, receive) = tokio::sync::mpsc::channel(1);
    std::thread::Builder::new().name("graphcoder-input".into()).spawn(move || {
        let stdin = std::io::stdin();
        let mut input = stdin.lock();
        loop {
            let mut line = String::new();
            let result = match (&mut input).take(terminal::MAX_FRAME_BYTES as u64 + 1).read_line(&mut line) {
                Ok(0) => break,
                Ok(_) if line.len() <= terminal::MAX_FRAME_BYTES => Ok(line),
                Ok(_) => Err(acyclic_harness::Error::Invalid("terminal input exceeds bounds".into())),
                Err(error) => Err(acyclic_harness::Error::Storage(error.to_string())),
            };
            let failed = result.is_err();
            if send.blocking_send(result).is_err() || failed { break; }
        }
    }).map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    Ok(receive)
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::too_many_lines, reason = "one bounded input and worker-result presentation loop")]
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness::{Error, wire, wire_api::current_protocol};
    use std::io::{IsTerminal as _, Write as _};
    let Some((selection, headless)) = options()? else { return Ok(()); };
    let backend = match selection {
        Selection::Fixture => Backend::Fixture(fixture::WireFixture::load()?),
        Selection::Native(path) => {
            #[cfg(feature = "filesystem-local")]
            { Backend::Native(std::sync::Arc::new(native::NativeSession::open(read_json(&path)?).await?)) }
            #[cfg(not(feature = "filesystem-local"))]
            { let _ = path; return Err(Error::Unsupported("native session requires filesystem-local feature".into()).into()); }
        }
    };
    let mut backend = Some(backend);
    let handshake = wire::client_frame::Frame::Handshake(wire::HandshakeRequest {
        protocol: Some(current_protocol()), required: None,
    });
    let api = backend.as_ref().ok_or_else(|| Error::Conflict("session missing".into()))?;
    terminal::dispatch(api.api(), handshake).await?;
    let mut cursor = api.cursor();
    let fixture_mode = matches!(api, Backend::Fixture(_));
    let stdin = std::io::stdin();
    let interactive = !headless && stdin.is_terminal();
    let mut input = input_lines()?;
    let mut output = std::io::stdout().lock();
    let mut scrollback = terminal::Scrollback::default();
    #[cfg(feature = "filesystem-local")]
    let mut parent = None;
    #[cfg(feature = "filesystem-local")]
    let mut workers = tokio::task::JoinSet::<acyclic_harness::Result<String>>::new();
    writeln!(output, "{}", if fixture_mode {
        "GraphCoder — fixture wire; UI playback only, no durable effects"
    } else {
        "GraphCoder — actual local Harness runtime; deterministic request-digest provider; live-model/native-process adapters unavailable"
    })?;
    loop {
        if interactive { write!(output, "{}> ", if fixture_mode { "fixture" } else { "graphcoder" })?; }
        output.flush()?;
        #[cfg(feature = "filesystem-local")]
        let line = loop {
            tokio::select! {
                line = input.recv() => break line,
                outcome = workers.join_next(), if !workers.is_empty() => {
                    let rendered = outcome.ok_or_else(|| Error::Conflict("worker handle missing".into()))?
                        .map_err(|error| Error::Storage(format!("frontend worker ended without a receipt; retain its original lease: {error}")))??;
                    scrollback.push(rendered)?;
                    if let Some(page) = scrollback.page(0) { write!(output, "{page}")?; }
                    if interactive { write!(output, "graphcoder> ")?; }
                    output.flush()?;
                }
            }
        };
        #[cfg(not(feature = "filesystem-local"))]
        let line = input.recv().await;
        let Some(line) = line else {
            #[cfg(feature = "filesystem-local")]
            if !workers.is_empty() {
                writeln!(output, "input closed with active leases; outcomes remain uncertain until authoritative reconciliation")?;
            }
            break;
        };
        let line = line?;
        let command = line.trim_end_matches(['\r', '\n']);
        if command == "/quit" {
            #[cfg(feature = "filesystem-local")]
            if !workers.is_empty() {
                writeln!(output, "closing presentation with active leases; outcomes remain uncertain until authoritative reconciliation")?;
            }
            break;
        }
        if command == "/help" { write!(output, "{HELP}")?; continue; }
        if command.is_empty() { continue; }
        if let Some(back) = command.strip_prefix("/back ") {
            let result = back.parse::<usize>().ok().and_then(|back| {
                if back < terminal::MAX_DISPLAY_PAGES { scrollback.page(back) } else { None }
            });
            if let Some(page) = result { write!(output, "{page}")?; }
            else { writeln!(output, "display page unavailable; request an explicit archival page")?; }
            continue;
        }
        let result = execute(command, &mut backend, &mut cursor,
            #[cfg(feature = "filesystem-local")] &mut parent,
            #[cfg(feature = "filesystem-local")] &mut workers,
            &mut output).await;
        match result {
            Ok(rendered) => {
                scrollback.push(rendered)?;
                if let Some(page) = scrollback.page(0) { write!(output, "{page}")?; }
                output.flush()?;
            }
            Err(error) if interactive => writeln!(output, "{}", terminal::text(&error.to_string()))?,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::too_many_lines, reason = "one finite terminal command table delegates all semantics to public owners")]
async fn execute(command: &str, backend: &mut Option<Backend>, cursor: &mut acyclic_harness::wire::ReplayCursor,
    #[cfg(feature = "filesystem-local")] parent: &mut Option<acyclic_harness::TaskId>,
    #[cfg(feature = "filesystem-local")] workers: &mut tokio::task::JoinSet<acyclic_harness::Result<String>>,
    output: &mut impl std::io::Write) -> acyclic_harness::Result<String>
{
    use acyclic_harness::{Error, wire, wire_api::current_protocol};
    let api = backend.as_ref().ok_or_else(|| Error::Conflict("session is closed after failed reopen; restart with the explicit config".into()))?;
    #[cfg(not(feature = "filesystem-local"))]
    let _ = output;
    if command == "/reset" {
        match api {
            Backend::Fixture(fixture) => *cursor = fixture.start_cursor(),
            #[cfg(feature = "filesystem-local")]
            Backend::Native(_) => return Err(Error::Unsupported("native sessions never reset authoritative state".into())),
        }
        return Ok("fixture display reset; no session restored\n".into());
    }
    #[cfg(feature = "filesystem-local")]
    {
        if command == "/reopen" {
            if !workers.is_empty() {
                return Err(Error::Conflict("active worker handles must settle or the process must close before native reopen; no implicit cancellation".into()));
            }
            let config = api.native()?.config();
            drop(backend.take());
            *backend = Some(Backend::Native(std::sync::Arc::new(native::NativeSession::open(config).await?)));
            return Ok("native providers reopened; no admission or effect resent\n".into());
        }
        if let Some(task) = command.strip_prefix("/parent ") {
            let native = api.native()?;
            *parent = if task == "root" { None } else {
                let task = acyclic_harness::TaskId::parse(task)?;
                native.outcome(task).await?;
                Some(task)
            };
            return Ok(format!("selected parent-task={parent:?}\n"));
        }
        if let Some(task) = command.strip_prefix("/run ") {
            if workers.len() >= terminal::MAX_PAGE_EVENTS {
                return Err(Error::Invalid("active frontend worker bound reached".into()));
            }
            let task = acyclic_harness::TaskId::parse(task)?;
            let worker = acyclic_harness::distributed::Worker {
                id: format!("graphcoder-{}", uuid::Uuid::new_v4()),
                available: Default::default(), labels: Default::default(),
            };
            let native = std::sync::Arc::clone(api.native()?);
            match native.claim(task, &worker).await? {
                acyclic_harness::distributed::WorkPull::Idle => return Ok(format!("worker idle task={task}\n")),
                acyclic_harness::distributed::WorkPull::Unresolved { lease, error } => {
                    return native::render_attempt(acyclic_harness::filesystem::TaskWorkerAttempt::Unresolved { lease, error });
                }
                acyclic_harness::distributed::WorkPull::Claimed(lease) => {
                    writeln!(output, "worker-claim task={task} lease-json-hex={}", native::lease_hex(&lease)?)
                        .and_then(|()| output.flush()).map_err(|error| Error::Storage(error.to_string()))?;
                    let _ = workers.spawn(async move { native.resume(lease).await });
                    return Ok(format!("worker started task={task}; input and cancellation remain available\n"));
                }
            }
        }
        if let Some(task) = command.strip_prefix("/cancel-task ") {
            return api.native()?.cancel_task(acyclic_harness::TaskId::parse(task)?).await;
        }
        if let Some(task) = command.strip_prefix("/outcome ") {
            return api.native()?.outcome(acyclic_harness::TaskId::parse(task)?).await;
        }
        if let Some(operation) = command.strip_prefix("/reconcile ") {
            return api.native()?.reconcile(acyclic_harness::OperationId::parse(operation)?).await;
        }
        if let Some(path) = command.strip_prefix("/resume ") {
            if workers.len() >= terminal::MAX_PAGE_EVENTS {
                return Err(Error::Invalid("active frontend worker bound reached".into()));
            }
            let lease = read_json(std::path::Path::new(path))?;
            let native = std::sync::Arc::clone(api.native()?);
            let _ = workers.spawn(async move { native.resume(lease).await });
            return Ok("original lease submitted for authoritative reconciliation; input remains available\n".into());
        }
        if let Some(path) = command.strip_prefix("/children ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Children { task: acyclic_harness::TaskId, revision: Option<u64>, after: Option<String> }
            let request: Children = read_json(std::path::Path::new(path))?;
            return api.native()?.children(request.task, request.revision, request.after).await;
        }
        if let Some(path) = command.strip_prefix("/file ") {
            let file = read_json(std::path::Path::new(path))?;
            let bytes = api.native()?.read(&file, terminal::MAX_FRAME_BYTES as u64).await?;
            let text = std::str::from_utf8(&bytes).map_err(|_| Error::Unsupported("file is not UTF-8 text".into()))?;
            return Ok(format!("file {}\n{}\n", terminal::text(&serde_json::to_string(&file)
                .map_err(|error| Error::Invalid(error.to_string()))?), terminal::text(text)));
        }
        if let Some(path) = command.strip_prefix("/diff ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Diff { before: acyclic_harness::conversation::FileRef, after: acyclic_harness::conversation::FileRef }
            let diff: Diff = read_json(std::path::Path::new(path))?;
            let native = api.native()?;
            let before = native.read(&diff.before, (terminal::MAX_FRAME_BYTES / 2) as u64).await?;
            let after = native.read(&diff.after, (terminal::MAX_FRAME_BYTES / 2) as u64).await?;
            let before = std::str::from_utf8(&before).map_err(|_| Error::Unsupported("before diff is not UTF-8".into()))?;
            let after = std::str::from_utf8(&after).map_err(|_| Error::Unsupported("after diff is not UTF-8".into()))?;
            return Ok(format!("diff exact before={} after={}\n--- before\n{}\n+++ after\n{}\n",
                terminal::text(diff.before.path()), terminal::text(diff.after.path()), terminal::text(before), terminal::text(after)));
        }
        if !command.starts_with('/') || command.starts_with("/queue ") {
            let native = api.native()?;
            let operation = acyclic_harness::OperationId::from_bytes(*uuid::Uuid::new_v4().as_bytes());
            let command = if let Some(path) = command.strip_prefix("/queue ") {
                read_json(std::path::Path::new(path))?
            } else {
                let command_operation = acyclic_harness::OperationId::from_bytes(*uuid::Uuid::new_v4().as_bytes());
                native::NativeSession::prompt_command(command, command_operation)?
            };
            // Publish the caller-held identity before attempting admission. On
            // lost acknowledgement, /reconcile observes it without resending.
            writeln!(output, "admission-attempt operation={operation} command-operation={}", command.operation_id)
                .and_then(|()| output.flush()).map_err(|error| Error::Storage(error.to_string()))?;
            return match native.queue(operation, command, *parent).await? {
                acyclic_harness::Admission::Accepted(value) => Ok(format!("queued task={} admission-operation={operation}\n", value.identity())),
                acyclic_harness::Admission::Rejected { reason } => Err(Error::Conflict(format!("admission rejected operation={operation}: {reason}"))),
                acyclic_harness::Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
            };
        }
    }
    let request = if command == "/next" {
        wire::client_frame::Frame::Resume(wire::ResumeRequest {
            protocol: Some(current_protocol()), cursors: vec![cursor.clone()],
        })
    } else if let Some(path) = command.strip_prefix("/cursor ") {
        let wire::client_frame::Frame::Resume(request) = read_request(path)? else {
            return Err(Error::Invalid("cursor file must contain a Resume frame".into()));
        };
        acyclic_harness::wire_api::validate_resume_protocol(&request)?;
        if request.cursors.len() != 1 { return Err(Error::Invalid("select exactly one aggregate cursor".into())); }
        wire::client_frame::Frame::Resume(request)
    } else if let Some(path) = command.strip_prefix("/cancel ") {
        let request = read_request(path)?;
        if !matches!(request, wire::client_frame::Frame::Cancel(_)) {
            return Err(Error::Invalid("cancel file must contain a Cancel frame".into()));
        }
        request
    } else if let Some(path) = command.strip_prefix("/input ").or_else(|| command.strip_prefix("/approve ")) {
        let request = read_request(path)?;
        let wire::client_frame::Frame::Command(envelope) = &request else {
            return Err(Error::Invalid("input/approval file must contain a scoped Command frame".into()));
        };
        let expected = if command.starts_with("/input ") { "append_conversation_message" } else { "resolve_interaction" };
        if envelope.action_type != expected {
            return Err(Error::Invalid("prepared command has the wrong input/approval action".into()));
        }
        request
    } else if let Some(path) = command.strip_prefix("/request ") { read_request(path)? }
    else { return Err(Error::Unsupported("operation requires a durable product host; fixture input is not queued".into())); };
    let requested_cursor = match &request {
        wire::client_frame::Frame::Resume(request) => request.cursors.first().cloned(),
        _ => None,
    };
    let Some(frame) = terminal::dispatch(api.api(), request).await? else {
        if let Some(selected) = requested_cursor { *cursor = selected; }
        return Ok(if matches!(api, Backend::Fixture(_)) { "end of fixture activity\n" } else { "end of current activity page\n" }.into());
    };
    let response = wire::ServerFrame { frame: Some(frame) };
    let rendered = terminal::render(&response)?;
    if let Some(wire::server_frame::Frame::Delivery(delivery)) = response.frame {
        *cursor = wire::ReplayCursor { authority: delivery.authority, generation: delivery.generation, revision: delivery.through_revision };
    }
    Ok(rendered)
}

#[cfg(not(target_arch = "wasm32"))]
fn read_bytes(path: &std::path::Path) -> acyclic_harness::Result<Vec<u8>> {
    use std::io::Read as _;
    let mut bytes = Vec::new();
    std::fs::File::open(path).and_then(|file| file.take(terminal::MAX_FRAME_BYTES as u64 + 1).read_to_end(&mut bytes))
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    if bytes.len() > terminal::MAX_FRAME_BYTES { return Err(acyclic_harness::Error::Invalid("request exceeds terminal bounds".into())); }
    Ok(bytes)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local"))]
fn read_json<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> acyclic_harness::Result<T> {
    serde_json::from_slice(&read_bytes(path)?).map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))
}

#[cfg(not(target_arch = "wasm32"))]
fn read_request(path: &str) -> acyclic_harness::Result<acyclic_harness::wire::client_frame::Frame> {
    use prost::Message as _;
    acyclic_harness::wire::ClientFrame::decode(read_bytes(std::path::Path::new(path))?.as_slice())
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?.frame
        .ok_or_else(|| acyclic_harness::Error::Invalid("request frame is missing".into()))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_harness::Error;
    use std::io::Write as _;

    #[test]
    fn malformed_and_oversized_requests_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut request = tempfile::NamedTempFile::new()?;
        request.write_all(&[0xff])?;
        let path = request.path().to_str().ok_or("temporary path is not UTF-8")?;
        assert!(matches!(read_request(path), Err(Error::Invalid(_))));
        request.as_file().set_len(terminal::MAX_FRAME_BYTES as u64 + 1)?;
        assert!(matches!(read_request(path), Err(Error::Invalid(_))));
        Ok(())
    }
}
