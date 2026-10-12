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
#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local", feature = "native-execution"))]
#[path = "graphcoder/native_scenario.rs"]
mod native_scenario;
#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local"))]
#[path = "graphcoder/native_tools.rs"]
mod native_tools;
#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local"))]
#[path = "graphcoder/native_writeback.rs"]
mod native_writeback;

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
    Native session requires terminal,filesystem-local; full fixture/writeback adds native-execution.\n\
    graphcoder --prepare-scenario ABSOLUTE_DIRECTORY  Create genuine local qualification providers\n\
    graphcoder --fixture wire [--headless]  Explicit presentation playback only.\n\
    TEXT        Queue a stock root/selected-parent task, without starting it\n\
    /parent TASK|root  Select an actual retained task as parent, or root\n\
    /queue FILE Queue a typed WorkflowCommand JSON with fresh admission identity\n\
    /command FILE Run/reconcile an explicitly configured approved native effect\n\
    /context FILE Capture discovery and replace typed attributes for later requests\n\
    /fork-selection FILE Select full/fresh/summary from an authenticated original cut\n\
    /prepare FILE Admit exact typed fork ResourcePreparationRequest JSON under its caller\n\
    /publish-fork FILE Publish actual report/admit exact child: caller,lease,preparation\n\
    /inspect FILE Inspect authorized source allocations: caller,lease,source\n\
    /history FILE Page owned history: caller,lease,cursor (16 events/64 KiB)\n\
    /inherited-history FILE Page verified inherited slice: caller,lease,seed,after_sequence\n\
    /checkpoint FILE Observe real machine checkpoint: caller,lease\n\
    /run TASK   Drive one finite ordinary worker turn for this exact task\n\
    /claim TASK Retain/start an actual original owner lease without model dispatch\n\
    /resume FILE Reconcile the original WorkLease JSON; never guess a new lease\n\
    /reconcile OPERATION  Observe a lost admission acknowledgement, without resend\n\
    /outcome TASK Observe the durable task result and distinct admission identity\n\
    /children FILE  Page direct children from task/revision/after JSON (max 16)\n\
    /mail FILE  Send one scoped pinned file under an actual caller lease\n\
    /inbox FILE Observe one finite inbox page; never claim model consumption\n\
    /queue-mailbox FILE Queue ordinary receive/checkpoint/model workflow\n\
    /write FILE Stage bounded content in an explicit caller-owned volume\n\
    /read FILE  Read exact pinned content: caller,lease,file (max 64 KiB)\n\
    /read-path FILE Read granted head/explicit cut: caller,lease,volume,path,generation? (max 64 KiB)\n\
    /publication-open FILE Prepare original SDK plan: operation_id,caller_lease\n\
    /publication-observe FILE Observe retained publication/approval journal\n\
    /publication-apply FILE Publish only the actual approved original plan\n\
    /publication-reconcile FILE Reconcile the original publication, never replan\n\
    /next       Display one bounded activity delivery\n\
    /cursor FILE Select a protobuf Resume ClientFrame for bounded archival paging\n\
    /back N     Show a retained display page (0 newest; 64 pages/256 KiB max)\n\
    /file FILE  Display scoped immutable content: caller,lease,file (max 64 KiB)\n\
    /diff FILE  Display scoped exact refs: caller,lease,before,after (max 32 KiB each)\n\
    /request FILE Dispatch one scoped protobuf ClientFrame; no automatic retries\n\
    /input FILE Dispatch a prepared conversation-message command\n\
    /approve FILE Dispatch an exact scoped interaction resolution command\n\
    /cancel FILE Dispatch an exact scoped protobuf Cancel ClientFrame\n\
    /cancel-task TASK  Request cancellation through the actual typed task host\n\
    /reopen     Drop/reopen actual local providers; do not execute queued work\n\
    /reset      Reset fixture display only (unavailable for native sessions)\n\
    /help       Display these commands\n\
    /quit       Exit without claiming pending work completed or cancelled\n\
    Live models and optional remote capabilities require explicit adapters.\n";

#[cfg(not(target_arch = "wasm32"))]
enum Selection { Fixture, Native(std::path::PathBuf), Scenario(std::path::PathBuf) }

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
            Some("--prepare-scenario") if selection.is_none() => {
                let path = arguments.next().ok_or_else(|| Error::Invalid("scenario directory missing".into()))?;
                selection = Some(Selection::Scenario(path.into()));
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
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness::{Error, wire, wire_api::current_protocol};
    use std::io::{IsTerminal as _, Write as _};
    let Some((selection, headless)) = options()? else { return Ok(()); };
    let backend = match selection {
        Selection::Fixture => Backend::Fixture(fixture::WireFixture::load()?),
        Selection::Native(path) => {
            #[cfg(feature = "filesystem-local")]
            { Backend::Native(std::sync::Arc::new(native::NativeSession::open(native::Config::from_file(&path)?).await?)) }
            #[cfg(not(feature = "filesystem-local"))]
            { let _ = path; return Err(Error::Unsupported("native session requires filesystem-local feature".into()).into()); }
        }
        Selection::Scenario(path) => {
            #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
            {
                let cancellation = acyclic_fs::CancellationToken::new();
                let preparation = native_scenario::prepare(&path, &cancellation);
                tokio::pin!(preparation);
                let manifest = tokio::select! {
                    result = &mut preparation => result?,
                    signal = tokio::signal::ctrl_c() => {
                        signal?;
                        cancellation.cancel();
                        preparation.await?
                    }
                };
                println!("scenario-json={}", serde_json::json!({"manifest": manifest}));
                return Ok(());
            }
            #[cfg(not(all(feature = "filesystem-local", feature = "native-execution")))]
            { let _ = path; return Err(Error::Unsupported("native scenario requires filesystem-local,native-execution features".into()).into()); }
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
    let stdin_is_terminal = stdin.is_terminal();
    let interactive = !headless && stdin_is_terminal;
    let mut input = input_lines()?;
    let mut output = std::io::stdout().lock();
    let mut scrollback = terminal::Scrollback::default();
    #[cfg(feature = "filesystem-local")]
    let mut parent = None;
    #[cfg(feature = "filesystem-local")]
    let mut workers = tokio::task::JoinSet::<acyclic_harness::Result<String>>::new();
    writeln!(output, "terminal-mode stdin-is-terminal={stdin_is_terminal} interactive={interactive} headless={headless} process-id={}",
        std::process::id())?;
    writeln!(output, "{}", if fixture_mode {
        "GraphCoder — fixture wire; UI playback only, no durable effects"
    } else {
        "GraphCoder — actual local Harness runtime; deterministic request-digest provider; native commands require explicit approved bindings"
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
        #[cfg(feature = "native-execution")]
        if let Some(path) = command.strip_prefix("/command ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct NativeCommand {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                operation: acyclic_harness::OperationId,
                decision: Option<bool>,
            }
            let request: NativeCommand = read_json(std::path::Path::new(path))?;
            return api.native()?.command(request.caller, &request.lease, request.operation, request.decision).await;
        }
        if let Some(path) = command.strip_prefix("/context ") {
            return api.native()?.configure_context(read_json(std::path::Path::new(path))?).await;
        }
        if let Some(path) = command.strip_prefix("/fork-selection ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Selection {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                operation: acyclic_harness::OperationId,
                history: acyclic_harness::fork::ForkHistoryPolicy,
            }
            let request: Selection = read_json(std::path::Path::new(path))?;
            return api.native()?.select_fork(request.caller, &request.lease, request.operation, request.history).await;
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
        if let Some(task) = command.strip_prefix("/run ").or_else(|| command.strip_prefix("/claim ")) {
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
                    if command.starts_with("/claim ") {
                        native.runtime.task_host().start_task(&lease).await?;
                        return Ok(format!("worker retained task={task}; no command dispatched\n"));
                    }
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
        if let Some(path) = command.strip_prefix("/prepare ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Preparation {
                operation: acyclic_harness::OperationId,
                parent: acyclic_harness::TaskId,
                caller_lease: acyclic_harness::distributed::WorkLease,
                request: acyclic_harness::resource_preparation::ResourcePreparationRequest,
            }
            let request: Preparation = read_json(std::path::Path::new(path))?;
            let fork: acyclic_harness::fork::ForkRequest =
                serde_json::from_value(request.request.payload.clone())
                    .map_err(|error| Error::Invalid(format!("invalid exact fork request: {error}")))?;
            fork.validate()?;
            writeln!(output, "preparation-attempt operation={} caller-task={} new-child-agent={} child-admission-operation={} fork-operation={}",
                request.operation, request.parent, fork.child_agent,
                request.request.child.operation_id, fork.operation_id)
                .and_then(|()| output.flush()).map_err(|error| Error::Storage(error.to_string()))?;
            return match api.native()?.queue_preparation(request.operation, request.parent, &request.caller_lease, request.request).await? {
                acyclic_harness::Admission::Accepted(value) => Ok(format!(
                    "queued preparation-task={} admission-operation={} caller-task={} new-child-agent={}\n",
                    value.identity(), request.operation, request.parent, fork.child_agent)),
                acyclic_harness::Admission::Rejected { reason } => Err(Error::Conflict(format!(
                    "preparation rejected operation={}: {reason}", request.operation))),
                acyclic_harness::Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
            };
        }
        if let Some(path) = command.strip_prefix("/publish-fork ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Publish {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                preparation: acyclic_harness::TaskId,
            }
            let request: Publish = read_json(std::path::Path::new(path))?;
            return api.native()?.publish_preparation(request.caller, &request.lease, request.preparation).await;
        }
        if let Some(path) = command.strip_prefix("/inspect ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Inspect {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                source: acyclic_harness::TaskId,
            }
            let request: Inspect = read_json(std::path::Path::new(path))?;
            let native = api.native()?;
            native.caller_context(request.caller, &request.lease).await?;
            let source = native.runtime.inspect_source(request.caller, request.source).await?;
            return Ok(format!("inspection-json={}\n", serde_json::json!({
                "caller": request.caller, "source_task": request.source,
                "agent": source.agent()?, "source": source.source(),
                "volumes": source.volumes(), "project": source.project()?,
                "private_volume": source.private_volume()?, "generation_semantics": "original_allocation",
            })));
        }
        if let Some(path) = command.strip_prefix("/history ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct History {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                cursor: Option<acyclic_harness::store::HistoryCursor>,
            }
            let request: History = read_json(std::path::Path::new(path))?;
            let native = api.native()?;
            native.caller_context(request.caller, &request.lease).await?;
            let binding = native.caller_binding(request.caller).await?;
            let verifier = acyclic_harness::core::AuthorityIssuer::new(
                &binding.issuer, binding.key, binding.authority.clone()).verifier(&mut None).expect("verifier copy");
            let reader = acyclic_harness::store::HistoryReader::new(
                &native.stream, &binding.authority, verifier)?;
            let cursor = match request.cursor {
                Some(cursor) if cursor.authority == binding.authority => cursor,
                Some(_) => return Err(Error::Unauthorized("history cursor belongs to a different caller conversation".into())),
                None => reader.pin(0).await?,
            };
            let page = reader.read_page(&cursor, acyclic_harness::store::HistoryReadLimits {
                maximum_events: terminal::MAX_PAGE_EVENTS as u32,
                maximum_bytes: terminal::MAX_FRAME_BYTES as u64,
            }).await?;
            return Ok(format!("history-json={}\n", serde_json::to_string(&page)
                .map_err(|error| Error::Invalid(error.to_string()))?));
        }
        if let Some(path) = command.strip_prefix("/inherited-history ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Inherited {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                seed: acyclic_harness::fork::ForkSeed,
                after_sequence: u64,
            }
            let request: Inherited = read_json(std::path::Path::new(path))?;
            let native = api.native()?;
            native.caller_context(request.caller, &request.lease).await?;
            let child = native.runtime.inspect_source(request.caller, request.caller).await?;
            let child_binding = native.caller_binding(request.caller).await?;
            if child.agent()? != request.seed.child_agent || child_binding.authority != request.seed.child {
                return Err(Error::Unauthorized("inherited history does not belong to this admitted caller".into()));
            }
            let parent = native.config.observed_authorities.iter()
                .find(|binding| binding.authority == request.seed.parent)
                .or_else(|| if native.config.owner.authority == request.seed.parent { Some(&native.config.owner) } else { None })
                .ok_or_else(|| Error::Unsupported("original parent history verifier unavailable".into()))?;
            let verifier = acyclic_harness::core::AuthorityIssuer::new(
                &parent.issuer, parent.key, parent.authority.clone()).verifier(&mut None).expect("verifier copy");
            let reader = acyclic_harness::store::HistoryReader::new(&native.stream, &parent.authority, verifier)?;
            let messages = reader.inherited_conversation_page(
                &request.seed, child.agent()?, request.after_sequence,
                acyclic_harness::store::HistoryReadLimits {
                    maximum_events: terminal::MAX_PAGE_EVENTS as u32,
                    maximum_bytes: terminal::MAX_FRAME_BYTES as u64,
                },
            ).await?;
            return Ok(format!("inherited-history-json={}\n", serde_json::json!({
                "next_after": messages.last().map(|message| message.sequence), "messages": messages,
            })));
        }
        if let Some(path) = command.strip_prefix("/checkpoint ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Checkpoint {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
            }
            let request: Checkpoint = read_json(std::path::Path::new(path))?;
            let native = api.native()?;
            native.caller_context(request.caller, &request.lease).await?;
            let session = native.runtime.open_task(request.caller,
                acyclic_harness::scheduler::LeaseFence::from(&request.lease.reservation)).await?;
            return Ok(format!("checkpoint-json={}\n", serde_json::json!({
                "task": request.caller, "checkpoint": session.checkpoint(),
                "transition": session.latest_transition().await?,
            })));
        }
        if let Some(path) = command.strip_prefix("/read-path ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct ReadPath {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                volume: acyclic_harness::conversation::VolumeRef,
                path: String,
                #[serde(default)]
                generation: Option<acyclic_harness::resources::GenerationRef>,
            }
            let request: ReadPath = read_json(std::path::Path::new(path))?;
            let native = api.native()?;
            let context = native.caller_context(request.caller, &request.lease).await?;
            let full = request.volume.capability(acyclic_harness::conversation::VolumeOperation::Read, &mut None)?;
            let directory = request.volume.directory_read_capability("", &mut None)?;
            if !context.scope().grants().contains(&full) && !context.scope().grants().contains(&directory) {
                return Err(Error::Unauthorized("caller cannot discover this current volume".into()));
            }
            let binding = native.caller_binding(request.caller).await?;
            let verifier = acyclic_harness::core::AuthorityIssuer::new(
                &binding.issuer, binding.key, binding.authority.clone()).verifier(&mut None).expect("verifier copy");
            let grant = acyclic_harness::conversation::ContentGrant::verify_directory_read(
                &verifier, &binding.scope, &request.volume, "", &mut None)?;
            let (file, bytes) = native.files.read_private_path(
                &request.volume, &grant, &request.path, request.generation.as_ref(),
                terminal::MAX_FRAME_BYTES as u64,
            ).await?;
            if bytes.len() > terminal::MAX_FRAME_BYTES {
                return Err(Error::Invalid("current file display exceeds terminal bounds".into()));
            }
            let text = std::str::from_utf8(&bytes).map_err(|_| Error::Unsupported("file is not UTF-8 text".into()))?;
            return Ok(format!("file-json={}\n", serde_json::json!({"file": file, "text": text})));
        }
        if let Some(path) = command.strip_prefix("/publication-open ")
            .or_else(|| command.strip_prefix("/publication-observe "))
            .or_else(|| command.strip_prefix("/publication-apply "))
            .or_else(|| command.strip_prefix("/publication-reconcile "))
        {
            #[cfg(feature = "native-execution")]
            {
                let request: native_writeback::NativePublicationRequest = read_json(std::path::Path::new(path))?;
                let native = api.native()?;
                let result = if command.starts_with("/publication-open ") { native.publication_open(&request).await? }
                    else if command.starts_with("/publication-observe ") { native.publication_observe(&request).await? }
                    else if command.starts_with("/publication-apply ") { native.publication_apply(&request).await? }
                    else { native.publication_reconcile(&request).await? };
                return Ok(format!("publication-json={}\n", serde_json::to_string(&result)
                    .map_err(|error| Error::Invalid(error.to_string()))?));
            }
            #[cfg(not(feature = "native-execution"))]
            { let _ = path; return Err(Error::Unsupported("native root publication requires native-execution".into())); }
        }
        if let Some(path) = command.strip_prefix("/mail ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Mail {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                recipient: acyclic_harness::TaskId,
                operation: acyclic_harness::OperationId,
                payload: acyclic_harness::conversation::FileRef,
            }
            let request: Mail = read_json(std::path::Path::new(path))?;
            let receipt = api.native()?.caller_context(request.caller, &request.lease).await?
                .send(request.recipient, request.operation, request.payload).await?;
            return Ok(format!("mail-json={}\n", serde_json::to_string(&receipt)
                .map_err(|error| Error::Invalid(error.to_string()))?));
        }
        if let Some(path) = command.strip_prefix("/inbox ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Inbox {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                after: u64,
            }
            let request: Inbox = read_json(std::path::Path::new(path))?;
            let items = api.native()?.caller_context(request.caller, &request.lease).await?
                .inbox(request.after, terminal::MAX_PAGE_EVENTS).await?;
            return Ok(format!("inbox-json={}\n", serde_json::json!({
                "task": request.caller, "after": request.after,
                "next_after": items.last().map(|item| item.sequence), "items": items,
                "consumption": "observation_only",
            })));
        }
        if let Some(path) = command.strip_prefix("/queue-mailbox ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Mailbox {
                operation: acyclic_harness::OperationId,
                parent: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                input: serde_json::Value,
            }
            let request: Mailbox = read_json(std::path::Path::new(path))?;
            writeln!(output, "mailbox-admission-attempt operation={} caller-task={}", request.operation, request.parent)
                .and_then(|()| output.flush()).map_err(|error| Error::Storage(error.to_string()))?;
            return match api.native()?.queue_mailbox(
                request.operation, request.parent, &request.lease, request.input,
            ).await? {
                acyclic_harness::Admission::Accepted(value) => Ok(format!(
                    "queued task={} admission-operation={}\n", value.identity(), request.operation)),
                acyclic_harness::Admission::Rejected { reason } => Err(Error::Conflict(format!(
                    "mailbox admission rejected operation={}: {reason}", request.operation))),
                acyclic_harness::Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
            };
        }
        if let Some(path) = command.strip_prefix("/write ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Write {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                volume: acyclic_harness::conversation::VolumeRef,
                operation: acyclic_harness::OperationId,
                path: String,
                bytes: String,
            }
            let request: Write = read_json(std::path::Path::new(path))?;
            if request.bytes.len() > terminal::MAX_FRAME_BYTES / 2 {
                return Err(Error::Invalid("file staging exceeds terminal bounds".into()));
            }
            let file = api.native()?.caller_context(request.caller, &request.lease).await?
                .stage_file_in(&request.volume, request.operation, &request.path,
                    request.bytes.as_bytes(), "text/plain", &request.path).await?;
            return Ok(format!("file-json={}\n", serde_json::json!({"file": file})));
        }
        if let Some(path) = command.strip_prefix("/read ").or_else(|| command.strip_prefix("/file ")) {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Read {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                file: acyclic_harness::conversation::FileRef,
            }
            let request: Read = read_json(std::path::Path::new(path))?;
            if request.file.descriptor().byte_length() > terminal::MAX_FRAME_BYTES as u64 {
                return Err(Error::Invalid("file display exceeds terminal bounds".into()));
            }
            let bytes = api.native()?.caller_context(request.caller, &request.lease).await?
                .read_file(&request.file).await?;
            let text = std::str::from_utf8(&bytes).map_err(|_| Error::Unsupported("file is not UTF-8 text".into()))?;
            if command.starts_with("/file ") {
                return Ok(format!("file {}\n{}\nfile-json={}\n",
                    terminal::text(request.file.path()), terminal::text(text),
                    serde_json::json!({"file": request.file, "text": text})));
            }
            return Ok(format!("file-json={}\n", serde_json::json!({"file": request.file, "text": text})));
        }
        if let Some(path) = command.strip_prefix("/diff ") {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Diff {
                caller: acyclic_harness::TaskId,
                lease: acyclic_harness::distributed::WorkLease,
                before: acyclic_harness::conversation::FileRef,
                after: acyclic_harness::conversation::FileRef,
            }
            let diff: Diff = read_json(std::path::Path::new(path))?;
            if diff.before.descriptor().byte_length() > (terminal::MAX_FRAME_BYTES / 2) as u64
                || diff.after.descriptor().byte_length() > (terminal::MAX_FRAME_BYTES / 2) as u64 {
                return Err(Error::Invalid("diff display exceeds terminal bounds".into()));
            }
            let context = api.native()?.caller_context(diff.caller, &diff.lease).await?;
            let before = context.read_file(&diff.before).await?;
            let after = context.read_file(&diff.after).await?;
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
                writeln!(output, "prompt-publication-attempt command-operation={command_operation}")
                    .and_then(|()| output.flush()).map_err(|error| Error::Storage(error.to_string()))?;
                native.prompt_command(command, command_operation).await?
            };
            // Publish the caller-held identity before attempting admission.
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
