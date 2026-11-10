//! `GraphCoder`: the harness's `GraphCoder` preset on the local harness, from
//! argv and config files, driven headless (`-p`, JSONL) or through the
//! terminal view. Every behaviour is a harness component; this binary only
//! picks defaults and renders. A host serving through other seams runs the
//! same binary through [`main_with`].
#[cfg(test)]
acyclic_test_support::counting_allocator!();

mod args;
/// Shipping argument and input boundaries for decoder conformance and fuzzing.
#[cfg(feature = "test-support")]
pub mod test_support {
    pub use crate::args::{Args, Command, InputFormat, OutputFormat};
    pub use crate::headless::input::{Request, parse};
}
mod headless;
mod lifecycle;
mod setup;
mod usage_export;
mod view;

pub use acyclic_harness::completion::{CliCompletion, CliFailure, CliPhase, Exit, FailureReport};
pub use lifecycle::DiagnosticConfigurationError;

use acyclic_harness::{
    OperationId, completion,
    local::{
        Connection, LocalHarness, LocalHarnessConfig, WorkspaceMounting,
        agent_conversation::Conversation,
    },
    native_settings::SettingsConfig,
    server::ServerConfig,
};
use acyclic_harness_providers::{Credentials, Registry};
use args::{Args, Command, InputFormat, OutputFormat};
use clap::{CommandFactory, FromArgMatches, parser::ValueSource};
use futures::{FutureExt as _, future::LocalBoxFuture};
use std::{future::Future, sync::Arc};

// Construct each phase on its own call frame. Boxing an already constructed
// future in the awaiting frame still retains its construction scratch space
// throughout nested polls, and devirtualization can inline its poll there too.
#[inline(never)]
fn phase<'a, F: Future + 'a>(build: impl FnOnce() -> F) -> LocalBoxFuture<'a, F::Output> {
    build().boxed_local()
}

/// `GraphCoder` on argv, its server (`graphcoder serve`) through the seams
/// `seams` makes of the self-deployed defaults: the one entry point a
/// self-deployed and a managed server share.
/// Enabled diagnostics require caller-owned text buffers or an explicit paired
/// CLI reservation. Logging off constructs no text delivery or storage pool.
/// The original completion endpoint is admitted before runtime/setup. Setup
/// rejection uses its same announcement before the shared terminal; an absent
/// endpoint/runtime is explicitly incomplete observation. The original cause
/// remains available through [`CliCompletion::into_result`]. A failure returned
/// after setup uses that same channel before completion, independently of an
/// earlier diagnostic snapshot. Failure stderr uses the original runtime and
/// shutdown event; pre-runtime refusals retain an incomplete stderr outcome.
/// No output starts in OS termination. JSONL execution errors also use their
/// original event output owner before their result returns.
/// Diagnostic delivery ends with the original operation, report and shutdown
/// owner; its best-effort failure never replaces the original operation result.
pub fn main_with(
    seams: impl FnOnce(ServerConfig) -> ServerConfig,
    diagnostic_storage: Option<Vec<Box<[u8]>>>,
) -> CliCompletion {
    let arguments = arguments();
    let runtime = match acyclic_native_runtime::coordination_runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            let (phase, error) = arguments.err().map_or((CliPhase::Runtime, error), |error| {
                (CliPhase::ReportAdmission, error)
            });
            return CliCompletion::rejected(
                phase,
                Box::new(error),
                FailureReport::Incomplete(None),
            );
        }
    };
    // Keep Tokio's entry frame independent of the size of the CLI future.
    let result = runtime.block_on(phase(|| async {
        let shutdown = lifecycle::shutdown();
        let completion = match arguments {
            Ok((args, admission)) => {
                execute(args, admission, seams, diagnostic_storage, shutdown.clone()).await
            }
            Err(error) => CliCompletion::rejected(
                CliPhase::ReportAdmission,
                Box::new(error),
                FailureReport::Incomplete(None),
            ),
        };
        completion
            .write_failure(shutdown, acyclic_native_runtime::InheritedOutput::stderr)
            .await
    }));
    runtime.shutdown_background();
    result
}

fn arguments() -> std::io::Result<(Args, Option<acyclic_native_runtime::obs::ReportAdmission>)> {
    let admission = acyclic_native_runtime::obs::ReportAdmission::from_environment()?;
    let matches = Args::command().get_matches();
    let mut args = Args::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());
    if args.default_config && matches.value_source("model") != Some(ValueSource::CommandLine) {
        args.model = None;
    }
    if let Err(error) = args.prepare_input() {
        Args::command()
            .error(clap::error::ErrorKind::ArgumentConflict, error)
            .exit();
    }
    Ok((args, admission))
}

async fn execute(
    mut args: Args,
    admission: Option<acyclic_native_runtime::obs::ReportAdmission>,
    seams: impl FnOnce(ServerConfig) -> ServerConfig,
    diagnostic_storage: Option<Vec<Box<[u8]>>>,
    shutdown: completion::Shutdown,
) -> CliCompletion {
    let event_failure = (args.output_format == OutputFormat::Jsonl).then(|| {
        let operation = args
            .operation
            .clone()
            .unwrap_or_else(|| OperationId::new().to_string());
        (operation, args.operation.is_none())
    });
    if let Some((operation, _)) = &event_failure {
        args.operation = Some(operation.clone());
    }
    let graceful = args.host_workspace || matches!(args.command, Some(Command::Serve(_)));
    let port = match admission
        .map(acyclic_native_runtime::obs::ReportAdmission::into_port)
        .transpose()
    {
        Ok(port) => port,
        Err(error) => {
            return CliCompletion::rejected(
                CliPhase::ReportRegistration,
                Box::new(error),
                FailureReport::Incomplete(None),
            );
        }
    };
    let report = acyclic_native_runtime::obs::WaitReport::default();
    let diagnostics = match lifecycle::setup(
        &report,
        diagnostic_storage,
        args.diagnostic_records,
        args.diagnostic_record_bytes,
    ) {
        Ok(diagnostics) => diagnostics,
        Err((phase, cause)) => {
            let report =
                completion::report_failure(port, shutdown.clone(), phase, cause.as_ref()).await;
            return CliCompletion::rejected(phase, cause, report);
        }
    };
    let run_shutdown = shutdown.clone();
    let operation = phase(|| async move {
        let mut event_output = event_failure
            .as_ref()
            .map(|_| acyclic_native_runtime::InheritedOutput::stdout().map(headless::Output::new))
            .transpose()?;
        let result = run(args, seams, run_shutdown, event_output.as_mut()).await;
        // The original completion scope owns stderr delivery. JSONL also
        // reports it in-stream, unless the stream already ended with the
        // outcome this exit reports.
        if let (Err(error), Some((operation, generated))) = (&result, &event_failure)
            && !error.is::<Exit>()
        {
            let recovery = headless::Recovery::capture(generated.then_some(operation.as_str()))?;
            let step = if error.is::<headless::PreparationFailure>() {
                "prepare_output"
            } else {
                "execute"
            };
            let output = event_output.as_mut().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "original event output owner is absent",
                )
            })?;
            if output.available() {
                headless::terminal_error(output, operation, &error.to_string(), step, &recovery)
                    .await?;
            }
        }
        result
    });
    let original = completion::observe(&report, Ok(port), shutdown.clone(), graceful, operation);
    let ((result, _report_result, failure_report), _diagnostic_failure) =
        lifecycle::diagnostics(original, diagnostics).await;
    CliCompletion::executed(result, failure_report)
}

/// The `GraphCoder` preset with argv's overrides for the local harness: the
/// harness resolves its models through the provider registry and its catalog,
/// under the workspace's native settings and search.
fn configure(
    args: &Args,
    registry: Registry,
) -> Result<LocalHarnessConfig, Box<dyn std::error::Error>> {
    let mut config = registry
        .graphcoder(&args.workspace, args.model.clone())
        .working_directory(&std::env::current_dir()?)?;
    if let Some(directory) = &args.host_state_directory {
        config = config.state_directory(directory);
    }
    if let Some(path) = &args.approval_policy {
        config = config.approval_policy(serde_json::from_slice(
            &acyclic_harness::local::read_regular_file(path)?,
        )?);
    }
    if let Some(directory) = &args.directory {
        config = config.active_directory(directory);
    }
    let mounting = match args.workspace_mounting {
        args::Mounting::Native => WorkspaceMounting::Native,
        args::Mounting::Physical => WorkspaceMounting::Physical,
        args::Mounting::Auto => {
            let (mounting, unavailable) = WorkspaceMounting::detect();
            if let Some(reason) = unavailable {
                eprintln!(
                    "graphcoder: native workspace mounting is unavailable ({reason}); using physical workspace mounting, which captures changes at each tool boundary"
                );
            }
            mounting
        }
    };
    config = config.workspace_mounting(mounting);
    if args.default_config {
        config = config.without_user_settings();
    } else if let Some(root) = &args.settings_root {
        config = config
            .settings(SettingsConfig::new(&args.workspace, root).codex_home(root.join(".codex")));
    }
    Ok(config)
}

// Own native phase futures separately from the CLI orchestration frame.
async fn run(
    args: Args,
    seams: impl FnOnce(ServerConfig) -> ServerConfig,
    shutdown: completion::Shutdown,
    event_output: Option<&mut headless::Output>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut ready = acyclic_native_runtime::take_service_ready_signal()?;
    let result = run_inner(args, seams, shutdown, event_output, &mut ready).await;
    if let Some(ready) = ready
        && let Err(error) = &result
    {
        ready.fail(&error.to_string())?;
    }
    result
}

async fn run_inner(
    mut args: Args,
    seams: impl FnOnce(ServerConfig) -> ServerConfig,
    shutdown: completion::Shutdown,
    event_output: Option<&mut headless::Output>,
    ready: &mut Option<acyclic_native_runtime::ServiceReadySignal>,
) -> Result<(), Box<dyn std::error::Error>> {
    if matches!(args.command, Some(Command::Doctor))
        && (args.host_workspace || args.endpoint.is_some())
    {
        return Err("doctor checks local startup: incompatible host workspace or endpoint; recovery: remove --host-workspace and --endpoint when running doctor".into());
    }
    args.prepare_outputs().map_err(|error| {
        headless::PreparationFailure(Box::new(error) as Box<dyn std::error::Error>)
    })?;
    let output = headless::Prepared::open(&args).map_err(headless::PreparationFailure)?;
    // A server is this machine's harness alone: refuse every client mode
    // before any of them starts.
    if matches!(args.command, Some(Command::Serve(_)))
        && (args.prompt.is_some()
            || args.endpoint.is_some()
            || args.host_workspace
            || args.usage_jsonl.is_some()
            || args.transcripts.is_some())
    {
        return Err("serve takes no prompt, endpoint, host workspace or usage export".into());
    }
    if args.host_workspace {
        return host_workspace(&args, shutdown).await;
    }
    // An endpoint serves the agent tree: nothing of this machine's harness
    // is opened.
    if let Some(endpoint) = &args.endpoint {
        let connection = phase(|| Connection::connect_endpoint(endpoint.as_deref())).await?;
        return connected(&args, connection, output, event_output, None).await;
    }
    if matches!(args.command, Some(Command::Doctor)) {
        return setup::doctor(&args).await;
    }
    let interactive = !args.local_service
        && args.command.is_none()
        && args.prompt.is_none()
        && args.input_format == InputFormat::Text
        && args.output_format == OutputFormat::Text
        && !acyclic_harness::tui::term::Environment::capture().is_dumb();
    let service = if interactive {
        let directory = acyclic_harness::local::service::Directory::for_workspace(
            &args.workspace,
            args.host_state_directory.as_deref(),
        )?;
        let existing = directory.clone();
        if tokio::task::spawn_blocking(move || existing.existing()).await?? {
            // A running session needs only its existing capability and files;
            // attaching never asks the user for the owner's model credentials.
            return connected(
                &args,
                directory.connect().await?,
                output,
                event_output,
                None,
            )
            .await;
        }
        Some(directory)
    } else {
        None
    };
    let mut registry = setup::registry(&args)?;
    let terminal = setup::first_run(&mut args, &mut registry)?;
    let config = configure(&args, registry)?;
    if args.local_service {
        let directory = acyclic_harness::local::service::Directory::for_config(&config)?;
        let owner = directory.own()?;
        let signal = ready
            .take()
            .ok_or("local service requires its readiness channel")?;
        return acyclic_harness::local::service::serve(
            seams(ServerConfig::new(config)),
            owner,
            signal,
            async {
                let _ = shutdown.await;
            },
        )
        .await
        .map_err(Into::into);
    }
    if let Some(Command::Serve(serve)) = &args.command {
        let tls = match (&serve.tls_certificate, &serve.tls_key, &serve.url) {
            (Some(certificate), Some(key), Some(url)) => {
                Some(acyclic_harness::server::TlsIdentity {
                    url: url.clone(),
                    certificate: acyclic_harness::local::read_regular_file(certificate)?,
                    key: acyclic_harness::local::read_regular_file(key)?,
                    peer: None,
                })
            }
            _ => None,
        };
        let listener = tokio::net::TcpListener::bind(serve.address).await?;
        phase(|| {
            acyclic_harness::server::serve(
                listener,
                seams(ServerConfig {
                    tls,
                    ..ServerConfig::new(config)
                }),
                |endpoint| {
                    use std::io::Write as _;
                    let mut output = std::io::stdout().lock();
                    let _ = serde_json::to_writer(&mut output, endpoint);
                    let _ = writeln!(output);
                    let _ = output.flush();
                },
                async {
                    let _ = shutdown.await;
                },
            )
        })
        .await?;
        return Ok(());
    }
    // Interactive sessions belong to the serving process. The terminal owns
    // only a client, so dropping it neither cancels a turn nor ends its owner.
    if let Some(directory) = service {
        let starter = directory.clone();
        let arguments = service_arguments(&args);
        tokio::task::spawn_blocking(move || {
            starter.start(
                &std::env::current_exe()
                    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?,
                &arguments,
            )
        })
        .await??;
        let connection = directory.connect().await?;
        return connected(&args, connection, output, event_output, terminal).await;
    }
    let local = Arc::new(phase(|| LocalHarness::open(config)).await?);
    let connection = phase(|| local.connect()).await?;
    connected(&args, connection, output, event_output, terminal).await
}

// Only owner configuration crosses the process boundary. Prompt identities,
// view selections and output destinations belong to the attaching client.
fn service_arguments(args: &Args) -> Vec<std::ffi::OsString> {
    let mut arguments = vec![
        "--local-service".into(),
        "--workspace".into(),
        args.workspace.clone().into_os_string(),
    ];
    for (flag, path) in [
        ("--host-state-directory", args.host_state_directory.as_ref()),
        ("--catalog", args.catalog.as_ref()),
        ("--credentials", args.credentials.as_ref()),
        ("--settings-root", args.settings_root.as_ref()),
        ("--approval-policy", args.approval_policy.as_ref()),
    ] {
        if let Some(path) = path {
            arguments.extend([flag.into(), path.clone().into_os_string()]);
        }
    }
    for (flag, value) in [
        ("--model", args.model.as_ref()),
        ("--directory", args.directory.as_ref()),
    ] {
        if let Some(value) = value {
            arguments.extend([flag.into(), value.into()]);
        }
    }
    arguments.extend([
        "--workspace-mounting".into(),
        match args.workspace_mounting {
            args::Mounting::Auto => "auto",
            args::Mounting::Native => "native",
            args::Mounting::Physical => "physical",
        }
        .into(),
    ]);
    if args.default_config {
        arguments.push("--default-config".into());
    }
    if let Some(records) = args.diagnostic_records {
        arguments.extend(["--diagnostic-records".into(), records.to_string().into()]);
    }
    if let Some(bytes) = args.diagnostic_record_bytes {
        arguments.extend(["--diagnostic-record-bytes".into(), bytes.to_string().into()]);
    }
    arguments
}

async fn host_workspace(
    args: &Args,
    shutdown: completion::Shutdown,
) -> Result<(), Box<dyn std::error::Error>> {
    let directory = args
        .host_state_directory
        .as_ref()
        .ok_or("host state directory absent")?;
    // Native hosts keep their own approvals; the configured policy
    // answers only their permission prompts for coordination tools.
    let policy = match &args.approval_policy {
        Some(path) => Some(serde_json::from_slice::<
            acyclic_harness::approvals::ApprovalPolicy,
        >(&acyclic_harness::local::read_regular_file(
            path,
        )?)?),
        None => None,
    };
    let local = Arc::new(
        phase(|| {
            let config =
                LocalHarnessConfig::native_host(&args.workspace).state_directory(directory);
            LocalHarness::open(match policy {
                Some(policy) => config.approval_policy(policy),
                None => config,
            })
        })
        .await?,
    );
    phase(|| {
        acyclic_harness::local::host_files::serve_until(
            local,
            || {
                use std::io::Write as _;
                std::io::stdout()
                    .lock()
                    .write_all(b"{\"type\":\"workspace-owner-ready\"}\n")
                    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
            },
            async {
                shutdown
                    .await
                    .map(|_| ())
                    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
            },
        )
    })
    .await?;
    Ok(())
}

async fn connected(
    args: &Args,
    connection: Connection,
    output: headless::Prepared,
    event_output: Option<&mut headless::Output>,
    terminal: Option<acyclic_harness::tui::term::Tty>,
) -> Result<(), Box<dyn std::error::Error>> {
    let connection = match &args.agent {
        Some(agent) => {
            connection
                .select(acyclic_harness::agent::AgentPath::try_from(agent.clone())?)
                .await?
        }
        None => connection,
    };
    let connection = if let Some(source) = &args.fork_from {
        let request = match &args.override_file {
            Some(path) => serde_json::from_slice(&std::fs::read(path)?)?,
            None => acyclic_harness::agent::ContinuationRequest {
                version: 1,
                override_item: None,
            },
        };
        let operation = args
            .operation
            .clone()
            .unwrap_or_else(|| OperationId::new().to_string());
        acyclic_harness::local::continuation::fork_from(&connection, source, &operation, request)
            .await?
    } else {
        connection
    };
    if let Some(Command::Sessions(page) | Command::History(page)) = &args.command {
        let page = acyclic_harness::client::AgentPageRequest::new(
            page.page_size,
            page.page_bytes,
            page.cursor.clone(),
        )?;
        let agent = connection.root().clone();
        let command = if matches!(args.command, Some(Command::History(_))) {
            acyclic_harness::client::AgentCommand::History { agent, page }
        } else {
            acyclic_harness::client::AgentCommand::Children { agent, page }
        };
        let reply = connection.execute(&command).await?;
        let mut output = headless::Output::new(acyclic_native_runtime::InheritedOutput::stdout()?);
        headless::write_event(
            &mut output,
            OperationId::from_bytes(connection.root().id_bytes()),
            &reply,
        )
        .await?;
        return Ok(());
    }
    if let Some(Command::Study(study)) = &args.command {
        // Admitted before any model call.
        let admitted = acyclic_harness::local::study::Study::load(&study.arms)?;
        admitted.admit_model(args.model.as_deref())?;
        let mut report = usage_export::Output::open(&study.report)?;
        let operation = args
            .operation
            .clone()
            .unwrap_or_else(|| OperationId::new().to_string());
        let failures =
            acyclic_harness::local::study::run(&connection, &admitted, &operation, report.writer())
                .await?;
        report.finish()?;
        // Failed trials are report rows; the operator also sees each one.
        for failure in failures {
            eprintln!("graphcoder: study trial failed: {failure}");
        }
        return Ok(());
    }
    drive(
        args,
        Conversation::open(connection),
        output,
        event_output,
        terminal,
    )
    .await
}

/// Drives `conversation` headless (`-p`, JSONL) or through the terminal view.
async fn drive(
    args: &Args,
    conversation: Arc<Conversation>,
    prepared: headless::Prepared,
    event_output: Option<&mut headless::Output>,
    terminal: Option<acyclic_harness::tui::term::Tty>,
) -> Result<(), Box<dyn std::error::Error>> {
    let headless::Prepared { schema, projection } = prepared;
    let events = projection.is_some();
    let interactive = args.prompt.is_none()
        && args.input_format != InputFormat::Jsonl
        && args.output_format != OutputFormat::Jsonl
        && !acyclic_harness::tui::term::Environment::capture().is_dumb();
    let operation = args
        .operation
        .clone()
        .unwrap_or_else(|| OperationId::new().to_string());
    if interactive {
        if args.usage_jsonl.is_some() || args.transcripts.is_some() {
            return Err(
                "--usage-jsonl and --transcripts require -p or --input-format jsonl".into(),
            );
        }
        return phase(|| view::run(conversation, args.inline, args.resume, terminal)).await;
    }
    drop(terminal);
    let usage_stdout = args
        .usage_jsonl
        .as_ref()
        .is_some_and(|path| path.as_os_str() == "-");
    // Admit the explicit output before sending any model work. The snapshot
    // replaces an earlier export, so replaying a prompt never appends duplicates.
    let mut usage = args
        .usage_jsonl
        .as_deref()
        .map(usage_export::Output::open)
        .transpose()?;
    let mut local_output;
    let output = if let Some(output) = event_output {
        output
    } else {
        local_output = headless::Output::new(if usage_stdout {
            acyclic_native_runtime::InheritedOutput::stderr()?
        } else {
            acyclic_native_runtime::InheritedOutput::stdout()?
        });
        &mut local_output
    };
    let run = async {
        if let Some(prompt) = &args.prompt
            && projection.is_none()
            && args.input_format == InputFormat::Text
        {
            phase(|| headless::prompt(&conversation, &operation, prompt, output)).await
        } else {
            phase(|| {
                headless::jsonl(
                    &conversation,
                    &operation,
                    output,
                    projection,
                    args.prompt.as_deref(),
                    schema,
                    args.output.clone(),
                    args.input_format,
                )
            })
            .await
        }
    };
    let result = if let Some(directory) = &args.transcripts {
        conversation
            .with_transcripts(directory, &operation, run)
            .await?
    } else {
        run.await
    };
    if let Some(mut usage) = usage.take() {
        // A refused/failed run can still have committed admissions. Exporting
        // them never changes its result into success or fabricates completion.
        // The export reads committed records, so it observes them through a
        // session that outlives a runtime failure which ended the run.
        let exported = async {
            conversation
                .observation()
                .await?
                .write_usage_jsonl(usage.writer())
                .await?;
            usage
                .finish()
                .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
        }
        .await;
        if let Err(error) = exported {
            if result.is_err() {
                eprintln!("graphcoder: usage export failed: {error}");
            } else {
                return Err(error.into());
            }
        }
    }
    if events
        && (result.is_ok()
            || result
                .as_ref()
                .is_err_and(|error| error.is::<completion::Exit>()))
    {
        headless::completed(output, conversation.task(), &operation, result.is_ok()).await?;
    }
    result
}
