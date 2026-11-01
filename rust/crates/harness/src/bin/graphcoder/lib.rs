//! `GraphCoder`: the harness's `GraphCoder` preset on the local harness, from
//! argv and config files, driven headless (`-p`, JSONL) or through the
//! terminal view. Every behaviour is a harness component; this binary only
//! picks defaults and renders. A host serving through other seams runs the
//! same binary through [`main_with`].
#[cfg(test)]
acyclic_test_support::counting_allocator!();

mod args;
mod headless;
mod lifecycle;
mod usage_export;
mod view;

pub use lifecycle::DiagnosticConfigurationError;

use acyclic_harness::{
    OperationId,
    local::{
        Connection, LocalHarness, LocalHarnessConfig, ModelResolver,
        agent_conversation::Conversation,
    },
    native_settings::SettingsConfig,
    preset::Preset,
    server::ServerConfig,
};
use acyclic_harness_providers::{Credentials, Registry};
use args::{Args, Command, InputFormat, OutputFormat};
use clap::{CommandFactory, FromArgMatches, parser::ValueSource};
use futures::{FutureExt as _, future::LocalBoxFuture};
use std::{future::Future, process::ExitCode, sync::Arc};
use tracing_subscriber::util::SubscriberInitExt as _;

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
/// Diagnostic configuration errors return before execution. JSONL execution
/// errors use the original output owner before their result returns.
/// Diagnostic delivery ends with the original operation, report and shutdown
/// owner; its best-effort failure never replaces the original operation result.
pub fn main_with(
    seams: impl FnOnce(ServerConfig) -> ServerConfig,
    diagnostic_storage: Option<Vec<Box<[u8]>>>,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let matches = Args::command().get_matches();
    let mut args = Args::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());
    if args.default_config && matches.value_source("model") != Some(ValueSource::CommandLine) {
        args.model = None;
    }
    if let Err(error) = args.prepare_input() {
        eprintln!("graphcoder: {error}");
        return Ok(ExitCode::FAILURE);
    }
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
    acyclic_native_runtime::coordination_runtime()
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
        .and_then(|runtime| {
            // Keep Tokio's entry frame independent of the size of the CLI future.
            let result = runtime.block_on(phase(|| async {
                let report = acyclic_native_runtime::obs::WaitReport::default();
                let (subscriber, receiver) =
                    lifecycle::subscriber(&report, std::env::var("GRAPHCODER_LOG").ok(), || {
                        lifecycle::storage(
                            diagnostic_storage,
                            args.diagnostic_records,
                            args.diagnostic_record_bytes,
                        )
                    })?;
                subscriber.try_init()?;
                let diagnostics = receiver
                    .map(|receiver| {
                        acyclic_native_runtime::InheritedOutput::stderr()
                            .map(|writer| (receiver, writer))
                    })
                    .transpose()?;
                let shutdown = lifecycle::shutdown();
                let port = acyclic_native_runtime::obs::ReportPort::from_environment();
                let run_shutdown = shutdown.clone();
                let operation = phase(|| async move {
                    let mut event_output = event_failure
                        .as_ref()
                        .map(|_| {
                            acyclic_native_runtime::InheritedOutput::stdout()
                                .map(headless::Output::new)
                        })
                        .transpose()?;
                    let result = run(args, seams, run_shutdown, event_output.as_mut()).await;
                    if let (Err(error), Some((operation, generated))) = (&result, &event_failure) {
                        let recovery =
                            headless::Recovery::capture(generated.then_some(operation.as_str()))?;
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
                            headless::terminal_error(
                                output,
                                operation,
                                &error.to_string(),
                                step,
                                &recovery,
                            )
                            .await?;
                        }
                    }
                    result
                });
                let original = lifecycle::observe(&report, port, shutdown, graceful, operation);
                let ((result, _report_result), _diagnostic_failure) =
                    lifecycle::diagnostics(original, diagnostics).await;
                result
            }));
            runtime.shutdown_background();
            result
        })
        .map(ExitCode::from)
}

/// The `GraphCoder` preset with argv's overrides for the local harness: the
/// harness resolves its models through the provider registry and its catalog,
/// under the workspace's native settings and search.
fn configure(
    args: &Args,
    credentials: Credentials,
) -> Result<LocalHarnessConfig, Box<dyn std::error::Error>> {
    let mut registry = Registry::new(credentials)?;
    if let Some(path) = &args.catalog {
        registry.load_catalog(path)?;
    }
    let models = ModelResolver::new(registry.default_model().to_owned(), move |selection| {
        let provider = Arc::new(registry.resolve(selection)?);
        Ok((provider.model().clone(), provider as _))
    });
    let mut preset = Preset::graphcoder();
    if let Some(path) = &args.approval_policy {
        preset.approval_policy = serde_json::from_slice(&std::fs::read(path)?)?;
    }
    let mut config =
        LocalHarnessConfig::resolving(&args.workspace, Arc::new(models), args.model.clone())
            .working_directory(&std::env::current_dir()?)?;
    if let Some(directory) = &args.directory {
        config = config.active_directory(directory);
    }
    if args.default_config {
        config = config.without_user_settings();
    } else if let Some(root) = &args.settings_root {
        config = config
            .settings(SettingsConfig::new(&args.workspace, root).codex_home(root.join(".codex")));
    }
    Ok(config.preset(&preset))
}

// Own native phase futures separately from the CLI orchestration frame.
async fn run(
    args: Args,
    seams: impl FnOnce(ServerConfig) -> ServerConfig,
    shutdown: lifecycle::Shutdown,
    event_output: Option<&mut headless::Output>,
) -> Result<u8, Box<dyn std::error::Error>> {
    let output = headless::Prepared::open(&args).map_err(headless::PreparationFailure)?;
    // A server is this machine's harness alone: refuse every client mode
    // before any of them starts.
    if matches!(args.command, Some(Command::Serve(_)))
        && (args.prompt.is_some()
            || args.endpoint.is_some()
            || args.host_workspace
            || args.usage_jsonl.is_some())
    {
        return Err("serve takes no prompt, endpoint, host workspace or usage export".into());
    }
    if args.host_workspace {
        let directory = args
            .host_state_directory
            .as_ref()
            .ok_or("host state directory absent")?;
        // Native hosts keep their own approvals; the configured policy
        // answers only their permission prompts for coordination tools.
        let policy = match &args.approval_policy {
            Some(path) => Some(serde_json::from_slice::<
                acyclic_harness::approvals::ApprovalPolicy,
            >(&std::fs::read(path)?)?),
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
            acyclic_harness::local::host_files::serve_until(local, async {
                shutdown
                    .await
                    .map(|_| ())
                    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
            })
        })
        .await?;
        return Ok(0);
    }
    // An endpoint serves the agent tree: nothing of this machine's harness
    // is opened.
    if let Some(endpoint) = &args.endpoint {
        let connection = phase(|| Connection::connect_endpoint(endpoint.as_deref())).await?;
        return connected(&args, connection, output, event_output).await;
    }
    let credentials = if args.default_config {
        let configured = args
            .credentials
            .as_deref()
            .map(|path| -> Result<Credentials, Box<dyn std::error::Error>> {
                let bytes = std::fs::read(path)?;
                serde_json::from_slice::<Credentials>(&bytes)
                    .map_err(|_| "invalid provider credential file".into())
            })
            .transpose()?;
        Credentials::explicit(
            configured
                .map(|credentials| credentials.api_keys)
                .unwrap_or_default(),
        )
    } else {
        args.credentials
            .as_deref()
            .map(Credentials::from_file)
            .transpose()?
            .unwrap_or_default()
    };
    let config = configure(&args, credentials)?;
    if let Some(Command::Serve(serve)) = &args.command {
        let tls = match (&serve.tls_certificate, &serve.tls_key, &serve.url) {
            (Some(certificate), Some(key), Some(url)) => {
                Some(acyclic_harness::server::TlsIdentity {
                    url: url.clone(),
                    certificate: std::fs::read(certificate)?,
                    key: std::fs::read(key)?,
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
        return Ok(0);
    }
    let local = Arc::new(phase(|| LocalHarness::open(config)).await?);
    let connection = phase(|| local.connect()).await?;
    connected(&args, connection, output, event_output).await
}

async fn connected(
    args: &Args,
    connection: Connection,
    output: headless::Prepared,
    event_output: Option<&mut headless::Output>,
) -> Result<u8, Box<dyn std::error::Error>> {
    let connection = match &args.agent {
        Some(agent) => {
            connection
                .select(acyclic_harness::agent::AgentPath::try_from(agent.clone())?)
                .await?
        }
        None => connection,
    };
    if let Some(Command::Sessions(page) | Command::History(page)) = &args.command {
        let page = acyclic_harness::client::AgentPageRequest::new(
            page.page_size,
            32 * 1024,
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
            OperationId::from_bytes(connection.root().id().to_bytes()),
            &reply,
        )
        .await?;
        return Ok(0);
    }
    drive(args, Conversation::open(connection)?, output, event_output).await
}

/// Drives `conversation` headless (`-p`, JSONL) or through the terminal view.
async fn drive(
    args: &Args,
    conversation: Arc<Conversation>,
    prepared: headless::Prepared,
    event_output: Option<&mut headless::Output>,
) -> Result<u8, Box<dyn std::error::Error>> {
    let headless::Prepared { schema, projection } = prepared;
    let interactive = args.prompt.is_none()
        && args.input_format != InputFormat::Jsonl
        && args.output_format != OutputFormat::Jsonl
        && !acyclic_harness::tui::term::Environment::capture().is_dumb();
    let operation = args
        .operation
        .clone()
        .unwrap_or_else(|| OperationId::new().to_string());
    if interactive {
        if args.usage_jsonl.is_some() {
            return Err("--usage-jsonl requires -p or --input-format jsonl".into());
        }
        return phase(|| view::run(conversation, args.inline, args.resume)).await;
    }
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
    let output = match event_output {
        Some(output) => output,
        None => {
            local_output = headless::Output::new(if usage_stdout {
                acyclic_native_runtime::InheritedOutput::stderr()?
            } else {
                acyclic_native_runtime::InheritedOutput::stdout()?
            });
            &mut local_output
        }
    };
    let result = if let Some(prompt) = &args.prompt
        && projection.is_none()
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
    };
    if let Some(mut usage) = usage.take() {
        // A refused/failed run can still have committed admissions. Exporting
        // them never changes its result into success or fabricates completion.
        let exported = conversation
            .write_usage_jsonl(usage.writer())
            .await
            .and_then(|()| {
                usage
                    .finish()
                    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
            });
        if let Err(error) = exported {
            if result.is_err() {
                eprintln!("graphcoder: usage export failed: {error}");
            } else {
                return Err(error.into());
            }
        }
    }
    result
}
