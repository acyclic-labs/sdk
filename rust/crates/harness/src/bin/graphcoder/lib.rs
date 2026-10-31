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
mod view;

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
use args::{Args, Command, InputFormat};
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
pub fn main_with(seams: impl FnOnce(ServerConfig) -> ServerConfig) -> ExitCode {
    let matches = Args::command().get_matches();
    let mut args = Args::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());
    if args.default_config && matches.value_source("model") != Some(ValueSource::CommandLine) {
        args.model = None;
    }
    let report = acyclic_native_runtime::obs::WaitReport::default();
    if let Err(error) =
        lifecycle::subscriber(&report, std::env::var("GRAPHCODER_LOG").ok()).try_init()
    {
        eprintln!("graphcoder: diagnostics initialization failed: {error}");
        return ExitCode::FAILURE;
    }
    let graceful = args.host_workspace || matches!(args.command, Some(Command::Serve(_)));
    let result = acyclic_native_runtime::coordination_runtime()
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
        .and_then(|runtime| {
            // Keep Tokio's entry frame independent of the size of the CLI future.
            let result = runtime.block_on(phase(|| async {
                let shutdown = lifecycle::shutdown();
                let operation = phase(|| run(args, seams, shutdown.clone()));
                let (result, _) = lifecycle::observe(
                    &report,
                    &mut std::io::stderr(),
                    shutdown,
                    graceful,
                    operation,
                )
                .await;
                result
            }));
            runtime.shutdown_background();
            result
        });
    match result {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("graphcoder: {error}");
            ExitCode::FAILURE
        }
    }
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
) -> Result<u8, Box<dyn std::error::Error>> {
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
        let local = Arc::new(
            phase(|| {
                LocalHarness::open(
                    LocalHarnessConfig::native_host(&args.workspace).state_directory(directory),
                )
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
        return drive(&args, Conversation::open(connection)?).await;
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
    drive(&args, Conversation::open(connection)?).await
}

/// Drives `conversation` headless (`-p`, JSONL) or through the terminal view.
async fn drive(
    args: &Args,
    conversation: Arc<Conversation>,
) -> Result<u8, Box<dyn std::error::Error>> {
    let interactive = args.prompt.is_none()
        && args.input_format != InputFormat::Jsonl
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
    use std::io::Write as _;
    let usage_stdout = args
        .usage_jsonl
        .as_ref()
        .is_some_and(|path| path.as_os_str() == "-");
    // Admit the explicit output before sending any model work. The snapshot
    // replaces an earlier export, so replaying a prompt never appends duplicates.
    let mut usage: Option<Box<dyn std::io::Write>> = args
        .usage_jsonl
        .as_ref()
        .map(|path| {
            if usage_stdout {
                Ok(Box::new(std::io::stdout().lock()) as Box<dyn std::io::Write>)
            } else {
                std::fs::File::create(path)
                    .map(|file| Box::new(std::io::BufWriter::new(file)) as Box<dyn std::io::Write>)
            }
        })
        .transpose()?;
    let mut output: Box<dyn std::io::Write> = if usage_stdout {
        Box::new(std::io::stderr().lock())
    } else {
        Box::new(std::io::stdout().lock())
    };
    let result = if let Some(prompt) = &args.prompt {
        phase(|| headless::prompt(&conversation, &operation, prompt, &mut output)).await
    } else {
        phase(|| headless::jsonl(&conversation, &operation, &mut output)).await
    };
    if let Some(usage) = &mut usage {
        // A refused/failed run can still have committed admissions. Exporting
        // them never changes its result into success or fabricates completion.
        let exported = conversation
            .write_usage_jsonl(usage.as_mut())
            .await
            .and_then(|()| {
                usage
                    .flush()
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
