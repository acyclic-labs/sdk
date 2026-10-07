//! Opt-in tracing for the CLI and service (see `docs/observability.md`).
//! Nothing is installed unless `ACYCLIC_LOG` or `ACYCLIC_TRACE_FILE` is set,
//! and nothing is ever written to standard output, which carries JSON-RPC.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::{EnvFilter, Layer as _, Registry, fmt};

pub(crate) type BoxedLayer = Box<dyn tracing_subscriber::Layer<Registry> + Send + Sync>;

const DEFAULT_TRACE_FILTER: &str = "acyclic_fs=debug,acyclic_stream=debug,acyclic_objects=debug,acyclic_native_runtime=debug,acyclic_plugin=debug";

/// The trace writer, dropped by [`finish`] so the file ends as valid JSON.
static TRACE: Mutex<Option<tracing_chrome::FlushGuard>> = Mutex::new(None);

/// Installs the subscriber the environment asks for, if any.
pub(crate) fn init() {
    let service = std::env::args_os().nth(1).as_deref() == Some("__service".as_ref());
    let (layers, guard) = layers(|name| std::env::var(name).ok(), service);
    if layers.is_empty() {
        return;
    }
    if tracing::subscriber::set_global_default(Registry::default().with(layers)).is_ok()
        && let Ok(mut trace) = TRACE.lock()
    {
        *trace = guard;
    }
}

/// Completes the trace file. Call before the process exits.
pub(crate) fn finish() {
    if let Ok(mut trace) = TRACE.lock() {
        drop(trace.take());
    }
}

/// The layers `var` asks for, and the trace file's guard. `service` sends
/// the log to a file, as the background service has no standard error.
pub(crate) fn layers(
    var: impl Fn(&str) -> Option<String>,
    service: bool,
) -> (Vec<BoxedLayer>, Option<tracing_chrome::FlushGuard>) {
    let var = |name| var(name).filter(|value| !value.is_empty());
    let mut layers = Vec::new();
    if let Some(directives) = var("ACYCLIC_LOG") {
        let file = var("ACYCLIC_LOG_FILE")
            .map(|path| with_pid(&path))
            .or_else(|| {
                service.then(|| {
                    crate::default_data_directory()
                        .join("logs")
                        .join(with_pid("service-{pid}.log"))
                })
            });
        let layer = match file.map(append) {
            None => Some(fmt::layer().with_writer(io::stderr).boxed()),
            Some(Ok(file)) => Some(
                fmt::layer()
                    .with_ansi(false)
                    .with_writer(Mutex::new(file))
                    .boxed(),
            ),
            Some(Err(error)) => {
                eprintln!("acyclic: cannot open the ACYCLIC_LOG file: {error}");
                None
            }
        };
        layers.extend(layer.map(|layer| layer.with_filter(EnvFilter::new(directives)).boxed()));
    }
    let guard = var("ACYCLIC_TRACE_FILE").and_then(|path| {
        let file = File::create(trace_path(&path, service))
            .map_err(|error| eprintln!("acyclic: cannot create ACYCLIC_TRACE_FILE: {error}"))
            .ok()?;
        let filter = var("ACYCLIC_TRACE_FILTER").unwrap_or_else(|| DEFAULT_TRACE_FILTER.to_owned());
        let (layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
            .writer(file)
            .include_args(true)
            .trace_style(tracing_chrome::TraceStyle::Async)
            .build();
        layers.push(layer.with_filter(EnvFilter::new(filter)).boxed());
        Some(guard)
    });
    (layers, guard)
}

/// The active filters, for `doctor`.
pub(crate) fn active_filters() -> Option<String> {
    let var = |name| std::env::var(name).ok().filter(|value| !value.is_empty());
    let log = var("ACYCLIC_LOG").map(|log| format!("log={log}"));
    let trace = var("ACYCLIC_TRACE_FILE").map(|_| {
        format!(
            "trace={}",
            var("ACYCLIC_TRACE_FILTER").unwrap_or_else(|| DEFAULT_TRACE_FILTER.to_owned())
        )
    });
    match (log, trace) {
        (None, None) => None,
        (log, trace) => Some(log.into_iter().chain(trace).collect::<Vec<_>>().join(" ")),
    }
}

/// Records `outcome` on the current span.
pub(crate) fn record_outcome<T, E>(result: &Result<T, E>) {
    tracing::Span::current().record("outcome", if result.is_ok() { "ok" } else { "err" });
}

/// A hook host as a span field: one of the supported hosts, never input.
pub(crate) fn hook_host(host: &str) -> &'static str {
    match host {
        "codex" => "codex",
        "claude-code" => "claude-code",
        "copilot" => "copilot",
        "cursor" => "cursor",
        _ => "other",
    }
}

/// A JSON-RPC method as a span field.
pub(crate) fn rpc_method(method: &str) -> &'static str {
    match method {
        "initialize" => "initialize",
        "ping" => "ping",
        "tools/list" => "tools/list",
        "tools/call" => "tools/call",
        _ => "other",
    }
}

/// The trace file for this process. The background service inherits the
/// CLI's environment, so without `{pid}` it writes beside, not over, the
/// CLI's trace: each process truncates the file it creates.
fn trace_path(path: &str, service: bool) -> PathBuf {
    let path = with_pid(path);
    if !service
        || path
            .as_os_str()
            .to_string_lossy()
            .contains(&std::process::id().to_string())
    {
        return path;
    }
    let mut name = path.file_stem().unwrap_or_default().to_os_string();
    name.push(format!(".service-{}", std::process::id()));
    if let Some(extension) = path.extension() {
        name.push(".");
        name.push(extension);
    }
    path.with_file_name(name)
}

fn with_pid(path: &str) -> PathBuf {
    PathBuf::from(path.replace("{pid}", &std::process::id().to_string()))
}

fn append(path: PathBuf) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}
