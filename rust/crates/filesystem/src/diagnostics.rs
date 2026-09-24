//! Process-wide structured diagnostics.
//!
//! A long-lived service has no terminal: its stderr is discarded, and the
//! errors that matter (a mount callback failing, a merge refusing) otherwise
//! reach a client only as a bare errno. Events are appended as JSON lines to
//! one bounded file, rotated to `<file>.1` past [`MAXIMUM_LOG_BYTES`].
//!
//! Nothing is written until [`install`] names a file, so libraries and tests
//! stay silent. `ACYCLIC_LOG_LEVEL` (`error`, `warn`, `info`, `debug`) sets the
//! threshold; the default is `info`.

use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Rotation threshold for the active log file.
pub const MAXIMUM_LOG_BYTES: u64 = 16 * 1024 * 1024;

/// Event severity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Level {
    /// Detailed tracing of individual operations.
    Debug,
    /// Lifecycle events: start, stop, merge, refresh.
    Info,
    /// Degraded but handled conditions.
    Warn,
    /// Failures surfaced to a caller.
    Error,
}

impl Level {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }

    fn from_env() -> Self {
        match std::env::var("ACYCLIC_LOG_LEVEL").as_deref() {
            Ok("debug") => Self::Debug,
            Ok("warn") => Self::Warn,
            Ok("error") => Self::Error,
            _ => Self::Info,
        }
    }
}

struct Sink {
    path: PathBuf,
    file: File,
    written: u64,
    threshold: Level,
}

static SINK: OnceLock<Mutex<Option<Sink>>> = OnceLock::new();

fn sink() -> &'static Mutex<Option<Sink>> {
    SINK.get_or_init(|| Mutex::new(None))
}

/// Starts appending events to `path` for the rest of the process.
///
/// # Errors
///
/// Returns the I/O error when the file cannot be opened.
pub fn install(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let written = file.metadata().map_or(0, |metadata| metadata.len());
    let mut guard = sink()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = Some(Sink {
        path: path.to_owned(),
        file,
        written,
        threshold: Level::from_env(),
    });
    Ok(())
}

/// The file events are written to, when diagnostics are installed.
#[must_use]
pub fn log_path() -> Option<PathBuf> {
    sink()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .map(|sink| sink.path.clone())
}

/// Whether an event at `level` would be written.
#[must_use]
pub fn enabled(level: Level) -> bool {
    sink()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .is_some_and(|sink| level >= sink.threshold)
}

/// Appends one event. `fields` are rendered as JSON strings.
pub fn emit(level: Level, component: &str, event: &str, fields: &[(&str, &dyn std::fmt::Display)]) {
    let mut guard = sink()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(sink) = guard.as_mut() else {
        return;
    };
    if level < sink.threshold {
        return;
    }
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    let mut line = String::with_capacity(160);
    let _ = write!(
        line,
        "{{\"ts_ms\":{millis},\"level\":\"{}\",\"pid\":{},\"component\":",
        level.as_str(),
        std::process::id()
    );
    push_json_string(&mut line, component);
    line.push_str(",\"event\":");
    push_json_string(&mut line, event);
    for (name, value) in fields {
        line.push(',');
        push_json_string(&mut line, name);
        line.push(':');
        push_json_string(&mut line, &value.to_string());
    }
    line.push_str("}\n");
    if sink.written.saturating_add(line.len() as u64) > MAXIMUM_LOG_BYTES {
        let rotated = sink.path.with_extension("log.1");
        let _ = std::fs::rename(&sink.path, rotated);
        if let Ok(file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&sink.path)
        {
            sink.file = file;
            sink.written = 0;
        }
    }
    if sink.file.write_all(line.as_bytes()).is_ok() {
        sink.written = sink.written.saturating_add(line.len() as u64);
    }
}

fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if u32::from(control) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(control));
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

/// Emits an event: `diag!(Level::Error, "mount", "callback_failed", op = "create", path = p)`.
#[macro_export]
macro_rules! diag {
    ($level:expr, $component:expr, $event:expr $(, $name:ident = $value:expr)* $(,)?) => {
        if $crate::diagnostics::enabled($level) {
            $crate::diagnostics::emit(
                $level,
                $component,
                $event,
                &[$((stringify!($name), &$value as &dyn ::std::fmt::Display)),*],
            );
        }
    };
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::{Level, emit, install, log_path};

    #[test]
    fn events_are_json_lines_with_escaped_fields() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("service.log");
        install(&path).expect("install diagnostics");
        assert_eq!(log_path().as_deref(), Some(path.as_path()));
        emit(
            Level::Error,
            "mount",
            "callback_failed",
            &[("path", &"/a \"b\"\n"), ("errno", &5)],
        );
        let text = std::fs::read_to_string(&path).expect("log text");
        let line = text.lines().last().expect("one event");
        assert!(line.contains(r#""component":"mount","event":"callback_failed""#));
        assert!(line.contains(r#""path":"/a \"b\"\n""#));
        assert!(line.contains(r#""errno":"5""#));
    }
}
