//! Extensible command-line interface for Acyclic SDK tools.

use acyclic_harness::{Outcome, TaskGroup, recursive_sum};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

const CONTEXT_VERSION: &str = "1";
const MAXIMUM_CONTROL_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
enum CliError {
    Usage(String),
    Context(&'static str),
    UnsupportedEndpoint(String),
    MessageTooLarge,
    Io(io::Error),
    Json(serde_json::Error),
    Control(String),
}

impl Display for CliError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) | Self::UnsupportedEndpoint(message) | Self::Control(message) => {
                formatter.write_str(message)
            }
            Self::Context(name) => write!(
                formatter,
                "acyclic git requires managed workspace context variable {name}"
            ),
            Self::MessageTooLarge => formatter.write_str("Acyclic control response exceeds 4 MiB"),
            Self::Io(error) => write!(formatter, "Acyclic control connection failed: {error}"),
            Self::Json(error) => {
                write!(formatter, "Acyclic control protocol is malformed: {error}")
            }
        }
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Usage(_)
            | Self::Context(_)
            | Self::UnsupportedEndpoint(_)
            | Self::MessageTooLarge
            | Self::Control(_) => None,
        }
    }
}

impl From<io::Error> for CliError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for CliError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Debug, Serialize)]
struct ControlRequest<'a> {
    version: u32,
    token: &'a str,
    command: &'static str,
    argv: &'a [String],
}

#[derive(Debug, Deserialize)]
struct ControlResponse {
    version: u32,
    ok: bool,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<String>,
}

#[tokio::main]
async fn main() {
    let code = match run(env::args().skip(1).collect()).await {
        Ok(()) => 0,
        Err(CliError::Usage(message)) => {
            eprintln!("{message}");
            2
        }
        Err(error) => {
            eprintln!("{error}");
            1
        }
    };
    if code != 0 {
        std::process::exit(code);
    }
}

async fn run(arguments: Vec<String>) -> Result<(), CliError> {
    let Some((command, command_arguments)) = arguments.split_first() else {
        return Err(CliError::Usage(usage()));
    };
    match command.as_str() {
        "--help" | "-h" | "help" => {
            println!("{}", usage());
            Ok(())
        }
        "--version" | "-V" => {
            println!("acyclic {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        "harness-demo" => run_harness_demo().await,
        "git" => run_git(command_arguments).await,
        other => Err(CliError::Usage(format!(
            "unknown acyclic command '{other}'\n\n{}",
            usage()
        ))),
    }
}

fn usage() -> String {
    "Usage:\n  acyclic git <git-style argv...>\n  acyclic harness-demo\n\nOrdinary system `git` is never intercepted.".to_owned()
}

async fn run_harness_demo() -> Result<(), CliError> {
    let result = recursive_sum(TaskGroup::new(8), (1..=32).collect(), 4).await;
    match result {
        Outcome::Succeeded(value) => println!("recursive result: {value}"),
        outcome => eprintln!("recursive workload did not succeed: {outcome:?}"),
    }
    Ok(())
}

async fn run_git(argv: &[String]) -> Result<(), CliError> {
    if argv.is_empty() {
        return Err(CliError::Usage(
            "acyclic git requires a compatibility subcommand".to_owned(),
        ));
    }
    let version = required_context("ACYCLIC_CONTEXT_VERSION")?;
    if version != CONTEXT_VERSION {
        return Err(CliError::Control(format!(
            "unsupported Acyclic context version '{version}'"
        )));
    }
    let endpoint = required_context("ACYCLIC_CONTROL_ENDPOINT")?;
    let token = required_context("ACYCLIC_WORKSPACE_TOKEN")?;
    let request = serde_json::to_vec(&ControlRequest {
        version: 1,
        token: &token,
        command: "git",
        argv,
    })?;
    if request.len() >= MAXIMUM_CONTROL_MESSAGE_BYTES {
        return Err(CliError::MessageTooLarge);
    }
    let response = request_control(&endpoint, &request).await?;
    if response.version != 1 {
        return Err(CliError::Control(format!(
            "unsupported Acyclic control response version '{}'",
            response.version
        )));
    }
    if !response.ok {
        return Err(CliError::Control(
            response
                .error
                .unwrap_or_else(|| "Acyclic control request failed".to_owned()),
        ));
    }
    if response.error.is_some() {
        return Err(CliError::Control(
            "successful Acyclic control response contains an error".to_owned(),
        ));
    }
    let result = response.result.ok_or_else(|| {
        CliError::Control("successful Acyclic control response lacks a result".to_owned())
    })?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

fn required_context(name: &'static str) -> Result<String, CliError> {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(CliError::Context(name))
}

async fn request_control(endpoint: &str, request: &[u8]) -> Result<ControlResponse, CliError> {
    #[cfg(unix)]
    if let Some(path) = endpoint.strip_prefix("unix://") {
        if path.is_empty() {
            return Err(CliError::UnsupportedEndpoint(
                "Acyclic Unix control endpoint has an empty path".to_owned(),
            ));
        }
        let stream = tokio::net::UnixStream::connect(path).await?;
        return exchange(stream, request).await;
    }

    #[cfg(windows)]
    if let Some(name) = endpoint.strip_prefix("npipe://./pipe/") {
        if name.is_empty() || name.contains('/') || name.contains('\\') {
            return Err(CliError::UnsupportedEndpoint(
                "Acyclic named-pipe endpoint has an invalid name".to_owned(),
            ));
        }
        let path = format!(r"\\.\pipe\{name}");
        let stream = tokio::net::windows::named_pipe::ClientOptions::new().open(path)?;
        return exchange(stream, request).await;
    }

    Err(CliError::UnsupportedEndpoint(
        "Acyclic control endpoint must use the platform-local unix:// or npipe:// scheme"
            .to_owned(),
    ))
}

async fn exchange<S>(mut stream: S, request: &[u8]) -> Result<ControlResponse, CliError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(request).await?;
    stream.write_all(b"\n").await?;
    stream.flush().await?;
    let reader = BufReader::new(stream);
    let mut reader = reader.take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64);
    let mut response = Vec::new();
    let read = reader.read_until(b'\n', &mut response).await?;
    if read == 0 {
        return Err(CliError::Control(
            "Acyclic control endpoint closed without a response".to_owned(),
        ));
    }
    if response.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
        return Err(CliError::MessageTooLarge);
    }
    if response.last() != Some(&b'\n') {
        return Err(CliError::Control(
            "Acyclic control response is not newline terminated".to_owned(),
        ));
    }
    response.pop();
    serde_json::from_slice(&response).map_err(Into::into)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex};

    #[tokio::test]
    async fn control_protocol_is_bounded_versioned_and_machine_readable() {
        let (client, server) = duplex(4_096);
        let server = tokio::spawn(async move {
            let mut server = BufReader::new(server);
            let mut request = String::new();
            server.read_line(&mut request).await.expect("request");
            let value: Value = serde_json::from_str(&request).expect("request JSON");
            assert_eq!(value.get("version"), Some(&serde_json::json!(1)));
            assert_eq!(value.get("command"), Some(&serde_json::json!("git")));
            assert_eq!(value.get("argv"), Some(&serde_json::json!(["status"])));
            assert_eq!(value.get("token"), Some(&serde_json::json!("secret")));
            server
                .get_mut()
                .write_all(b"{\"version\":1,\"ok\":true,\"result\":{\"Status\":{}}}\n")
                .await
                .expect("response");
        });
        let request = serde_json::to_vec(&ControlRequest {
            version: 1,
            token: "secret",
            command: "git",
            argv: &["status".to_owned()],
        })
        .expect("encode");
        let response = exchange(client, &request).await.expect("exchange");
        assert!(response.ok);
        assert_eq!(response.result, Some(serde_json::json!({ "Status": {} })));
        server.await.expect("server");
    }

    #[tokio::test]
    async fn control_protocol_rejects_unterminated_and_oversized_responses() {
        let (client, mut server) = duplex(128);
        let task = tokio::spawn(async move {
            let mut request = Vec::new();
            let mut reader = BufReader::new(&mut server);
            reader
                .read_until(b'\n', &mut request)
                .await
                .expect("request");
            reader
                .get_mut()
                .write_all(b"{\"version\":1}")
                .await
                .expect("response");
        });
        assert!(matches!(
            exchange(client, b"{}").await,
            Err(CliError::Control(message)) if message.contains("newline")
        ));
        task.await.expect("server");

        let oversized = vec![b'x'; MAXIMUM_CONTROL_MESSAGE_BYTES + 1];
        let (client, mut server) = duplex(oversized.len() + 16);
        let task = tokio::spawn(async move {
            let mut request = Vec::new();
            let mut reader = BufReader::new(&mut server);
            reader
                .read_until(b'\n', &mut request)
                .await
                .expect("request");
            reader
                .get_mut()
                .write_all(&oversized)
                .await
                .expect("response");
            reader.get_mut().write_all(b"\n").await.expect("newline");
        });
        assert!(matches!(
            exchange(client, b"{}").await,
            Err(CliError::MessageTooLarge)
        ));
        task.await.expect("server");
    }

    #[test]
    fn endpoint_parser_rejects_cross_platform_and_malformed_schemes() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        assert!(matches!(
            runtime.block_on(request_control("tcp://127.0.0.1:1", b"{}")),
            Err(CliError::UnsupportedEndpoint(_))
        ));
    }
}
