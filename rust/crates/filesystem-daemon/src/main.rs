//! Optional bounded transport process over the embedded filesystem engine.

mod mount_journal;
mod protocol;

use acyclic_fs::{LocalFs, LocalOptions, ObjectCacheOptions, OperationId};
use mount_journal::MountJournal;
use protocol::{DaemonState, Request, Response};
use serde::Serialize;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::{Duration, timeout};

const MAXIMUM_REQUEST_BYTES: usize = 24 * 1024 * 1024;
const MAXIMUM_HTTP_HEADER_BYTES: usize = 64 * 1024;

#[derive(Debug, Error)]
enum DaemonError {
    #[error(
        "usage: fsd --root <path> [--listen <loopback-address>] [--object-cache-entries <positive-u32>] [--object-cache-bytes <positive-u64>] [--object-cache-in-flight <positive-u32>] [--object-cache-waiters <positive-u32>]"
    )]
    Usage,
    #[error("fsd accepts loopback listen addresses only")]
    NonLoopback,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fs(#[from] acyclic_fs::FsError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("native mount cleanup failed: {0}")]
    MountCleanup(String),
    #[error("fsd connection shutdown exceeded 30 seconds")]
    ConnectionDrainTimeout,
    #[error(transparent)]
    MountJournal(#[from] mount_journal::MountJournalError),
}

#[derive(Clone, Debug)]
struct Options {
    root: PathBuf,
    listen: SocketAddr,
    object_cache: ObjectCacheOptions,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StartupReceipt {
    schema: &'static str,
    version: &'static str,
    listen: SocketAddr,
    http_endpoint: String,
    http_bearer_token: String,
    pid: u32,
    maximum_request_bytes: usize,
    object_cache: ObjectCacheReceipt,
    recovered_mount_intents: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectCacheReceipt {
    entries: u32,
    bytes: u64,
    in_flight: u32,
    waiters_per_object: u32,
}

fn main() -> Result<(), DaemonError> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), DaemonError> {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    if arguments.len() == 1 && arguments[0] == "--version" {
        println!("fsd {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let options = parse_options(arguments.into_iter())?;
    if !options.listen.ip().is_loopback() {
        return Err(DaemonError::NonLoopback);
    }
    let mut local_options = LocalOptions::new(&options.root);
    local_options.object_cache = options.object_cache;
    let fs = Arc::new(LocalFs::local(local_options)?);
    let mount_journal = MountJournal::open(&options.root)?;
    let recovered_mount_intents = mount_journal.recover()?;
    let state = Arc::new(DaemonState::with_mount_journal(fs, mount_journal));
    let listener = TcpListener::bind(options.listen).await?;
    let listen = listener.local_addr()?;
    let http_bearer_token = hex::encode(OperationId::new().into_bytes());
    println!(
        "{}",
        serde_json::to_string(&StartupReceipt {
            schema: "acyclic-fsd-startup-v1",
            version: env!("CARGO_PKG_VERSION"),
            listen,
            http_endpoint: format!("http://{listen}/v1/fs"),
            http_bearer_token: http_bearer_token.clone(),
            pid: std::process::id(),
            maximum_request_bytes: MAXIMUM_REQUEST_BYTES,
            object_cache: ObjectCacheReceipt {
                entries: options.object_cache.maximum_entries,
                bytes: options.object_cache.maximum_bytes,
                in_flight: options.object_cache.maximum_in_flight,
                waiters_per_object: options.object_cache.maximum_waiters_per_object,
            },
            recovered_mount_intents,
        })?
    );
    let http_bearer_token: Arc<str> = Arc::from(http_bearer_token);
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                if !peer.ip().is_loopback() {
                    continue;
                }
                let state = Arc::clone(&state);
                let http_bearer_token = Arc::clone(&http_bearer_token);
                connections.spawn(async move {
                    if let Err(error) = serve_connection(stream, state, http_bearer_token).await {
                        eprintln!("fsd connection failed: {error}");
                    }
                });
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed {
                    eprintln!("fsd connection task failed: {error}");
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                state.request_shutdown();
                break;
            }
            () = state.shutdown_requested() => {
                break;
            }
        }
    }
    let drained = timeout(Duration::from_secs(30), async {
        while let Some(completed) = connections.join_next().await {
            if let Err(error) = completed {
                eprintln!("fsd connection task failed during shutdown: {error}");
            }
        }
    })
    .await;
    if drained.is_err() {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        state
            .stop_all_mounts()
            .await
            .map_err(DaemonError::MountCleanup)?;
        return Err(DaemonError::ConnectionDrainTimeout);
    }
    state
        .stop_all_mounts()
        .await
        .map_err(DaemonError::MountCleanup)?;
    Ok(())
}

fn parse_options(
    mut arguments: impl Iterator<Item = std::ffi::OsString>,
) -> Result<Options, DaemonError> {
    let mut root = None;
    let mut listen = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let mut object_cache = ObjectCacheOptions::default();
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--root") => root = arguments.next().map(PathBuf::from),
            Some("--listen") => {
                let value = arguments.next().ok_or(DaemonError::Usage)?;
                listen = value
                    .to_str()
                    .ok_or(DaemonError::Usage)?
                    .parse()
                    .map_err(|_| DaemonError::Usage)?;
            }
            Some("--object-cache-entries") => {
                object_cache.maximum_entries = parse_positive(&mut arguments)?;
            }
            Some("--object-cache-bytes") => {
                object_cache.maximum_bytes = parse_positive(&mut arguments)?;
            }
            Some("--object-cache-in-flight") => {
                object_cache.maximum_in_flight = parse_positive(&mut arguments)?;
            }
            Some("--object-cache-waiters") => {
                object_cache.maximum_waiters_per_object = parse_positive(&mut arguments)?;
            }
            _ => return Err(DaemonError::Usage),
        }
    }
    Ok(Options {
        root: root.ok_or(DaemonError::Usage)?,
        listen,
        object_cache,
    })
}

fn parse_positive<T>(
    arguments: &mut impl Iterator<Item = std::ffi::OsString>,
) -> Result<T, DaemonError>
where
    T: std::str::FromStr + PartialEq + Default,
{
    let value = arguments
        .next()
        .ok_or(DaemonError::Usage)?
        .to_str()
        .ok_or(DaemonError::Usage)?
        .parse()
        .map_err(|_| DaemonError::Usage)?;
    if value == T::default() {
        return Err(DaemonError::Usage);
    }
    Ok(value)
}

async fn serve_connection(
    stream: TcpStream,
    state: Arc<DaemonState>,
    http_bearer_token: Arc<str>,
) -> Result<(), DaemonError> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut frame = Vec::new();
    let mut has_frame = tokio::select! {
        biased;
        () = state.shutdown_requested() => return Ok(()),
        result = read_frame(&mut reader, &mut frame) => result?,
    };
    if has_frame && (frame.starts_with(b"POST ") || frame.starts_with(b"OPTIONS ")) {
        return serve_http(&frame, &mut reader, &mut write, state, &http_bearer_token).await;
    }
    loop {
        if !has_frame {
            return Ok(());
        }
        let request: Request = serde_json::from_slice(&frame)?;
        let shutdown = request.requests_shutdown();
        let response: Response = state.handle(request).await;
        let mut encoded = serde_json::to_vec(&response)?;
        encoded.push(b'\n');
        write.write_all(&encoded).await?;
        write.flush().await?;
        if shutdown {
            state.request_shutdown();
            return Ok(());
        }
        has_frame = tokio::select! {
            biased;
            () = state.shutdown_requested() => return Ok(()),
            result = read_frame(&mut reader, &mut frame) => result?,
        };
    }
}

async fn serve_http<R, W>(
    request_line: &[u8],
    reader: &mut BufReader<R>,
    write: &mut W,
    state: Arc<DaemonState>,
    bearer_token: &str,
) -> Result<(), DaemonError>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let request_line = std::str::from_utf8(request_line)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
        .trim_end_matches('\r');
    if request_line == "OPTIONS /v1/fs HTTP/1.1" {
        return write_http(write, 204, b"").await;
    }
    if request_line != "POST /v1/fs HTTP/1.1" {
        return write_http(write, 404, br#"{"error":"not_found"}"#).await;
    }
    let mut content_length = None;
    let mut authorization = None;
    let mut frame = Vec::new();
    let mut header_bytes = request_line.len();
    loop {
        if !read_frame(reader, &mut frame).await? {
            return write_http(write, 400, br#"{"error":"truncated_headers"}"#).await;
        }
        header_bytes = header_bytes.saturating_add(frame.len()).saturating_add(1);
        if header_bytes > MAXIMUM_HTTP_HEADER_BYTES {
            return write_http(write, 431, br#"{"error":"headers_too_large"}"#).await;
        }
        let header = std::str::from_utf8(&frame)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
            .trim_end_matches('\r');
        if header.is_empty() {
            break;
        }
        let Some((name, value)) = header.split_once(':') else {
            return write_http(write, 400, br#"{"error":"malformed_header"}"#).await;
        };
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => {
                content_length = Some(value.trim().parse::<usize>().map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                })?);
            }
            "authorization" => authorization = Some(value.trim().to_owned()),
            _ => {}
        }
    }
    if authorization
        .as_deref()
        .and_then(|value| value.strip_prefix("Bearer "))
        != Some(bearer_token)
    {
        return write_http(write, 401, br#"{"error":"unauthorized"}"#).await;
    }
    let Some(content_length) = content_length else {
        return write_http(write, 411, br#"{"error":"length_required"}"#).await;
    };
    if content_length > MAXIMUM_REQUEST_BYTES {
        return write_http(write, 413, br#"{"error":"request_too_large"}"#).await;
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).await?;
    let request: Request = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return write_http(write, 400, br#"{"error":"invalid_request"}"#).await,
    };
    let shutdown = request.requests_shutdown();
    let response = state.handle(request).await;
    let encoded = serde_json::to_vec(&response)?;
    write_http(write, 200, &encoded).await?;
    if shutdown {
        state.request_shutdown();
    }
    Ok(())
}

async fn write_http<W>(write: &mut W, status: u16, body: &[u8]) -> Result<(), DaemonError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        411 => "Length Required",
        413 => "Content Too Large",
        431 => "Request Header Fields Too Large",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: POST, OPTIONS\r\nAccess-Control-Allow-Headers: authorization, content-type\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    write.write_all(head.as_bytes()).await?;
    write.write_all(body).await?;
    write.flush().await?;
    Ok(())
}

async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    frame: &mut Vec<u8>,
) -> Result<bool, std::io::Error> {
    frame.clear();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(!frame.is_empty());
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index);
        if frame.len().saturating_add(take) > MAXIMUM_REQUEST_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "daemon request exceeds its byte bound",
            ));
        }
        frame.extend_from_slice(&available[..take]);
        let consumed = take.saturating_add(usize::from(newline.is_some()));
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn http_exchange(
        state: Arc<DaemonState>,
        token: Arc<str>,
        authorization: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            serve_connection(stream, state, token).await
        });
        let body = br#"{"id":"health","method":"health"}"#;
        let mut client = TcpStream::connect(address).await?;
        client
            .write_all(
                format!(
                    "POST /v1/fs HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {authorization}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await?;
        client.write_all(body).await?;
        client.shutdown().await?;
        let mut response = String::new();
        client.read_to_string(&mut response).await?;
        server.await??;
        Ok(response)
    }

    #[test]
    fn parser_admits_exact_object_cache_limits() -> Result<(), DaemonError> {
        let options = parse_options(
            [
                "--root",
                "state",
                "--object-cache-entries",
                "17",
                "--object-cache-bytes",
                "4097",
                "--object-cache-in-flight",
                "3",
                "--object-cache-waiters",
                "5",
            ]
            .into_iter()
            .map(std::ffi::OsString::from),
        )?;
        assert_eq!(options.object_cache.maximum_entries, 17);
        assert_eq!(options.object_cache.maximum_bytes, 4097);
        assert_eq!(options.object_cache.maximum_in_flight, 3);
        assert_eq!(options.object_cache.maximum_waiters_per_object, 5);
        Ok(())
    }

    #[test]
    fn parser_rejects_zero_object_cache_limits() {
        for flag in [
            "--object-cache-entries",
            "--object-cache-bytes",
            "--object-cache-in-flight",
            "--object-cache-waiters",
        ] {
            assert!(
                parse_options(
                    ["--root", "state", flag, "0"]
                        .into_iter()
                        .map(std::ffi::OsString::from),
                )
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn http_transport_is_bearer_authenticated_bounded_and_cors_visible()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "acyclic-fsd-http-{}",
            hex::encode(OperationId::new().into_bytes())
        ));
        let state = Arc::new(DaemonState::new(Arc::new(LocalFs::local(
            LocalOptions::new(&root),
        )?)));
        let token: Arc<str> = Arc::from("0123456789abcdef0123456789abcdef");

        let rejected = http_exchange(Arc::clone(&state), Arc::clone(&token), "wrong").await?;
        assert!(rejected.starts_with("HTTP/1.1 401 Unauthorized\r\n"));
        assert!(!rejected.contains("ready"));

        let accepted = http_exchange(state, Arc::clone(&token), &token).await?;
        assert!(accepted.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(accepted.contains("Access-Control-Allow-Origin: *\r\n"));
        assert!(
            accepted
                .ends_with(r#"{"id":"health","ok":true,"result":{"status":"ready"},"error":null}"#)
        );

        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn bounded_reader_rejects_before_unbounded_growth()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut writer, reader) = tokio::io::duplex(MAXIMUM_REQUEST_BYTES + 2);
        let payload = vec![b'x'; MAXIMUM_REQUEST_BYTES + 1];
        tokio::spawn(async move {
            let _ = writer.write_all(&payload).await;
        });
        let mut reader = BufReader::new(reader);
        let mut frame = Vec::new();
        assert!(read_frame(&mut reader, &mut frame).await.is_err());
        assert!(frame.len() <= MAXIMUM_REQUEST_BYTES);
        Ok(())
    }
}
