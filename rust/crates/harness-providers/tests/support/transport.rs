//! One socket fixture for provider transport and captured-export controls.
use acyclic_harness_core::{Error, Result};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

/// One scripted connection.
pub(crate) enum Reply {
    /// A complete response with this status and body.
    Full(u16, &'static str),
    /// A response declaring the whole body but sending only `usize` bytes before closing.
    Cut(u16, &'static str, usize),
    /// The server reads the request, then closes without returning headers.
    Lost,
    /// Bytes that are not a TLS record.
    Garbage,
}

/// Serves `replies` in order, one per connection, and returns the port and the accept count.
pub(crate) async fn serve(replies: Vec<Reply>) -> Result<(u16, Arc<AtomicUsize>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| invalid("bind"))?;
    let port = listener.local_addr().map_err(|_| invalid("addr"))?.port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    let mut replies: VecDeque<_> = replies.into();
    tokio::spawn(async move {
        while let Some(reply) = replies.pop_front() {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            if matches!(reply, Reply::Garbage) {
                // Take the ClientHello, so the answer is read rather than reset away.
                let mut hello = [0; 4096];
                let _ = socket.read(&mut hello).await;
            } else {
                read_request(&mut socket).await;
            }
            let bytes = match reply {
                Reply::Full(status, body) => format!(
                    "HTTP/1.1 {status} S\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                ),
                Reply::Cut(status, body, sent) => format!(
                    "HTTP/1.1 {status} S\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    &body[..sent]
                ),
                Reply::Garbage => "this is not a TLS record\r\n\r\n".repeat(4),
                Reply::Lost => String::new(),
            };
            let _ = socket.write_all(bytes.as_bytes()).await;
            let _ = socket.flush().await;
            // Wait for the client to hang up, so no unread bytes turn the close into a reset;
            // closing short of the declared length cuts the body mid-stream.
            let _ = socket.shutdown().await;
            let mut rest = [0; 4096];
            while matches!(socket.read(&mut rest).await, Ok(read) if read > 0) {}
            drop(socket);
        }
    });
    Ok((port, accepted))
}

/// Reads one request's headers and its declared body.
async fn read_request(socket: &mut tokio::net::TcpStream) {
    let mut buffer = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        let Ok(read) = socket.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&buffer[..end]).to_ascii_lowercase();
        let length = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if buffer.len() >= end + 4 + length {
            return;
        }
    }
}
