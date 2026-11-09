//! Line-framed readiness for subprocess fixtures sharing stdout with libtest.
use std::io::{self, BufRead, Write};

/// Publish a complete record even when libtest has left an unterminated prefix.
/// Hold the stdout lock while calling this function to keep the frame together.
pub fn signal(writer: &mut impl Write, marker: &str) -> io::Result<()> {
    writeln!(writer, "\n{marker}")?;
    writer.flush()
}

/// Consume fixture output through its complete readiness record.
/// The caller retains the reader, including any buffered output after the record.
pub fn wait(reader: &mut impl BufRead, marker: &str) -> io::Result<()> {
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if ready(read, &line, marker)? {
            return Ok(());
        }
    }
}

/// Consume a complete readiness record before the caller's original deadline.
/// A silent producer or an unfinished line cannot extend the wait. The caller
/// retains the reader and owns process cleanup on timeout, EOF or cancellation.
pub async fn wait_until(
    reader: &mut (impl tokio::io::AsyncBufRead + Unpin),
    marker: &str,
    deadline: tokio::time::Instant,
) -> io::Result<()> {
    use tokio::io::AsyncBufReadExt as _;

    tokio::time::timeout_at(deadline, async {
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line).await?;
            if ready(read, &line, marker)? {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "fixture readiness deadline exceeded",
        )
    })?
}

fn ready(read: usize, line: &str, marker: &str) -> io::Result<bool> {
    if read == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "fixture exited before signaling readiness",
        ));
    }
    Ok(line.ends_with('\n') && line.trim_end_matches(['\r', '\n']) == marker)
}

#[cfg(test)]
mod tests;
