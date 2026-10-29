//! Streaming protocol pipes on the original process completion service.
use super::{events, platform};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::process::{ChildStdin, ChildStdout};
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A transferred child output pipe. Waiting for input occupies no worker.
pub struct ProcessPipeReader {
    wait: Option<events::Wait>,
    pipe: ChildStdout,
    operation: Arc<events::PipeOperation>,
}

impl ProcessPipeReader {
    /// Transfers the original stdout handle to the process completion service.
    pub fn from_stdout(pipe: ChildStdout) -> io::Result<Self> {
        let operation = events::PipeOperation::reader(&pipe)?;
        Ok(Self {
            wait: None,
            pipe,
            operation,
        })
    }
}

impl AsyncRead for ProcessPipeReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let this = self.get_mut();
        loop {
            if let Some(wait) = &mut this.wait {
                ready!(Pin::new(wait).poll(cx));
                this.wait = None;
            }
            // SAFETY: read_pipe writes into the destination without reading it.
            let unfilled = unsafe { buffer.unfilled_mut() };
            let (read, recorded) = platform::read_pipe(&mut this.pipe, unfilled, None)?;
            recorded?;
            if let Some(read) = read {
                // SAFETY: read_pipe reports only the bytes initialized by ReadFile.
                unsafe { buffer.assume_init(read) };
                buffer.advance(read);
                return Poll::Ready(Ok(()));
            }
            this.wait = Some(events::arm(&[events::Source::Pipe(&this.operation)], None)?);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::process::{Command, Stdio};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    #[test]
    fn quiet_protocol_read_leaves_the_coordination_worker_available() -> io::Result<()> {
        let runtime = crate::coordination_runtime()?;
        runtime.block_on(async {
            let mut command = Command::new("powershell.exe");
            command.args(["-NoProfile", "-NonInteractive", "-Command",
                "[Console]::Out.WriteLine('ready'); $line = [Console]::ReadLine(); [Console]::Out.WriteLine($line)"])
                .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit());
            let mut tree = super::super::ProcessTree::spawn(&mut command)?;
            let input = tree.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?;
            let output = tree.take_stdout().ok_or(io::ErrorKind::BrokenPipe)?;
            let mut input = ProcessPipeWriter::from_stdin(input)?;
            let mut output = BufReader::new(ProcessPipeReader::from_stdout(output)?);
            let mut line = String::new();
            output.read_line(&mut line).await?;
            assert_eq!(line.trim_end(), "ready");
            {
                let mut byte = [0u8];
                let mut read = std::pin::pin!(output.read(&mut byte));
                std::future::poll_fn(|cx| {
                    assert!(read.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                }).await;
            }
            // The same worker needed for the next request must remain runnable
            // while the real child waits for it with its output pipe open.
            tokio::task::spawn_blocking(|| ()).await.map_err(io::Error::other)?;
            input.write_all(b"request\n").await?;
            input.flush().await?;
            line.clear();
            output.read_line(&mut line).await?;
            assert_eq!(line.trim_end(), "request");
            line.clear();
            assert_eq!(output.read_line(&mut line).await?, 0);
            input.shutdown().await?;
            drop(tree);
            Ok(())
        })
    }
}

/// A transferred child input pipe. A write completes after physical delivery.
/// The shared completion service retains pending bytes if the caller disappears.
pub struct ProcessPipeWriter {
    wait: Option<events::Wait>,
    pipe: Option<ChildStdin>,
    writer: platform::Writer,
}

impl ProcessPipeWriter {
    /// Transfers the original stdin handle without adding a blocking worker.
    pub fn from_stdin(pipe: ChildStdin) -> io::Result<Self> {
        let operation = events::PipeOperation::writer(&pipe)?;
        Ok(Self {
            wait: None,
            pipe: Some(pipe),
            writer: platform::Writer(Some(operation)),
        })
    }

    fn poll_completion(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let operation = self.writer.0.as_ref().ok_or(io::ErrorKind::BrokenPipe)?;
        if operation.pending() {
            if self.wait.is_none() {
                self.wait = Some(events::arm(&[events::Source::Pipe(operation)], None)?);
            }
            ready!(Pin::new(self.wait.as_mut().ok_or(io::ErrorKind::BrokenPipe)?).poll(cx));
        }
        self.wait = None;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ProcessPipeWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            ready!(this.poll_completion(cx))?;
            let pipe = this.pipe.as_mut().ok_or(io::ErrorKind::BrokenPipe)?;
            let (written, recorded) =
                platform::write_pipe(pipe, &mut this.writer, bytes, None, None)?;
            recorded?;
            if let Some(written) = written {
                return Poll::Ready(Ok(written));
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_completion(cx))?;
        if let Some(result) = this
            .writer
            .0
            .as_ref()
            .and_then(|operation| operation.take_written())
        {
            result?;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.as_mut().poll_flush(cx))?;
        self.pipe.take();
        Poll::Ready(Ok(()))
    }
}
