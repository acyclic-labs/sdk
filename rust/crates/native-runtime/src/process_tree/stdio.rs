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

    fn exchange(future: impl Future<Output = io::Result<()>>) -> io::Result<()> {
        crate::coordination_runtime()?.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(5), future)
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "protocol exchange did not complete")
                })?
        })
    }

    #[test]
    fn quiet_protocol_read_leaves_the_coordination_worker_available() -> io::Result<()> {
        exchange(async {
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
            assert_eq!(
                input.write(b"late").await.unwrap_err().kind(),
                io::ErrorKind::BrokenPipe,
            );
            drop(tree);
            Ok(())
        })
    }

    #[test]
    fn cancelled_backpressure_keeps_each_accepted_prefix_with_its_write() -> io::Result<()> {
        use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
        use windows_sys::Win32::System::Threading::{CreateEventW, SetEvent};

        let name = format!("Local\\acyclic-protocol-write-{}", std::process::id());
        let wide: Vec<_> = name.encode_utf16().chain(Some(0)).collect();
        // SAFETY: the terminated name remains live through this call.
        let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, wide.as_ptr()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: creation returned one owned event handle.
        let event = unsafe { OwnedHandle::from_raw_handle(event) };
        exchange(async {
            let mut command = Command::new("powershell.exe");
            command.args(["-NoProfile", "-NonInteractive", "-Command", &format!(
                "$event = [Threading.EventWaitHandle]::OpenExisting('{name}'); [Console]::Out.WriteLine('ready'); $null = $event.WaitOne(); $line = [Console]::ReadLine(); [Console]::Out.WriteLine($line.Length); [Console]::Out.WriteLine([Console]::ReadLine())"
            )]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit());
            let mut tree = super::super::ProcessTree::spawn(&mut command)?;
            let mut input = ProcessPipeWriter::from_stdin(
                tree.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?,
            )?;
            let mut output = BufReader::new(ProcessPipeReader::from_stdout(
                tree.take_stdout().ok_or(io::ErrorKind::BrokenPipe)?,
            )?);
            let mut line = String::new();
            output.read_line(&mut line).await?;
            assert_eq!(line.trim_end(), "ready");
            let completed_wait = events::arm(
                &[events::Source::Pipe(input.writer.0.as_ref().unwrap())],
                None,
            )?;
            assert!(completed_wait.fired());
            let bytes = vec![b'x'; events::PipeOperation::write_length(usize::MAX)];
            let mut accepted = input.write(&bytes).await?;
            assert_eq!(accepted, bytes.len());
            // Standard child pipes have a 64 KiB buffer. If the first chunk
            // fitted inline, the second must wait for this held child to read.
            if !input.writer.0.as_ref().unwrap().pending() {
                assert_eq!(input.write(&bytes).await?, bytes.len());
                accepted += bytes.len();
            }
            assert!(input.writer.0.as_ref().unwrap().pending());
            // An earlier completion may wake a newer wait. Its notification
            // must not certify this still-pending write or accept later bytes.
            input.wait = Some(completed_wait);
            {
                let mut next = std::pin::pin!(input.write(b"discarded"));
                std::future::poll_fn(|cx| {
                    assert!(next.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                }).await;
            }
            // The original child is the sole drain producer, released by this
            // event; abandoning the second write must not submit its bytes.
            // SAFETY: the parent still owns the live event handle.
            if unsafe { SetEvent(event.as_raw_handle()) } == 0 {
                return Err(io::Error::last_os_error());
            }
            input.write_all(b"\nafter\n").await?;
            input.shutdown().await?;
            line.clear();
            output.read_line(&mut line).await?;
            assert_eq!(line.trim_end(), accepted.to_string());
            line.clear();
            output.read_line(&mut line).await?;
            assert_eq!(line.trim_end(), "after");
            line.clear();
            assert_eq!(output.read_line(&mut line).await?, 0);
            drop(tree);
            Ok(())
        })
    }
}

/// A transferred child input pipe. Flush joins physical delivery.
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
        while operation.pending() {
            if self.wait.is_none() {
                self.wait = Some(events::arm(&[events::Source::Pipe(operation)], None)?);
            }
            ready!(Pin::new(self.wait.as_mut().ok_or(io::ErrorKind::BrokenPipe)?).poll(cx));
            self.wait = None;
        }
        self.wait = None;
        if let Some(result) = operation.take_written() {
            result?;
        }
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
        ready!(this.poll_completion(cx))?;
        let pipe = this.pipe.as_mut().ok_or(io::ErrorKind::BrokenPipe)?;
        let (written, recorded) =
            platform::write_pipe(pipe, &mut this.writer, bytes, None, None)?;
        recorded?;
        // The completion service owns this exact prefix once submitted. Report
        // acceptance now so cancelling a write cannot attribute its eventual
        // completion count to a later, different caller buffer.
        Poll::Ready(Ok(written.unwrap_or_else(|| {
            events::PipeOperation::write_length(bytes.len())
        })))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_completion(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.as_mut().poll_flush(cx))?;
        self.pipe.take();
        Poll::Ready(Ok(()))
    }
}
