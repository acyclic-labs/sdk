//! This process's standard input as an async byte stream.
//!
//! An open stdin's read returns only when input arrives, possibly never, so it
//! never occupies a bounded blocking pool: the coordination runtime's one
//! blocking slot runs only work that completes on its own. One reader thread
//! per process owns the descriptor and hands each chunk over a one-slot
//! channel, which is its backpressure.
use bytes::Bytes;
use std::{
    io::{self, Read},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll, ready},
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::mpsc,
};

static TAKEN: AtomicBool = AtomicBool::new(false);

/// Input read on its own thread; end of input is a zero-byte read. The first
/// native error ends the stream. Unexpected producer loss returns BrokenPipe.
pub struct Stdin {
    chunks: mpsc::Receiver<io::Result<Bytes>>,
    current: Bytes,
    closed: bool,
}

/// This process's stdin. It has one reader, so a second call is refused.
///
/// # Errors
/// [`io::ErrorKind::AlreadyExists`] once taken, or the reader thread's spawn
/// failure.
pub fn stdin() -> io::Result<Stdin> {
    if TAKEN.swap(true, Ordering::AcqRel) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "this process's stdin already has its reader",
        ));
    }
    reader(io::stdin())
}

fn reader(mut input: impl Read + Send + 'static) -> io::Result<Stdin> {
    let (sender, chunks) = mpsc::channel(1);
    std::thread::Builder::new()
        .name("acyclic-stdin".into())
        .spawn(move || {
            let mut buffer = vec![0; 64 * 1024];
            loop {
                let chunk = match input.read(&mut buffer) {
                    Ok(0) => Ok(Bytes::new()),
                    Ok(read) => buffer
                        .get(..read)
                        .map(Bytes::copy_from_slice)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "stdin reader returned an oversized byte count",
                            )
                        }),
                    Err(error) => Err(error),
                };
                let terminal = chunk.as_ref().map_or(true, Bytes::is_empty);
                // A dropped reader or a failed read ends the thread.
                if sender.blocking_send(chunk).is_err() || terminal {
                    return;
                }
            }
        })?;
    Ok(Stdin {
        chunks,
        current: Bytes::new(),
        closed: false,
    })
}

impl AsyncRead for Stdin {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 || this.closed {
            return Poll::Ready(Ok(()));
        }
        if this.current.is_empty() {
            match ready!(this.chunks.poll_recv(cx)) {
                Some(Ok(chunk)) => {
                    this.closed = chunk.is_empty();
                    this.current = chunk;
                }
                Some(Err(error)) => {
                    this.closed = true;
                    return Poll::Ready(Err(error));
                }
                None => {
                    this.closed = true;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "stdin producer ended without an EOF or error result",
                    )));
                }
            }
        }
        let read = this.current.len().min(buf.remaining());
        buf.put_slice(&this.current.split_to(read));
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use crate::lines::AsyncLines;
    use std::io::{Read as _, Write as _};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::AsyncReadExt as _;

    const BODY: &[u8; 8] = b"retained";

    struct FaultReader {
        input: Option<std::io::PipeReader>,
        calls: Arc<AtomicUsize>,
        fail_at: usize,
        unwind: bool,
        retired: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl std::io::Read for FaultReader {
        fn read(&mut self, destination: &mut [u8]) -> std::io::Result<usize> {
            let call = self.calls.fetch_add(1, Ordering::AcqRel) + 1;
            if call == self.fail_at {
                if self.unwind {
                    std::panic::resume_unwind(Box::new("injected stdin producer loss"));
                }
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            let offered = destination.len().min(3);
            self.input
                .as_mut()
                .ok_or(std::io::ErrorKind::BrokenPipe)?
                .read(&mut destination[..offered])
        }
    }

    impl Drop for FaultReader {
        fn drop(&mut self) {
            drop(self.input.take());
            if let Some(retired) = self.retired.take() {
                let _ = retired.send(());
            }
        }
    }

    fn input(
        fail_at: usize,
        unwind: bool,
    ) -> std::io::Result<(
        FaultReader,
        Arc<AtomicUsize>,
        tokio::sync::oneshot::Receiver<()>,
    )> {
        let (input, mut writer) = std::io::pipe()?;
        writer.write_all(BODY)?;
        drop(writer);
        let calls = Arc::new(AtomicUsize::new(0));
        let (retired, retirement) = tokio::sync::oneshot::channel();
        Ok((
            FaultReader {
                input: Some(input),
                calls: calls.clone(),
                fail_at,
                unwind,
                retired: Some(retired),
            },
            calls,
            retirement,
        ))
    }

    #[test]
    fn input_returns_the_first_failure_and_distinguishes_eof_from_producer_loss()
    -> std::io::Result<()> {
        let runtime = crate::coordination_runtime()?;
        for fail_at in [1, 2, 3] {
            let (original, calls, retired) = input(fail_at, false)?;
            let mut stream = super::reader(original)?;
            runtime.block_on(async {
                let mut prefix = Vec::new();
                let mut bytes = [0; 8];
                loop {
                    match stream.read(&mut bytes).await {
                        Ok(0) => {
                            return Err(std::io::Error::other(
                                "stdin hid the original interruption",
                            ));
                        }
                        Ok(count) => prefix.extend_from_slice(&bytes[..count]),
                        Err(error) => {
                            assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
                            break;
                        }
                    }
                }
                assert_eq!(prefix, BODY[..(fail_at - 1) * 3]);
                assert_eq!(stream.read(&mut bytes).await?, 0);
                retired.await.map_err(std::io::Error::other)?;
                assert_eq!(calls.load(Ordering::Acquire), fail_at);
                Ok::<_, std::io::Error>(())
            })?;
            // The actual std helper consumes the same fault and returns all
            // physical pipe bytes: the first-error oracle rejects that policy.
            let (mut old, calls, _) = input(fail_at, false)?;
            let mut bytes = [0; 8];
            std::io::Read::read_exact(&mut old, &mut bytes)?;
            assert_eq!(&bytes, BODY);
            assert_eq!(calls.load(Ordering::Acquire), 4);
        }
        let (original, calls, retired) = input(1, true)?;
        let mut stream = super::reader(original)?;
        runtime.block_on(async {
            // An empty destination must be immediately ready and consume no
            // queued terminal, even if the producer has already disappeared.
            let mut empty = [];
            let mut buffer = tokio::io::ReadBuf::new(&mut empty);
            assert!(matches!(
                super::AsyncRead::poll_read(
                    std::pin::Pin::new(&mut stream),
                    &mut std::task::Context::from_waker(std::task::Waker::noop()),
                    &mut buffer
                ),
                std::task::Poll::Ready(Ok(()))
            ));
            let error = stream.read(&mut [0; 1]).await.unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
            retired.await.map_err(std::io::Error::other)?;
            assert_eq!(calls.load(Ordering::Acquire), 1);
            Ok::<_, std::io::Error>(())
        })
    }

    #[test]
    fn an_open_input_never_holds_the_coordination_blocking_slot() -> std::io::Result<()> {
        let runtime = crate::coordination_runtime()?;
        let (input, mut writer) = std::io::pipe()?;
        runtime.block_on(async {
            let mut lines = AsyncLines::new(super::reader(input)?, usize::MAX);
            // The input is open and empty: its read waits on its own thread
            // while the one blocking slot runs other work to completion.
            tokio::select! {
                biased;
                line = lines.next_line() => panic!("an empty input produced {line:?}"),
                ran = tokio::task::spawn_blocking(|| 7) => assert_eq!(ran?, 7),
            }
            writer.write_all(b"first\nsecond")?;
            drop(writer);
            assert_eq!(
                lines.next_line().await.ok().flatten().as_deref(),
                Some("first")
            );
            assert_eq!(
                lines.next_line().await.ok().flatten().as_deref(),
                Some("second")
            );
            assert!(matches!(lines.next_line().await, Ok(None)));
            Ok(())
        })
    }

    /// Negative control: the same open read on the blocking slot holds every
    /// job queued behind it until input arrives.
    #[test]
    fn an_open_read_on_the_blocking_slot_starves_queued_work() -> std::io::Result<()> {
        let runtime = crate::coordination_runtime()?;
        let (mut input, mut writer) = std::io::pipe()?;
        let (started, reading) = std::sync::mpsc::channel();
        let read = runtime.spawn_blocking(move || {
            started.send(()).ok();
            input.read(&mut [0; 1])
        });
        reading.recv().map_err(std::io::Error::other)?;
        let (ran, queued) = std::sync::mpsc::channel();
        let job = runtime.spawn_blocking(move || ran.send(()).ok());
        assert!(
            queued.try_recv().is_err(),
            "a queued job ran while the slot was held"
        );
        writer.write_all(b"x")?;
        runtime.block_on(async {
            assert_eq!(read.await??, 1);
            job.await?;
            Ok::<_, std::io::Error>(())
        })?;
        assert!(queued.try_recv().is_ok());
        Ok(())
    }
}
