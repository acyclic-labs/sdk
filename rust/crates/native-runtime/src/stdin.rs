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

/// Input read on its own thread; end of input is a zero-byte read.
pub struct Stdin {
    chunks: mpsc::Receiver<io::Result<Bytes>>,
    current: Bytes,
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
                    Ok(0) => return,
                    Ok(read) => buffer
                        .get(..read)
                        .map(Bytes::copy_from_slice)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "stdin reader returned an oversized byte count",
                            )
                        }),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => Err(error),
                };
                let failed = chunk.is_err();
                // A dropped reader or a failed read ends the thread.
                if sender.blocking_send(chunk).is_err() || failed {
                    return;
                }
            }
        })?;
    Ok(Stdin {
        chunks,
        current: Bytes::new(),
    })
}

impl AsyncRead for Stdin {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.current.is_empty() {
            match ready!(this.chunks.poll_recv(cx)) {
                Some(Ok(chunk)) => this.current = chunk,
                Some(Err(error)) => return Poll::Ready(Err(error)),
                None => return Poll::Ready(Ok(())),
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
