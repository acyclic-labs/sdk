//! Native input adapters; JSONL decoding and settlement stay in the shared runner.
use crate::{Error, Result};
use futures::{StreamExt, stream::BoxStream};

/// Bounded JSONL stdin, independently scheduled from replay/follow. Must be
/// called within a Tokio runtime with I/O enabled.
pub fn stdin_jsonl() -> Result<BoxStream<'static, Result<String>>> {
    #[cfg(any(unix, windows))]
    let input = stdin::open().map_err(|error| Error::Storage(error.to_string()))?;
    #[cfg(not(any(unix, windows)))]
    let input = tokio::io::stdin();
    Ok(Box::pin(
        tokio_util::codec::FramedRead::new(
            input,
            tokio_util::codec::LinesCodec::new_with_max_length(65_536),
        )
        .map(|line| line.map_err(|error| Error::Invalid(error.to_string()))),
    ))
}

/// Unix stdin. Only a description this process owns and opened nonblocking
/// waits on the reactor: no other reader can turn a readiness edge into a
/// blocking read, and its flags affect no other process even if this one is
/// killed. That description exists only where stdin can be reopened (Linux's
/// /proc/self/fd). Every other description is shared or blocking, so a
/// dedicated reader thread reads it off the reactor, flags untouched.
#[cfg(unix)]
mod stdin {
    use std::fs::File;
    use std::io::{self, Read};
    use std::os::fd::{AsFd, AsRawFd, RawFd};
    use std::pin::Pin;
    use std::task::{Context, Poll, ready};

    use tokio::io::unix::AsyncFd;
    use tokio::io::{AsyncRead, ReadBuf};

    use super::reader::Reader;

    /// A nonblocking open file description held by this process alone. The
    /// only constructor reopens the input, so the reactor path cannot be
    /// handed a shared or blocking description.
    pub(super) struct Private(File);

    impl Private {
        #[cfg(target_os = "linux")]
        fn reopen(file: &File) -> Option<Self> {
            use rustix::fs::{Mode, OFlags};
            use std::os::unix::fs::FileTypeExt;
            let kind = file.metadata().ok()?.file_type();
            // Only sources whose readiness means data: pipes, sockets and
            // terminals. Files, block and other character devices (which
            // epoll may still accept) belong to the reader.
            let terminal = kind.is_char_device() && std::io::IsTerminal::is_terminal(file);
            if !(kind.is_fifo() || kind.is_socket() || terminal) {
                return None;
            }
            rustix::fs::open(
                format!("/proc/self/fd/{}", file.as_raw_fd()),
                OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .ok()
            .map(|fd| Self(File::from(fd)))
        }

        #[cfg(not(target_os = "linux"))]
        fn reopen(_: &File) -> Option<Self> {
            None
        }
    }

    impl AsRawFd for Private {
        fn as_raw_fd(&self) -> RawFd {
            self.0.as_raw_fd()
        }
    }

    pub(super) enum Stdin {
        Reactor(AsyncFd<Private>),
        Reader(Reader),
    }

    pub(super) fn open() -> io::Result<Stdin> {
        from_file(File::from(io::stdin().as_fd().try_clone_to_owned()?))
    }

    pub(super) fn from_file(file: File) -> io::Result<Stdin> {
        if let Some(private) = Private::reopen(&file)
            && let Ok(fd) = AsyncFd::try_new(private)
        {
            return Ok(Stdin::Reactor(fd));
        }
        Reader::spawn(file).map(Stdin::Reader)
    }

    impl AsyncRead for Stdin {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let fd = match self.get_mut() {
                Self::Reader(reader) => return reader.poll_read(cx, buf),
                Self::Reactor(fd) => fd,
            };
            loop {
                let mut ready = ready!(fd.poll_read_ready(cx))?;
                // Nonblocking: bytes taken by another reader since the edge
                // yield WouldBlock, which clears readiness and waits again.
                match ready.try_io(|fd| (&fd.get_ref().0).read(buf.initialize_unfilled())) {
                    Ok(Ok(count)) => {
                        buf.advance(count);
                        return Poll::Ready(Ok(()));
                    }
                    Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                    Ok(Err(error)) => return Poll::Ready(Err(error)),
                    Err(_would_block) => {}
                }
            }
        }
    }
}

/// Windows stdin, awaited the way its handle allows. Overlapped pipes
/// complete on the runtime's IOCP with no thread. Everything else may block
/// (a pipe on its writer, a file on a slow share, a device on its peer), so
/// the dedicated reader takes it: the console through its own handle, waited
/// on with a cancel event and decoded from UTF-16 there, never through std's
/// locked stdin. On anonymous pipes the reader measured about 1.7x the
/// throughput of `tokio::io::stdin` at equal latency, and dropping it
/// cancels the pending wait or read and joins the thread, so no orphaned
/// read consumes later input.
#[cfg(windows)]
#[path = "native_input/stdin_windows.rs"]
mod stdin;

#[cfg(any(unix, windows))]
mod reader;

/// Multiplex terminal input with authorized file publications.
#[cfg(all(feature = "tui", feature = "filesystem-local"))]
pub async fn next_file_tui_event(
    client: &mut super::TaskFileClient,
    input: &mut (impl futures::Stream<Item = Result<crate::tui::key_event::InputEvent>> + Unpin),
    completions: &mut crate::tui_files::CompletionFiles,
    prompt: &mut crate::tui::PromptInput,
    commands: &mut super::CommandSession<acyclic_stream::LocalStream>,
    animation: bool,
    renderer: &mut crate::tui::ClientRenderer,
    documents: &mut crate::task_volume::ChangeStream,
) -> Result<Option<crate::tui::key_event::InputEvent>> {
    tokio::select! {
        biased;
        event = input.next() => event.transpose()?.map(Some).ok_or_else(|| Error::Storage("terminal input closed".into())),
        _ = animation_frame::<true>(animation) => Ok(None),
        _ = renderer.search_progress() => Ok(None),
        change = client.next_change() => { change?; Ok(None) },
        update = documents.next() => {
            client.apply_change(update.ok_or_else(|| Error::Conflict("document follow ended; reopen snapshots".into()))??)?;
            Ok(None)
        },
        change = commands.observe_change() => { change?; Ok(None) },
        change = completions.read_change() => {
            completions.apply_change(change?, prompt)?;
            Ok(None)
        },
    }
}

#[cfg(feature = "tui")]
async fn animation_frame<const ENABLED: bool>(busy: bool) {
    if !ENABLED || !busy {
        return std::future::pending().await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(
        crate::tui::Thinking::FRAME_MS,
    ))
    .await;
}

#[cfg(all(test, feature = "tui", not(acyclic_harness_loom)))]
#[path = "native_input/animation.rs"]
mod animation;

/// Follow controlling-terminal input through the decoder retained by
/// negotiation. Input waits on the reactor (a console waiter on Windows);
/// dropping the stream releases the terminal immediately.
#[cfg(feature = "tui")]
pub fn terminal_events(
    tty: crate::tui::term::Tty,
) -> Result<acyclic_stream::BoxProviderStream<'static, Result<crate::tui::key_event::InputEvent>>> {
    let events = tty
        .events()
        .map_err(|error| Error::Storage(error.to_string()))?;
    Ok(Box::pin(events.map(|event| {
        event.map_err(|error| Error::Storage(error.to_string()))
    })))
}

#[cfg(all(test, windows))]
mod windows_tests;

#[cfg(all(test, unix))]
mod unix_tests;
