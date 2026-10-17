//! Native input adapters; JSONL decoding and settlement stay in the shared runner.
use crate::{Error, Result};
use futures::{StreamExt, stream::BoxStream};

/// Bounded JSONL stdin, independently scheduled from replay/follow. Must be
/// called within a Tokio runtime with I/O enabled.
pub fn stdin_jsonl() -> Result<BoxStream<'static, Result<String>>> {
    #[cfg(unix)]
    let input = stdin::open().map_err(|error| Error::Storage(error.to_string()))?;
    #[cfg(not(unix))]
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

/// A dedicated reader for input the reactor cannot wait on. One persistent
/// thread fills recycled batches and feeds a bounded channel, so a slow or
/// stalled read never blocks the runtime. The thread waits in poll on the
/// input and on a private cancel pipe: dropping the stream writes a cancel
/// byte, so inherited writer handles cannot delay cancellation.
#[cfg(unix)]
mod reader {
    use std::fs::File;
    use std::io::{self, Read};
    use std::sync::mpsc;
    use std::task::{Context, Poll, ready};

    use tokio::io::ReadBuf;

    const BATCH: usize = 64 * 1024;

    /// Read once the input is ready, or `None` once `cancel` is hung up or
    /// written. The wait is in poll, so a read starts only with input
    /// present and a cancel seen in the same wake wins. An inherited
    /// description may be nonblocking (a socket, say): WouldBlock waits
    /// again instead of ending input, without changing the shared flags.
    pub(super) fn read_until_cancelled(
        file: &mut File,
        cancel: &io::PipeReader,
        bytes: &mut [u8],
    ) -> io::Result<Option<usize>> {
        use rustix::event::{PollFd, PollFlags, poll};
        loop {
            let mut fds = [
                PollFd::new(&*file, PollFlags::IN),
                PollFd::new(cancel, PollFlags::IN),
            ];
            match poll(&mut fds, None) {
                Err(rustix::io::Errno::INTR) => continue,
                Err(error) => return Err(error.into()),
                Ok(_) => {}
            }
            if !fds[1].revents().is_empty() {
                return Ok(None);
            }
            // A device poll cannot watch (macOS reports some as invalid)
            // answers reads at once; waiting on it again would spin.
            let unwatchable = fds[0].revents().contains(PollFlags::NVAL);
            match file.read(bytes) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock && !unwatchable => {}
                read => return read.map(Some),
            }
        }
    }

    pub(super) struct Reader {
        batches: tokio::sync::mpsc::Receiver<io::Result<Vec<u8>>>,
        spare: mpsc::Sender<Vec<u8>>,
        current: Vec<u8>,
        offset: usize,
        cancel: io::PipeWriter,
        // A live read peer prevents SIGPIPE even after the worker ended.
        _cancel_reader: io::PipeReader,
    }

    impl Reader {
        pub(super) fn spawn(mut file: File) -> io::Result<Self> {
            let (send, batches) = tokio::sync::mpsc::channel(2);
            let (spare, recycled) = mpsc::channel::<Vec<u8>>();
            let (cancelled, cancel) = io::pipe()?;
            let cancel_reader = cancelled.try_clone()?;
            // Detached: an explicit cancel byte ends its wait, while closing
            // the receiver releases a blocked send.
            std::thread::Builder::new()
                .name("harness-stdin".into())
                .spawn(move || {
                    loop {
                        let mut batch = recycled.try_recv().unwrap_or_default();
                        batch.resize(BATCH, 0);
                        let result = match read_until_cancelled(&mut file, &cancelled, &mut batch) {
                            Ok(None) => return,
                            Ok(Some(count)) => Ok(count),
                            Err(error) => Err(error),
                        };
                        let result = result.map(|count| {
                            batch.truncate(count);
                            batch
                        });
                        let last = !matches!(&result, Ok(batch) if !batch.is_empty());
                        if send.blocking_send(result).is_err() || last {
                            return;
                        }
                    }
                })?;
            Ok(Self {
                batches,
                spare,
                current: Vec::new(),
                offset: 0,
                cancel,
                _cancel_reader: cancel_reader,
            })
        }

        #[cfg(test)]
        pub(super) fn duplicate_cancel_writer(&self) -> io::Result<io::PipeWriter> {
            self.cancel.try_clone()
        }

        pub(super) fn poll_read(
            &mut self,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.offset == self.current.len() {
                let batch = match ready!(self.batches.poll_recv(cx)) {
                    Some(batch) => batch?,
                    None => return Poll::Ready(Ok(())),
                };
                let spent = std::mem::replace(&mut self.current, batch);
                self.offset = 0;
                if spent.capacity() != 0 {
                    let _ = self.spare.send(spent);
                }
            }
            let count = buf.remaining().min(self.current.len() - self.offset);
            buf.put_slice(&self.current[self.offset..self.offset + count]);
            self.offset += count;
            Poll::Ready(Ok(()))
        }
    }
    impl Drop for Reader {
        fn drop(&mut self) {
            self.batches.close();
            // Exactly one write to an otherwise empty private pipe.
            let _ = io::Write::write_all(&mut self.cancel, &[0]);
        }
    }
}

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

#[cfg(all(test, unix))]
mod tests {
    use super::stdin::{Stdin, from_file};
    use futures::StreamExt;
    use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
    use std::fs::File;
    use std::io::{self, Write};
    use std::time::Duration;
    use tokio_util::codec::{FramedRead, LinesCodec};

    fn runtime() -> io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
    }

    fn pipe() -> io::Result<(File, File)> {
        let (read, write) = io::pipe()?;
        Ok((
            File::from(std::os::fd::OwnedFd::from(read)),
            File::from(std::os::fd::OwnedFd::from(write)),
        ))
    }

    #[test]
    fn pipe_input_never_changes_the_shared_description() -> io::Result<()> {
        let (read, mut write) = pipe()?;
        let observer = read.try_clone()?;
        let original = fcntl_getfl(&observer)?;
        assert!(!original.contains(OFlags::NONBLOCK));
        let runtime = runtime()?;
        let lines = runtime.block_on(async {
            let input = from_file(read)?;
            assert_eq!(
                matches!(input, Stdin::Reactor(_)),
                cfg!(target_os = "linux")
            );
            // Nothing a killed process could leave behind: the description
            // shared with the parent keeps its flags while input is followed.
            assert_eq!(fcntl_getfl(&observer)?, original);
            write.write_all(b"one\ntwo\n")?;
            drop(write);
            let lines: Vec<_> = FramedRead::new(input, LinesCodec::new()).collect().await;
            io::Result::Ok(lines)
        })?;
        let lines: Vec<String> = lines
            .into_iter()
            .collect::<Result<_, _>>()
            .map_err(io::Error::other)?;
        assert_eq!(lines, ["one", "two"]);
        assert_eq!(fcntl_getfl(&observer)?, original);
        Ok(())
    }

    #[test]
    fn negative_control_detects_flags_set_through_a_duplicate() -> io::Result<()> {
        let (read, _write) = pipe()?;
        let original = fcntl_getfl(&read)?;
        let duplicate = read.try_clone()?;
        fcntl_setfl(&duplicate, original | OFlags::NONBLOCK)?;
        assert_ne!(
            fcntl_getfl(&read)?,
            original,
            "a duplicate shares the description"
        );
        Ok(())
    }

    /// Another reader takes the announced byte between the readiness edge and
    /// this stream's read. The reactor path must keep waiting without
    /// blocking the runtime. (The thread path cannot be raced into blocking
    /// the runtime: only its own thread ever waits in a read.)
    #[cfg(target_os = "linux")]
    #[test]
    fn input_stolen_after_readiness_leaves_the_runtime_running() -> io::Result<()> {
        let (read, mut write) = pipe()?;
        let mut thief = read.try_clone()?;
        let runtime = runtime()?;
        runtime.block_on(async {
            let mut input = from_file(read)?;
            let Stdin::Reactor(fd) = &input else {
                return Err(io::ErrorKind::Unsupported.into());
            };
            write.write_all(b"x")?;
            // Hold the edge, then let the thief consume its byte.
            fd.readable().await?.retain_ready();
            assert_eq!(io::Read::read(&mut thief, &mut [0; 1])?, 1);
            let stalled = tokio::time::timeout(
                Duration::from_millis(5),
                tokio::io::AsyncReadExt::read(&mut input, &mut [0; 1]),
            )
            .await;
            assert!(stalled.is_err(), "the stolen byte was read");
            write.write_all(b"y\n")?;
            let mut lines = FramedRead::new(input, LinesCodec::new());
            let line = lines.next().await.ok_or(io::ErrorKind::UnexpectedEof)?;
            assert_eq!(line.map_err(io::Error::other)?, "y");
            io::Result::Ok(())
        })
    }

    #[test]
    fn negative_control_detects_a_blocking_read_after_stolen_readiness() -> io::Result<()> {
        // The removed shared path: readiness seen, byte stolen, then a read of
        // the blocking description holds its thread.
        let (read, mut write) = pipe()?;
        let mut thief = read.try_clone()?;
        write.write_all(b"x")?;
        let mut fds = [rustix::event::PollFd::new(
            &read,
            rustix::event::PollFlags::IN,
        )];
        rustix::event::poll(&mut fds, None)?;
        assert_eq!(io::Read::read(&mut thief, &mut [0; 1])?, 1);
        let mut shared = read;
        let blocked = std::thread::spawn(move || io::Read::read(&mut shared, &mut [0; 1]));
        std::thread::sleep(Duration::from_millis(5));
        assert!(!blocked.is_finished(), "the stale read returned");
        write.write_all(b"y")?;
        assert_eq!(blocked.join().map_err(|_| io::ErrorKind::Other)??, 1);
        Ok(())
    }

    #[test]
    fn regular_files_are_read_off_the_reactor() -> io::Result<()> {
        let mut file = tempfile::tempfile()?;
        file.write_all(b"alpha\nbeta\n")?;
        std::io::Seek::rewind(&mut file)?;
        let runtime = runtime()?;
        let lines = runtime.block_on(async {
            let input = from_file(file)?;
            assert!(matches!(input, Stdin::Reader(_)));
            let lines: Vec<_> = FramedRead::new(input, LinesCodec::new()).collect().await;
            io::Result::Ok(lines)
        })?;
        let lines: Vec<String> = lines
            .into_iter()
            .collect::<Result<_, _>>()
            .map_err(io::Error::other)?;
        assert_eq!(lines, ["alpha", "beta"]);
        Ok(())
    }

    #[test]
    fn a_stalled_dedicated_read_leaves_the_runtime_running() -> io::Result<()> {
        // A read that cannot complete stands in for a stalled filesystem.
        let (read, mut write) = pipe()?;
        let runtime = runtime()?;
        runtime.block_on(async {
            let reader = super::reader::Reader::spawn(read)?;
            let mut lines = FramedRead::new(Stdin::Reader(reader), LinesCodec::new());
            assert!(
                tokio::time::timeout(Duration::from_millis(5), lines.next())
                    .await
                    .is_err()
            );
            write.write_all(b"late\n")?;
            let line = lines.next().await.ok_or(io::ErrorKind::UnexpectedEof)?;
            assert_eq!(line.map_err(io::Error::other)?, "late");
            io::Result::Ok(())
        })
    }

    #[test]
    fn negative_control_detects_a_read_that_blocks_its_thread() -> io::Result<()> {
        // The removed path read regular files inline; on a stalled source that
        // read holds its thread, which the timer here observes.
        let (mut read, mut write) = pipe()?;
        let blocked = std::thread::spawn(move || io::Read::read(&mut read, &mut [0; 8]));
        let runtime = runtime()?;
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(5)).await });
        assert!(!blocked.is_finished());
        write.write_all(b"x")?;
        assert_eq!(blocked.join().map_err(|_| io::ErrorKind::Other)??, 1);
        Ok(())
    }

    /// Writes one byte after `drop_reader` and reports whether it is still
    /// unread once the reader has had time to take it.
    fn byte_after_drop_unread(
        spawn: impl FnOnce(File) -> io::Result<Box<dyn FnOnce()>>,
    ) -> io::Result<bool> {
        let (read, mut write) = pipe()?;
        let probe = read.try_clone()?;
        let drop_reader = spawn(read)?;
        // Let the reader reach its wait before the stream goes away.
        std::thread::sleep(Duration::from_millis(5));
        drop_reader();
        write.write_all(b"x")?;
        std::thread::sleep(Duration::from_millis(20));
        let mut fds = [rustix::event::PollFd::new(
            &probe,
            rustix::event::PollFlags::IN,
        )];
        Ok(rustix::event::poll(&mut fds, Some(&rustix::event::Timespec::default()))? == 1)
    }

    #[test]
    fn a_dropped_reader_reads_nothing_written_after_it() -> io::Result<()> {
        let inherited = std::cell::RefCell::new(None);
        let unread = byte_after_drop_unread(|read| {
            let runtime = runtime()?;
            let reader = runtime.block_on(async { super::reader::Reader::spawn(read) })?;
            *inherited.borrow_mut() = Some(reader.duplicate_cancel_writer()?);
            Ok(Box::new(move || drop((reader, runtime))))
        })?;
        assert!(unread);
        Ok(())
    }

    #[test]
    fn negative_control_detects_hangup_delayed_by_an_inherited_writer() -> io::Result<()> {
        let (mut read, mut write) = pipe()?;
        let (cancelled, cancel) = io::pipe()?;
        let _inherited = cancel.try_clone()?;
        drop(cancel); // The former cancellation action does not produce HUP.
        write.write_all(b"x")?;
        assert_eq!(
            super::reader::read_until_cancelled(&mut read, &cancelled, &mut [0; 1])?,
            Some(1),
        );
        Ok(())
    }

    #[test]
    fn negative_control_detects_a_dropped_reader_left_reading() -> io::Result<()> {
        // The removed reader waited inside read, which the stream's drop
        // could not interrupt: the next byte after the drop was consumed.
        let unread = byte_after_drop_unread(|mut read| {
            std::thread::spawn(move || io::Read::read(&mut read, &mut [0; 8]));
            Ok(Box::new(|| {}))
        })?;
        assert!(!unread);
        Ok(())
    }

    #[test]
    fn a_nonblocking_socket_waits_for_input_instead_of_ending_it() -> io::Result<()> {
        let (read, mut write) = std::os::unix::net::UnixStream::pair()?;
        read.set_nonblocking(true)?;
        let read = File::from(std::os::fd::OwnedFd::from(read));
        let runtime = runtime()?;
        let lines = runtime.block_on(async {
            let input = from_file(read)?;
            assert!(matches!(input, Stdin::Reader(_)));
            let mut lines = FramedRead::new(input, LinesCodec::new());
            // Empty: the reader's read would block, so it waits in poll.
            assert!(
                tokio::time::timeout(Duration::from_millis(5), lines.next())
                    .await
                    .is_err()
            );
            write.write_all(b"late\n")?;
            drop(write);
            let lines: Vec<_> = lines.collect().await;
            io::Result::Ok(lines)
        })?;
        let lines: Vec<String> = lines
            .into_iter()
            .collect::<Result<_, _>>()
            .map_err(io::Error::other)?;
        assert_eq!(lines, ["late"]);
        Ok(())
    }

    #[test]
    fn negative_control_detects_a_nonblocking_read_ending_input() -> io::Result<()> {
        // The removed reader loop took this error as the end of input.
        let (read, _write) = std::os::unix::net::UnixStream::pair()?;
        read.set_nonblocking(true)?;
        let mut read = File::from(std::os::fd::OwnedFd::from(read));
        let error = io::Read::read(&mut read, &mut [0; 8]).err();
        assert_eq!(
            error.map(|error| error.kind()),
            Some(io::ErrorKind::WouldBlock)
        );
        Ok(())
    }

    #[test]
    fn devices_other_than_terminals_are_read_off_the_reactor() -> io::Result<()> {
        let runtime = runtime()?;
        runtime.block_on(async {
            for device in ["/dev/null", "/dev/random", "/dev/urandom", "/dev/zero"] {
                let input = from_file(File::open(device)?)?;
                assert!(matches!(input, Stdin::Reader(_)), "{device}");
            }
            io::Result::Ok(())
        })
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn negative_control_detects_epoll_accepting_a_device() -> io::Result<()> {
        // Without the allow-list, a reopened /dev/random registers with epoll
        // and would be awaited on the reactor.
        let runtime = runtime()?;
        runtime.block_on(async {
            let device = File::open("/dev/random")?;
            assert!(tokio::io::unix::AsyncFd::try_new(device).is_ok());
            io::Result::Ok(())
        })
    }
}
