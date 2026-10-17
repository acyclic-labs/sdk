//! Unix input: the nonblocking terminal descriptor is registered with Tokio's
//! reactor. Reads still enter through the lifecycle's close/suspend fence.
use std::fs::File;
use std::io;
use std::task::{Context, Poll};

use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

use super::super::Tty;
use super::super::lifecycle;
use super::{Chunk, Events};
use crate::key_event::{InputEvent, KeyCode, KeyEventKind, KeyModifiers};
use tokio::signal::unix::{Signal, SignalKind, signal};

pub(super) struct Source {
    // Declared first: deregistration precedes the terminal's restoration.
    readiness: AsyncFd<File>,
    resized: Signal,
    resize_pending: bool,
    tty: Tty,
    bytes: [u8; 1024],
}

impl Tty {
    /// Follow input through the decoder retained by negotiation. Must be
    /// called within a Tokio runtime with I/O and time enabled.
    pub fn events(mut self) -> io::Result<Events> {
        // A duplicate shares the open file description, so its readiness is
        // the terminal's; reads still use the owned descriptor under state.
        let duplicate = lifecycle::active(&self.state, |guard| guard.writer().try_clone_file())?;
        let resized = signal(SignalKind::window_change())?;
        let readiness = AsyncFd::with_interest(duplicate, Interest::READABLE)?;
        let decoder = std::mem::take(&mut self.decoder);
        Events::new(
            Source {
                readiness,
                resized,
                resize_pending: false,
                tty: self,
                bytes: [0; 1024],
            },
            decoder,
        )
    }
}

impl Source {
    /// The next chunk in this source's storage; empty is end of input.
    pub(super) fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Chunk<'_>>> {
        self.resize_pending |= matches!(self.resized.poll_recv(cx), Poll::Ready(Some(())));
        if self.resize_pending {
            match lifecycle::poll_input(&self.tty.state, cx.waker(), |_| Ok(())) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(_)) => {
                    self.resize_pending = false;
                    return Poll::Ready(Ok(Chunk::Resized));
                }
            }
        }
        let bytes = &mut self.bytes;
        loop {
            let Poll::Ready(ready) = self.readiness.poll_read_ready(cx) else {
                // Not readable: still register for close/resume and observe
                // the close fence, so dropping the output ends this stream.
                return match lifecycle::poll_input(&self.tty.state, cx.waker(), |_| Ok(())) {
                    Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                    Poll::Ready(Ok(lifecycle::Input::Resumed)) => Poll::Ready(Ok(Chunk::Resized)),
                    _ => Poll::Pending,
                };
            };
            let mut ready = ready?;
            match lifecycle::poll_input(&self.tty.state, cx.waker(), |guard| {
                guard.writer().read(bytes)
            }) {
                // Suspended: resume or close wakes the registered waker, and
                // cached readiness is kept for the retry.
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                    ready.clear_ready();
                }
                Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(lifecycle::Input::Resumed)) => {
                    return Poll::Ready(Ok(Chunk::Resized));
                }
                Poll::Ready(Ok(lifecycle::Input::Value(count))) => {
                    return Poll::Ready(
                        bytes
                            .get(..count)
                            .map(Chunk::Bytes)
                            .ok_or_else(|| io::ErrorKind::InvalidData.into()),
                    );
                }
            }
        }
    }

    pub(super) fn size(&self) -> io::Result<(u16, u16)> {
        let size = rustix::termios::tcgetwinsize(self.readiness.get_ref())?;
        Ok((size.ws_col, size.ws_row))
    }

    /// A typed Ctrl+Z suspends the process; pasted Ctrl+Z never reaches here.
    pub(super) fn intercept(&mut self, event: &InputEvent) -> io::Result<bool> {
        let suspend = matches!(event, InputEvent::Key(key) if key.code == KeyCode::Char('z')
            && key.modifiers == KeyModifiers::CONTROL && key.kind == KeyEventKind::Press);
        if suspend {
            self.tty.suspend()?;
        }
        Ok(suspend)
    }
}
