use std::fs::File;
use std::io;
use std::os::windows::io::AsHandle;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use acyclic_native_runtime::terminal::{AsyncInput, async_input};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::windows::named_pipe::NamedPipeClient;

use super::reader::{Reader, end_of_input};

pub(super) enum Stdin {
    Overlapped(NamedPipeClient),
    Reader(Reader),
}

pub(super) fn open() -> io::Result<Stdin> {
    from_file(File::from(io::stdin().as_handle().try_clone_to_owned()?))
}

pub(super) fn from_file(file: File) -> io::Result<Stdin> {
    Ok(match async_input(file)? {
        AsyncInput::Overlapped(pipe) => Stdin::Overlapped(pipe),
        AsyncInput::Blocking(file) => Stdin::Reader(Reader::blocking(file)?),
        AsyncInput::Console(file) => Stdin::Reader(Reader::console(file)?),
    })
}

impl AsyncRead for Stdin {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Overlapped(pipe) => {
                let before = buf.filled().len();
                let read = ready!(Pin::new(pipe).poll_read(cx, buf));
                let read = read.map(|()| buf.filled().len() - before);
                Poll::Ready(end_of_input(read).map(drop))
            }
            Self::Reader(reader) => reader.poll_read(cx, buf),
        }
    }
}
