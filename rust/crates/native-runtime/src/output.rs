//! Readiness-driven output without a blocking worker or an output queue.
use std::{
    fs::File,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncWrite;

#[cfg(unix)]
mod unix;

/// An owned output descriptor driven by the runtime's original OS reactor.
///
/// Each successful write reports only bytes accepted by the kernel, never
/// consumer receipt, a committed result or durability. A pending write accepts
/// no bytes and retains no borrowed buffer: its caller keeps the original
/// buffer and credit. No OS request retains those bytes after a write returns,
/// so cancellation can discard only the caller's not-yet-submitted suffix.
///
/// There is no internal queue, worker, timer or flush barrier. Dropping or
/// shutting down this writer closes its owned descriptor, not other inherited
/// copies. Independent writers can interleave; callers serialize their records.
pub struct InheritedOutput {
    #[cfg(unix)]
    descriptor: Option<tokio::io::unix::AsyncFd<File>>,
}

fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "inherited output needs an independently nonblocking pipe, socket or terminal; synchronous handles and regular files are not admitted",
    )
}

impl InheritedOutput {
    /// Transfers a descriptor without changing shared open-file-description flags.
    ///
    /// Already nonblocking Unix pipes, sockets and terminals are admitted.
    /// Linux can reopen a blocking pipe or terminal through its original procfs
    /// descriptor with independent nonblocking flags. Other blocking descriptors
    /// and regular files return [`io::ErrorKind::Unsupported`]. Windows inherited
    /// synchronous handles are unsupported; they are never submitted as IOCP
    /// child-pipe operations. Refusal consumes and closes only the supplied file.
    /// Other holders of an already nonblocking open file description must not
    /// turn blocking mode back on while this writer uses it.
    ///
    /// Construction must run inside the original Tokio IO runtime.
    pub fn from_file(file: File) -> io::Result<Self> {
        #[cfg(unix)]
        {
            let file = unix::admit(file)?;
            Ok(Self {
                descriptor: Some(tokio::io::unix::AsyncFd::with_interest(
                    file,
                    tokio::io::Interest::WRITABLE,
                )?),
            })
        }
        #[cfg(not(unix))]
        {
            drop(file);
            Err(unsupported())
        }
    }

    /// Admits an owned copy of stdout; the process retains its original handle.
    pub fn stdout() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::fd::AsFd as _;
            Self::from_file(io::stdout().as_fd().try_clone_to_owned()?.into())
        }
        #[cfg(not(unix))]
        Err(unsupported())
    }

    /// Admits an owned copy of stderr under the same contract as stdout.
    pub fn stderr() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::fd::AsFd as _;
            Self::from_file(io::stderr().as_fd().try_clone_to_owned()?.into())
        }
        #[cfg(not(unix))]
        Err(unsupported())
    }
}

impl AsyncWrite for InheritedOutput {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        #[cfg(unix)]
        {
            use std::io::Write as _;
            use std::task::ready;
            let Some(descriptor) = &self.descriptor else {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            };
            if bytes.is_empty() {
                return Poll::Ready(Ok(0));
            }
            loop {
                let mut writable = ready!(descriptor.poll_write_ready(cx))?;
                match writable.try_io(|descriptor| {
                    let mut file = descriptor.get_ref();
                    file.write(bytes)
                }) {
                    Ok(result) => return Poll::Ready(result),
                    // The syscall consumed this readiness. The same OS reactor
                    // registers the original peer's next drain or close event.
                    Err(_) => continue,
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (self, cx, bytes);
            Poll::Ready(Err(unsupported()))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        // There is no userspace buffer or outstanding kernel-owned write.
        #[cfg(unix)]
        return Poll::Ready(if self.descriptor.is_some() {
            Ok(())
        } else {
            Err(io::ErrorKind::BrokenPipe.into())
        });
        #[cfg(not(unix))]
        {
            let _ = self;
            Poll::Ready(Err(unsupported()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        #[cfg(unix)]
        {
            self.descriptor.take();
            Poll::Ready(Ok(()))
        }
        #[cfg(not(unix))]
        {
            let _ = &mut self;
            Poll::Ready(Err(unsupported()))
        }
    }
}

#[cfg(all(test, target_os = "linux", not(loom)))]
mod tests;
