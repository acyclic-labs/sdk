//! Owned inherited output through native readiness or the shared task engine.
use std::{
    fs::File,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncWrite;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

/// An owned inherited output descriptor.
///
/// Each successful write reports only bytes accepted by the kernel, never
/// consumer receipt, a committed result or durability. [`Self::write_owned`]
/// moves the descriptor, original buffer and its credit together until the
/// physical write returns. There is no second writer through this owner while
/// that operation is outstanding. Independent inherited copies can interleave.
///
/// Nonblocking Unix descriptors use the original OS reactor. Synchronous sinks
/// use the shared native task engine's output work class, isolated from
/// persistence and host callbacks and never executed inline. There is no
/// per-descriptor thread, full-buffer copy, timer or durability barrier.
/// Windows console text uses a fixed stack chunk for UTF-8 to UTF-16 conversion;
/// redirected output preserves the original bytes.
///
/// Borrowed [`AsyncWrite`] is available only for the readiness backend. Its
/// pending writes accept no bytes and retain no borrowed buffer. Synchronous
/// backends require [`Self::write_owned`] and refuse borrowed writes.
pub struct InheritedOutput {
    backend: Backend,
}

enum Backend {
    #[cfg(unix)]
    Readiness(tokio::io::unix::AsyncFd<File>),
    Synchronous(Synchronous),
    Closed,
}

struct Synchronous {
    file: File,
    #[cfg(windows)]
    console: bool,
}

fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "borrowed inherited output requires native readiness; use write_owned for synchronous sinks",
    )
}

impl InheritedOutput {
    /// Transfers a descriptor without changing shared open-file-description flags.
    ///
    /// Pipes, sockets, terminals and regular files are admitted. Already
    /// nonblocking Unix streams use native readiness. Linux reopens blocking
    /// pipes with independent nonblocking flags; other blocking sinks retain
    /// the supplied descriptor for owned writes on shared output workers.
    /// Windows synchronous handles are never treated as overlapped child pipes.
    /// Other holders of an already nonblocking open file description must not
    /// turn blocking mode back on while this writer uses it.
    ///
    /// Readiness admission must run inside the original Tokio IO runtime.
    pub fn from_file(file: File) -> io::Result<Self> {
        #[cfg(unix)]
        {
            unix::admit(file)
        }
        #[cfg(windows)]
        {
            windows::admit(file)
        }
        #[cfg(not(any(unix, windows)))]
        {
            Ok(Self {
                backend: Backend::Synchronous(Synchronous { file }),
            })
        }
    }

    /// Admits an owned copy of stdout; the process retains its original handle.
    pub fn stdout() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::fd::AsFd as _;
            Self::from_file(io::stdout().as_fd().try_clone_to_owned()?.into())
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsHandle as _;
            Self::from_file(io::stdout().as_handle().try_clone_to_owned()?.into())
        }
        #[cfg(not(any(unix, windows)))]
        Err(unsupported())
    }

    /// Admits an owned copy of stderr under the same contract as stdout.
    pub fn stderr() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::fd::AsFd as _;
            Self::from_file(io::stderr().as_fd().try_clone_to_owned()?.into())
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsHandle as _;
            Self::from_file(io::stderr().as_handle().try_clone_to_owned()?.into())
        }
        #[cfg(not(any(unix, windows)))]
        Err(unsupported())
    }

    /// Writes the original owned buffer and returns it with this same writer.
    ///
    /// The buffer may retain caller-owned allocation or semaphore credit; it is
    /// neither copied nor released while an OS write can access it. The caller
    /// must await the returned future to recover the writer and issue another
    /// write. A native error ends this writer and retains the original cause.
    /// Success adds no flush or durability operation.
    ///
    /// Before task admission, dropping the future drops the unsubmitted write.
    /// After synchronous task admission, the original task owns both descriptor
    /// and buffer through physical completion even if its observer disappears;
    /// dropping that observer does not cancel accepted bytes. Readiness writes
    /// can discard only a not-yet-submitted suffix. Neither kind of observer
    /// loss certifies consumer receipt or physical completion to the caller.
    /// A caller that needs a completion join after cancellation must retain
    /// this same future and cancel only its wait, then await the original future.
    pub async fn write_owned<B: AsRef<[u8]> + Send + 'static>(
        mut self,
        buffer: B,
    ) -> io::Result<(Self, B)> {
        if matches!(self.backend, Backend::Closed) {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if buffer.as_ref().is_empty() {
            return Ok((self, buffer));
        }
        if matches!(self.backend, Backend::Synchronous(_)) {
            return crate::task_completion(
                move || {
                    let Backend::Synchronous(sink) = &mut self.backend else {
                        return Err(io::ErrorKind::BrokenPipe.into());
                    };
                    sink.write_all(buffer.as_ref())?;
                    Ok((self, buffer))
                },
                None,
                None,
                None,
                &crate::workers::OUTPUT_WORKERS,
            )?
            .await;
        }
        #[cfg(unix)]
        {
            use tokio::io::AsyncWriteExt as _;
            self.write_all(buffer.as_ref()).await?;
            Ok((self, buffer))
        }
        #[cfg(not(unix))]
        Err(io::ErrorKind::BrokenPipe.into())
    }
}

impl Synchronous {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        #[cfg(windows)]
        if self.console {
            return windows::write_console(&self.file, bytes);
        }
        use std::io::Write as _;
        self.file.write_all(bytes)
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
            let descriptor = match &self.backend {
                Backend::Readiness(descriptor) => descriptor,
                Backend::Synchronous(_) => return Poll::Ready(Err(unsupported())),
                Backend::Closed => return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
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
            let _ = (cx, bytes);
            Poll::Ready(Err(if matches!(self.backend, Backend::Closed) {
                io::ErrorKind::BrokenPipe.into()
            } else {
                unsupported()
            }))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        // An owned write consumes Self until physical completion; a borrowed
        // caller cannot encounter an outstanding write or userspace buffer.
        Poll::Ready(if matches!(self.backend, Backend::Closed) {
            Err(io::ErrorKind::BrokenPipe.into())
        } else {
            Ok(())
        })
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.backend = Backend::Closed;
        Poll::Ready(Ok(()))
    }
}

#[cfg(all(test, target_os = "linux", not(loom)))]
mod tests;
