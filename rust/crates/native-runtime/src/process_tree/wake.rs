//! A retained kernel notification wakes capture without polling a callback.
use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Coalescing wake-up for one process-capture owner. A producer records its
/// control state before notifying; the consumer checks that state after wake.
#[derive(Clone)]
pub struct ProcessWake(Arc<Signal>);

struct Signal {
    #[cfg(unix)]
    reader: std::os::unix::net::UnixStream,
    #[cfg(unix)]
    writer: std::os::unix::net::UnixStream,
    #[cfg(windows)]
    event: std::os::windows::io::OwnedHandle,
    stopped: AtomicBool,
}

impl ProcessWake {
    /// Heap storage of one signal, including its shared ownership header.
    /// Callers with an original allocation ledger admit this before `new` and
    /// retain it through the last clone. Descriptors are owned by the signal.
    #[must_use]
    pub const fn allocation_bytes() -> usize {
        // Both atomics in Arc's header have usize alignment. Signal's native
        // descriptors and AtomicBool cannot require a larger alignment.
        let align = std::mem::align_of::<usize>();
        let bytes = 2 * std::mem::size_of::<usize>() + std::mem::size_of::<Signal>();
        bytes.next_multiple_of(align)
    }

    /// Creates one close-on-exec signal shared by producers and its consumer.
    pub fn new() -> io::Result<Self> {
        #[cfg(unix)]
        {
            let (reader, writer) = std::os::unix::net::UnixStream::pair()?;
            reader.set_nonblocking(true)?;
            writer.set_nonblocking(true)?;
            Ok(Self(Arc::new(Signal {
                reader,
                writer,
                stopped: AtomicBool::new(false),
            })))
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::FromRawHandle;
            // SAFETY: unnamed manual-reset event; a successful call transfers
            // its fresh handle to the owner constructed below.
            let event = unsafe {
                windows_sys::Win32::System::Threading::CreateEventW(
                    std::ptr::null(),
                    1,
                    0,
                    std::ptr::null(),
                )
            };
            if event.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: this is the fresh event handle returned above.
            let event = unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(event) };
            Ok(Self(Arc::new(Signal {
                event,
                stopped: AtomicBool::new(false),
            })))
        }
    }

    /// Waits on the retained kernel signal without consuming it. No thread or
    /// task is created. A stop remains observable by every concurrent waiter.
    /// Returns false only when the caller's optional timeout expires.
    pub fn wait(&self, timeout: Option<Duration>) -> io::Result<bool> {
        let deadline = timeout
            .map(|timeout| {
                Instant::now()
                    .checked_add(timeout)
                    .ok_or(io::ErrorKind::InvalidInput)
            })
            .transpose()?;
        loop {
            let remaining = deadline.map(|at| at.saturating_duration_since(Instant::now()));
            match self.wait_once(remaining) {
                Ok(true) => return Ok(true),
                Ok(false) if deadline.is_some_and(|at| Instant::now() >= at) => return Ok(false),
                Ok(false) => {} // native timeout representation chunk; continue the same wait
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn wait_once(&self, timeout: Option<Duration>) -> io::Result<bool> {
        #[cfg(unix)]
        {
            let milliseconds = timeout.map_or(-1, |timeout| {
                i32::try_from(timeout.as_nanos().div_ceil(1_000_000)).unwrap_or(i32::MAX)
            });
            let mut event = libc::pollfd {
                fd: self.raw(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: the retained signal and this writable descriptor remain
            // live for the complete kernel wait. No periodic recheck is armed.
            let result = unsafe { libc::poll(&raw mut event, 1, milliseconds) };
            if result < 0 {
                return Err(io::Error::last_os_error());
            }
            if event.revents & libc::POLLNVAL != 0 {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            Ok(result != 0)
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::{
                Foundation::{WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT},
                System::Threading::{INFINITE, WaitForSingleObject},
            };
            let milliseconds = timeout.map_or(INFINITE, |timeout| {
                u32::try_from(timeout.as_nanos().div_ceil(1_000_000))
                    .unwrap_or(INFINITE - 1)
                    .min(INFINITE - 1)
            });
            // SAFETY: this retained manual-reset event remains owned throughout
            // the kernel wait and is neither closed nor acknowledged here.
            let result = unsafe { WaitForSingleObject(self.raw(), milliseconds) };
            match result {
                WAIT_OBJECT_0 => Ok(true),
                WAIT_TIMEOUT => Ok(false),
                WAIT_FAILED => Err(io::Error::last_os_error()),
                _ => Err(io::ErrorKind::InvalidData.into()),
            }
        }
    }

    /// Makes the capture owner runnable. Repeated notifications coalesce.
    pub fn notify(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::io::Write;
            loop {
                match (&self.0.writer).write(&[1]) {
                    Ok(1) => return Ok(()),
                    Ok(_) => return Err(io::ErrorKind::WriteZero.into()),
                    Err(cause) if cause.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                    Err(cause) if cause.kind() == io::ErrorKind::Interrupted => {}
                    Err(cause) => return Err(cause),
                }
            }
        }
        #[cfg(windows)]
        {
            // SAFETY: this event remains owned for the entire call.
            if unsafe { windows_sys::Win32::System::Threading::SetEvent(self.raw()) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    /// Permanently wakes all consumers after irreversible control loss. The
    /// signal remains readable until its last owner releases it.
    pub fn stop(&self) -> io::Result<()> {
        self.0.stopped.store(true, Ordering::Release);
        self.notify()
    }

    /// Whether irreversible stop has been recorded before signalling.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.0.stopped.load(Ordering::Acquire)
    }

    #[cfg(unix)]
    pub(super) fn raw(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.0.reader.as_raw_fd()
    }

    #[cfg(windows)]
    pub(super) fn raw(&self) -> windows_sys::Win32::Foundation::HANDLE {
        use std::os::windows::io::AsRawHandle;
        self.0.event.as_raw_handle()
    }

    #[cfg(unix)]
    /// Acknowledges a coalesced notification. A stop remains readable for all
    /// borrowers, including those that begin waiting after this call.
    pub fn consume(&self) -> io::Result<()> {
        if self.0.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut bytes = [0; 256];
        loop {
            match (&self.0.reader).read(&mut bytes) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(_) => {}
                Err(cause) if cause.kind() == io::ErrorKind::WouldBlock => {
                    if self.0.stopped.load(Ordering::Acquire) {
                        self.notify()?;
                    }
                    return Ok(());
                }
                Err(cause) if cause.kind() == io::ErrorKind::Interrupted => {}
                Err(cause) => return Err(cause),
            }
        }
    }

    #[cfg(windows)]
    /// Acknowledges a notification, preserving irreversible stop readiness.
    pub fn consume(&self) -> io::Result<()> {
        if self.0.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        // SAFETY: the retained manual-reset event has one capture consumer.
        if unsafe { windows_sys::Win32::System::Threading::ResetEvent(self.raw()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        if self.0.stopped.load(Ordering::Acquire) {
            self.notify()?;
        }
        Ok(())
    }
}

#[cfg(unix)]
impl std::os::fd::AsFd for ProcessWake {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.0.reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_notifications_are_acknowledged_but_stops_remain_broadcast() -> io::Result<()> {
        let wake = ProcessWake::new()?;
        assert!(!wake.wait(Some(Duration::ZERO))?);
        wake.notify()?;
        assert!(wake.wait(None)?);
        assert!(wake.wait(Some(Duration::ZERO))?);
        wake.consume()?;
        assert!(!wake.wait(Some(Duration::ZERO))?);
        wake.stop()?;
        for _ in 0..4 {
            let consumer = wake.clone();
            assert!(consumer.wait(None)?);
            assert!(
                consumer.wait(Some(Duration::ZERO))?,
                "a stopped owner must wake every consumer"
            );
            consumer.consume()?;
        }
        assert!(
            wake.wait(Some(Duration::ZERO))?,
            "acknowledging a stop must not clear the fence"
        );
        Ok(())
    }
}
