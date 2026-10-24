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
    pub(super) fn consume(&self) -> io::Result<()> {
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
    pub(super) fn consume(&self) -> io::Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ready(wake: &ProcessWake) -> io::Result<bool> {
        #[cfg(unix)]
        {
            let mut event = libc::pollfd {
                fd: wake.raw(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: this one retained descriptor and writable output stay live.
            let answer = unsafe { libc::poll(&raw mut event, 1, 0) };
            if answer < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(answer != 0)
        }
        #[cfg(windows)]
        {
            // SAFETY: the retained event is observed without blocking.
            let answer = unsafe {
                windows_sys::Win32::System::Threading::WaitForSingleObject(wake.raw(), 0)
            };
            if answer == windows_sys::Win32::Foundation::WAIT_FAILED {
                return Err(io::Error::last_os_error());
            }
            Ok(answer == windows_sys::Win32::Foundation::WAIT_OBJECT_0)
        }
    }

    #[test]
    fn ordinary_notifications_are_acknowledged_but_stops_remain_broadcast() -> io::Result<()> {
        let wake = ProcessWake::new()?;
        assert!(!ready(&wake)?);
        wake.notify()?;
        assert!(ready(&wake)?);
        wake.consume()?;
        assert!(!ready(&wake)?);
        wake.stop()?;
        for _ in 0..4 {
            let consumer = wake.clone();
            assert!(
                ready(&consumer)?,
                "a stopped owner must wake every consumer"
            );
            consumer.consume()?;
        }
        assert!(
            ready(&wake)?,
            "acknowledging a stop must not clear the fence"
        );
        Ok(())
    }
}
