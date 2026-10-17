use std::fs::File;
use std::io::{self, Read, Write};
use std::time::Instant;

use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use rustix::termios::{OptionalActions, Termios, tcgetattr, tcsetattr};

use super::{PROBE_TIMEOUT, ProbeIo};

/// Exclusive controlling-terminal connection with raw mode and descriptor flags
/// restored by the owning lifecycle before releasing ownership. DEC
/// modes are undone before the original line discipline is restored.
pub(crate) struct RawTty {
    file: File,
    original: Termios,
    flags: OFlags,
}

impl RawTty {
    /// Take exclusive ownership of a terminal descriptor (also usable with a
    /// PTY slave). Preserve already-enabled raw mode and nonblocking flags.
    pub fn from_file(file: File) -> io::Result<Self> {
        let original = tcgetattr(&file)?;
        let flags = fcntl_getfl(&file)?;
        let mut tty = Self {
            file,
            original,
            flags,
        };
        if let Err(error) = tty.reapply() {
            let _ = tty.restore();
            return Err(error);
        }
        Ok(tty)
    }

    /// Restore while the lifecycle still holds exclusive ownership. There is
    /// no destructor restoration: retained state can outlive a newer session.
    pub fn restore(&mut self) -> io::Result<()> {
        let termios = tcsetattr(&self.file, OptionalActions::Now, &self.original).and_then(|()| {
            #[cfg(target_os = "macos")]
            if !self
                .original
                .local_modes
                .contains(rustix::termios::LocalModes::PENDIN)
            {
                // Darwin sets PENDIN when ICANON is restored and preserves it
                // across tcsetattr. FIONREAD runs ttypend without consuming
                // input: queued bytes reenter the original line discipline
                // and the synthetic PENDIN bit is cleared (xnu tty.c/ttnread).
                let _ = rustix::io::ioctl_fionread(&self.file)?;
            }
            Ok(())
        });
        let flags = fcntl_setfl(&self.file, self.flags);
        termios.and(flags).map_err(Into::into)
    }

    pub(crate) fn reapply(&mut self) -> io::Result<()> {
        let mut raw = self.original.clone();
        raw.make_raw();
        tcsetattr(&self.file, OptionalActions::Now, &raw)?;
        fcntl_setfl(&self.file, self.flags | OFlags::NONBLOCK)?;
        Ok(())
    }

    fn write_until(&mut self, bytes: &[u8], deadline: Instant) -> io::Result<usize> {
        loop {
            if Instant::now() >= deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
            match self.file.write(bytes) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    writable(&self.file, deadline)?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => return result,
            }
        }
    }
}

impl ProbeIo for RawTty {
    fn send(&mut self, mut bytes: &[u8], deadline: Instant) -> io::Result<()> {
        while !bytes.is_empty() {
            let count = self.write_until(bytes, deadline)?;
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            bytes = bytes.get(count..).ok_or(io::ErrorKind::InvalidData)?;
        }
        self.flush()
    }

    fn receive(&mut self, bytes: &mut [u8], _deadline: Instant) -> io::Result<usize> {
        // The lifecycle takes its state lock around this call. Never sleep
        // while holding it: a pending input read must not starve frame output.
        self.file.read(bytes)
    }
}

impl Write for RawTty {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_until(bytes, Instant::now() + PROBE_TIMEOUT)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

// Wait for the terminal to drain instead of sleeping: a frame larger than the
// PTY queue (1 KiB on macOS) otherwise pays a fixed sleep per partial write.
#[cfg(not(target_vendor = "apple"))]
fn writable(file: &File, deadline: Instant) -> io::Result<()> {
    use rustix::event::{PollFd, PollFlags, Timespec, poll};
    let timeout = Timespec::try_from(deadline.saturating_duration_since(Instant::now()))
        .map_err(|_| io::ErrorKind::InvalidInput)?;
    match poll(&mut [PollFd::new(file, PollFlags::OUT)], Some(&timeout)) {
        Ok(_) | Err(rustix::io::Errno::INTR) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

// Darwin's poll(2) reports POLLNVAL for /dev/tty; select(2) supports it.
#[cfg(target_vendor = "apple")]
fn writable(file: &File, deadline: Instant) -> io::Result<()> {
    use nix::sys::{
        select::{FD_SETSIZE, FdSet, select},
        time::{TimeVal, TimeValLike},
    };
    use std::os::fd::{AsFd, AsRawFd};
    if usize::try_from(file.as_raw_fd()).ok().is_none_or(|fd| fd >= FD_SETSIZE) {
        // Not representable in an fd_set: retry the write without waiting.
        std::thread::yield_now();
        return Ok(());
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    let mut timeout = TimeVal::microseconds(i64::try_from(remaining.as_micros()).unwrap_or(i64::MAX));
    let mut set = FdSet::new();
    set.insert(file.as_fd());
    match select(None, None, &mut set, None, &mut timeout) {
        Ok(_) | Err(nix::errno::Errno::EINTR) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn empty_input_returns_would_block_without_waiting_under_state() -> io::Result<()> {
        let pty = rustix_openpty::openpty(None, None)?;
        let _master = File::from(pty.controller);
        let mut tty = RawTty::from_file(File::from(pty.user))?;
        let result = tty.receive(&mut [0; 8], Instant::now() + Duration::from_millis(1));
        let restored = tty.restore();
        // The old implementation slept until the deadline and returned zero,
        // keeping lifecycle::active locked for the entire wait.
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        restored
    }
}
