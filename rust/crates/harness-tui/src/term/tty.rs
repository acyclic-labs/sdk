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
    #[cfg(target_vendor = "apple")]
    readiness: Option<darwin::Readiness>,
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
            #[cfg(target_vendor = "apple")]
            readiness: None,
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
        write_until(self, bytes, deadline)
    }
}

/// Nonblocking device primitives the terminal write loop is built from.
pub(crate) trait NonblockingWrite {
    fn try_write(&mut self, bytes: &[u8]) -> io::Result<usize>;
    /// Return once a write can make progress, or at the deadline.
    fn wait_writable(&mut self, deadline: Instant) -> io::Result<()>;
}

/// Retry EINTR at once and wait for writability after EAGAIN: the loop never
/// sleeps a fixed interval and never spins on a full queue.
pub(crate) fn write_until(
    device: &mut impl NonblockingWrite,
    bytes: &[u8],
    deadline: Instant,
) -> io::Result<usize> {
    loop {
        if Instant::now() >= deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }
        match device.try_write(bytes) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                device.wait_writable(deadline)?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

impl NonblockingWrite for RawTty {
    fn try_write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.write(bytes)
    }

    #[cfg(not(target_vendor = "apple"))]
    fn wait_writable(&mut self, deadline: Instant) -> io::Result<()> {
        use rustix::event::{PollFd, PollFlags, Timespec, poll};
        let timeout = Timespec::try_from(deadline.saturating_duration_since(Instant::now()))
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        match poll(&mut [PollFd::new(&self.file, PollFlags::OUT)], Some(&timeout)) {
            Ok(_) | Err(rustix::io::Errno::INTR) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(target_vendor = "apple")]
    fn wait_writable(&mut self, deadline: Instant) -> io::Result<()> {
        if self.readiness.is_none() {
            self.readiness = Some(darwin::Readiness::register(&self.file)?);
        }
        match &mut self.readiness {
            Some(readiness) => readiness.wait(&self.file, deadline),
            None => Err(io::ErrorKind::NotConnected.into()),
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

#[cfg(target_vendor = "apple")]
mod darwin {
    use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};
    use nix::sys::select::{FD_SETSIZE, FdSet, select};
    use nix::sys::time::{TimeSpec, TimeVal, TimeValLike};
    use std::fs::File;
    use std::io;
    use std::os::fd::{AsFd, AsRawFd};
    use std::time::Instant;

    /// Writability of the terminal device. kqueue has no descriptor limit;
    /// Darwin's kqueue rejects only the /dev/tty alias (EINVAL), whose
    /// readiness select(2) reports when the descriptor fits an fd_set.
    /// (Darwin's poll(2) reports POLLNVAL for terminals.)
    pub(super) enum Readiness {
        Kqueue(Kqueue),
        Select,
    }

    fn event(file: &File, flags: EvFlags) -> io::Result<KEvent> {
        let ident = usize::try_from(file.as_raw_fd()).map_err(|_| io::ErrorKind::InvalidInput)?;
        Ok(KEvent::new(ident, EventFilter::EVFILT_WRITE, flags, FilterFlag::empty(), 0, 0))
    }

    impl Readiness {
        pub(super) fn register(file: &File) -> io::Result<Self> {
            let queue = Kqueue::new()?;
            // Level-triggered: a wait returns at once while the queue has room.
            match queue.kevent(&[event(file, EvFlags::EV_ADD)?], &mut [], Some(*TimeSpec::from_duration(std::time::Duration::ZERO).as_ref())) {
                Ok(_) => Ok(Self::Kqueue(queue)),
                Err(nix::errno::Errno::EINVAL)
                    if usize::try_from(file.as_raw_fd()).is_ok_and(|fd| fd < FD_SETSIZE) =>
                {
                    Ok(Self::Select)
                }
                Err(error) => Err(error.into()),
            }
        }

        pub(super) fn wait(&mut self, file: &File, deadline: Instant) -> io::Result<()> {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let waited = match self {
                Self::Kqueue(queue) => {
                    let mut ready = [event(file, EvFlags::empty())?];
                    queue
                        .kevent(&[], &mut ready, Some(*TimeSpec::from_duration(remaining).as_ref()))
                        .map(drop)
                }
                Self::Select => {
                    let mut timeout = TimeVal::microseconds(
                        i64::try_from(remaining.as_micros()).unwrap_or(i64::MAX),
                    );
                    let mut set = FdSet::new();
                    set.insert(file.as_fd());
                    select(None, None, &mut set, None, &mut timeout).map(drop)
                }
            };
            match waited {
                Ok(()) | Err(nix::errno::Errno::EINTR) => Ok(()),
                Err(error) => Err(error.into()),
            }
        }
    }
}

#[cfg(test)]
#[path = "tty_tests.rs"]
mod tests;
