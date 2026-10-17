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
        let termios = tcsetattr(&self.file, OptionalActions::Now, &self.original);
        #[cfg(target_os = "macos")]
        let termios = termios.and_then(|()| {
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

    /// One nonblocking read; the caller owns readiness and retry policy.
    pub(crate) fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.file.read(bytes)
    }

    /// A descriptor sharing this terminal's open file description, used only
    /// for reactor readiness.
    pub(crate) fn try_clone_file(&self) -> io::Result<File> {
        self.file.try_clone()
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
        super::counters::wrote();
        self.file.write(bytes)
    }

    #[cfg(not(target_vendor = "apple"))]
    fn wait_writable(&mut self, deadline: Instant) -> io::Result<()> {
        use rustix::event::{PollFd, PollFlags, Timespec, poll};
        super::counters::waited();
        let timeout = Timespec::try_from(deadline.saturating_duration_since(Instant::now()))
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        match poll(
            &mut [PollFd::new(&self.file, PollFlags::OUT)],
            Some(&timeout),
        ) {
            Ok(_) | Err(rustix::io::Errno::INTR) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(target_vendor = "apple")]
    fn wait_writable(&mut self, deadline: Instant) -> io::Result<()> {
        super::counters::waited();
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
        self.read(bytes)
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
    use acyclic_native_runtime::terminal::Writable;
    use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};
    use nix::sys::time::TimeSpec;
    use std::fs::File;
    use std::io;
    use std::os::fd::{AsFd, AsRawFd};
    use std::time::Instant;

    /// Writability of the terminal device, for every descriptor number.
    /// kqueue serves every terminal but the /dev/tty alias, which it rejects
    /// with EINVAL; that alias waits in select(2) through its unlimited entry
    /// point, so no descriptor is left without a working wait. (Darwin's
    /// poll(2) reports POLLNVAL for terminals.)
    pub(super) enum Readiness {
        Kqueue(Kqueue),
        Select(Writable),
    }

    fn event(file: &File, flags: EvFlags) -> io::Result<KEvent> {
        let ident = usize::try_from(file.as_raw_fd()).map_err(|_| io::ErrorKind::InvalidInput)?;
        Ok(KEvent::new(
            ident,
            EventFilter::EVFILT_WRITE,
            flags,
            FilterFlag::empty(),
            0,
            0,
        ))
    }

    impl Readiness {
        pub(super) fn register(file: &File) -> io::Result<Self> {
            let queue = Kqueue::new()?;
            // Level-triggered: a wait returns at once while the queue has room.
            let zero = *TimeSpec::from_duration(std::time::Duration::ZERO).as_ref();
            match queue.kevent(&[event(file, EvFlags::EV_ADD)?], &mut [], Some(zero)) {
                Ok(_) => Ok(Self::Kqueue(queue)),
                Err(nix::errno::Errno::EINVAL) => Self::select(file),
                Err(error) => Err(error.into()),
            }
        }

        /// The /dev/tty alias's wait; also what tests force on any terminal.
        pub(super) fn select(file: &File) -> io::Result<Self> {
            Ok(Self::Select(Writable::new(file.as_fd())?))
        }

        pub(super) fn wait(&mut self, file: &File, deadline: Instant) -> io::Result<()> {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self {
                Self::Kqueue(queue) => {
                    let mut ready = [event(file, EvFlags::empty())?];
                    let timeout = *TimeSpec::from_duration(remaining).as_ref();
                    match queue.kevent(&[], &mut ready, Some(timeout)) {
                        Ok(_) | Err(nix::errno::Errno::EINTR) => Ok(()),
                        Err(error) => Err(error.into()),
                    }
                }
                Self::Select(writable) => writable.wait(file.as_fd(), remaining),
            }
        }
    }
}

#[cfg(test)]
#[path = "tty_tests.rs"]
mod tests;
