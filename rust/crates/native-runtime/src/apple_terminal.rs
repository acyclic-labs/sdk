//! Terminal readiness on Darwin for every descriptor number.
//!
//! Darwin's poll(2) reports `POLLNVAL` for terminals and kqueue rejects the
//! /dev/tty alias with EINVAL. The remaining wait is select(2), whose default
//! libc entry fails EINVAL for descriptors at or beyond `FD_SETSIZE` (1024).
//! [`Writable`] calls the unlimited entry point (what `_DARWIN_UNLIMITED_SELECT`
//! selects in C) over a descriptor set sized to the descriptor, so no terminal
//! descriptor lacks a working wait.
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::time::Duration;

/// Bits per `fd_set` word: Darwin's `fds_bits` is an array of `i32`.
const WORD_BITS: usize = 32;
/// Darwin's select(2) rejects a timeout of more than 10^8 seconds.
const MAX_SECONDS: u64 = 100_000_000;

/// A reusable writability wait bound to one descriptor number. The set is
/// allocated once; each wait sets one bit and makes one system call.
pub struct Writable {
    words: Vec<u32>,
    fd: usize,
}

impl Writable {
    /// Size the descriptor set for `fd`.
    pub fn new(fd: BorrowedFd<'_>) -> io::Result<Self> {
        let fd = usize::try_from(fd.as_raw_fd()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let length = fd / WORD_BITS + 1;
        let mut words = Vec::new();
        words.try_reserve_exact(length)?;
        words.resize(length, 0);
        Ok(Self { words, fd })
    }

    /// Return once `fd` accepts output, the timeout passes, or a signal
    /// interrupts the wait; the caller retries its write in every case.
    pub fn wait(&mut self, fd: BorrowedFd<'_>, timeout: Duration) -> io::Result<()> {
        let raw = fd.as_raw_fd();
        if usize::try_from(raw).ok() != Some(self.fd) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let nfds = raw.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
        // select(2) rewrites only the words below nfds and clears unready
        // bits, so every other word is still zero: set the one bit again.
        let word = self
            .words
            .get_mut(self.fd / WORD_BITS)
            .ok_or(io::ErrorKind::InvalidInput)?;
        *word = 1 << (self.fd % WORD_BITS);
        let seconds = timeout.as_secs().min(MAX_SECONDS);
        let mut timeval = libc::timeval {
            tv_sec: libc::time_t::try_from(seconds).map_err(|_| io::ErrorKind::InvalidInput)?,
            tv_usec: libc::suseconds_t::try_from(timeout.subsec_micros())
                .map_err(|_| io::ErrorKind::InvalidInput)?,
        };
        let write = self.words.as_mut_ptr().cast::<libc::fd_set>();
        let result = select_unlimited(nfds, std::ptr::null_mut(), write, Some(&mut timeval));
        if result >= 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            Ok(())
        } else {
            Err(error)
        }
    }
}

/// One terminal input descriptor and its lifecycle notification descriptor.
/// Both are level triggered; restored input observes only lifecycle changes.
pub struct Readable {
    words: Vec<u32>,
    input: usize,
    changes: usize,
}

impl Readable {
    /// Allocate the one descriptor set, including descriptors beyond FD_SETSIZE.
    pub fn new(input: BorrowedFd<'_>, changes: BorrowedFd<'_>) -> io::Result<Self> {
        let input = usize::try_from(input.as_raw_fd()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let changes =
            usize::try_from(changes.as_raw_fd()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let length = input.max(changes) / WORD_BITS + 1;
        let mut words = Vec::new();
        words.try_reserve_exact(length)?;
        words.resize(length, 0);
        Ok(Self {
            words,
            input,
            changes,
        })
    }

    /// Wait for input or a lifecycle change without retaining a terminal lock.
    pub fn wait(
        &mut self,
        input: BorrowedFd<'_>,
        changes: BorrowedFd<'_>,
        active: bool,
        timeout: Option<Duration>,
    ) -> io::Result<()> {
        if usize::try_from(input.as_raw_fd()).ok() != Some(self.input)
            || usize::try_from(changes.as_raw_fd()).ok() != Some(self.changes)
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.words.fill(0);
        for fd in [Some(self.changes), active.then_some(self.input)]
            .into_iter()
            .flatten()
        {
            let word = self
                .words
                .get_mut(fd / WORD_BITS)
                .ok_or(io::ErrorKind::InvalidInput)?;
            *word |= 1 << (fd % WORD_BITS);
        }
        let maximum = if active {
            self.input.max(self.changes)
        } else {
            self.changes
        };
        let nfds = libc::c_int::try_from(maximum)
            .ok()
            .and_then(|fd| fd.checked_add(1))
            .ok_or(io::ErrorKind::InvalidInput)?;
        let mut timeval = timeout
            .map(|timeout| {
                Ok::<_, io::Error>(libc::timeval {
                    tv_sec: libc::time_t::try_from(timeout.as_secs().min(MAX_SECONDS))
                        .map_err(|_| io::ErrorKind::InvalidInput)?,
                    tv_usec: libc::suseconds_t::try_from(timeout.subsec_micros())
                        .map_err(|_| io::ErrorKind::InvalidInput)?,
                })
            })
            .transpose()?;
        let result = select_unlimited(
            nfds,
            self.words.as_mut_ptr().cast(),
            std::ptr::null_mut(),
            timeval.as_mut(),
        );
        if result >= 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            Ok(())
        } else {
            Err(error)
        }
    }
}

#[allow(
    unsafe_code,
    reason = "the unlimited select entry point reads and writes only the owned set below nfds"
)]
fn select_unlimited(
    nfds: libc::c_int,
    read: *mut libc::fd_set,
    write: *mut libc::fd_set,
    timeout: Option<&mut libc::timeval>,
) -> libc::c_int {
    // SAFETY: `select$DARWIN_EXTSN` is libSystem's select(2) without the
    // FD_SETSIZE check (the symbol C code binds under
    // _DARWIN_UNLIMITED_SELECT); the signature is select(2)'s.
    unsafe extern "C" {
        #[link_name = "select$DARWIN_EXTSN"]
        fn select(
            nfds: libc::c_int,
            read: *mut libc::fd_set,
            write: *mut libc::fd_set,
            error: *mut libc::fd_set,
            timeout: *mut libc::timeval,
        ) -> libc::c_int;
    }
    // SAFETY: every non-null set covers nfds bits of owned, initialized words.
    // Readable and Writable size each set before passing it here. The sets
    // are exclusive; a present timeout is live and exclusive, while null
    // requests the native indefinite wait.
    let timeout = timeout.map_or(std::ptr::null_mut(), |timeout| timeout as *mut _);
    unsafe { select(nfds, read, write, std::ptr::null_mut(), timeout) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;

    /// Writability at fd 1500, past `FD_SETSIZE`, plus the default entry
    /// point's EINVAL there as the negative control.
    #[test]
    fn waits_at_descriptors_beyond_fd_setsize() -> io::Result<()> {
        use rustix::process::{Resource, getrlimit, setrlimit};
        let mut limit = getrlimit(Resource::Nofile);
        if limit.maximum.is_some_and(|maximum| maximum <= 1500) {
            eprintln!("skipped: RLIMIT_NOFILE maximum cannot reach fd 1500");
            return Ok(());
        }
        if limit.current.is_some_and(|current| current <= 1500) {
            limit.current = Some(limit.maximum.map_or(2048, |maximum| maximum.min(2048)));
            setrlimit(Resource::Nofile, limit)?;
        }
        let (_read, write) = std::io::pipe()?;
        let high = rustix::io::fcntl_dupfd_cloexec(&write, 1500)?;
        let mut wait = Writable::new(high.as_fd())?;
        for _ in 0..2 {
            wait.wait(high.as_fd(), Duration::from_secs(5))?;
        }
        let mut words = [0u32; 1500 / WORD_BITS + 1];
        if let Some(word) = words.get_mut(1500 / WORD_BITS) {
            *word = 1 << (1500 % WORD_BITS);
        }
        let mut timeval = libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        };
        assert_eq!(
            default_select(1501, words.as_mut_ptr().cast(), &mut timeval),
            Err(Some(libc::EINVAL))
        );
        // A different descriptor number is refused, never waited on.
        assert_eq!(
            wait.wait(std::io::stdin().as_fd(), Duration::ZERO)
                .map_err(|error| error.kind()),
            Err(io::ErrorKind::InvalidInput)
        );
        Ok(())
    }

    #[allow(
        unsafe_code,
        reason = "calls the default libc select for the negative control"
    )]
    fn default_select(
        nfds: libc::c_int,
        write: *mut libc::fd_set,
        timeout: &mut libc::timeval,
    ) -> Result<libc::c_int, Option<i32>> {
        // SAFETY: `write` covers nfds bits of an owned set; other sets are null.
        let result = unsafe {
            libc::select(
                nfds,
                std::ptr::null_mut(),
                write,
                std::ptr::null_mut(),
                timeout,
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error().raw_os_error())
        } else {
            Ok(result)
        }
    }
}
