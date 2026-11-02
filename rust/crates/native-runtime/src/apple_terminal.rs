//! Terminal readiness on Darwin for every descriptor number.
//!
//! Darwin's poll(2) reports `POLLNVAL` for terminals and kqueue rejects the
//! /dev/tty alias with EINVAL. The remaining wait is select(2), whose default
//! libc entry fails EINVAL for descriptors at or beyond `FD_SETSIZE` (1024).
//! [`Readiness`] calls the unlimited entry point (what `_DARWIN_UNLIMITED_SELECT`
//! selects in C) over a descriptor set sized to the descriptor, so no terminal
//! descriptor lacks a working wait.
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::time::Duration;

/// Bits per `fd_set` word: Darwin's `fds_bits` is an array of `i32`.
const WORD_BITS: usize = 32;
/// Darwin's select(2) rejects a timeout of more than 10^8 seconds.
const MAX_SECONDS: u64 = 100_000_000;

/// A reusable read or write wait bound to original descriptor numbers. The
/// set is allocated once; each wait touches only the selected source bits.
pub struct Readiness<const N: usize> {
    words: Vec<u32>,
    fds: [usize; N],
    nfds: libc::c_int,
    reading: bool,
}

impl<const N: usize> Readiness<N> {
    /// Size one read set for the original sources, including an optional
    /// lifecycle wake source selected by a later wait.
    pub fn readable(fds: [BorrowedFd<'_>; N]) -> io::Result<Self> {
        Self::new(fds, true)
    }

    /// Size one write set for the original sources.
    pub fn writable(fds: [BorrowedFd<'_>; N]) -> io::Result<Self> {
        Self::new(fds, false)
    }

    fn new(fds: [BorrowedFd<'_>; N], reading: bool) -> io::Result<Self> {
        let maximum = fds
            .iter()
            .map(AsRawFd::as_raw_fd)
            .max()
            .ok_or(io::ErrorKind::InvalidInput)?;
        let nfds = maximum.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
        let length =
            usize::try_from(maximum).map_err(|_| io::ErrorKind::InvalidInput)? / WORD_BITS + 1;
        let mut numbers = [0; N];
        for (number, fd) in numbers.iter_mut().zip(fds) {
            *number = usize::try_from(fd.as_raw_fd()).map_err(|_| io::ErrorKind::InvalidInput)?;
        }
        let mut words = Vec::new();
        words.try_reserve_exact(length)?;
        words.resize(length, 0);
        Ok(Self {
            words,
            fds: numbers,
            nfds,
            reading,
        })
    }

    /// Wait for the selected original sources, timeout or interruption. A
    /// `None` source is disabled for this wait. Replaced descriptors refuse.
    /// False means the timeout elapsed; readiness or interruption returns true.
    pub fn wait(
        &mut self,
        fds: [Option<BorrowedFd<'_>>; N],
        timeout: Option<Duration>,
    ) -> io::Result<bool> {
        for (original, fd) in self.fds.iter().zip(fds) {
            if fd.is_some_and(|fd| usize::try_from(fd.as_raw_fd()).ok() != Some(*original)) {
                return Err(io::ErrorKind::InvalidInput.into());
            }
        }
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
        for (number, fd) in self.fds.iter().zip(fds) {
            if fd.is_some() {
                self.words[number / WORD_BITS] |= 1 << (number % WORD_BITS);
            }
        }
        let selected = self.words.as_mut_ptr().cast::<libc::fd_set>();
        let (read, write) = if self.reading {
            (selected, std::ptr::null_mut())
        } else {
            (std::ptr::null_mut(), selected)
        };
        let result = select_unlimited(self.nfds, read, write, timeval.as_mut());
        // Clear only our original source bits, including previously-ready
        // sources disabled on the next wait. No descriptor-set scan.
        for number in self.fds {
            self.words[number / WORD_BITS] &= !(1 << (number % WORD_BITS));
        }
        if result >= 0 {
            return Ok(result != 0);
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            Ok(true)
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
    // SAFETY: exactly one of read/write points at nfds.div_ceil(32) or more
    // owned, initialized words, sized from the highest original descriptor.
    // Other sets are null. A present timeout is live and exclusive; null waits indefinitely.
    let timeout = timeout.map_or(std::ptr::null_mut(), |timeout| timeout as *mut _);
    unsafe { select(nfds, read, write, std::ptr::null_mut(), timeout) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
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
        let mut wait = Readiness::writable([high.as_fd()])?;
        for _ in 0..2 {
            wait.wait([Some(high.as_fd())], Some(Duration::from_secs(5)))?;
        }
        let (reader, mut writer) = std::io::pipe()?;
        let mut input = std::fs::File::from(rustix::io::fcntl_dupfd_cloexec(&reader, 1500)?);
        let (changed, mut notify) = std::io::pipe()?;
        let mut readable = Readiness::readable([input.as_fd(), changed.as_fd()])?;
        writer.write_all(b"x")?;
        assert!(readable.wait([Some(input.as_fd()), None], Some(Duration::ZERO))?);
        // A ready terminal disabled for suspension cannot leave a stale bit.
        assert!(!readable.wait([None, Some(changed.as_fd())], Some(Duration::ZERO))?);
        notify.write_all(b"x")?;
        assert!(readable.wait([None, Some(changed.as_fd())], Some(Duration::ZERO))?);
        let mut byte = [0];
        input.read_exact(&mut byte)?;
        assert_eq!(&byte, b"x");
        drop(writer);
        assert!(readable.wait([Some(input.as_fd()), None], Some(Duration::ZERO))?);
        assert_eq!(input.read(&mut byte)?, 0);
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
            wait.wait([Some(std::io::stdin().as_fd())], Some(Duration::ZERO))
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
        timeout: Option<&mut libc::timeval>,
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
