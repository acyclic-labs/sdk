//! Readiness of owned pipes, direct-child exit and explicit control changes.
#[cfg(unix)]
use super::*;
#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};

#[cfg(unix)]
pub(super) struct CaptureWait<'a> {
    exit: exit_wait::ExitWatch<'a>,
}

#[cfg(unix)]
impl<'a> CaptureWait<'a> {
    pub(super) fn new(
        id: libc::id_t,
        meter: Option<&'a dyn crate::NativeReadMeter>,
    ) -> io::Result<Self> {
        let exit = exit_wait::open(id, meter)?;
        Ok(Self { exit })
    }

    pub(super) fn wait(
        &self,
        pipes: [Option<RawFd>; 3],
        wake: Option<&[super::wake::ProcessWake]>,
        exited: bool,
        deadline: Instant,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        // No heap allocation or per-pipe worker. Closed pipes and an observed
        // leader are omitted so EOF/exit cannot create a permanent ready loop.
        if wake.is_some_and(|wakes| wakes.len() > 2) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut ready = [
            libc::pollfd {
                fd: pipes[0].unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: pipes[1].unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: pipes[2].unwrap_or(-1),
                events: libc::POLLOUT,
                revents: 0,
            },
            libc::pollfd {
                fd: if exited { -1 } else { self.exit.as_raw_fd() },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake
                    .and_then(|wakes| wakes.first())
                    .map_or(-1, super::wake::ProcessWake::raw),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake
                    .and_then(|wakes| wakes.get(1))
                    .map_or(-1, super::wake::ProcessWake::raw),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(());
            }
            let milliseconds =
                i32::try_from(remaining.as_nanos().div_ceil(1_000_000)).unwrap_or(i32::MAX);
            let (result, recorded) =
                pipe_operation_attempt(meter, crate::NativeOperationKind::Read, || {
                    // SAFETY: this fixed writable array contains exclusively
                    // retained descriptors and lives throughout the bounded wait.
                    let count = unsafe {
                        libc::poll(
                            ready.as_mut_ptr(),
                            ready.len().try_into().map_err(io::Error::other)?,
                            milliseconds,
                        )
                    };
                    if count < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(count)
                    }
                })?;
            recorded?;
            match result {
                Ok(_) => {
                    if ready
                        .iter()
                        .any(|entry| entry.revents & libc::POLLNVAL != 0)
                    {
                        return Err(io::Error::other(
                            "capture readiness lost a retained descriptor",
                        ));
                    }
                    for wake in wake.into_iter().flatten() {
                        wake.consume()?;
                    }
                    return Ok(());
                }
                Err(cause) if cause.kind() == io::ErrorKind::Interrupted => {}
                Err(cause) => return Err(cause),
            }
        }
    }
}
