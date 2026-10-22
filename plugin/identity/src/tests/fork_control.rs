use super::*;
use std::io::Read as _;
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, ExitStatus};
use std::time::Instant;

#[allow(
    unsafe_code,
    reason = "the fork-inheritance control needs async-signal-safe FFI with live owned descriptors"
)]
fn pipe() -> io::Result<(File, File)> {
    let mut descriptors = [-1; 2];
    // SAFETY: descriptors provides two writable descriptor slots. CLOEXEC
    // keeps the fixture pipes out of the executed program.
    if unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful pipe2 returned two distinct owned descriptors;
    // neither is owned elsewhere before these Files take ownership.
    Ok(unsafe {
        (
            File::from_raw_fd(descriptors[0]),
            File::from_raw_fd(descriptors[1]),
        )
    })
}

#[allow(
    unsafe_code,
    reason = "the fork-inheritance control needs async-signal-safe FFI with live owned descriptors"
)]
fn await_child(reader: &mut File) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "pre_exec barrier"));
        }
        let mut descriptor = libc::pollfd {
            fd: reader.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: descriptor is valid for one pollfd and reader owns its
        // descriptor throughout this bounded readiness wait.
        let ready = unsafe {
            libc::poll(
                &mut descriptor,
                1,
                libc::c_int::try_from(remaining.as_millis().max(1)).map_err(io::Error::other)?,
            )
        };
        if ready < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if ready == 0 {
            continue;
        }
        let mut byte = [0];
        reader.read_exact(&mut byte)?;
        return if byte == [1] {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pre_exec signal",
            ))
        };
    }
}

pub(super) struct ForkControl {
    ready: File,
    release: File,
    spawning: Option<std::thread::JoinHandle<io::Result<Child>>>,
}

impl ForkControl {
    pub(super) fn finish(&mut self) -> Result<ExitStatus, String> {
        // Keep both parent endpoints alive until spawn has returned:
        // pre_exec closes their raw descriptor numbers in the child.
        // This also works if unwinding starts before the child has forked.
        let released = self.release.write_all(&[1]);
        let reaped = self.join_and_reap();
        let status = reaped?;
        released.map_err(|error| format!("release child to exec: {error}"))?;
        Ok(status)
    }

    fn join_and_reap(&mut self) -> Result<ExitStatus, String> {
        let spawning = self
            .spawning
            .take()
            .ok_or_else(|| "barrier controller already joined".to_owned())?;
        let mut child = spawning
            .join()
            .map_err(|_| "barrier controller panicked".to_owned())?
            .map_err(|error| format!("exec true: {error}"))?;
        match child.wait() {
            Ok(status) => Ok(status),
            Err(error) => {
                let killed = child.kill();
                let reaped = child.wait();
                Err(format!(
                    "wait true: {error}; cleanup kill: {killed:?}; cleanup wait: {reaped:?}"
                ))
            }
        }
    }
}

impl Drop for ForkControl {
    fn drop(&mut self) {
        if self.spawning.is_some()
            && let Err(error) = self.finish()
        {
            // Cleanup cannot panic during an existing unwind. Normal
            // completion reports errors through finish's Result.
            let _ = writeln!(io::stderr(), "fork control cleanup failed: {error}");
        }
    }
}

impl ForkControl {
    pub(super) fn await_ready(&mut self) -> io::Result<()> {
        await_child(&mut self.ready)
    }

    #[allow(
        unsafe_code,
        reason = "the fork-inheritance control needs async-signal-safe FFI with live owned descriptors"
    )]
    pub(super) fn start() -> io::Result<Self> {
        let (ready, child_ready) = pipe()?;
        let (child_release, release) = pipe()?;
        let mut control = ForkControl {
            ready,
            release,
            spawning: None,
        };
        let unused_ready_fd = control.ready.as_raw_fd();
        let unused_release_fd = control.release.as_raw_fd();
        // Command::spawn waits for exec, so it cannot run on the barrier controller.
        control.spawning = Some(
            std::thread::Builder::new()
                .name("identity-store-fork-control".to_owned())
                .spawn(move || {
                    let ready_fd = child_ready.as_raw_fd();
                    let release_fd = child_release.as_raw_fd();
                    let mut command = Command::new("/bin/true");
                    // SAFETY: both pipe ends remain owned until spawn returns. The
                    // child only uses stack bytes and async-signal-safe libc calls:
                    // no allocation, Rust locks, destructors, or runtime interaction.
                    unsafe {
                        command.pre_exec(move || {
                            // CLOEXEC does not act until exec. Close the unused ends
                            // now so parent death gives this reader EOF rather than
                            // leaving the child holding its own release writer.
                            if libc::close(unused_ready_fd) != 0
                                || libc::close(unused_release_fd) != 0
                            {
                                libc::_exit(124);
                            }
                            let mut byte = 1_u8;
                            loop {
                                let count = libc::write(ready_fd, (&raw const byte).cast(), 1);
                                if count == 1 {
                                    break;
                                }
                                if count >= 0 || *libc::__errno_location() != libc::EINTR {
                                    libc::_exit(125);
                                }
                            }
                            loop {
                                let count = libc::read(release_fd, (&raw mut byte).cast(), 1);
                                if count == 1 && byte == 1 {
                                    return Ok(());
                                }
                                if count >= 0 || *libc::__errno_location() != libc::EINTR {
                                    libc::_exit(126);
                                }
                            }
                        });
                    }
                    let child = command.spawn();
                    drop(child_ready);
                    drop(child_release);
                    child
                })?,
        );

        Ok(control)
    }
}
