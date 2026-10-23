use nix::mount::{MntFlags, MsFlags};
use std::{
    ffi::CStr,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    os::{
        fd::{AsRawFd, OwnedFd},
        unix::{ffi::OsStrExt, fs::PermissionsExt, net::UnixStream},
    },
    path::Path,
    process::{Child, Command, Stdio},
};
mod reply;
use reply::receive_device;
#[cfg(test)]
mod tests;

// These controls are owned by the existing FuseSessionState lock. A received
// device is published there before helper completion or the FUSE handshake.
#[derive(Default)]
enum MountPhase {
    #[default]
    Detached,
    Mounted,
    // The request succeeded; only device acknowledgment can finish retirement.
    Unmounting,
}

#[derive(Default)]
pub(super) struct Mount {
    helper: Option<Child>,
    program: Option<&'static CStr>,
    phase: MountPhase,
    #[cfg(test)]
    detachment_poll: Option<Box<dyn FnMut(i32) -> io::Result<bool> + Send>>,
}

impl Mount {
    pub(super) fn open_into(
        &mut self,
        path: &Path,
        writable: bool,
        program: impl FnOnce() -> Result<&'static CStr, super::NativeMountError>,
        device: &mut Option<OwnedFd>,
    ) -> Result<(), super::NativeMountError> {
        let mode = File::open(path)?.metadata()?.permissions().mode();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/fuse")?;
        let options = format!(
            "fd={},rootmode={mode:o},user_id={},group_id={},default_permissions",
            file.as_raw_fd(),
            nix::unistd::getuid(),
            nix::unistd::getgid()
        );
        let mut flags = MsFlags::MS_NODEV | MsFlags::MS_NOSUID | MsFlags::MS_NOATIME;
        if !writable {
            flags |= MsFlags::MS_RDONLY;
        }
        match nix::mount::mount(
            Some("acyclic-fs"),
            path,
            Some("fuse"),
            flags,
            Some(options.as_str()),
        ) {
            Ok(()) => {
                *device = Some(file.into());
                self.phase = MountPhase::Mounted;
                return Ok(());
            }
            Err(nix::errno::Errno::EPERM) => {}
            Err(error) => {
                return Err(io::Error::other(format!(
                    "Error calling mount() at {:?} with {options:?} and flags={flags:?}: {error}",
                    path.as_os_str()
                ))
                .into());
            }
        }
        drop(file);
        let program = program()?;
        self.program = Some(program);
        let (receive, child) = UnixStream::pair()?;
        let mut diagnostic = File::from(
            rustix::fs::memfd_create(c"acyclic-fusermount", rustix::fs::MemfdFlags::CLOEXEC)
                .map_err(io::Error::from)?,
        );
        let options = if writable {
            "fsname=acyclic-fs,default_permissions,exec,noatime"
        } else {
            "fsname=acyclic-fs,default_permissions,exec,noatime,ro"
        };
        self.helper = Some(spawn_mount_helper(
            program,
            path,
            options,
            child,
            &diagnostic,
        )?);
        let received = receive_device(&receive);
        drop(receive);
        if let Ok(fd) = received {
            *device = Some(fd);
            self.phase = MountPhase::Mounted;
        }
        self.helper
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?
            .wait()?;
        self.helper.take();
        if matches!(self.phase, MountPhase::Detached) {
            diagnostic.seek(SeekFrom::Start(0))?;
            let mut text = Vec::new();
            diagnostic.read_to_end(&mut text)?;
            let text = String::from_utf8_lossy(&text).into_owned();
            let kind = if text.contains("only allowed if 'user_allow_other' is set") {
                io::ErrorKind::PermissionDenied
            } else {
                io::ErrorKind::Other
            };
            return Err(io::Error::new(kind, text).into());
        }
        Ok(())
    }

    pub(super) fn stop(&mut self, path: &CStr, device: Option<&OwnedFd>) -> io::Result<()> {
        if let Some(child) = self.helper.as_mut() {
            if child.try_wait()?.is_none() {
                child.kill()?;
            }
            child.wait()?;
            self.helper.take();
        }
        self.attempt_unmount(
            |mount| device.map_or(Ok(false), |device| mount.device_detached(device)),
            |mount| {
                unmount_with(
                    |flags| nix::mount::umount2(path, flags),
                    || mount.unmount_helper(path),
                )
            },
        )
    }

    fn unmount_helper(&mut self, path: &CStr) -> io::Result<()> {
        let program = self.program.unwrap_or(c"fusermount3");
        let mut command = helper_command(program);
        command
            .args(["-u", "-q", "-z", "--"])
            .arg(std::ffi::OsStr::from_bytes(path.to_bytes()));
        self.helper = Some(command.spawn()?);
        drop(command);
        let status = self
            .helper
            .as_mut()
            .ok_or(io::ErrorKind::NotFound)?
            .wait()?;
        self.helper.take();
        if !status.success() {
            // A refusal admits no device-acknowledgment wait.
            return Err(io::ErrorKind::Other.into());
        }
        // Helper exit alone cannot establish device destruction.
        Ok(())
    }

    fn attempt_unmount(
        &mut self,
        detached: impl FnOnce(&mut Self) -> io::Result<bool>,
        unmount: impl FnOnce(&mut Self) -> io::Result<()>,
    ) -> io::Result<()> {
        if matches!(self.phase, MountPhase::Detached) {
            return Ok(());
        }
        if detached(self)? {
            return Ok(());
        }
        if matches!(self.phase, MountPhase::Unmounting) {
            return Ok(());
        }
        unmount(self)?;
        self.phase = MountPhase::Unmounting;
        #[cfg(test)]
        if tests::forget_pending() {
            self.phase = MountPhase::Detached;
        }
        Ok(())
    }

    // Only a device acknowledgment can finish an accepted unmount. A refused
    // request remains mounted and permits another explicitly admitted attempt.
    pub(super) fn device_detached(&mut self, device: &OwnedFd) -> io::Result<bool> {
        self.poll_detachment(device, 0)
    }

    pub(super) fn wait_device_detached(&mut self, device: &OwnedFd) -> io::Result<bool> {
        // Unmount can return before the last kernel reference releases the
        // connection. Wait for its acknowledgment instead of treating a probe
        // in that interval as a failed retirement.
        self.poll_detachment(device, -1)
    }

    fn poll_detachment(&mut self, device: &OwnedFd, timeout: i32) -> io::Result<bool> {
        #[cfg(test)]
        if let Some(poll) = self.detachment_poll.as_mut() {
            let result = poll(timeout);
            return self.observe_detachment(|| result);
        }
        self.observe_detachment(|| {
            let mut poll = libc::pollfd {
                fd: device.as_raw_fd(),
                events: 0,
                revents: 0,
            };
            // SAFETY: one initialized record; the state owner retains its descriptor.
            if unsafe { libc::poll(&mut poll, 1, timeout) } < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(poll.revents & libc::POLLERR != 0)
        })
    }

    fn observe_detachment(&mut self, query: impl FnOnce() -> io::Result<bool>) -> io::Result<bool> {
        let detached = query()?;
        if detached {
            self.phase = MountPhase::Detached;
        }
        Ok(detached)
    }
    pub(super) fn pending(&self) -> bool {
        !matches!(self.phase, MountPhase::Detached) || self.helper.is_some()
    }

    pub(super) fn needs_unmount(&self) -> bool {
        self.helper.is_some() || matches!(self.phase, MountPhase::Mounted)
    }

    #[cfg(test)]
    pub(super) fn accepted_unmount(
        poll: impl FnMut(i32) -> io::Result<bool> + Send + 'static,
    ) -> io::Result<Self> {
        let mut mount = Self {
            phase: MountPhase::Mounted,
            detachment_poll: Some(Box::new(poll)),
            ..Self::default()
        };
        mount.attempt_unmount(|_| Ok(false), |_| Ok(()))?;
        Ok(mount)
    }
}

fn unmount_with(
    kernel: impl FnOnce(MntFlags) -> nix::Result<()>,
    helper: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    #[cfg(test)]
    let old = tests::old_unmount();
    #[cfg(not(test))]
    let old = false;
    let flags = if old {
        MntFlags::MNT_DETACH
    } else {
        MntFlags::empty()
    };
    match kernel(flags) {
        Ok(()) => Ok(()),
        Err(error) if old || error == nix::errno::Errno::EPERM => helper(),
        Err(error) => Err(error.into()),
    }
}

fn helper_command(program: &CStr) -> Command {
    let mut command = Command::new(std::ffi::OsStr::from_bytes(program.to_bytes()));
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

fn spawn_mount_helper(
    program: &CStr,
    path: &Path,
    options: &str,
    child: UnixStream,
    diagnostic: &File,
) -> io::Result<Child> {
    let mut command = helper_command(program);
    command
        .args(["-o", options, "--"])
        .arg(path)
        .env("_FUSE_COMMFD", "0")
        .stdin(Stdio::from(OwnedFd::from(child)))
        .stderr(Stdio::from(diagnostic.try_clone()?));
    #[cfg(test)]
    if tests::force_fork() {
        use std::os::unix::process::CommandExt;
        // SAFETY: the control makes no child-side calls. Registering even this
        // empty hook restores the dependency's original fork selection.
        unsafe {
            command.pre_exec(|| Ok(()));
        }
    }
    // Drop Command before returning: it retains its configured stdin, which
    // otherwise suppresses EOF when the helper exits without sending a device.
    command.spawn()
}
