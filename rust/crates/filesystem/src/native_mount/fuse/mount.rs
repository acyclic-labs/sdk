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
pub(super) struct Mount {
    helper: Option<Child>,
    program: Option<&'static CStr>,
    pending: bool,
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
                self.pending = true;
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
            self.pending = true;
        }
        self.helper
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?
            .wait()?;
        self.helper.take();
        if !self.pending {
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
                    || {
                        let program = mount.program.unwrap_or(c"fusermount3");
                        let mut command = helper_command(program);
                        command
                            .args(["-u", "-q", "-z", "--"])
                            .arg(std::ffi::OsStr::from_bytes(path.to_bytes()));
                        mount.helper = Some(command.spawn()?);
                        drop(command);
                        mount
                            .helper
                            .as_mut()
                            .ok_or(io::ErrorKind::NotFound)?
                            .wait()?;
                        mount.helper.take();
                        // Helper exit alone cannot establish device destruction.
                        Ok(())
                    },
                )
            },
        )
    }

    fn attempt_unmount(
        &mut self,
        detached: impl FnOnce(&mut Self) -> io::Result<bool>,
        unmount: impl FnOnce(&mut Self) -> io::Result<()>,
    ) -> io::Result<()> {
        if !self.pending {
            return Ok(());
        }
        if detached(self)? {
            return Ok(());
        }
        unmount(self)?;
        #[cfg(test)]
        if tests::forget_pending() {
            self.pending = false;
        }
        Ok(())
    }

    // This is the sole production transition that clears pending. A failed
    // helper or an unsuccessful device check must leave the next retry enabled.
    pub(super) fn device_detached(&mut self, device: &OwnedFd) -> io::Result<bool> {
        self.observe_detachment(|| {
            let mut poll = libc::pollfd {
                fd: device.as_raw_fd(),
                events: 0,
                revents: 0,
            };
            // SAFETY: one initialized record; the state owner retains its descriptor.
            if unsafe { libc::poll(&mut poll, 1, 0) } < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(poll.revents & libc::POLLERR != 0)
        })
    }

    fn observe_detachment(&mut self, query: impl FnOnce() -> io::Result<bool>) -> io::Result<bool> {
        let detached = query()?;
        if detached {
            self.pending = false;
        }
        Ok(detached)
    }
    pub(super) fn pending(&self) -> bool {
        self.pending || self.helper.is_some()
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
