use nix::mount::{MntFlags, MsFlags};
use std::{
    ffi::CStr,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    os::{
        fd::{AsRawFd, BorrowedFd, OwnedFd},
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
    exit_action: Option<acyclic_native_runtime::ProcessExitAction>,
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
        fence: &File,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), super::NativeMountError> {
        let program = program()?;
        self.program = Some(program);
        let mut cleanup = acyclic_native_runtime::ProcessCommand::new(std::ffi::OsStr::from_bytes(
            program.to_bytes(),
        ));
        cleanup
            .env_clear()
            .args(["-u", "-q", "-z", "--"])
            .arg(path)
            // Retain the original lock through cleanup's exec and exit. A
            // successor cannot publish a mount that this action could detach.
            .stdin(fence.try_clone()?)
            .stdout(acyclic_native_runtime::ProcessStdio::null())
            .stderr(acyclic_native_runtime::ProcessStdio::null());
        self.exit_action = Some(acyclic_native_runtime::ProcessExitAction::spawn(
            &cleanup,
            &work.runtime_meter(),
        )?);
        // Ownership is armed before any mount effect, including the helper.
        // An absent mount is an idempotent namespace retirement, not a reason
        // to release this action while a producer could still mount it.
        self.phase = MountPhase::Mounted;
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
            self.exit_action
                .as_ref()
                .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?
                .activity()?,
        )?);
        let received = receive_device(&receive);
        drop(receive);
        let received_device = received.is_ok();
        if let Ok(fd) = received {
            *device = Some(fd);
            self.phase = MountPhase::Mounted;
        }
        self.helper
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?
            .wait()?;
        self.helper.take();
        if !received_device {
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

    pub(super) fn stop(&mut self, path: &CStr) -> io::Result<()> {
        if let Some(child) = self.helper.as_mut() {
            if child.try_wait()?.is_none() {
                child.kill()?;
            }
            child.wait()?;
            self.helper.take();
        }
        self.attempt_unmount(|mount| {
            // A closed connection does not mean its namespace is gone.
            unmount_with(
                |flags| nix::mount::umount2(path, flags),
                || mount.unmount_helper(path),
            )
        })?;
        self.cancel_exit_action()?;
        Ok(())
    }

    pub(super) fn cancel_exit_action(&mut self) -> io::Result<()> {
        if let Some(action) = self.exit_action.as_mut() {
            action.cancel()?;
        }
        self.exit_action.take();
        Ok(())
    }

    // Retain the original namespace lock until the admitted cleanup has
    // physically exited, including when the ordinary unmount was refused.
    pub(super) fn finish_exit_action(&mut self) -> io::Result<()> {
        if let Some(action) = self.exit_action.as_mut() {
            action.finish()?;
        }
        self.exit_action.take();
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn exit_action_id(&self) -> Option<u32> {
        self.exit_action
            .as_ref()
            .map(acyclic_native_runtime::ProcessExitAction::id)
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
        unmount: impl FnOnce(&mut Self) -> io::Result<()>,
    ) -> io::Result<()> {
        if matches!(self.phase, MountPhase::Detached) {
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
    pub(super) fn wait_device_detached(
        &mut self,
        device: BorrowedFd<'_>,
        wake: Option<&acyclic_native_runtime::ProcessWake>,
    ) -> io::Result<bool> {
        // Unmount can return before the last kernel reference releases the
        // connection. Wait for its acknowledgment instead of treating a probe
        // in that interval as a failed retirement.
        self.poll_detachment(device, -1, wake)
    }

    pub(super) fn observe_closed(&mut self) -> io::Result<bool> {
        self.observe_detachment(|| Ok(true))
    }

    fn poll_detachment(
        &mut self,
        device: BorrowedFd<'_>,
        timeout: i32,
        wake: Option<&acyclic_native_runtime::ProcessWake>,
    ) -> io::Result<bool> {
        loop {
            match self.query_detachment(device, timeout, wake) {
                // A signal has not completed the admitted observation. Resume
                // it here, while the session retains the same read credit.
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => return self.observe_detachment(|| result),
            }
        }
    }

    fn query_detachment(
        &mut self,
        device: BorrowedFd<'_>,
        timeout: i32,
        wake: Option<&acyclic_native_runtime::ProcessWake>,
    ) -> io::Result<bool> {
        #[cfg(test)]
        if let Some(poll) = self.detachment_poll.as_mut() {
            return poll(timeout);
        }
        if let Some(wake) = wake {
            loop {
                match super::transport_io::wait(wake, Some(device), 0)? {
                    super::transport_io::Ready::Closed => return Ok(true),
                    super::transport_io::Ready::Device(events) => {
                        return Ok(events & libc::POLLERR != 0);
                    }
                    super::transport_io::Ready::Wake => wake.consume()?,
                }
            }
        }
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
    }

    fn observe_detachment(&mut self, query: impl FnOnce() -> io::Result<bool>) -> io::Result<bool> {
        let detached = query()?;
        if detached {
            self.phase = MountPhase::Detached;
        }
        Ok(detached)
    }
    pub(super) fn pending(&self) -> bool {
        !matches!(self.phase, MountPhase::Detached)
            || self.helper.is_some()
            || self.exit_action.is_some()
    }

    pub(super) fn needs_unmount(&self) -> bool {
        self.helper.is_some()
            || self.exit_action.is_some()
            || matches!(self.phase, MountPhase::Mounted)
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
        mount.attempt_unmount(|_| Ok(()))?;
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
        Err(nix::errno::Errno::EINVAL | nix::errno::Errno::ENOENT) if !old => Ok(()),
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
    activity: File,
) -> io::Result<Child> {
    let mut command = helper_command(program);
    command
        .args(["-o", options, "--"])
        .arg(path)
        .env("_FUSE_COMMFD", "0")
        .stdin(Stdio::from(OwnedFd::from(child)))
        .stdout(Stdio::from(activity))
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
