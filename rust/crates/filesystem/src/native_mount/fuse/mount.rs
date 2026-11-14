use acyclic_native_runtime::PacketDevice;
#[cfg(test)]
use acyclic_native_runtime::receive_packet_device_reply_for_test as receive_device;
use nix::mount::MntFlags;
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
#[cfg(test)]
#[path = "mount/reply_check.rs"]
mod reply_check;
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

pub(super) enum Device {
    Hosted {
        device: PacketDevice,
        allocation: crate::NativeAllocation,
        maximum_write: u32,
    },
    #[cfg(test)]
    InProcess(File),
}

#[cfg(test)]
pub(super) fn in_process_negative_control<T>(operation: impl FnOnce() -> T) -> T {
    tests::with_in_process(operation)
}

impl Mount {
    pub(super) fn open_into(
        &mut self,
        path: &Path,
        writable: bool,
        program: impl FnOnce() -> Result<&'static CStr, super::NativeMountError>,
        device: &mut Option<Device>,
        fence: &File,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), super::NativeMountError> {
        // A privileged mount and its crash cleanup use the kernel directly.
        // In particular, do not ask fusermount to interpret a Sysbox mount.
        // Force invokes FUSE's umount_begin connection abort before namespace
        // detach. Closing a device cannot abort a notification syscall that
        // still borrows it while waiting for a request's directory lock.
        self.arm_exit_action(
            c"/bin/umount",
            &["-i", "-c", "-f", "-l", "--"],
            path,
            fence,
            work,
            false,
        )
        .map_err(|error| error.during("arm kernel unmount owner-exit action", path))?;
        self.phase = MountPhase::Mounted;
        let directory = File::open(path).map_err(|error| {
            super::NativeMountError::host_operation("open mountpoint before mount", path, error)
        })?;
        let mode = directory
            .metadata()
            .map_err(|error| {
                super::NativeMountError::host_operation(
                    "fstat mountpoint before mount",
                    path,
                    error,
                )
            })?
            .permissions()
            .mode();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/fuse")
            .map_err(|error| {
                super::NativeMountError::host_operation("open /dev/fuse", path, error)
            })?;
        // Transfer before installing the namespace: the owner must never hold
        // /dev/fuse while another of its threads can enter the mounted volume.
        #[cfg(test)]
        if tests::in_process() {
            tests::mount_in_process(path, writable, &file, mode)?;
            *device = Some(Device::InProcess(file));
            self.arm_exit_action(
                c"/bin/umount",
                &["-i", "-c", "-f", "-l", "--"],
                path,
                fence,
                work,
                true,
            )?;
            return Ok(());
        }
        let funded = super::host_io::fund_device(work)?;
        let hosted = PacketDevice::prepare(file, funded.capacity)?;
        match hosted.mount_fuse(
            &directory,
            mode,
            nix::unistd::getuid().as_raw(),
            nix::unistd::getgid().as_raw(),
            writable,
        ) {
            Ok(()) => {
                *device = Some(Device::Hosted {
                    device: hosted,
                    allocation: funded.allocation,
                    maximum_write: funded.maximum_write,
                });
                // The physical mount is complete; no producer can mount late.
                // Follow owner death before any kernel callback is admitted.
                self.arm_exit_action(
                    c"/bin/umount",
                    &["-i", "-c", "-f", "-l", "--"],
                    path,
                    fence,
                    work,
                    true,
                )?;
                return Ok(());
            }
            Err(error) if error.raw_os_error() == Some(libc::EPERM) => {}
            Err(error) => {
                return Err(super::NativeMountError::host_operation(
                    "mount(2) FUSE in device host",
                    path,
                    error,
                ));
            }
        }
        drop(hosted);
        drop(funded);
        // Only kernel permission refusal requires the unprivileged helper.
        // Replace the still-unused direct cleanup before admitting its mount.
        self.cancel_exit_action().map_err(|error| {
            super::NativeMountError::host_operation(
                "retire unused kernel unmount action",
                path,
                error,
            )
        })?;
        let program = program()?;
        self.program = Some(program);
        self.arm_exit_action(program, &["-u", "-q", "-z", "--"], path, fence, work, false)
            .map_err(|error| error.during("arm unprivileged FUSE owner-exit action", path))?;
        self.open_helper(program, path, writable, device, work)
            .map_err(|error| error.during("mount FUSE via unprivileged helper", path))?;
        self.arm_exit_action(program, &["-u", "-q", "-z", "--"], path, fence, work, true)
    }

    fn arm_exit_action(
        &mut self,
        program: &'static CStr,
        arguments: &[&str],
        path: &Path,
        fence: &File,
        work: &crate::NativeMaterializationWork,
        owner_exit: bool,
    ) -> Result<(), super::NativeMountError> {
        let mut cleanup = acyclic_native_runtime::ProcessCommand::new(std::ffi::OsStr::from_bytes(
            program.to_bytes(),
        ));
        cleanup
            .env_clear()
            .args(arguments.iter().copied())
            .arg(path)
            // Retain the original lock through cleanup's exec and exit. A
            // successor cannot publish a mount that this action could detach.
            .stdin(fence.try_clone()?)
            .stdout(acyclic_native_runtime::ProcessStdio::null())
            .stderr(acyclic_native_runtime::ProcessStdio::null());
        let action = if owner_exit {
            acyclic_native_runtime::ProcessExitAction::spawn_on_owner_exit(
                &cleanup,
                &work.runtime_meter(),
            )?
        } else {
            acyclic_native_runtime::ProcessExitAction::spawn(&cleanup, &work.runtime_meter())?
        };
        if let Some(mut previous) = self.exit_action.replace(action) {
            previous.cancel()?;
        }
        Ok(())
    }

    fn open_helper(
        &mut self,
        program: &'static CStr,
        path: &Path,
        writable: bool,
        device: &mut Option<Device>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), super::NativeMountError> {
        let (receive, child) = UnixStream::pair()?;
        let funded = super::host_io::fund_device(work)?;
        let hosted = PacketDevice::receive_device(receive, funded.capacity)?;
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
        let received = hosted.wait_mounted();
        let received_device = received.is_ok();
        if received_device {
            *device = Some(Device::Hosted {
                device: hosted,
                allocation: funded.allocation,
                maximum_write: funded.maximum_write,
            });
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
