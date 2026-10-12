use super::*;

/// Crash recovery paired with the original destination fence.
#[derive(Default)]
pub(in crate::native_mount) struct FuseRecoveryControl {
    pub(super) helper: Option<acyclic_native_runtime::ProcessTree>,
    pub(super) helper_work: Option<crate::NativeMaterializationWork>,
    pub(super) detached: bool,
}

impl FuseRecoveryControl {
    pub(in crate::native_mount) fn namespace_detach_confirmed(&self) -> bool {
        self.detached
    }
    pub(in crate::native_mount) fn controls_pending(&self) -> bool {
        self.helper.is_some()
    }
    pub(in crate::native_mount) fn retire_controls(
        &mut self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if let Some(helper) = self.helper.as_mut() {
            helper
                .terminate(Some(work.runtime_meter().as_ref()))
                .map_err(|error| work.runtime_error(error))?;
        }
        self.helper.take();
        self.helper_work.take();
        Ok(())
    }
    pub(in crate::native_mount) fn recover(
        &mut self,
        destination: &Path,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        self.detached = !fuse_recovery_is_mounted(destination, work)?;
        self.retire_controls(work)?;
        if self.detached {
            return Ok(());
        }
        let direct =
            crate::native_host::unix_name(destination.as_os_str(), &mut &*work, |path, work| {
                crate::native_host::NativeHostWork::attempt_io(
                    work,
                    crate::WorkCounters {
                        backend_write_operations: 1,
                        ..crate::WorkCounters::UNCHARGED
                    },
                    || {
                        // SAFETY: the terminated pathname lives through the syscall.
                        if unsafe { libc::umount2(path.as_ptr(), libc::MNT_DETACH) } == 0 {
                            Ok(())
                        } else {
                            Err(std::io::Error::last_os_error())
                        }
                    },
                )
            });
        if let Err(error) = direct {
            if !matches!(&error, crate::NativeTreeMaterializationError::Io(_)) {
                return Err(error.into());
            }
        }
        if !fuse_recovery_is_mounted(destination, work)? {
            self.detached = true;
            return Ok(());
        }
        let program = fuse_recovery_helper(work)?;
        self.helper_work = Some(work.clone());
        let spawned =
            crate::native_host::unix_name(destination.as_os_str(), &mut &*work, |path, work| {
                acyclic_native_runtime::ProcessTree::spawn_closed_measured(
                    program,
                    &[c"-u", c"-q", c"-z", c"--", path],
                    work.runtime_meter().as_ref(),
                    &mut self.helper,
                )
                .map_err(|error| work.runtime_error(error))
            });
        if self.helper.is_none() {
            self.helper_work.take();
        }
        spawned?;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while self
            .helper
            .as_mut()
            .expect("accepted helper")
            .try_wait(Some(work.runtime_meter().as_ref()))
            .map_err(|error| work.runtime_error(error))?
            .is_none()
        {
            if std::time::Instant::now() >= deadline {
                self.retire_controls(work)?;
                return Err(NativeMountError::Lifecycle(
                    "fusermount crash recovery exceeded 30 seconds",
                ));
            }
            crate::native_host::NativeHostWork::attempt_io(
                &mut &*work,
                crate::WorkCounters {
                    backend_read_operations: 1,
                    ..crate::WorkCounters::UNCHARGED
                },
                || {
                    std::thread::sleep(Duration::from_millis(10));
                    Ok(())
                },
            )?;
        }
        self.retire_controls(work)?;
        self.detached = !fuse_recovery_is_mounted(destination, work)?;
        if self.detached {
            Ok(())
        } else {
            Err(NativeMountError::Lifecycle(
                "FUSE destination remained mounted after crash recovery",
            ))
        }
    }
}

pub(super) fn fuse_recovery_stat<W: crate::native_host::NativeHostWork>(
    path: &std::ffi::CStr,
    work: &mut W,
) -> Result<rustix::fs::Stat, W::Error> {
    loop {
        let result = crate::native_host::NativeHostWork::attempt_io(
            work,
            crate::WorkCounters {
                backend_read_operations: 1,
                ..crate::WorkCounters::UNCHARGED
            },
            || rustix::fs::stat(path).map_err(std::io::Error::from),
        );
        if result
            .as_ref()
            .err()
            .is_some_and(|error| W::io_error_kind(error) == Some(std::io::ErrorKind::Interrupted))
        {
            continue;
        }
        return result;
    }
}

pub(super) fn fuse_recovery_is_mounted(
    destination: &Path,
    work: &crate::NativeMaterializationWork,
) -> Result<bool, NativeMountError> {
    let parent = destination
        .parent()
        .ok_or(NativeMountError::InvalidDestination)?;
    let parent =
        crate::native_host::unix_name(parent.as_os_str(), &mut &*work, fuse_recovery_stat)?;
    match crate::native_host::unix_name(destination.as_os_str(), &mut &*work, fuse_recovery_stat) {
        Ok(metadata) => Ok(metadata.st_dev != parent.st_dev),
        Err(crate::NativeTreeMaterializationError::Io(error))
            if error.raw_os_error() == Some(libc::ENOTCONN) =>
        {
            Ok(true)
        }
        Err(crate::NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

fn fuse_recovery_helper(
    work: &crate::NativeMaterializationWork,
) -> Result<&'static std::ffi::CStr, NativeMountError> {
    for candidate in [
        c"/usr/bin/fusermount3",
        c"/bin/fusermount3",
        c"/usr/bin/fusermount",
        c"/bin/fusermount",
    ] {
        fuse_item(work)?;
        match fuse_recovery_stat(candidate, &mut &*work) {
            Ok(metadata) if metadata.st_mode & libc::S_IFMT == libc::S_IFREG => {
                return Ok(candidate);
            }
            Ok(_) | Err(crate::NativeTreeMaterializationError::Io(_)) => (),
            Err(error) => return Err(error.into()),
        }
    }
    Err(NativeMountError::CapabilityUnavailable(
        "no canonical fusermount helper is available for crash recovery",
    ))
}
