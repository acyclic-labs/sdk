use super::*;
use crate::materializer::NativeResourceCredit::{ReadOperations, WriteOperations};
use crate::native_host::unix_name;
use crate::{NativeMaterializationWork, NativeTreeMaterializationError, WorkCounters};
use std::ffi::CStr;
use std::time::Instant;

/// Crash recovery paired with the original destination fence.
#[derive(Default)]
pub(in crate::native_mount) struct FuseRecoveryControl {
    pub(super) helper: Option<acyclic_native_runtime::ProcessTree>,
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
        work: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if let Some(helper) = self.helper.as_mut() {
            helper
                .terminate(Some(work.runtime_meter().as_ref()))
                .map_err(|error| work.runtime_error(error))?;
        }
        self.helper.take();
        Ok(())
    }
    pub(in crate::native_mount) fn recover(
        &mut self,
        destination: &Path,
        work: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        work.admit(WorkCounters::UNCHARGED)?;
        self.detached = !fuse_recovery_is_mounted(destination, work)?;
        self.retire_controls(work)?;
        if self.detached {
            return Ok(());
        }
        match unix_name(destination.as_os_str(), &mut &*work, |path, work| {
            work.charge(WriteOperations.delta(1))?;
            // SAFETY: the terminated pathname lives through the syscall.
            if unsafe { libc::umount2(path.as_ptr(), libc::MNT_DETACH) } == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error().into())
            }
        }) {
            Ok(()) | Err(NativeTreeMaterializationError::Io(_)) => (),
            Err(error) => return Err(error.into()),
        }
        if !fuse_recovery_is_mounted(destination, work)? {
            self.detached = true;
            return Ok(());
        }
        let program = fuse_recovery_helper(work)?;
        unix_name(destination.as_os_str(), &mut &*work, |path, work| {
            acyclic_native_runtime::ProcessTree::spawn_closed_measured(
                program,
                &[c"-u", c"-q", c"-z", c"--", path],
                work.runtime_meter().as_ref(),
                &mut self.helper,
            )
            .map_err(|error| work.runtime_error(error))
        })?;
        let deadline = Instant::now() + Duration::from_secs(30);
        while self
            .helper
            .as_mut()
            .ok_or(NativeMountError::FuseShutdownFailed)?
            .try_wait(Some(work.runtime_meter().as_ref()))
            .map_err(|error| work.runtime_error(error))?
            .is_none()
        {
            if Instant::now() >= deadline {
                self.retire_controls(work)?;
                return Err(NativeMountError::Lifecycle("fusermount recovery timed out"));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.retire_controls(work)?;
        self.detached = !fuse_recovery_is_mounted(destination, work)?;
        if self.detached {
            Ok(())
        } else {
            Err(NativeMountError::Lifecycle(
                "FUSE recovery left destination mounted",
            ))
        }
    }
}

pub(super) fn fuse_recovery_stat(
    path: &CStr,
    work: &mut &NativeMaterializationWork,
) -> Result<rustix::fs::Stat, NativeTreeMaterializationError> {
    loop {
        work.charge(ReadOperations.delta(1))?;
        match rustix::fs::stat(path) {
            Err(rustix::io::Errno::INTR) => continue,
            result => return result.map_err(|error| std::io::Error::from(error).into()),
        }
    }
}

pub(super) fn fuse_recovery_is_mounted(
    destination: &Path,
    work: &NativeMaterializationWork,
) -> Result<bool, NativeMountError> {
    let parent = destination
        .parent()
        .ok_or(NativeMountError::InvalidDestination)?;
    let parent = unix_name(parent.as_os_str(), &mut &*work, fuse_recovery_stat)?;
    // The recovery owner holds the exclusive destination fence before this probe.
    fuse_recovery_needs_detach(
        parent.st_dev,
        unix_name(destination.as_os_str(), &mut &*work, fuse_recovery_stat),
    )
}

fn fuse_recovery_needs_detach(
    parent_device: u64,
    destination: Result<rustix::fs::Stat, NativeTreeMaterializationError>,
) -> Result<bool, NativeMountError> {
    match destination {
        Ok(metadata) => Ok(metadata.st_dev != parent_device),
        Err(NativeTreeMaterializationError::Io(error))
            if error.raw_os_error() == Some(libc::ENOTCONN) =>
        {
            Ok(true)
        }
        Err(NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

fn fuse_recovery_helper(
    work: &NativeMaterializationWork,
) -> Result<&'static CStr, NativeMountError> {
    for candidate in [
        c"/usr/bin/fusermount3",
        c"/bin/fusermount3",
        c"/usr/bin/fusermount",
        c"/bin/fusermount",
    ] {
        work.charge(Items.delta(1))?;
        match fuse_recovery_stat(candidate, &mut &*work) {
            Ok(metadata) if metadata.st_mode & libc::S_IFMT == libc::S_IFREG => {
                return Ok(candidate);
            }
            Ok(_) | Err(NativeTreeMaterializationError::Io(_)) => (),
            Err(error) => return Err(error.into()),
        }
    }
    Err(NativeMountError::CapabilityUnavailable(
        "no canonical fusermount helper",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_detaches_mounts_after_acquiring_the_owner_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let mut metadata = rustix::fs::stat(root.path())?;
        let device = metadata.st_dev;
        assert!(!fuse_recovery_needs_detach(device, Ok(metadata))?);
        metadata.st_dev = device.wrapping_add(1);
        assert!(fuse_recovery_needs_detach(device, Ok(metadata))?);
        assert!(fuse_recovery_needs_detach(
            device,
            Err(std::io::Error::from_raw_os_error(libc::ENOTCONN).into())
        )?);
        assert!(!fuse_recovery_needs_detach(
            device,
            Err(std::io::Error::from_raw_os_error(libc::ENOENT).into())
        )?);
        assert!(
            fuse_recovery_needs_detach(
                device,
                Err(std::io::Error::from_raw_os_error(libc::EACCES).into())
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn recovery_helper_cleanup_remains_funded_after_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let meter = work.runtime_meter();
        let unused = meter.reserve_cleanup_write()?;
        meter.cancel_cleanup_write(unused);
        let cleanup = meter.reserve_cleanup_write()?;
        cancellation.cancel();
        meter.record_cleanup_write(cleanup)?;
        Ok(())
    }

    #[test]
    fn recovery_helper_setup_reaches_spawn_instead_of_unsupported()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let missing_helper = root.path().join("absent-fusermount3");
        let work = NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| error)?;
        let mut helper = None;
        let error = unix_name(missing_helper.as_os_str(), &mut &work, |program, work| {
            acyclic_native_runtime::ProcessTree::spawn_closed_measured(
                program,
                &[c"-u", c"-z", c"--", c"/unused-mount"],
                work.runtime_meter().as_ref(),
                &mut helper,
            )
            .map_err(|error| work.runtime_error(error))
        })
        .expect_err("the missing helper must reach the OS spawn boundary");
        assert!(
            matches!(error, NativeTreeMaterializationError::Io(error) if error.kind() == std::io::ErrorKind::NotFound)
        );
        assert!(helper.is_none());
        Ok(())
    }
}
