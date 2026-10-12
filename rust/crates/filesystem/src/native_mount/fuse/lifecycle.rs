use super::*;

/// Crash-recovery custody paired by the root owner with its original fence.
#[derive(Default)]
pub(in crate::native_mount) struct FuseRecoveryControl {
    pub(super) helper: Option<acyclic_native_runtime::ProcessTree>,
    pub(super) helper_work: Option<crate::NativeMaterializationWork>,
    pub(super) deadline: Option<std::time::Instant>,
    pub(super) failed_spawn: bool,
    pub(super) detached: bool,
}

impl FuseRecoveryControl {
    pub(in crate::native_mount) fn namespace_detach_confirmed(&self) -> bool { self.detached }
    pub(in crate::native_mount) fn controls_pending(&self) -> bool { self.helper.is_some() }

    pub(in crate::native_mount) fn retire_controls(&mut self, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        if let Some(helper) = &mut self.helper {
            helper.terminate(Some(work.runtime_meter().as_ref())).map_err(|error| work.runtime_error(error))?;
        }
        self.helper.take();
        self.helper_work.take();
        self.deadline = None;
        self.failed_spawn = false;
        Ok(())
    }

    /// Performs crash recovery on the supplied meter.
    pub(in crate::native_mount) fn recover(
        &mut self, destination: &Path, native: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let work = native;
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let mounted = fuse_recovery_is_mounted(destination, work)?;
        self.detached = !mounted;
        if self.helper.is_some() && (!mounted || self.failed_spawn) { self.retire_controls(work)?; }
        if !mounted { return Ok(()); }
        if self.helper.is_none() {
            let direct = crate::native_host::unix_name_with_work(
                destination.as_os_str(), &mut &*work, |path, work| {
                    fuse_recovery_attempt(work, true, || {
                        // SAFETY: the admitted terminated pathname stays alive
                        // throughout this actual native control call.
                        if unsafe { libc::umount2(path.as_ptr(), libc::MNT_DETACH) } == 0 { Ok(()) }
                        else { Err(std::io::Error::last_os_error()) }
                    })
                },
            );
            if let Err(error) = direct {
                if !matches!(&error, crate::NativeTreeMaterializationError::Io(_)) { return Err(error.into()); }
            }
            if !fuse_recovery_is_mounted(destination, work)? {
                self.detached = true;
                return Ok(());
            }
            let program = fuse_recovery_helper(work)?;
            self.helper_work = Some(work.clone());
            let spawned = crate::native_host::unix_name_with_work(
                destination.as_os_str(), &mut &*work, |path, work| {
                    acyclic_native_runtime::ProcessTree::spawn_closed_measured(
                        program, &[c"-u", c"-q", c"-z", c"--", path], work.runtime_meter().as_ref(), &mut self.helper,
                    ).map_err(|error| work.runtime_error(error))
                },
            );
            if self.helper.is_some() {
                self.deadline = Some(std::time::Instant::now() + Duration::from_secs(30));
                self.failed_spawn = spawned.is_err();
            }
            if self.helper.is_none() { self.helper_work.take(); }
            spawned?;
        }
        loop {
            let status = {
                let helper = self.helper.as_mut().expect("accepted helper remains owned");
                helper.try_wait(Some(work.runtime_meter().as_ref())).map_err(|error| work.runtime_error(error))?
            };
            if status.is_some() { break; }
            if self.deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                self.retire_controls(work)?;
                return Err(NativeMountError::Lifecycle("fusermount crash recovery exceeded 30 seconds"));
            }
            fuse_recovery_attempt(&mut &*work, false, || { std::thread::sleep(Duration::from_millis(10)); Ok(()) })?;
        }
        self.retire_controls(work)?;
        if fuse_recovery_is_mounted(destination, work)? {
            return Err(NativeMountError::Lifecycle("FUSE destination remained mounted after crash recovery"));
        }
        self.detached = true;
        Ok(())
    }
}

pub(super) fn fuse_recovery_attempt<W: crate::native_host::NativeHostWork>(
    work: &mut W, write: bool,
    call: impl FnOnce() -> std::io::Result<()>,
) -> Result<(), W::Error> {
    let delta = if write {
        crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED }
    } else {
        crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED }
    };
    crate::native_host::NativeHostWork::admit(work, delta)?;
    crate::native_host::NativeHostWork::record(work, delta)?;
    call().map_err(Into::into)
}

pub(super) fn fuse_recovery_stat<W: crate::native_host::NativeHostWork>(
    path: &std::ffi::CStr, work: &mut W,
) -> Result<libc::stat, W::Error> {
    loop {
        let result = crate::native_host::NativeHostWork::attempt_io(work,
            crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED }, || {
            let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: pathname is terminated; success initializes all fields.
            if unsafe { libc::stat(path.as_ptr(), metadata.as_mut_ptr()) } == 0 {
                Ok(unsafe { metadata.assume_init() })
            } else { Err(std::io::Error::last_os_error()) }
        });
        if result.as_ref().err().is_some_and(|error| W::io_error_kind(error)
            == Some(std::io::ErrorKind::Interrupted)) { continue; }
        return result;
    }
}

pub(super) fn fuse_recovery_is_mounted(
    destination: &Path, work: &crate::NativeMaterializationWork,
) -> Result<bool, NativeMountError> {
    let parent = destination.parent().ok_or(NativeMountError::InvalidDestination)?;
    let parent = crate::native_host::unix_name_with_work(
        parent.as_os_str(), &mut &*work, fuse_recovery_stat,
    )?;
    let destination = crate::native_host::unix_name_with_work(
        destination.as_os_str(), &mut &*work, fuse_recovery_stat,
    );
    match destination {
        Ok(metadata) => Ok(metadata.st_dev != parent.st_dev),
        Err(crate::NativeTreeMaterializationError::Io(error)) if error.raw_os_error() == Some(libc::ENOTCONN) => Ok(true),
        Err(crate::NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn fuse_recovery_helper(
    work: &crate::NativeMaterializationWork,
) -> Result<&'static std::ffi::CStr, NativeMountError> {
    for candidate in [c"/usr/bin/fusermount3", c"/bin/fusermount3", c"/usr/bin/fusermount", c"/bin/fusermount"] {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        match fuse_recovery_stat(candidate, &mut &*work) {
            Ok(metadata) if metadata.st_mode & libc::S_IFMT == libc::S_IFREG => return Ok(candidate),
            Ok(_) | Err(crate::NativeTreeMaterializationError::Io(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(NativeMountError::CapabilityUnavailable(
        "no canonical fusermount helper is available for crash recovery",
    ))
}
