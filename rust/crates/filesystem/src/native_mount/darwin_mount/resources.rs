use super::*;

pub(super) struct DriverSessionResources {
    pub(super) context: Arc<DarwinMountContext>,
    pub(super) native: Mutex<(
        usize,
        Option<crate::NativeAllocation>,
        crate::materializer::NativeWorkReservation,
    )>,
    pub(super) startup: StartupBuffers,
    pub(super) _allocation: crate::NativeAllocation,
}

impl DriverSessionResources {
    pub(super) fn new_sdk_session(
        work: &crate::NativeMaterializationWork,
    ) -> Result<(usize, crate::NativeAllocation), NativeMountError> {
        // SAFETY: the bridge reports its fixed session allocation size.
        let bytes = unsafe { acyclic_fs_darwin_mount_session_allocation_size() } as u64;
        let allocation = work.allocation_work().claim(bytes, 1, WRITE_WORK)?;
        // SAFETY: the bridge allocator has no preconditions.
        let pointer = unsafe { acyclic_fs_darwin_mount_session_new() } as usize;
        if pointer == 0 {
            return Err(driver_errno(libc::ENOMEM));
        }
        Ok((pointer, allocation))
    }

    pub(super) fn pointer(&self) -> usize {
        self.native.lock().unwrap_or_else(PoisonError::into_inner).0
    }

    pub(super) fn interrupt(
        &self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        let mut native = self.native.lock().unwrap_or_else(PoisonError::into_inner);
        if native.0 == 0 || native.2.remaining_work().backend_write_operations < 2 {
            return Ok(());
        }
        let write = WRITE_WORK;
        if let Some(wait) = wait {
            native.2.loan(Some(wait)).started_io(write)?;
        } else {
            native.2.started_drop_io(write);
        }
        // SAFETY: this locked owner retains the live bridge session.
        unsafe {
            acyclic_fs_darwin_mount_interrupt(native.0 as *mut c_void);
        }
        Ok(())
    }

    pub(super) fn start_join(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        self.native
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .2
            .loan(Some(wait))
            .started_io(READ_WORK)?;
        Ok(())
    }

    pub(super) fn free(
        &self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        let mut native = self.native.lock().unwrap_or_else(PoisonError::into_inner);
        if native.0 == 0 {
            return Ok(());
        }
        let write = WRITE_WORK;
        if let Some(wait) = wait {
            native.2.loan(Some(wait)).started_io(write)?;
        } else {
            native.2.started_drop_io(write);
        }
        // SAFETY: only the quiescent loop or its joined owner calls free.
        unsafe {
            acyclic_fs_darwin_mount_session_free(native.0 as *mut c_void);
        }
        native.0 = 0;
        drop(native.1.take());
        Ok(())
    }
}

impl Drop for DriverSessionResources {
    fn drop(&mut self) {
        self.free(None).expect("birth-paid SDK destruction");
    }
}
