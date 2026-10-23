pub(super) struct InputBuffers<'a, 'w> {
    pub(super) carrier: &'a mut NativeDemandCarrier<'w>,
    pub(super) bytes: u64,
    allocation: Option<crate::NativeAllocation>,
    pending: Option<crate::materializer::NativeWorkReservation>,
}

impl<'a, 'w> InputBuffers<'a, 'w> {
    pub(super) fn new(carrier: &'a mut NativeDemandCarrier<'w>) -> Result<Self, MountSourceError> {
        let allocation = carrier
            .original_native_work()
            .map(|work| {
                work.claim(0, 0, crate::WorkCounters::UNCHARGED)
                    .map_err(MountSourceError::NativeWork)
            })
            .transpose()?;
        carrier
            .with_work(|work| work.check())
            .map_err(MountSourceError::from)?;
        Ok(Self {
            carrier,
            bytes: 0,
            allocation,
            pending: None,
        })
    }

    pub(super) fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError> {
        if let Some(work) = self.carrier.original_native_work() {
            let (values, mut allocation) = crate::native_host::buffer(count, &mut &work)
                .map_err(MountSourceError::NativeWork)?;
            self.allocation
                .as_mut()
                .ok_or(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ))?
                .absorb(&mut allocation)
                .map_err(MountSourceError::NativeWork)?;
            return Ok(values);
        }
        let (values, bytes) = self
            .carrier
            .with_work(|work| crate::native_host::buffer(count, work))
            .map_err(MountSourceError::from)?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(MountSourceError::Work(WorkError::Overflow))?;
        Ok(values)
    }

    fn mount_name(&mut self, logical: &LogicalName) -> Result<Vec<u8>, MountSourceError> {
        let (name, mut allocation) = native_mount_name(logical, self.carrier)?;
        self.allocation
            .as_mut()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?
            .absorb(allocation.as_mut().ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?)
            .map_err(MountSourceError::NativeWork)?;
        Ok(name)
    }

    pub(super) fn copy_bytes(&mut self, bytes: &[u8]) -> Result<Vec<u8>, MountSourceError> {
        let delta = crate::materializer::NativeResourceCredit::Copy.delta(bytes.len() as u64);
        self.admit(delta)?;
        let mut owned = self.vec(bytes.len())?;
        owned.extend_from_slice(bytes);
        self.record(delta)?;
        Ok(owned)
    }

    fn attribute_page(
        &mut self,
        listing: &crate::kernel::AttributeListing,
    ) -> Result<MountAttributePage, MountSourceError> {
        let mut names = self.vec(listing.entries.len())?;
        for entry in &listing.entries {
            self.items(1)?;
            if entry.name.class() != AttributeClass::PosixXattr {
                return Err(MountSourceError::Unsupported(
                    "POSIX mount encountered a non-POSIX named attribute".into(),
                ));
            }
            names.push(self.copy_bytes(entry.name.as_bytes())?);
        }
        let next_cursor = if listing.has_more {
            names.last().map(|name| self.copy_bytes(name)).transpose()?
        } else {
            None
        };
        Ok(MountAttributePage { names, next_cursor })
    }

    fn attribute_name(
        &mut self,
        bytes: &[u8],
        profile: FilesystemProfile,
        limits: VolumeLimits,
    ) -> Result<AttributeName, MountSourceError> {
        if profile != FilesystemProfile::Posix {
            return Err(MountSourceError::Unsupported(
                "native POSIX attributes require a POSIX volume profile".into(),
            ));
        }
        let owned = self.copy_bytes(bytes)?;
        AttributeName::new(
            AttributeClass::PosixXattr,
            owned,
            limits.maximum_component_bytes,
        )
        .map_err(engine_error)
    }

    fn mutation_attribute_name(
        &mut self,
        bytes: &[u8],
        profile: FilesystemProfile,
        limits: VolumeLimits,
    ) -> Result<AttributeName, MountSourceError> {
        // Policy records are changed through the policy owner, never through
        // native xattr operations (including handles opened before revocation).
        if bytes == crate::access::POLICY_METADATA_KEY {
            return Err(MountSourceError::AccessDenied);
        }
        self.attribute_name(bytes, profile, limits)
    }

    pub(super) fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        if let Some(work) = self.carrier.original_native_work() {
            self.pending = Some(
                work.reserve_work(delta)
                    .map_err(MountSourceError::NativeWork)?,
            );
            Ok(())
        } else {
            self.carrier
                .with_work(|work| work.admit(delta))
                .map_err(MountSourceError::from)
        }
    }

    pub(super) fn record(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        if let Some(work) = self.carrier.original_native_work() {
            match &mut self.pending {
                Some(pending) => pending.record_completed(delta),
                None => work.record_admitted(delta),
            }
            .map_err(MountSourceError::NativeWork)
        } else {
            self.carrier
                .with_work(|work| work.record(delta))
                .map_err(MountSourceError::from)
        }
    }

    pub(super) fn charge(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        match self.carrier.original_native_work() {
            Some(work) => work.charge(delta).map_err(MountSourceError::NativeWork),
            None => self
                .carrier
                .with_work(|work| work.charge(delta))
                .map_err(MountSourceError::from),
        }
    }

    pub(super) fn items(&mut self, count: usize) -> Result<(), MountSourceError> {
        self.charge(crate::materializer::NativeResourceCredit::Items.delta(count as u64))
    }

    pub(super) fn release(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        if let Some(allocation) = &mut self.allocation {
            drop(
                allocation
                    .split_off(bytes)
                    .map_err(MountSourceError::NativeWork)?,
            );
        } else {
            self.carrier
                .with_work(|work| work.release(bytes))
                .map_err(MountSourceError::from)?;
            self.bytes = self
                .bytes
                .checked_sub(bytes)
                .ok_or(MountSourceError::Work(WorkError::Overflow))?;
        }
        Ok(())
    }

    pub(super) fn clone_path(
        &mut self,
        path: &Path,
    ) -> Result<std::path::PathBuf, MountSourceError> {
        let original = self
            .carrier
            .original_native_work()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?;
        let (path, mut allocation) = crate::native_host::clone_path(path, &mut &original)
            .map_err(MountSourceError::NativeWork)?;
        self.allocation
            .as_mut()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?
            .absorb(&mut allocation)
            .map_err(MountSourceError::NativeWork)?;
        Ok(path)
    }

    pub(super) fn retain(mut self) -> Option<crate::NativeAllocation> {
        self.bytes = 0;
        self.allocation.take()
    }
}

impl Drop for InputBuffers<'_, '_> {
    fn drop(&mut self) {
        if self.allocation.is_none() {
            self.carrier.with_work(|work| {
                if let Err(error) = work.release(self.bytes) {
                    work.record_native_io_failure(error);
                }
            });
        }
    }
}
