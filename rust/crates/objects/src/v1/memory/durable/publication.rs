use super::*;

/// Current invocation authority and custody for temporary publication controls.
pub(crate) struct Publication {
    pub(crate) native: Option<(Arc<dyn NativeReadMeter>, ReadAllocation)>,
}
impl Publication {
    /// The meter this publication charges, when it is metered.
    pub(crate) fn meter(&self) -> Option<&dyn NativeReadMeter> {
        self.native.as_ref().map(|(meter, _)| meter.as_ref())
    }

    pub(crate) fn new(meter: Option<Arc<dyn NativeReadMeter>>) -> Self {
        Self {
            native: meter.map(|meter| {
                let retained = ReadAllocation::empty(&meter);
                (meter, retained)
            }),
        }
    }
    pub(crate) fn check(&self) -> Result<(), NativeObjectsError> {
        if let Some((meter, _)) = &self.native {
            meter.check()?;
        }
        Ok(())
    }
    pub(crate) fn allocate(&mut self, bytes: usize) -> Result<(), NativeObjectsError> {
        if let Some((meter, retained)) = &mut self.native {
            let mut claim = ReadAllocation::new(meter, bytes)?;
            retained.absorb(&mut claim)?;
        }
        Ok(())
    }
    pub(crate) fn retain(&mut self, bytes: usize) -> Result<(), NativeObjectsError> {
        if let Some((meter, retained)) = &mut self.native {
            let mut claim = ReadAllocation::adopt(meter, bytes)?;
            retained.absorb(&mut claim)?;
        }
        Ok(())
    }
    pub(crate) fn retain_capacity(
        &mut self,
        requested: usize,
        actual: usize,
    ) -> Result<(), NativeObjectsError> {
        if let Some((_, retained)) = &mut self.native {
            let total =
                usize::try_from(retained.bytes()).map_err(|_| Error::from(QuotaExceeded))?;
            retained.reconcile_capacity(
                total
                    .checked_sub(requested)
                    .and_then(|bytes| bytes.checked_add(actual))
                    .ok_or(Error::from(QuotaExceeded))?,
            )?;
        }
        Ok(())
    }
    pub(crate) fn allocation_failed(
        &self,
        error: std::collections::TryReserveError,
    ) -> NativeObjectsError {
        match &self.native {
            Some((meter, _)) => NativeObjectsError::from(NativeObjectsCause::Admission(
                meter.allocation_failed(error),
            )),
            None => NativeObjectsError::from(NativeObjectsCause::Service(Unavailable.into())),
        }
    }
    pub(crate) fn retire(&mut self, bytes: usize) -> Result<(), NativeObjectsError> {
        if let Some((_, retained)) = &mut self.native {
            retained.release(bytes)?;
        }
        Ok(())
    }
    pub(crate) fn reserve<T>(
        &mut self,
        value: &mut Vec<T>,
        additional: usize,
    ) -> Result<(), NativeObjectsError> {
        let needed = value
            .len()
            .checked_add(additional)
            .ok_or(Error::from(QuotaExceeded))?;
        if needed <= value.capacity() {
            return self.check();
        }
        let width = std::mem::size_of::<T>();
        let old = value
            .capacity()
            .checked_mul(width)
            .ok_or(Error::from(QuotaExceeded))?;
        let requested = needed
            .checked_mul(width)
            .ok_or(Error::from(QuotaExceeded))?;
        let mut claim = self
            .native
            .as_ref()
            .map(|(meter, _)| ReadAllocation::new(meter, requested))
            .transpose()?;
        if let Err(error) = value.try_reserve_exact(additional) {
            return Err(self.allocation_failed(error));
        }
        if let (Some(claim), Some((_, retained))) = (&mut claim, &mut self.native) {
            let reconciled = claim.reconcile_capacity(
                value
                    .capacity()
                    .checked_mul(width)
                    .ok_or(Error::from(QuotaExceeded))?,
            );
            retained.resize_retained(
                usize::try_from(retained.bytes())
                    .map_err(|_| Error::from(QuotaExceeded))?
                    .saturating_sub(old),
            )?;
            retained.absorb(claim)?;
            reconciled?;
        }
        Ok(())
    }
    pub(crate) fn push<T>(
        &mut self,
        value: &mut Vec<T>,
        item: T,
    ) -> Result<(), NativeObjectsError> {
        if value.len() == value.capacity() {
            self.reserve(value, value.capacity().max(1))?;
        } else {
            self.check()?;
        }
        value.push(item);
        Ok(())
    }
    pub(crate) fn codec<T>(
        &self,
        encoded: usize,
        copied: usize,
        hashed: usize,
        work: impl FnOnce() -> T,
    ) -> Result<T, NativeObjectsError> {
        if let Some((meter, _)) = &self.native {
            meter.admit_codec(encoded as u64, copied as u64, hashed as u64)?;
        }
        let value = work();
        if let Some((meter, _)) = &self.native {
            meter.record_codec(encoded as u64, copied as u64, hashed as u64)?;
        }
        Ok(value)
    }
    pub(crate) fn text(&mut self, value: &str) -> Result<String, NativeObjectsError> {
        match &mut self.native {
            Some((meter, retained)) => clone_native_text(value, meter, retained),
            _ => Ok(value.to_owned()),
        }
    }
    pub(in crate::v1::memory) fn info(
        &mut self,
        value: &wire::ObjectInfo,
    ) -> Result<wire::ObjectInfo, NativeObjectsError> {
        match &mut self.native {
            Some((meter, retained)) => clone_native_info(value, meter, retained),
            _ => Ok(value.clone()),
        }
    }
    pub(in crate::v1::memory) fn metadata(
        &mut self,
        value: &wire::ObjectMetadata,
    ) -> Result<wire::ObjectMetadata, NativeObjectsError> {
        match &mut self.native {
            Some((meter, retained)) => clone_native_metadata(value, meter, retained),
            _ => Ok(value.clone()),
        }
    }
    pub(crate) fn bytes(&mut self, value: &[u8]) -> Result<Vec<u8>, NativeObjectsError> {
        let mut output = Vec::new();
        self.reserve(&mut output, value.len())?;
        self.codec(0, value.len(), 0, || output.extend_from_slice(value))?;
        Ok(output)
    }
    pub(crate) fn encode<M: prost::Message>(
        &mut self,
        value: &M,
    ) -> Result<Vec<u8>, NativeObjectsError> {
        let length = value.encoded_len();
        let mut output = Vec::new();
        self.reserve(&mut output, length)?;
        self.codec(length, 0, 0, || value.encode(&mut output))?
            .map_err(|_| {
                NativeObjectsError::from(NativeObjectsCause::Service(Unavailable.into()))
            })?;
        Ok(output)
    }
}
