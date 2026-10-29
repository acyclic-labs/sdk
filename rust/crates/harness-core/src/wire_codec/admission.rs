//! Retained construction custody on the original history observer.
use super::*;

/// Custody released on drop or transferred to the output owner.
pub(crate) struct Quota<'a, 'b> {
    work: &'a mut Option<&'b mut dyn HistoryIo>,
    bytes: u64,
}
impl<'a, 'b> Quota<'a, 'b> {
    pub(crate) fn new(work: &'a mut Option<&'b mut dyn HistoryIo>, bytes: u64) -> Result<Self> {
        if let Some(io) = work.as_deref_mut() {
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
        }
        Ok(Self { work, bytes })
    }
    pub(crate) fn from_retained(work: &'a mut Option<&'b mut dyn HistoryIo>, bytes: u64) -> Self {
        Self { work, bytes }
    }
    pub(crate) fn work(&mut self) -> &mut Option<&'b mut dyn HistoryIo> {
        self.work
    }
    pub(crate) fn observer(&mut self) -> Result<&mut (dyn HistoryIo + 'b)> {
        self.work
            .as_deref_mut()
            .ok_or_else(|| Error::Invalid("funded codec quota requires its observer".into()))
    }
    pub(crate) fn replace(&mut self, bytes: u64) -> Result<()> {
        if let Some(work) = self.work.as_deref_mut() {
            if bytes <= self.bytes {
                work.release_owned(self.bytes - bytes)?;
            } else {
                work.admit_owned(bytes - self.bytes)?;
                work.retain_owned(bytes - self.bytes)?;
            }
        }
        self.bytes = bytes;
        Ok(())
    }
    pub(crate) fn adopt(&mut self, bytes: u64) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| boundary(self.work, Storage, "custody overflow"))?;
        Ok(())
    }
    /// Funds the heap slot separately from the value's already retained children.
    #[cfg(test)]
    pub(crate) fn boxed<T>(&mut self, value: T) -> Result<Box<T>> {
        let bytes = std::mem::size_of::<T>() as u64;
        let retained = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| boundary(self.work, Storage, "boxed custody overflow"))?;
        self.replace(retained)?;
        if bytes != 0
            && let Some(work) = self.work.as_deref_mut()
        {
            work.allocation_attempt(bytes)?;
        }
        Ok(Box::new(value))
    }
    pub(crate) fn relinquish(&mut self, bytes: u64) -> Result<()> {
        let remaining = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| boundary(self.work, Storage, "release exceeds custody"))?;
        self.replace(remaining)
    }
    pub(crate) fn retained(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn transfer(&mut self) {
        self.bytes = 0;
    }
    /// Transfers this construction only when the observer supplies an output owner.
    #[cfg(feature = "filesystem")]
    pub(crate) fn take_owned_custody(
        &mut self,
    ) -> Result<Option<crate::filesystem::PreparationCustody>> {
        let custody = match self.work.as_deref_mut() {
            Some(io) => io.take_owned_custody(self.bytes)?,
            None => None,
        };
        if custody.is_some() {
            self.transfer();
        }
        Ok(custody)
    }
    /// Move this construction's exact storage into its native output owner.
    #[cfg(feature = "filesystem")]
    pub(crate) fn into_custody(mut self) -> Result<NativeCustody> {
        #[cfg(not(target_arch = "wasm32"))]
        let custody = match self.work.as_deref_mut() {
            Some(io) if io.original_native_work().is_some() => {
                Some(io.take_native_custody(self.bytes)?)
            }
            _ => None,
        };
        #[cfg(target_arch = "wasm32")]
        let custody = None;
        self.transfer();
        Ok(custody)
    }
    pub(crate) fn json_error(&mut self, error: Error) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut error = error;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Error::NativeJson { custody, .. } = &mut error
            && custody.is_none()
        {
            let bytes = self.bytes;
            *custody = Some(
                match self.observer().and_then(|io| io.take_native_custody(bytes)) {
                    Ok(custody) => custody,
                    Err(error) => return error,
                },
            );
            self.transfer();
        }
        error
    }
}
impl Drop for Quota<'_, '_> {
    fn drop(&mut self) {
        if let Some(work) = self.work.as_deref_mut() {
            let _ = work.release_owned(self.bytes);
        }
    }
}
impl<'a, 'b> Quota<'a, 'b> {
    pub(crate) fn observed(work: &'a mut Option<&'b mut dyn HistoryIo>) -> Result<Self> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        Ok(Self::from_retained(work, 0))
    }

    pub(crate) fn invalid(&mut self, reason: &'static str) -> Error {
        crate::store::guarded_boundary(self.work(), crate::wire::ErrorCode::Invalid, reason)
    }

    pub(crate) fn checkpoint(&mut self) -> Result<()> {
        if let Some(observer) = self.work().as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                bytes: 0,
                items: 1,
            })?;
        }
        Ok(())
    }

    pub(crate) fn retain(&mut self, bytes: u64) -> Result<()> {
        if self.work().is_none() {
            return Ok(());
        }
        let retained = self
            .retained()
            .checked_add(bytes)
            .ok_or_else(|| self.invalid("fork validation storage overflows"))?;
        self.replace(retained)
    }

    pub(crate) fn release(&mut self, bytes: u64) -> Result<()> {
        if self.work().is_none() {
            return Ok(());
        }
        self.retained()
            .checked_sub(bytes)
            .ok_or_else(|| self.invalid("fork validation storage underflows"))?;
        self.relinquish(bytes)
    }

    pub(crate) fn slots<T>(&mut self, count: usize) -> Result<()> {
        let bytes = (count as u64)
            .checked_mul(std::mem::size_of::<T>() as u64)
            .ok_or_else(|| self.invalid("fork validation slot count overflows"))?;
        self.retain(bytes)
    }
}
