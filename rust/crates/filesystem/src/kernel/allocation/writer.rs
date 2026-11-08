//! One admitted output backing for formatting and serialization.

use super::KernelAccounting;
#[cfg(not(target_arch = "wasm32"))]
use super::KernelOwnedAllocation;
use crate::kernel::Owned;
use crate::{FsError, WorkCounters, WorkError};

pub(crate) struct ImageWriter<'w, 'a> {
    bytes: Owned<Vec<u8>>,
    accounting: &'w mut KernelAccounting<'a>,
    failure: Option<FsError>,
}

impl ImageWriter<'_, '_> {
    /// The scope retires failed output after its physical storage drops,
    /// including during unwind. Successful output keeps its original guard.
    pub(crate) fn produce<E: From<FsError>, F>(
        accounting: &mut KernelAccounting<'_>,
        initial_capacity: usize,
        write: impl FnOnce(&mut ImageWriter<'_, '_>) -> Result<(), F>,
        convert_error: impl FnOnce(F) -> E,
    ) -> Result<Owned<Vec<u8>>, E> {
        let mut scope = accounting.allocation_scope().map_err(E::from)?;
        let bytes = Owned::allocate(initial_capacity, &mut scope).map_err(E::from)?;
        let mut writer = ImageWriter {
            bytes,
            accounting: &mut scope,
            failure: None,
        };
        let result = write(&mut writer);
        if let Some(error) = writer.failure.take() {
            return Err(error.into());
        }
        result.map_err(convert_error)?;
        let bytes = writer.bytes;
        scope.keep();
        Ok(bytes)
    }

    pub(crate) fn append(&mut self, bytes: &[u8]) -> Result<(), FsError> {
        let delta = WorkCounters {
            bytes_copied: bytes.len() as u64,
            ..WorkCounters::UNCHARGED
        };
        #[cfg(not(target_arch = "wasm32"))]
        let mut credit = super::effects::EffectCredit::for_accounting(self.accounting);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(credit) = &mut credit {
            credit.reserve(delta, false)?;
        } else {
            self.accounting.admit(delta)?;
        }
        #[cfg(target_arch = "wasm32")]
        self.accounting.admit(delta)?;
        let length = self
            .bytes
            .value
            .len()
            .checked_add(bytes.len())
            .ok_or(WorkError::Overflow)?;
        if length > self.bytes.value.capacity() {
            let capacity = length.max(self.bytes.value.capacity().saturating_mul(2));
            self.bytes.reserve_capacity_with(
                capacity,
                self.accounting,
                FsError::EncodedAllocationFailed,
            )?;
        }
        self.bytes.value.extend_from_slice(bytes);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(credit) = &mut credit {
            credit.complete(delta)?;
        } else {
            self.accounting.record_completed(delta)?;
        }
        #[cfg(target_arch = "wasm32")]
        self.accounting.record_completed(delta)?;
        Ok(())
    }
}

impl std::fmt::Write for ImageWriter<'_, '_> {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.append(value.as_bytes()).map_err(|error| {
            self.failure = Some(error);
            std::fmt::Error
        })
    }
}

impl std::io::Write for ImageWriter<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.accounting
            .charge(WorkCounters {
                bytes_encoded: bytes.len() as u64,
                ..WorkCounters::UNCHARGED
            })
            .and_then(|()| self.append(bytes))
            .map(|()| bytes.len())
            .map_err(|error| {
                self.failure = Some(error);
                std::io::ErrorKind::Other.into()
            })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_image(
    work: &crate::NativeMaterializationWork,
    write: impl FnOnce(&mut ImageWriter<'_, '_>) -> Result<(), FsError>,
) -> Result<(Vec<u8>, crate::NativeAllocation), crate::NativeTreeMaterializationError> {
    let mut accounting = KernelAccounting::native(work, work.cancellation());
    let bytes = ImageWriter::produce(&mut accounting, 0, write, std::convert::identity)
        .map_err(native_error)?;
    let KernelOwnedAllocation::NativeOriginal(allocation) = bytes.allocation else {
        unreachable!("native output retains the original allocation guard")
    };
    Ok((bytes.value, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_error(error: FsError) -> crate::NativeTreeMaterializationError {
    match error {
        FsError::NativeKernel(error) => error,
        FsError::Work(error) => error.into(),
        FsError::Cancelled(_) => crate::NativeTreeMaterializationError::Cancelled,
        error => crate::NativeTreeMaterializationError::Io(std::io::Error::other(error)),
    }
}
