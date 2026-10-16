//! Scoped storage for decoded values and codec diagnostics.
use super::*;

/// Retained storage released with its decoded value or content result.
pub struct DecodeReservation<'a> {
    pub(super) io: &'a PreparationIo,
    pub(super) bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) custody: Option<acyclic_fs::NativeAllocation>,
}

impl<'a> DecodeReservation<'a> {
    pub(crate) fn bytes(&self) -> u64 {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = &self.custody {
            return custody.bytes();
        }
        self.bytes
    }

    pub(crate) fn absorb(&mut self, mut other: Self) -> Result<()> {
        if !std::ptr::eq(self.io, other.io) {
            return Err(self.io.invalid("reservation ledgers differ"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let (Some(custody), Some(incoming)) = (&mut self.custody, &mut other.custody) {
            custody.absorb(incoming)?;
            return Ok(());
        }
        self.bytes = self
            .bytes
            .checked_add(other.bytes)
            .ok_or_else(|| self.io.invalid("reservation ownership overflows"))?;
        other.bytes = 0;
        Ok(())
    }

    pub(crate) fn substitute(&mut self, bytes: u64) -> Result<()> {
        self.io.check_cancellation()?;
        let released = self.bytes().checked_sub(bytes).ok_or_else(|| {
            self.io
                .invalid("audited decoded ownership exceeds reservation")
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = &mut self.custody {
            custody.release(released)?;
            return Ok(());
        }
        self.io.release(released)?;
        self.bytes = bytes;
        Ok(())
    }

    pub(crate) fn into_retained(mut self) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = self.custody.take() {
            self.io.absorb_native_custody(custody)?;
        }
        self.bytes = 0;
        Ok(())
    }

    pub(in crate::filesystem) fn io(&self) -> &'a PreparationIo {
        self.io
    }

    pub(in crate::filesystem) fn adopt(&mut self, bytes: u64) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = &mut self.custody {
            let mut incoming = self.io.take_native_custody(bytes)?;
            custody
                .absorb(&mut incoming)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: Some(incoming),
                })?;
            return Ok(());
        }
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| self.io.invalid("reservation ownership overflows"))?;
        Ok(())
    }

    pub(in crate::filesystem) fn retain_part(&mut self, bytes: u64) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = &mut self.custody {
            return self.io.absorb_native_custody(custody.split_off(bytes)?);
        }
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| self.io.invalid("reservation ownership underflows"))?;
        Ok(())
    }
}

impl Drop for DecodeReservation<'_> {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        if let PreparationIo::Owned {
            budget,
            work,
            cancellation,
        } = self.io
        {
            let mut state = work
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (spent, allocations) = &mut *state;
            if let Ok(ledger) = allocations
                && KernelAccounting::borrowed_standalone(ledger, spent, *budget, cancellation)
                    .release_owned(&mut KernelOwnedAllocation::Standalone, self.bytes)
                    .is_err()
            {
                // A poisoned ledger cannot admit further work; the next call returns the fault.
                *allocations = Err(acyclic_fs::WorkError::Overflow);
            }
        }
    }
}

pub(crate) fn retain_decoded<T>(decoded: (T, DecodeReservation<'_>)) -> Result<T> {
    decoded.1.into_retained()?;
    Ok(decoded.0)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_decoded<T>(
    mut decoded: (T, DecodeReservation<'_>),
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    let custody = match decoded.1.custody.take() {
        Some(custody) => custody,
        None => decoded.1.io.take_native_custody(decoded.1.bytes)?,
    };
    decoded.1.bytes = 0;
    Ok((decoded.0, custody))
}

pub(super) fn finish_decode<'a, T>(
    io: &'a PreparationIo,
    result: Result<T>,
    admission: Option<(DecodeReservation<'a>, u64)>,
) -> Result<(T, DecodeReservation<'a>)> {
    let Some((mut held, diagnostics)) = admission else {
        return result.and_then(|_| Err(io.invalid("trusted decode omitted admission")));
    };
    match result {
        Ok(value) => {
            if diagnostics != 0 {
                held.substitute(held.bytes() - diagnostics)?;
            }
            Ok((value, held))
        }
        #[cfg(not(target_arch = "wasm32"))]
        Err(Error::NativeJson {
            code,
            source,
            custody: None,
        }) if io.original_native_work().is_some() => {
            let (source, custody) = native_decoded((source, held))?;
            Err(Error::NativeJson {
                code,
                source,
                custody: Some(custody),
            })
        }
        Err(error) => Err(error),
    }
}

pub(super) fn finish_encoding<T>(
    diagnostics: DecodeReservation<'_>,
    result: Result<T>,
) -> Result<T> {
    match result {
        #[cfg(not(target_arch = "wasm32"))]
        Err(Error::NativeJson {
            code,
            source,
            custody: None,
        }) => {
            let (source, custody) = native_decoded((source, diagnostics))?;
            Err(Error::NativeJson {
                code,
                source,
                custody: Some(custody),
            })
        }
        result => result,
    }
}
