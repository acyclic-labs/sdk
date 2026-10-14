//! Scoped storage for decoded values and codec diagnostics.
use super::*;

/// Retained storage released with its decoded value or content result.
pub struct DecodeReservation<'a> {
    pub(super) io: &'a PreparationIo,
    pub(super) bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) custody: Option<acyclic_fs::NativeAllocation>,
}

impl DecodeReservation<'_> {
    pub(crate) fn bytes(&self) -> u64 {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = &self.custody {
            return custody.bytes();
        }
        self.bytes
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

    pub(crate) fn into_retained(mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = self.custody.take() {
            self.io
                .absorb_native_custody(custody)
                .expect("reservation returns to its original ledger");
        }
        self.bytes = 0;
    }
}

impl Drop for DecodeReservation<'_> {
    fn drop(&mut self) {
        self.io
            .release(self.bytes)
            .expect("reservation belongs to its live ledger");
    }
}

pub(crate) fn retain_decoded<T>(decoded: (T, DecodeReservation<'_>)) -> T {
    decoded.1.into_retained();
    decoded.0
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
