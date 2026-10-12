//! Scoped storage for decoded values and codec diagnostics.
use super::*;

/// Retained storage released with its decoded value or content result.
pub struct DecodeReservation<'a> {
    pub(super) io: &'a PreparationIo,
    pub(super) bytes: u64,
}

impl DecodeReservation<'_> {
    pub(crate) const fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn substitute(&mut self, bytes: u64) -> Result<()> {
        self.io.check_cancellation()?;
        let released = self.bytes.checked_sub(bytes).ok_or_else(|| {
            self.io
                .invalid("audited decoded ownership exceeds reservation")
        })?;
        self.io.release(released)?;
        self.bytes = bytes;
        Ok(())
    }

    pub(crate) fn into_retained(mut self) {
        self.bytes = 0;
    }
}

impl Drop for DecodeReservation<'_> {
    fn drop(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        if self.io.original_native_work().is_some() {
            drop(
                self.io
                    .take_native_custody(self.bytes)
                    .expect("reservation belongs to its live ledger"),
            );
            return;
        }
        self.io
            .release(self.bytes)
            .expect("reservation belongs to its live ledger");
    }
}

pub(crate) fn retain_decoded<T>(mut decoded: (T, DecodeReservation<'_>)) -> T {
    decoded.1.bytes = 0;
    decoded.0
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_decoded<T>(
    mut decoded: (T, DecodeReservation<'_>),
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    let custody = decoded.1.io.take_native_custody(decoded.1.bytes)?;
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
