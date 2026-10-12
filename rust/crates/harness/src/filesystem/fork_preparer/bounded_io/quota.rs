//! Scoped ownership of decoded values and temporary buffers.
use super::*;

/// Retained storage released when a content or decode result is dropped.
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
        if bytes > self.bytes {
            return Err(self
                .io
                .invalid("audited decoded ownership exceeds reservation"));
        }
        self.io.release(self.bytes - bytes)?;
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
    let (held, diagnostics) =
        admission.map_or((None, 0), |(held, diagnostics)| (Some(held), diagnostics));
    #[cfg(not(target_arch = "wasm32"))]
    if io.original_native_work().is_some() {
        let mut custody = held
            .map(|mut held| {
                let custody = io.take_native_custody(held.bytes)?;
                held.bytes = 0;
                Ok::<_, Error>(custody)
            })
            .transpose()?;
        let value = result.map_err(|error| match error {
            Error::NativeDecode {
                code,
                source,
                custody: None,
            } => Error::NativeDecode {
                code,
                source,
                custody: custody.take(),
            },
            error => error,
        })?;
        let mut custody = custody.ok_or_else(|| io.invalid("trusted decode omitted admission"))?;
        if diagnostics != 0
            && let Err(cause) = custody.substitute(custody.bytes() - diagnostics)
        {
            return Err(Error::NativeWork {
                cause,
                custody: Some(custody),
            });
        }
        let bytes = custody.bytes();
        io.absorb_native_custody(custody)?;
        return Ok((value, DecodeReservation { io, bytes }));
    }
    let value = result?;
    let mut held = held.ok_or_else(|| io.invalid("trusted decode omitted admission"))?;
    if diagnostics != 0 {
        held.substitute(held.bytes - diagnostics)?;
    }
    Ok((value, held))
}

pub(super) fn finish_encoding<T>(
    mut diagnostics: DecodeReservation<'_>,
    result: Result<T>,
) -> Result<T> {
    match result {
        #[cfg(not(target_arch = "wasm32"))]
        Err(Error::NativeEncode {
            code,
            source,
            custody: None,
        }) => {
            let custody = diagnostics.io.take_native_custody(diagnostics.bytes)?;
            diagnostics.bytes = 0;
            Err(Error::NativeEncode {
                code,
                source,
                custody: Some(custody),
            })
        }
        result => result,
    }
}
