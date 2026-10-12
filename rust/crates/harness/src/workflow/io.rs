use super::*;

pub(super) fn error(io: Option<&PreparationIo>, code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.is_some() { return Error::NativeBoundary { code, reason }; }
    let _ = io;
    match code {
        Storage => Error::Storage(reason.into()), Conflict => Error::Conflict(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}
pub(super) fn validate_key(key: &IdempotencyKey, io: Option<&PreparationIo>) -> Result<()> {
    if key.0.is_empty() || key.0.chars().any(char::is_control) {
        return Err(error(io, Invalid, "idempotency key is invalid"));
    }
    Ok(())
}
pub(super) fn next_revision(revision: u64, io: Option<&PreparationIo>) -> Result<u64> {
    revision.checked_add(1).ok_or_else(|| error(io, Invalid, "revision exhausted"))
}
pub(crate) fn workflow_owned_bytes(io: Option<&PreparationIo>) -> Result<u64> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { return io.owned_bytes(); }
    let _ = io;
    Ok(0)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn finish_native_io<T>(result: Result<T>, io: PreparationIo, retained: u64) -> Result<(T, acyclic_fs::NativeAllocation)> {
    match result {
        Ok(value) => Ok((value, io.into_native_custody(retained)?)),
        Err(Error::NativeWork { cause, custody }) => {
            // Do not observe cancellation, clocks or authority after failure.
            let owned = match io.owned_bytes() { Ok(bytes) => bytes, Err(_) => return Err(Error::NativeWork { cause, custody }) };
            let mut held = match io.into_native_custody(owned) { Ok(held) => held, Err(_) => return Err(Error::NativeWork { cause, custody }) };
            if let Some(mut incoming) = custody {
                if !held.work().shares_ledger(incoming.work()) || held.absorb(&mut incoming).is_err() {
                    return Err(Error::NativeWork { cause, custody: Some(incoming) });
                }
            }
            Err(Error::NativeWork { cause, custody: Some(held) })
        }
        Err(error) => Err(error),
    }
}
