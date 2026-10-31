use super::*;

// A token transports the original reference; it is never an authority or an
// index into mutable process state. Its decoded reference goes through the
// same grant and immutable-content checks as every other content read.
pub(super) fn decode_version(
    path: &str,
    version: &str,
    work: crate::NativeWork<'_>,
) -> Result<(FileRef, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let (file, mut reservation) = io.decode::<FileRef>(
            std::marker::PhantomData,
            version.as_bytes(),
            crate::wire::ErrorCode::Invalid,
        )?;
        if file.path() != path {
            return Err(Error::tool_rejected("file version belongs to another path"));
        }
        let retained = (std::mem::size_of::<FileRef>() as u64)
            .checked_add(file.logical_owned_bytes()?)
            .ok_or_else(|| Error::tool_rejected("file version ownership overflow"))?;
        reservation.substitute(retained)?;
        reservation.into_retained()?;
        return Ok((file, Some(io.into_native_custody(retained)?)));
    }
    let file: FileRef =
        serde_json::from_str(version).map_err(|error| Error::Invalid(error.to_string()))?;
    if file.path() != path {
        return Err(Error::Invalid(
            "file version belongs to another path".into(),
        ));
    }
    file.validate(&mut None)?;
    Ok((file, None))
}

pub(super) fn encode_version(
    file: &FileRef,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let bytes = io.encode(file, work.remaining()?.bytes_encoded, false)?;
        // The shared codec produces UTF-8 JSON; display retains the string on
        // the same original ledger while its encoded scratch is still alive.
        let text = std::str::from_utf8(&bytes).map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "file version encoder returned invalid UTF-8",
        })?;
        let (text, custody) = work.display(text)?;
        return Ok((text, Some(custody)));
    }
    serde_json::to_string(file)
        .map(|text| (text, None))
        .map_err(|error| Error::Invalid(error.to_string()))
}
