use super::*;

const AUTHORED: &str = "authored-file-v1:";

pub(super) fn is_authored(version: &str) -> bool {
    version.starts_with(AUTHORED)
}

pub(super) fn decode_tool_version(
    context: Option<&ToolContext>,
    path: &str,
    version: &str,
    work: crate::NativeWork<'_>,
) -> Result<(FileReadSource, crate::NativeCustody)> {
    let Some(context) = context else {
        return decode_version(path, version, work);
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let owned_io = work
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let io = owned_io.as_ref();
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let io = None;
    let (volume, relative) = context.tool_path_selection(path, io)?;
    let (file, custody) = decode_version(&relative, version, work)?;
    if file.volume() != volume {
        return Err(super::executor::rejection(
            work,
            "file version belongs to another selected volume",
        ));
    }
    Ok((file, custody))
}

// A token transports the original reference; it is never an authority or an
// index into mutable process state. Its decoded reference goes through the
// same grant and immutable-content checks as every other content read.
pub(super) fn decode_version(
    path: &str,
    version: &str,
    work: crate::NativeWork<'_>,
) -> Result<(FileReadSource, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let (file, mut reservation) = if let Some(selected) = version.strip_prefix(AUTHORED) {
            let (file, reservation) = io.decode::<crate::conversation::FilePathSelection>(
                std::marker::PhantomData,
                selected.as_bytes(),
                crate::wire::ErrorCode::Invalid,
            )?;
            (FileReadSource::Authored(file), reservation)
        } else {
            let (file, reservation) = io.decode::<FileRef>(
                std::marker::PhantomData,
                version.as_bytes(),
                crate::wire::ErrorCode::Invalid,
            )?;
            (FileReadSource::Content(file), reservation)
        };
        crate::executor::with_history_io(Some(&io), |history| file.validate(history))?;
        if file.path() != path {
            return Err(Error::tool_rejected("file version belongs to another path"));
        }
        let retained = (std::mem::size_of::<FileReadSource>() as u64)
            .checked_add(file.logical_owned_bytes()?)
            .ok_or_else(|| Error::tool_rejected("file version ownership overflow"))?;
        reservation.substitute(retained)?;
        reservation.into_retained()?;
        return Ok((file, Some(io.into_native_custody(retained)?)));
    }
    let file = if let Some(selected) = version.strip_prefix(AUTHORED) {
        FileReadSource::Authored(
            serde_json::from_str(selected).map_err(|error| Error::Invalid(error.to_string()))?,
        )
    } else {
        FileReadSource::Content(
            serde_json::from_str(version).map_err(|error| Error::Invalid(error.to_string()))?,
        )
    };
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
    encode_token(file, "", work)
}

pub(super) fn encode_source(
    file: &FileReadSource,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    match file {
        FileReadSource::Content(file) => encode_version(file, work),
        FileReadSource::Authored(file) => encode_token(file, AUTHORED, work),
    }
}

fn encode_token<T: Serialize>(
    file: &T,
    prefix: &str,
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
        let (text, custody) = work.display(&format_args!("{prefix}{text}"))?;
        return Ok((text, Some(custody)));
    }
    serde_json::to_string(file)
        .map(|text| (format!("{prefix}{text}"), None))
        .map_err(|error| Error::Invalid(error.to_string()))
}
