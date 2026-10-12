use super::*;

pub(super) fn mount_path(path: *const c_char) -> Result<MountPath, i32> {
    let bytes = c_bytes(path)?;
    if bytes.first().copied() != Some(b'/') {
        return Err(libc::EINVAL);
    }
    let mut result = MountPath::root();
    for component in bytes.split(|byte| *byte == b'/').skip(1) {
        if component.is_empty() {
            continue;
        }
        if component == b"." || component == b".." {
            return Err(libc::EPERM);
        }
        result = result.child(component.to_vec());
    }
    Ok(result)
}

pub(super) fn c_bytes<'a>(value: *const c_char) -> Result<&'a [u8], i32> {
    if value.is_null() {
        return Err(libc::EINVAL);
    }
    // SAFETY: callers pass only bridge strings, which FUSE NUL-terminates and keeps
    // valid for the callback; null was rejected above.
    Ok(unsafe { CStr::from_ptr(value) }.to_bytes())
}

pub(super) fn path_cstring(path: &Path) -> Result<CString, i32> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| libc::EINVAL)
}

pub(super) fn bounded_length(length: usize) -> Result<u32, i32> {
    if length > MAXIMUM_CALLBACK_BYTES {
        return Err(libc::E2BIG);
    }
    u32::try_from(length).map_err(|_| libc::EOVERFLOW)
}

pub(super) fn copy_variable_result(bytes: &[u8], output: *mut c_char, length: usize) -> Result<c_int, i32> {
    if length == 0 {
        return c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW);
    }
    if output.is_null() || bytes.len() > length {
        return Err(libc::ERANGE);
    }
    // SAFETY: `output` is a non-null bridge buffer of `length` bytes and
    // `bytes` fits in it (checked above).
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), output.cast(), bytes.len()) };
    c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW)
}

pub(super) fn update_time(field: &mut MetadataField<i64>, seconds: i64, nanoseconds: i64) -> Result<(), i32> {
    if nanoseconds == libc::UTIME_OMIT {
        return Ok(());
    }
    if nanoseconds == libc::UTIME_NOW {
        *field = MetadataField::Value(system_time_ns(SystemTime::now())?);
        return Ok(());
    }
    if !(0..1_000_000_000).contains(&nanoseconds) {
        return Err(libc::EINVAL);
    }
    let nanos = i128::from(seconds)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(nanoseconds)))
        .ok_or(libc::EOVERFLOW)?;
    *field = MetadataField::Value(i64::try_from(nanos).map_err(|_| libc::EOVERFLOW)?);
    Ok(())
}

pub(super) fn metadata_time(field: MetadataField<i64>) -> (i64, u32) {
    let nanos = match field {
        MetadataField::Unavailable => 0,
        MetadataField::Value(value) => value,
    };
    (
        nanos.div_euclid(1_000_000_000),
        u32::try_from(nanos.rem_euclid(1_000_000_000)).unwrap_or(0),
    )
}

pub(super) fn driver_errno(error: i32) -> NativeMountError {
    NativeMountError::Driver(std::io::Error::from_raw_os_error(error).to_string())
}

