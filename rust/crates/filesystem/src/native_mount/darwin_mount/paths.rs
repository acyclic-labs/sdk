use super::*;

pub(super) fn clone_path(
    path: &MountPath,
    work: &crate::NativeMaterializationWork,
) -> Result<DarwinOwned<MountPath>, NativeMountError> {
    let slots = path
        .components()
        .len()
        .checked_mul(std::mem::size_of::<Vec<u8>>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let bytes = path
        .components()
        .iter()
        .try_fold(slots, |bytes, component| {
            work.charge(scan_work(1))?;
            bytes
                .checked_add(component.len() as u64)
                .ok_or_else(|| NativeMountError::from(crate::WorkError::Overflow))
        })?;
    owned(work, bytes, || {
        copying(work, copy_work(bytes), || path.clone())
    })
}

pub(super) fn child_path(
    path: &MountPath,
    name: &[u8],
    work: &crate::NativeMaterializationWork,
) -> Result<DarwinOwned<MountPath>, NativeMountError> {
    let count = path.components().len();
    let slots = count
        .checked_add(
            count
                .checked_mul(2)
                .ok_or(crate::WorkError::Overflow)?
                .max(4),
        )
        .and_then(|n| n.checked_mul(std::mem::size_of::<Vec<u8>>()))
        .ok_or(crate::WorkError::Overflow)? as u64;
    let bytes = path
        .components()
        .iter()
        .try_fold(slots, |bytes, component| {
            bytes
                .checked_add(component.len() as u64)
                .ok_or(crate::WorkError::Overflow)
        })?
        .checked_add(name.len() as u64)
        .ok_or(crate::WorkError::Overflow)?;
    let copied = bytes
        .checked_sub(slots)
        .and_then(|names| {
            count
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .and_then(|n| n.checked_mul(std::mem::size_of::<Vec<u8>>()))
                .and_then(|slots| names.checked_add(slots as u64))
        })
        .ok_or(crate::WorkError::Overflow)?;
    let mut child = owned(work, bytes, || {
        copying(
            work,
            crate::WorkCounters {
                items_examined: count as u64,
                bytes_copied: copied,
                ..crate::WorkCounters::UNCHARGED
            },
            || path.child(name.to_vec()),
        )
    })?;
    let retained = child
        .components
        .capacity()
        .checked_mul(std::mem::size_of::<Vec<u8>>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let retained = child
        .components
        .iter()
        .try_fold(retained, |bytes, component| {
            bytes
                .checked_add(component.capacity() as u64)
                .ok_or(crate::WorkError::Overflow)
        })?;
    child
        .allocation
        .as_mut()
        .expect("funded child")
        .reconcile_capacity(retained)?;
    Ok(child)
}

pub(super) fn native_name(
    name: &[u8],
    work: &crate::NativeMaterializationWork,
) -> Result<DarwinOwned<CString>, NativeMountError> {
    work.charge(scan_work(name.len() as u64))?;
    if name.contains(&0) {
        return Err(driver_errno(libc::EIO));
    }
    let bytes = name
        .len()
        .checked_add(1)
        .ok_or(crate::WorkError::Overflow)? as u64;
    let mut value = owned(work, bytes, || {
        let mut value = Vec::new();
        value.try_reserve_exact(bytes as usize)?;
        Ok(value)
    })?;
    let extra = (value.capacity() as u64)
        .checked_sub(bytes)
        .ok_or(crate::WorkError::Overflow)?;
    value.claim_more(extra, work)?;
    copying(work, copy_work(bytes), || {
        value.extend_from_slice(name);
        value.push(0);
    })?;
    Ok(value.map(|value| CString::from_vec_with_nul(value).expect("validated native name")))
}

pub(super) fn mount_path(
    path: *const c_char,
    work: &crate::NativeMaterializationWork,
) -> Result<DarwinOwned<MountPath>, NativeMountError> {
    let bytes = c_bytes(path, work)?;
    if bytes.first().copied() != Some(b'/') {
        return Err(driver_errno(libc::EINVAL));
    }
    work.charge(scan_work(bytes.len() as u64))?;
    let count = bytes
        .split(|byte| *byte == b'/')
        .filter(|part| !part.is_empty())
        .count();
    let slots = count
        .checked_mul(std::mem::size_of::<Vec<u8>>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let mut path = owned(work, slots, || {
        let mut components = Vec::new();
        components.try_reserve_exact(count)?;
        Ok(MountPath { components })
    })?;
    work.charge(scan_work(bytes.len() as u64))?;
    for component in bytes
        .split(|byte| *byte == b'/')
        .filter(|part| !part.is_empty())
    {
        if component == b"." || component == b".." {
            return Err(driver_errno(libc::EPERM));
        }
        path.claim_more(component.len() as u64, work)?;
        let mut name = Vec::new();
        name.try_reserve_exact(component.len())?;
        path.claim_more((name.capacity() - component.len()) as u64, work)?;
        copying(work, copy_work(component.len() as u64), || {
            name.extend_from_slice(component);
            path.components.push(name);
        })?;
    }
    Ok(path)
}

pub(super) fn c_bytes<'a>(
    value: *const c_char,
    work: &crate::NativeMaterializationWork,
) -> Result<&'a [u8], NativeMountError> {
    if value.is_null() {
        return Err(driver_errno(libc::EINVAL));
    }
    let mut length = 0usize;
    loop {
        work.charge(scan_work(1))?;
        // SAFETY: the bridge retains each terminated string through its callback.
        if unsafe { value.add(length).read() } == 0 {
            break;
        }
        length = length.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    // SAFETY: the bounded scan read this initialized prefix before its terminator.
    Ok(unsafe { std::slice::from_raw_parts(value.cast(), length) })
}

pub(super) fn bounded_length(length: usize) -> Result<u32, i32> {
    if length > MAXIMUM_CALLBACK_BYTES {
        return Err(libc::E2BIG);
    }
    u32::try_from(length).map_err(|_| libc::EOVERFLOW)
}

pub(super) fn copy_variable_result(
    bytes: &[u8],
    output: *mut c_char,
    length: usize,
    work: &crate::NativeMaterializationWork,
) -> Result<c_int, NativeMountError> {
    if length == 0 {
        return c_int::try_from(bytes.len()).map_err(|_| driver_errno(libc::EOVERFLOW));
    }
    if output.is_null() || bytes.len() > length {
        return Err(driver_errno(libc::ERANGE));
    }
    // SAFETY: `output` is a non-null bridge buffer of `length` bytes and `bytes` fits in it (checked above).
    copying(work, copy_work(bytes.len() as u64), || unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), output.cast(), bytes.len())
    })?;
    c_int::try_from(bytes.len()).map_err(|_| driver_errno(libc::EOVERFLOW))
}

pub(super) fn update_time(
    field: &mut MetadataField<i64>,
    seconds: i64,
    nanoseconds: i64,
) -> Result<(), i32> {
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
    NativeMountError::Io(std::io::Error::from_raw_os_error(error))
}
