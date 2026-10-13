use super::*;

pub(super) fn capture_drop_host_path(
    path: CaptureHostPath,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    #[cfg(unix)]
    let bytes = path.capacity() as u64;
    #[cfg(windows)]
    let bytes = path.capacity() as u64 * 2;
    drop(path);
    meter
        .native
        .clone()
        .release(bytes)
        .map_err(capture_demand_error)
}

#[cfg(unix)]
pub(super) type CaptureHostPath = PathBuf;

#[cfg(windows)]
pub(super) type CaptureHostPath = Vec<u16>;

pub(super) fn namespace_to_host_path_measured(
    path: &NamespacePath,
    meter: &CaptureWorkScope,
) -> Result<CaptureHostPath, CaptureError> {
    #[cfg(unix)]
    let result = namespace_to_host_path_with_work(path, &mut meter.native.clone())
        .map(|(path, _allocation)| path);
    #[cfg(windows)]
    let result = namespace_host_units_with_work(
        path,
        &mut meter.native.clone(),
        u16::from(b'\\'),
        namespace_component_wide,
    )
    .map(|(units, _allocation)| units);
    result.map_err(|error| match error {
        DemandError::InvalidRequest => CaptureError::UnrepresentablePath,
        error => capture_demand_error(error),
    })
}

/// Converts the canonical namespace with the same bounded component scanner used by capture.
pub(crate) fn namespace_to_host_path_with_work<W: crate::native_host::NativeHostWork>(
    path: &NamespacePath,
    work: &mut W,
) -> Result<(PathBuf, W::Allocation), W::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        let (bytes, allocation) =
            namespace_host_units_with_work(path, work, b'/', namespace_component_bytes)?;
        Ok((std::ffi::OsString::from_vec(bytes).into(), allocation))
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt as _;
        let (wide, wide_allocation) =
            namespace_host_units_with_work(path, work, u16::from(b'\\'), namespace_component_wide)?;
        let reserved = (wide.len() as u64)
            .checked_mul(3)
            .ok_or(WorkError::Overflow)?;
        if let Err(error) = work.admit(WorkCounters {
            bytes_encoded: reserved,
            ..WorkCounters::UNCHARGED
        }) {
            drop(wide);
            let _retirement = work.release(wide_allocation);
            return Err(error);
        }
        let allocation = work.reserve_opaque_decode(reserved);
        let mut allocation = match allocation {
            Ok(allocation) => allocation,
            Err(error) => {
                drop(wide);
                let _retirement = work.release(wide_allocation);
                return Err(error);
            }
        };
        // Reserve before opaque UTF-16 decoding, then reconcile the owned output.
        let native = std::ffi::OsString::from_wide(&wide);
        let reconciled = work.reconcile_allocation(&mut allocation, native.capacity() as u64);
        let encoded = work.record(WorkCounters {
            bytes_encoded: native.as_encoded_bytes().len() as u64,
            ..WorkCounters::UNCHARGED
        });
        drop(wide);
        let retired = work.release(wide_allocation);
        let completed = reconciled
            .and(encoded)
            .and(retired)
            .and_then(|()| work.check());
        if let Err(error) = completed {
            drop(native);
            let _retirement = work.release(allocation);
            return Err(error);
        }
        Ok((native.into(), allocation))
    }
}

pub(crate) fn namespace_host_units_with_work<W: crate::native_host::NativeHostWork, T: Copy>(
    path: &NamespacePath,
    work: &mut W,
    separator: T,
    encode: impl Fn(
        &LogicalName,
        &mut W,
        &mut dyn FnMut(T, &mut W) -> Result<(), W::Error>,
    ) -> Result<(), W::Error>,
) -> Result<(Vec<T>, W::Allocation), W::Error> {
    let mut count = 0usize;
    for (index, component) in path.components().iter().enumerate() {
        work.check()?;
        let examined = WorkCounters {
            items_examined: component.as_bytes().len() as u64,
            source_path_components: 1,
            ..WorkCounters::UNCHARGED
        };
        work.admit(examined)?;
        work.record(examined)?;
        if index != 0 {
            count = count.checked_add(1).ok_or(WorkError::Overflow)?;
        }
        encode(component, work, &mut |_, _| {
            count = count.checked_add(1).ok_or(WorkError::Overflow)?;
            Ok(())
        })?;
    }
    let width = std::mem::size_of::<T>();
    let bytes = count.checked_mul(width).ok_or(WorkError::Overflow)?;
    let copied = WorkCounters {
        bytes_copied: bytes as u64,
        source_path_components: path.depth() as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(copied)?;
    let mut allocation = work.claim(bytes as u64)?;
    let mut buffer = Vec::new();
    if let Err(error) = buffer.try_reserve_exact(count) {
        drop(buffer);
        let _retirement = work.release(allocation);
        return Err(W::allocation_failed(error));
    }
    let result = (|| {
        let actual = buffer
            .capacity()
            .checked_mul(width)
            .ok_or(WorkError::Overflow)?;
        work.reconcile_allocation(&mut allocation, actual as u64)?;
        for (index, component) in path.components().iter().enumerate() {
            work.check()?;
            let examined = WorkCounters {
                items_examined: component.as_bytes().len() as u64,
                source_path_components: 1,
                ..WorkCounters::UNCHARGED
            };
            work.admit(examined)?;
            work.record(examined)?;
            if index != 0 {
                buffer.push(separator);
                work.record(WorkCounters {
                    bytes_copied: width as u64,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            encode(component, work, &mut |unit, work| {
                buffer.push(unit);
                work.record(WorkCounters {
                    bytes_copied: width as u64,
                    ..WorkCounters::UNCHARGED
                })
            })?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        drop(buffer);
        let _retirement = work.release(allocation);
        return Err(error);
    }
    Ok((buffer, allocation))
}

#[cfg(unix)]
pub(super) fn namespace_component_bytes<W: crate::native_host::NativeHostWork>(
    component: &LogicalName,
    work: &mut W,
    emit: &mut dyn FnMut(u8, &mut W) -> Result<(), W::Error>,
) -> Result<(), W::Error> {
    match component.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            for byte in component.as_bytes() {
                work.check()?;
                emit(*byte, work)?;
            }
        }
        NameEncoding::WindowsUtf16Le => {
            let units = crate::kernel::types::utf16le_units(component.as_bytes())
                .ok_or_else(W::invalid_request)?;
            for character in char::decode_utf16(units) {
                work.check()?;
                let character = character.map_err(|_| W::invalid_request())?;
                let mut bytes = [0; 4];
                for byte in character.encode_utf8(&mut bytes).as_bytes() {
                    emit(*byte, work)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn namespace_component_wide<W: crate::native_host::NativeHostWork>(
    component: &LogicalName,
    work: &mut W,
    emit: &mut dyn FnMut(u16, &mut W) -> Result<(), W::Error>,
) -> Result<(), W::Error> {
    match component.encoding() {
        NameEncoding::WindowsUtf16Le => {
            for unit in crate::kernel::types::utf16le_units(component.as_bytes())
                .ok_or_else(W::invalid_request)?
            {
                work.check()?;
                emit(unit, work)?;
            }
        }
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            let text =
                std::str::from_utf8(component.as_bytes()).map_err(|_| W::invalid_request())?;
            if text.contains(['/', '\\']) {
                return Err(W::invalid_request());
            }
            for unit in text.encode_utf16() {
                work.check()?;
                emit(unit, work)?;
            }
        }
    }
    Ok(())
}

pub(super) fn logical_host_name(
    name: &std::ffi::OsStr,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
) -> Result<LogicalName, CaptureError> {
    let (encoding, bytes) =
        crate::native_name::host_name_bytes(name, profile, limits.maximum_component_bytes)
            .map_err(|_| CaptureError::UnrepresentablePath)?;
    LogicalName::new(encoding, bytes, limits.maximum_component_bytes).map_err(CaptureError::from)
}

/// Converts one host-relative path to an exact bounded namespace path.
pub fn host_path_to_namespace(
    path: &std::path::Path,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
) -> Result<NamespacePath, CaptureError> {
    use std::path::Component;

    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => components.push(logical_host_name(name, profile, limits)?),
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                return Err(CaptureError::UnrepresentablePath);
            }
        }
    }
    if components.is_empty() {
        return Err(CaptureError::UnrepresentablePath);
    }
    NamespacePath::new(components, limits).map_err(CaptureError::from)
}

/// Converts one exact namespace path to a host-relative path without lossy text projection.
pub fn namespace_to_host_path(path: &NamespacePath) -> Result<PathBuf, CaptureError> {
    let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    namespace_to_host_path_with_work(path, &mut work)
        .map(|(path, _allocation)| path)
        .map_err(|error| match error {
            DemandError::InvalidRequest => CaptureError::UnrepresentablePath,
            error => capture_demand_error(error),
        })
}

pub(super) fn capture_link_target(
    target: Vec<u8>,
    meter: &CaptureWorkScope,
) -> Result<bytes::Bytes, CaptureError> {
    meter.native.clone().check().map_err(capture_demand_error)?;
    if target.len() != target.capacity() {
        meter
            .native
            .with_original_leaf(|work| work.claim_allocation(0, 1, crate::WorkCounters::UNCHARGED))
            .map_err(capture_demand_error)?;
    }
    Ok(bytes::Bytes::from(target))
}

pub(super) fn read_link_bytes(
    root: &HostRoot,
    path: &CaptureHostPath,
    meter: &CaptureWorkScope,
) -> Result<Vec<u8>, CaptureError> {
    let maximum = meter
        .native
        .with_original_leaf(|meter| meter.remaining())
        .map_err(capture_demand_error)?
        .source_bytes_read;
    #[cfg(unix)]
    {
        // The returned byte buffer keeps the driver's existing capture claim.
        let (bytes, _allocation) = root
            .read_link(path, maximum, &mut meter.native.clone())
            .map_err(capture_demand_error)?;
        Ok(bytes)
    }
    #[cfg(windows)]
    {
        let (units, allocation) = root
            .read_link_wide(path, maximum, &mut meter.native.clone())
            .map_err(capture_demand_error)?;
        let length = units
            .len()
            .checked_mul(2)
            .ok_or(CaptureError::from(WorkError::Overflow))?;
        let mut bytes = capture_vec(length, meter)?;
        for unit in &units {
            capture_charge(
                meter,
                WorkCounters {
                    bytes_copied: 2,
                    ..WorkCounters::UNCHARGED
                },
            )?;
            bytes.extend_from_slice(&unit.to_le_bytes());
        }
        drop(units);
        meter
            .native
            .clone()
            .release(allocation)
            .map_err(capture_demand_error)?;
        Ok(bytes)
    }
}
