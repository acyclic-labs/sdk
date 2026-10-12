use super::*;

pub(in crate::native_mount::lazy) fn mount_path_text_measure(
    root: &str, path: &MountPath,
    mut examine: impl FnMut() -> Result<(), MountSourceError>,
    invalid: fn(&'static str) -> MountSourceError,
    unsupported: fn(&'static str) -> MountSourceError,
) -> Result<(usize, u64), MountSourceError> {
    let prefix = if path.components().is_empty() || root != "/" { root } else { "" };
    let mut length = prefix.len();
    let mut fill_items = 0_u64;
    for component in path.components() {
        examine()?;
        fill_items = fill_items.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        length = length.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        if cfg!(target_os = "windows") {
            let units = crate::kernel::types::utf16le_units(component)
                .ok_or_else(|| invalid("native UTF-16 name has an odd byte length"))?;
            for scalar in char::decode_utf16(units) {
                examine()?;
                fill_items = fill_items.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                let scalar = scalar.map_err(|_| unsupported(
                    "lazy portable mount cannot represent an unpaired UTF-16 name",
                ))?;
                length = length.checked_add(scalar.len_utf8())
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
        } else {
            std::str::from_utf8(component).map_err(|_| unsupported(
                "lazy portable mount cannot represent a non-UTF-8 name",
            ))?;
            length = length.checked_add(component.len())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        }
    }
    Ok((length, fill_items))
}

pub(in crate::native_mount::lazy) fn mount_path_text_fill(root: &str, path: &MountPath, mut bytes: Vec<u8>) -> String {
    let prefix = if path.components().is_empty() || root != "/" { root } else { "" };
    bytes.extend_from_slice(prefix.as_bytes());
    for component in path.components() {
        bytes.push(b'/');
        if cfg!(target_os = "windows") {
            let units = crate::kernel::types::utf16le_units(component)
                .expect("the immutable native name was validated before allocation");
            for scalar in char::decode_utf16(units) {
                let scalar = scalar.expect("the immutable UTF-16 scalar was validated before allocation");
                let mut encoded = [0_u8; 4];
                bytes.extend_from_slice(scalar.encode_utf8(&mut encoded).as_bytes());
            }
        } else {
            bytes.extend_from_slice(component);
        }
    }
    String::from_utf8(bytes).expect("the path contains only validated UTF-8")
}

pub(in crate::native_mount::lazy) fn mount_path_text(
    root: &str, path: &MountPath, work: &mut crate::demand::DemandWork,
) -> Result<String, MountSourceError> {
    let (length, fill_items) = mount_path_text_measure(
        root, path, || mount_items(work, 1),
        |text| MountSourceError::Invalid(text.to_owned()),
        |text| MountSourceError::Unsupported(text.to_owned()),
    )?;
    let copied = crate::WorkCounters {
        bytes_copied: length as u64, items_examined: fill_items,
        ..crate::WorkCounters::UNCHARGED
    };
    work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
    let bytes = mount_vec::<u8>(length, work)?;
    let text = mount_path_text_fill(root, path, bytes);
    work.record(copied).map_err(mount_demand_error)?;
    Ok(text)
}

pub(in crate::native_mount::lazy) fn mount_path_text_with_carrier(
    root: &str, path: &MountPath,
    carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
) -> Result<MountOwnedBuffer<String>, MountSourceError> {
    let Some(original) = carrier.original_native_work() else {
        return carrier.with_work(|work| mount_path_text(root, path, work))
            .map(|text| MountOwnedBuffer { legacy_bytes: text.capacity() as u64, value: text, allocation: None });
    };
    let (length, fill_items) = mount_path_text_measure(
        root, path,
        || original.charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(MountSourceError::NativeWork),
        |text| MountSourceError::NativeText { kind: crate::native_mount::NativeSourceTextKind::Invalid, text },
        |text| MountSourceError::NativeText { kind: crate::native_mount::NativeSourceTextKind::Unsupported, text },
    )?;
    let mut copied = original.reserve_work(crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
    let mut examined = original.reserve_work(crate::WorkCounters { items_examined: fill_items, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
    let allocation = original.claim(length as u64, u64::from((length as u64) != 0), crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(MountSourceError::Allocation)?;
    let text = mount_path_text_fill(root, path, bytes);
    let recorded_copy = { let mut copy = copied; copy.record_completed(crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED }) }.map_err(MountSourceError::NativeWork);
    let recorded_items = examined.record_completed(crate::WorkCounters { items_examined: fill_items, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork);
    recorded_copy.and(recorded_items)?;
    Ok(MountOwnedBuffer { value: text, legacy_bytes: 0, allocation: Some(allocation) })
}

pub(in crate::native_mount::lazy) fn mount_namespace(
    path: &str,
    config: crate::VolumeConfig,
    work: &mut crate::demand::DemandWork,
) -> Result<crate::kernel::NamespacePath, MountSourceError> {
    let mut owned_bytes = 0_u64;
    let prepared = (|| {
        if !path.starts_with('/') || (path != "/" && path.ends_with('/')) {
            return Err(MountSourceError::Invalid("noncanonical removal path".to_owned()));
        }
        let mut count = 0_usize;
        for name in path.split('/').skip(1) {
            if name.is_empty() && path == "/" { continue; }
            mount_items(work, 1)?;
            count = count.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        }
        let mut names = mount_vec(count, work)?;
        owned_bytes = (names.capacity() * std::mem::size_of::<crate::kernel::LogicalName>()) as u64;
        for name in path.split('/').skip(1) {
            if name.is_empty() && path == "/" { continue; }
            mount_items(work, 1)?;
            let (encoding, length) = match config.profile {
                crate::FilesystemProfile::Portable | crate::FilesystemProfile::Browser =>
                    (crate::kernel::NameEncoding::Utf8, name.len()),
                crate::FilesystemProfile::Posix =>
                    (crate::kernel::NameEncoding::PosixBytes, name.len()),
                crate::FilesystemProfile::Windows =>
                    (crate::kernel::NameEncoding::WindowsUtf16Le, name.encode_utf16().count() * 2),
            };
            let mut bytes = mount_vec::<u8>(length, work)?;
            owned_bytes = owned_bytes.checked_add(bytes.capacity() as u64)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            work.charge(crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::default() })
                .map_err(mount_demand_error)?;
            if encoding == crate::kernel::NameEncoding::WindowsUtf16Le {
                bytes.extend(name.encode_utf16().flat_map(u16::to_le_bytes));
            } else {
                bytes.extend_from_slice(name.as_bytes());
            }
            names.push(crate::kernel::LogicalName::new(encoding, bytes, config.limits.maximum_component_bytes)
                .map_err(|error| MountSourceError::Invalid(error.to_string()))?);
        }
        crate::kernel::NamespacePath::new(names, config.limits)
            .map_err(|error| MountSourceError::Invalid(error.to_string()))
    })();
    if prepared.is_err() { work.release(owned_bytes).map_err(mount_demand_error)?; }
    prepared
}

pub(in crate::native_mount::lazy) fn mount_effect_bytes(effect: &ViewEffect) -> u64 {
    (effect.nodes.capacity() * std::mem::size_of::<FileId>()
        + effect.names.capacity() * std::mem::size_of::<(crate::kernel::NamespacePath, crate::kernel::LogicalName)>()
        + (effect.subtrees.capacity() + effect.listings.capacity()) * std::mem::size_of::<crate::kernel::NamespacePath>()) as u64
        + effect.names.iter().map(|(path, name)| mount_namespace_bytes(path) + name.as_bytes().len() as u64).sum::<u64>()
        + effect.subtrees.iter().chain(&effect.listings).map(mount_namespace_bytes).sum::<u64>()
}

pub(in crate::native_mount::lazy) fn mount_components_with_scratch<S: crate::native_mount::adapter::CaptureInputScratch>(
    source: &[Vec<u8>], extra: Option<&[u8]>, scratch: &mut S,
) -> Result<MountPath, MountSourceError> {
    let count = source.len().checked_add(usize::from(extra.is_some()))
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    let mut components = scratch.vec::<Vec<u8>>(count)?;
    for component in source.iter().map(Vec::as_slice).chain(extra.into_iter()) {
        scratch.items(1)?;
        let delta = crate::WorkCounters { bytes_copied: component.len() as u64,
            ..crate::WorkCounters::UNCHARGED };
        scratch.admit(delta)?;
        let mut copied = scratch.vec::<u8>(component.len())?;
        copied.extend_from_slice(component);
        scratch.record(delta)?;
        components.push(copied);
    }
    Ok(MountPath { components })
}

pub(in crate::native_mount::lazy) fn mount_components_with_work(
    source: &[Vec<u8>], extra: Option<&[u8]>, work: &mut crate::demand::DemandWork,
) -> Result<(MountPath, u64), MountSourceError> {
    let mut scratch = crate::native_mount::adapter::CaptureScratchAllocation::new(work);
    let path = mount_components_with_scratch(source, extra, &mut scratch)?;
    Ok((path, scratch.retain()))
}

pub(in crate::native_mount::lazy) struct MountOwnedBuffer<T> {
    pub(in crate::native_mount::lazy) value: T,
    pub(in crate::native_mount::lazy) legacy_bytes: u64,
    pub(in crate::native_mount::lazy) allocation: Option<crate::NativeAllocation>,
}

impl<T> std::ops::Deref for MountOwnedBuffer<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.value }
}

impl<T> MountOwnedBuffer<T> {
    pub(in crate::native_mount::lazy) fn legacy_owned_bytes(&self) -> u64 {
        if self.allocation.is_some() { 0 } else { self.legacy_bytes }
    }

    pub(in crate::native_mount::lazy) fn into_parts(self) -> (T, Option<crate::NativeAllocation>, u64) {
        (self.value, self.allocation, self.legacy_bytes)
    }
}

pub(in crate::native_mount::lazy) fn mount_components_with_carrier(
    source: &[Vec<u8>], extra: Option<&[u8]>,
    carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
) -> Result<MountOwnedBuffer<MountPath>, MountSourceError> {
    if let Some(original) = carrier.original_native_work() {
        let mut scratch = crate::native_mount::adapter::NativeCaptureScratchAllocation::new(&original)?;
        let path = mount_components_with_scratch(source, extra, &mut scratch)?;
        Ok(MountOwnedBuffer { value: path, legacy_bytes: 0, allocation: Some(scratch.retain()) })
    } else {
        let (path, bytes) = carrier.with_work(|work| mount_components_with_work(source, extra, work))?;
        Ok(MountOwnedBuffer { value: path, legacy_bytes: bytes, allocation: None })
    }
}

pub(in crate::native_mount::lazy) fn mount_name_with_carrier(
    name: &crate::kernel::LogicalName,
    carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
) -> Result<MountOwnedBuffer<Vec<u8>>, MountSourceError> {
    if carrier.original_native_work().is_some() {
        let (name, allocation) = crate::native_mount::adapter::native_mount_name(name, carrier)?;
        Ok(MountOwnedBuffer { value: name, legacy_bytes: 0, allocation })
    } else {
        let name = crate::native_mount::adapter::native_mount_name(name, carrier)?.0;
        let bytes = name.capacity() as u64;
        Ok(MountOwnedBuffer { value: name, legacy_bytes: bytes, allocation: None })
    }
}
