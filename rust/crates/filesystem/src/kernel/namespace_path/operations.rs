use super::super::{codec::ReadAllocationStep, file_read::OriginalReadAccounting};
use super::*;

fn components(path: &str) -> impl Iterator<Item = &str> {
    path.strip_prefix('/')
        .unwrap_or_default()
        .split('/')
        .filter(|part| !part.is_empty())
}

impl NamespacePath {
    pub(crate) fn validate_component_lengths(
        lengths: impl IntoIterator<Item = usize>,
        limits: VolumeLimits,
    ) -> Result<u32, NamespacePathError> {
        let mut depth = 0;
        let mut encoded = 1_u32;
        for length in lengths {
            depth += 1;
            if depth > usize::from(limits.maximum_path_depth) {
                return Err(NamespacePathError::TooDeep);
            }
            let length = u32::try_from(length).map_err(|_| NamespacePathError::ComponentTooLong)?;
            if length > limits.maximum_component_bytes {
                return Err(NamespacePathError::ComponentTooLong);
            }
            encoded = encoded
                .checked_add(length)
                .and_then(|n| n.checked_add(1))
                .ok_or(NamespacePathError::PathTooLong)?;
        }
        if depth != 0 {
            encoded -= 1;
        }
        if encoded > limits.maximum_path_bytes {
            return Err(NamespacePathError::PathTooLong);
        }
        Ok(encoded)
    }

    pub(crate) fn validate_in_profile_with_accounting(
        path: &str,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        validate(path, profile, limits, accounting, cancellation).map(|_| ())
    }

    /// Parses a namespace path on the supplied accounting ledger.
    pub fn parse_in_profile(
        path: &str,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        let (depth, encoding, encoded_bytes) =
            validate(path, profile, limits, accounting, cancellation)?;
        let mut owned = OriginalReadAccounting::new(accounting)?;
        let result = (|| {
            let mut names = vector(depth, &mut owned, cancellation)?;
            for component in components(path) {
                cancellation.check()?;
                let length = if encoding == NameEncoding::WindowsUtf16Le {
                    component.encode_utf16().count() * 2
                } else {
                    component.len()
                };
                let delta = if encoding == NameEncoding::WindowsUtf16Le {
                    WorkCounters {
                        bytes_encoded: length as u64,
                        ..WorkCounters::UNCHARGED
                    }
                } else {
                    WorkCounters {
                        bytes_copied: length as u64,
                        ..WorkCounters::UNCHARGED
                    }
                };
                owned.admit(delta)?;
                let mut bytes = vector(length, &mut owned, cancellation)?;
                if encoding == NameEncoding::WindowsUtf16Le {
                    bytes.extend(component.encode_utf16().flat_map(u16::to_le_bytes));
                } else {
                    bytes.extend_from_slice(component.as_bytes());
                }
                owned.record_completed(delta)?;
                names.push(LogicalName::from_validated_bytes(encoding, bytes));
            }
            Ok(Self {
                components: names,
                encoded_bytes,
            })
        })();
        finish_owned(owned, result)
    }

    pub(crate) fn child_with_accounting(
        &self,
        name: &LogicalName,
        limits: VolumeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        accounting.check()?;
        cancellation.check()?;
        let names = self.components.iter().chain(std::iter::once(name));
        let encoded = Self::validate_component_lengths(
            names.clone().map(|name| name.as_bytes().len()),
            limits,
        )?;
        copy_names(names, self.depth() + 1, encoded, accounting, cancellation)
    }

    pub(crate) fn clone_with_accounting(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        copy_names(
            self.components.iter(),
            self.depth(),
            self.encoded_bytes,
            accounting,
            cancellation,
        )
    }

    pub(crate) fn prefix_with_accounting(
        &self,
        depth: usize,
        limits: VolumeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        accounting.check()?;
        cancellation.check()?;
        let names = self
            .components
            .get(..depth)
            .ok_or(NamespacePathError::TooDeep)?;
        let encoded = Self::validate_component_lengths(
            names.iter().map(|name| name.as_bytes().len()),
            limits,
        )?;
        copy_names(names.iter(), depth, encoded, accounting, cancellation)
    }

    pub(crate) fn unicode_text_with_accounting(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(String, KernelOwnedAllocation), crate::FsError> {
        accounting.check()?;
        let mut length = usize::from(self.is_root());
        for name in &self.components {
            cancellation.check()?;
            accounting.charge_items(1)?;
            let bytes = name
                .unicode_utf8_bytes()
                .ok_or(NamespacePathError::NonUnicodeProjection)?;
            length = length
                .checked_add(1)
                .and_then(|n| n.checked_add(bytes))
                .ok_or(crate::WorkError::Overflow)?;
        }
        let delta = WorkCounters {
            bytes_encoded: length as u64,
            output_bytes: length as u64,
            ..WorkCounters::UNCHARGED
        };
        let mut owned = OriginalReadAccounting::new(accounting)?;
        let result = (|| {
            owned.admit(delta)?;
            let bytes = vector::<u8>(length, &mut owned, cancellation)?;
            let mut output =
                String::from_utf8(bytes).map_err(|_| NamespacePathError::NonUnicodeProjection)?;
            if self.is_root() {
                output.push('/');
                owned.record_completed(delta)?;
            }
            for name in &self.components {
                cancellation.check()?;
                let before = output.len();
                output.push('/');
                name.append_unicode(&mut output)
                    .map_err(NamespacePathError::Name)?;
                let bytes = (output.len() - before) as u64;
                owned.record_completed(WorkCounters {
                    bytes_encoded: bytes,
                    output_bytes: bytes,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            Ok(output)
        })();
        finish_owned(owned, result)
    }
}

fn validate(
    path: &str,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(usize, NameEncoding, u32), crate::FsError> {
    accounting.check()?;
    cancellation.check()?;
    let depth = PortablePath::validate(path, limits).map_err(NamespacePathError::Path)?;
    let encoding = NameEncoding::native_for_profile(profile);
    let mut encoded = 1_u32;
    for part in components(path) {
        cancellation.check()?;
        accounting.charge(WorkCounters {
            source_path_components: 1,
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let length = LogicalName::validate_unicode(encoding, part, limits.maximum_component_bytes)
            .map_err(NamespacePathError::Name)?;
        encoded = encoded
            .checked_add(u32::try_from(length).map_err(|_| NamespacePathError::PathTooLong)?)
            .and_then(|n| n.checked_add(1))
            .ok_or(NamespacePathError::PathTooLong)?;
    }
    if depth != 0 {
        encoded -= 1;
    }
    if encoded > limits.maximum_path_bytes {
        return Err(NamespacePathError::PathTooLong.into());
    }
    Ok((depth, encoding, encoded))
}

fn vector<T>(
    capacity: usize,
    owned: &mut OriginalReadAccounting<'_, '_>,
    cancellation: &CancellationToken,
) -> Result<Vec<T>, crate::FsError> {
    cancellation.check()?;
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    owned.observe(ReadAllocationStep::Allocate {
        bytes,
        operations: u64::from(capacity != 0),
    })?;
    let mut values = Vec::new();
    if let Err(error) = values.try_reserve_exact(capacity) {
        owned.observe(ReadAllocationStep::ReserveFailed(error))?;
    }
    owned.observe(ReadAllocationStep::Capacity {
        admitted_bytes: bytes,
        actual_bytes: (values.capacity() * std::mem::size_of::<T>()) as u64,
    })?;
    Ok(values)
}

fn copy_names<'a>(
    names: impl Iterator<Item = &'a LogicalName> + Clone,
    depth: usize,
    encoded_bytes: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(NamespacePath, KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    for _ in names.clone() {
        cancellation.check()?;
        accounting.charge(WorkCounters {
            source_path_components: 1,
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
    }
    let mut owned = OriginalReadAccounting::new(accounting)?;
    let result = (|| {
        let mut components = vector(depth, &mut owned, cancellation)?;
        for name in names {
            cancellation.check()?;
            let delta = WorkCounters {
                bytes_copied: name.as_bytes().len() as u64,
                ..WorkCounters::UNCHARGED
            };
            owned.admit(delta)?;
            let mut bytes = vector(name.as_bytes().len(), &mut owned, cancellation)?;
            bytes.extend_from_slice(name.as_bytes());
            owned.record_completed(delta)?;
            components.push(LogicalName::from_validated_bytes(name.encoding(), bytes));
        }
        Ok(NamespacePath {
            components,
            encoded_bytes,
        })
    })();
    finish_owned(owned, result)
}

pub(super) fn path_error(error: crate::FsError) -> NamespacePathError {
    match error {
        crate::FsError::Work(error) => NamespacePathError::Work(error),
        crate::FsError::Cancelled(_) => NamespacePathError::Cancelled,
        crate::FsError::Path(super::super::PathLookupError::NamespacePath(error)) => error,
        _ => NamespacePathError::AllocationFailed,
    }
}
