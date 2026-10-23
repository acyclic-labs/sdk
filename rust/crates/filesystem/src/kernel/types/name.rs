use super::*;

/// Canonical host-name representation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NameEncoding {
    /// Portable UTF-8 scalar sequence.
    Utf8,
    /// Raw POSIX bytes excluding NUL and `/`.
    PosixBytes,
    /// Little-endian UTF-16 code units excluding NUL, `/`, and `\`.
    WindowsUtf16Le,
}

impl NameEncoding {
    pub(crate) const fn native_for_profile(profile: FilesystemProfile) -> Self {
        match profile {
            FilesystemProfile::Portable | FilesystemProfile::Browser => Self::Utf8,
            FilesystemProfile::Posix => Self::PosixBytes,
            FilesystemProfile::Windows => Self::WindowsUtf16Le,
        }
    }

    pub(crate) fn write_unicode_case_fold_key(
        self,
        text: &str,
        sink: &mut impl std::fmt::Write,
    ) -> std::fmt::Result {
        match self {
            Self::Utf8 => super::case_fold::write_key(text, sink),
            Self::PosixBytes | Self::WindowsUtf16Le => sink.write_str(text),
        }
    }

    pub(crate) fn unicode_case_fold_key(self, text: &str) -> std::borrow::Cow<'_, str> {
        match self {
            Self::Utf8 => super::case_fold::key(text),
            Self::PosixBytes | Self::WindowsUtf16Le => std::borrow::Cow::Borrowed(text),
        }
    }

    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::Utf8 => 1,
            Self::PosixBytes => 2,
            Self::WindowsUtf16Le => 3,
        }
    }

    pub(crate) const fn from_tag(tag: u8) -> Result<Self, TreePageError> {
        match tag {
            1 => Ok(Self::Utf8),
            2 => Ok(Self::PosixBytes),
            3 => Ok(Self::WindowsUtf16Le),
            value => Err(TreePageError::UnknownNameEncoding(value)),
        }
    }

    pub(crate) const fn is_admitted_in_profile(self, profile: FilesystemProfile) -> bool {
        matches!(
            (profile, self),
            (
                FilesystemProfile::Portable | FilesystemProfile::Browser,
                Self::Utf8
            ) | (FilesystemProfile::Posix, Self::Utf8 | Self::PosixBytes)
                | (
                    FilesystemProfile::Windows,
                    Self::Utf8 | Self::WindowsUtf16Le
                )
        )
    }
}

/// One separator-free canonical logical name.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalName {
    encoding: NameEncoding,
    bytes: Vec<u8>,
}

impl LogicalName {
    /// Owned encoded-name buffer capacity; excludes inline and allocator storage.
    #[must_use]
    pub fn logical_owned_bytes(&self) -> u64 {
        self.bytes.capacity() as u64
    }

    /// Validates a bounded name in its declared host representation.
    pub fn new(
        encoding: NameEncoding,
        bytes: Vec<u8>,
        maximum_bytes: u32,
    ) -> Result<Self, TreePageError> {
        Self::validate_bytes(encoding, &bytes, maximum_bytes)?;
        Ok(Self { encoding, bytes })
    }

    pub(crate) fn from_validated_bytes(encoding: NameEncoding, bytes: Vec<u8>) -> Self {
        Self { encoding, bytes }
    }

    pub(crate) fn try_clone<E: From<CanonicalDecodeError>>(
        &self,
        maximum_bytes: u32,
        steps: &mut impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self, E> {
        use crate::kernel::codec::ReadAllocationStep;
        let length = self.bytes.len();
        let mut bytes = Vec::new();
        steps(ReadAllocationStep::Allocate {
            bytes: length as u64,
            operations: 1,
        })?;
        if let Err(error) = bytes.try_reserve_exact(length) {
            steps(ReadAllocationStep::ReserveFailed(error))?;
            return Err(CanonicalDecodeError::AllocationFailed.into());
        }
        steps(ReadAllocationStep::Capacity {
            admitted_bytes: length as u64,
            actual_bytes: bytes.capacity() as u64,
        })?;
        steps(ReadAllocationStep::BeforeCopy(length as u64))?;
        bytes.extend_from_slice(&self.bytes);
        steps(ReadAllocationStep::Copied(length as u64))?;
        Self::new(self.encoding, bytes, maximum_bytes).map_err(|error| {
            E::from(CanonicalDecodeError::Invariant(
                crate::kernel::codec::CanonicalInvariant::Name(error),
            ))
        })
    }

    pub(crate) fn clone_with_accounting(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        let mut owned = OriginalReadAccounting::new(accounting)?;
        cancellation.check()?;
        let maximum_bytes = u32::try_from(self.bytes.len())
            .map_err(|_| NamespacePathError::Name(TreePageError::NameTooLarge))?;
        owned.observe(ReadAllocationStep::CheckCopy(self.bytes.len() as u64))?;
        let result = self.try_clone(maximum_bytes, &mut |step| owned.observe(step));
        finish_owned(owned, result)
    }

    fn validate_bytes(
        encoding: NameEncoding,
        bytes: &[u8],
        maximum_bytes: u32,
    ) -> Result<(), TreePageError> {
        if bytes.is_empty() {
            return Err(TreePageError::EmptyName);
        }
        if u32::try_from(bytes.len()).unwrap_or(u32::MAX) > maximum_bytes {
            return Err(TreePageError::NameTooLarge);
        }
        match encoding {
            NameEncoding::Utf8 => {
                let value = std::str::from_utf8(bytes).map_err(|_| TreePageError::InvalidName)?;
                if value == "." || value == ".." || value.contains('/') || value.contains('\0') {
                    return Err(TreePageError::InvalidName);
                }
            }
            NameEncoding::PosixBytes => {
                if bytes.contains(&0) || bytes.contains(&b'/') || bytes == b"." || bytes == b".." {
                    return Err(TreePageError::InvalidName);
                }
            }
            NameEncoding::WindowsUtf16Le => {
                let Some(mut units) = utf16le_units(bytes) else {
                    return Err(TreePageError::InvalidName);
                };
                if bytes == [b'.', 0]
                    || bytes == [b'.', 0, b'.', 0]
                    || units.any(invalid_windows_name_unit)
                {
                    return Err(TreePageError::InvalidName);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_unicode(
        encoding: NameEncoding,
        value: &str,
        maximum_bytes: u32,
    ) -> Result<usize, TreePageError> {
        if encoding != NameEncoding::WindowsUtf16Le {
            Self::validate_bytes(encoding, value.as_bytes(), maximum_bytes)?;
            return Ok(value.len());
        }
        if value.is_empty() {
            return Err(TreePageError::EmptyName);
        }
        let length = value
            .encode_utf16()
            .count()
            .checked_mul(2)
            .ok_or(TreePageError::NameTooLarge)?;
        if u32::try_from(length).unwrap_or(u32::MAX) > maximum_bytes {
            return Err(TreePageError::NameTooLarge);
        }
        if value == "." || value == ".." || value.encode_utf16().any(invalid_windows_name_unit) {
            return Err(TreePageError::InvalidName);
        }
        Ok(length)
    }

    pub(crate) fn unicode_utf8_bytes(&self) -> Option<usize> {
        match self.encoding {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                std::str::from_utf8(&self.bytes).ok().map(str::len)
            }
            NameEncoding::WindowsUtf16Le => char::decode_utf16(utf16le_units(&self.bytes)?)
                .try_fold(0_usize, |length, value| {
                    length.checked_add(value.ok()?.len_utf8())
                }),
        }
    }

    pub(crate) fn append_unicode(&self, output: &mut String) -> Result<(), TreePageError> {
        match self.encoding {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => output.push_str(
                std::str::from_utf8(&self.bytes).map_err(|_| TreePageError::InvalidName)?,
            ),
            NameEncoding::WindowsUtf16Le => {
                for value in char::decode_utf16(
                    utf16le_units(&self.bytes).ok_or(TreePageError::InvalidName)?,
                ) {
                    output.push(value.map_err(|_| TreePageError::InvalidName)?);
                }
            }
        }
        Ok(())
    }

    /// Returns the canonical representation kind.
    #[must_use]
    pub const fn encoding(&self) -> NameEncoding {
        self.encoding
    }

    /// Borrows canonical name bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Projects a non-lossy Unicode name under the supplied work scope.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "The public operation consumes its work scope and owned controls."
    )]
    pub fn unicode_text<W: crate::facade::FsWork>(
        &self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<String> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| {
            let mut owned = OriginalReadAccounting::new(&mut accounting)?;
            let result = (|| {
                let check = || {
                    cancellation
                        .check()
                        .map_err(|_| crate::FsError::from(NamespacePathError::Cancelled))
                };
                check()?;
                owned.observe(ReadAllocationStep::BeforeItem)?;
                owned.observe(ReadAllocationStep::Item)?;
                let borrowed = match self.encoding {
                    NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                        Some(std::str::from_utf8(&self.bytes).map_err(|_| {
                            crate::FsError::from(NamespacePathError::NonUnicodeProjection)
                        })?)
                    }
                    NameEncoding::WindowsUtf16Le => None,
                };
                let length = match borrowed {
                    Some(value) => value.len(),
                    None => self.unicode_utf8_bytes().ok_or_else(|| {
                        crate::FsError::from(NamespacePathError::NonUnicodeProjection)
                    })?,
                };
                let delta = match borrowed {
                    Some(_) => WorkCounters {
                        bytes_copied: length as u64,
                        output_bytes: length as u64,
                        ..WorkCounters::UNCHARGED
                    },
                    None => WorkCounters {
                        bytes_encoded: length as u64,
                        output_bytes: length as u64,
                        ..WorkCounters::UNCHARGED
                    },
                };
                owned.admit(delta)?;
                check()?;
                owned.observe(ReadAllocationStep::Allocate {
                    bytes: length as u64,
                    operations: u64::from(length != 0),
                })?;
                let mut text = String::new();
                if let Err(error) = text.try_reserve_exact(length) {
                    owned.observe(ReadAllocationStep::ReserveFailed(error))?;
                    return Err(crate::FsError::from(NamespacePathError::AllocationFailed));
                }
                owned.observe(ReadAllocationStep::Capacity {
                    admitted_bytes: length as u64,
                    actual_bytes: text.capacity() as u64,
                })?;
                if let Some(value) = borrowed {
                    text.push_str(value);
                } else {
                    self.append_unicode(&mut text)
                        .map_err(|error| crate::FsError::from(NamespacePathError::Name(error)))?;
                }
                owned.record_completed(delta)?;
                Ok(text)
            })();
            finish_owned(owned, result)
        })();
        work.finish_owned(result, &accounting)
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.bytes.capacity()
    }

    /// Comparison key for `CaseSensitivity::ProfileFolded` lookup and collision detection.
    #[must_use]
    pub fn case_fold_key(&self) -> Vec<u8> {
        match self.encoding {
            NameEncoding::Utf8 => {
                let Ok(value) = std::str::from_utf8(&self.bytes) else {
                    return self.bytes.clone();
                };
                self.encoding
                    .unicode_case_fold_key(value)
                    .into_owned()
                    .into_bytes()
            }
            NameEncoding::PosixBytes | NameEncoding::WindowsUtf16Le => self.bytes.clone(),
        }
    }
}

pub(crate) fn utf16le_units(bytes: &[u8]) -> Option<impl Iterator<Item = u16> + Clone + '_> {
    let (units, rest) = bytes.as_chunks::<2>();
    rest.is_empty()
        .then(|| units.iter().map(|&unit| u16::from_le_bytes(unit)))
}
fn invalid_windows_name_unit(unit: u16) -> bool {
    unit == 0 || unit == u16::from(b'/') || unit == u16::from(b'\\')
}
