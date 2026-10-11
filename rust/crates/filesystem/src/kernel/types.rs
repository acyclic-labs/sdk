//! Typed invariants shared by authenticated namespace algorithms.

use crate::foundation::{Digest, FileId};
use crate::model::FilesystemProfile;
use crate::storage::{ObjectId, ObjectKind};
use std::borrow::Cow;
use thiserror::Error;
use super::allocation::AllocationLedger;
use super::codec::{CanonicalDecodeError, Decoder, Encoder, ReadAllocationStep};
use super::namespace_path::{NamespacePathError, NamespacePathStep, path_vec};
use super::namespace_path::NamespacePathAccounting;
use crate::{CancellationToken, WorkBudget, WorkCounters};

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
        matches!((profile, self),
            (FilesystemProfile::Portable | FilesystemProfile::Browser, Self::Utf8)
            | (FilesystemProfile::Posix, Self::Utf8 | Self::PosixBytes)
            | (FilesystemProfile::Windows, Self::Utf8 | Self::WindowsUtf16Le))
    }
}

/// One separator-free canonical logical name.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalName {
    encoding: NameEncoding,
    bytes: Vec<u8>,
}

impl LogicalName {
    /// Validates a bounded name in its declared host representation.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, malformed, NUL, or separator-bearing names.
    pub fn new(
        encoding: NameEncoding,
        bytes: Vec<u8>,
        maximum_bytes: u32,
    ) -> Result<Self, TreePageError> {
        Self::validate_bytes(encoding, &bytes, maximum_bytes)?;
        Ok(Self { encoding, bytes })
    }

    /// Owns bytes emitted from the same already-validated Unicode spelling.
    /// Only the canonical namespace producer may skip the redundant validation.
    pub(super) fn from_validated_bytes(encoding: NameEncoding, bytes: Vec<u8>) -> Self {
        Self { encoding, bytes }
    }

    pub(crate) fn try_clone_with_steps<E: From<CanonicalDecodeError>>(
        &self,
        maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self, E> {
        use super::codec::ReadAllocationStep;
        let length = self.bytes.len();
        let mut bytes = Vec::new();
        steps(ReadAllocationStep::Allocate { bytes: length as u64, operations: 1 })?;
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
        Self::new(self.encoding, bytes, maximum_bytes)
            .map_err(|error| E::from(CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Name(error))))
    }

    pub(crate) fn clone_with_accounting(
        &self,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, super::allocation::KernelOwnedAllocation), crate::FsError> {
        let mut owned = NamespacePathAccounting::new(accounting)?;
        cancellation.check()?;
        let maximum_bytes = u32::try_from(self.bytes.len())
            .map_err(|_| NamespacePathError::Name(TreePageError::NameTooLarge))?;
        owned.observe(NamespacePathStep::Admit(&WorkCounters {
            bytes_copied: self.bytes.len() as u64, ..WorkCounters::UNCHARGED
        }))?;
        let result = self.try_clone_with_steps(
            maximum_bytes, &mut |step| owned.observe(NamespacePathStep::Allocation(step)),
        );
        owned.finish(result)
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

    /// Validates the Unicode subset using the same native-name predicates.
    /// Returns its exact encoded size without allocating the native bytes.
    pub(crate) fn validate_unicode(
        encoding: NameEncoding,
        value: &str,
        maximum_bytes: u32,
    ) -> Result<usize, TreePageError> {
        if encoding != NameEncoding::WindowsUtf16Le {
            Self::validate_bytes(encoding, value.as_bytes(), maximum_bytes)?;
            return Ok(value.len());
        }
        if value.is_empty() { return Err(TreePageError::EmptyName); }
        let length = value.encode_utf16().count().checked_mul(2)
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
            NameEncoding::Utf8 | NameEncoding::PosixBytes =>
                std::str::from_utf8(&self.bytes).ok().map(str::len),
            NameEncoding::WindowsUtf16Le => char::decode_utf16(utf16le_units(&self.bytes)?)
                .try_fold(0_usize, |length, value| length.checked_add(value.ok()?.len_utf8())),
        }
    }

    pub(crate) fn append_unicode(&self, output: &mut String) -> Result<(), TreePageError> {
        match self.encoding {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => output.push_str(
                std::str::from_utf8(&self.bytes).map_err(|_| TreePageError::InvalidName)?),
            NameEncoding::WindowsUtf16Le => {
                for value in char::decode_utf16(utf16le_units(&self.bytes).ok_or(TreePageError::InvalidName)?) {
                    output.push(value.map_err(|_| TreePageError::InvalidName)?);
                }
            }
        }
        Ok(())
    }

    /// Copies an already-validated payload through the canonical path ledger.
    /// The containing operation counts the borrowed name/component visit.
    pub(crate) fn clone_with_ledger(
        &self,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, NamespacePathError> {
        cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
        let delta = WorkCounters { bytes_copied: self.bytes.len() as u64,
            ..WorkCounters::default() };
        work.admit(&delta, &budget).map_err(NamespacePathError::Work)?;
        let initial_live = ledger.live_bytes();
        let result = (|| {
            let mut bytes = path_vec(self.bytes.len(), ledger, work, budget)?;
            bytes.extend_from_slice(&self.bytes);
            work.charge(&delta, &budget).map_err(NamespacePathError::Work)?;
            Ok(Self { encoding: self.encoding, bytes })
        })();
        if result.is_err() {
            ledger.release(ledger.live_bytes() - initial_live).map_err(|error| match error {
                super::allocation::AllocationError::Work(error) => NamespacePathError::Work(error),
                _ => NamespacePathError::AllocationFailed,
            })?;
        }
        result
    }

    pub(crate) fn cursor_encoded_len(&self) -> Result<usize, NamespacePathError> {
        u32::try_from(self.bytes.len()).map_err(|_| NamespacePathError::ComponentTooLong)?;
        self.bytes.len().checked_add(5)
            .ok_or(NamespacePathError::Codec(CanonicalDecodeError::LengthOverflow))
    }

    /// Appends into the containing codec's already-admitted output capacity.
    pub(crate) fn encode_cursor_into(
        &self,
        encoder: &mut Encoder,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(), NamespacePathError> {
        self.encode_cursor_into_with_steps(
            encoder, cancellation, &mut |step| {
                super::namespace_path::observe_cursor_write(step, work, budget)
            },
        )
    }

    pub(crate) fn encode_cursor_into_with_accounting(
        &self,
        encoder: &mut Encoder,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        accounting.check()?;
        self.encode_cursor_into_with_steps(
            encoder, cancellation, &mut |step| {
                super::namespace_path::observe_cursor_write_with_accounting(step, accounting)
            },
        )
    }

    pub(crate) fn encode_cursor_into_with_steps<E: From<NamespacePathError>>(
        &self,
        encoder: &mut Encoder,
        cancellation: &CancellationToken,
        steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
        let length = self.cursor_encoded_len().map_err(E::from)?;
        if encoder.remaining_capacity() < length {
            return Err(E::from(NamespacePathError::AllocationFailed));
        }
        let delta = WorkCounters { bytes_encoded: length as u64,
            bytes_copied: self.bytes.len() as u64, items_examined: 1,
            ..WorkCounters::UNCHARGED };
        steps(NamespacePathStep::Admit(&delta))?;
        encoder.u8(self.encoding.tag());
        encoder.bounded_bytes(&self.bytes)
            .map_err(|error| E::from(NamespacePathError::Codec(error)))?;
        steps(NamespacePathStep::Performed(&delta))
    }


    pub(crate) fn read_cursor_borrowed_with_steps<'a, E: From<NamespacePathError>>(
        decoder: &mut Decoder<'a>,
        profile: FilesystemProfile,
        maximum_bytes: u32,
        cancellation: &CancellationToken,
        steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
    ) -> Result<(NameEncoding, &'a [u8]), E> {
        cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
        steps(NamespacePathStep::Charge(&WorkCounters {
            items_examined: 1, ..WorkCounters::UNCHARGED
        }))?;
        let encoding = NameEncoding::from_tag(
            decoder.u8().map_err(|error| E::from(NamespacePathError::Codec(error)))?,
        ).map_err(|error| E::from(NamespacePathError::Name(error)))?;
        if !encoding.is_admitted_in_profile(profile) {
            return Err(E::from(NamespacePathError::Name(TreePageError::InvalidName)));
        }
        let length = decoder.u32().map_err(|error| E::from(NamespacePathError::Codec(error)))?;
        if length > maximum_bytes {
            return Err(E::from(NamespacePathError::Name(TreePageError::NameTooLarge)));
        }
        let bytes = decoder.take_exact(length as usize)
            .map_err(|error| E::from(NamespacePathError::Codec(error)))?;
        Self::validate_bytes(encoding, bytes, maximum_bytes)
            .map_err(|error| E::from(NamespacePathError::Name(error)))?;
        Ok((encoding, bytes))
    }

    pub(crate) fn decode_cursor_with_ledger(
        decoder: &mut Decoder<'_>,
        profile: FilesystemProfile,
        maximum_bytes: u32,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, NamespacePathError> {
        let initial_live = ledger.live_bytes();
        let result = Self::decode_cursor_with_steps(
            decoder, profile, maximum_bytes, cancellation, &mut |step| {
                super::namespace_path::observe_path_ledger(step, ledger, work, budget)
            },
        );
        if result.is_err() {
            ledger.release(ledger.live_bytes() - initial_live)
                .map_err(|error| match error {
                    super::allocation::AllocationError::Work(error) => NamespacePathError::Work(error),
                    _ => NamespacePathError::AllocationFailed,
                })?;
        }
        result
    }

    pub(crate) fn decode_cursor_with_accounting(
        decoder: &mut Decoder<'_>,
        profile: FilesystemProfile,
        maximum_bytes: u32,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, super::allocation::KernelOwnedAllocation), crate::FsError> {
        let mut owned = NamespacePathAccounting::new(accounting)?;
        let result = Self::decode_cursor_with_steps(
            decoder, profile, maximum_bytes, cancellation, &mut |step| owned.observe(step),
        );
        owned.finish(result)
    }

    pub(crate) fn decode_cursor_with_steps<E: From<NamespacePathError>>(
        decoder: &mut Decoder<'_>,
        profile: FilesystemProfile,
        maximum_bytes: u32,
        cancellation: &CancellationToken,
        steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
    ) -> Result<Self, E> {
        let (encoding, source) = Self::read_cursor_borrowed_with_steps(
            decoder, profile, maximum_bytes, cancellation, steps,
        )?;
        let delta = WorkCounters { bytes_copied: source.len() as u64,
            ..WorkCounters::UNCHARGED };
        steps(NamespacePathStep::Admit(&delta))?;
        let mut bytes = super::namespace_path::namespace_path_vec_with_steps(
            source.len(), cancellation, steps,
        )?;
        bytes.extend_from_slice(source);
        steps(NamespacePathStep::Performed(&delta))?;
        Ok(Self::from_validated_bytes(encoding, bytes))
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

    /// Presents this exact name as Unicode when its host encoding permits it.
    /// Raw POSIX names outside UTF-8 remain unrepresentable as strings.
    #[must_use]
    pub fn unicode_text(&self) -> Option<Cow<'_, str>> {
        match self.encoding {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                std::str::from_utf8(&self.bytes).ok().map(Cow::Borrowed)
            }
            NameEncoding::WindowsUtf16Le => self.unicode_text_with_steps::<NamespacePathError>(
                None, &mut |_| Ok(()),
            ).ok().map(Cow::Owned),
        }
    }

    /// Owns the exact non-lossy Unicode name on this original native work scope.
    /// Drop the returned string before its allocation guard.
    ///
    /// # Errors
    /// Returns the original work/cancellation failure, reserve failure, or an
    /// inline non-Unicode projection cause before allocating opaque names.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn unicode_text_with_native_work(
        &self,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(String, crate::NativeAllocation), crate::FsError> {
        let mut accounting = super::allocation::KernelAccounting::native(original, cancellation);
        let mut owned = NamespacePathAccounting::new(&mut accounting)?;
        let result = self.unicode_text_with_steps(
            Some(cancellation), &mut |step| owned.observe(step),
        );
        let (text, allocation) = owned.finish(result)?;
        match allocation {
            super::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => Ok((text, allocation)),
            super::allocation::KernelOwnedAllocation::Standalone => {
                drop(text);
                Err(crate::NativeTreeMaterializationError::InvalidLayout.into())
            }
        }
    }

    fn unicode_text_with_steps<E: From<NamespacePathError>>(
        &self,
        cancellation: Option<&CancellationToken>,
        steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
    ) -> Result<String, E> {
        let check = || cancellation.map_or(Ok(()), |token| {
            token.check().map_err(|_| E::from(NamespacePathError::Cancelled))
        });
        check()?;
        steps(NamespacePathStep::Charge(&WorkCounters {
            items_examined: 1, ..WorkCounters::UNCHARGED
        }))?;
        let borrowed = match self.encoding {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => Some(
                std::str::from_utf8(&self.bytes)
                    .map_err(|_| E::from(NamespacePathError::NonUnicodeProjection))?,
            ),
            NameEncoding::WindowsUtf16Le => None,
        };
        let length = match borrowed {
            Some(value) => value.len(),
            None => self.unicode_utf8_bytes()
                .ok_or_else(|| E::from(NamespacePathError::NonUnicodeProjection))?,
        };
        let delta = match borrowed {
            Some(_) => WorkCounters { bytes_copied: length as u64,
                output_bytes: length as u64, ..WorkCounters::UNCHARGED },
            None => WorkCounters { bytes_encoded: length as u64,
                output_bytes: length as u64, ..WorkCounters::UNCHARGED },
        };
        steps(NamespacePathStep::Admit(&delta))?;
        check()?;
        steps(NamespacePathStep::Allocation(ReadAllocationStep::Allocate {
            bytes: length as u64, operations: u64::from(length != 0),
        }))?;
        let mut text = String::new();
        if let Err(error) = text.try_reserve_exact(length) {
            steps(NamespacePathStep::Allocation(ReadAllocationStep::ReserveFailed(error)))?;
            return Err(E::from(NamespacePathError::AllocationFailed));
        }
        steps(NamespacePathStep::Allocation(ReadAllocationStep::Capacity {
            admitted_bytes: length as u64, actual_bytes: text.capacity() as u64,
        }))?;
        if let Some(value) = borrowed {
            text.push_str(value);
        } else {
            self.append_unicode(&mut text)
                .map_err(|error| E::from(NamespacePathError::Name(error)))?;
        }
        steps(NamespacePathStep::Performed(&delta))?;
        Ok(text)
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.bytes.capacity()
    }

    /// Comparison key for `CaseSensitivity::ProfileFolded` lookup and
    /// collision detection.
    ///
    /// Only [`NameEncoding::Utf8`] names are folded (via full Unicode
    /// case-folding, `str::to_lowercase`); other encodings return their raw
    /// bytes unchanged, so `ProfileFolded` behaves like `Sensitive` for
    /// `PosixBytes`/`WindowsUtf16Le` names. This never changes the physical,
    /// byte-ordered on-disk representation — it is a comparison-only key used
    /// by callers that already hold the name, not a new canonical form.
    #[must_use]
    pub fn case_fold_key(&self) -> Vec<u8> {
        match self.encoding {
            NameEncoding::Utf8 => {
                // `LogicalName::new` already rejected non-UTF-8 bytes for
                // this encoding, so this is exact, not lossy.
                let Ok(value) = std::str::from_utf8(&self.bytes) else {
                    return self.bytes.clone();
                };
                lowercase_unicode_with_ledger_inner(
                    value,
                    &mut AllocationLedger::default(),
                    &mut WorkCounters::UNCHARGED,
                    WorkBudget::UNBOUNDED,
                    None,
                ).expect("unbounded canonical lowercase allocation succeeds")
            }
            NameEncoding::PosixBytes | NameEncoding::WindowsUtf16Le => self.bytes.clone(),
        }
    }
}

// ICU's pinned compiled Cased/Case_Ignorable data and Rust's character
// mappings both use Unicode 17. Updating either requires updating both.
const _: () = assert!(matches!(char::UNICODE_VERSION, (17, 0, 0)));

/// The canonical lowercase engine used by ordinary name keys and admitted
/// native keys. Both accounting adapters observe the same scalar, contextual
/// ICU-property, allocation, and emission steps; no native scan is unmetered.
/// The returned capacity remains owned by the supplied ledger.
pub(crate) fn lowercase_unicode_with_ledger(
    text: &str,
    ledger: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, NamespacePathError> {
    lowercase_unicode_with_ledger_inner(text, ledger, work, budget, Some(cancellation))
}

fn lowercase_unicode_with_ledger_inner(
    text: &str,
    ledger: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<u8>, NamespacePathError> {
    let initial_live = ledger.live_bytes();
    let result = lowercase_unicode_with_steps(text, cancellation, &mut |step| {
        super::namespace_path::observe_path_ledger(step, ledger, work, budget)
    });
    if result.is_err() {
        ledger.release(ledger.live_bytes() - initial_live)
            .expect("canonical lowercase ownership balances");
    }
    result
}

pub(crate) fn lowercase_unicode_with_accounting(
    text: &str,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<u8>, super::allocation::KernelOwnedAllocation), crate::FsError> {
    let mut owned = NamespacePathAccounting::new(accounting)?;
    let result = lowercase_unicode_with_steps(text, Some(cancellation), &mut |step| owned.observe(step));
    owned.finish(result)
}

fn lowercase_unicode_with_steps<E: From<NamespacePathError>>(
    text: &str,
    cancellation: Option<&CancellationToken>,
    steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
) -> Result<Vec<u8>, E> {

    lowercase_check(cancellation)?;
    let mut length = 0_usize;
    let mut scalars = text.chars();
    while !scalars.as_str().is_empty() {
        lowercase_check(cancellation)?;
        steps(NamespacePathStep::Charge(&WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
        let scalar = scalars.next().expect("the borrowed scalar is present");
        // Contextual sigma changes its spelling, but not its encoded size.
        for mapped in scalar.to_lowercase() {
            lowercase_check(cancellation)?;
            steps(NamespacePathStep::Charge(&WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
            length = length.checked_add(mapped.len_utf8())
                .ok_or_else(|| E::from(NamespacePathError::Work(crate::WorkError::Overflow)))?;
        }
    }
    let requested = length as u64;
    steps(NamespacePathStep::Allocation(ReadAllocationStep::Allocate {
        bytes: requested, operations: u64::from(length != 0),
    }))?;
    let mut output = Vec::new();
    if let Err(error) = output.try_reserve_exact(length) {
        steps(NamespacePathStep::Allocation(ReadAllocationStep::ReserveFailed(error)))?;
        return Err(E::from(NamespacePathError::AllocationFailed));
    }
    steps(NamespacePathStep::Allocation(ReadAllocationStep::Capacity {
        admitted_bytes: requested, actual_bytes: output.capacity() as u64,
    }))?;
    let mut scalars = text.chars();
    while !scalars.as_str().is_empty() {
        lowercase_check(cancellation)?;
        steps(NamespacePathStep::Charge(&WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
        let offset = text.len() - scalars.as_str().len();
        let scalar = scalars.next().expect("the borrowed scalar is present");
        let scalar = if scalar == 'Σ' {
            lowercase_sigma(text, offset, cancellation, steps)?
        } else {
            scalar
        };
        for mapped in scalar.to_lowercase() {
            lowercase_check(cancellation)?;
            steps(NamespacePathStep::Charge(&WorkCounters {
                items_examined: 1,
                bytes_encoded: mapped.len_utf8() as u64,
                ..WorkCounters::UNCHARGED
            }))?;
            let mut encoded = [0_u8; 4];
            output.extend_from_slice(mapped.encode_utf8(&mut encoded).as_bytes());
        }
    }
    lowercase_check(cancellation)?;
    Ok(output)
}

fn lowercase_check(cancellation: Option<&CancellationToken>) -> Result<(), NamespacePathError> {
    if let Some(cancellation) = cancellation {
        cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
    }
    Ok(())
}

fn lowercase_sigma<E: From<NamespacePathError>>(
    text: &str,
    offset: usize,
    cancellation: Option<&CancellationToken>,
    steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
) -> Result<char, E> {
    fn non_ignorable_is_cased<E: From<NamespacePathError>>(
        mut scalars: std::str::Chars<'_>,
        reverse: bool,
        cancellation: Option<&CancellationToken>,
        steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
    ) -> Result<bool, E> {
        let ignorable = icu_properties::CodePointSetData::new::<icu_properties::props::CaseIgnorable>();
        let cased = icu_properties::CodePointSetData::new::<icu_properties::props::Cased>();
        while !scalars.as_str().is_empty() {
            lowercase_check(cancellation)?;
            steps(NamespacePathStep::Charge(&WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
            let scalar = if reverse { scalars.next_back() } else { scalars.next() }
                .expect("the contextual scalar is present");
            if !ignorable.contains(scalar) {
                return Ok(cased.contains(scalar));
            }
        }
        Ok(false)
    }

    let preceded_by_cased = non_ignorable_is_cased(
        text[..offset].chars(), true, cancellation, steps,
    )?;
    let final_sigma = preceded_by_cased && !non_ignorable_is_cased(
        text[offset + 'Σ'.len_utf8()..].chars(), false, cancellation, steps,
    )?;
    Ok(if final_sigma { 'ς' } else { 'σ' })
}

fn invalid_windows_name_unit(unit: u16) -> bool {
    unit == 0 || unit == u16::from(b'/') || unit == u16::from(b'\\')
}

/// Complete namespace entry kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "typescript", target_arch = "wasm32"),
    derive(serde::Serialize, tsify::Tsify)
)]
#[cfg_attr(
    all(feature = "typescript", target_arch = "wasm32"),
    serde(rename_all = "kebab-case")
)]
#[cfg_attr(feature = "napi-types", napi_derive::napi(string_enum = "kebab-case"))]
pub enum FileKind {
    /// Ordinary byte file.
    Regular,
    /// Directory namespace.
    Directory,
    /// Symbolic link.
    SymbolicLink,
    /// FIFO.
    Fifo,
    /// Socket namespace entry.
    Socket,
    /// Character device.
    CharacterDevice,
    /// Block device.
    BlockDevice,
    /// Windows reparse point not represented as a symbolic link.
    ReparsePoint,
    /// Opaque boundary delegated to another mounted volume.
    MountBoundary,
}

impl FileKind {
    /// Whether this kind has an exact representation in one volume profile.
    #[must_use]
    pub const fn is_supported_by_profile(self, profile: FilesystemProfile) -> bool {
        match self {
            Self::Regular | Self::Directory | Self::SymbolicLink | Self::MountBoundary => true,
            Self::Fifo | Self::Socket | Self::CharacterDevice | Self::BlockDevice => {
                matches!(profile, FilesystemProfile::Posix)
            }
            Self::ReparsePoint => matches!(profile, FilesystemProfile::Windows),
        }
    }

    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::Regular => 1,
            Self::Directory => 2,
            Self::SymbolicLink => 3,
            Self::Fifo => 4,
            Self::Socket => 5,
            Self::CharacterDevice => 6,
            Self::BlockDevice => 7,
            Self::ReparsePoint => 8,
            Self::MountBoundary => 9,
        }
    }

    pub(crate) const fn from_tag(tag: u8) -> Result<Self, TreePageError> {
        match tag {
            1 => Ok(Self::Regular),
            2 => Ok(Self::Directory),
            3 => Ok(Self::SymbolicLink),
            4 => Ok(Self::Fifo),
            5 => Ok(Self::Socket),
            6 => Ok(Self::CharacterDevice),
            7 => Ok(Self::BlockDevice),
            8 => Ok(Self::ReparsePoint),
            9 => Ok(Self::MountBoundary),
            value => Err(TreePageError::UnknownFileKind(value)),
        }
    }
}

/// One name-to-file binding in a directory leaf page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeEntry {
    /// Canonical logical name.
    pub name: LogicalName,
    /// Path-independent file record identity; hard links reuse it.
    pub file_id: FileId,
    /// Namespace kind used for bounded routing and restoration.
    pub kind: FileKind,
}

/// One lower-bound-to-page binding in an internal page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeChild {
    /// Exact first name represented by the child subtree.
    pub first_name: LogicalName,
    /// Authenticated tree-page identity.
    pub page: ObjectId,
}

/// One immutable authenticated directory B+tree page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreePage {
    /// Ordered file bindings.
    Leaf(Vec<TreeEntry>),
    /// Ordered child lower bounds.
    Internal(Vec<TreeChild>),
}

impl TreePage {
    /// Validates ordering, uniqueness, object kinds, and page bounds.
    ///
    /// # Errors
    ///
    /// Rejects malformed pages before hashing or publication.
    pub fn validate(&self, maximum_items: u32) -> Result<(), TreePageError> {
        let count = match self {
            Self::Leaf(entries) => {
                validate_order(entries.iter().map(|entry| &entry.name))?;
                entries.len()
            }
            Self::Internal(children) => {
                if children.is_empty() {
                    return Err(TreePageError::EmptyInternalPage);
                }
                validate_order(children.iter().map(|child| &child.first_name))?;
                if children
                    .iter()
                    .any(|child| child.page.kind != ObjectKind::TreePage)
                {
                    return Err(TreePageError::WrongChildKind);
                }
                children.len()
            }
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(TreePageError::TooManyItems);
        }
        Ok(())
    }
}

fn validate_order<'a>(
    mut names: impl Iterator<Item = &'a LogicalName>,
) -> Result<(), TreePageError> {
    let Some(mut previous) = names.next() else {
        return Ok(());
    };
    for name in names {
        if previous >= name {
            return Err(TreePageError::NamesNotStrictlyOrdered);
        }
        previous = name;
    }
    Ok(())
}

/// Authenticated tree-page invariant failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TreePageError {
    /// Logical names cannot be empty.
    #[error("logical name is empty")]
    EmptyName,
    /// Name exceeds the configured encoded-byte bound.
    #[error("logical name exceeds its byte bound")]
    NameTooLarge,
    /// Name bytes are malformed or contain a forbidden separator/component.
    #[error("logical name is invalid for its declared encoding")]
    InvalidName,
    /// Decoder encountered an unknown name representation.
    #[error("unknown logical name encoding {0}")]
    UnknownNameEncoding(u8),
    /// Decoder encountered an unknown file kind.
    #[error("unknown file kind {0}")]
    UnknownFileKind(u8),
    /// Names must be canonical, strictly ordered, and unique.
    #[error("tree page names are not strictly ordered")]
    NamesNotStrictlyOrdered,
    /// Internal pages must contain at least one routed child.
    #[error("internal tree page is empty")]
    EmptyInternalPage,
    /// Internal child points at another object class.
    #[error("internal tree child is not a tree page")]
    WrongChildKind,
    /// Page exceeds its admitted item bound.
    #[error("tree page exceeds its item bound")]
    TooManyItems,
    /// Canonical bytes contain an invalid digest or field.
    #[error("invalid canonical tree page field: {0}")]
    InvalidCanonicalField(&'static str),
}

pub(crate) fn digest_object(kind: ObjectKind, digest: [u8; 32]) -> ObjectId {
    ObjectId {
        kind,
        digest: Digest::from_bytes(digest),
    }
}

/// Physical representation of one logical byte interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtentKind {
    /// Unallocated bytes that read as zero.
    Hole,
    /// Physically allocated bytes whose canonical value is zero.
    AllocatedZero,
    /// Bytes stored in one immutable blob span.
    Content {
        /// Typed immutable blob.
        object: ObjectId,
        /// First byte used from the blob.
        object_offset: u64,
    },
}

/// One non-empty logical file interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Extent {
    /// Inclusive logical byte offset.
    pub offset: u64,
    /// Positive logical byte length.
    pub length: u64,
    /// Physical representation.
    pub kind: ExtentKind,
}

/// One lower-bound-to-page entry in an extent B+tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentChild {
    /// First logical byte represented by the child.
    pub first_offset: u64,
    /// Exclusive logical end represented by the child.
    pub end_offset: u64,
    /// Authenticated extent page.
    pub page: ObjectId,
}

/// One immutable authenticated extent B+tree page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtentPage {
    /// Ordered non-overlapping extents.
    Leaf(Vec<Extent>),
    /// Ordered child lower bounds.
    Internal(Vec<ExtentChild>),
}

impl ExtentPage {
    /// Validates bounds, ordering, non-overlap, and typed child/content identities.
    ///
    /// # Errors
    ///
    /// Rejects malformed extent pages before hashing or publication.
    pub fn validate(&self, maximum_items: u32) -> Result<(), ExtentPageError> {
        let count = match self {
            Self::Leaf(extents) => {
                let mut previous_end = None;
                for extent in extents {
                    if extent.length == 0 {
                        return Err(ExtentPageError::ZeroLength);
                    }
                    let end = extent
                        .offset
                        .checked_add(extent.length)
                        .ok_or(ExtentPageError::RangeOverflow)?;
                    if previous_end.is_some_and(|prior| extent.offset != prior) {
                        return Err(ExtentPageError::NonContiguous);
                    }
                    if let ExtentKind::Content {
                        object,
                        object_offset,
                    } = extent.kind
                    {
                        if object.kind != ObjectKind::Blob {
                            return Err(ExtentPageError::WrongContentKind);
                        }
                        object_offset
                            .checked_add(extent.length)
                            .ok_or(ExtentPageError::RangeOverflow)?;
                    }
                    previous_end = Some(end);
                }
                extents.len()
            }
            Self::Internal(children) => {
                if children.is_empty() {
                    return Err(ExtentPageError::EmptyInternalPage);
                }
                if children.windows(2).any(|pair| {
                    let [left, right] = pair else {
                        unreachable!("windows(2) always yields exactly two elements")
                    };
                    left.end_offset != right.first_offset
                }) {
                    return Err(ExtentPageError::NonContiguous);
                }
                if children
                    .iter()
                    .any(|child| child.first_offset >= child.end_offset)
                {
                    return Err(ExtentPageError::InvalidChildRange);
                }
                if children
                    .iter()
                    .any(|child| child.page.kind != ObjectKind::ExtentPage)
                {
                    return Err(ExtentPageError::WrongChildKind);
                }
                children.len()
            }
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(ExtentPageError::TooManyItems);
        }
        Ok(())
    }
}

/// Authenticated extent-page invariant failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExtentPageError {
    /// Extents must represent at least one logical byte.
    #[error("extent length is zero")]
    ZeroLength,
    /// Logical or content-offset arithmetic overflowed.
    #[error("extent range overflowed")]
    RangeOverflow,
    /// Extents/children overlap, have a gap, or are out of order.
    #[error("extent page ranges are not contiguous")]
    NonContiguous,
    /// Content extent references another object class.
    #[error("extent content is not a blob object")]
    WrongContentKind,
    /// Internal page has no routed children.
    #[error("internal extent page is empty")]
    EmptyInternalPage,
    /// Internal child has an empty or reversed range.
    #[error("extent child range is empty or reversed")]
    InvalidChildRange,
    /// Internal child references another object class.
    #[error("extent child is not an extent page")]
    WrongChildKind,
    /// Page exceeds the admitted item bound.
    #[error("extent page exceeds its item bound")]
    TooManyItems,
    /// Decoder encountered an unknown physical representation.
    #[error("unknown extent kind {0}")]
    UnknownKind(u8),
}

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;

/// The UTF-16 code units little-endian `bytes` hold, or `None` for an odd
/// length, which holds none.
pub(crate) fn utf16le_units(bytes: &[u8]) -> Option<impl Iterator<Item = u16> + Clone + '_> {
    let (units, rest) = bytes.as_chunks::<2>();
    rest.is_empty()
        .then(|| units.iter().map(|&unit| u16::from_le_bytes(unit)))
}
