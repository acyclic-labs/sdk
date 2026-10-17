use super::*;
use std::borrow::Cow;
use writeable::Writeable;

// Full folding is locale-independent and context-insensitive. Separators and
// glob syntax remain verbatim; each component uses the native UTF-8 name key.
struct FoldedComponents<'a>(&'a str);

impl Writeable for FoldedComponents<'_> {
    fn write_to<W: std::fmt::Write + ?Sized>(&self, sink: &mut W) -> std::fmt::Result {
        let mapper = icu_casemap::CaseMapper::new();
        for component in self.0.split_inclusive('/') {
            mapper.fold(component).write_to(sink)?;
        }
        Ok(())
    }

    fn writeable_length_hint(&self) -> writeable::LengthHint {
        icu_casemap::CaseMapper::new()
            .fold(self.0)
            .writeable_length_hint()
    }
}

pub(crate) fn write_key(text: &str, sink: &mut impl std::fmt::Write) -> std::fmt::Result {
    FoldedComponents(text).write_to(sink)
}

pub(super) fn key(text: &str) -> Cow<'_, str> {
    writeable::to_string_or_borrow(&FoldedComponents(text), text.as_bytes())
}

pub(crate) fn case_fold_unicode(
    text: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError> {
    let mut owned = OriginalReadAccounting::new(accounting)?;
    let result = (|| {
        let mut length = 0_usize;
        folded_chunks(text, &mut owned, cancellation, |chunk, owned| {
            length = length
                .checked_add(chunk.len())
                .ok_or(crate::WorkError::Overflow)?;
            for _ in chunk.chars() {
                cancellation.check()?;
                owned.observe(ReadAllocationStep::BeforeItem)?;
                owned.observe(ReadAllocationStep::Item)?;
            }
            Ok(())
        })?;
        let requested = length as u64;
        owned.observe(ReadAllocationStep::Allocate {
            bytes: requested,
            operations: u64::from(length != 0),
        })?;
        let mut output = Vec::new();
        if let Err(error) = output.try_reserve_exact(length) {
            owned.observe(ReadAllocationStep::ReserveFailed(error))?;
            return Err(crate::FsError::from(NamespacePathError::AllocationFailed));
        }
        owned.observe(ReadAllocationStep::Capacity {
            admitted_bytes: requested,
            actual_bytes: output.capacity() as u64,
        })?;
        folded_chunks(text, &mut owned, cancellation, |chunk, owned| {
            let delta = WorkCounters {
                items_examined: chunk.chars().count() as u64,
                bytes_encoded: chunk.len() as u64,
                ..WorkCounters::UNCHARGED
            };
            owned.admit(delta)?;
            if chunk.len() > length.saturating_sub(output.len()) {
                return Err(NamespacePathError::NonUnicodeProjection.into());
            }
            output.extend_from_slice(chunk.as_bytes());
            owned.record_completed(delta)?;
            Ok(())
        })?;
        cancellation.check()?;
        Ok(output)
    })();
    finish_owned(owned, result)
}

fn folded_chunks(
    text: &str,
    owned: &mut OriginalReadAccounting<'_, '_>,
    cancellation: &CancellationToken,
    mut emit: impl FnMut(&str, &mut OriginalReadAccounting<'_, '_>) -> Result<(), crate::FsError>,
) -> Result<(), crate::FsError> {
    let mapper = icu_casemap::CaseMapper::new();
    let mut scalars = text.chars();
    while !scalars.as_str().is_empty() {
        cancellation.check()?;
        owned.observe(ReadAllocationStep::BeforeItem)?;
        owned.observe(ReadAllocationStep::Item)?;
        let Some(scalar) = scalars.next() else { break };
        let mut encoded = [0_u8; 4];
        let mut writer = FoldWriter {
            emit: |chunk: &str| emit(chunk, owned),
            failure: None,
        };
        let status = mapper
            .fold(scalar.encode_utf8(&mut encoded))
            .write_to(&mut writer);
        if let Some(error) = writer.failure {
            return Err(error);
        }
        status.map_err(|_| crate::FsError::from(NamespacePathError::NonUnicodeProjection))?;
    }
    cancellation.check()?;
    Ok(())
}

struct FoldWriter<F> {
    emit: F,
    failure: Option<crate::FsError>,
}

impl<F: FnMut(&str) -> Result<(), crate::FsError>> std::fmt::Write for FoldWriter<F> {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        (self.emit)(text).map_err(|error| {
            self.failure = Some(error);
            std::fmt::Error
        })
    }
}
