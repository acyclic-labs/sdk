use super::*;

const _: () = assert!(matches!(char::UNICODE_VERSION, (17, 0, 0)));

#[allow(
    clippy::expect_used,
    reason = "A nonempty Chars remainder guarantees the next scalar without consuming it before admission."
)]
pub(crate) fn lowercase_unicode(
    text: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError> {
    let mut owned = OriginalReadAccounting::new(accounting)?;
    let result = (|| {
        lowercase_check(cancellation)?;
        let mut length = 0_usize;
        let mut scalars = text.chars();
        while !scalars.as_str().is_empty() {
            lowercase_check(cancellation)?;
            owned.observe(ReadAllocationStep::BeforeItem)?;
            owned.observe(ReadAllocationStep::Item)?;
            let scalar = scalars.next().expect("the borrowed scalar is present");
            // Contextual sigma preserves the encoded size.
            for mapped in scalar.to_lowercase() {
                lowercase_check(cancellation)?;
                owned.observe(ReadAllocationStep::BeforeItem)?;
                owned.observe(ReadAllocationStep::Item)?;
                length = length.checked_add(mapped.len_utf8()).ok_or_else(|| {
                    crate::FsError::from(NamespacePathError::Work(crate::WorkError::Overflow))
                })?;
            }
        }
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
        let mut scalars = text.chars();
        while !scalars.as_str().is_empty() {
            lowercase_check(cancellation)?;
            owned.observe(ReadAllocationStep::BeforeItem)?;
            owned.observe(ReadAllocationStep::Item)?;
            let offset = text.len() - scalars.as_str().len();
            let scalar = scalars.next().expect("the borrowed scalar is present");
            let scalar = if scalar == 'Σ' {
                lowercase_sigma(text, offset, cancellation, &mut owned)?
            } else {
                scalar
            };
            for mapped in scalar.to_lowercase() {
                lowercase_check(cancellation)?;
                let delta = WorkCounters {
                    items_examined: 1,
                    bytes_encoded: mapped.len_utf8() as u64,
                    ..WorkCounters::UNCHARGED
                };
                owned.admit(delta)?;
                owned.record_completed(delta)?;
                let mut encoded = [0_u8; 4];
                output.extend_from_slice(mapped.encode_utf8(&mut encoded).as_bytes());
            }
        }
        lowercase_check(cancellation)?;
        Ok(output)
    })();
    finish_owned(owned, result)
}

fn lowercase_check(cancellation: &CancellationToken) -> Result<(), NamespacePathError> {
    cancellation
        .check()
        .map_err(|_| NamespacePathError::Cancelled)
}

fn lowercase_sigma(
    text: &str,
    offset: usize,
    cancellation: &CancellationToken,
    owned: &mut OriginalReadAccounting<'_, '_>,
) -> Result<char, crate::FsError> {
    #[allow(
        clippy::expect_used,
        reason = "A nonempty Chars remainder guarantees the next scalar without consuming it before admission."
    )]
    fn non_ignorable_is_cased(
        mut scalars: std::str::Chars<'_>,
        reverse: bool,
        cancellation: &CancellationToken,
        owned: &mut OriginalReadAccounting<'_, '_>,
    ) -> Result<bool, crate::FsError> {
        let ignorable =
            icu_properties::CodePointSetData::new::<icu_properties::props::CaseIgnorable>();
        let cased = icu_properties::CodePointSetData::new::<icu_properties::props::Cased>();
        while !scalars.as_str().is_empty() {
            lowercase_check(cancellation)?;
            owned.observe(ReadAllocationStep::BeforeItem)?;
            owned.observe(ReadAllocationStep::Item)?;
            let scalar = if reverse {
                scalars.next_back()
            } else {
                scalars.next()
            }
            .expect("the contextual scalar is present");
            if !ignorable.contains(scalar) {
                return Ok(cased.contains(scalar));
            }
        }
        Ok(false)
    }
    let (before, rest) = text.split_at(offset);
    let (_, after) = rest.split_at('Σ'.len_utf8());
    let preceded_by_cased = non_ignorable_is_cased(before.chars(), true, cancellation, owned)?;
    let final_sigma =
        preceded_by_cased && !non_ignorable_is_cased(after.chars(), false, cancellation, owned)?;
    Ok(if final_sigma { 'ς' } else { 'σ' })
}
