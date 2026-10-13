use super::*;

const _: () = assert!(matches!(char::UNICODE_VERSION, (17, 0, 0)));

pub(crate) fn lowercase_unicode(
    text: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError> {
    let mut owned = NamespacePathAccounting::new(accounting)?;
    let result = lowercase_unicode_with_steps(text, cancellation, &mut |step| owned.observe(step));
    owned.finish(result)
}

fn lowercase_unicode_with_steps(
    text: &str,
    cancellation: &CancellationToken,
    steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), crate::FsError>,
) -> Result<Vec<u8>, crate::FsError> {
    lowercase_check(cancellation)?;
    let mut length = 0_usize;
    let mut scalars = text.chars();
    while !scalars.as_str().is_empty() {
        lowercase_check(cancellation)?;
        steps(NamespacePathStep::Charge(&WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        }))?;
        let scalar = scalars.next().expect("the borrowed scalar is present");
        // Contextual sigma changes its spelling, but not its encoded size.
        for mapped in scalar.to_lowercase() {
            lowercase_check(cancellation)?;
            steps(NamespacePathStep::Charge(&WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            }))?;
            length = length.checked_add(mapped.len_utf8()).ok_or_else(|| {
                crate::FsError::from(NamespacePathError::Work(crate::WorkError::Overflow))
            })?;
        }
    }
    let requested = length as u64;
    steps(NamespacePathStep::Allocation(
        ReadAllocationStep::Allocate {
            bytes: requested,
            operations: u64::from(length != 0),
        },
    ))?;
    let mut output = Vec::new();
    if let Err(error) = output.try_reserve_exact(length) {
        steps(NamespacePathStep::Allocation(
            ReadAllocationStep::ReserveFailed(error),
        ))?;
        return Err(crate::FsError::from(NamespacePathError::AllocationFailed));
    }
    steps(NamespacePathStep::Allocation(
        ReadAllocationStep::Capacity {
            admitted_bytes: requested,
            actual_bytes: output.capacity() as u64,
        },
    ))?;
    let mut scalars = text.chars();
    while !scalars.as_str().is_empty() {
        lowercase_check(cancellation)?;
        steps(NamespacePathStep::Charge(&WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        }))?;
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

fn lowercase_check(cancellation: &CancellationToken) -> Result<(), NamespacePathError> {
    cancellation
        .check()
        .map_err(|_| NamespacePathError::Cancelled)
}

fn lowercase_sigma(
    text: &str,
    offset: usize,
    cancellation: &CancellationToken,
    steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), crate::FsError>,
) -> Result<char, crate::FsError> {
    fn non_ignorable_is_cased(
        mut scalars: std::str::Chars<'_>,
        reverse: bool,
        cancellation: &CancellationToken,
        steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), crate::FsError>,
    ) -> Result<bool, crate::FsError> {
        let ignorable =
            icu_properties::CodePointSetData::new::<icu_properties::props::CaseIgnorable>();
        let cased = icu_properties::CodePointSetData::new::<icu_properties::props::Cased>();
        while !scalars.as_str().is_empty() {
            lowercase_check(cancellation)?;
            steps(NamespacePathStep::Charge(&WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            }))?;
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

    let preceded_by_cased =
        non_ignorable_is_cased(text[..offset].chars(), true, cancellation, steps)?;
    let final_sigma = preceded_by_cased
        && !non_ignorable_is_cased(
            text[offset + 'Σ'.len_utf8()..].chars(),
            false,
            cancellation,
            steps,
        )?;
    Ok(if final_sigma { 'ς' } else { 'σ' })
}
