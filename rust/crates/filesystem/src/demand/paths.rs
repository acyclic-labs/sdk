use super::*;

pub(crate) fn lowercase_unicode_with_work(
    text: &str,
    work: &mut DemandWork,
) -> Result<Vec<u8>, DemandError> {
    work.reconcile_cancellation_registry()?;
    work.check()?;
    let (allocations, counters, budget, cancellation) = work.allocation_parts();
    let bytes = crate::kernel::types::lowercase_unicode(
        text,
        allocations,
        counters,
        budget,
        Some(cancellation),
    )?;
    if let Err(error) = work.check() {
        let owned = bytes.capacity() as u64;
        drop(bytes);
        work.release(owned)?;
        return Err(error);
    }
    Ok(bytes)
}
