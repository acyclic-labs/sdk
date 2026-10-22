use super::*;

// Compare the authenticated bytes already staged from the observed host node.
// Unallocated ranges are zeros; neither timestamps nor equal lengths prove content.
pub(super) async fn matches<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &Checkout<A, O>,
    expected: &[u8],
    logical_bytes: u64,
    ranges: &[(HostDataRange, StagedContent)],
    meter: &NativeMaterializationWork,
) -> Result<bool, CaptureError> {
    if logical_bytes != expected.len() as u64 {
        return Ok(false);
    }
    let cancellation = meter.cancellation();
    let mut accounting = crate::kernel::KernelAccounting::native(meter, cancellation);
    let mut cursor = 0;
    for (range, content) in ranges {
        accounting.charge_items(1)?;
        let start = usize::try_from(range.offset).map_err(|_| WorkError::Overflow)?;
        let length = usize::try_from(range.length).map_err(|_| WorkError::Overflow)?;
        let end = start.checked_add(length).ok_or(WorkError::Overflow)?;
        if start < cursor || end > expected.len() {
            return Ok(false);
        }
        accounting.charge_items((start - cursor) as u64)?;
        if expected[cursor..start].iter().any(|byte| *byte != 0)
            || !checkout
                .staged_content_matches_inline(*content, &expected[start..end], meter)
                .await?
        {
            return Ok(false);
        }
        cursor = end;
    }
    accounting.charge_items((expected.len() - cursor) as u64)?;
    Ok(expected[cursor..].iter().all(|byte| *byte == 0))
}
