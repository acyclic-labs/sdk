//! Whole-text specification of the line scanner, shared by its unit
//! properties and the fuzz target.
use super::LineScan;

/// Specification: the requested window of terminator-inclusive lines of the
/// whole text; an empty file has only line 1, and offsets past the end select
/// nothing. Shared by the unit properties and the fuzz target.
#[must_use]
pub fn reference(source: &[u8], offset: u64, limit: Option<u64>) -> Option<Vec<u8>> {
    let lines: Vec<&[u8]> = source.split_inclusive(|byte| *byte == b'\n').collect();
    if offset == 0 || limit == Some(0) {
        None
    } else if lines.is_empty() {
        (offset == 1).then(Vec::new)
    } else if offset > lines.len() as u64 {
        None
    } else {
        let start = (offset - 1) as usize;
        let end = limit.map_or(lines.len(), |limit| {
            usize::try_from(limit)
                .ok()
                .and_then(|limit| start.checked_add(limit))
                .map_or(lines.len(), |end| end.min(lines.len()))
        });
        Some(lines[start..end].concat())
    }
}

/// Drives the scanner over `bytes` split at `cuts`, collecting the retained
/// ranges the way the production reader does.
#[must_use]
pub fn scan_chunks(
    bytes: &[u8],
    cuts: impl IntoIterator<Item = usize>,
    offset: u64,
    limit: Option<u64>,
) -> (Option<Vec<u8>>, Option<&'static str>) {
    let mut scan = LineScan::new(offset, limit);
    let mut retained = Vec::new();
    let mut from = 0;
    for cut in cuts.into_iter().chain([bytes.len()]) {
        let cut = cut.min(bytes.len());
        if cut > from {
            let chunk = &bytes[from..cut];
            if let Some(range) = scan.update(chunk) {
                retained.extend_from_slice(&chunk[range]);
            }
            from = cut;
        }
    }
    let (selects, media) = scan.finish();
    (selects.then_some(retained), media)
}
