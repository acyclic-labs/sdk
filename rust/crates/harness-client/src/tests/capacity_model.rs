use super::{page, subject};
use crate::{PageDirection, PageWindow, PageWindowPolicy, WindowPage};
use proptest::prelude::*;

#[derive(Clone, Copy)]
struct Span {
    start: u32,
    end: u32,
    bytes: u64,
}

// The specification chooses the largest contiguous prefix/suffix containing
// the incoming page that fits all three limits. It does not simulate eviction.
fn retained_spans(
    candidates: &[Span],
    before: bool,
    pages: usize,
    items: usize,
    bytes: u64,
) -> Result<Vec<Span>, TestCaseError> {
    let item_limit =
        u64::try_from(items).map_err(|error| TestCaseError::fail(error.to_string()))?;
    let mut best = None;
    for count in 1..=pages.min(candidates.len()) {
        let range = if before {
            0..count
        } else {
            candidates.len() - count..candidates.len()
        };
        let selected = candidates
            .get(range)
            .ok_or_else(|| TestCaseError::fail("invalid model page range"))?;
        let entries: u64 = selected
            .iter()
            .map(|span| u64::from(span.end - span.start))
            .sum();
        let charge: u64 = selected.iter().map(|span| span.bytes).sum();
        if entries <= item_limit && charge <= bytes {
            best = Some(selected.to_vec());
        }
    }
    best.ok_or_else(|| TestCaseError::fail("the incoming page must fit by itself"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn variable_capacities_preserve_contiguous_whole_pages(
        start in 0u32..64,
        page_limit in 1usize..5,
        item_limit in 1usize..17,
        byte_pages in 1u64..5,
        operations in prop::collection::vec((any::<bool>(), 1u32..9), 1..100),
    ) {
        let max_count = u32::try_from(item_limit.min(8))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let byte_limit = page(0, 8).retained_bytes() * byte_pages;
        let initial = page(start, (start + max_count).min(64));
        let mut model = vec![Span {
            start,
            end: (start + max_count).min(64),
            bytes: initial.retained_bytes(),
        }];
        let mut window = PageWindow::new(subject(PageWindowPolicy::new(
            page_limit, item_limit, byte_limit,
        ))?);
        subject(window.reset(initial))?;
        for (before, count) in operations {
            let first = model.first().ok_or_else(|| TestCaseError::fail("missing first page"))?.start;
            let end = model.last().ok_or_else(|| TestCaseError::fail("missing last page"))?.end;
            let direction = if before { PageDirection::Before } else { PageDirection::After };
            let more = if before { first > 0 } else { end < 64 };
            let revision = window.revision();
            prop_assert_eq!(window.has_more(direction), more);
            let mut request = subject(window.begin(direction))?;
            if more {
                let expected_cursor = if before { first } else { end - 1 };
                prop_assert_eq!(request.as_ref().and_then(crate::PageRequest::cursor), Some(&expected_cursor));
                let count = count.min(max_count);
                let (start, end) = if before {
                    (first.saturating_sub(count), first)
                } else {
                    (end, (end + count).min(64))
                };
                let incoming = page(start, end);
                let span = Span { start, end, bytes: incoming.retained_bytes() };
                let mut candidates = model.clone();
                if before { candidates.insert(0, span); } else { candidates.push(span); }
                model = retained_spans(&candidates, before, page_limit, item_limit, byte_limit)?;
                subject(window.finish(&mut request, Some(incoming)))?;
                prop_assert!(request.is_none());
                prop_assert_eq!(window.revision(), revision + 1);
            } else {
                prop_assert!(request.is_none());
                prop_assert_eq!(window.revision(), revision);
            }
            let expected: Vec<_> = model.iter().flat_map(|span| span.start..span.end).collect();
            let actual: Vec<_> = window.items().copied().collect();
            prop_assert_eq!(&actual, &expected);
            prop_assert!(actual.windows(2).all(|pair| matches!(pair, [left, right] if *right == *left + 1)));
            prop_assert_eq!(window.len(), expected.len());
            prop_assert_eq!(window.page_count(), model.len());
            prop_assert_eq!(window.retained_page_bytes(), model.iter().map(|span| span.bytes).sum::<u64>());
            prop_assert!(window.len() <= item_limit);
            prop_assert!(window.page_count() <= page_limit);
            prop_assert!(window.retained_page_bytes() <= byte_limit);
            prop_assert_eq!(window.has_more(PageDirection::Before), expected.first().is_some_and(|item| *item > 0));
            prop_assert_eq!(window.has_more(PageDirection::After), expected.last().is_some_and(|item| *item < 63));
        }
    }
}
