use super::*;
use proptest::prelude::*;

mod capacity_model;

struct Page {
    entries: Vec<u32>,
    before: Arc<u32>,
    after: Arc<u32>,
    generation: u64,
    left: bool,
    right: bool,
}

impl WindowPage for Page {
    type Item = u32;
    type Key = u32;
    type Cursor = u32;
    type Generation = u64;
    fn items(&self) -> &[u32] {
        &self.entries
    }
    fn key(item: &u32) -> &u32 {
        item
    }
    fn before(&self) -> &Arc<u32> {
        &self.before
    }
    fn after(&self) -> &Arc<u32> {
        &self.after
    }
    fn generation(&self) -> &u64 {
        &self.generation
    }
    fn has_more_before(&self) -> bool {
        self.left
    }
    fn has_more_after(&self) -> bool {
        self.right
    }
    fn retained_bytes(&self) -> u64 {
        let bytes = std::mem::size_of::<Self>()
            + self.entries.capacity() * std::mem::size_of::<u32>()
            + 2 * (std::mem::size_of::<u32>() + 2 * std::mem::size_of::<usize>());
        u64::try_from(bytes).unwrap_or(u64::MAX)
    }
}

fn page(start: u32, end: u32) -> Page {
    assert!(start < end && end <= 64);
    Page {
        entries: (start..end).collect(),
        before: Arc::new(start),
        after: Arc::new(end - 1),
        generation: 1,
        left: start > 0,
        right: end < 64,
    }
}

type TestResult = Result<(), PageWindowError>;
fn policy(items: usize) -> Result<PageWindowPolicy, PageWindowError> {
    PageWindowPolicy::new(2, items, 4096)
}
fn edge(request: &Option<PageRequest<u32>>) -> Result<u32, PageWindowError> {
    request
        .as_ref()
        .and_then(PageRequest::cursor)
        .copied()
        .ok_or(PageWindowError::NoProgress)
}
fn load(window: &mut PageWindow<Page>, direction: PageDirection, count: u32) -> TestResult {
    let mut request = window.begin(direction)?;
    if request.is_none() {
        return Ok(());
    }
    let cursor = edge(&request)?;
    let incoming = match direction {
        PageDirection::Before => page(cursor.saturating_sub(count), cursor),
        PageDirection::After => page(cursor + 1, (cursor + 1 + count).min(64)),
    };
    window.finish(&mut request, Some(incoming))
}

#[test]
fn whole_page_eviction_moves_the_exclusive_cursor() -> TestResult {
    let mut window = PageWindow::new(policy(2)?);
    window.reset(page(1, 3))?;
    load(&mut window, PageDirection::After, 1)?;
    assert_eq!(window.items().copied().collect::<Vec<_>>(), vec![3]);
    let mut request = window.begin(PageDirection::Before)?;
    assert_eq!(edge(&request)?, 3);
    window.finish(&mut request, Some(page(1, 3)))?;
    assert_eq!(window.items().copied().collect::<Vec<_>>(), vec![1, 2]);
    assert!(window.has_more(PageDirection::After));
    Ok(())
}

#[test]
fn the_surviving_page_owns_both_exclusive_edges() -> TestResult {
    let mut window = PageWindow::new(policy(6)?);
    window.reset(page(3, 6))?;
    load(&mut window, PageDirection::After, 3)?;
    load(&mut window, PageDirection::After, 3)?;
    let mut request = window.begin(PageDirection::Before)?;
    assert_eq!(edge(&request)?, 6);
    window.finish(&mut request, Some(page(3, 6)))?;
    assert_eq!(
        window.items().copied().collect::<Vec<_>>(),
        (3..9).collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn reset_retains_one_outstanding_fetch_until_physical_settlement() -> TestResult {
    let mut window = PageWindow::new(policy(6)?);
    window.reset(page(3, 6))?;
    let mut request = window.begin(PageDirection::After)?;
    for _ in 0..100 {
        window.reset(page(30, 33))?;
        assert!(matches!(
            window.begin(PageDirection::Before),
            Err(PageWindowError::Busy)
        ));
    }
    assert_eq!(
        window.finish(&mut request, Some(page(6, 9))),
        Err(PageWindowError::StaleResponse)
    );
    assert!(request.is_none());
    assert_eq!(
        window.items().copied().collect::<Vec<_>>(),
        vec![30, 31, 32]
    );
    let mut next = window.begin(PageDirection::Before)?;
    window.finish(&mut next, None)?;
    assert!(window.begin(PageDirection::After)?.is_some());
    Ok(())
}

#[test]
fn foreign_or_settled_responses_cannot_consume_another_fetch() -> TestResult {
    let mut left = PageWindow::new(policy(6)?);
    let mut right = PageWindow::new(policy(6)?);
    left.reset(page(3, 6))?;
    right.reset(page(3, 6))?;
    let mut request = left.begin(PageDirection::After)?;
    assert_eq!(
        right.finish(&mut request, None),
        Err(PageWindowError::ForeignRequest)
    );
    assert!(request.is_some());
    left.finish(&mut request, None)?;
    let mut pending = left.begin(PageDirection::Before)?;
    assert_eq!(
        left.finish(&mut request, None),
        Err(PageWindowError::SettledRequest)
    );
    assert!(matches!(
        left.begin(PageDirection::After),
        Err(PageWindowError::Busy)
    ));
    left.finish(&mut pending, None)?;
    Ok(())
}

#[test]
fn invalid_pages_are_rejected_before_mutation() -> TestResult {
    let mut window = PageWindow::new(policy(6)?);
    window.reset(page(3, 6))?;
    let revision = window.revision();
    assert_eq!(window.reset(page(0, 64)), Err(PageWindowError::Capacity));
    let mut request = window.begin(PageDirection::After)?;
    assert_eq!(
        window.finish(&mut request, Some(page(4, 7))),
        Err(PageWindowError::DuplicateKey)
    );
    let mut request = window.begin(PageDirection::After)?;
    let mut changed = page(6, 9);
    changed.generation = 2;
    assert_eq!(
        window.finish(&mut request, Some(changed)),
        Err(PageWindowError::GenerationChanged)
    );
    assert_eq!(window.revision(), revision);
    assert_eq!(window.items().copied().collect::<Vec<_>>(), vec![3, 4, 5]);
    Ok(())
}

#[test]
fn nonterminal_pages_must_advance_the_exclusive_edge() -> TestResult {
    let mut window = PageWindow::new(policy(6)?);
    window.reset(page(3, 6))?;
    let mut request = window.begin(PageDirection::After)?;
    let mut incoming = page(6, 9);
    incoming.after = Arc::new(5);
    assert_eq!(
        window.finish(&mut request, Some(incoming)),
        Err(PageWindowError::NoProgress)
    );
    assert_eq!(window.items().copied().collect::<Vec<_>>(), vec![3, 4, 5]);
    Ok(())
}

#[test]
fn byte_budget_evicts_complete_custody_and_rejects_oversized_pages() -> TestResult {
    let bytes = page(0, 3).retained_bytes();
    let mut window = PageWindow::new(PageWindowPolicy::new(4, 64, bytes)?);
    window.reset(page(0, 3))?;
    load(&mut window, PageDirection::After, 3)?;
    assert_eq!(window.page_count(), 1);
    assert_eq!(window.retained_page_bytes(), bytes);
    assert_eq!(window.items().copied().collect::<Vec<_>>(), vec![3, 4, 5]);
    assert_eq!(window.reset(page(0, 64)), Err(PageWindowError::Capacity));
    Ok(())
}

#[test]
fn empty_terminal_pages_preserve_the_admitted_data_page() -> TestResult {
    for direction in [PageDirection::Before, PageDirection::After] {
        let mut window = PageWindow::new(PageWindowPolicy::new(1, 3, 4096)?);
        window.reset(page(3, 6))?;
        let mut request = window.begin(direction)?;
        let mut terminal = page(3, 6);
        terminal.entries.clear();
        terminal.left = false;
        terminal.right = false;
        window.finish(&mut request, Some(terminal))?;
        assert_eq!(window.items().copied().collect::<Vec<_>>(), vec![3, 4, 5]);
        assert_eq!(window.page_count(), 1);
        assert!(!window.has_more(direction));
        assert!(window.begin(direction)?.is_none());
    }
    Ok(())
}

#[test]
fn unchanged_reads_borrow_the_same_items_and_keep_the_revision() -> TestResult {
    let mut window = PageWindow::new(policy(6)?);
    window.reset(page(3, 6))?;
    let pointer = std::ptr::from_ref(window.items().next().ok_or(PageWindowError::NoProgress)?);
    let revision = window.revision();
    for _ in 0..100 {
        assert_eq!(
            std::ptr::from_ref(window.items().next().ok_or(PageWindowError::NoProgress)?),
            pointer
        );
        assert_eq!(window.revision(), revision);
    }
    Ok(())
}

#[test]
fn finite_policies_reject_zero_and_excessive_limits() {
    for args in [
        (0, 1, 1),
        (65, 1, 1),
        (1, 0, 1),
        (1, 4097, 1),
        (1, 1, 0),
        (1, 1, 16 * 1024 * 1024 + 1),
    ] {
        assert_eq!(
            PageWindowPolicy::new(args.0, args.1, args.2),
            Err(PageWindowError::Capacity)
        );
    }
}

fn subject<T>(result: Result<T, PageWindowError>) -> Result<T, TestCaseError> {
    result.map_err(|error| TestCaseError::fail(error.to_string()))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn alternating_exclusive_pages_have_no_holes_or_duplicates(
        start in 0u32..61, directions in prop::collection::vec(any::<bool>(), 1..100)
    ) {
        let mut window = PageWindow::new(subject(policy(6))?);
        subject(window.reset(page(start, start + 3)))?;
        for before in directions {
            let previous: Vec<_> = window.items().copied().collect();
            let (&first, &last) = previous.first().zip(previous.last())
                .ok_or_else(|| TestCaseError::fail("empty preceding window"))?;
            let direction = if before { PageDirection::Before } else { PageDirection::After };
            let had_more = window.has_more(direction);
            subject(load(&mut window, direction, 3))?;
            let actual: Vec<_> = window.items().copied().collect();
            let (&new_first, &new_last) = actual.first().zip(actual.last())
                .ok_or_else(|| TestCaseError::fail("empty resulting window"))?;
            prop_assert_eq!(&actual, &(new_first..=new_last).collect::<Vec<_>>());
            if had_more {
                if before { prop_assert_eq!(new_first, first.saturating_sub(3)); }
                else { prop_assert_eq!(new_last, (last + 3).min(63)); }
            } else { prop_assert_eq!(&actual, &previous); }
            prop_assert!(window.len() <= 6 && window.page_count() <= 2 && window.retained_page_bytes() <= 4096);
            prop_assert_eq!(window.has_more(PageDirection::Before), new_first > 0);
            prop_assert_eq!(window.has_more(PageDirection::After), new_last < 63);
        }
    }
}
