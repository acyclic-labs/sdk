use super::*;
use acyclic_test_support::measure;
use proptest::prelude::*;
use ratatui::text::{Line, Span};
use std::mem::size_of;

fn projection(size: usize, matches: usize) -> WrappedLines {
    let text = format!("{}{}", "needle ".repeat(matches), "x".repeat(size));
    let mut projection = WrappedLines::default();
    projection.update(&[Line::from(Span::raw(text))], 0, 80);
    projection
}

fn capacities(search: &TranscriptSearch) -> [usize; 7] {
    [
        search.needle.capacity(),
        search.previous_needle.capacity(),
        search.prefix.capacity(),
        search.positions.capacity(),
        search.previous.capacity(),
        search.matches.capacity(),
        search.local.capacity(),
    ]
}

fn closed<const RESERVE: bool>(size: usize) {
    let projection = projection(size, 2);
    let mut search = TranscriptSearch::default();
    for previously_open in [false, true] {
        if previously_open {
            search.update(&projection, "needle", 0, Style::default());
            search.prepare(&projection, 0, 10);
            while search.pending() {
                search.advance(&projection, 256);
            }
        }
        WORK.with(|work| work.set((0, 0)));
        let allocated = measure(|| {
            if RESERVE {
                // The removed production path allocated from transcript length
                // before checking whether Find was open.
                search.needle.reserve(projection.index.text.len());
            }
            search.project(&projection, false);
        });
        assert_eq!(
            allocated.count_total, 0,
            "closed Find must allocate zero times"
        );
        assert_eq!(allocated.bytes_total, 0);
        assert_eq!(WORK.with(|work| work.get()), (0, 0));
        assert_eq!(capacities(&search), [0; 7]);
        assert!(search.revision.is_none());
        assert!(!search.pending());
    }
}

#[test]
fn closed_find_allocates_zero_before_and_after_opening() {
    for size in [0, 8192, 16384, 65536] {
        closed::<false>(size);
    }
}

#[test]
#[should_panic(expected = "closed Find must allocate zero times")]
fn closed_transcript_reservation_negative_control() {
    closed::<true>(8192);
}

fn sparse<const RESERVE: bool>(size: usize, matches: usize) -> ([usize; 7], u64) {
    let projection = projection(size, matches);
    let mut search = TranscriptSearch::default();
    let initial = measure(|| search.project(&projection, true));
    assert_eq!(initial.count_total, 0);
    assert_eq!(initial.bytes_total, 0);
    let query = measure(|| {
        search.update(&projection, "needle", 0, Style::default());
        search.prepare(&projection, 0, 10);
    });
    let background = measure(|| {
        if RESERVE {
            search.positions.reserve(projection.index.text.len());
        }
        while search.pending() {
            search.advance(&projection, 256);
        }
    });
    assert_eq!(search.count(), matches);
    // Six-byte query: one String and one prefix vector; two visible matches
    // add one local vector, then two global vectors during counting.
    assert_eq!(query.count_total, 2 + u64::from(matches != 0));
    assert_eq!(
        background.count_total,
        2 * u64::from(matches != 0),
        "match storage must grow only for matches"
    );
    let capacity = capacities(&search);
    assert_eq!(
        capacity,
        [
            8,
            0,
            6,
            if matches == 0 { 0 } else { 4 },
            0,
            if matches == 0 { 0 } else { 4 },
            if matches == 0 { 0 } else { 4 }
        ]
    );
    let bytes = capacity[0]
        + capacity[1]
        + size_of::<usize>() * (capacity[2] + capacity[3] + capacity[4])
        + size_of::<Range<usize>>() * (capacity[5] + capacity[6]);
    assert_eq!(query.bytes_total + background.bytes_total, bytes as u64);
    (capacity, bytes as u64)
}

#[test]
fn sparse_open_find_capacity_and_exact_allocations_ignore_transcript_length() {
    for matches in [0, 2] {
        let n = sparse::<false>(8192, matches);
        assert_eq!(n, sparse::<false>(16384, matches));
        assert_eq!(n, sparse::<false>(65536, matches));
    }
}

#[test]
#[should_panic(expected = "match storage must grow only for matches")]
fn open_transcript_reservation_negative_control() {
    sparse::<true>(8192, 0);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn search_storage_tracks_query_viewport_and_matches(size in 1024usize..32768, matches in 0usize..3) {
        closed::<false>(size);
        prop_assert_eq!(sparse::<false>(size, matches), sparse::<false>(size * 2, matches));
    }
}

#[test]
fn candidate_capacity_tracks_matches_and_drops_unrelated_query_history() {
    let projection = projection(16384, 0);
    let mut search = TranscriptSearch::default();
    search.update(&projection, "x", 0, Style::default());
    search.prepare(&projection, 0, 10);
    while search.pending() {
        search.advance(&projection, 256);
        assert!(search.positions.capacity() <= (2 * search.positions.len()).max(4));
        assert!(search.matches.capacity() <= (2 * search.matches.len()).max(4));
        assert!(search.local.capacity() <= 2 * 80 * 10);
    }
    search.update(&projection, "needle", 0, Style::default());
    search.prepare(&projection, 0, 10);
    while search.pending() {
        search.advance(&projection, 256);
    }
    assert_eq!(search.positions.capacity(), 0);
    assert_eq!(search.previous.capacity(), 0);
    assert_eq!(search.matches.capacity(), 0);
    assert!(search.local.capacity() <= 2 * 80 * 10);
}
