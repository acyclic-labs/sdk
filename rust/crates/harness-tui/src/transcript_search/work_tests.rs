use super::*;
use proptest::prelude::*;
use ratatui::{text::Line, widgets::Widget};

fn projection(text: &str) -> WrappedLines {
    let mut projection = WrappedLines::default();
    projection.update(&[Line::raw(text.to_owned())], 0, 80);
    projection
}
fn finish(search: &mut TranscriptSearch, projection: &WrappedLines) {
    while search.pending() {
        search.advance(projection, 31);
    }
}
fn work_model<const FULL: bool>(size: usize, query: &str) -> (usize, usize) {
    let projection = projection(&"a".repeat(size));
    let mut search = TranscriptSearch::default();
    search.project(&projection);
    search.update(&projection, "a", 0, Style::default());
    search.prepare(&projection, 10, 10);
    finish(&mut search, &projection);
    let area = Rect::new(0, 0, 80, 1);
    let mut buffer = Buffer::empty(area);
    WORK.with(|work| work.set((0, 0)));
    search.update(&projection, query, 1, Style::default());
    search.prepare(&projection, 10, 10);
    for row in 10..20 {
        search.row(&projection, row).render(area, &mut buffer);
    }
    if FULL {
        finish(&mut search, &projection);
    }
    let (front, back) = WORK.with(|work| work.get());
    let frame = front + back;
    // This includes all query comparison/prefix/matching work, not just painting.
    WORK.with(|work| work.set((0, 0)));
    finish(&mut search, &projection);
    let background = WORK.with(|work| work.get().1);
    (frame, background)
}
fn assert_frame<const FULL: bool>(size: usize) {
    for query in ["A", "b", "aa", "ab"] {
        let n = work_model::<FULL>(size, query);
        let two_n = work_model::<FULL>(2 * size, query);
        assert_eq!(
            n.0, two_n.0,
            "complete foreground query work must be independent of retained length"
        );
        assert!(n.0 <= 1610, "foreground includes no archive scan");
        assert!(
            n.1 <= 4 * size + 64 && two_n.1 <= 8 * size + 64,
            "background work must be linear"
        );
    }
}
#[test]
fn complete_query_path_has_equal_n_and_2n_frame_work_and_linear_background() {
    assert_frame::<false>(8192);
}
#[test]
#[should_panic(expected = "complete foreground query work must be independent of retained length")]
fn whole_query_in_frame_negative_control() {
    assert_frame::<true>(8192);
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn full_query_work_bound_property(size in 2000usize..6000) {assert_frame::<false>(size);}
    #[test]
    fn background_partitions_and_refinement_equal_reference(
        text in "[a-zA-ZßΣσςİ日€🦀 ]{0,96}",
        query in "[a-zA-ZßΣσςİ日€🦀 ]{0,8}",
        budget in 1usize..32,
    ) {
        let projection=projection(&text);
        let mut search=TranscriptSearch::default();
        search.update(&projection,&query,0,Style::default());
        while search.pending() {search.advance(&projection,budget);}
        prop_assert_eq!(&search.matches,&crate::transcript_matches(&text,&query));
        let refined=format!("{query}s");
        search.update(&projection,&refined,0,Style::default());
        while search.pending() {search.advance(&projection,budget);}
        prop_assert_eq!(&search.matches,&crate::transcript_matches(&text,&refined));
    }
}
#[test]
fn next_key_cancels_partial_count_and_transcript_changes_refresh_the_index() {
    let mut projection = projection(&format!("{}b", "a".repeat(20000)));
    let mut search = TranscriptSearch::default();
    search.update(&projection, "a", 0, Style::default());
    search.advance(&projection, 256);
    assert!(search.pending());
    search.update(&projection, "b", 0, Style::default());
    finish(&mut search, &projection);
    assert_eq!(search.matches, [20000..20001]);
    projection.update(&[Line::raw("Straße STRASSE")], 0, 80);
    search.update(&projection, "strasse", 0, Style::default());
    finish(&mut search, &projection);
    assert_eq!(search.matches, [0..7, 8..15]);
}
#[test]
fn nearest_direction_probe_precedes_the_full_count() {
    let projection = projection(&format!(
        "{}needle{}needle{}",
        "x".repeat(4000),
        "x".repeat(4000),
        "x".repeat(4000)
    ));
    for (direction, expected) in [(0, 8006..8012), (-1, 4000..4006)] {
        let mut search = TranscriptSearch::default();
        search.update(&projection, "needle", direction, Style::default());
        search.prepare(&projection, 75, 1);
        for _ in 0..10 {
            search.advance(&projection, 256);
            if search.nearest.is_some() {
                break;
            }
        }
        assert!(search.pending());
        assert_eq!(search.nearest, Some(expected));
    }
}

#[test]
fn refinement_checks_only_the_new_suffix_of_prior_candidates() {
    let projection = projection(&"a".repeat(8192));
    let mut search = TranscriptSearch::default();
    search.update(&projection, &"a".repeat(128), 0, Style::default());
    finish(&mut search, &projection);
    let candidates = search.positions.len();
    search.update(&projection, &"a".repeat(129), 0, Style::default());
    search.prepare(&projection, 10, 10);
    WORK.with(|work| work.set((0, 0)));
    finish(&mut search, &projection);
    assert!(
        WORK.with(|work| work.get().1) <= 2 * candidates,
        "refinement checks only the appended suffix"
    );
    assert_eq!(search.count(), 8192 - 128);
}

#[test]
fn indexed_navigation_covers_line_terminators_and_changed_suffixes() {
    let mut projection = projection("first\nsecond\nthird");
    let mut search = TranscriptSearch::default();
    search.update(&projection, "\n", 0, Style::default());
    finish(&mut search, &projection);
    assert_eq!(search.selected_row(&projection, 1), 1);
    projection.update(
        &[Line::raw("first"), Line::raw("second"), Line::raw("third")],
        0,
        80,
    );
    projection.update(
        &[
            Line::raw("first"),
            Line::raw("second"),
            Line::raw("final\nrow"),
        ],
        2,
        80,
    );
    search.update(&projection, "\n", 0, Style::default());
    finish(&mut search, &projection);
    assert_eq!(search.selected_row(&projection, 2), 2);
}
