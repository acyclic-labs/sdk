use super::*;
use proptest::prelude::*;
use ratatui::{text::Line, widgets::Widget};

fn projection(text: &str) -> WrappedLines {
    let mut projection = WrappedLines::default();
    projection.update(
        &[Line::from(ratatui::text::Span::raw(text.to_owned()))],
        0,
        80,
    );
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
        search.prepare(&projection,0,1);
        WORK.with(|work|work.set((0,0)));
        while search.pending() {search.advance(&projection,budget);}
        prop_assert!(WORK.with(|work|work.get().1)<=10*(projection.index.text.len()+search.needle.len()));
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
            Line::from(ratatui::text::Span::raw("final\nrow")),
        ],
        2,
        80,
    );
    search.update(&projection, "\n", 0, Style::default());
    finish(&mut search, &projection);
    assert_eq!(search.selected_row(&projection, 2), 2);
}

#[test]
#[should_panic(expected = "background must not rescan prefixes")]
fn background_prefix_rescan_negative_control() {
    let projection = projection(&"a".repeat(4096));
    WORK.with(|work| work.set((0, 0)));
    for end in (32..=4096).step_by(32) {
        scan(&projection, "b", &[0], 0..end, true, |_| {});
    }
    assert!(
        WORK.with(|work| work.get().1) <= 10 * projection.index.text.len(),
        "background must not rescan prefixes"
    );
}

#[test]
fn resize_keeps_completed_matches_but_repaints_the_new_viewport() {
    let source = [Line::raw("one one".repeat(1000))];
    let mut projection = WrappedLines::default();
    projection.update(&source, 0, 80);
    let mut search = TranscriptSearch::default();
    search.update(&projection, "one", 0, Style::default());
    finish(&mut search, &projection);
    let count = search.count();
    let generation = search.generation;
    projection.update(&source, source.len(), 40);
    search.update(&projection, "one", 0, Style::default());
    search.prepare(&projection, 1, 10);
    assert!(!search.pending());
    assert_eq!(search.count(), count);
    assert_eq!(search.generation, generation);
    assert_eq!(search.viewport.unwrap().1, projection.revision());
}

#[test]
fn appended_sources_index_the_preceding_separator() {
    let mut projection = projection("first");
    projection.update(&[Line::raw("first"), Line::raw("second")], 1, 80);
    assert_eq!(projection.index.text, "first\nsecond");
    let mut search = TranscriptSearch::default();
    search.update(&projection, "first\nsecond", 0, Style::default());
    finish(&mut search, &projection);
    assert_eq!(search.matches, [0..12]);
}

fn slice_model<const LINEAR: bool>(size: usize, budget: usize, index: i64) {
    let projection = projection(&"a".repeat(size));
    let mut search = TranscriptSearch::default();
    search.update(&projection, "a", index, Style::default());
    search.prepare(&projection, projection.rows().len().saturating_sub(2), 1);
    let selected = search.active()[search.position(index) - 1].clone();
    while search.pending() {
        WORK.with(|work| work.set((0, 0)));
        search.advance(&projection, budget);
        if LINEAR && !search.pending() {
            charge(search.matches.len(), true);
            std::hint::black_box(search.matches.iter().position(|range| *range == selected));
        }
        let work = WORK.with(|work| work.get().1);
        // One count pass, at most one nearest-direction probe, and indexed
        // selection lookup bounded by the machine word size, on EVERY slice.
        assert!(
            work <= 2 * budget + usize::BITS as usize,
            "every cooperative slice must be bounded: {work}"
        );
    }
    assert_eq!(search.active()[search.position(index) - 1], selected);
    assert_eq!(search.count(), size);
}

#[test]
fn completion_selection_lookup_stays_inside_every_slice_bound() {
    for size in [8192, 16384] {
        slice_model::<false>(size, 256, 0);
    }
}

#[test]
#[should_panic(expected = "every cooperative slice must be bounded")]
fn linear_completion_selection_negative_control() {
    slice_model::<true>(8192, 256, 0);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn every_count_slice_is_bounded_property(size in 4096usize..20000, budget in 64usize..512, index in -10i64..10) {
        slice_model::<false>(size,budget,index);
        slice_model::<false>(2*size,budget,index);
    }
}

fn stable_selection_model<const IDENTITY: bool>(size: usize, initial: usize, delta: i64) {
    let projection = projection(&"a\n".repeat(size));
    let mut search = TranscriptSearch::default();
    search.update(&projection, "a", 0, Style::default());
    search.prepare(&projection, initial, 10);
    search.advance(&projection, 31);
    search.update(&projection, "a", delta, Style::default());
    let selected = search.selected.clone().expect("viewport match");
    for _ in 0..12 {
        let first = search.selected_row(&projection, delta);
        search.prepare(&projection, first, 10);
        if !IDENTITY {
            // The old implementation reapplied the index to the rebuilt local list.
            search.selected = search
                .active()
                .get(delta.rem_euclid(search.active().len() as i64) as usize)
                .cloned();
        }
        search.advance(&projection, 31);
        assert_eq!(
            search.selected,
            Some(selected.clone()),
            "background frames must preserve match identity"
        );
        assert!(search.pending());
    }
    finish(&mut search, &projection);
    assert_eq!(
        search.selected,
        Some(selected),
        "count completion must preserve match identity"
    );
}

#[test]
fn enter_mid_count_preserves_identity_across_background_frames() {
    stable_selection_model::<true>(8192, 40, 1);
}

#[test]
#[should_panic(expected = "background frames must preserve match identity")]
fn local_index_selection_drift_negative_control() {
    stable_selection_model::<false>(8192, 40, 1);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn stable_match_identity_property(size in 4096usize..12000, first in 1usize..40, delta in 1i64..10) {
        stable_selection_model::<true>(size,first,delta);
    }
}

fn mixed_navigation_model<const LOCAL_WRAP: bool>(size: usize, gap: usize, events: &[(i8, u8)]) {
    let events: Vec<_> = events
        .iter()
        .map(|&(direction, frames)| (direction, frames, None))
        .collect();
    resize_navigation_model::<LOCAL_WRAP, false>(size, gap, false, &events);
}

fn resize_navigation_model<const LOCAL_WRAP: bool, const REPLAY_INITIAL: bool>(
    size: usize,
    gap: usize,
    initially_hidden: bool,
    events: &[(i8, u8, Option<(u16, u16)>)],
) {
    let mut text = format!("needle{}\n", "日x".repeat(gap)).repeat(size);
    if initially_hidden {
        text.insert_str(0, &format!("{}\n", "x".repeat(80)));
        text.push_str(&"x".repeat(12000));
    }
    let mut projection = projection(&text);
    let source = [Line::from(ratatui::text::Span::raw(text.clone()))];
    let reference = crate::transcript_matches(&text, "needle");
    let mut search = TranscriptSearch::default();
    search.update(&projection, "needle", 0, Style::default());
    let mut height = if initially_hidden { 1 } else { 10 };
    search.prepare(&projection, if initially_hidden { 0 } else { 40 }, height);
    let mut expected = reference
        .iter()
        .position(|r| Some(r) == search.selected.as_ref())
        .map(|r| r as i64);
    let mut index = 0;
    for &(direction, frames, resize) in events {
        index += i64::from(direction);
        expected =
            expected.map(|rank| (rank + i64::from(direction)).rem_euclid(reference.len() as i64));
        if LOCAL_WRAP && direction != 0 {
            let rank = search
                .local
                .iter()
                .position(|r| Some(r) == search.selected.as_ref())
                .unwrap_or(0);
            let rank =
                (rank as i64 + i64::from(direction)).rem_euclid(search.local.len() as i64) as usize;
            search.selected = Some(search.local[rank].clone());
            search.index = index;
        }
        search.update(&projection, "needle", index, Style::default());
        if let Some((width, rows)) = resize {
            height = usize::from(rows);
            projection.update(&source, source.len(), width);
            search.update(&projection, "needle", index, Style::default());
        }
        for _ in 0..=frames {
            let first = if expected.is_some() {
                search.selected_row(&projection, index)
            } else {
                0
            };
            let unanchored = search.selected.is_none();
            let queued = search.steps;
            search.prepare(&projection, first, height);
            if expected.is_none() {
                // Reference viewport matches use source identities, independently
                // of the production selection and its queued navigation state.
                let visible: Vec<_> = reference
                    .iter()
                    .enumerate()
                    .filter(|(_, range)| {
                        (first..first + height).contains(&projection.source_rows[range.start])
                    })
                    .map(|(rank, _)| rank)
                    .collect();
                if !visible.is_empty() {
                    expected =
                        Some(visible[index.rem_euclid(visible.len() as i64) as usize] as i64);
                }
            }
            if REPLAY_INITIAL && unanchored && search.selected.is_some() {
                // Restore the old handoff: initial rank consumes index but leaves
                // the same moves queued for the next navigate() call.
                search.steps = queued;
            }
            WORK.with(|w| w.set((0, 0)));
            search.advance(&projection, 64);
            let (front, back) = WORK.with(|w| w.get());
            assert!(
                front + back <= 6 * 64 + 128,
                "directional slice must stay bounded"
            );
            if expected.is_none() {
                expected = if !search.pending() {
                    Some(index.rem_euclid(reference.len() as i64))
                } else {
                    reference
                        .iter()
                        .position(|range| Some(range) == search.nearest.as_ref())
                        .map(|rank| rank as i64)
                };
            }
            if search.steps == 0 {
                assert_eq!(
                    search.selected.as_ref(),
                    expected.map(|rank| &reference[rank as usize]),
                    "mixed navigation must consume each move once across frames and resize"
                );
            }
        }
    }
    finish(&mut search, &projection);
    let expected = expected.unwrap_or_else(|| index.rem_euclid(reference.len() as i64));
    assert_eq!(
        search.selected.as_ref(),
        Some(&reference[expected as usize]),
        "mixed navigation must consume each move once across frames and resize"
    );
}

#[test]
fn backwards_after_viewport_handoff_returns_to_the_previous_source_match() {
    mixed_navigation_model::<false>(8192, 0, &[(1, 2), (-1, 2), (-1, 0), (1, 3)]);
}

#[test]
#[should_panic(expected = "mixed navigation must consume each move once")]
fn partial_viewport_wrap_negative_control() {
    mixed_navigation_model::<true>(8192, 0, &[(1, 2), (-1, 2)]);
}

#[test]
fn directional_lookup_wraps_only_after_reaching_the_transcript_boundary() {
    let projection = projection(&format!("needle{}needle", "x".repeat(12000)));
    let mut search = TranscriptSearch::default();
    search.update(&projection, "needle", 0, Style::default());
    search.prepare(&projection, 0, 1);
    search.update(&projection, "needle", -1, Style::default());
    assert_eq!(
        search.selected,
        Some(0..6),
        "a viewport edge is not a wrap boundary"
    );
    finish(&mut search, &projection);
    assert_eq!(search.selected, Some(12006..12012));
    search.update(&projection, "needle", 0, Style::default());
    assert_eq!(search.selected, Some(0..6));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn mixed_navigation_and_background_frames_property(
        size in 256usize..800,
        gap in 0usize..160,
        initially_hidden in any::<bool>(),
        events in prop::collection::vec((-1i8..=1, 0u8..4, prop::option::of((16u16..120,12u16..24))), 1..64),
    ) {
        resize_navigation_model::<false,false>(size,gap,initially_hidden,&events);
    }
}

#[test]
fn navigation_before_the_first_known_match_is_applied_once() {
    let projection = projection(&format!("needle needle needle{}", "x".repeat(12000)));
    let mut search = TranscriptSearch::default();
    search.update(&projection, "needle", 0, Style::default());
    search.prepare(&projection, 100, 1);
    assert!(search.selected.is_none());
    search.update(&projection, "needle", 1, Style::default());
    finish(&mut search, &projection);
    assert_eq!(search.selected, Some(7..13));
}

#[test]
fn reversing_navigation_cancels_a_probe_between_sparse_matches() {
    mixed_navigation_model::<false>(128, 300, &[(1, 0), (-1, 0), (-1, 2), (1, 0), (1, 4)]);
}

#[test]
fn resize_before_first_match_consumes_queued_navigation_once() {
    resize_navigation_model::<false, false>(
        8192,
        0,
        true,
        &[
            (1, 0, Some((40, 20))),
            (-1, 2, Some((80, 12))),
            (1, 1, Some((24, 16))),
        ],
    );
}

#[test]
#[should_panic(expected = "mixed navigation must consume each move once")]
fn initial_selection_replays_queued_move_negative_control() {
    resize_navigation_model::<false, true>(8192, 0, true, &[(1, 0, Some((40, 20)))]);
}
