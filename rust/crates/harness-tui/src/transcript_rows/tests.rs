use super::*;
use crate::{TranscriptSearch, transcript_matches};
use proptest::prelude::*;
use ratatui::{text::Line, widgets::Widget};

fn check<const OMIT_TOOLS: bool>(
    answer: &[String],
    tools: &[String],
    query: &str,
    width: u16,
    budget: usize,
) {
    let mut left = WrappedLines::default();
    let mut right = WrappedLines::default();
    let lines = |values: &[String]| values.iter().cloned().map(Line::raw).collect::<Vec<_>>();
    left.update(&lines(answer), 0, width);
    right.update(&lines(tools), 0, width);
    let expected_text = answer
        .iter()
        .chain(tools)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    let expected = transcript_matches(&expected_text, query);
    let rows = if OMIT_TOOLS {
        TranscriptRows::from(&left)
    } else {
        TranscriptRows::new(&left, &right)
    };
    let mut search = TranscriptSearch::default();
    search.update(rows, query, 0, Style::default());
    search.prepare(rows, 0, 1);
    while search.pending() {
        search.advance(rows, budget);
    }
    assert_eq!(
        search.count(),
        expected.len(),
        "Find must cover every rendered row"
    );
    assert_eq!(rows.len(), left.rows().len() + right.rows().len());
    let offset = if answer.is_empty() {
        0
    } else {
        answer.join("\n").len() + usize::from(!tools.is_empty())
    };
    let origins: Vec<_> = left
        .ranges()
        .iter()
        .cloned()
        .chain(
            right
                .ranges()
                .iter()
                .map(|range| offset + range.start..offset + range.end),
        )
        .collect();
    for (index, matched) in expected.iter().enumerate() {
        search.update(
            rows,
            query,
            i64::try_from(index).expect("bounded match count"),
            Style::default(),
        );
        let expected_row = origins
            .partition_point(|range| range.start <= matched.start)
            .saturating_sub(1);
        assert_eq!(
            search.selected_row(rows, i64::try_from(index).expect("bounded match count")),
            expected_row
        );
        assert_eq!(
            search.position(i64::try_from(index).expect("bounded match count")),
            index + 1
        );
    }
    search.prepare(rows, 0, rows.len());
    let area = Rect::new(
        0,
        0,
        width,
        u16::try_from(rows.len()).expect("bounded row count"),
    );
    let mut actual = Buffer::empty(area);
    let mut plain = Buffer::empty(area);
    for (row, line) in left.rows().iter().chain(right.rows()).enumerate() {
        let area = Rect::new(0, u16::try_from(row).expect("bounded row count"), width, 1);
        search.row(rows, row).render(area, &mut actual);
        line.render(area, &mut plain);
    }
    assert_eq!(
        actual
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>(),
        plain
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // A borrowed composition must have exactly the matches, order and painted
    // symbols of the independently concatenated rendered source, at every split.
    #[test]
    fn composed_rows_match_rendered_text(
        answer in prop::collection::vec("[a-zA-ZßΣσςİ日€🦀 ]{0,24}", 0..5),
        tools in prop::collection::vec("[a-zA-ZßΣσςİ日€🦀 ]{0,24}", 0..5),
        query in "[a-zA-ZßΣσςİ日€🦀 \n]{0,5}",
        width in 4u16..40,
        budget in 1usize..16,
    ) { check::<false>(&answer, &tools, &query, width, budget); }
}

#[test]
fn unicode_match_crosses_the_answer_tool_boundary() {
    check::<false>(&["Straße".into()], &["Σigma".into()], "se\nσi", 4, 1);
}

#[test]
#[should_panic(expected = "Find must cover every rendered row")]
fn omitted_tool_rows_negative_control() {
    check::<true>(&["answer".into()], &["tool needle".into()], "needle", 40, 1);
}

fn allocation_check<const COPY: bool>() {
    let mut answer = WrappedLines::default();
    let mut tools = WrappedLines::default();
    answer.update(&[Line::raw("answer")], 0, 80);
    tools.update(&[Line::raw("output".repeat(10000))], 0, 80);
    let measured = acyclic_test_support::measure(|| {
        let rows = TranscriptRows::new(&answer, &tools);
        if COPY {
            std::hint::black_box(tools.rows().to_vec());
        }
        std::hint::black_box((rows.len(), rows.index_revision(), rows.revision()));
    });
    assert_eq!(
        measured.count_total, 0,
        "the row source must borrow retained rows"
    );
}

#[test]
fn borrowing_the_row_source_has_no_allocations() {
    allocation_check::<false>();
}

#[test]
#[should_panic(expected = "the row source must borrow retained rows")]
fn recopied_tool_rows_negative_control() {
    allocation_check::<true>();
}

#[test]
fn tool_replacement_and_reflow_invalidate_only_the_matching_cut() {
    let mut answer = WrappedLines::default();
    let mut tools = WrappedLines::default();
    answer.update(&[Line::raw("answer")], 0, 8);
    tools.update(&[Line::raw("old")], 0, 8);
    let mut search = TranscriptSearch::default();
    for (output, width, expected) in [
        ("old", 8, 0),
        ("needle needle", 8, 2),
        ("needle needle", 4, 2),
        ("removed", 4, 0),
    ] {
        tools.update(&[Line::raw(output)], 0, width);
        let rows = TranscriptRows::new(&answer, &tools);
        search.update(rows, "needle", 0, Style::default());
        search.prepare(rows, 0, rows.len());
        while search.pending() {
            search.advance(rows, 1);
        }
        assert_eq!(search.count(), expected);
    }
}
