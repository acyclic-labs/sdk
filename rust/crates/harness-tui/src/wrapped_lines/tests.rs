use super::*;
use crate::TranscriptSearch;

fn viewport_model<const FULL: bool, const REFLOW: bool>(length: usize) {
    let source = [Line::raw("a".repeat(length))];
    let mut projection = WrappedLines::default();
    projection.update(&source, 0, 80);
    let revision = projection.revision();
    let mut search = TranscriptSearch::default();
    search.update(&projection, "a", 0, Style::default());
    WORK.with(|work| work.set((0, 0, 0)));
    crate::layout::BUILDS.with(|builds| builds.set(0));
    projection.update(&source, source.len(), 80);
    search.update(&projection, "A", 1, Style::default());
    if REFLOW {
        std::hint::black_box(Layout::new(&Editor::new(projection.text()), 80));
    }
    if FULL {
        for row in 0..projection.rows().len() {
            std::hint::black_box(projection.highlighted_row(row, &[], None, Style::default()));
        }
    }
    search.prepare(&projection, 10, 10);
    let mut buffer = ratatui::buffer::Buffer::empty(ratatui::layout::Rect::new(0, 0, 80, 1));
    for row in 10..20 {
        ratatui::widgets::Widget::render(
            search.row(&projection, row),
            ratatui::layout::Rect::new(0, 0, 80, 1),
            &mut buffer,
        );
    }
    assert_eq!(projection.revision(), revision);
    assert_eq!(
        crate::layout::BUILDS.with(|builds| builds.get()),
        0,
        "query changes must reuse the existing layout"
    );
    assert_eq!(
        WORK.with(|work| work.get()),
        (10, 800, 0),
        "frame work must depend only on the visible viewport"
    );
}

#[test]
fn frame_work_is_exactly_visible_rows_and_cells_without_layout() {
    for length in [8192, 81920] {
        viewport_model::<false, false>(length);
    }
}

#[test]
#[should_panic(expected = "frame work must depend only on the visible viewport")]
fn full_transcript_repaint_negative_control() {
    viewport_model::<true, false>(8192);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn viewport_work_is_independent_of_retained_length(length in 1600usize..8000) {
        viewport_model::<false, false>(length);
    }
}

#[test]
#[should_panic(expected = "query changes must reuse the existing layout")]
fn second_query_layout_negative_control() {
    viewport_model::<false, true>(8192);
}
