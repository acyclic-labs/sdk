use super::*;
use proptest::prelude::*;

// Independent eager reference: old whole-transcript wrapping, initialized
// separately from the candidate's source table, layouts and row cache.
fn eager(source: &[Line<'_>], width: u16) -> (Vec<Line<'static>>, Vec<RowPosition>) {
    let mut rows = Vec::new();
    let mut positions = Vec::new();
    for (line, text) in source.iter().enumerate() {
        let text: String = text
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect();
        let layout = Layout::new(&Editor::new(&text), usize::from(width));
        for (row, (_, wrapped)) in visible_rows(&layout).enumerate() {
            let text: String = wrapped
                .cells
                .iter()
                .map(|cell| cell.symbol.as_str())
                .collect();
            rows.push(Line::raw(text));
            positions.push(RowPosition { line, row });
        }
    }
    (rows, positions)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Property: lazy page navigation clamps exactly like ordered eager row
    // navigation, and appending at the tail never moves a retained anchor.
    #[test]
    fn source_anchors_match_the_eager_view(
        text in prop::collection::vec("[a-z日🙂 ]{0,48}", 1..40),
        width in 1u16..30,
        height in 1usize..12,
        motions in prop::collection::vec(-20isize..20, 1..20),
    ) {
        let mut source: Vec<_> = text.into_iter().map(Line::raw).collect();
        let (old, positions) = eager(&source, width);
        let maximum = old.len().saturating_sub(height);
        let mut expected = maximum;
        let mut anchor = None;
        let mut candidate = WrappedLines::default();
        candidate.update(&source, 0, width);
        for motion in motions {
            expected = expected.saturating_add_signed(motion).min(maximum);
            anchor = candidate.move_by(anchor, motion, height);
            let top = anchor.map_or(Top::Tail, Top::At);
            let window = candidate.view(top, height);
            prop_assert_eq!(Some(window.top), positions.get(expected).copied());
            let contents = |rows: &[Line<'_>]| rows.iter().map(|row| {
                row.spans.iter().map(|span| span.content.as_ref()).collect::<String>()
            }).collect::<Vec<_>>();
            prop_assert_eq!(contents(candidate.shown()), contents(old.get(expected..(expected + height).min(old.len())).unwrap()));
            prop_assert_eq!(anchor.is_none(), expected == maximum);
        }
        if let Some(anchor) = anchor {
            let before = candidate.shown().to_vec();
            let changed = source.len();
            source.push(Line::raw("appended output"));
            candidate.update(&source, changed, width);
            candidate.view(Top::At(anchor), height);
            prop_assert_eq!(candidate.shown(), before.as_slice());
        }
    }
}

fn visible_work(length: usize, eager_control: bool) -> (usize, usize, usize) {
    let source = vec![Line::raw("plain transcript row"); length];
    let mut candidate = WrappedLines::default();
    candidate.update(&source, 0, 80);
    WORK.with(|work| work.set((0, 0, 0)));
    if eager_control {
        candidate.wrap_all();
    } else {
        candidate.view(Top::Tail, 10);
    }
    WORK.with(|work| work.get())
}

#[test]
fn first_frame_and_resize_wrap_exactly_the_visible_rows() {
    for length in [1000, 2000] {
        assert_eq!(visible_work(length, false), (10, 200, 10));
        let source = vec![Line::raw("plain transcript row"); length];
        let mut candidate = WrappedLines::default();
        candidate.update(&source, 0, 80);
        candidate.view(Top::Tail, 10);
        candidate.update(&source, length, 40);
        WORK.with(|work| work.set((0, 0, 0)));
        candidate.view(Top::Tail, 10);
        assert_eq!(WORK.with(|work| work.get()), (10, 200, 10));
    }
}

#[test]
fn negative_control_eager_wrapping_grows_with_the_archive() {
    assert_eq!(visible_work(1000, true), (1000, 20000, 1000));
    assert_eq!(visible_work(2000, true), (2000, 40000, 2000));
}

fn assert_equal_frame_costs(eager_control: bool) {
    let mut costs = Vec::new();
    for length in [1000, 2000] {
        let source = vec![Line::raw("plain transcript row"); length];
        let mut candidate = WrappedLines::default();
        candidate.update(&source, 0, 80);
        for width in [80, 40] {
            candidate.update(&source, length, width);
            WORK.with(|work| work.set((0, 0, 0)));
            let allocations = acyclic_test_support::measure(|| {
                if eager_control {
                    candidate.wrap_all();
                } else {
                    candidate.view(Top::Tail, 10);
                }
            });
            costs.push((
                WORK.with(|work| work.get()),
                allocations.count_total,
                allocations.bytes_total,
            ));
            if !eager_control {
                WORK.with(|work| work.set((0, 0, 0)));
                let repeated = acyclic_test_support::measure(|| {
                    candidate.view(Top::Tail, 10);
                });
                assert_eq!(WORK.with(|work| work.get()), (0, 0, 0));
                assert_eq!((repeated.count_total, repeated.bytes_total), (0, 0));
            }
        }
    }
    assert_eq!(
        costs.get(..2).unwrap(),
        costs.get(2..).unwrap(),
        "frame work and allocations depend on the archive"
    );
}

#[test]
fn first_paint_and_resize_have_equal_work_and_allocations_at_n_and_2n() {
    assert_equal_frame_costs(false);
}

#[test]
#[should_panic(expected = "frame work and allocations depend on the archive")]
fn negative_control_eager_work_fails_the_same_cost_assertion() {
    assert_equal_frame_costs(true);
}

#[test]
fn an_anchored_wheel_step_wraps_only_the_three_new_rows() {
    let mut costs = Vec::new();
    for length in [1000, 2000] {
        let mut source = vec![Line::raw("plain transcript row"); length];
        let mut candidate = WrappedLines::default();
        candidate.update(&source, 0, 80);
        let from = RowPosition {
            line: length / 2,
            row: 0,
        };
        candidate.view(Top::At(from), 10);
        WORK.with(|work| work.set((0, 0, 0)));
        let allocations = acyclic_test_support::measure(|| {
            assert_eq!(
                candidate.move_by(Some(from), -3, 10),
                Some(RowPosition {
                    line: length / 2 - 3,
                    row: 0,
                })
            );
        });
        assert_eq!(WORK.with(|work| work.get()), (3, 60, 3));
        costs.push((allocations.count_total, allocations.bytes_total));
        let anchor = candidate.window().top;
        source.push(Line::raw("streamed output"));
        candidate.update(&source, length, 80);
        WORK.with(|work| work.set((0, 0, 0)));
        let unchanged = acyclic_test_support::measure(|| {
            candidate.view(Top::At(anchor), 10);
        });
        assert_eq!(WORK.with(|work| work.get()), (0, 0, 0));
        assert_eq!((unchanged.count_total, unchanged.bytes_total), (0, 0));
    }
    assert_eq!(costs.first().unwrap(), costs.get(1).unwrap());
}

#[test]
fn reflow_preserves_the_source_and_search_index() {
    let mut candidate = WrappedLines::default();
    candidate.replace(&[Line::raw("searchable source")], 0);
    candidate.reflow(80);
    let text = candidate.text().to_owned();
    let revision = candidate.search_index().revision;
    let measured = acyclic_test_support::measure(|| candidate.reflow(40));
    assert_eq!(candidate.text(), text);
    assert_eq!(candidate.search_index().revision, revision);
    assert_eq!((measured.count_total, measured.bytes_total), (0, 0));
}

#[test]
#[ignore = "manual lane before/after measurement"]
fn client_perf_baseline() {
    for length in [1000, 10000] {
        let source = vec![Line::raw("plain transcript row"); length];
        let mut candidate = WrappedLines::default();
        for width in [80, 40] {
            WORK.with(|work| work.set((0, 0, 0)));
            let started = std::time::Instant::now();
            let measured = acyclic_test_support::measure(|| {
                candidate.update(&source, if width == 80 { 0 } else { length }, width);
                candidate.view(Top::Tail, 10);
            });
            eprintln!(
                "CLIENT_PERF_WINDOWED os={} n={length} width={width} elapsed_us={} work={:?} allocations={} bytes={}",
                std::env::consts::OS,
                started.elapsed().as_micros(),
                WORK.with(|work| work.get()),
                measured.count_total,
                measured.bytes_total
            );
        }
    }
}

#[test]
#[ignore = "manual million-line scroll measurement"]
fn client_perf_million_line_scroll() {
    let source = vec![Line::raw("plain transcript row"); 1_000_000];
    let mut candidate = WrappedLines::default();
    let started = std::time::Instant::now();
    candidate.update(&source, 0, 80);
    eprintln!(
        "CLIENT_PERF_MILLION os={} ingest_us={}",
        std::env::consts::OS,
        started.elapsed().as_micros()
    );
    for width in [80, 40] {
        candidate.reflow(width);
        WrappedLines::take_work();
        let started = std::time::Instant::now();
        let measured = acyclic_test_support::measure(|| {
            candidate.view(
                Top::At(RowPosition {
                    line: 500_000,
                    row: 0,
                }),
                10,
            );
        });
        assert_eq!(WrappedLines::take_work(), (10, 200, 10));
        eprintln!(
            "CLIENT_PERF_MILLION os={} width={width} frame_us={} allocations={} bytes={}",
            std::env::consts::OS,
            started.elapsed().as_micros(),
            measured.count_total,
            measured.bytes_total
        );
        WrappedLines::take_work();
        let started = std::time::Instant::now();
        candidate.move_by(
            Some(RowPosition {
                line: 500_000,
                row: 0,
            }),
            -3,
            10,
        );
        assert_eq!(WrappedLines::take_work(), (3, 60, 3));
        eprintln!(
            "CLIENT_PERF_MILLION os={} width={width} wheel_us={}",
            std::env::consts::OS,
            started.elapsed().as_micros()
        );
    }
}

fn styled_cells(source: &[Line<'_>], width: u16) -> Vec<(RowPosition, ratatui::buffer::Buffer)> {
    use ratatui::{
        buffer::Buffer,
        layout::Rect,
        widgets::{Paragraph, Widget},
    };
    let area = Rect::new(0, 0, width, 1);
    let mut output = Vec::new();
    for (line, source) in source.iter().enumerate() {
        let text: String = source
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect();
        let layout = Layout::new(&Editor::new(&text), usize::from(width));
        let mut row = 0;
        for (index, wrapped) in layout.rows.iter().enumerate() {
            if wrapped.cells.is_empty() && index > 0 {
                let previous = layout.rows.get(index - 1).unwrap();
                if previous.end == wrapped.start
                    && previous
                        .cells
                        .last()
                        .is_some_and(|cell| cell.column + cell.width == usize::from(width))
                {
                    continue;
                }
            }
            let spans = wrapped
                .cells
                .iter()
                .map(|cell| {
                    let mut end = 0;
                    let style = source
                        .spans
                        .iter()
                        .find_map(|span| {
                            end += span.content.len();
                            (cell.source.start < end).then_some(span.style)
                        })
                        .unwrap_or_default();
                    Span::styled(cell.symbol.clone(), style)
                })
                .collect::<Vec<_>>();
            let mut expected = Line::from(spans).style(source.style);
            expected.alignment = source.alignment;
            let mut buffer = Buffer::empty(area);
            Paragraph::new(expected).render(area, &mut buffer);
            output.push((RowPosition { line, row }, buffer));
            row += 1;
        }
    }
    output
}

fn assert_styled_windows(lose_styles: bool) {
    use ratatui::{
        buffer::Buffer,
        layout::Rect,
        style::Color,
        widgets::{Paragraph, Widget},
    };
    let source: Vec<_> = (0..40)
        .map(|index| {
            let mut line = Line::from(vec![
                Span::styled("a日", Style::default().fg(Color::Red)),
                Span::styled("e", Style::default().fg(Color::Blue)),
                Span::styled("\u{301}🙂\t", Style::default().add_modifier(Modifier::BOLD)),
                Span::styled("\u{7}z", Style::default().fg(Color::Green)),
            ])
            .style(Style::default().bg(Color::DarkGray));
            line.alignment = Some(match index % 3 {
                0 => Alignment::Left,
                1 => Alignment::Center,
                _ => Alignment::Right,
            });
            line
        })
        .collect();
    let mut candidate_source = source.clone();
    if lose_styles {
        for line in &mut candidate_source {
            for span in &mut line.spans {
                span.style = Style::default();
            }
        }
    }
    let mut candidate = WrappedLines::default();
    for width in [3, 7, 11] {
        candidate.update(
            &candidate_source,
            if width == 3 { 0 } else { source.len() },
            width,
        );
        candidate.view(Top::At(RowPosition { line: 20, row: 0 }), 8);
        let eager = styled_cells(&source, width);
        let first = eager
            .iter()
            .position(|(position, _)| *position == candidate.window().top)
            .unwrap();
        let area = Rect::new(0, 0, width, 1);
        for (row, (_, expected)) in candidate.shown().iter().zip(eager.get(first..).unwrap()) {
            let mut actual = Buffer::empty(area);
            Paragraph::new(row.clone()).render(area, &mut actual);
            assert_eq!(
                &actual, expected,
                "styled Unicode viewport differs from eager cells"
            );
        }
    }
}

#[test]
fn styled_unicode_windows_match_independent_eager_cells_after_resize() {
    assert_styled_windows(false);
}

#[test]
#[should_panic(expected = "styled Unicode viewport differs from eager cells")]
fn negative_control_losing_span_styles_fails_the_same_cell_comparison() {
    assert_styled_windows(true);
}

#[test]
fn scroll_distance_uses_only_retained_physical_rows() {
    let mut candidate = WrappedLines::default();
    candidate.update(&vec![Line::raw("abcdefgh"); 1000], 0, 4);
    candidate.view(Top::At(RowPosition { line: 500, row: 1 }), 10);
    let from = RowPosition { line: 500, row: 1 };
    assert_eq!(
        candidate.cached_distance(from, RowPosition { line: 503, row: 0 }),
        Some(5)
    );
    assert_eq!(
        candidate.cached_distance(from, RowPosition { line: 0, row: 0 }),
        None
    );
    assert_eq!(
        candidate.cached_distance(from, RowPosition { line: 501, row: 2 }),
        None
    );
}
