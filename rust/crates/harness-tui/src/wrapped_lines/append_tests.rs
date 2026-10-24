use super::*;
use ratatui::style::Color;

fn same_projection(actual: &mut WrappedLines, source: &[Line<'_>], width: u16) {
    let mut expected = WrappedLines::default();
    expected.update(source, 0, width);
    expected.wrap_all();
    actual.view(Top::Tail, usize::MAX);
    assert_eq!(actual.rows(), expected.rows());
    assert_eq!(actual.ranges(), expected.ranges());
    for row in 0..actual.rows().len() {
        assert_eq!(actual.position_at(row), expected.position_at(row));
    }
}

#[test]
fn appended_unicode_styles_and_crlf_equal_eager_replacement() {
    for width in [1, 2, 7, 80] {
        let mut source = vec![Line::raw("prefix")];
        let mut actual = WrappedLines::default();
        actual.update(&source, 0, width);
        actual.view(Top::Tail, 10);
        for (index, text) in [
            "e", "\u{301}", " 界", "🇬", "🇧", "👩", "\u{200d}", "💻", "\t", "\r", "\n", "tail",
        ]
        .into_iter()
        .enumerate()
        {
            let style = Style::default().fg(if index % 2 == 0 {
                Color::Red
            } else {
                Color::Blue
            });
            for line in &mut source {
                line.spans.push(Span::styled(text, style));
            }
            actual.append_line(text, style);
            same_projection(&mut actual, &source, width);
        }
    }
}

fn flat<const REPLACE: bool>(bytes: usize) -> (usize, usize, usize) {
    let mut source = vec![Line::raw("x".repeat(bytes))];
    let mut actual = WrappedLines::default();
    actual.update(&source, 0, 80);
    actual.view(Top::Tail, 10);
    WrappedLines::take_work();
    if REPLACE {
        for line in &mut source {
            for span in &mut line.spans {
                span.content.to_mut().push('x');
            }
        }
        actual.update(&source, 0, 80);
    } else {
        actual.append_line("x", Style::default());
    }
    actual.view(Top::Tail, 10);
    let work = WrappedLines::take_work();
    assert!(
        work.1 <= 160,
        "open-line work is bounded by the tail: {work:?}"
    );
    assert_eq!(actual.shown().len(), 10);
    work
}

#[test]
fn one_token_rebuilds_the_same_tail_at_n_and_2n() {
    let work = flat::<false>(960);
    assert_eq!(work, flat::<false>(9600));
    assert_eq!(work, flat::<false>(96000));
}

#[test]
#[should_panic(expected = "open-line work is bounded by the tail")]
fn negative_control_replacing_the_open_line_rebuilds_its_prefix() {
    flat::<true>(9600);
}

fn hidden<const WRAP: bool>() {
    let mut actual = WrappedLines::default();
    let mut source = vec![Line::raw("x".repeat(9600))];
    actual.update(&source, 0, 80);
    actual.view(Top::Tail, 10);
    actual.hide();
    WrappedLines::take_work();
    for _ in 0..32 {
        actual.append_line("x", Style::default());
        if WRAP {
            actual.view(Top::Tail, 10);
        } else {
            actual.view(Top::Tail, 0);
        }
    }
    assert_eq!(
        WrappedLines::take_work(),
        (0, 0, 0),
        "hidden appends do not wrap"
    );
    for line in &mut source {
        for span in &mut line.spans {
            span.content.to_mut().push_str(&"x".repeat(32));
        }
    }
    same_projection(&mut actual, &source, 80);
}

#[test]
fn hidden_and_zero_height_appends_defer_wrapping_until_shown() {
    hidden::<false>();
}

#[test]
#[should_panic(expected = "hidden appends do not wrap")]
fn negative_control_hidden_appends_wrap_without_visibility_gate() {
    hidden::<true>();
}

#[test]
#[ignore = "manual incremental open-line measurement"]
fn open_line_append_measurement() {
    for bytes in [1000, 10000, 100000] {
        let mut actual = WrappedLines::default();
        actual.update(&[Line::raw("x".repeat(bytes))], 0, 80);
        actual.view(Top::Tail, 10);
        WrappedLines::take_work();
        let start = std::time::Instant::now();
        let allocations = acyclic_test_support::measure(|| {
            actual.append_line("x", Style::default());
            actual.view(Top::Tail, 10);
        });
        eprintln!(
            "CLIENT_OPEN_LINE_AFTER bytes={bytes} elapsed_us={} allocations={} allocated_bytes={} work={:?}",
            start.elapsed().as_micros(),
            allocations.count_total,
            allocations.bytes_total,
            WrappedLines::take_work()
        );
    }
}
