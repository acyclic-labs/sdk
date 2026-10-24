use super::*;
use ratatui::style::Color;

fn compare(actual: &mut WrappedLines, source: &[Line<'_>], width: u16) {
    let mut expected = WrappedLines::default();
    expected.update(source, 0, width);
    assert_eq!(actual.text(), expected.text());
    let index = actual.search_index();
    let fresh_index = expected.search_index();
    assert_eq!(index.text, fresh_index.text);
    for byte in 0..index.text.len() {
        assert_eq!(index.source(byte, 1), fresh_index.source(byte, 1));
    }
    actual.wrap_all();
    expected.wrap_all();
    assert_eq!(actual.rows(), expected.rows());
    assert_eq!(actual.ranges(), expected.ranges());
    assert_eq!(actual.origins, expected.origins);
}

#[test]
fn range_edits_match_eager_text_styles_positions_and_search() {
    for width in [1, 7, 80] {
        for top in [0, 8, 19] {
            let mut source: Vec<_> = (0..20)
                .map(|index| Line::raw(format!("line {index} 界")))
                .collect();
            let mut actual = WrappedLines::default();
            actual.update(&source, 0, width);
            actual.view(Top::At(RowPosition { line: top, row: 0 }), 5);
            actual.search_index();
            for (range, rows) in [
                (
                    1..2,
                    vec![Line::from(Span::styled(
                        "Straße\r\n👩‍💻",
                        Style::default().fg(Color::Red),
                    ))],
                ),
                (10..10, vec![Line::raw("inserted"), Line::raw("two")]),
                (18..20, vec![]),
                (0..3, vec![Line::raw("replacement")]),
                (2..6, vec![Line::raw("last")]),
            ] {
                source.splice(range.clone(), rows.clone());
                actual.replace_range(range, &rows);
                compare(&mut actual, &source, width);
                actual.view(Top::At(RowPosition { line: top, row: 0 }), 5);
            }
        }
    }
}

fn offscreen<const FULL: bool>() {
    let mut source: Vec<_> = (0..100)
        .map(|index| Line::raw(format!("line {index}")))
        .collect();
    let mut actual = WrappedLines::default();
    actual.update(&source, 0, 80);
    actual.view(Top::Tail, 10);
    WrappedLines::take_work();
    source.splice(0..1, [Line::raw("changed"), Line::raw("inserted")]);
    if FULL {
        actual.replace(&source, 0);
    } else {
        actual.replace_range(0..1, source.get(..2).unwrap());
    }
    actual.view(Top::Tail, 10);
    assert_eq!(
        WrappedLines::take_work().2,
        0,
        "offscreen edit must preserve the tail layouts"
    );
    compare(&mut actual, &source, 80);
}

#[test]
fn offscreen_range_edit_preserves_tail_layouts() {
    offscreen::<false>();
}

#[test]
#[should_panic(expected = "offscreen edit must preserve the tail layouts")]
fn negative_control_full_replacement_rewraps_unchanged_tail() {
    offscreen::<true>();
}

#[test]
fn onscreen_edit_lays_out_only_replacement_lines() {
    let mut source: Vec<_> = (0..10)
        .map(|index| Line::raw(format!("line {index}")))
        .collect();
    let mut actual = WrappedLines::default();
    actual.update(&source, 0, 80);
    actual.wrap_all();
    WrappedLines::take_work();
    *source.get_mut(4).unwrap() = Line::raw("changed");
    actual.replace_range(4..5, source.get(4..5).unwrap());
    actual.wrap_all();
    assert_eq!(WrappedLines::take_work().2, 1);
    compare(&mut actual, &source, 80);
}
