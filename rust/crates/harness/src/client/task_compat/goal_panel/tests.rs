use super::*;
use crate::{
    goal::GoalState,
    task_volume::{Cursor, SCHEMA},
};
use proptest::prelude::*;

fn draw(markdown: String, reason: Option<String>, area: Rect) -> (usize, Buffer) {
    let cursor = Cursor {
        volume: [1; 32],
        epoch: uuid::Uuid::nil(),
        schema: SCHEMA,
        query: [2; 32],
        position: 1,
        accepted: 1,
        durable: 1,
        commit: [3; 32],
    };
    let goal = GoalFileView::Present {
        goal: GoalState {
            status: GoalStatus::Active,
            reason,
            markdown,
        },
    };
    let line_starts = super::super::file_query::goal_line_starts(&goal);
    let view = GoalQueryView {
        cursor: &cursor,
        request_cursor: None,
        goal: &goal,
        progress: None,
        line_starts: &line_starts,
    };
    let mut buffer = Buffer::empty(area);
    let work = GoalPanel { view }.render_counted(area, &mut buffer);
    (work, buffer)
}

#[test]
fn multi_megabyte_single_lines_have_viewport_bounded_redraw_work() {
    let area = Rect::new(0, 0, 42, 8);
    for scalar in ["x", "\u{301}", "\0", "界"] {
        let long = scalar.repeat(2 * 1024 * 1024);
        let (work, buffer) = draw(format!("{long}\nNext"), None, area);
        assert!(work <= 41 * 6, "visited {work} scalars");
        let next = (1..41).map(|x| buffer[(x, 3)].symbol()).collect::<String>();
        assert_eq!(next.trim_end(), "Next");
        let (work, _) = draw("body".into(), Some(long), area);
        assert!(work <= 41 * 6, "visited {work} reason scalars");
    }
    // Negative control: the former eager line escaping traverses the whole line.
    let old_work = "x"
        .repeat(2 * 1024 * 1024)
        .lines()
        .next()
        .unwrap()
        .chars()
        .count();
    assert!(old_work > 41 * 6);
}

#[test]
fn clipping_preserves_line_breaks_and_escapes_terminal_controls() {
    let (_, buffer) = draw(
        "one\r\ntwo\u{1b}[31m\nthree\r".into(),
        None,
        Rect::new(0, 0, 42, 8),
    );
    let row = |y| {
        (1..41)
            .map(|x| buffer[(x, y)].symbol())
            .collect::<String>()
            .trim_end()
            .to_owned()
    };
    assert_eq!(row(2), "one");
    assert_eq!(row(3), "two\\u{1b}[31m");
    assert_eq!(row(4), "three\\r");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn arbitrary_unicode_traversal_is_bounded_by_visible_cells(
        markdown in prop::collection::vec(any::<char>(), 0..512),
        reason in prop::option::of(prop::collection::vec(any::<char>(), 0..256)),
        width in 0u16..48, height in 0u16..12,
    ) {
        let area = Rect::new(0, 0, width, height);
        let (work, buffer) = draw(markdown.into_iter().collect(), reason.map(|chars| chars.into_iter().collect()), area);
        let body = Block::bordered().inner(area);
        prop_assert!(work <= (usize::from(body.width) + 1) * usize::from(body.height));
        prop_assert_eq!(buffer.area, area);
    }
}
