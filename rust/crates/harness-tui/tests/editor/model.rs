use acyclic_harness_tui::{Editor, Layout, Motion};
use proptest::prelude::*;
use unicode_segmentation::UnicodeSegmentation;

// Independent String + cursor model. Each generated operation is an explicit
// undo group, so its reference history is just a stack of String snapshots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Model {
    text: String,
    cursor: usize,
}

fn boundaries(text: &str) -> Vec<usize> {
    text.grapheme_indices(true)
        .map(|(i, _)| i)
        .chain([text.len()])
        .collect()
}

fn operation() -> impl Strategy<Value = (u8, String)> {
    (
        0u8..9,
        prop::collection::vec(
            prop_oneof![
                Just("a"),
                Just(" "),
                Just("\n"),
                Just("界"),
                Just("👩‍💻"),
                Just("e\u{301}"),
                Just("\u{301}"),
                Just("\u{200d}"),
                Just("🇺"),
                Just("🇸"),
            ],
            0..4,
        )
        .prop_map(|parts| parts.concat()),
    )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]
    #[test]
    fn random_edits_match_string_model(ops in prop::collection::vec(operation(), 0..80)) {
        let mut editor = Editor::default();
        let mut model = Model::default();
        let mut undo = vec![];
        let mut redo = vec![];
        for (step, (op, text)) in ops.into_iter().enumerate() {
            editor.break_group();
            let before = model.clone();
            let points = boundaries(&model.text);
            let left = points.iter().copied().take_while(|i| *i < model.cursor).last().unwrap_or(0);
            let right = points.iter().copied().find(|i| *i > model.cursor).unwrap_or(model.text.len());
            match op {
                0 | 1 => {
                    editor.insert(&text, step as u64);
                    model.text.insert_str(model.cursor, &text);
                    let desired = model.cursor + text.len();
                    model.cursor = boundaries(&model.text).into_iter().find(|i| *i >= desired).unwrap_or(model.text.len());
                    if !text.is_empty() { undo.push((before, model.clone())); redo.clear(); }
                }
                2 | 3 => {
                    let range = if op == 2 { left..model.cursor } else { model.cursor..right };
                    editor.delete(op == 2, step as u64);
                    if !range.is_empty() {
                        let desired = range.start;
                        model.text.replace_range(range, "");
                        model.cursor = boundaries(&model.text).into_iter().find(|i| *i >= desired).unwrap_or(model.text.len());
                        undo.push((before, model.clone())); redo.clear();
                    }
                }
                4 => { editor.move_cursor(Motion::Left, false, 8); model.cursor = left; }
                5 => { editor.move_cursor(Motion::Right, false, 8); model.cursor = right; }
                6 => { editor.set_cursor(0, false); model.cursor = 0; }
                7 => {
                    let expected = undo.pop();
                    prop_assert_eq!(editor.undo(), expected.is_some());
                    if let Some((before, after)) = expected { model = before.clone(); redo.push((before, after)); }
                }
                _ => {
                    let expected = redo.pop();
                    prop_assert_eq!(editor.redo(), expected.is_some());
                    if let Some((before, after)) = expected { model = after.clone(); undo.push((before, after)); }
                }
            }
            prop_assert_eq!(editor.text(), &model.text);
            prop_assert_eq!(editor.cursor(), model.cursor);
            prop_assert!(boundaries(editor.text()).contains(&editor.cursor()));
            prop_assert!(boundaries(editor.text()).contains(&editor.anchor()));
        }
        let final_text = editor.text().to_owned();
        let mut count = 0;
        while editor.undo() { count += 1; }
        prop_assert_eq!(editor.text(), "");
        for _ in 0..count { prop_assert!(editor.redo()); }
        prop_assert_eq!(editor.text(), final_text);
    }

    #[test]
    fn layout_preserves_graphemes_and_cell_bounds(
        parts in prop::collection::vec(operation().prop_map(|(_, text)| text), 0..12), width in 1usize..32
    ) {
        let text = parts.concat();
        let editor = Editor::new(&text);
        let layout = Layout::new(&editor, width);
        let points = boundaries(&text);
        for row in &layout.rows {
            prop_assert!(points.contains(&row.start));
            prop_assert!(points.contains(&row.end));
            let mut end_column = 0;
            for cell in &row.cells {
                prop_assert!(points.contains(&cell.source.start));
                prop_assert!(points.contains(&cell.source.end));
                prop_assert_eq!(cell.column, end_column);
                end_column += cell.width;
                prop_assert!(end_column <= width);
            }
        }
        for offset in points {
            let (row, column) = layout.position(offset);
            prop_assert!(row < layout.rows.len());
            prop_assert!(column < width || column == width && text[offset..].starts_with('\n'));
        }
    }
}
