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

fn normalize_reference(text: &str) -> String {
    let mut normalized = String::new();
    let mut previous_cr = false;
    for character in text.chars() {
        if character != '\n' || !previous_cr {
            normalized.push(if character == '\r' { '\n' } else { character });
        }
        previous_cr = character == '\r';
    }
    normalized
}

fn operation() -> impl Strategy<Value = (u8, String)> {
    (
        0u8..11,
        prop::collection::vec(
            prop_oneof![
                Just("a"),
                Just(" "),
                Just("\n"),
                Just("\r"),
                Just("\r\n"),
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
    fn undo_storage_is_bounded_by_edits_independently_of_buffer_size(size in 0usize..8192, edits in 0usize..40) {
        let initial = "界".repeat(size);
        let mut editor = Editor::new(&initial);
        for i in 0..edits { editor.insert(" ", i as u64); }
        // Each space is a separate group. The bound includes allocated vector
        // capacity and fold metadata, not just the payload strings' lengths.
        prop_assert!(editor.retained_undo_bytes() <= edits * 512);
        for _ in 0..edits { prop_assert!(editor.undo()); }
        prop_assert_eq!(editor.text(), initial);
        prop_assert!(editor.retained_undo_bytes() <= edits * 512);
    }
    #[test]
    fn random_edits_match_string_model(ops in prop::collection::vec(operation(), 0..80)) {
        let mut editor = Editor::default();
        let mut model = Model::default();
        let mut history = acyclic_harness_tui::PromptHistory::default();
        let mut undo = vec![];
        let mut redo = vec![];
        let mut recalled_draft = None;
        for (step, (op, text)) in ops.into_iter().enumerate() {
            editor.break_group();
            let before = model.clone();
            let points = boundaries(&model.text);
            let left = points.iter().copied().take_while(|i| *i < model.cursor).last().unwrap_or(0);
            let right = points.iter().copied().find(|i| *i > model.cursor).unwrap_or(model.text.len());
            match op {
                0 | 1 => {
                    editor.insert(&text, step as u64);
                    let normalized = normalize_reference(&text);
                    model.text.insert_str(model.cursor, &normalized);
                    let desired = model.cursor + normalized.len();
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
                8 => {
                    let expected = redo.pop();
                    prop_assert_eq!(editor.redo(), expected.is_some());
                    if let Some((before, after)) = expected { model = after.clone(); undo.push((before, after)); }
                }
                9 => {
                    let entry = format!("saved{text}");
                    history.push(entry.clone());
                    recalled_draft = None;
                    prop_assert_eq!(editor.recall(&mut history, true, step as u64), model.cursor == 0);
                    if model.cursor == 0 {
                        recalled_draft = Some(model.text.clone());
                        model.text = normalize_reference(&entry);
                        undo.push((before, model.clone())); redo.clear();
                    }
                }
                _ => {
                    let expected = model.cursor == model.text.len() && recalled_draft.is_some();
                    prop_assert_eq!(editor.recall(&mut history, false, step as u64), expected);
                    if expected {
                        model.text = recalled_draft.take().unwrap();
                        model.cursor = model.text.len();
                        if !before.text.is_empty() || !model.text.is_empty() {
                            undo.push((before, model.clone())); redo.clear();
                        }
                    }
                }
            }
            prop_assert_eq!(editor.text(), &model.text);
            prop_assert_eq!(editor.cursor(), model.cursor);
            prop_assert_eq!(editor.anchor(), model.cursor);
            prop_assert!(!editor.text().contains('\r'));
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
        let text = normalize_reference(&parts.concat());
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
            prop_assert!(column < width);
        }
    }
}
