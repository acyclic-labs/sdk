use acyclic_harness_tui::{Editor, Layout, Motion};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness_tui::{
    InputEffect, PromptInput,
    key_event::{KeyCode, KeyEvent, KeyModifiers},
};
use unicode_segmentation::UnicodeSegmentation;

/// Apply editor operations and check grapheme, paste, layout and undo semantics.
///
/// # Panics
/// Panics if a production editor transition violates a shared invariant.
pub fn exercise(data: &[u8]) -> Editor {
    let mut editor = Editor::default();
    let mut history = Vec::<String>::new();
    let mut cursor = None;
    let mut draft = String::new();
    for (step, chunk) in data.chunks(8).enumerate() {
        let Some((&op, rest)) = chunk.split_first() else {
            continue;
        };
        let text = String::from_utf8_lossy(rest);
        let mut recalled = false;
        match op % 22 {
            0..=2 => editor.insert(&text, step as u64),
            3 => editor.paste(&text.repeat(150), step as u64),
            4 => editor.delete(true, step as u64),
            5 => editor.delete(false, step as u64),
            6 => editor.move_cursor(Motion::Left, op > 128, 1 + usize::from(op % 32)),
            7 => editor.move_cursor(Motion::Right, op > 128, 1 + usize::from(op % 32)),
            8 => editor.move_cursor(Motion::Up, op > 128, 1 + usize::from(op % 32)),
            9 => editor.move_cursor(Motion::Down, op > 128, 1 + usize::from(op % 32)),
            10 => {
                editor.undo();
            }
            11 => {
                editor.redo();
            }
            12 => editor.select_all(),
            13 => editor.kill(Motion::WordLeft, step as u64),
            14 => editor.yank(step as u64),
            15 => editor.rotate_yank(step as u64),
            16 => {
                if let Some(text) = editor.enter(op > 128, step as u64) {
                    history.push(text);
                    cursor = None;
                }
            }
            17 => editor.set_cursor(usize::from(op), op > 128),
            18 => editor.delete_word(op > 128, step as u64),
            19 => {
                let previous = op > 128;
                if editor.selection().is_empty()
                    && editor.cursor() == if previous { 0 } else { editor.text().len() }
                {
                    let expected = editor.text().to_owned();
                    let selected = if previous && !history.is_empty() {
                        if cursor.is_none() {
                            draft = expected.clone();
                        }
                        let index = cursor.unwrap_or(history.len()).saturating_sub(1);
                        cursor = Some(index);
                        history.get(index).cloned()
                    } else if !previous && cursor.is_some() {
                        cursor = None;
                        Some(draft.clone())
                    } else {
                        None
                    };
                    recalled = selected.is_some();
                    assert!(apply_history(
                        &mut editor,
                        previous,
                        selected.as_deref(),
                        step as u64
                    ));
                }
            }
            20 => editor.set_text(&text, step as u64),
            _ => {
                history.push(text.into_owned());
                cursor = None;
            }
        }
        assert!(!editor.text().contains('\r'));
        let boundaries: Vec<_> = editor
            .text()
            .grapheme_indices(true)
            .map(|(i, _)| i)
            .chain([editor.text().len()])
            .collect();
        assert!(boundaries.contains(&editor.cursor()));
        assert!(boundaries.contains(&editor.anchor()));
        for fold in editor.collapsed_pastes() {
            assert!(boundaries.contains(&fold.start));
            assert!(boundaries.contains(&fold.end));
            assert!(fold.start < fold.end);
        }
        let layout = Layout::new(&editor, 1 + usize::from(op % 32));
        let (row, _) = layout.position(editor.cursor());
        assert!(row < layout.rows.len());
        let snapshot = (editor.text().to_owned(), editor.collapsed_pastes().to_vec());
        let selection = (editor.cursor(), editor.anchor());
        let mut round_trip = editor.clone();
        if round_trip.undo() {
            assert!(round_trip.redo());
            assert_eq!(
                (round_trip.text(), round_trip.collapsed_pastes()),
                (snapshot.0.as_str(), snapshot.1.as_slice())
            );
            if recalled {
                assert_eq!((round_trip.cursor(), round_trip.anchor()), selection);
            }
        }
    }
    editor
}

/// Deliver one selected history value through the supported input boundary.
pub fn recall(editor: &mut Editor, previous: bool, text: &str, at: u64) -> bool {
    if !editor.selection().is_empty()
        || editor.cursor() != if previous { 0 } else { editor.text().len() }
    {
        return false;
    }
    apply_history(editor, previous, Some(text), at)
}

#[cfg(not(target_arch = "wasm32"))]
fn apply_history(editor: &mut Editor, previous: bool, selected: Option<&str>, at: u64) -> bool {
    let mut input = PromptInput::new(std::mem::take(editor));
    let expected = input.editor.text().to_owned();
    let effects = input.key(
        KeyEvent::new(
            if previous { KeyCode::Up } else { KeyCode::Down },
            KeyModifiers::NONE,
        ),
        at,
        80,
    );
    let accepted = if let [InputEffect::History { request, .. }] = effects.as_slice() {
        input.resolve_history(*request, &expected, selected, previous, at)
    } else {
        false
    };
    *editor = input.editor;
    accepted
}

#[cfg(target_arch = "wasm32")]
fn apply_history(editor: &mut Editor, previous: bool, selected: Option<&str>, at: u64) -> bool {
    if let Some(text) = selected {
        if previous {
            editor.set_text_at_start(text, at);
        } else {
            editor.set_text(text, at);
        }
    }
    true
}
