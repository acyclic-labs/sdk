use acyclic_harness_tui::{Editor, Layout, Motion};
use unicode_segmentation::UnicodeSegmentation;

pub fn exercise(data: &[u8]) -> Editor {
    let mut editor = Editor::default();
    let mut history = acyclic_harness_tui::PromptHistory::default();
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
                }
            }
            17 => editor.set_cursor(usize::from(op), op > 128),
            18 => editor.delete_word(op > 128, step as u64),
            19 => {
                recalled = editor.recall(&mut history, op > 128, step as u64);
            }
            20 => editor.set_text(&text, step as u64),
            _ => history.push(text.into_owned()),
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
