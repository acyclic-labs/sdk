use acyclic_harness_tui::{CompletionMenu, Editor};

/// Compare incremental popup output and acceptance with a fresh full parse after
/// every edit. The reference menu never shares cached parser/editor state.
pub fn exercise(data: &[u8]) {
    let mut catalog = CompletionMenu::default();
    for path in ["workspace/a.rs", "workspace/a b.rs", "workspace/aa file.rs"] {
        catalog.set_file(path, Some(b"")).unwrap();
    }
    catalog.set_file("commands/a.md", Some(b"a")).unwrap();
    let mut menu = catalog.clone();
    let mut editor = Editor::default();
    menu.refresh(&editor);
    for (step, chunk) in data.chunks(8).enumerate() {
        let (&op, bytes) = chunk.split_first().unwrap();
        let text = String::from_utf8_lossy(bytes);
        match op % 9 {
            0 => editor.insert(&text, step as u64),
            1 => editor.delete(true, step as u64),
            2 => editor.delete(false, step as u64),
            3 | 4 => editor.set_cursor(
                usize::from(bytes.first().copied().unwrap_or(0)) % (editor.text().len() + 1),
                op % 9 == 4,
            ),
            5 => {
                editor.undo();
            }
            6 => {
                editor.redo();
            }
            7 => editor.set_text(&text, step as u64),
            _ => editor.paste(&text, step as u64),
        }
        menu.refresh(&editor);
        let mut full = catalog.clone();
        full.refresh(&editor);
        assert_eq!(
            menu.items().collect::<Vec<_>>(),
            full.items().collect::<Vec<_>>(),
            "incremental candidates must match a fresh parse"
        );
        let mut actual = editor.clone();
        let mut expected = editor.clone();
        assert_eq!(
            menu.clone().accept(&mut actual, 0),
            full.accept(&mut expected, 0)
        );
        assert_eq!(
            (actual.text(), actual.cursor(), actual.anchor()),
            (expected.text(), expected.cursor(), expected.anchor()),
        );
    }
}
