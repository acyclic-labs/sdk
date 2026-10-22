use acyclic_harness_tui::key_event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use acyclic_harness_tui::{
    CompletionMenu, Editor, InputEffect, InputKeymap, PromptInput, completion_request,
    parse_history,
};
use unicode_segmentation::UnicodeSegmentation;

fn candidate(menu: &mut CompletionMenu, path: &str) -> Result<(), std::io::Error> {
    menu.clear_files();
    menu.set_file(&format!("workspace/{path}"), Some(b""))
}

pub fn exercise(data: &[u8]) -> Result<(), std::io::Error> {
    exercise_with(data, false)
}

/// Deliberately reproduce the old dropped-@ bug for the shared semantic oracle.
#[cfg(test)]
pub fn exercise_broken(data: &[u8]) -> Result<(), std::io::Error> {
    exercise_with(data, true)
}

fn exercise_with(data: &[u8], broken: bool) -> Result<(), std::io::Error> {
    let suffix: String = data
        .iter()
        .take(24)
        .map(|byte| char::from(b'a' + byte % 26))
        .collect();
    let theme: String = String::from_utf8_lossy(data)
        .chars()
        .filter(|c| !c.is_control())
        .take(48)
        .collect();
    catalog_round_trip(
        &format!("cmd{suffix}"),
        &format!("alias{suffix}"),
        &format!("É{theme} X"),
    )?;
    // Exercise malformed theme inventories too; replacement must be atomic.
    let mut probe = CompletionMenu::default();
    probe.set_file(
        "commands/base.json",
        Some(br#"[{"name":"theme","action":{"kind":"theme"}}]"#),
    )?;
    probe.set_file("commands/themes.json", Some(b"[\"Theme\"]"))?;
    probe.refresh(&Editor::new("/theme "));
    let before: Vec<_> = probe.items().cloned().collect();
    if probe.set_file("commands/catalog.json", Some(data)).is_err() {
        assert_eq!(probe.items().cloned().collect::<Vec<_>>(), before);
    }
    if probe.set_file("commands/themes.json", Some(data)).is_err() {
        assert_eq!(
            probe.items().next().map(|item| item.value.as_str()),
            Some("Theme")
        );
    }
    let mut editor = semantic_prelude(data, broken)?;
    let mut menu = CompletionMenu::default();
    for (step, chunk) in data.chunks(8).enumerate() {
        let Some((&op, rest)) = chunk.split_first() else {
            continue;
        };
        let text = String::from_utf8_lossy(rest);
        match op % 14 {
            0 => editor.insert(&text, step as u64),
            1 => editor.paste(&text.repeat(150), step as u64),
            2 => editor.set_cursor(usize::from(chunk.get(1).copied().unwrap_or(0)), false),
            3 => editor.select_all(),
            4 => {
                candidate(&mut menu, &text)?;
                menu.refresh(&editor);
            }
            5 => menu.cycle(op > 128),
            6 => accept_undoably(&mut menu, &mut editor, step as u64),
            7 => menu.dismiss(&editor),
            8 => {
                menu.key(
                    &mut editor,
                    key(chunk),
                    &InputKeymap::default(),
                    step as u64,
                );
            }
            9 => {
                editor.undo();
            }
            10 => {
                editor.redo();
            }
            12 => {
                let before: Vec<_> = menu.items().cloned().collect();
                if menu
                    .set_file("commands/fuzz.json", Some(text.as_bytes()))
                    .is_err()
                {
                    assert_eq!(menu.items().cloned().collect::<Vec<_>>(), before);
                }
                menu.refresh(&editor);
            }
            13 => {
                let commands = serde_json::json!([{"name": "theme", "description": text.as_ref()}]);
                menu.set_file("commands/fuzz.json", Some(&serde_json::to_vec(&commands)?))?;
                menu.refresh(&editor);
            }
            _ => {
                let start = usize::from(chunk.get(1).copied().unwrap_or(0));
                let end = usize::from(chunk.get(2).copied().unwrap_or(0));
                let before = (editor.text().to_owned(), editor.cursor(), editor.anchor());
                if !editor.replace_range(start..end, &text, step as u64) {
                    assert_eq!(
                        (editor.text(), editor.cursor(), editor.anchor()),
                        (before.0.as_str(), before.1, before.2)
                    );
                }
            }
        }
        assert_on_graphemes(&editor);
    }
    Ok(())
}

/// Accepting a completion is one undo step that redo restores exactly.
fn accept_undoably(menu: &mut CompletionMenu, editor: &mut Editor, step: u64) {
    let before = (editor.text().to_owned(), editor.cursor(), editor.anchor());
    if menu.accept(editor, step) {
        let after = (editor.text().to_owned(), editor.cursor(), editor.anchor());
        let mut probe = editor.clone();
        assert!(probe.undo());
        assert_eq!(
            (probe.text(), probe.cursor(), probe.anchor()),
            (before.0.as_str(), before.1, before.2)
        );
        assert!(probe.redo());
        assert_eq!(
            (probe.text(), probe.cursor(), probe.anchor()),
            (after.0.as_str(), after.1, after.2)
        );
    }
}

/// A completion-menu key chosen by the chunk's code, modifier and kind bytes.
fn key(chunk: &[u8]) -> KeyEvent {
    let byte = |index: usize| chunk.get(index).copied().unwrap_or(0);
    let code = match byte(1) % 5 {
        0 => KeyCode::Tab,
        1 => KeyCode::Enter,
        2 => KeyCode::Up,
        3 => KeyCode::Down,
        _ => KeyCode::Esc,
    };
    let mut key = KeyEvent::new(code, KeyModifiers::from_bits_truncate(byte(2)));
    key.kind = match byte(3) % 3 {
        0 => KeyEventKind::Press,
        1 => KeyEventKind::Repeat,
        _ => KeyEventKind::Release,
    };
    key
}

/// The cursor, anchor and any completion range sit on grapheme boundaries.
fn assert_on_graphemes(editor: &Editor) {
    let points: Vec<_> = editor
        .text()
        .grapheme_indices(true)
        .map(|(at, _)| at)
        .chain([editor.text().len()])
        .collect();
    assert!(points.contains(&editor.cursor()) && points.contains(&editor.anchor()));
    assert!(!editor.text().contains('\r'));
    if let Some(request) = completion_request(editor) {
        assert!(points.contains(&request.range.start) && points.contains(&request.range.end));
        assert!(request.range.start <= editor.cursor() && editor.cursor() <= request.range.end);
    }
}

/// Production JSON file parsing preserves aliases, tool flags and theme spelling.
/// Shared by shrinking properties and the instrumented completion fuzz target.
pub fn catalog_round_trip(name: &str, alias: &str, theme: &str) -> Result<(), std::io::Error> {
    let mut menu = CompletionMenu::default();
    let definition = serde_json::json!([{"name":name,"aliases":[alias],"action":{"kind":"tool","tool":"shell"}}, {"name":"palette","aliases":["t"],"action":{"kind":"theme"}}]);
    menu.set_file(
        "commands/catalog.json",
        Some(&serde_json::to_vec(&definition)?),
    )?;
    let mut editor = Editor::new(&format!("/{}", alias.to_ascii_uppercase()));
    menu.refresh(&editor);
    assert_eq!(menu.items().filter(|item| item.value == alias).count(), 1);
    let selected = menu.items().position(|item| item.value == alias);
    menu.selected = selected.unwrap_or(usize::MAX);
    assert!(
        menu.items()
            .nth(menu.selected)
            .is_some_and(|item| item.tool)
    );
    assert!(menu.accept(&mut editor, 0));
    assert_eq!(editor.text(), format!("/{alias} "));
    menu.set_file("commands/themes.json", Some(&serde_json::to_vec(&[theme])?))?;
    editor.set_text("/T ", 1);
    menu.refresh(&editor);
    let initial = theme.to_lowercase();
    editor.insert(&initial, 2);
    menu.refresh(&editor);
    assert_eq!(menu.items().len(), 1);
    assert!(menu.accept(&mut editor, 2));
    assert_eq!(editor.text(), format!("/T {theme}"));
    assert!(editor.undo());
    assert_eq!(editor.text(), format!("/T {initial}"));
    Ok(())
}

fn semantic_prelude(data: &[u8], broken: bool) -> Result<Editor, std::io::Error> {
    // The same file parser used by production must preserve command spelling
    // and metadata through JSON encoding, independently of current prompt state.
    let mut commands = CompletionMenu::default();
    let description = String::from_utf8_lossy(data);
    let definition = serde_json::json!([{"name":"theme", "description":description.as_ref()}]);
    commands.set_file(
        "commands/round-trip.json",
        Some(&serde_json::to_vec(&definition)?),
    )?;
    commands.refresh(&Editor::new("/th"));
    let items: Vec<_> = commands
        .items()
        .map(|item| (item.value.as_str(), item.description.as_str()))
        .collect();
    assert_eq!(items, [("theme", description.as_ref())]);
    let mut input = PromptInput::default();
    input.keymap = InputKeymap::parse(r#"{"bindings":{"Ctrl+j":"submit"}}"#)
        .map_err(|errors| std::io::Error::other(format!("{errors:?}")))?;
    let history = parse_history(r#"["older\rprompt","newer\r\nhistory"]"#)?;
    input.replace_history(history);
    input.committed_text("@dr", 0);
    let suffix: String = String::from_utf8_lossy(data)
        .chars()
        .filter(|c| !c.is_control() && *c != '"')
        .take(24)
        .collect();
    let path = format!("dir/{suffix}.txt");
    let expected = if path.chars().any(char::is_whitespace) {
        format!("@\"{path}\" ")
    } else {
        format!("@{path} ")
    };
    candidate(&mut input.completion, &path)?;
    input.refresh_completions();
    let effects = input.key(
        KeyEvent::new(KeyCode::Char('j'), KeyModifiers::CONTROL),
        1,
        80,
    );
    assert!(
        effects.is_empty(),
        "acceptance must consume Enter without submission"
    );
    if broken {
        // This edit preserves every structural invariant but violates acceptance.
        input
            .editor
            .set_text(expected.strip_prefix('@').unwrap_or(&expected), 2);
    }
    assert_eq!(
        input.editor.text(),
        expected,
        "completion preserves the mention marker"
    );
    let after = (input.editor.cursor(), input.editor.anchor());
    let mut probe = input.editor.clone();
    assert!(probe.undo());
    assert_eq!(
        (probe.text(), probe.cursor(), probe.anchor()),
        ("@dr", 3, 3)
    );
    assert!(probe.redo());
    assert_eq!(
        (probe.text(), probe.cursor(), probe.anchor()),
        (expected.as_str(), after.0, after.1)
    );
    assert_eq!(
        input.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), 3, 80),
        vec![InputEffect::Submit {
            text: expected,
            queued: false,
            edit: None
        }]
    );
    // The same live editor retains populated, normalized history during fuzzing.
    input.replace_history(parse_history(r#"["newest\r\nhistory"]"#)?);
    assert!(
        input
            .key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), 4, 80)
            .is_empty()
    );
    assert_eq!(
        (input.editor.text(), input.editor.cursor()),
        ("newest\nhistory", 0)
    );
    input.editor.set_text("", 5);
    Ok(input.editor)
}
