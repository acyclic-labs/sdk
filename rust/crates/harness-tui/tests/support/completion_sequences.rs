use acyclic_harness_tui::key_event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use acyclic_harness_tui::{
    Completion, CompletionMenu, CompletionProvider, CompletionRequest, Editor, InputEffect,
    InputKeymap, PromptInput, completion_request, parse_history,
};
use unicode_segmentation::UnicodeSegmentation;

struct Candidate(String);

impl CompletionProvider for Candidate {
    fn complete(&self, _: &CompletionRequest) -> Vec<Completion> {
        vec![Completion {
            label: self.0.clone(),
            value: self.0.clone(),
            description: self.0.clone(),
            tool: false,
            append_space: true,
        }]
    }
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
    let mut editor = semantic_prelude(data, broken)?;
    let mut menu = CompletionMenu::default();
    for (step, chunk) in data.chunks(8).enumerate() {
        let op = chunk[0];
        let text = String::from_utf8_lossy(&chunk[1..]);
        match op % 12 {
            0 => editor.insert(&text, step as u64),
            1 => editor.paste(&text.repeat(150), step as u64),
            2 => editor.set_cursor(usize::from(chunk.get(1).copied().unwrap_or(0)), false),
            3 => editor.select_all(),
            4 => menu.refresh(&editor, &[&Candidate(text.into_owned())]),
            5 => menu.cycle(op > 128),
            6 => {
                let before = (editor.text().to_owned(), editor.cursor(), editor.anchor());
                if menu.accept(&mut editor, step as u64) {
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
            7 => menu.dismiss(&editor),
            8 => {
                let code = [
                    KeyCode::Tab,
                    KeyCode::Enter,
                    KeyCode::Up,
                    KeyCode::Down,
                    KeyCode::Esc,
                ][usize::from(chunk.get(1).copied().unwrap_or(0)) % 5];
                let mut key = KeyEvent::new(
                    code,
                    KeyModifiers::from_bits_truncate(chunk.get(2).copied().unwrap_or(0)),
                );
                key.kind = [
                    KeyEventKind::Press,
                    KeyEventKind::Repeat,
                    KeyEventKind::Release,
                ][usize::from(chunk.get(3).copied().unwrap_or(0)) % 3];
                menu.key(&mut editor, key, &InputKeymap::default(), step as u64);
            }
            9 => {
                editor.undo();
            }
            10 => {
                editor.redo();
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
        let points: Vec<_> = editor
            .text()
            .grapheme_indices(true)
            .map(|(at, _)| at)
            .chain([editor.text().len()])
            .collect();
        assert!(points.contains(&editor.cursor()) && points.contains(&editor.anchor()));
        assert!(!editor.text().contains('\r'));
        if let Some(request) = completion_request(&editor) {
            assert!(points.contains(&request.range.start) && points.contains(&request.range.end));
            assert!(request.range.start <= editor.cursor() && editor.cursor() <= request.range.end);
        }
    }
    Ok(())
}

fn semantic_prelude(data: &[u8], broken: bool) -> Result<Editor, std::io::Error> {
    let mut input = PromptInput::default();
    input.keymap = InputKeymap::from_json(r#"{"Ctrl+j":"Enter"}"#)?;
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
    input.refresh_completions(&[&Candidate(path)]);
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
