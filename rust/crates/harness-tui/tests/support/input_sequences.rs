use acyclic_harness_tui::key_event::{
    InputEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use acyclic_harness_tui::ratatui::layout::Rect;
use acyclic_harness_tui::{
    Editor, InputEffect, InputKeymap, PromptInput, Viewport, editor_command, format_history,
    parse_history, prepare_image, windows_editor_command,
};
use unicode_segmentation::UnicodeSegmentation;

pub fn exercise(data: &[u8]) {
    exercise_variant(data, false);
}

#[cfg(test)]
pub fn exercise_broken(data: &[u8]) {
    exercise_variant(data, true);
}

fn exercise_variant(data: &[u8], broken: bool) {
    let json = String::from_utf8_lossy(data);
    let mut parsed_map = None;
    if let Ok(map) = InputKeymap::parse(&json) {
        let encoded = map.encode();
        assert_eq!(InputKeymap::parse(&encoded).ok(), Some(map.clone()));
        parsed_map = Some(map);
    }
    let mut parsed_history = None;
    if let Ok(history) = parse_history(&json) {
        let encoded = format_history(&history).expect("validated history serializes");
        assert_eq!(
            parse_history(&encoded)
                .and_then(|restored| format_history(&restored))
                .ok(),
            Some(encoded)
        );
        parsed_history = Some(history);
    }
    let _ = editor_command(&json);
    if let Ok((program, args)) = windows_editor_command(&json) {
        let words: Vec<_> = std::iter::once(program)
            .chain(args)
            .map(|word| word.to_string_lossy().into_owned())
            .collect();
        let encoded = windows_command_line(&words);
        let (program, args) =
            windows_editor_command(&encoded).expect("encoded Windows arguments parse");
        assert_eq!(
            std::iter::once(program)
                .chain(args)
                .map(|word| word.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            words
        );
    }
    let _ = prepare_image(
        u32::from(data.first().copied().unwrap_or(0)),
        1,
        data.to_vec(),
    );
    let (mut input, mut viewport, mut at) = semantic_prelude(data, broken);
    if let Some(map) = parsed_map {
        input.keymap = map;
    }
    if let Some(history) = parsed_history {
        input.replace_history(history);
    }
    for chunk in data.chunks(8) {
        let op = chunk[0];
        at += u64::from(chunk.get(7).copied().unwrap_or(0));
        let text = String::from_utf8_lossy(&chunk[1..]);
        let area = Rect::new(0, 0, 1 + u16::from(op % 16), 3);
        match op % 11 {
            0 => {
                dispatch(
                    &mut input,
                    InputEvent::CommittedText(text.into_owned()),
                    area,
                    &mut viewport,
                    at,
                    broken,
                );
            }
            1 => {
                dispatch(
                    &mut input,
                    InputEvent::Paste(text.repeat(160)),
                    area,
                    &mut viewport,
                    at,
                    broken,
                );
            }
            2..=5 => {
                let code = match chunk.get(1).copied().unwrap_or(0) % 20 {
                    0 => KeyCode::Char('c'),
                    1 => KeyCode::Char('x'),
                    2 => KeyCode::Char('e'),
                    3 => KeyCode::Char('r'),
                    4 => KeyCode::Char('z'),
                    5 => KeyCode::Char('y'),
                    6 => KeyCode::Left,
                    7 => KeyCode::Right,
                    8 => KeyCode::Up,
                    9 => KeyCode::Down,
                    10 => KeyCode::Enter,
                    11 => KeyCode::Backspace,
                    12 => KeyCode::Delete,
                    13 => KeyCode::Tab,
                    14 => KeyCode::BackTab,
                    15 => KeyCode::Esc,
                    16 => KeyCode::Home,
                    17 => KeyCode::End,
                    _ => KeyCode::Char(char::from(chunk.get(4).copied().unwrap_or(op))),
                };
                let mods = KeyModifiers::from_bits_truncate(chunk.get(2).copied().unwrap_or(0));
                let mut key = KeyEvent::new(code, mods);
                key.kind = match chunk.get(3).copied().unwrap_or(0) % 3 {
                    0 => KeyEventKind::Press,
                    1 => KeyEventKind::Repeat,
                    _ => KeyEventKind::Release,
                };
                dispatch(
                    &mut input,
                    InputEvent::Key(key),
                    area,
                    &mut viewport,
                    at,
                    broken,
                );
            }
            6 => {
                input.tick(at + 1000);
            }
            7 => {
                let mouse = MouseEvent {
                    kind: if op > 128 {
                        MouseEventKind::Drag(MouseButton::Left)
                    } else {
                        MouseEventKind::Down(MouseButton::Left)
                    },
                    column: u16::from(op % 20),
                    row: u16::from(op % 6),
                    modifiers: KeyModifiers::NONE,
                };
                dispatch(
                    &mut input,
                    InputEvent::Mouse(mouse),
                    area,
                    &mut viewport,
                    at,
                    broken,
                );
            }
            8 => {
                dispatch(
                    &mut input,
                    InputEvent::Paste(text.into_owned()),
                    area,
                    &mut viewport,
                    at,
                    broken,
                );
            }
            9 => {
                dispatch(
                    &mut input,
                    InputEvent::Focus(op > 128),
                    area,
                    &mut viewport,
                    at,
                    broken,
                );
            }
            _ => {
                dispatch(
                    &mut input,
                    InputEvent::Resize(area.width, area.height),
                    area,
                    &mut viewport,
                    at,
                    broken,
                );
            }
        }
        invariants(&input, usize::from(area.width));
    }
    // A final pending chord must execute, even if the fuzzer supplied
    // no explicit timer action. Keep the live sequence intact until this flush.
    input.tick(at + 1000);
    invariants(&input, 16);
}

fn dispatch(
    input: &mut PromptInput,
    event: InputEvent,
    area: Rect,
    viewport: &mut Viewport,
    at: u64,
    broken: bool,
) -> Vec<InputEffect> {
    if broken
        && matches!(&event, InputEvent::CommittedText(_))
        && !input.editor.selection().is_empty()
    {
        // Negative control: append instead of replacing the selection. Cursor
        // bounds and grapheme structure remain valid, so semantics must catch it.
        input.editor.set_cursor(input.editor.text().len(), false);
    }
    input.event(event, area, viewport, at)
}

fn semantic_prelude(data: &[u8], broken: bool) -> (PromptInput, Viewport, u64) {
    let mut input = PromptInput::new(Editor::default());
    input.keymap = InputKeymap::parse(r#"{"bindings":{"Super+k":"cursor.line-start"}}"#)
        .expect("valid keymap fixture");
    input.replace_history(
        parse_history(r#"["older\r\nprompt","newer\rhistory"]"#).expect("valid history fixture"),
    );
    let mut viewport = Viewport::default();
    let mut at = 0;
    let mut send = |input: &mut PromptInput, event| {
        let effects = dispatch(
            input,
            event,
            Rect::new(0, 0, 80, 4),
            &mut viewport,
            at,
            broken,
        );
        at += 1;
        effects
    };
    let text = format!("draft:{}", String::from_utf8_lossy(data));
    send(&mut input, InputEvent::CommittedText(text.clone()));
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    assert_prompt(
        &input,
        &normalized,
        normalized.len(),
        "committed text is inserted",
    );
    send(&mut input, key(KeyCode::Char('a'), KeyModifiers::CONTROL));
    send(&mut input, InputEvent::CommittedText("replacement".into()));
    assert_prompt(
        &input,
        "replacement",
        11,
        "committed text replaces the selection",
    );
    send(&mut input, key(KeyCode::Home, KeyModifiers::NONE));
    send(&mut input, key(KeyCode::Up, KeyModifiers::NONE));
    assert_prompt(
        &input,
        "newer\nhistory",
        0,
        "Up recalls populated file history",
    );
    send(&mut input, key(KeyCode::Char('z'), KeyModifiers::CONTROL));
    assert_prompt(&input, "replacement", 0, "undo restores the recalled draft");
    send(
        &mut input,
        key(
            KeyCode::Char('z'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        ),
    );
    assert_prompt(
        &input,
        "newer\nhistory",
        0,
        "redo restores the recalled cursor",
    );
    send(&mut input, key(KeyCode::End, KeyModifiers::NONE));
    send(&mut input, key(KeyCode::Char('k'), KeyModifiers::SUPER));
    assert_prompt(
        &input,
        "newer\nhistory",
        0,
        "parsed Super override moves instead of killing",
    );
    send(&mut input, key(KeyCode::End, KeyModifiers::CONTROL));
    send(&mut input, key(KeyCode::Char('r'), KeyModifiers::CONTROL));
    send(&mut input, InputEvent::CommittedText("older".into()));
    send(&mut input, key(KeyCode::Esc, KeyModifiers::NONE));
    send(&mut input, key(KeyCode::Down, KeyModifiers::NONE));
    assert_prompt(
        &input,
        "replacement",
        11,
        "search cancellation preserves Down-to-draft",
    );
    input.edit_message("message".into(), "edited prompt");
    assert_eq!(
        send(&mut input, key(KeyCode::Enter, KeyModifiers::NONE)),
        vec![InputEffect::Submit {
            text: "edited prompt".into(),
            queued: false,
            edit: Some("message".into())
        }]
    );
    assert_eq!(
        input.history().entries(),
        &["older\nprompt", "newer\nhistory", "edited prompt"],
        "edited submissions remain in the shared history file projection"
    );
    assert_prompt(
        &input,
        "replacement",
        11,
        "editing preserves the live draft",
    );
    (input, viewport, at)
}

fn key(code: KeyCode, modifiers: KeyModifiers) -> InputEvent {
    InputEvent::Key(KeyEvent::new(code, modifiers))
}

fn assert_prompt(input: &PromptInput, text: &str, cursor: usize, reason: &str) {
    assert_eq!(input.editor.text(), text, "{reason}");
    assert_eq!(
        (input.editor.cursor(), input.editor.anchor()),
        (cursor, cursor),
        "{reason}"
    );
}

/// Independent CRT argument encoder used by both properties and fuzz assertions.
pub fn windows_command_line(words: &[String]) -> String {
    words
        .iter()
        .map(|word| {
            let mut encoded = String::from("\"");
            let mut slashes = 0;
            for character in word.chars() {
                if character == '\\' {
                    slashes += 1;
                    continue;
                }
                encoded.push_str(&"\\".repeat(slashes * if character == '"' { 2 } else { 1 }));
                if character == '"' {
                    encoded.push('\\');
                }
                encoded.push(character);
                slashes = 0;
            }
            encoded.push_str(&"\\".repeat(slashes * 2));
            encoded.push('"');
            encoded
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn invariants(input: &PromptInput, width: usize) {
    assert!(!input.editor.text().contains('\r'));
    let points: Vec<_> = input
        .editor
        .text()
        .grapheme_indices(true)
        .map(|(i, _)| i)
        .chain([input.editor.text().len()])
        .collect();
    assert!(points.contains(&input.editor.cursor()));
    assert!(points.contains(&input.editor.anchor()));
    let mut end = 0;
    for range in input.editor.collapsed_pastes() {
        assert!(end <= range.start && range.start < range.end);
        assert!(points.contains(&range.start) && points.contains(&range.end));
        assert!(!(range.start < input.editor.cursor() && input.editor.cursor() < range.end));
        assert!(!(range.start < input.editor.anchor() && input.editor.anchor() < range.end));
        end = range.end;
    }
    let layout = input.editor.layout(width);
    assert!(layout.position(input.editor.cursor()).0 < layout.rows.len());
}
