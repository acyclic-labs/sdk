use acyclic_harness_tui::key_event::{
    InputEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use acyclic_harness_tui::ratatui::layout::Rect;
use acyclic_harness_tui::{
    Editor, InputKeymap, PromptInput, Viewport, editor_command, format_history, parse_history,
    prepare_image,
};
use unicode_segmentation::UnicodeSegmentation;

pub fn exercise(data: &[u8]) {
    let json = String::from_utf8_lossy(data);
    if let Ok(map) = InputKeymap::from_json(&json) {
        let encoded = map.to_json().expect("validated keymap serializes");
        assert_eq!(InputKeymap::from_json(&encoded).ok(), Some(map));
    }
    if let Ok(history) = parse_history(&json) {
        let encoded = format_history(&history).expect("validated history serializes");
        assert_eq!(
            parse_history(&encoded)
                .and_then(|restored| format_history(&restored))
                .ok(),
            Some(encoded)
        );
    }
    let _ = editor_command(&json);
    let _ = prepare_image(
        u32::from(data.first().copied().unwrap_or(0)),
        1,
        data.to_vec(),
    );
    let mut input = PromptInput::new(Editor::default());
    let mut viewport = Viewport::default();
    let mut at = 0u64;
    for chunk in data.chunks(8) {
        let op = chunk[0];
        at += u64::from(chunk.get(7).copied().unwrap_or(0));
        let text = String::from_utf8_lossy(&chunk[1..]);
        let area = Rect::new(0, 0, 1 + u16::from(op % 16), 3);
        match op % 11 {
            0 => {
                input.event(
                    InputEvent::CommittedText(text.into_owned()),
                    area,
                    &mut viewport,
                    at,
                );
            }
            1 => {
                input.paste(&text.repeat(160), at);
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
                    _ => KeyCode::Char(char::from(op)),
                };
                let mods = KeyModifiers::from_bits_truncate(chunk.get(2).copied().unwrap_or(0));
                let mut key = KeyEvent::new(code, mods);
                key.kind = match chunk.get(3).copied().unwrap_or(0) % 3 {
                    0 => KeyEventKind::Press,
                    1 => KeyEventKind::Repeat,
                    _ => KeyEventKind::Release,
                };
                input.event(InputEvent::Key(key), area, &mut viewport, at);
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
                input.event(InputEvent::Mouse(mouse), area, &mut viewport, at);
            }
            8 => {
                input.event(
                    InputEvent::Paste(text.into_owned()),
                    area,
                    &mut viewport,
                    at,
                );
            }
            9 => {
                input.event(InputEvent::Focus(op > 128), area, &mut viewport, at);
            }
            _ => {
                input.event(
                    InputEvent::Resize(area.width, area.height),
                    area,
                    &mut viewport,
                    at,
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
