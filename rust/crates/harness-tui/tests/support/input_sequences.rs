use acyclic_harness_tui_core::key_event::{
    InputEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use acyclic_harness_tui::ratatui::layout::Rect;
use acyclic_harness_tui::{InputEffect, PromptInput, Viewport, editor_command, prepare_image, windows_editor_command};
use acyclic_harness_tui_core::{Editor, InputKeymap};
use unicode_segmentation::UnicodeSegmentation;

mod history_source {
    //! Reference source for generated input events. Production owns history in the session.
    use acyclic_harness_tui::{InputEffect, PromptInput};

    #[derive(Default)]
    pub struct Source {
        pub entries: Vec<String>,
        cursor: Option<usize>,
        draft: String,
        search_cursor: Option<usize>,
        search: Option<String>,
    }

    impl Source {
        pub fn new(entries: impl IntoIterator<Item = String>) -> Self {
            Self {
                entries: entries.into_iter().collect(),
                ..Self::default()
            }
        }

        pub fn apply(&mut self, input: &mut PromptInput, effects: &[InputEffect], at: u64) {
            for effect in effects {
                match effect {
                    InputEffect::Submit { text, .. } => self.entries.push(text.clone()),
                    InputEffect::History {
                        request,
                        previous,
                        search,
                        restart,
                    } => {
                        let expected = input.editor.text().to_owned();
                        let selected = if let Some(query) = search {
                            if *restart || self.search.as_deref() != Some(query) {
                                self.search_cursor = None;
                                self.search = Some(query.clone());
                            }
                            let end = self.search_cursor.unwrap_or(self.entries.len());
                            let found = self
                                .entries
                                .iter()
                                .take(end)
                                .enumerate()
                                .rev()
                                .find(|(_, text)| text.contains(query));
                            if let Some((index, _)) = found {
                                self.search_cursor = Some(index);
                            }
                            found.map(|(_, text)| text.clone())
                        } else if *previous {
                            if *restart || self.cursor.is_none() {
                                self.draft = expected.clone();
                                self.cursor = Some(self.entries.len());
                            }
                            let index = self.cursor.unwrap_or(0).saturating_sub(1);
                            self.cursor = Some(index);
                            self.entries.get(index).cloned()
                        } else if let Some(index) = self.cursor {
                            if index + 1 < self.entries.len() {
                                self.cursor = Some(index + 1);
                                self.entries.get(index + 1).cloned()
                            } else {
                                self.cursor = None;
                                Some(self.draft.clone())
                            }
                        } else {
                            None
                        };
                        assert!(input.resolve_history(
                            *request,
                            &expected,
                            selected.as_deref(),
                            *previous,
                            at
                        ));
                    }
                    InputEffect::HistorySearchFinished { .. } => {
                        self.search_cursor = None;
                        self.search = None;
                    }
                    _ => {}
                }
            }
        }
    }
}
use history_source::Source;

/// Exercise input transitions against the shared production semantic oracle.
///
/// # Panics
/// Panics if an input transition violates semantic or structural invariants.
pub fn exercise(data: &[u8]) {
    exercise_variant(data, false);
}

#[cfg(test)]
pub fn exercise_broken(data: &[u8]) {
    exercise_variant(data, true);
}

fn exercise_variant(data: &[u8], broken: bool) {
    let json = String::from_utf8_lossy(data);
    let (parsed_map, parsed_history) = round_trip_files(&json);
    let _ = prepare_image(
        u32::from(data.first().copied().unwrap_or(0)),
        1,
        data.to_vec(),
    );
    let (mut input, mut source, mut viewport, mut at) = semantic_prelude(data, broken);
    if let Some(map) = parsed_map {
        input.keymap = map;
    }
    if let Some(history) = parsed_history {
        source = Source::new(history);
    }
    for chunk in data.chunks(8) {
        let Some((&op, rest)) = chunk.split_first() else {
            continue;
        };
        at += u64::from(chunk.get(7).copied().unwrap_or(0));
        let text = String::from_utf8_lossy(rest);
        let area = Rect::new(0, 0, 1 + u16::from(op % 16), 3);
        let event = match op % 11 {
            0 => InputEvent::CommittedText(text.into_owned()),
            1 => InputEvent::Paste(text.repeat(160)),
            2..=5 => InputEvent::Key(chunk_key(chunk, op)),
            6 => {
                input.tick(at + 1000);
                invariants(&input, usize::from(area.width));
                continue;
            }
            7 => InputEvent::Mouse(MouseEvent {
                kind: if op > 128 {
                    MouseEventKind::Drag(MouseButton::Left)
                } else {
                    MouseEventKind::Down(MouseButton::Left)
                },
                column: u16::from(op % 20),
                row: u16::from(op % 6),
                modifiers: KeyModifiers::NONE,
            }),
            8 => InputEvent::Paste(text.into_owned()),
            9 => InputEvent::Focus(op > 128),
            _ => InputEvent::Resize(area.width, area.height),
        };
        dispatch(
            &mut input,
            &mut source,
            event,
            area,
            &mut viewport,
            at,
            broken,
        );
        invariants(&input, usize::from(area.width));
    }
    // A final pending chord must execute, even if the fuzzer supplied
    // no explicit timer action. Keep the live sequence intact until this flush.
    input.tick(at + 1000);
    invariants(&input, 16);
}

/// Keymap and Windows editor command files that parse must round-trip; a
/// prompt list becomes a history snapshot.
fn round_trip_files(json: &str) -> (Option<InputKeymap>, Option<Vec<String>>) {
    let mut parsed_map = None;
    if let Ok(map) = InputKeymap::parse(json) {
        let encoded = map.encode();
        assert_eq!(InputKeymap::parse(&encoded).ok(), Some(map.clone()));
        parsed_map = Some(map);
    }
    // A prompt list folds into a history snapshot, as a live query's does.
    let parsed_history = serde_json::from_str::<Vec<String>>(json).ok();
    let _ = editor_command(json);
    if let Ok((program, args)) = windows_editor_command(json) {
        let lossy = |(program, args): (std::ffi::OsString, Vec<std::ffi::OsString>)| {
            std::iter::once(program)
                .chain(args)
                .map(|word| word.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        };
        let words = lossy((program, args));
        let decoded = windows_editor_command(&windows_command_line(&words)).map(lossy);
        assert_eq!(decoded.ok(), Some(words), "encoded Windows arguments parse");
    }
    (parsed_map, parsed_history)
}

/// A prompt key chosen by the chunk's code, modifier and kind bytes.
fn chunk_key(chunk: &[u8], op: u8) -> KeyEvent {
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
    key
}

fn dispatch(
    input: &mut PromptInput,
    source: &mut Source,
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
    let effects: Vec<_> = input.event(event, area, viewport, at).collect();
    source.apply(input, &effects, at);
    effects
}

/// A prompt with a Super chord override and CR/CRLF-separated history.
fn fixture_input() -> PromptInput {
    let mut input = PromptInput::new(Editor::default());
    let keymap = InputKeymap::parse(r#"{"bindings":{"Super+k":"cursor.line-start"}}"#);
    assert!(keymap.is_ok(), "valid keymap fixture: {keymap:?}");
    input.keymap = keymap.unwrap_or_default();
    input
}

fn semantic_prelude(data: &[u8], broken: bool) -> (PromptInput, Source, Viewport, u64) {
    let mut input = fixture_input();
    let mut source = Source::new(["older\r\nprompt", "newer\rhistory"].map(String::from));
    let mut viewport = Viewport::default();
    let mut at = 0;
    let mut send = |input: &mut PromptInput, event| {
        let effects = dispatch(
            input,
            &mut source,
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
    send(&mut input, InputEvent::CommittedText("history".into()));
    assert_eq!(
        input.editor.text(),
        "newer\nhistory",
        "edited search revisits the newest match"
    );
    assert_eq!(
        input.search_status(),
        Some(("history", true)),
        "edited search retains its match"
    );
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
        source.entries,
        &["older\r\nprompt", "newer\rhistory", "edited prompt"],
        "edited submissions remain in the shared history file projection"
    );
    assert_prompt(
        &input,
        "replacement",
        11,
        "editing preserves the live draft",
    );
    (input, source, viewport, at)
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
/// Encode generated argument words using Windows command-line quoting rules.
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
