//! Each row is executable scenario data for the composed shared prompt.
use acyclic_harness::tui::{
    Editor, InputEffect, PromptInput, Screen, Viewport,
    key_event::{InputEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    ratatui::{Terminal, backend::TestBackend},
};

#[test]
fn declarative_prompt_parity_rows_render_through_test_backend() {
    let rows: Vec<serde_json::Value> = serde_json::from_str(include_str!("parity.json")).unwrap();
    for row in rows {
        let name = row["row"].as_str().unwrap();
        let mut input = PromptInput::new(Editor::new(row["text"].as_str().unwrap()));
        input.question = row["question"].as_bool().unwrap_or(false);
        input.busy = row["busy"].as_bool().unwrap_or(false);
        input.recording = row["recording"].as_bool().unwrap_or(false);
        input.images = row["images"].as_u64().unwrap_or(0) as usize;
        if let Some(text) = row["edit"].as_str() {
            assert!(input.edit_message("earlier".into(), text).is_empty());
        }
        input.transcript_selection = row["transcript_selection"].as_str().map(str::to_owned);
        if let Some(entries) = row["history"].as_array() {
            let mut history = acyclic_harness::tui::PromptHistory::default();
            for entry in entries {
                history.push(entry.as_str().unwrap().into());
            }
            input.replace_history(history);
        }
        if let Some(files) = row["files"].as_object() {
            for (path, text) in files {
                input
                    .completion
                    .set_file(path, Some(text.as_str().unwrap().as_bytes()))
                    .unwrap();
            }
        }
        input.refresh_completions();
        let mut effects = Vec::new();
        for (index, event) in row["events"].as_array().unwrap().iter().enumerate() {
            let now = event["at"].as_u64().unwrap_or((index as u64 + 1) * 1000);
            if let Some(focused) = event["focus"].as_bool() {
                effects.extend(input.event(
                    InputEvent::Focus(focused),
                    acyclic_harness::tui::ratatui::layout::Rect::new(1, 1, 38, 8),
                    &mut Viewport::default(),
                    now,
                ));
            } else if event.get("resize").is_some() {
                effects.extend(input.event(
                    InputEvent::Resize(40, 20),
                    acyclic_harness::tui::ratatui::layout::Rect::new(1, 1, 38, 8),
                    &mut Viewport::default(),
                    now,
                ));
            } else if let Some(text) = event["text"].as_str() {
                effects.extend(input.committed_text(text, now));
            } else if let Some(text) = event["paste"].as_str() {
                effects.extend(input.paste(text, now));
            } else {
                let code = match event["key"].as_str().unwrap() {
                    "Enter" => KeyCode::Enter,
                    "Home" => KeyCode::Home,
                    "End" => KeyCode::End,
                    "Left" => KeyCode::Left,
                    "Right" => KeyCode::Right,
                    "Backspace" => KeyCode::Backspace,
                    "Delete" => KeyCode::Delete,
                    "Tab" => KeyCode::Tab,
                    "Up" => KeyCode::Up,
                    "Down" => KeyCode::Down,
                    "Escape" => KeyCode::Esc,
                    character => KeyCode::Char(character.chars().next().unwrap()),
                };
                let mut modifiers = KeyModifiers::NONE;
                for (field, modifier) in [
                    ("ctrl", KeyModifiers::CONTROL),
                    ("shift", KeyModifiers::SHIFT),
                    ("alt", KeyModifiers::ALT),
                ] {
                    if event[field].as_bool() == Some(true) {
                        modifiers |= modifier;
                    }
                }
                let mut key = KeyEvent::new(code, modifiers);
                if event["repeat"].as_bool() == Some(true) {
                    key.kind = KeyEventKind::Repeat;
                }
                if event["release"].as_bool() == Some(true) {
                    key.kind = KeyEventKind::Release;
                }
                effects.extend(input.key(key, now, 38));
            }
            if event["folded"].as_bool() == Some(true) {
                assert!(!input.editor.collapsed_pastes().is_empty(), "{name}");
                let mut terminal = Terminal::new(TestBackend::new(40, 20)).unwrap();
                terminal
                    .draw(|frame| {
                        Screen::default().render(frame, &input, &mut Viewport::default(), 0, now);
                    })
                    .unwrap();
                assert!(super::screen_text(&terminal).contains("Pasted"), "{name}");
            }
        }
        if let Some(submitted) = row["submitted"].as_str() {
            assert!(
                effects.iter().any(|effect| matches!(effect,
                    InputEffect::Submit { text, queued, edit }
                    if text == submitted
                        && *queued == row["queued"].as_bool().unwrap_or(false)
                        && edit.as_deref() == row["edit_id"].as_str()
                )),
                "{name}"
            );
        }
        if row["interrupt"].as_bool() == Some(true) {
            assert_eq!(effects, vec![InputEffect::Interrupt], "{name}");
        }
        if let Some(copied) = row["copied"].as_str() {
            assert!(
                effects
                    .iter()
                    .any(|effect| matches!(effect, InputEffect::Copy(text) if text == copied)),
                "{name}"
            );
        }
        if row["quit"].as_bool() == Some(true) {
            assert_eq!(effects, vec![InputEffect::Quit], "{name}");
        }
        if let Some(comment) = row["comment"].as_bool() {
            assert_eq!(input.comment_mode, comment, "{name}");
        }
        if row["clipboard_paste"].as_bool() == Some(true) {
            assert_eq!(effects, vec![InputEffect::Paste], "{name}");
        }
        assert_eq!(
            input.editor.text(),
            row["expected"].as_str().unwrap(),
            "{name}"
        );
        if let Some(active) = row["completion"].as_bool() {
            assert_eq!(input.completion.active(&input.editor), active, "{name}");
        }
        assert_eq!(
            effects.len(),
            row["effects"].as_u64().unwrap_or(0) as usize,
            "{name}"
        );
        for width in [40, 80] {
            let mut terminal = Terminal::new(TestBackend::new(width, 20)).unwrap();
            terminal
                .draw(|frame| {
                    Screen::default().render(frame, &input, &mut Viewport::default(), 0, 10_000);
                })
                .unwrap();
            let picture = super::screen_text(&terminal);
            for expected in row["visible"].as_array().unwrap() {
                assert!(
                    picture.contains(expected.as_str().unwrap()),
                    "{name}: {picture}"
                );
            }
        }
    }
}
