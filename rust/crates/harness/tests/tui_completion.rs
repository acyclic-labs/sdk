//! Real cursor-follow files and terminal decoding, without a second input decoder.
#![cfg(all(feature = "tui", unix))]
use acyclic_harness::{
    task_volume::{FileKey, Query, TaskVolume, Write},
    tui::{Editor, InputEffect, PromptInput},
    tui_files::CompletionFiles,
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::{
    io::{self, Write as _},
    sync::Arc,
};

#[path = "support/input_path_sequences.rs"]
mod path_sequences;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn put(volume: &TaskVolume<MemoryStream>, path: &str, bytes: &[u8]) -> Result {
    let cut = volume.snapshot(&Query::Inputs, 100).await?.generation;
    volume
        .append(cut, &Write::new(FileKey::parse(path)?, bytes.to_vec()))
        .await?;
    Ok(())
}

async fn volume() -> Result<TaskVolume<MemoryStream>> {
    Ok(TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("completion/files")?,
    )
    .await?)
}

#[test]
fn files_snapshot_follow_and_resume() -> Result {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let volume = volume().await?;
            put(
                &volume,
                "commands/catalog.json",
                include_bytes!("../src/client/commands.json"),
            )
            .await?;
            // Builtin names and aliases are reserved, and completion matches
            // either by prefix: no builtin may complete `/thes`.
            let catalog: Vec<serde_json::Value> =
                serde_json::from_slice(include_bytes!("../src/client/commands.json"))?;
            let reserved = catalog.iter().flat_map(|command| {
                std::iter::once(&command["name"])
                    .chain(command["aliases"].as_array().into_iter().flatten())
            });
            for spelling in reserved {
                let spelling = spelling.as_str().ok_or("catalog spelling must be text")?;
                assert!(!spelling.starts_with("thes"), "builtin {spelling} completes /thes");
            }
            put(&volume, "commands/thesis.md", b"Draft a thesis").await?;
            put(&volume, "workspace/Straße.rs", b"source").await?;
            let mut input = PromptInput::new(Editor::new("/thes"));
            let mut files = CompletionFiles::observe(&volume, &mut input, None, 100).await?;
            assert_eq!(
                input
                    .completion
                    .items()
                    .map(|i| i.value.as_str())
                    .collect::<Vec<_>>(),
                ["thesis"]
            );
            input.editor.set_text("@STRASSE", 0);
            input.refresh_completions();
            assert!(input.completion.accept(&mut input.editor, 1));
            assert_eq!(input.editor.text(), "@Straße.rs ");
            input.editor.set_text("@new", 2);
            input.refresh_completions();
            assert!(!input.completion.active(&input.editor));
            put(&volume, "workspace/new.rs", b"new source").await?;
            files.next(&mut input).await?;
            assert!(input.completion.active(&input.editor));
            assert_eq!(input.completion.items().next().unwrap().value, "new.rs");
            let cursor = files.cursor.clone();
            let work = input.completion.work;
            let mut files =
                CompletionFiles::observe(&volume, &mut input, Some(cursor), 100).await?;
            assert_eq!(input.completion.work, work);
            put(&volume, "commands/thesis.md", b"Updated description").await?;
            files.next(&mut input).await?;
            input.editor.set_text("/thes", 5);
            input.refresh_completions();
            assert_eq!(
                input.completion.items().next().unwrap().description,
                "Updated description"
            );
            Ok(())
        })
}

#[test]
fn completion_through_a_real_pty() -> Result {
    let output = std::process::Command::new("python3")
        .args(["-c", include_str!("support/completion_pty.py")])
        .arg(std::env::current_exe()?)
        .output()?;
    assert!(
        output.status.success(),
        "PTY scenario: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
fn pty_child() -> Result {
    if std::env::var_os("GC_COMPLETION_PTY").is_none() {
        return Ok(());
    }
    use acyclic_harness::tui::crossterm::{
        event::{self, Event},
        terminal::{disable_raw_mode, enable_raw_mode},
    };
    use acyclic_harness::tui::key_event::KeyCode;
    struct Raw;
    impl Drop for Raw {
        fn drop(&mut self) {
            let _ = disable_raw_mode();
        }
    }
    enable_raw_mode()?;
    let _raw = Raw;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let volume = volume().await?;
            put(&volume, "commands/theme.md", b"Choose a theme").await?;
            let mut input = PromptInput::default();
            let mut files = CompletionFiles::observe(&volume, &mut input, None, 100).await?;
            checkpoint("READY")?;
            loop {
                if let Event::Key(key) = event::read()? {
                    let effects = input.key(key, 0, 80);
                    input.refresh_completions();
                    assert!(effects.is_empty(), "first Enter accepts without submission");
                    if key.code == KeyCode::Enter {
                        break;
                    }
                }
            }
            assert_eq!(input.editor.text(), "/theme ");
            checkpoint("ACCEPT")?;
            loop {
                if let Event::Key(key) = event::read()? {
                    let effects = input.key(key, 1, 80);
                    if key.code == KeyCode::Enter {
                        assert_eq!(
                            effects,
                            vec![InputEffect::Submit {
                                text: "/theme ".into(),
                                queued: false,
                                edit: None
                            }]
                        );
                        break;
                    }
                }
            }
            checkpoint("SUBMIT")?;
            input.editor.set_text("@new", 2);
            input.refresh_completions();
            assert!(!input.completion.active(&input.editor));
            put(&volume, "workspace/new.rs", b"created after snapshot").await?;
            files.next(&mut input).await?;
            assert!(input.completion.active(&input.editor));
            assert_eq!(input.completion.items().next().unwrap().value, "new.rs");
            // Render the real popup into cells, then put those cells on this PTY.
            // Terminal escape decoding remains crossterm's implementation.
            use acyclic_harness::tui::{
                CompletionPopup,
                ratatui::{buffer::Buffer, layout::Rect, widgets::Widget},
            };
            let mut frame = Buffer::empty(Rect::new(0, 0, 40, 3));
            CompletionPopup::new(&input.completion, &input.editor).render(frame.area, &mut frame);
            for row in frame.content.chunks(40) {
                let line: String = row.iter().map(|cell| cell.symbol()).collect();
                print!("\r\n{line}");
            }
            checkpoint("LIVE")?;
            loop {
                if let Event::Key(key) = event::read()? {
                    assert_eq!(key.code, KeyCode::Esc);
                    assert!(input.key(key, 3, 80).is_empty());
                    assert!(!input.completion.active(&input.editor));
                    assert_eq!(input.editor.text(), "@new");
                    break;
                }
            }
            checkpoint("ESCAPED")?;
            Ok(())
        })
}

fn checkpoint(message: &str) -> io::Result<()> {
    println!("\r\n{message}\r");
    io::stdout().flush()
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config { cases: 48, ..Default::default() })]
    #[test]
    fn source_paths_are_canonical_and_round_trip(path in ".{0,256}") {
        for prefix in ["commands/", "workspace/", "config/"] {
            let path = format!("{prefix}{path}");
            path_sequences::exercise(path.as_bytes());
            if let Ok(key) = FileKey::parse(&path) {
                proptest::prop_assert_eq!(key.path(), path.as_str());
                let encoded = serde_json::to_vec(&key).unwrap();
                proptest::prop_assert_eq!(serde_json::from_slice::<FileKey>(&encoded).unwrap(), key);
                proptest::prop_assert!(path.split('/').all(|part| !part.is_empty() && part != "." && part != ".."));
                proptest::prop_assert!(!path.chars().any(|c| c.is_control() || matches!(c, '\\' | ':')));
            }
        }
    }
}

#[test]
fn deserialization_cannot_bypass_source_path_validation() {
    for path in [
        "commands/../secret",
        "workspace/",
        "workspace/C:/secret",
        "workspace/a\\b",
        "workspace//x",
    ] {
        let encoded = serde_json::json!({"Input": path});
        assert!(serde_json::from_value::<FileKey>(encoded).is_err());
        assert!(FileKey::parse(path).is_err());
    }
}

#[test]
#[should_panic(expected = "accepted paths must already be canonical")]
fn path_fuzz_negative_control_detects_alias_acceptance() {
    path_sequences::broken_alias_acceptance();
}
