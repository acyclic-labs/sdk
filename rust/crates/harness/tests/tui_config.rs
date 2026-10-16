//! Real snapshots and cursor-follow updates drive the same mounted input.
#![cfg(feature = "tui")]
use acyclic_harness::{
    task_volume::{FileKey, Owner, Query, TaskVolume, Write},
    tui::{
        Action, AppAction, Editor, InputEffect, PromptInput,
        config::{ConfigDiagnostics, ConfigFile, DiagnosticKind, Scope, ThemeRole},
        key_event::{KeyCode, KeyEvent, KeyModifiers},
        ratatui::{Terminal, backend::TestBackend, style::Color},
        term::{Capabilities, ColourDepth},
    },
    tui_config::ConfigFiles,
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
async fn volume(name: &str) -> Result<TaskVolume<MemoryStream>> {
    Ok(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new(name)?).await?)
}
async fn put(volume: &TaskVolume<MemoryStream>, path: &str, bytes: &[u8]) -> Result {
    let key = FileKey::parse(path)?;
    let generation = volume
        .snapshot(&Query::File(key.clone()), 1)
        .await?
        .generation;
    volume
        .append(generation, &Write::new(key, bytes.to_vec()))
        .await?;
    Ok(())
}
#[test]
fn config_files_live_snapshot_follow_actions_and_reopen() -> Result {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let user = volume("config/user").await?;
            let workspace = volume("config/workspace").await?;
            put(
                &user,
                "config/settings.json",
                br#"{"tab_width":2,"wrap":false}"#,
            )
            .await?;
            put(&workspace, "config/settings.json", br#"{"tab_width":3}"#).await?;
            put(&user, "config/unrelated.json", b"{}").await?;
            assert_eq!(
                FileKey::parse("config/settings.json")?.owner(),
                Owner::Caller
            );
            assert_eq!(user.snapshot(&Query::Inputs, 10).await?.files.len(), 0);
            assert_eq!(user.snapshot(&Query::Config, 3).await?.files.len(), 1);
            let profile = Capabilities {
                colour: ColourDepth::TrueColour,
                ..Default::default()
            };
            let mut input = PromptInput::new(Editor::new("draft"));
            input.committed_text("!", 1);
            let mut files = ConfigFiles::observe(&user, &workspace, &mut input, &profile).await?;
            assert_eq!(input.editor.settings().tab_width, 3);
            assert!(!input.editor.settings().wrap);
            let draft = input.editor.text().to_owned();
            // Both UI actions and external editors publish the same ordinary files.
            assert!(
                files
                    .action(&workspace, Scope::Workspace, AppAction::ToggleWrap)
                    .await?
            );
            assert!(!input.editor.settings().wrap, "write waits for follow");
            files.next(Scope::Workspace, &mut input, &profile).await?;
            assert!(input.editor.settings().wrap);
            put(
                &user,
                "config/keymap.json",
                br#"{"bindings":{"F12":"app.tree"}}"#,
            )
            .await?;
            files.next(Scope::User, &mut input, &profile).await?;
            assert_eq!(
                input.key(KeyEvent::new(KeyCode::F(12), KeyModifiers::NONE), 2, 80),
                [InputEffect::Host(Action::App(AppAction::Tree))]
            );
            put(
                &user,
                "config/theme.json",
                br##"{"diagnostic":{"foreground":"#123456"}}"##,
            )
            .await?;
            files.next(Scope::User, &mut input, &profile).await?;
            assert_eq!(
                input.theme.style(ThemeRole::Diagnostic).fg,
                Some(Color::Rgb(18, 52, 86))
            );
            put(&workspace, "config/settings.json", br#"{"tab_wdth":0}"#).await?;
            files.next(Scope::Workspace, &mut input, &profile).await?;
            assert_eq!(
                input.editor.settings().tab_width,
                2,
                "invalid workspace falls back to user"
            );
            assert_eq!(
                files.view.diagnostics().next().unwrap().kind,
                DiagnosticKind::UnknownKey
            );
            let mut terminal = Terminal::new(TestBackend::new(100, 2))?;
            terminal.draw(|frame| {
                frame.render_widget(ConfigDiagnostics::new(&files.view, &input), frame.area())
            })?;
            assert_eq!(
                terminal.backend().buffer()[(0, 0)].fg,
                Color::Rgb(18, 52, 86)
            );
            let rendered: String = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|c| c.symbol())
                .collect();
            assert!(rendered.contains("$.tab_wdth"));
            assert_eq!(input.editor.text(), draft);
            assert!(input.editor.undo());
            assert_eq!(input.editor.text(), "draft");
            let notification = acyclic_harness::client::AttentionNotification {
                task: "task".into(),
                sequence: 1,
                event: acyclic_harness::client::AttentionEvent::TaskFailed,
                detail: String::new(),
            };
            let bell = acyclic_harness::client::TerminalNotificationProtocol::Bell;
            assert_eq!(files.notification(&notification, bell, false)?, "\x07");
            assert_eq!(files.notification(&notification, bell, true)?, "");
            files
                .action(&user, Scope::User, AppAction::ToggleNotifications)
                .await?;
            assert_eq!(files.notification(&notification, bell, false)?, "\x07");
            files.next(Scope::User, &mut input, &profile).await?;
            assert_eq!(files.notification(&notification, bell, false)?, "");
            // A new projection at a fresh cut is equivalent, without remounting input.
            let reopened = ConfigFiles::observe(&user, &workspace, &mut input, &profile).await?;
            for file in ConfigFile::ALL {
                assert_eq!(files.view.effective(file), reopened.view.effective(file));
            }
            assert_eq!(input.editor.text(), "draft");
            Ok(())
        })
}

#[test]
fn config_path_parser_regression_corpus() {
    for path in [
        "config/settings.json",
        "config/keymap.json",
        "config/theme.json",
        "config/../theme.json",
        "config//theme.json",
        "config/\\theme.json",
    ] {
        if let Ok(key) = FileKey::parse(path) {
            assert_eq!(key.path(), path);
            assert_eq!(FileKey::parse(&key.path()).unwrap(), key);
            let encoded = serde_json::to_vec(&key).unwrap();
            assert_eq!(serde_json::from_slice::<FileKey>(&encoded).unwrap(), key);
        }
    }
}

#[test]
fn rapid_setting_actions_read_the_file_before_toggling() -> Result {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let user = volume("rapid/user").await?;
            let workspace = volume("rapid/workspace").await?;
            let profile = Capabilities::default();
            let mut input = PromptInput::default();
            let mut files = ConfigFiles::observe(&user, &workspace, &mut input, &profile).await?;
            files
                .action(&workspace, Scope::Workspace, AppAction::ToggleWrap)
                .await?;
            files
                .action(&workspace, Scope::Workspace, AppAction::ToggleWrap)
                .await?;
            let snapshot = workspace.snapshot(&Query::Config, 3).await?;
            let value: serde_json::Value = serde_json::from_slice(&snapshot.files[0].bytes)?;
            assert_eq!(value["wrap"], true);
            files.next(Scope::Workspace, &mut input, &profile).await?;
            assert!(!input.editor.settings().wrap);
            files.next(Scope::Workspace, &mut input, &profile).await?;
            assert!(input.editor.settings().wrap);
            Ok(())
        })
}

#[test]
fn either_volume_can_publish_while_the_other_is_idle() -> Result {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let user = volume("either/user").await?;
            let workspace = volume("either/workspace").await?;
            let profile = Capabilities::default();
            let mut input = PromptInput::default();
            let mut files = ConfigFiles::observe(&user, &workspace, &mut input, &profile).await?;
            for (scope, value) in [
                (Scope::Workspace, 3),
                (Scope::User, 2),
                (Scope::Workspace, 5),
                (Scope::User, 7),
            ] {
                let source = if scope == Scope::User {
                    &user
                } else {
                    &workspace
                };
                let bytes = serde_json::json!({"tab_width":value}).to_string();
                put(source, "config/settings.json", bytes.as_bytes()).await?;
                let observed = tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    files.next_any(&mut input, &profile),
                )
                .await??;
                assert_eq!(observed, scope);
                assert_eq!(
                    input.editor.settings().tab_width,
                    if scope == Scope::Workspace {
                        value
                    } else if value == 2 {
                        3
                    } else {
                        5
                    }
                );
            }
            // Both queued streams are retained even when the other wins selection.
            put(&user, "config/settings.json", br#"{"wrap":false}"#).await?;
            put(&workspace, "config/settings.json", br#"{"tab_width":9}"#).await?;
            let first = files.next_any(&mut input, &profile).await?;
            let second = files.next_any(&mut input, &profile).await?;
            assert_ne!(first, second);
            assert!(!input.editor.settings().wrap);
            assert_eq!(input.editor.settings().tab_width, 9);
            Ok(())
        })
}

#[test]
fn shadowed_user_shortcuts_are_rejected_and_workspace_toggles_work() -> Result {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let user = volume("conflict/user").await?;
            let workspace = volume("conflict/workspace").await?;
            put(
                &user,
                "config/settings.json",
                br#"{"wrap":true,"notifications":"on"}"#,
            )
            .await?;
            put(
                &workspace,
                "config/settings.json",
                br#"{"wrap":false,"notifications":"off"}"#,
            )
            .await?;
            let profile = Capabilities::default();
            let mut input = PromptInput::default();
            let mut files = ConfigFiles::observe(&user, &workspace, &mut input, &profile).await?;
            let before = user.snapshot(&Query::Config, 3).await?;
            for action in [AppAction::ToggleWrap, AppAction::ToggleNotifications] {
                assert!(matches!(
                    files.action(&user, Scope::User, action).await,
                    Err(acyclic_harness::tui_config::ConfigError::ShadowedSetting)
                ));
            }
            let after = user.snapshot(&Query::Config, 3).await?;
            assert_eq!(
                before.generation, after.generation,
                "rejected shortcuts must not publish"
            );
            assert!(!input.editor.settings().wrap);
            assert!(
                files
                    .action(&workspace, Scope::Workspace, AppAction::ToggleWrap)
                    .await?
            );
            files.next_any(&mut input, &profile).await?;
            assert!(input.editor.settings().wrap);
            let saved: serde_json::Value = serde_json::from_slice(
                &workspace.snapshot(&Query::Config, 3).await?.files[0].bytes,
            )?;
            assert_eq!(saved["wrap"], true);
            Ok(())
        })
}

#[test]
fn unrelated_publications_keep_the_configuration_and_advance_cursors() -> Result {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let user = volume("unrelated/user").await?;
            let workspace = volume("unrelated/workspace").await?;
            let profile = Capabilities::default();
            let mut input = PromptInput::new(Editor::new("draft"));
            let mut files = ConfigFiles::observe(&user, &workspace, &mut input, &profile).await?;
            let cursor = files.cursors[0].clone();
            put(&user, "workspace/unrelated.rs", b"source").await?;
            files.next(Scope::User, &mut input, &profile).await?;
            assert!(files.cursors[0].position > cursor.position);
            assert_eq!(input.editor.text(), "draft");
            assert_eq!(
                *input.editor.settings(),
                acyclic_harness::tui::DEFAULT_SETTINGS
            );
            Ok(())
        })
}

#[test]
fn invalid_fresh_toggle_snapshot_is_rejected_without_rewriting_it() -> Result {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let user = volume("invalid/user").await?;
            let workspace = volume("invalid/workspace").await?;
            let profile = Capabilities::default();
            let mut input = PromptInput::default();
            let files = ConfigFiles::observe(&user, &workspace, &mut input, &profile).await?;
            // Publish after observation: action must validate the fresh file, not the cached view.
            put(&workspace, "config/settings.json", br#"{"unknown":true}"#).await?;
            let before = workspace.snapshot(&Query::Config, 3).await?;
            assert!(matches!(
                files
                    .action(&workspace, Scope::Workspace, AppAction::ToggleWrap)
                    .await,
                Err(acyclic_harness::tui_config::ConfigError::Diagnostics(_))
            ));
            let after = workspace.snapshot(&Query::Config, 3).await?;
            assert_eq!(after.generation, before.generation);
            assert_eq!(after.files[0].bytes, before.files[0].bytes);
            Ok(())
        })
}
