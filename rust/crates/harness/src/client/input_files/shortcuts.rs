use super::*;
use crate::{
    OperationId, TaskId,
    client::{PromptFiles, TaskFileClient},
    task_volume::{Channel, Component, FileKey, FileValue, TaskFile, TaskVolume, ToolFile},
    tui::{
        Action, ClientRenderer, Viewport,
        key_event::{KeyCode, KeyEvent, KeyModifiers},
        ratatui::{Terminal, backend::TestBackend},
    },
    wire,
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

struct NoFiles;
impl ContentResidencyVerifier for NoFiles {}

fn rows() -> Vec<Value> {
    serde_json::from_str(include_str!("shortcuts.json")).unwrap()
}

async fn scenario<const BOUND: bool>(row: &Value, rename: &str, remap: bool) {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("shortcuts").unwrap(),
        )
        .await
        .unwrap(),
    );
    let task = TaskId::from_bytes([22; 16]);
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    };
    for (file, channel, text) in [
        (
            TaskFile::Chunk {
                execution: OperationId::from_bytes([0; 16]),
                turn: 0,
                sequence: 0,
            },
            Some(Channel::Reasoning),
            "Reasoning marker",
        ),
        (
            TaskFile::Tool {
                turn: 0,
                call: Component::new("call").unwrap(),
                file: ToolFile::Stdout(0),
            },
            None,
            "Tool output marker",
        ),
    ] {
        let mut value = FileValue::new(
            FileKey::Task {
                task: Component::new(task.to_string()).unwrap(),
                file,
            },
            text.as_bytes().to_vec(),
        );
        value.channel = channel;
        volume
            .append(volume.head().await.unwrap(), &value)
            .await
            .unwrap();
    }
    let mut files = CommandSession::open(
        volume.clone(),
        OperationId::from_bytes([23; 16])
            .to_string()
            .parse()
            .unwrap(),
        65_536,
        128,
    )
    .await
    .unwrap();
    files
        .write_state(
            CommandFileRoot::Config,
            "settings.json",
            &serde_json::json!({"renderer":"ratatui"}),
        )
        .await
        .unwrap();
    let query: CommandQuery = serde_json::from_value(row["query"].clone()).unwrap();
    let selected = CommandAction::Select { query };
    let name = format!("ui-{rename}-{}", row["action"].as_str().unwrap());
    let mut catalog: Vec<_> = files.catalog().commands().cloned().collect();
    let spec = catalog
        .iter_mut()
        .find(|spec| spec.action == selected)
        .unwrap();
    spec.name = name.clone();
    spec.aliases.clear();
    files
        .install_catalog(serde_json::to_vec(&catalog).unwrap())
        .await
        .unwrap();
    let mut input = PromptInput::default();
    if remap {
        files.write_state(CommandFileRoot::Config,"keymap.json",&serde_json::json!({"bindings":{"Alt+z":format!("app.{}",row["action"].as_str().unwrap())}})).await.unwrap();
    }
    PromptFiles::default().refresh(&files, &mut input).unwrap();
    let client = TaskFileClient::observe(volume.as_ref(), &owner, 128, 65_536)
        .await
        .unwrap();
    let mut renderer = ClientRenderer::default();
    for step in 1..=3 {
        let key = if remap {
            KeyEvent::new(KeyCode::Char('z'), KeyModifiers::ALT)
        } else {
            KeyEvent::new(
                KeyCode::Char(row["key"].as_str().unwrap().chars().next().unwrap()),
                KeyModifiers::CONTROL
                    | if row["shift"] == true {
                        KeyModifiers::SHIFT
                    } else {
                        KeyModifiers::NONE
                    },
            )
        };
        let effects = input.key(key, step * 1000, 80);
        assert_eq!(effects.len(), 1);
        for effect in effects {
            let effect = if BOUND {
                effect
            } else {
                InputEffect::Host(Action::Voice)
            };
            file_tui_input(&mut files, &NoFiles, effect, 20, 0)
                .await
                .unwrap();
        }
        let enabled = step % 2 == 1;
        if let Some(setting) = row["setting"].as_str() {
            assert_eq!(
                files
                    .state(CommandFileRoot::Config, "settings.json")
                    .unwrap()
                    .unwrap()[setting],
                enabled,
                "declared shortcut must execute its catalog action"
            );
        }
        renderer.update(client.views(), &owner);
        renderer.configure(client.views(), &owner, &files).unwrap();
        let screen = renderer.screen_for(&files).unwrap();
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal
            .draw(|frame| {
                screen.render(frame, &input, &mut Viewport::default(), 0, step * 1000);
            })
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert_eq!(
            text.contains(row["marker"].as_str().unwrap()),
            row["setting"].is_null() || enabled,
            "shortcut must update the composed widget"
        );
        assert!(
            files.take_request().is_none(),
            "view shortcuts must not dispatch model work"
        );
    }
    let before = files
        .state(CommandFileRoot::Config, "settings.json")
        .unwrap();
    let catalog: Vec<_> = files
        .catalog()
        .commands()
        .filter(|spec| spec.action != selected)
        .cloned()
        .collect();
    files
        .install_catalog(serde_json::to_vec(&catalog).unwrap())
        .await
        .unwrap();
    let effect = InputEffect::Host(Action::App(match query {
        CommandQuery::Reasoning => crate::tui::AppAction::Reasoning,
        CommandQuery::Details => crate::tui::AppAction::Details,
        _ => crate::tui::AppAction::Help,
    }));
    file_tui_input(&mut files, &NoFiles, effect, 20, 0)
        .await
        .unwrap();
    assert_eq!(
        files
            .state(CommandFileRoot::Config, "settings.json")
            .unwrap(),
        before
    );
    assert!(
        !files
            .catalog()
            .commands()
            .any(|spec| spec.action == selected)
    );
    assert!(files.take_request().is_none());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(12))]
    // Shortcuts resolve catalog actions rather than fixed command names. User
    // keymap overrides, renames and removals remain authoritative.
    #[test]
    fn declared_shortcuts_follow_catalog_and_keymap_files(rename in "[a-z]{1,8}",remap in proptest::prelude::any::<bool>()) {
        futures::executor::block_on(async {for row in rows(){scenario::<true>(&row,&rename,remap).await;}});
    }
}

#[test]
#[should_panic(expected = "declared shortcut must execute its catalog action")]
fn missing_shortcut_binding_is_detected_negative_control() {
    futures::executor::block_on(scenario::<false>(&rows()[0], "renamed", false));
}
