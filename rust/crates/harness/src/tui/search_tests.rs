use super::*;
use crate::{
    TaskId,
    client::{CommandFileRoot, CommandSession, PromptFiles, TaskFileClient, file_tui_input},
    task_volume::{Channel, Component, FileKey, FileValue, TaskFile, TaskVolume},
    tui::{
        Action, AppAction, InputEffect, PromptInput, Viewport,
        key_event::{KeyCode, KeyEvent, KeyModifiers as M},
        ratatui::{Terminal, backend::TestBackend},
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;
struct NoFiles;
impl crate::conversation::ContentResidencyVerifier for NoFiles {}

fn paint_frame(
    renderer: &mut ClientRenderer,
    files: &CommandSession<MemoryStream>,
    input: &PromptInput,
    width: u16,
) -> super::super::ratatui::buffer::Buffer {
    renderer.layout(files, input, width, 20).unwrap();
    let screen = renderer.screen_for(files).unwrap();
    let mut terminal = Terminal::new(TestBackend::new(width, 20)).unwrap();
    terminal
        .draw(|frame| {
            screen.render(frame, input, &mut Viewport::default(), 0, 0);
        })
        .unwrap();
    terminal.backend().buffer().clone()
}

fn paint(
    renderer: &mut ClientRenderer,
    files: &CommandSession<MemoryStream>,
    input: &PromptInput,
    width: u16,
) -> String {
    paint_frame(renderer, files, input, width)
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect()
}

async fn scenario<const BOUND: bool>(query: &str) {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("find").unwrap(),
        )
        .await
        .unwrap(),
    );
    let task = TaskId::from_bytes([24; 16]);
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    };
    let mut file = FileValue::new(
        FileKey::Task {
            task: Component::new(task.to_string()).unwrap(),
            file: TaskFile::Chunk {
                turn: 0,
                sequence: 0,
            },
        },
        format!("Straße\n\n{}\nSTRASSE\n", "filler\n\n".repeat(30)).into_bytes(),
    );
    file.channel = Some(Channel::Text);
    volume
        .append(volume.head().await.unwrap(), &file)
        .await
        .unwrap();
    let mut files = CommandSession::open(
        volume.clone(),
        uuid::Uuid::from_bytes([25; 16]),
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
    let client = TaskFileClient::observe(volume.as_ref(), &owner, 128, 65_536)
        .await
        .unwrap();
    let mut renderer = ClientRenderer::default();
    renderer.update(client.views(), &owner);
    renderer.configure(client.views(), &owner, &files).unwrap();
    let mut input = PromptInput::default();
    let mut prompt = PromptFiles::default();
    prompt.refresh(&files, &mut input).unwrap();
    input.editor.set_text("unsent draft", 1);
    input.editor.paste(&"folded paste\n".repeat(20), 2);
    input.editor.set_cursor(0, false);
    assert!(!input.editor.collapsed_pastes().is_empty());
    let draft = format!("{:?}", input.editor);
    prompt.save(&mut files, &mut input).await.unwrap();
    let effects = input.key(KeyEvent::new(KeyCode::Char('f'), M::CONTROL), 2, 78);
    assert_eq!(
        effects,
        vec![InputEffect::Host(Action::App(AppAction::Find))]
    );
    for effect in effects {
        file_tui_input(&mut files, &NoFiles, effect, 20, 0)
            .await
            .unwrap();
    }
    prompt.refresh(&files, &mut input).unwrap();
    assert!(input.transcript_search(), "search form must own the input");
    input.paste(query, 2);
    prompt.save(&mut files, &mut input).await.unwrap();
    prompt.refresh(&files, &mut input).unwrap();
    if !BOUND {
        input.set_transcript_search(None);
    }
    let effects = input.key(KeyEvent::new(KeyCode::Enter, M::NONE), 3, 78);
    assert!(
        matches!(
            effects.as_slice(),
            [InputEffect::FindStep { previous: false }]
        ),
        "search must navigate instead of submitting chat"
    );
    for effect in effects {
        file_tui_input(&mut files, &NoFiles, effect, 20, 0)
            .await
            .unwrap();
    }
    assert!(files.take_request().is_none());
    let text = paint(&mut renderer, &files, &input, 80);
    assert!(
        text.contains("2/2 matches"),
        "Unicode matches must reach the composed status"
    );
    assert!(text.contains("STRASSE"), "next match must scroll into view");
    // Changing only the resolved theme must invalidate cached search styles.
    let mut theme = super::super::config::ConfigView::default();
    for (color, expected) in [
        ("#112233", Color::Rgb(17, 34, 51)),
        ("#445566", Color::Rgb(68, 85, 102)),
    ] {
        theme.set_file(
            super::super::config::Scope::User,
            super::super::config::ConfigFile::Theme,
            Some(&serde_json::json!({"selection":{"foreground":color}}).to_string()),
        );
        input.theme = theme
            .theme(&super::super::term::Capabilities {
                colour: super::super::term::ColourDepth::TrueColour,
                ..Default::default()
            })
            .unwrap();
        let frame = paint_frame(&mut renderer, &files, &input, 80);
        assert!(
            frame
                .content
                .iter()
                .any(|cell| cell.fg == expected && cell.modifier.contains(Modifier::UNDERLINED)),
            "resolved selection theme must reach the composed search frame"
        );
    }
    let reopened = CommandSession::open(
        volume.clone(),
        uuid::Uuid::from_bytes([25; 16]),
        65_536,
        128,
    )
    .await
    .unwrap();
    let mut restored = PromptInput::default();
    PromptFiles::default()
        .refresh(&reopened, &mut restored)
        .unwrap();
    assert_eq!(restored.editor.text(), query);
    assert_eq!(paint(&mut renderer, &reopened, &restored, 80), text);
    assert!(paint(&mut renderer, &reopened, &restored, 40).contains("2 matches"));
    let effects = input.key(KeyEvent::new(KeyCode::Enter, M::SHIFT), 4, 78);
    for effect in effects {
        file_tui_input(&mut files, &NoFiles, effect, 20, 0)
            .await
            .unwrap();
    }
    assert!(paint(&mut renderer, &files, &input, 80).contains("1/2 matches"));
    for effect in input.key(KeyEvent::new(KeyCode::Esc, M::NONE), 5, 78) {
        file_tui_input(&mut files, &NoFiles, effect, 20, 0)
            .await
            .unwrap();
    }
    prompt.save(&mut files, &mut input).await.unwrap();
    prompt.refresh(&files, &mut input).unwrap();
    assert!(!input.transcript_search());
    assert_eq!(
        format!("{:?}", input.editor),
        draft,
        "complete chat editor must survive file-backed search"
    );
    for effect in input.key(KeyEvent::new(KeyCode::Enter, M::NONE), 6, 78) {
        file_tui_input(&mut files, &NoFiles, effect, 20, 0)
            .await
            .unwrap();
    }
    assert!(
        files.take_request().is_some(),
        "closing find must restore ordinary submission"
    );
    let catalog: Vec<_> = files
        .catalog()
        .commands()
        .filter(|spec| {
            spec.action
                != crate::client::CommandAction::Select {
                    query: crate::client::CommandQuery::Find,
                }
        })
        .cloned()
        .collect();
    files
        .install_catalog(serde_json::to_vec(&catalog).unwrap())
        .await
        .unwrap();
    file_tui_input(
        &mut files,
        &NoFiles,
        InputEffect::Host(Action::App(AppAction::Find)),
        20,
        0,
    )
    .await
    .unwrap();
    assert!(
        files
            .state(CommandFileRoot::Session, "layout.json")
            .unwrap()
            .unwrap()["open_view"]
            .is_null()
    );
    assert!(!files.catalog().commands().any(|spec| spec.action
        == crate::client::CommandAction::Select {
            query: crate::client::CommandQuery::Find
        }));
    assert!(
        files.take_request().is_none(),
        "a removed search command cannot dispatch work"
    );
}

#[test]
fn find_form_navigates_reopens_resizes_and_restores_chat() {
    for query in ["strasse", "STRASSE", "Strasse"] {
        futures::executor::block_on(scenario::<true>(query));
    }
}

#[test]
#[should_panic(expected = "search must navigate instead of submitting chat")]
fn missing_input_binding_is_detected_negative_control() {
    futures::executor::block_on(scenario::<false>("strasse"));
}
