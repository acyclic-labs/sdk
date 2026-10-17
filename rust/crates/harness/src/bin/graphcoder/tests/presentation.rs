use acyclic_harness::{
    OperationId,
    client::{CommandFileRoot, CommandSession, TaskFileViews},
    model::ModelEvent,
    task_volume::TaskVolume,
    tui::{
        ClientRenderer, PromptInput, Viewport,
        ratatui::{Terminal, backend::TestBackend},
    },
    wire,
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

#[tokio::test]
async fn followed_presentation_rows_are_declarative_and_live() {
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("presentation.json")).unwrap();
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("presentation").unwrap(),
        )
        .await
        .unwrap(),
    );
    let mut files = CommandSession::open(
        volume,
        OperationId::from_bytes([12; 16])
            .to_string()
            .parse()
            .unwrap(),
        65_536,
        64,
    )
    .await
    .unwrap();
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "task".into(),
    };
    let turn = OperationId::from_bytes([13; 16]);
    let mut views = TaskFileViews::default();
    views
        .model_event(
            &owner,
            turn,
            ModelEvent::Reasoning {
                delta: "Private reasoning".into(),
            },
        )
        .unwrap();
    views
        .model_event(
            &owner,
            turn,
            ModelEvent::Content {
                delta: "**Visible answer**".into(),
            },
        )
        .unwrap();
    let mut renderer = ClientRenderer::default();
    renderer.update(&views, &owner);
    for row in rows {
        views.tasks.clear();
        views.tools.clear();
        if let Some(count) = row["tool_lines"].as_u64() {
            views
                .model_event(
                    &owner,
                    turn,
                    ModelEvent::ToolCall(acyclic_harness::model::ToolCall::new(
                        "call".into(),
                        "read".into(),
                        Default::default(),
                    )),
                )
                .unwrap();
            views.tool_output(
                &owner,
                "call",
                &(0..count)
                    .map(|index| format!("detail {index:02}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
        }
        views.documents.clear();
        let mut documents = acyclic_harness::client::DocumentViews::default();
        for (field, key) in [
            ("goal", acyclic_harness::task_volume::FileKey::Goal),
            (
                "plan",
                acyclic_harness::task_volume::FileKey::Plan(
                    acyclic_harness::task_volume::PlanName::new("layout").unwrap(),
                ),
            ),
        ] {
            if let Some(text) = row[field].as_str() {
                documents
                    .apply(&acyclic_harness::task_volume::Write::new(
                        key,
                        text.as_bytes().to_vec(),
                    ))
                    .unwrap();
            }
        }
        views.documents.insert(
            format!("{}:{}", wire::AggregateKind::Task.as_str_name(), owner.id),
            documents,
        );
        if let Some(status) = row.get("status") {
            views.tasks.insert(
                format!("{}:{}", wire::AggregateKind::Task.as_str_name(), owner.id),
                acyclic_harness::client::TaskView {
                    status: serde_json::from_value(status.clone()).unwrap(),
                    lifecycle_operation_id: None,
                    reason: row
                        .get("feedback")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned),
                    parent: None,
                },
            );
        }
        renderer.update(&views, &owner);
        files
            .write_state(
                CommandFileRoot::Config,
                "settings.json",
                &serde_json::json!({"reasoning":row["reasoning"], "tool_details":row["details"].as_bool().unwrap_or(false)}),
            )
            .await
            .unwrap();
        let width = row["width"].as_u64().unwrap_or(80) as u16;
        let height = row["height"].as_u64().unwrap_or(20) as u16;
        renderer.configure(&views, &owner, &files).unwrap();
        renderer
            .layout(&files, &PromptInput::default(), width, height)
            .unwrap();
        let screen = renderer.screen_for(&files).unwrap();
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| {
                screen.render(
                    frame,
                    &PromptInput::default(),
                    &mut Viewport::default(),
                    0,
                    0,
                );
            })
            .unwrap();
        let picture = super::screen_text(&terminal);
        if row["bold_header"].as_bool() == Some(true) {
            for column in 2..12 {
                assert!(
                    terminal.backend().buffer()[(column, 0)]
                        .modifier
                        .contains(acyclic_harness::tui::ratatui::style::Modifier::BOLD),
                    "{}",
                    row["row"]
                );
            }
        }
        for text in row["expected"].as_array().unwrap() {
            assert!(
                picture.contains(text.as_str().unwrap()),
                "{}: {picture}",
                row["row"]
            );
        }
        for text in row["absent"].as_array().unwrap() {
            assert!(
                !picture.contains(text.as_str().unwrap()),
                "{}: {picture}",
                row["row"]
            );
        }
    }
    files
        .write_state(
            CommandFileRoot::Config,
            "settings.json",
            &serde_json::json!({"reasoning":"invalid"}),
        )
        .await
        .unwrap();
    assert!(renderer.screen_for(&files).is_err());
    let before = format!("{:?}", renderer.screen().transcript);
    files
        .write_state(
            CommandFileRoot::Config,
            "settings.json",
            &serde_json::json!({"reasoning":false,"tool_details":"invalid"}),
        )
        .await
        .unwrap();
    assert!(renderer.configure(&views, &owner, &files).is_err());
    assert_eq!(format!("{:?}", renderer.screen().transcript), before);
}

#[tokio::test]
async fn wrapped_transcript_follows_tail_across_chunks_resize_and_reset() {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("wrapped-transcript").unwrap(),
        )
        .await
        .unwrap(),
    );
    let files = CommandSession::open(
        volume,
        OperationId::from_bytes([21; 16])
            .to_string()
            .parse()
            .unwrap(),
        65_536,
        64,
    )
    .await
    .unwrap();
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "task".into(),
    };
    let mut views = TaskFileViews::default();
    let mut live = ClientRenderer::default();
    for width in [12, 20, 12] {
        views
            .model_event(
                &owner,
                OperationId::from_bytes([22; 16]),
                ModelEvent::Content {
                    delta: "界👩‍💻e\u{301} end ".repeat(8),
                },
            )
            .unwrap();
        live.update(&views, &owner);
        let mut replay = ClientRenderer::default();
        replay.update(&views, &owner);
        let mut pictures = Vec::new();
        for renderer in [&mut live, &mut replay] {
            renderer
                .layout(&files, &PromptInput::default(), width, 12)
                .unwrap();
            let screen = renderer.screen_for(&files).unwrap();
            assert!(
                screen
                    .transcript
                    .iter()
                    .all(|line| line.width() <= usize::from(width))
            );
            assert!(screen.transcript.len() > 1);
            let mut terminal = Terminal::new(TestBackend::new(width, 12)).unwrap();
            terminal
                .draw(|frame| {
                    let scroll = screen.tail_scroll(frame.area(), &PromptInput::default());
                    screen.render(
                        frame,
                        &PromptInput::default(),
                        &mut Viewport::default(),
                        scroll,
                        0,
                    );
                })
                .unwrap();
            assert!(super::screen_text(&terminal).contains("end"));
            pictures.push(terminal.backend().buffer().clone());
        }
        assert_eq!(pictures[0], pictures[1]);
    }
    live.update(&TaskFileViews::default(), &owner);
    live.layout(&files, &PromptInput::default(), 12, 12)
        .unwrap();
    assert!(live.screen().transcript.is_empty());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: streamed reasoning and live visibility changes paint the same
    // cells as reopening the current TaskFileViews and settings-file snapshot.
    #[test]
    fn reasoning_visibility_and_chunking_are_replay_equivalent(chunks in proptest::collection::vec((".{0,12}", proptest::bool::ANY), 1..6)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("reasoning-replay").unwrap()).await.unwrap());
            let mut files = CommandSession::open(volume, OperationId::from_bytes([12;16]).to_string().parse().unwrap(), 65_536, 64).await.unwrap();
            let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: "task".into() };
            let turn = OperationId::from_bytes([13;16]);
            let mut views = TaskFileViews::default();
            views.model_event(&owner, turn, ModelEvent::ToolCall(acyclic_harness::model::ToolCall::new("call".into(), "read".into(), Default::default()))).unwrap();
            let mut live = ClientRenderer::default();
            for (chunk, visible) in chunks {
                views.tool_output(&owner, "call", &chunk);
                views.model_event(&owner, turn, ModelEvent::Reasoning { delta: chunk }).unwrap();
                files.write_state(CommandFileRoot::Config, "settings.json", &serde_json::json!({"reasoning":visible,"tool_details":visible})).await.unwrap();
                live.update(&views, &owner);
                let mut replay = ClientRenderer::default();
                replay.update(&views, &owner);
                let mut pictures = Vec::new();
                for renderer in [&mut live, &mut replay] {
                    renderer.configure(&views, &owner, &files).unwrap();
                    renderer.layout(&files, &PromptInput::default(), 40, 20).unwrap();
                    let screen = renderer.screen_for(&files).unwrap();
                    if !visible { proptest::prop_assert!(screen.reasoning.is_empty()); }
                    let mut terminal = Terminal::new(TestBackend::new(40, 20)).unwrap();
                    terminal.draw(|frame| { screen.render(frame, &PromptInput::default(), &mut Viewport::default(), 0, 0); }).unwrap();
                    pictures.push(terminal.backend().buffer().clone());
                }
                proptest::prop_assert_eq!(&pictures[0], &pictures[1]);
            }
            Ok(())
        })?;
    }
}
