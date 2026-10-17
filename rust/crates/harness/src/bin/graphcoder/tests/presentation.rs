use acyclic_harness::{
    OperationId,
    client::{CommandFileRoot, CommandSession},
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
    let mut views = super::file_views::empty();
    super::file_views::append_text(&mut views, &owner.id, true, "Private reasoning");
    super::file_views::append_text(&mut views, &owner.id, false, "**Visible answer**");
    let mut renderer = ClientRenderer::default();
    renderer.update(&views, &owner);
    for row in rows {
        let keys: Vec<_> = views
            .selections()
            .flat_map(|(_, files)| files.files().keys())
            .filter(|key| {
                !matches!(
                    key,
                    acyclic_harness::task_volume::FileKey::Task {
                        file: acyclic_harness::task_volume::TaskFile::Chunk { .. },
                        ..
                    }
                )
            })
            .cloned()
            .collect();
        for key in keys {
            super::file_views::remove(&mut views, key);
        }
        if let Some(count) = row["tool_lines"].as_u64() {
            let output = (0..count)
                .map(|index| format!("detail {index:02}"))
                .collect::<Vec<_>>()
                .join("\n");
            super::file_views::tool(&mut views, &owner.id, "read", &output);
        }
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
                super::file_views::document(&mut views, key, text);
            }
        }
        if let Some(status) = row["status"].as_str() {
            super::file_views::status(
                &mut views,
                &owner.id,
                &status.to_ascii_lowercase(),
                row["feedback"].as_str(),
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
    let mut views = super::file_views::empty();
    let mut live = ClientRenderer::default();
    for width in [12, 20, 12] {
        super::file_views::append_text(
            &mut views,
            &owner.id,
            false,
            &"界👩‍💻e\u{301} end ".repeat(8),
        );
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
    live.update(&super::file_views::empty(), &owner);
    live.layout(&files, &PromptInput::default(), 12, 12)
        .unwrap();
    assert!(live.screen().transcript.is_empty());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: streamed reasoning and live visibility changes paint the same
    // cells as reopening the current ClientViews and settings-file snapshot.
    #[test]
    fn reasoning_visibility_and_chunking_are_replay_equivalent(chunks in proptest::collection::vec((".{0,12}", proptest::bool::ANY), 1..6)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("reasoning-replay").unwrap()).await.unwrap());
            let mut files = CommandSession::open(volume, OperationId::from_bytes([12;16]).to_string().parse().unwrap(), 65_536, 64).await.unwrap();
            let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: "task".into() };
            let mut views = super::file_views::empty();
            super::file_views::tool(&mut views,&owner.id,"read","");
            let mut live = ClientRenderer::default();
            for (index,(chunk, visible)) in chunks.into_iter().enumerate() {
                super::file_views::put(&mut views,acyclic_harness::task_volume::FileKey::parse(&format!(".tasks/task/turns/0/tools/call/stdout/{index}")).unwrap(),chunk.as_bytes().to_vec(),None);
                super::file_views::append_text(&mut views,&owner.id,true,&chunk);
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
