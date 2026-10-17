use super::*;
use crate::tui::{
    ClientRenderer, PromptInput, Viewport,
    ratatui::{Terminal, backend::TestBackend},
};
fn picture(
    renderer: &mut ClientRenderer,
    views: &TaskFileViews,
    owner: &wire::Authority,
) -> String {
    renderer.update(views, owner);
    let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
    terminal
        .draw(|frame| {
            renderer.screen().render(
                frame,
                &PromptInput::default(),
                &mut Viewport::default(),
                0,
                0,
            );
        })
        .unwrap();
    terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect()
}
fn authority(task: crate::TaskId) -> wire::Authority {
    wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    }
}
fn query(task: crate::TaskId, recent: u32) -> Query {
    Query::TaskView {
        task: Component::new(task.to_string()).unwrap(),
        recent,
        reasoning: true,
        details: true,
        terminal: true,
    }
}
fn chunk(task: crate::TaskId, sequence: u64, bytes: &[u8]) -> FileValue {
    FileValue {
        requester: None,
        publication: None,
        key: FileKey::Task {
            task: Component::new(task.to_string()).unwrap(),
            file: TaskFile::Chunk {
                execution: crate::OperationId::from_bytes([0; 16]),
                turn: 0,
                sequence,
            },
        },
        channel: Some(Channel::Text),
        bytes: bytes.to_vec().into(),
    }
}
#[tokio::test]
async fn split_utf8_reaches_live_markdown_and_composed_transcript() {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("utf8-live-render").unwrap(),
        )
        .await
        .unwrap(),
    );
    let mut commands =
        crate::client::CommandSession::open(volume.clone(), uuid::Uuid::new_v4(), 65_536, 128)
            .await
            .unwrap();
    let operation = commands
        .publish_resolution(crate::client::CommandResolution::Prompt {
            text: "request".into(),
            source: None,
            mentions: Vec::new(),
        })
        .await
        .unwrap();
    let task = crate::runtime::task_id_for_admission(operation);
    let owner = authority(task);
    let root = authority(crate::TaskId::new());
    let mut client = TaskFileClient::observe_task(volume.as_ref(), &owner, task, 16, 1024)
        .await
        .unwrap();
    let mut direct = ClientRenderer::default();
    let mut composed = ClientRenderer::default();
    let mut generation = volume.snapshot(&Query::Root, 128).await.unwrap().generation;
    for (sequence, bytes) in [vec![0xe2], vec![0x82, 0xac]].iter().enumerate() {
        generation = volume
            .append(generation, &chunk(task, sequence as u64, bytes))
            .await
            .unwrap();
        client.next_change().await.unwrap();
        client.project_commands(&commands, &root).unwrap();
        let expected = if sequence == 0 { "�" } else { "€" };
        for screen in [
            picture(&mut direct, client.views(), &owner),
            picture(&mut composed, client.views(), &root),
        ] {
            assert!(
                screen.contains('�') == (sequence == 0),
                "incomplete UTF-8 must not become a permanent Markdown prefix"
            );
            assert!(screen.contains(expected));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    // Every bounded window equals a fresh snapshot, including repeated
    // same-length prefix replacements; unrelated completed tasks stay frozen.
    #[test]
    fn evicted_segments_leave_no_text_or_state_and_repaint_the_live_window(
        pieces in prop::collection::vec("[a-z]{8}", 4..16),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let volume = TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("window-repaint").unwrap()).await.unwrap();
            let task = crate::TaskId::new(); let owner = authority(task); let selected = query(task, 2);
            let frozen_task = crate::TaskId::new();
            let mut generation = volume.snapshot(&Query::Root, 32).await.unwrap().generation;
            generation = volume.append(generation, &chunk(frozen_task, 0, b"frozen")).await.unwrap();
            let mut client = TaskFileClient::observe_query(&volume, &owner, selected.clone(), 2, 512).await.unwrap();
            let frozen_query = Query::Task(Component::new(frozen_task.to_string()).unwrap());
            client.session.query_snapshot(frozen_query.clone(), volume.snapshot(&frozen_query, 32).await.unwrap(), 32, 512).unwrap();
            let frozen_key = crate::client::task_compat::views::authority_key(&authority(frozen_task));
            let frozen = client.views().transcripts[&frozen_key].clone();
            let key = crate::client::task_compat::views::authority_key(&owner);
            let mut renderer = ClientRenderer::default();
            for (index, text) in pieces.iter().enumerate() {
                generation = volume.append(generation, &chunk(task, index as u64, text.as_bytes())).await.unwrap();
                client.next_change().await.unwrap();
                let expected = pieces[index.saturating_sub(1)..=index].concat();
                prop_assert_eq!(&client.views().transcripts[&key].streaming_text, &expected);
                let reopened = TaskFileClient::observe_query(&volume, &owner, selected.clone(), 2, 512).await.unwrap();
                prop_assert_eq!(&client.views().transcripts[&key], &reopened.views().transcripts[&key]);
                prop_assert_eq!(&client.views().task_folds[&key], &reopened.views().task_folds[&key]);
                prop_assert_eq!(&client.views().transcripts[&frozen_key], &frozen);
                prop_assert!(picture(&mut renderer, client.views(), &owner).contains(&expected));
            }
            Ok(())
        })?;
    }
}
#[tokio::test]
async fn removing_the_last_chunk_clears_the_projection() {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("empty-window").unwrap(),
    )
    .await
    .unwrap();
    let task = crate::TaskId::new();
    let owner = authority(task);
    let selected = query(task, 1);
    let generation = volume.snapshot(&Query::Root, 4).await.unwrap().generation;
    let generation = volume
        .append(generation, &chunk(task, 0, b"evicted"))
        .await
        .unwrap();
    let mut client = TaskFileClient::observe_query(&volume, &owner, selected.clone(), 1, 128)
        .await
        .unwrap();
    let status = FileValue::new(
        FileKey::Task {
            task: Component::new(task.to_string()).unwrap(),
            file: TaskFile::Status,
        },
        b"{}".to_vec(),
    );
    volume.append(generation, &status).await.unwrap();
    client.next_change().await.unwrap();
    let reopened = TaskFileClient::observe_query(&volume, &owner, selected, 1, 128)
        .await
        .unwrap();
    assert_eq!(client.views().transcripts, reopened.views().transcripts);
    assert_eq!(client.views().tools, reopened.views().tools);
    assert!(client.views().transcripts.is_empty());
}

#[tokio::test]
async fn outcome_shaped_chunk_bytes_stay_text_during_window_rebuild() {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("typed-window").unwrap(),
    )
    .await
    .unwrap();
    let task = crate::TaskId::new();
    let owner = authority(task);
    let selected = query(task, 2);
    let mut generation = volume.snapshot(&Query::Root, 4).await.unwrap().generation;
    let mut client = TaskFileClient::observe_query(&volume, &owner, selected.clone(), 2, 512)
        .await
        .unwrap();
    let json = br#"{"sequence":1,"ok":{"text":"not an outcome"}}"#;
    for (sequence, bytes) in [b"old".as_slice(), json.as_slice(), b"new".as_slice()]
        .into_iter()
        .enumerate()
    {
        generation = volume
            .append(generation, &chunk(task, sequence as u64, bytes))
            .await
            .unwrap();
        client.next_change().await.unwrap();
    }
    let expected = format!("{}new", std::str::from_utf8(json).unwrap());
    let key = crate::client::task_compat::views::authority_key(&owner);
    assert_eq!(client.views().transcripts[&key].streaming_text, expected);
    let reopened = TaskFileClient::observe_query(&volume, &owner, selected, 2, 512)
        .await
        .unwrap();
    assert_eq!(client.views().transcripts, reopened.views().transcripts);
}
