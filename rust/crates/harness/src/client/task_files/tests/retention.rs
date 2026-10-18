use super::*;
use crate::tui::{
    ClientRenderer, PromptInput, Viewport,
    ratatui::{Terminal, backend::TestBackend},
};
fn picture(renderer: &mut ClientRenderer, views: &ClientViews, owner: &wire::Authority) -> String {
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
            file: TaskFile::Chunk { turn: 0, sequence },
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
            let task = crate::TaskId::from_bytes([11;16]); let owner = authority(task); let selected = query(task, 2);
            let frozen_task = crate::TaskId::from_bytes([12;16]);
            let mut generation = volume.snapshot(&Query::Root, 32).await.unwrap().generation;
            generation = volume.append(generation, &chunk(frozen_task, 0, b"frozen")).await.unwrap();
            let mut client = TaskFileClient::observe_query(&volume, &owner, selected.clone(), 2, 512).await.unwrap();
            let frozen_query = Query::Task(Component::new(frozen_task.to_string()).unwrap());
            client.views.query_snapshot(frozen_query.clone(), volume.snapshot(&frozen_query, 32).await.unwrap(), 32, 512).unwrap();
            let frozen_identity=client.views().selections().find(|(id,_)|**id!=client.task_selection).unwrap().0.to_owned();
            let frozen=client.views().selection(&frozen_identity).unwrap().clone();
            let mut renderer = ClientRenderer::default();
            for (index, text) in pieces.iter().enumerate() {
                generation = volume.append(generation, &chunk(task, index as u64, text.as_bytes())).await.unwrap();
                client.next_change().await.unwrap();
                let expected = pieces[index.saturating_sub(1)..=index].concat();
                prop_assert_eq!(joined_text(&client,&owner.id,false), expected.clone());
                let reopened = TaskFileClient::observe_query(&volume, &owner, selected.clone(), 2, 512).await.unwrap();
                same_files(selection(&client),selection(&reopened));
                prop_assert_eq!(client.views().selection(&frozen_identity).unwrap(), &frozen);
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
    let mut renderer = ClientRenderer::default();
    assert!(picture(&mut renderer, client.views(), &owner).contains("evicted"));
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
    same_files(selection(&client), selection(&reopened));
    let files = selection(&client);
    assert_eq!(files.files().len(), 1);
    assert!(files.files().contains_key(&FileKey::Task {
        task: Component::new(task.to_string()).unwrap(),
        file: TaskFile::Status,
    }));
    assert!(
        files
            .task(&Component::new(task.to_string()).unwrap())
            .text()
            .next()
            .is_none()
    );
    assert!(!picture(&mut renderer, client.views(), &owner).contains("evicted"));
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
    assert_eq!(joined_text(&client, &owner.id, false), expected);
    let reopened = TaskFileClient::observe_query(&volume, &owner, selected, 2, 512)
        .await
        .unwrap();
    same_files(selection(&client), selection(&reopened));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    // The real snapshot/follow -> ClientViews -> Markdown path parses each
    // stable UTF-8 byte once; rebuilding its cache is the negative control.
    #[test]
    fn followed_byte_partitions_parse_only_the_new_suffix(body in "[a-z日€🙂]{2,16}") {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let volume=TaskVolume::open(Arc::new(MemoryStream::default()),StreamPath::new("linear-markdown").unwrap()).await.unwrap();
            let task=crate::TaskId::from_bytes([43;16]);let owner=authority(task);
            let mut client=TaskFileClient::observe(&volume,&owner,128,4096).await.unwrap();
            let mut generation=volume.head().await.unwrap();
            let mut live=ClientRenderer::default();
            let mut redundant=0;
            for (index,byte) in body.bytes().enumerate() {
                generation=volume.append(generation,&chunk(task,index as u64,&[byte])).await.unwrap();
                client.next_change().await.unwrap();
                live.update(client.views(),&owner);
                let mut rebuilt=ClientRenderer::default();rebuilt.update(client.views(),&owner);
                redundant+=rebuilt.text_work();
                assert_eq!(live.screen().transcript,rebuilt.screen().transcript);
            }
            assert_eq!(live.text_work(),body.len());
            assert!(std::panic::catch_unwind(||assert_eq!(redundant,body.len())).is_err(),
                "replaying the prefix must fail the same exact work assertion");
        });
    }
}
