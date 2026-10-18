use super::*;
use acyclic_harness::task_volume::{Channel, Component, FileKey, FileValue, TaskFile, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

struct NoFiles;
impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}
const REQUEST: &str = "Own every body row";

async fn catch_up(client: &mut TaskFileClient, volume: &TaskVolume<MemoryStream>) {
    let cut = volume.head().await.unwrap();
    while client
        .views()
        .selections()
        .any(|(_, view)| view.cursor().position < cut.position)
    {
        client.next_change().await.unwrap();
    }
}

async fn show(
    renderer: &mut ClientRenderer,
    client: &TaskFileClient,
    authority: &wire::Authority,
    commands: &CommandSession<MemoryStream>,
    expected: &[String],
) {
    renderer.update_files(client, authority);
    renderer
        .configure(client.views(), authority, commands)
        .unwrap();
    assert_painted(renderer, commands, expected).unwrap();
}

async fn reopen(
    volume: &TaskVolume<MemoryStream>,
    authority: &wire::Authority,
    selected: TaskId,
    commands: &CommandSession<MemoryStream>,
    expected: &[String],
) {
    let mut replay = TaskFileClient::observe_task(volume, authority, selected, 128, 65536)
        .await
        .unwrap();
    replay.follow_commands(commands).await.unwrap();
    show(
        &mut ClientRenderer::default(),
        &replay,
        authority,
        commands,
        expected,
    )
    .await;
}

async fn find(
    renderer: &mut ClientRenderer,
    commands: &mut CommandSession<MemoryStream>,
    query: &str,
    count: usize,
) {
    let mut layout = commands
        .state(CommandFileRoot::Session, "layout.json")
        .unwrap()
        .unwrap();
    layout["open_view"] = "find".into();
    layout["view_input"] = "find".into();
    layout["search_query"] = query.into();
    commands
        .write_state(CommandFileRoot::Session, "layout.json", &layout)
        .await
        .unwrap();
    let mut status = String::new();
    for _ in 0..MAXIMUM_SEARCH_STEPS {
        let painted = frame(renderer, commands, 120, 40).unwrap();
        status = painted
            .lines()
            .find_map(|row| row.trim().strip_prefix("Find: "))
            .unwrap_or_default()
            .to_owned();
        if !status.contains('\u{2026}') {
            break;
        }
        renderer.search_progress().await;
    }
    let (selected, total) = status
        .split_once(" matches")
        .and_then(|(counts, _)| counts.split_once('/'))
        .and_then(|(selected, total)| {
            Some((
                selected.parse::<usize>().ok()?,
                total.parse::<usize>().ok()?,
            ))
        })
        .expect("completed search exposes its selected index and exact total");
    assert_eq!(
        total, count,
        "each command identity has exactly one searchable transcript owner: {query}: {status}"
    );
    assert!(
        (1..=total).contains(&selected),
        "selected search result is in range: {status}"
    );
}

async fn ownership(bodies: &[String], result_first: bool) {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("answer-ownership").unwrap(),
        )
        .await
        .unwrap(),
    );
    let root = TaskId::from_bytes([91; 16]);
    let task = TaskId::from_bytes([92; 16]);
    let authority = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: root.to_string(),
    };
    let mut commands = CommandSession::open(
        volume.clone(),
        OperationId::new().to_string().parse().unwrap(),
        65536,
        128,
    )
    .await
    .unwrap();
    commands.submit(REQUEST, &NoFiles).await.unwrap();
    let request = commands.take_request().unwrap();
    let mut client = TaskFileClient::observe_task(volume.as_ref(), &authority, task, 128, 65536)
        .await
        .unwrap();
    client.follow_commands(&commands).await.unwrap();
    let mut renderer = ClientRenderer::default();
    let mut answer = String::new();
    show(
        &mut renderer,
        &client,
        &authority,
        &commands,
        &expected_transcript(REQUEST, ""),
    )
    .await;
    reopen(
        volume.as_ref(),
        &authority,
        task,
        &commands,
        &expected_transcript(REQUEST, ""),
    )
    .await;
    for (section, body) in bodies.iter().enumerate() {
        let text = format!("# Section {section}\n\nBody {section}: {body}.\n\n");
        answer.push_str(&text);
        let mut chunk = FileValue::new(
            FileKey::Task {
                task: Component::new(task.to_string()).unwrap(),
                file: TaskFile::Chunk {
                    turn: 0,
                    sequence: section as u64,
                },
            },
            text.into_bytes(),
        );
        chunk.channel = Some(Channel::Text);
        volume
            .append(volume.head().await.unwrap(), &chunk)
            .await
            .unwrap();
        catch_up(&mut client, volume.as_ref()).await;
        let expected = expected_transcript(REQUEST, &answer);
        // Every live prefix is checked before a canonical result exists.
        show(&mut renderer, &client, &authority, &commands, &expected).await;
        reopen(volume.as_ref(), &authority, task, &commands, &expected).await;
    }
    let status = FileValue::new(
        FileKey::Task {
            task: Component::new(task.to_string()).unwrap(),
            file: TaskFile::Status,
        },
        br#"{"state":"completed"}"#.to_vec(),
    );
    let expected = expected_transcript(REQUEST, &answer);
    if !result_first {
        volume
            .append(volume.head().await.unwrap(), &status)
            .await
            .unwrap();
        catch_up(&mut client, volume.as_ref()).await;
        show(&mut renderer, &client, &authority, &commands, &expected).await;
    }
    commands
        .consume(&request, |_, _, _| async {
            Ok(serde_json::json!({"task":task,"text":answer}))
        })
        .await
        .unwrap();
    catch_up(&mut client, volume.as_ref()).await;
    // The session now owns the answer while the same task/chunks are selected.
    show(&mut renderer, &client, &authority, &commands, &expected).await;
    reopen(volume.as_ref(), &authority, task, &commands, &expected).await;
    if result_first {
        volume
            .append(volume.head().await.unwrap(), &status)
            .await
            .unwrap();
        catch_up(&mut client, volume.as_ref()).await;
        show(&mut renderer, &client, &authority, &commands, &expected).await;
    }
    // Leave the producing task: only the durable session archive remains.
    client.follow_task(volume.as_ref(), root).await.unwrap();
    show(&mut renderer, &client, &authority, &commands, &expected).await;
    reopen(volume.as_ref(), &authority, root, &commands, &expected).await;
    for query in std::iter::once(REQUEST.to_owned()).chain(
        bodies
            .iter()
            .enumerate()
            .map(|(section, body)| format!("Body {section}: {body}.")),
    ) {
        find(&mut renderer, &mut commands, &query, 1).await;
    }
    let mut layout = commands
        .state(CommandFileRoot::Session, "layout.json")
        .unwrap()
        .unwrap();
    // Equal content is still two different admitted commands. A content-based
    // deduplication pass would erase the second prompt and answer here.
    layout["open_view"] = serde_json::Value::Null;
    layout["view_input"] = serde_json::Value::Null;
    commands
        .write_state(CommandFileRoot::Session, "layout.json", &layout)
        .await
        .unwrap();
    commands.submit(REQUEST, &NoFiles).await.unwrap();
    let second_request = commands.take_request().unwrap();
    assert_ne!(request.key, second_request.key);
    catch_up(&mut client, volume.as_ref()).await;
    let mut twice = expected.clone();
    twice.extend(expected_transcript(REQUEST, ""));
    show(&mut renderer, &client, &authority, &commands, &twice).await;
    reopen(volume.as_ref(), &authority, root, &commands, &twice).await;
    commands
        .consume(&second_request, |_, _, _| async {
            Ok(serde_json::json!({"task": TaskId::from_bytes([93;16]), "text":answer}))
        })
        .await
        .unwrap();
    catch_up(&mut client, volume.as_ref()).await;
    twice = [expected.clone(), expected].concat();
    show(&mut renderer, &client, &authority, &commands, &twice).await;
    reopen(volume.as_ref(), &authority, root, &commands, &twice).await;
    for query in std::iter::once(REQUEST.to_owned()).chain(
        bodies
            .iter()
            .enumerate()
            .map(|(section, body)| format!("Body {section}: {body}.")),
    ) {
        find(&mut renderer, &mut commands, &query, 2).await;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn every_body_has_one_live_final_archive_reopen_and_search_owner(
        bodies in prop::collection::vec("[a-z日€]{1,8}", 1..6),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(async {
                for result_first in [false, true] {
                    ownership(&bodies, result_first).await;
                }
            });
    }
}
