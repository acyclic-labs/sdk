//! tui-parity.md section 1: real consumers, including snapshot/follow updates.
use super::*;
use acyclic_harness::{
    client::{CommandFileRoot as Root, CommandSession, CommandViewFiles},
    task_volume::{FileKey, FileValue, Query, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::{FutureExt as _, StreamExt as _};
use proptest::prelude::*;
use std::sync::Arc;

async fn volume() -> Arc<TaskVolume<MemoryStream>> {
    Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("command-views").unwrap(),
        )
        .await
        .unwrap(),
    )
}
async fn put(volume: &TaskVolume<MemoryStream>, path: &str, text: &str) {
    let key = FileKey::parse(path).unwrap();
    let cut = volume
        .snapshot(&Query::File(key.clone()), 1)
        .await
        .unwrap()
        .generation;
    let mut file = FileValue::new(key, text.as_bytes().to_vec());
    if matches!(
        file.key,
        FileKey::Task {
            file: acyclic_harness::task_volume::TaskFile::Chunk { .. },
            ..
        }
    ) {
        file.channel = Some(if text == "private reasoning" {
            acyclic_harness::task_volume::Channel::Reasoning
        } else {
            acyclic_harness::task_volume::Channel::Text
        });
    }
    volume.append(cut, &file).await.unwrap();
}
async fn client(volume: Arc<TaskVolume<MemoryStream>>) -> CommandSession<MemoryStream> {
    CommandSession::open(volume, uuid::Uuid::from_bytes([9; 16]), 65_536, 128)
        .await
        .unwrap()
}

#[tokio::test]
async fn catalog_edit_conflicts_preserve_the_winning_bytes() -> Result<()> {
    let volume = volume().await;
    let mut stale = client(volume.clone()).await;
    let mut winner = client(volume.clone()).await;
    // Advance stale to the common cut, then compute two independent edits.
    let original: Value =
        serde_json::from_slice(acyclic_harness::client::DEFAULT_COMMANDS_JSON).unwrap();
    let mut a = original.clone();
    a[0]["description"] = json!("stale edit");
    let mut b = original;
    b[0]["description"] = json!("winning edit");
    winner
        .install_catalog(serde_json::to_vec(&b).unwrap())
        .await?;
    assert!(matches!(
        stale.install_catalog(serde_json::to_vec(&a).unwrap()).await,
        Err(Error::Conflict(_))
    ));
    let key = FileKey::parse("commands/catalog.json").unwrap();
    let saved = volume
        .snapshot(&Query::File(key), 1)
        .await
        .map_err(volume_error)?;
    assert_eq!(
        serde_json::from_slice::<Value>(&saved.files[0].bytes).unwrap(),
        b
    );
    // Conflict advances the fold; the user can explicitly review and reapply.
    assert_eq!(
        stale
            .catalog()
            .commands()
            .find(|spec| spec.name == "compact")
            .unwrap()
            .description,
        "winning edit"
    );
    Ok(())
}

#[cfg(feature = "test-support")]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn root_pages_visit_only_the_page_and_cover_each_key_once(count in 1_usize..90, page_size in 1_usize..12) {
        futures::executor::block_on(async {
            let volume = volume().await;
            for i in 0..count { put(&volume, &format!(".ui/archive-{i:04}/settings.json"), "{}").await; }
            let mut after = None;
            let mut keys = Vec::new();
            loop {
                let (page, work) = acyclic_harness::test_support::measure_task_volume_work(|| volume.page(after.as_ref(), page_size).now_or_never().expect("in-memory page is ready"));
                let page = page.unwrap().expect("the writer's index is current");
                assert!(page.len() <= page_size);
                assert_eq!(work, 1 + page.len() as u64, "one anchor plus exactly the returned entries");
                if page.is_empty() { break; }
                after = page.last().map(|file| file.key.clone());
                keys.extend(page.into_iter().map(|file| file.key));
            }
            assert_eq!(keys.len(), count);
            assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
            let provider = Arc::new(MemoryStream::default());
            let path = StreamPath::new("cold-page-index").unwrap();
            let writer = TaskVolume::open(provider.clone(), path.clone()).await.unwrap();
            let reader = TaskVolume::open(provider, path).await.unwrap();
            for i in 0..count { put(&writer, &format!(".ui/archive-{i:04}/settings.json"), "{}").await; }
            let mut after = None;
            let mut cold_keys = Vec::new();
            let mut steps = 0;
            loop {
                steps += 1;
                assert!(steps <= 2 * count + 2, "replay and enumeration must make progress");
                let (page, work) = acyclic_harness::test_support::measure_task_volume_work(|| reader.page(after.as_ref(), page_size).now_or_never().unwrap());
                // Each replay record costs one record visit, one validation
                // lookup and one insertion; enumeration costs one per row.
                assert!(work <= 4 * page_size as u64 + 1, "unbounded stale-index replay: {work}");
                let Some(page) = page.unwrap() else { continue; };
                if page.is_empty() { break; }
                after = page.last().map(|file| file.key.clone());
                cold_keys.extend(page.into_iter().map(|file| file.key));
            }
            assert_eq!(cold_keys, keys);
            assert!(volume.page(None, 0).await.is_err());
        });
    }
}

#[tokio::test]
async fn parity_rows_consume_selected_files_and_follow_live_updates() -> Result<()> {
    let volume = volume().await;
    let mut client = client(volume.clone()).await;
    let task = acyclic_harness::TaskId::from_bytes([1; 16]);
    client
        .write_state(Root::Session, "layout.json", &json!({"selected_task":task}))
        .await?;
    client
        .write_state(
            Root::Task,
            "settings.json",
            &json!({"agent_mode":"code","agent_modes":["code","plan"]}),
        )
        .await?;
    let base = format!(".tasks/{task}");
    for (leaf, body) in [
        (
            "turns/0/chunks/0",
            "visible transcript
find the needle",
        ),
        ("turns/0/chunks/1", "private reasoning"),
        (
            "turns/0/tools/call/request.json",
            r#"{"command":"details marker"}"#,
        ),
        ("turns/0/tools/call/stdout/0", "terminal marker"),
        ("status.json", r#"{"state":"running"}"#),
    ] {
        put(&volume, &format!("{base}/{leaf}"), body).await;
    }
    // Bring the command follow to the content cut before resolving settings.
    assert!(matches!(
        client.submit("/reason", &DeniedReader).await,
        Err(Error::Conflict(_))
    ));
    client.submit("/reason", &DeniedReader).await?;
    let (mut documents, mut changes) =
        CommandViewFiles::observe(volume.as_ref(), &client.view_queries()?, 128, 65_536).await?;
    let screen = client.render_view(&documents)?;
    assert!(screen.contains("visible transcript") && screen.contains("private reasoning"));
    assert!(!screen.contains("details marker") && !screen.contains("terminal marker"));
    // Toggle rows from tui-parity.md: reasoning, details, terminal, tree, diff, mode.
    for (input, expected, absent) in [
        ("/reason", "visible transcript", "private reasoning"),
        ("/detail", "details marker", "private reasoning"),
        ("/term", "terminal marker", "private reasoning"),
        ("/agents", "task ", "private reasoning"),
        ("/view", "diff-mode=unified", "private reasoning"),
        ("/m", "mode=plan", "private reasoning"),
    ] {
        client.submit(input, &DeniedReader).await?;
        if !documents.selects(&client.view_queries()?) {
            (documents, changes) =
                CommandViewFiles::observe(volume.as_ref(), &client.view_queries()?, 128, 65_536)
                    .await?;
        }
        let screen = client.render_view(&documents)?;
        assert!(screen.contains(expected), "{input}: {screen}");
        assert!(!screen.contains(absent), "{input}: {screen}");
    }
    client.submit("/f", &DeniedReader).await?;
    assert!(
        client
            .render_view(&documents)?
            .contains("Find in transcript")
    );
    client.submit("NEEDLE", &DeniedReader).await?;
    let screen = client.render_view(&documents)?;
    assert!(screen.contains("find the needle") && !screen.contains("visible transcript"));
    assert!(
        client.take_request().is_none(),
        "view input became a model request"
    );
    put(&volume, &format!("{base}/turns/1/chunks/0"), "later needle").await;
    // Cursor checkpoints precede the one matching publication.
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            documents.apply(changes.next().await.unwrap().map_err(volume_error)?)?;
            if client.render_view(&documents)?.contains("later needle") {
                break;
            }
        }
        Ok::<_, Error>(())
    })
    .await
    .expect("the existing follow must wake")?;
    assert!(client.render_view(&documents)?.contains("later needle"));
    client.observe_change().await?;
    client.submit("/bug", &DeniedReader).await?;
    assert!(client.render_view(&documents)?.contains("Feedback —"));
    client.submit("a local report", &DeniedReader).await?;
    assert!(
        client
            .render_view(&documents)?
            .contains("Feedback saved locally: a local report")
    );
    client
        .write_state(Root::Config, "themes.json", &json!(["Light", "Dark"]))
        .await?;
    client.submit("/theme unknown", &DeniedReader).await?;
    assert!(client.render_view(&documents)?.contains("Theme picker"));
    client.submit("Dark", &DeniedReader).await?;
    assert!(client.render_view(&documents)?.contains("theme=Dark"));
    client.submit("/model chosen", &DeniedReader).await?;
    client.submit("/model", &DeniedReader).await?;
    assert!(client.render_view(&documents)?.contains("Model: chosen"));
    let other = acyclic_harness::TaskId::from_bytes([2; 16]);
    client
        .write_state(
            Root::Session,
            "attention.json",
            &json!({"tasks":[other,task]}),
        )
        .await?;
    client.submit("/next", &DeniedReader).await?;
    assert_eq!(client.selected_task()?, Some(other));
    client.submit("/att", &DeniedReader).await?;
    assert_eq!(client.selected_task()?, Some(task));
    client.submit("/keys", &DeniedReader).await?;
    assert!(
        client
            .render_view(&documents)?
            .contains("/shortcuts — Show shortcuts (help, keys)")
    );
    Ok(())
}

#[test]
fn diff_parity_preference_changes_the_actual_endpoint_renderer() -> Result<()> {
    use acyclic_harness::client::render_command_diff;
    let unified = render_command_diff(b"old\n", b"new\n", "unified", 1024, 4096)?;
    let automatic = render_command_diff(b"old\n", b"new\n", "auto", 1024, 4096)?;
    let split = render_command_diff(b"old\n", b"new\n", "split", 1024, 4096)?;
    assert_eq!(automatic, unified);
    assert!(unified.contains("-old") && unified.contains("+new"));
    assert!(split.contains("old | new"));
    assert!(render_command_diff(b"old\n", b"new\n", "bad", 1024, 4096).is_err());
    assert!(render_command_diff(b"old\n", b"new\n", "split", 4, 4096).is_err());
    Ok(())
}

#[cfg(feature = "tui")]
#[tokio::test]
async fn parity_view_rows_reach_the_terminal_backend() -> Result<()> {
    use acyclic_harness::tui::ratatui::{Terminal, backend::TestBackend, widgets::Paragraph};
    let volume = volume().await;
    let mut client = client(volume).await;
    for (command, row) in [
        ("/help", "/quit"),
        ("/find", "Find in transcript"),
        ("/bug", "Feedback —"),
        ("/model", "Model:"),
        ("/theme unknown", "Theme picker"),
        ("/mcp", "Registered tools"),
    ] {
        client.submit(command, &DeniedReader).await?;
        let rendered = client.render_view(&CommandViewFiles::default())?;
        let mut terminal = Terminal::new(TestBackend::new(120, 64)).unwrap();
        terminal
            .draw(|frame| frame.render_widget(Paragraph::new(rendered), frame.area()))
            .unwrap();
        let screen: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(screen.contains(row), "{command}: {screen}");
    }
    Ok(())
}

async fn source_conflict_case(description: &str, alias: &str) -> Result<()> {
    use acyclic_harness::client::CommandSources;
    let volume = volume().await;
    let mut client = client(volume.clone()).await;
    let mut sources = CommandSources::observe(
        &mut client,
        acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(),
        vec![],
        vec![],
        Arc::new(DeniedReader),
        Default::default(),
        65_536,
        128,
    )
    .await?;
    put(&volume, ".codex/prompts/added.md", "Added $ARGUMENTS").await;
    sources.changed().await?;
    let mut winner =
        CommandSession::open(volume.clone(), uuid::Uuid::from_bytes([9; 16]), 65_536, 128).await?;
    let mut manual: Value =
        serde_json::from_slice(acyclic_harness::client::DEFAULT_COMMANDS_JSON).unwrap();
    manual[0]["description"] = json!(description);
    manual[0]["aliases"] = json!([alias]);
    winner
        .install_catalog(serde_json::to_vec(&manual).unwrap())
        .await?;
    assert!(matches!(
        sources.refresh(&mut client).await,
        Err(Error::Conflict(_))
    ));
    let saved = volume
        .snapshot(
            &Query::File(FileKey::parse("commands/catalog.json").unwrap()),
            1,
        )
        .await
        .map_err(volume_error)?;
    assert_eq!(
        serde_json::from_slice::<Value>(&saved.files[0].bytes).unwrap(),
        manual
    );
    // The ordinary catalog follow supplies the new base; the next source
    // publication preserves the edit rather than retrying stale computed bytes.
    sources.changed().await?;
    sources.refresh(&mut client).await?;
    assert!(client.catalog().commands().any(|spec| spec.name == "added"));
    let compact = client
        .catalog()
        .commands()
        .find(|spec| spec.name == "compact")
        .unwrap();
    assert_eq!(compact.description, description);
    assert_eq!(compact.aliases, [alias]);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn a_source_refresh_reports_the_catalog_conflict_and_rebases_before_retry(description in "Manual [a-z]{1,24}", alias in "alias-[a-z]{1,8}") {
        futures::executor::block_on(source_conflict_case(&description, &alias)).unwrap();
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn feedback_text_round_trips_without_becoming_a_model_request(text in "[^/]{0,160}") {
        futures::executor::block_on(async {
            let mut client = client(volume().await).await;
            client.submit("/feedback", &DeniedReader).await.unwrap();
            client.submit(&text, &DeniedReader).await.unwrap();
            let saved = client.state(Root::Session, "feedback.json").unwrap().unwrap();
            prop_assert_eq!(saved["text"].as_str(), Some(text.as_str()));
            prop_assert!(client.take_request().is_none());
            client.submit("/feedback", &DeniedReader).await.unwrap();
            prop_assert!(client.submit(&"x".repeat(65_537), &DeniedReader).await.is_err());
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn renamed_theme_action_accepts_picker_input() -> Result<()> {
    let volume = volume().await;
    let mut client = client(volume).await;
    let mut catalog: Value =
        serde_json::from_slice(acyclic_harness::client::DEFAULT_COMMANDS_JSON).unwrap();
    let theme = catalog
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|spec| spec["name"] == "theme")
        .unwrap();
    theme["name"] = json!("palette");
    theme["aliases"] = json!([]);
    client
        .install_catalog(serde_json::to_vec(&catalog).unwrap())
        .await?;
    client
        .write_state(Root::Config, "themes.json", &json!(["Light", "DARK"]))
        .await?;
    client.submit("/palette", &DeniedReader).await?;
    client.submit("dark", &DeniedReader).await?;
    assert_eq!(
        client.state(Root::Config, "theme.json")?.unwrap()["name"],
        "DARK"
    );
    assert!(client.take_request().is_none());
    Ok(())
}

proptest! {
    #[test]
    fn recent_task_windows_remain_bounded_through_snapshot_and_follow(count in 1usize..32, recent in 1u32..6, budget in 8usize..96) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            use acyclic_harness::task_volume::{Component, TaskFile};
            let volume=volume().await;
            let task=Component::new("00000000-0000-0000-0000-000000000001").unwrap();
            let query=Query::TaskView { task:task.clone(), recent, reasoning:false,details:false,terminal:false };
            let add=|sequence| {
                let mut file = FileValue::new(FileKey::Task { task:task.clone(), file:TaskFile::Chunk { execution: acyclic_harness::OperationId::from_bytes([0; 16]), turn:0,sequence } }, vec![b'x'; 8]);
                file.channel = Some(acyclic_harness::task_volume::Channel::Text);
                file
            };
            for sequence in 0..count as u64 {
                let file=add(sequence);
                let cut=volume.snapshot(&Query::File(file.key.clone()),1).await.unwrap().generation;
                volume.append(cut,&file).await.unwrap();
            }
            let (mut view, mut changes)=CommandViewFiles::observe(volume.as_ref(),&[query.clone()],16,budget).await.unwrap();
            for sequence in count as u64..count as u64+8 {
                let file=add(sequence);
                let cut=volume.snapshot(&Query::File(file.key.clone()),1).await.unwrap().generation;
                volume.append(cut,&file).await.unwrap();
                view.apply(changes.next().await.unwrap().unwrap()).unwrap();
                let selected=view.0[0].files();
                prop_assert!(selected.len() <= recent as usize);
                prop_assert!(selected.values().map(|file| file.length as usize).sum::<usize>() <= budget);
                prop_assert!(selected.keys().all(|key| query.matches(key)));
                prop_assert!(selected.contains_key(&file.key));
            }
            let encoded=serde_json::to_vec(&query).unwrap();
            prop_assert_eq!(serde_json::from_slice::<Query>(&encoded).unwrap(),query);
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn hidden_archived_results_do_not_fill_the_selected_task_view() -> Result<()> {
    let volume = volume().await;
    let mut client = client(volume.clone()).await;
    let task = acyclic_harness::TaskId::from_bytes([3; 16]);
    client
        .write_state(Root::Session, "layout.json", &json!({"selected_task":task}))
        .await?;
    for id in 0..48u8 {
        let operation = uuid::Uuid::from_bytes([id; 16]);
        let path = format!(".tasks/{task}/executions/{operation}/result.json");
        put(
            &volume,
            &path,
            &serde_json::to_string(
                &json!({"sequence":u64::from(id)+1,"ok":{"stdout":"x".repeat(4096)}}),
            )
            .unwrap(),
        )
        .await;
    }
    put(
        &volume,
        &format!(".tasks/{task}/turns/0/chunks/0"),
        "visible model text",
    )
    .await;
    let queries = client.view_queries()?;
    let (mut view, mut follows) =
        CommandViewFiles::observe(volume.as_ref(), &queries, 128, 65_536).await?;
    assert!(client.render_view(&view)?.contains("visible model text"));
    put(
        &volume,
        &format!(
            ".tasks/{task}/executions/{}/result.json",
            uuid::Uuid::from_bytes([55; 16])
        ),
        &serde_json::to_string(&json!({"sequence":55,"ok":{"stdout":"y".repeat(4096)}})).unwrap(),
    )
    .await;
    view.apply(follows.next().await.unwrap().map_err(volume_error)?)?;
    assert!(client.render_view(&view)?.contains("visible model text"));
    assert!(view.0.iter().all(|fold| {
        fold.files()
            .values()
            .map(|file| file.length as usize)
            .sum::<usize>()
            <= 65_536
    }));
    Ok(())
}

#[cfg(feature = "test-support")]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn recent_command_snapshot_work_ignores_other_session_history(foreign in 0usize..96, own in 0usize..12, foreign_pending in 1usize..96, own_pending in 0usize..8, claimed in any::<bool>(), recent in 1u32..8, bytes in any::<[u8;16]>()) {
        futures::executor::block_on(async {
            use acyclic_harness::task_volume::{Component, TaskFile};
            let volume = volume().await;
            let session = uuid::Uuid::from_bytes(bytes);
            let mut other = bytes; other[0] ^= 1;
            let other = uuid::Uuid::from_bytes(other);
            let task = Component::new(uuid::Uuid::from_bytes([7;16]).to_string()).unwrap();
            let mut ours = Vec::new();
            for (owner, count) in [(other,foreign),(session,own)] {
                for sequence in 0..count {
                    let key = FileKey::Task { task:task.clone(), file:TaskFile::ExecutionResult(Component::new(format!("command-{owner}.{}", uuid::Uuid::from_u128(sequence as u128+1))).unwrap()) };
                    let cut = volume.snapshot(&Query::File(key.clone()),1).await.unwrap().generation;
                    volume.append(cut,&FileValue::new(key.clone(),serde_json::to_vec(&json!({"sequence":cut.position,"ok":{}})).unwrap())).await.unwrap();
                    if owner == session { ours.push(key); }
                }
            }
            let mut pending = std::collections::BTreeSet::new();
            for (owner,count) in [(other,foreign_pending),(session,own_pending)] {
                for sequence in 0..count {
                    let id = Component::new(format!("command-{owner}.{}", uuid::Uuid::from_u128(sequence as u128+1_000))).unwrap();
                    let mut files = vec![TaskFile::Request(id.clone())];
                    if claimed { files.push(TaskFile::ExecutionClaim(id)); }
                    for file in files {
                        let key = FileKey::Task { task:task.clone(),file };
                        let cut = volume.snapshot(&Query::File(key.clone()),1).await.unwrap().generation;
                        volume.append(cut,&FileValue::new(key.clone(),b"{}".to_vec())).await.unwrap();
                        if owner == session { pending.insert(key); }
                    }
                }
            }
            let query = Query::CommandSession { session:Component::new(session.to_string()).unwrap(),recent };
            let (snapshot,work) = acyclic_harness::test_support::measure_task_volume_work(|| volume.snapshot(&query,64).now_or_never().unwrap());
            let selected: std::collections::BTreeSet<_> = snapshot.unwrap().files.into_iter().map(|file| file.key).collect();
            let mut expected = pending.clone();
            expected.extend(ours.into_iter().rev().take(recent as usize));
            prop_assert_eq!(selected,expected);
            // One history anchor, two catalog lookups, and exactly three
            // accesses per selected outcome (ordered index, outcome, request).
            prop_assert!(work <= 3 + 3 * u64::from(recent) + 2 * pending.len() as u64, "foreign={foreign} foreign_pending={foreign_pending} own={own} recent={recent} work={work}");
            let mut fresh=bytes; fresh[0] ^= 2;
            let query = Query::CommandSession { session:Component::new(uuid::Uuid::from_bytes(fresh).to_string()).unwrap(),recent };
            let (snapshot,work) = acyclic_harness::test_support::measure_task_volume_work(|| volume.snapshot(&query,64).now_or_never().unwrap());
            prop_assert!(snapshot.unwrap().files.is_empty());
            prop_assert_eq!(work,3,"a fresh terminal must not walk another terminal's requests, claims or outcomes");
            Ok(())
        })?;
    }
}
