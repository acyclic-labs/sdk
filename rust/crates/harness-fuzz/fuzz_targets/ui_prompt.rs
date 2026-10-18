#![no_main]
use acyclic_harness as harness;
use acyclic_harness::{
    client::{CommandFileRoot, CommandSession, PromptFiles, restore_tui_prompt, tui_scroll},
    task_volume::{FileKey, Query, TaskVolume, Write},
    tui::{
        ClientRenderer, Editor, InputKeymap, PromptInput, WrappedLines,
        ratatui::text::{Line, Span},
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use libfuzzer_sys::fuzz_target;
#[path = "../../harness/tests/support/file_views.rs"]
mod file_views;
use std::sync::Arc;

fuzz_target!(|data: &[u8]| {
    if data.len() > 4096 {
        return;
    }
    // The canonical immutable command envelope supplies the same parsed row to
    // native and WASM. Valid generated envelopes must never disappear silently.
    let sequence = u64::from_le_bytes(std::array::from_fn(|index| {
        data.get(index).copied().unwrap_or(0)
    }))
    .max(1);
    let text = String::from_utf8_lossy(data);
    let mut outcome = Write::new(
        FileKey::parse(".tasks/t/executions/r/result.json").unwrap(),
        serde_json::to_vec(&serde_json::json!({"sequence":sequence,"ok":{"text":text}})).unwrap(),
    );
    let projected = acyclic_harness::client::CommandOutputView::from_file(&outcome).unwrap();
    assert_eq!(projected.sequence, sequence);
    assert_eq!(projected.text.as_deref(), Some(text.as_ref()));
    outcome.bytes = data.to_vec().into();
    if let Some(projected) = acyclic_harness::client::CommandOutputView::from_file(&outcome) {
        let raw: serde_json::Value = serde_json::from_slice(data).unwrap();
        assert_eq!(Some(projected.sequence), raw["sequence"].as_u64());
        if let Some(text) = projected.text {
            assert_eq!(raw["ok"]["text"].as_str(), Some(text.as_ref()));
        }
    }
    // Every batch is all-or-nothing even when a later command file is malformed.
    let mut menu = acyclic_harness::tui::CompletionMenu::default();
    let seed = br#"[{"name":"seed"}]"#;
    menu.set_file("commands/seed.json", Some(seed)).unwrap();
    let editor = Editor::new("/");
    menu.refresh(&editor);
    let before: Vec<_> = menu.items().cloned().collect();
    if menu
        .set_files([
            ("commands/seed.json", None),
            ("workspace/batch.rs", Some(&b""[..])),
            ("commands/fuzz.json", Some(data)),
        ])
        .is_err()
    {
        assert_eq!(menu.items().cloned().collect::<Vec<_>>(), before);
    }
    if let Ok(query) = serde_json::from_slice::<Query>(data) {
        let encoded = serde_json::to_vec(&query).unwrap();
        assert_eq!(serde_json::from_slice::<Query>(&encoded).unwrap(), query);
    }
    let operation = acyclic_harness::OperationId::from_bytes([9; 16]);
    if let Ok(request) =
        acyclic_harness::client::parse_local_request(&String::from_utf8_lossy(data), operation)
    {
        let canonical = acyclic_harness::client::jsonl::request(&request).unwrap();
        assert_eq!(
            acyclic_harness::client::parse_local_request(&canonical, operation).unwrap(),
            request
        );
    }
    let width = u16::from(data.first().copied().unwrap_or(0) % 80) + 1;
    let text = String::from_utf8_lossy(data);
    let visible = acyclic_harness::tui::display_text(&text);
    assert!(!visible.chars().any(|character| {
        (character.is_control() && character != '\t')
            || matches!(character, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
    }));
    assert_eq!(acyclic_harness::tui::display_text(&visible), visible);
    // Line::raw removes newlines; both partitions must retain the same bytes.
    let source = [Line::from(vec![Span::raw(text.as_ref())])];
    let mut wrapped = WrappedLines::default();
    wrapped.update(&source, 0, width);
    assert!(
        wrapped
            .rows()
            .iter()
            .all(|row| row.width() <= usize::from(width))
    );
    let middle = text
        .char_indices()
        .nth(text.chars().count() / 2)
        .map_or(text.len(), |(index, _)| index);
    let split = [Line::from(vec![
        Span::raw(&text[..middle]),
        Span::raw(&text[middle..]),
    ])];
    assert_eq!(
        source[0]
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>(),
        split[0]
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>()
    );
    let mut split_wrapped = WrappedLines::default();
    split_wrapped.update(&split, 0, width);
    assert_eq!(wrapped.rows(), split_wrapped.rows());
    let mut fields = data.splitn(5, |byte| *byte == 0);
    let draft = fields.next().unwrap_or_default();
    // Standalone JSON seeds must reach the result parser too; separators are
    // optional inputs for the independent prompt-file checks below.
    let history = fields.next().unwrap_or(b"[]");
    let keymap = fields.next();
    let notifications = fields.next();
    let layout = fields.next();
    let expected_keymap = keymap.map_or(Some(InputKeymap::default()), |bytes| {
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| InputKeymap::parse(text).ok())
    });
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            // Untrusted command outcome bytes use the production file fold.
            // Only positive-sequence successful text outcomes enter a transcript.
            let outcomes = TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("command-outcome-fuzz").unwrap(),
            )
            .await
            .unwrap();
            let outcome_owner = acyclic_harness::wire::Authority {
                kind: acyclic_harness::wire::AggregateKind::Task as i32,
                id: acyclic_harness::TaskId::from_bytes([8; 16]).to_string(),
            };
            let outcome = Write::new(
                FileKey::Task {
                    task: acyclic_harness::task_volume::Component::new(&outcome_owner.id).unwrap(),
                    file: acyclic_harness::task_volume::TaskFile::ExecutionResult(
                        acyclic_harness::task_volume::Component::new("outcome").unwrap(),
                    ),
                },
                data.to_vec(),
            );
            let cut = outcomes.snapshot(&Query::Tasks, 8).await.unwrap();
            outcomes.append(cut.generation, &outcome).await.unwrap();
            let folded = acyclic_harness::client::TaskFileClient::observe(
                &outcomes,
                &outcome_owner,
                8,
                8192,
            )
            .await
            .unwrap();
            let reopened = acyclic_harness::client::TaskFileClient::observe(
                &outcomes,
                &outcome_owner,
                8,
                8192,
            )
            .await
            .unwrap();
            assert_eq!(folded.views(), reopened.views());
            let volume = Arc::new(
                TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("prompt-fuzz").unwrap(),
                )
                .await
                .unwrap(),
            );
            // The same file client must reassemble arbitrary byte boundaries
            // before projecting UTF-8, then match reopening at the final cut.
            let owner = acyclic_harness::wire::Authority {
                kind: acyclic_harness::wire::AggregateKind::Task as i32,
                id: acyclic_harness::TaskId::from_bytes([7; 16]).to_string(),
            };
            let mut client =
                acyclic_harness::client::TaskFileClient::observe(volume.as_ref(), &owner, 16, 8192)
                    .await
                    .unwrap();
            let mut renderer = ClientRenderer::default();
            let mut cut = volume.snapshot(&Query::Tasks, 16).await.unwrap().generation;
            for (index, bytes) in [&data[..data.len() / 2], &data[data.len() / 2..]]
                .into_iter()
                .enumerate()
            {
                cut = volume
                    .append(
                        cut,
                        &Write {
                            publication: None,
                            key: FileKey::Task {
                                task: acyclic_harness::task_volume::Component::new(&owner.id)
                                    .unwrap(),
                                file: acyclic_harness::task_volume::TaskFile::Chunk {
                                    execution: acyclic_harness::OperationId::from_bytes([0; 16]),
                                    turn: 0,
                                    sequence: index as u64,
                                },
                            },
                            channel: Some(acyclic_harness::task_volume::Channel::Text),
                            bytes: bytes.to_vec().into(),
                        },
                    )
                    .await
                    .unwrap();
                client.next_change().await.unwrap();
                renderer.update(client.views(), &owner);
            }
            let replay =
                acyclic_harness::client::TaskFileClient::observe(volume.as_ref(), &owner, 16, 8192)
                    .await
                    .unwrap();
            let live = client.views().selections().next().unwrap().1;
            let reopened = replay.views().selections().next().unwrap().1;
            assert_eq!(live.files(), reopened.files());
            assert_eq!(live.cursor(), reopened.cursor());
            let mut rebuilt = ClientRenderer::default();
            rebuilt.update(replay.views(), &owner);
            assert_eq!(renderer.screen().transcript, rebuilt.screen().transcript);
            let task = acyclic_harness::task_volume::Component::new(&owner.id).unwrap();
            let actual: Vec<u8> = live
                .task(&task)
                .text()
                .flat_map(|text| {
                    text.file
                        .segments
                        .iter()
                        .flat_map(|bytes| bytes.iter().copied())
                })
                .collect();
            assert_eq!(actual, data);
            assert_eq!(String::from_utf8_lossy(&actual), text);
            let mut documents = file_views::empty();
            let plan = FileKey::Plan(acyclic_harness::task_volume::PlanName::new("old").unwrap());
            file_views::document(&mut documents, FileKey::Goal, "original");
            file_views::document(&mut documents, plan.clone(), "previous");
            file_views::put(&mut documents, FileKey::Goal, data.to_vec(), None);
            file_views::remove(&mut documents, plan.clone());
            let selection = documents.selections().next().unwrap().1;
            assert_eq!(selection.files()[&FileKey::Goal].segments[0].as_ref(), data);
            assert!(!selection.files().contains_key(&plan));
            // Result metadata is untrusted; it may describe status but must
            // never copy the result's output into the transcript a second time.
            file_views::tool(&mut documents, &owner.id, "bash", "");
            // Exercise the nonstreaming parser too; streaming output itself is
            // covered by the shared chunk/replay property and live renderer.
            file_views::remove(
                &mut documents,
                FileKey::parse(&format!(".tasks/{}/turns/0/tools/call/stdout/0", owner.id))
                    .unwrap(),
            );
            let result_key = FileKey::parse(&format!(
                ".tasks/{}/turns/0/tools/call/result.json",
                owner.id
            ))
            .unwrap();
            file_views::put(&mut documents, result_key.clone(), data.to_vec(), None);
            renderer.update(&documents, &owner);
            assert!(renderer.screen().transcript.is_empty());
            assert_eq!(renderer.screen().tools.len(), 1);
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) {
                // Raw duplicate fields may be rejected by the typed parser even
                // though Value keeps the last one. Check its canonical form
                // separately; the arbitrary original was already rendered above.
                file_views::put(
                    &mut documents,
                    result_key.clone(),
                    serde_json::to_vec(&value).unwrap(),
                    None,
                );
                renderer.update(&documents, &owner);
                let title = renderer.screen().tools[0].to_string();
                let valid = value.is_object()
                    && (value["success"].is_null() || value["success"].is_boolean())
                    && (value["exit_code"].is_null()
                        || value["exit_code"]
                            .as_i64()
                            .is_some_and(|code| i32::try_from(code).is_ok()));
                if valid
                    && (value["success"] == false
                        || value["exit_code"].as_i64().is_some_and(|code| code != 0))
                {
                    assert!(title.contains("failed"));
                } else if valid && value["success"] == true {
                    assert!(title.contains("completed"));
                }
            }
            // Details below must exercise the original untrusted bytes, not
            // only the canonical form used by the independent status oracle.
            file_views::put(&mut documents, result_key, data.to_vec(), None);
            let session = acyclic_harness::OperationId::from_bytes([9; 16]).to_string();
            let mut writes = vec![
                (format!(".ui/session-{session}/draft.json"), draft),
                (".ui/config/history.json".into(), history),
            ];
            if let Some(bytes) = keymap {
                writes.push((".ui/config/keymap.json".into(), bytes));
            }
            if let Some(bytes) = notifications {
                writes.push((format!(".ui/session-{session}/notifications.json"), bytes));
            }
            if let Some(bytes) = layout {
                writes.push((format!(".ui/session-{session}/layout.json"), bytes));
            }
            for (path, bytes) in writes {
                let cut = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
                volume
                    .append(
                        cut,
                        &Write::new(FileKey::parse(&path).unwrap(), bytes.to_vec()),
                    )
                    .await
                    .unwrap();
            }
            let Ok(mut files) =
                CommandSession::open(volume, session.parse().unwrap(), 65_536, 64).await
            else {
                return;
            };
            files
                .write_state(
                    CommandFileRoot::Config,
                    "settings.json",
                    &serde_json::json!({"tool_details":true}),
                )
                .await
                .unwrap();
            renderer.update(&documents, &owner);
            renderer.configure(&documents, &owner, &files).unwrap();
            assert!(renderer.screen().tools.len() <= 21);
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) {
                if value.as_object().is_some_and(|fields| {
                    fields.contains_key("stdout") || fields.contains_key("stderr")
                }) {
                    assert_eq!(renderer.screen().tools.len(), 1);
                }
            }
            if let Some(bytes) = layout {
                let expected = serde_json::from_slice::<serde_json::Value>(bytes)
                    .ok()
                    .and_then(|value| {
                        if !value.is_object() {
                            return None;
                        }
                        let modal = value.get("open_view").and_then(serde_json::Value::as_str)
                            == Some("shortcuts");
                        value
                            .get(if modal {
                                "view_back"
                            } else {
                                "transcript_back"
                            })
                            .map_or(Some(if modal { usize::MAX } else { 0 }), |value| {
                                value.as_u64().and_then(|rows| usize::try_from(rows).ok())
                            })
                    });
                assert_eq!(tui_scroll(&files).ok(), expected);
            }
            let mut renderer = ClientRenderer::default();
            let result = renderer.screen_for(&files);
            let expected: Result<Vec<String>, _> =
                serde_json::from_slice(notifications.unwrap_or(b"[]"));
            match expected {
                Err(_) => assert!(result.is_err()),
                Ok(messages) => {
                    let screen = result.unwrap();
                    assert_eq!(screen.notifications.len(), messages.len());
                    for (row, message) in screen.notifications.iter().zip(messages) {
                        let text: String =
                            row.spans.iter().map(|span| span.content.as_ref()).collect();
                        assert_eq!(text, message.escape_debug().to_string());
                        assert!(!text.chars().any(char::is_control));
                    }
                }
            }
            // The notification assertion above includes malformed files. Clear
            // that independent input before checking valid settings/help below.
            files
                .write_state(
                    CommandFileRoot::Session,
                    "notifications.json",
                    &serde_json::json!([]),
                )
                .await
                .unwrap();
            let mut input = PromptInput::new(Editor::new("original"));
            let mut projected = input.clone();
            let mut live = PromptFiles::default();
            let refreshed = live.refresh(&files, &mut projected);
            let restored = restore_tui_prompt(&files, &mut input);
            if restored.is_err() || expected_keymap.is_none() {
                assert!(refreshed.is_err());
                assert_eq!(projected.editor.text(), "original");
                assert!(projected.history().entries().is_empty());
                assert_eq!(projected.keymap, InputKeymap::default());
                return;
            }
            files
                .write_state(
                    CommandFileRoot::Session,
                    "layout.json",
                    &serde_json::json!({"open_view":"shortcuts"}),
                )
                .await
                .unwrap();
            if expected_keymap.is_some() && notifications.is_none() {
                let help = renderer.screen_for(&files).unwrap();
                assert!(!help.overlay.is_empty());
                for row in help.overlay {
                    assert!(
                        row.spans
                            .iter()
                            .all(|span| !span.content.chars().any(char::is_control))
                    );
                }
            }
            refreshed.unwrap();
            assert_eq!(projected.editor.text(), input.editor.text());
            assert_eq!(projected.history().entries(), input.history().entries());
            assert_eq!(Some(&projected.keymap), expected_keymap.as_ref());
            let generation = projected.history().generation();
            live.refresh(&files, &mut projected).unwrap();
            assert_eq!(projected.history().generation(), generation);
            live.save(&mut files, &mut projected).await.unwrap();
            let mut replayed = PromptInput::default();
            PromptFiles::default()
                .refresh(&files, &mut replayed)
                .unwrap();
            assert_eq!(replayed.editor.text(), projected.editor.text());
            assert_eq!(replayed.history().entries(), projected.history().entries());
            assert_eq!(replayed.keymap, projected.keymap);
            let selected = data.first().copied().unwrap_or(0) % 3;
            let reasoning = match selected {
                0 => serde_json::json!(false),
                1 => serde_json::json!(true),
                _ => serde_json::json!("invalid"),
            };
            files
                .write_state(
                    CommandFileRoot::Config,
                    "settings.json",
                    &serde_json::json!({"reasoning":reasoning.clone(),"tool_details":reasoning}),
                )
                .await
                .unwrap();
            let owner = acyclic_harness::wire::Authority {
                kind: acyclic_harness::wire::AggregateKind::Task as i32,
                id: "task".into(),
            };
            assert_eq!(
                renderer
                    .configure(&file_views::empty(), &owner, &files)
                    .is_ok(),
                selected != 2
            );
            assert_eq!(renderer.screen_for(&files).is_ok(), selected != 2);
        });
});
