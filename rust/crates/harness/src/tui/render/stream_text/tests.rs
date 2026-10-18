use super::*;
use crate::task_volume::{Change, Component, FileKey, FileValue, TaskFile};
use proptest::prelude::*;

fn replaced_head(bodies: &[String], forget_prefix: bool) {
    let mut views = crate::test_support::empty_client_views();
    let task = Component::new("task").unwrap();
    let key = FileKey::Task {
        task: task.clone(),
        file: TaskFile::TextHead {
            execution: crate::OperationId::from_bytes([0; 16]),
            turn: 0,
            channel: Channel::Text,
        },
    };
    let mut decoder = StreamText::default();
    let mut rows = Vec::new();
    for body in bodies {
        let mut cursor = views.selections().next().unwrap().1.cursor().clone();
        cursor.position += 1;
        cursor.accepted = cursor.position;
        cursor.durable = cursor.position;
        views
            .query_change(Change {
                cursor,
                file: Some(FileValue {
                    requester: None,
                    key: key.clone(),
                    bytes: body.as_bytes().to_vec().into(),
                    channel: Some(Channel::Text),
                    publication: None,
                }),
                files: Vec::new(),
                removed: Vec::new(),
            })
            .unwrap();
        if forget_prefix {
            decoder.prefix = None;
        }
        let task = views.selections().next().unwrap().1.task(&task);
        let changed = decoder.update(&task, Channel::Text);
        rows.truncate(changed);
        rows.extend(decoder.rows(changed));
        let actual: String = rows
            .iter()
            .flat_map(|line| line.spans.iter())
            .map(|span| span.content.as_ref())
            .collect();
        assert_eq!(
            &actual, body,
            "a replaced logical head cannot retain old text"
        );
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn logical_head_replacement_matches_current_file(bodies in prop::collection::vec("[a-z日€🙂]{1,24}",1..12)) {
        replaced_head(&bodies,false);
    }
}
#[test]
fn lost_prefix_identity_negative_control() {
    let bodies = vec!["old".into(), "new".into()];
    replaced_head(&bodies, false);
    assert!(std::panic::catch_unwind(|| replaced_head(&bodies, true)).is_err());
}

fn bounded_tool(lines: usize, chunk: usize, broken: bool) {
    let text = (0..lines)
        .map(|index| format!("line{index}\n"))
        .collect::<String>();
    let mut actual = StreamText {
        tool_limit: 20,
        ..StreamText::default()
    };
    for bytes in text.as_bytes().chunks(chunk) {
        if broken {
            actual.push(bytes);
        } else {
            actual.push_tool(bytes);
        }
    }
    let mut baseline = Markdown::default();
    baseline.push(&text.lines().take(20).collect::<Vec<_>>().join("\n"));
    assert_eq!(actual.markdown.lines(), baseline.lines());
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn tool_output_bounds_source_lines_before_markdown(lines in 0usize..40, chunk in 1usize..32) {
        bounded_tool(lines,chunk,false);
    }
}
#[test]
fn markdown_row_cap_negative_control() {
    bounded_tool(21, 3, false);
    assert!(std::panic::catch_unwind(|| bounded_tool(21, 3, true)).is_err());
}

fn separate_entries(entries: &[String], broken: bool) {
    let mut actual = StreamText::default();
    let mut rows = Vec::new();
    let mut expected = Vec::new();
    let mut bytes = 0;
    for text in entries {
        bytes += text.len();
        let changed = if broken {
            if bytes == text.len() {
                actual.len()
            } else {
                actual.push(b"\n\n")
            }
        } else {
            actual.boundary()
        };
        let changed = changed.min(actual.push(text.as_bytes()));
        rows.truncate(changed);
        rows.extend(actual.rows(changed));
        // The former per-message renderer, initialized independently. A code
        // fence, emphasis or reference definition cannot capture another turn.
        let mut old = Markdown::default();
        old.push(text);
        let old: Vec<_> = old.lines().iter().map(line).collect();
        if !old.is_empty() {
            if !expected.is_empty() {
                expected.push(Line::default());
            }
            expected.extend(old);
        }
        assert_eq!(rows, expected);
        assert_eq!(actual.work, bytes);
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn markdown_turns_have_independent_block_state(entries in prop::collection::vec(
        prop_oneof![Just("```rust\nlet a = 1;".to_owned()),Just("next answer".to_owned()),
            Just("[ref]: /a\n\n[ref]".to_owned()),"[a-z日*# ]{1,16}"],1..8)) {
        separate_entries(&entries,false);
    }
}
#[test]
fn open_fence_cannot_capture_the_next_turn_negative_control() {
    let entries = vec!["```rust\nlet a = 1;".into(), "next answer".into()];
    separate_entries(&entries, false);
    assert!(std::panic::catch_unwind(|| separate_entries(&entries, true)).is_err());
}

fn publish(views: &mut crate::client::ClientViews, file: FileValue) {
    let mut cursor = views.selections().next().unwrap().1.cursor().clone();
    cursor.position += 1;
    cursor.accepted = cursor.position;
    cursor.durable = cursor.position;
    views
        .query_change(Change {
            cursor,
            file: Some(file),
            files: Vec::new(),
            removed: Vec::new(),
        })
        .unwrap();
}
fn decode_task(
    decoder: &mut StreamText,
    views: &crate::client::ClientViews,
    task: &Component,
    channel: Channel,
) {
    decoder.update(&views.selections().next().unwrap().1.task(task), channel);
}

async fn journal_chunk(
    volume: &crate::task_volume::TaskVolume<acyclic_stream::MemoryStream>,
    provider: &acyclic_stream::MemoryStream,
    task: Component,
    (execution, turn, channel): (u8, u64, Channel),
    bytes: &[u8],
    attempt: &str,
) -> acyclic_stream::wire::SessionData {
    use acyclic_stream::{
        CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey, StreamProvider,
    };
    let cut = volume.head().await.unwrap().position;
    let key = volume
        .next_chunk_key(
            task,
            crate::OperationId::from_bytes([execution; 16]),
            turn,
            None,
        )
        .await
        .unwrap();
    let prepared = volume
        .prepare_named_chunk(key, channel, bytes, None)
        .await
        .unwrap()
        .unwrap();
    let outcome = provider
        .commit(CommitRequest {
            conditions: vec![prepared.condition],
            mutations: vec![CommitMutation::Append {
                path: prepared.path,
                records: vec![prepared.record],
            }],
            idempotency_key: IdempotencyKey::new(attempt.as_bytes().to_vec()).unwrap(),
        })
        .await
        .unwrap();
    assert!(matches!(outcome, CommitOutcome::Committed(_)));
    use futures::StreamExt as _;
    let record = provider
        .read(acyclic_stream::ReadRequest {
            path: acyclic_stream::StreamPath::new("swarm/a/files").unwrap(),
            from: cut,
            limit: 1,
        })
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    let cursor = volume.heads(&Query::Root, None, 512).await.unwrap().cursor;
    acyclic_stream::wire::SessionData {
        after: Some(acyclic_stream::wire::SessionCursor {
            next: record.sequence + 1,
            ..Default::default()
        }),
        accepted: cursor.accepted,
        durable: cursor.durable,
        item: Some(acyclic_stream::wire::session_data::Item::Record(
            acyclic_stream::wire_codec::record_wire(record),
        )),
    }
}

fn head_views(snapshot: crate::task_volume::HeadSnapshot) -> crate::client::ClientViews {
    use crate::client::{
        ClientInterests, ClientViewLimits, ClientViews, QueryViewLimits, SwarmUpdate,
    };
    use acyclic_stream::StreamPath;
    let root = StreamPath::new("swarm").unwrap();
    let agent = StreamPath::new("swarm/a").unwrap();
    let mut views = ClientViews::new(
        root.clone(),
        ClientViewLimits {
            agents: 1,
            selected: 1,
            overview_values: 8,
            overview_bytes: 8192,
            query: QueryViewLimits {
                files: 512,
                segments: 1024,
                bytes: 65536,
            },
        },
    )
    .unwrap();
    views
        .apply(SwarmUpdate::Layout(
            ClientInterests::from_layout(root, [agent.clone()], [], 1).unwrap(),
        ))
        .unwrap();
    views
        .apply(SwarmUpdate::Snapshot { agent, snapshot })
        .unwrap();
    views
}

fn follow_head(views: &mut crate::client::ClientViews, data: acyclic_stream::wire::SessionData) {
    views
        .apply(crate::client::SwarmUpdate::Data {
            agent: acyclic_stream::StreamPath::new("swarm/a").unwrap(),
            overview: false,
            data,
        })
        .unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn interleaved_heads_render_identically_live_and_reopened(
        text in "[a-z日€🙂]{1,8}", reasoning in any::<bool>(),
        rest in prop::collection::vec(any::<bool>(), 0..12), batch in 1usize..5,
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            use crate::task_volume::TaskVolume;
            use acyclic_stream::{MemoryStream, StreamPath};
            let task_id=crate::TaskId::from_bytes([7;16]);
            let task=Component::new(task_id.to_string()).unwrap();
            let provider=Arc::new(MemoryStream::default());
            let volume=TaskVolume::open(provider.clone(),StreamPath::new("swarm/a/files").unwrap()).await.unwrap();
            let mut views=head_views(volume.heads(&Query::Root,None,512).await.unwrap());
            let channel=if reasoning {Channel::Reasoning} else {Channel::Text};
            let schedule:Vec<_>=[false,true,false,false,true].into_iter().chain(rest).collect();
            let mut live=StreamText::default();
            let mut broken=StreamText::default();
            let mut rows=Vec::new();
            for (index, execution) in schedule.iter().enumerate() {
                let data=journal_chunk(&volume,&provider,task.clone(),(if *execution {1}else{255},0,channel),text.as_bytes(),&index.to_string()).await;
                follow_head(&mut views,data);
                if index==3 || (index>4 && index%batch!=0 && index+1<schedule.len()) {continue;}
                let selection=views.selections().next().unwrap().1;
                let query=selection.task(&task);
                assert!(query.text().all(|text| matches!(text.file.key,FileKey::Task {file:TaskFile::TextHead {..},..})), "fixture must exercise materialized heads");
                let changed=live.update(&query,channel);
                rows.truncate(changed);
                rows.extend(live.rows(changed));
                // Restore the former last-head-only resume: pretend every
                // earlier-head update is already consumed by this reader.
                broken.cut=query.cursor.position;
                broken.update(&query,channel);
                let reopened=head_views(volume.reopen().await.unwrap().heads(&Query::Root,None,512).await.unwrap());
                let mut replay=StreamText::default();
                decode_task(&mut replay,&reopened,&task,channel);
                assert_eq!(rows,replay.rows(0),"interleaved prefix {index}");
                if index==2 {assert_ne!(broken.rows(0),rows,"last-head-only negative control escaped");}
            }
        });
    }
}
#[test]
fn restarted_execution_and_old_completion_preserve_stream_boundaries() {
    for channel in [Channel::Text, Channel::Reasoning] {
        let task = Component::new("task").unwrap();
        let mut views = crate::test_support::empty_client_views();
        let mut decoder = StreamText::default();
        for (execution, turn, bytes) in [(255, 0, b"old".as_slice()), (1, 1, &[0xe6])] {
            let mut file = FileValue::new(
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::Chunk {
                        execution: crate::OperationId::from_bytes([execution; 16]),
                        turn,
                        sequence: 0,
                    },
                },
                bytes.to_vec(),
            );
            file.channel = Some(channel);
            publish(&mut views, file);
            decode_task(&mut decoder, &views, &task, channel);
        }
        publish(
            &mut views,
            FileValue::new(
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::Completion(0),
                },
                b"{}".to_vec(),
            ),
        );
        decode_task(&mut decoder, &views, &task, channel);
        assert_eq!(
            decoder.pending_len, 1,
            "older completion froze the current turn"
        );
        assert!(
            decoder.work >= 3,
            "descending execution identity lost its stream"
        );
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(6))]
    #[test]
    fn reopening_completed_partial_utf8_preserves_the_frozen_tail(
        reasoning in any::<bool>(), suffix in prop::sample::select(vec![vec![0xe6u8],vec![0xc3],vec![0xf0,0x9f]]),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            use crate::{client::TaskFileClient, task_volume::TaskVolume};
            use acyclic_stream::{MemoryStream, StreamPath};
            let task_id=crate::TaskId::from_bytes([7;16]);
            let task=Component::new(task_id.to_string()).unwrap();
            let owner=crate::wire::Authority {kind:crate::wire::AggregateKind::Task as i32,id:task_id.to_string()};
            let volume=TaskVolume::open(Arc::new(MemoryStream::default()),StreamPath::new("completed").unwrap()).await.unwrap();
            let mut client=TaskFileClient::observe_task(&volume,&owner,task_id,32,4096).await.unwrap();
            let channel=if reasoning {Channel::Reasoning} else {Channel::Text};
            let mut file=FileValue::new(FileKey::Task {task:task.clone(),file:TaskFile::Chunk {
                turn:0,execution:crate::OperationId::from_bytes([2;16]),sequence:0}},suffix);
            file.channel=Some(channel);
            let cut=volume.append(volume.head().await.unwrap(),&file).await.unwrap();
            client.next_change().await.unwrap();
            let mut live=StreamText::default();
            decode_task(&mut live,client.views(),&task,channel);
            assert!(live.pending_len>0);
            let complete=FileValue::new(FileKey::Task {task:task.clone(),file:TaskFile::Completion(0)},b"{}".to_vec());
            volume.append(cut,&complete).await.unwrap();
            client.next_change().await.unwrap();
            decode_task(&mut live,client.views(),&task,channel);
            let reopened=TaskFileClient::observe_task(&volume.reopen().await.unwrap(),&owner,task_id,32,4096).await.unwrap();
            let mut replay=StreamText::default();
            decode_task(&mut replay,reopened.views(),&task,channel);
            assert_eq!(live.rows(0),replay.rows(0));
            assert_eq!(replay.pending_len,0);
            let mut broken=StreamText::default();
            for segment in &reopened.views().selections().next().unwrap().1.task(&task).text().next().unwrap().file.segments {broken.push(segment);}
            assert!(broken.pending_len>0,"omitted completion negative control escaped");
            assert_ne!(broken.pending_len,replay.pending_len);
        });
    }
}

#[test]
fn changed_head_lookup_has_exact_work_independent_of_unrelated_history() {
    use acyclic_test_support::{differential, measure};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for n in [64, 128] {
        let [baseline, candidate] = differential::pair::<_, _, bool>(
            || {
                runtime.block_on(async {
                    use crate::task_volume::TaskVolume;
                    use acyclic_stream::{MemoryStream, StreamPath};
                    let task_id = crate::TaskId::from_bytes([7; 16]);
                    let task = Component::new(task_id.to_string()).unwrap();
                    let other =
                        Component::new(crate::TaskId::from_bytes([8; 16]).to_string()).unwrap();
                    let provider = Arc::new(MemoryStream::default());
                    let volume = TaskVolume::open(
                        provider.clone(),
                        StreamPath::new("swarm/a/files").unwrap(),
                    )
                    .await
                    .unwrap();
                    let mut views =
                        head_views(volume.heads(&Query::Root, None, 512).await.unwrap());
                    for index in 0..n {
                        let data = journal_chunk(
                            &volume,
                            &provider,
                            if index % 2 == 0 {
                                task.clone()
                            } else {
                                other.clone()
                            },
                            (2, index as u64, Channel::Text),
                            b"noise",
                            &format!("noise-{index}"),
                        )
                        .await;
                        follow_head(&mut views, data);
                    }
                    for execution in [255, 1] {
                        let data = journal_chunk(
                            &volume,
                            &provider,
                            task.clone(),
                            (execution, 0, Channel::Text),
                            b"first",
                            &format!("initial-{execution}"),
                        )
                        .await;
                        follow_head(&mut views, data);
                    }
                    let selection = views.selections().next().unwrap().1;
                    let since = selection.cursor().position;
                    let last = selection
                        .task(&task)
                        .text()
                        .last()
                        .unwrap()
                        .file
                        .key
                        .clone();
                    assert!(matches!(
                        last,
                        FileKey::Task {
                            file: TaskFile::TextHead { .. },
                            ..
                        }
                    ));
                    let data = journal_chunk(
                        &volume,
                        &provider,
                        task.clone(),
                        (255, 0, Channel::Text),
                        b"extended",
                        "extend-a",
                    )
                    .await;
                    follow_head(&mut views, data);
                    (views, task, last, since)
                })
            },
            |(views, task, last, since), scan| {
                let selection = views.selections().next().unwrap().1;
                let query = selection.task(&task);
                let mut rewind = false;
                let (allocations, work) = crate::test_support::measure_task_volume_work(|| {
                    measure(|| {
                        if scan {
                            for _ in selection.files().values() {
                                crate::test_support::task_volume_item_examined();
                            }
                        }
                        rewind = query.text_changed_before(&last, since, Channel::Text);
                    })
                });
                differential::record(rewind);
                (
                    rewind,
                    work,
                    allocations.count_total,
                    allocations.bytes_total,
                )
            },
        );
        // pair calls the old branch first: it deliberately restores the scan.
        assert_eq!(baseline.1, candidate.1);
        assert!(baseline.0.0 && candidate.0.0);
        assert_eq!((candidate.0.1, candidate.0.2, candidate.0.3), (2, 0, 0));
        assert_eq!(baseline.0.1, n + 4);
        assert_ne!(
            baseline.0.1, candidate.0.1,
            "full-history scan negative escaped"
        );
    }
}

#[test]
fn scoped_stream_resume_has_exact_work_and_allocations_with_scan_negative() {
    use acyclic_test_support::{differential, measure};
    for n in [64, 128] {
        let [baseline, candidate] = differential::pair::<_, _, (FileKey, Vec<u8>)>(
            || {
                let mut views = crate::test_support::empty_client_views();
                for index in 0..n {
                    let mut file = FileValue::new(
                        FileKey::Task {
                            task: Component::new(format!("unrelated-{index}")).unwrap(),
                            file: TaskFile::Chunk {
                                execution: crate::OperationId::from_bytes([1; 16]),
                                turn: 0,
                                sequence: 0,
                            },
                        },
                        b"noise".to_vec(),
                    );
                    file.channel = Some(Channel::Text);
                    publish(&mut views, file);
                }
                for execution in [1, 2] {
                    let mut file = FileValue::new(
                        FileKey::Task {
                            task: Component::new("target").unwrap(),
                            file: TaskFile::Chunk {
                                execution: crate::OperationId::from_bytes([execution; 16]),
                                turn: 0,
                                sequence: 0,
                            },
                        },
                        vec![execution],
                    );
                    file.channel = Some(Channel::Text);
                    publish(&mut views, file);
                }
                views
            },
            |views, scan| {
                let task = Component::new("target").unwrap();
                let selection = views.selections().next().unwrap().1;
                let query = selection.task(&task);
                let from = query.text().next().unwrap().file.key.clone();
                let mut output = Vec::new();
                let (allocations, work) = crate::test_support::measure_task_volume_work(|| {
                    measure(|| {
                        output = if scan {
                            selection.files().values().inspect(|_|crate::test_support::task_volume_item_examined())
                        .filter(|file| matches!(&file.key,FileKey::Task {task:found,file:TaskFile::Chunk {..},..} if found==&task)).collect()
                        } else {
                            query.text_from(Some(&from)).map(|text| text.file).collect()
                        };
                    })
                });
                let bytes: Vec<_> = output
                    .iter()
                    .map(|file| {
                        let bytes: Vec<_> = file
                            .segments
                            .iter()
                            .flat_map(|part| part.iter().copied())
                            .collect();
                        differential::record((file.key.clone(), bytes.clone()));
                        bytes
                    })
                    .collect();
                (
                    bytes,
                    work,
                    allocations.count_total,
                    allocations.bytes_total,
                )
            },
        );
        assert_eq!(baseline.0.0, candidate.0.0);
        assert_eq!(baseline.1, candidate.1);
        assert_eq!(
            candidate.0.1, 5,
            "one anchor plus two scoped positions and two files"
        );
        assert_eq!(
            candidate.0.2, 1,
            "only the caller's output vector allocates"
        );
        assert_eq!(
            candidate.0.3,
            4 * std::mem::size_of::<&crate::task_volume::HeadFile>() as u64
        );
        assert_eq!(baseline.0.1, n + 2, "full-scan negative control escaped");
        assert_ne!(baseline.0.1, candidate.0.1);
    }
}

#[test]
fn descending_executions_in_one_turn_keep_their_text_boundary() {
    let task = Component::new("task").unwrap();
    let mut views = crate::test_support::empty_client_views();
    let mut live = StreamText::default();
    let mut combined = StreamText::default();
    for (execution, bytes) in [(255, b"first".as_slice()), (1, b"second".as_slice())] {
        let mut file = FileValue::new(
            FileKey::Task {
                task: task.clone(),
                file: TaskFile::Chunk {
                    execution: crate::OperationId::from_bytes([execution; 16]),
                    turn: 0,
                    sequence: 0,
                },
            },
            bytes.to_vec(),
        );
        file.channel = Some(Channel::Text);
        publish(&mut views, file);
        decode_task(&mut live, &views, &task, Channel::Text);
        combined.push(bytes);
    }
    let rows: Vec<_> = live.rows(0).iter().map(ToString::to_string).collect();
    assert_eq!(rows, ["first", "", "second"]);
    assert_ne!(
        combined.rows(0),
        live.rows(0),
        "execution-blind decoder escaped"
    );
}
