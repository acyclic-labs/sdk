use super::*;
use proptest::prelude::*;
fn chunk(task: &str, sequence: u64, bytes: &[u8]) -> FileValue {
    FileValue {
        requester: None,
        publication: None,
        key: FileKey::Task {
            task: crate::task_volume::Component::new(task).unwrap(),
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
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Real fold work is linear in new segment bytes, independent of retained
    // tasks. Removing the cursor (reopening every prefix) violates the bound.
    #[test]
    fn appended_segments_have_linear_work_and_completed_tasks_are_frozen(
        pieces in prop::collection::vec("[a-z]{8,16}", 8..32),
    ) {
        let mut views = TaskFileViews::default();
        let mut work = 0;
        let mut broken = 0;
        let mut bytes = 0;
        for (index, text) in pieces.iter().enumerate() {
            let file = chunk("active", index as u64, text.as_bytes());
            work += append(&mut views, &file);
            bytes += text.len();
            prop_assert_eq!(work, bytes);
            prop_assert_eq!(append(&mut views, &file), 0, "cursor must reject replay");
            let mut reopened = TaskFileViews::default();
            for (sequence, prior) in pieces[..=index].iter().enumerate() {
                broken += append(&mut reopened, &chunk("active", sequence as u64, prior.as_bytes()));
            }
        }
        prop_assert!(broken > bytes * 2, "negative control must exceed linear work bound");
        let completed = FileValue::new(FileKey::Task {
            task: crate::task_volume::Component::new("active").unwrap(), file: TaskFile::Completion(0),
        }, b"{{}}".to_vec());
        append(&mut views, &completed);
        let frozen = views.transcripts["AGGREGATE_KIND_TASK:active"].clone();
        prop_assert_eq!(append(&mut views, &chunk("active", pieces.len() as u64, b"late")), 0);
        for index in 0..pieces.len() {
            append(&mut views, &chunk("another", index as u64, b"new"));
            prop_assert_eq!(&views.transcripts["AGGREGATE_KIND_TASK:active"], &frozen);
        }
        prop_assert_eq!(views.transcripts["AGGREGATE_KIND_TASK:active"].streaming_text.as_str(), pieces.concat());
    }
}

#[test]
fn tool_channels_keep_utf8_boundaries_independent_of_interleaving() {
    let task = crate::task_volume::Component::new("tool-task").unwrap();
    let call = crate::task_volume::Component::new("call").unwrap();
    let values: Vec<_> = [
        (ToolFile::Stdout(0), vec![0xe6]),
        (ToolFile::Stderr(0), b"error".to_vec()),
        (ToolFile::Stdout(1), vec![0x97, 0xa5]),
    ]
    .into_iter()
    .map(|(file, bytes)| {
        FileValue::new(
            FileKey::Task {
                task: task.clone(),
                file: TaskFile::Tool {
                    turn: 0,
                    call: call.clone(),
                    file,
                },
            },
            bytes,
        )
    })
    .collect();
    let mut live = TaskFileViews::default();
    for file in &values {
        append(&mut live, file);
    }
    let mut sorted = values.clone();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));
    let mut reopened = TaskFileViews::default();
    for file in &sorted {
        append(&mut reopened, file);
    }
    assert_eq!(live.tools, reopened.tools);
    let crate::model::ToolResultContent::Parts { parts } =
        &live.tools["AGGREGATE_KIND_TASK:tool-task"]["call"].output
    else {
        panic!("text parts");
    };
    assert_eq!(
        parts,
        &vec![
            crate::model::ModelDataPart::Text { text: "日".into() },
            crate::model::ModelDataPart::Text {
                text: "error".into()
            }
        ]
    );
}

#[test]
fn older_completion_does_not_commit_the_current_turns_provisional_suffix() {
    let mut first = chunk("attempts", 0, b"old");
    let mut next = chunk("attempts", 1, &[0xe6]);
    if let FileKey::Task {
        file: TaskFile::Chunk { turn, .. },
        ..
    } = &mut next.key
    {
        *turn = 1;
    }
    let completion = FileValue::new(
        FileKey::Task {
            task: crate::task_volume::Component::new("attempts").unwrap(),
            file: TaskFile::Completion(0),
        },
        b"{}".to_vec(),
    );
    for channel in [Channel::Text, Channel::Reasoning] {
        first.channel = Some(channel);
        next.channel = Some(channel);
        let mut live = TaskFileViews::default();
        for file in [&first, &completion, &next] {
            append(&mut live, file);
        }
        let mut reopened = TaskFileViews::default();
        for file in [&first, &next, &completion] {
            append(&mut reopened, file);
        }
        assert_eq!(live.transcripts, reopened.transcripts);
        let transcript = &live.transcripts["AGGREGATE_KIND_TASK:attempts"];
        assert_eq!(transcript.pending(channel == Channel::Reasoning), 3);
    }
}

#[test]
fn separate_execution_cursors_do_not_drop_restarted_ordinals() {
    let mut views = TaskFileViews::default();
    for (id, payload) in [(1, b"first".as_slice()), (2, b"second".as_slice())] {
        let mut file = chunk("active", 0, payload);
        let FileKey::Task {
            file: TaskFile::Chunk { execution, .. },
            ..
        } = &mut file.key
        else {
            unreachable!()
        };
        *execution = crate::OperationId::from_bytes([id; 16]);
        assert_eq!(append(&mut views, &file), payload.len());
        assert_eq!(append(&mut views, &file), 0);
    }
    assert_eq!(
        views.transcripts["AGGREGATE_KIND_TASK:active"].streaming_text,
        "first\n\nsecond"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn chronological_task_chunks_reopen_identically_across_executions(
        pieces in prop::collection::vec("[a-zé猫]{1,8}",2..5), crossed_turns in any::<bool>(),
    ) {
        let runtime=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for unscoped in [false,true] {
        runtime.block_on(async {
            use crate::task_volume::{Component, Query, TaskVolume};
            use acyclic_stream::{CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey, MemoryStream, StreamPath, StreamProvider};
            let provider=std::sync::Arc::new(MemoryStream::default());
            let volume=TaskVolume::open(provider.clone(),StreamPath::new("chronological").unwrap()).await.unwrap();
            let task_id=crate::TaskId::from_bytes([9;16]);
            let task=Component::new(task_id.to_string()).unwrap();
            let owner=crate::wire::Authority {kind:crate::wire::AggregateKind::Task as i32,id:task_id.to_string()};
            let mut client=crate::client::TaskFileClient::observe(&volume,&owner,32,65536).await.unwrap();
            let mut live=TaskFileViews::default();
            let mut published=Vec::new();
            for (ordinal,execution) in [crate::OperationId::from_bytes([255;16]),crate::OperationId::from_bytes([1;16])].into_iter().enumerate() {
                let turn=if unscoped {ordinal as u64} else if crossed_turns && ordinal==0 {5} else {0};
                for (part,text) in pieces.iter().enumerate() {
                    let text=format!("{ordinal}:{text}");
                    let prepared=if unscoped {
                        volume.prepare_journal_file(FileKey::parse(&format!(".tasks/{}/turns/{turn}/chunks/{part}", task.as_str())).unwrap(),Channel::Text,text.as_bytes(),None).await
                    } else { volume.prepare_next_chunk(task.clone(),execution,turn,Channel::Text,text.as_bytes(),None).await }.unwrap().unwrap();
                    let request=CommitRequest { conditions:vec![prepared.condition],mutations:vec![CommitMutation::Append {path:prepared.path,records:vec![prepared.record]}],idempotency_key:IdempotencyKey::new(format!("{ordinal}:{part}").into_bytes()).unwrap() };
                    assert!(matches!(provider.commit(request).await.unwrap(),CommitOutcome::Committed(_)));
                    client.next_change().await.unwrap();
                    let files=volume.snapshot(&Query::Root,32).await.unwrap().files;
                    let current=files.last().unwrap().clone();
                    let FileKey::Task {file:TaskFile::Chunk {sequence,..},..}=&current.key else {unreachable!()};
                    assert_eq!(*sequence,part as u64,"ordinals restart; they are not a publication clock");
                    append(&mut live,&current); published.push(current);
                    assert_eq!(client.views().transcripts,live.transcripts);
                }
            }
            let reopened_volume=volume.reopen().await.unwrap();
            let cut=reopened_volume.snapshot(&Query::Root,32).await.unwrap();
            let query=TaskFileQueryView::from_snapshot(Query::Root,cut,32,65536).unwrap();
            let mut reopened=TaskFileViews::default();
            snapshot(&mut reopened,&query);
            assert_eq!(live.transcripts,reopened.transcripts);
            let public_reopened=crate::client::TaskFileClient::observe(&reopened_volume,&owner,32,65536).await.unwrap();
            assert_eq!(client.views().transcripts,public_reopened.views().transcripts);
            let heads=reopened_volume.heads(&Query::Root,None,32).await.unwrap();
            let headed=heads.files.iter().map(|head| head.segments.iter().flat_map(|bytes|bytes.iter().copied()).collect::<Vec<_>>()).map(|bytes|String::from_utf8(bytes).unwrap()).collect::<Vec<_>>().join("\n\n");
            assert_eq!(headed,live.transcripts[&format!("AGGREGATE_KIND_TASK:{}", task.as_str())].streaming_text);
            let modern=crate::client::ClientQuery::open(&reopened_volume,Query::Root,None,crate::client::QueryViewLimits {files:32,segments:32,bytes:65536}).await.unwrap();
            let modern_text=modern.files().map(|head|head.segments.iter().flat_map(|bytes|bytes.iter().copied()).collect::<Vec<_>>()).map(|bytes|String::from_utf8(bytes).unwrap()).collect::<Vec<_>>().join("\n\n");
            assert_eq!(headed,modern_text);
            // Restore the reviewed sequence-first ordering; both public scopes fail.
            published.sort_by_key(|file| match file.key {FileKey::Task {file:TaskFile::Chunk {turn,execution,sequence},..}=>(sequence,turn,execution),_=>unreachable!()});
            let mut broken=TaskFileViews::default();
            for file in published {append(&mut broken,&file);}
            assert_ne!(live.transcripts,broken.transcripts,"per-turn ordinals passed the chronology detector");
        });
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(6))]
    #[test]
    fn reopening_completed_partial_utf8_preserves_the_frozen_tail(
        reasoning in any::<bool>(), suffix in prop::sample::select(vec![vec![0xe6u8], vec![0xc3], vec![0xf0,0x9f]]),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            use crate::task_volume::{Component, TaskVolume};
            use acyclic_stream::{MemoryStream, StreamPath};
            let task_id=crate::TaskId::from_bytes([7;16]);
            let task=Component::new(task_id.to_string()).unwrap();
            let owner=crate::wire::Authority {kind:crate::wire::AggregateKind::Task as i32,id:task_id.to_string()};
            let volume=TaskVolume::open(std::sync::Arc::new(MemoryStream::default()),StreamPath::new("completed").unwrap()).await.unwrap();
            let mut client=crate::client::TaskFileClient::observe(&volume,&owner,32,4096).await.unwrap();
            let mut file=FileValue::new(FileKey::Task {task:task.clone(),file:TaskFile::Chunk {turn:0,execution:crate::OperationId::from_bytes([2;16]),sequence:0}},suffix);
            file.channel=Some(if reasoning {Channel::Reasoning} else {Channel::Text});
            let cut=volume.append(volume.head().await.unwrap(),&file).await.unwrap();
            client.next_change().await.unwrap();
            let complete=FileValue::new(FileKey::Task {task,file:TaskFile::Completion(0)},b"{}".to_vec());
            volume.append(cut,&complete).await.unwrap();
            client.next_change().await.unwrap();
            let reopened=crate::client::TaskFileClient::observe(&volume.reopen().await.unwrap(),&owner,32,4096).await.unwrap();
            assert_eq!(client.views().transcripts,reopened.views().transcripts);
            let key=format!("AGGREGATE_KIND_TASK:{task_id}");
            assert_eq!(reopened.views().transcripts[&key].pending(reasoning),0);
            let mut broken=TaskFileViews::default();
            append(&mut broken,&complete); append(&mut broken,&file);
            assert_eq!(broken.transcripts[&key].pending(reasoning),3);
            assert_ne!(broken.transcripts,client.views().transcripts);
        });
    }
}
