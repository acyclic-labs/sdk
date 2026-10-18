use super::*;
use crate::client::{ClientQuery, FileQueryView, QueryViewLimits};
use acyclic_stream::{
    CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey, StreamProvider,
};

async fn observe(
    volume: &TaskVolume<MemoryStream>,
    owner: &wire::Authority,
    task: crate::TaskId,
    selected: bool,
) -> TaskFileClient {
    if selected {
        TaskFileClient::observe_task(volume, owner, task, 32, 65536).await
    } else {
        TaskFileClient::observe(volume, owner, 32, 65536).await
    }
    .unwrap()
}
fn projected(query: &FileQueryView, task: &Component) -> Vec<u8> {
    query
        .task(task)
        .text()
        .flat_map(|text| {
            text.file
                .segments
                .iter()
                .flat_map(|bytes| bytes.iter().copied())
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn chronological_task_chunks_reopen_identically_across_executions(
        pieces in prop::collection::vec("[a-zé猫]{1,8}", 2..5), crossed_turns in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for unscoped in [false, true] {
          for selected in [false, true] {
            runtime.block_on(async {
                let provider = Arc::new(MemoryStream::default());
                let volume = TaskVolume::open(provider.clone(), StreamPath::new("chronological").unwrap()).await.unwrap();
                let task_id = crate::TaskId::from_bytes([9;16]);
                let task = Component::new(task_id.to_string()).unwrap();
                let owner = wire::Authority {kind: wire::AggregateKind::Task as i32, id: task_id.to_string()};
                let mut client = observe(&volume, &owner, task_id, selected).await;
                let mut expected = Vec::new();
                for (ordinal, execution) in [OperationId::from_bytes([255;16]), OperationId::from_bytes([1;16])].into_iter().enumerate() {
                    let turn = if unscoped {ordinal as u64} else if crossed_turns && ordinal == 0 {5} else {0};
                    for (part, text) in pieces.iter().enumerate() {
                        let text = format!("{ordinal}:{text}");
                        let prepared = if unscoped {
                            volume.prepare_journal_file(FileKey::parse(&format!(".tasks/{}/turns/{turn}/chunks/{part}", task.as_str())).unwrap(), Channel::Text, text.as_bytes(), None).await
                        } else {{
                            let key=volume.next_chunk_key(task.clone(),execution,turn,None).await.unwrap();
                            let FileKey::Task {file:TaskFile::Chunk {sequence,..},..}=key else {unreachable!()};
                            assert_eq!(sequence,(ordinal*pieces.len()+part) as u64);
                            volume.prepare_named_chunk(key,Channel::Text,text.as_bytes(),None).await
                        }}.unwrap().unwrap();
                        let request = CommitRequest {conditions: vec![prepared.condition], mutations: vec![CommitMutation::Append {path: prepared.path, records: vec![prepared.record]}], idempotency_key: IdempotencyKey::new(format!("{ordinal}:{part}").into_bytes()).unwrap()};
                        assert!(matches!(provider.commit(request).await.unwrap(), CommitOutcome::Committed(_)));
                        client.next_change().await.unwrap();
                        expected.extend_from_slice(text.as_bytes());
                        assert_eq!(projected(selection(&client), &task), expected);
                    }
                }
                let reopened_volume = volume.reopen().await.unwrap();
                let reopened = observe(&reopened_volume, &owner, task_id, selected).await;
                assert_eq!(projected(selection(&client), &task), projected(selection(&reopened), &task));
                let heads = reopened_volume.heads(&Query::Root, None, 32).await.unwrap();
                let bytes: Vec<_> = heads.files.iter().flat_map(|head| head.segments.iter().flat_map(|bytes|bytes.iter().copied())).collect();
                assert_eq!(bytes, expected);
                let modern = ClientQuery::open(&reopened_volume, Query::Root, None, QueryViewLimits {files:32, segments:32, bytes:65536}).await.unwrap();
                let bytes: Vec<_> = modern.files().flat_map(|head| head.segments.iter().flat_map(|bytes|bytes.iter().copied())).collect();
                assert_eq!(bytes, expected);
                // Restore the rejected snapshot key ordering at the public fold.
                let mut cut = reopened_volume.snapshot(&Query::Root, 32).await.unwrap();
                if !unscoped {
                    let mut keyed=reopened_volume.snapshot(&Query::Root,32).await.unwrap();
                    keyed.files.sort_by(|a,b| a.key.cmp(&b.key));
                    let broken=FileQueryView::from_snapshot(Query::Root,keyed,32,65536).unwrap();
                    assert_ne!(projected(&broken,&task),expected,"key-order negative control escaped");
                }
                if unscoped {
                    cut.files.sort_by_key(|file| match file.key {
                        FileKey::Task {file:TaskFile::Chunk {sequence,turn,execution},..}=>(sequence,turn,execution),
                        _=>unreachable!(),
                    });
                    let broken = FileQueryView::from_snapshot(Query::Root, cut, 32, 65536).unwrap();
                    assert_ne!(projected(&broken, &task), expected, "restarted-sequence negative control escaped");
                }
            });
          }
        }
    }
}
