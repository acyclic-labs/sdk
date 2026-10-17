use super::*;
use acyclic_stream::MemoryStream;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn execution_chunk_identity_preserves_independent_attempts(a in any::<[u8;16]>(), b in any::<[u8;16]>(), turn in any::<u64>(), sequence in any::<u64>()) {
        prop_assume!(a != b);
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let provider = Arc::new(MemoryStream::default());
            let volume = TaskVolume::open(provider.clone(), StreamPath::new("attempts").unwrap()).await.unwrap();
            for (execution, payload) in [(a, b"first".as_slice()), (b, b"second".as_slice())] {
                let key = FileKey::Task { task: Component::new("t").unwrap(), file: TaskFile::Chunk { turn, execution: crate::OperationId::from_bytes(execution), sequence } };
                assert_eq!(FileKey::parse(&key.path()).unwrap(), key);
                assert_eq!(serde_json::from_slice::<FileKey>(&serde_json::to_vec(&key).unwrap()).unwrap(), key);
                let prepared = volume.prepare_journal_file(key.clone(), Channel::Text, payload, None).await.unwrap().unwrap();
                let request = CommitRequest { conditions: vec![prepared.condition], mutations: vec![CommitMutation::Append { path: prepared.path, records: vec![prepared.record] }], idempotency_key: IdempotencyKey::new(execution.to_vec()).unwrap() };
                assert!(matches!(provider.commit(request).await.unwrap(), CommitOutcome::Committed(_)));
                // Same namespace plus changed bytes must fail preparation, before commit.
                assert!(matches!(volume.prepare_journal_file(key, Channel::Text, b"different", None).await, Err(crate::Error::TaskFiles(ref error)) if matches!(error.as_ref(), Error::ImmutableConflict(_))));
            }
            let files = volume.snapshot(&Query::Chunks { task: Component::new("t").unwrap(), turn }, 4).await.unwrap().files;
            assert_eq!(files.len(), 2);
            let actual_ids: std::collections::BTreeSet<_> = files.iter().map(|file| match file.key {
                FileKey::Task { file: TaskFile::Chunk { execution, .. }, .. } => execution.into_bytes(), _ => unreachable!(),
            }).collect();
            assert_eq!(actual_ids, std::collections::BTreeSet::from([a, b]));
            let heads = volume.heads(&Query::Root, None, 4).await.unwrap();
            assert_eq!(heads.files.len(), 2, "materialized heads aliased separate executions");
            for head in &heads.files {
                assert_eq!(FileKey::parse(&head.key.path()).unwrap(), head.key);
                let selected = volume.snapshot(&Query::File(head.key.clone()), 4).await.unwrap();
                assert_eq!(selected.files.len(), 1, "head query crossed execution scope");
                if let FileKey::Task { file: TaskFile::TextHead { execution, .. }, .. } = &head.key {
                    if execution.into_bytes() != [0;16] {
                        for directory in ["chunks", "executions"] {
                            let query = Query::folder(&format!(".tasks/t/turns/{turn}/{directory}/{execution}")).unwrap();
                            assert_eq!(serde_json::from_slice::<Query>(&serde_json::to_vec(&query).unwrap()).unwrap(), query);
                            assert_eq!(volume.snapshot(&query, 4).await.unwrap().files, selected.files);
                        }
                    }
                }
            }
            let mut payloads: Vec<_> = files.iter().map(|f| f.bytes.to_vec()).collect();
            payloads.sort();
            assert_eq!(payloads, [b"first".to_vec(), b"second".to_vec()]);
            // The old namespace demonstrably aliases distinct executions.
            let legacy = |_: [u8;16]| FileKey::parse(&format!(".tasks/t/turns/{turn}/chunks/{sequence}")).unwrap();
            assert_eq!(legacy(a), legacy(b));
        });
    }
}

#[test]
fn execution_paths_require_canonical_nonlegacy_ids() {
    for id in [
        "01010101010101010101010101010101",
        "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
        "00000000-0000-0000-0000-000000000000",
        "invalid",
    ] {
        assert!(FileKey::parse(&format!(".tasks/t/turns/0/chunks/{id}/0")).is_err());
        assert!(FileKey::parse(&format!(".tasks/t/turns/0/executions/{id}/text.md")).is_err());
    }
    // Existing paths and serialized records keep their reserved legacy scope.
    let key = FileKey::parse(".tasks/t/turns/0/chunks/0").unwrap();
    let legacy = br#"{"Task":{"task":"t","file":{"Chunk":{"turn":0,"sequence":0}}}}"#;
    assert_eq!(serde_json::from_slice::<FileKey>(legacy).unwrap(), key);
    assert_eq!(key.path(), ".tasks/t/turns/0/chunks/0");
}

#[tokio::test]
async fn preparation_preserves_native_denial_before_publication() {
    let provider = Arc::new(MemoryStream::default());
    let volume = TaskVolume::open(provider, StreamPath::new("denied").unwrap())
        .await
        .unwrap();
    let before = volume.head().await.unwrap();
    let mut budget = acyclic_fs::performance::work_budget(1_000_000, 1 << 20);
    budget.bytes_copied = 1;
    let work =
        acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
            .unwrap();
    let io = crate::filesystem::PreparationIo::from_native_work(&work).unwrap();
    let key = FileKey::Task {
        task: Component::new("t").unwrap(),
        file: TaskFile::Chunk {
            turn: 0,
            execution: crate::OperationId::from_bytes([2; 16]),
            sequence: 0,
        },
    };
    let error = volume
        .prepare_journal_file(key, Channel::Text, b"delta", Some(&io))
        .await
        .err()
        .unwrap();
    assert!(
        error.is_native_failure(),
        "native denial was wrapped: {error:?}"
    );
    assert_eq!(volume.head().await.unwrap(), before);
    let broken = crate::Error::TaskFiles(Box::new(Error::FileRead(error)));
    assert!(
        !broken.is_native_failure(),
        "wrapper negative control did not erase native classification"
    );
}
