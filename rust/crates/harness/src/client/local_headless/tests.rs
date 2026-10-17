use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Canonical file JSON preserves the exact selected identity and payload;
    // another session or an execution-owned key is rejected before publication.
    #[test]
    fn local_jsonl_requests_round_trip_without_authority_expansion(
        identity in any::<[u8;16]>(), text in ".{1,128}",
    ) {
        let operation = OperationId::from_bytes(identity);
        let request = prepare_local_prompt(operation, &text).unwrap();
        let line = serde_json::to_string(&request).unwrap();
        prop_assert_eq!(parse_local_request(&line, operation).unwrap(), request.clone());
        let mut foreign = identity; foreign[0] ^= 1;
        prop_assert!(matches!(parse_local_request(&line, OperationId::from_bytes(foreign)), Err(Error::Unauthorized(_))));
        for make in [TaskFile::ExecutionClaim, TaskFile::ExecutionResult] {
            let mut forbidden = request.clone();
            forbidden.key = key(operation, make).unwrap();
            prop_assert!(matches!(parse_local_request(&serde_json::to_string(&forbidden).unwrap(), operation), Err(Error::Unauthorized(_))));
        }
    }

    // Every settled result, including bounded output omission, preserves its
    // explicit completion class instead of trying to classify error strings.
    #[test]
    fn completion_class_survives_bounded_outcome_files(count in 0usize..4096) {
        for kind in 0u8..5 { futures::executor::block_on(async {
            use crate::task_volume::TaskVolume;
            let volume = Arc::new(TaskVolume::open(Arc::new(acyclic_stream::MemoryStream::default()), acyclic_stream::StreamPath::new("headless-outcomes").unwrap()).await.unwrap());
            let cut = volume.snapshot(&Query::Root, 32).await.unwrap();
            volume.append(cut.generation, &FileValue::new(FileKey::parse("commands/catalog.json").unwrap(), b"[]".to_vec())).await.unwrap();
            let operation = OperationId::new();
            let mut commands = super::super::CommandSession::open(volume, uuid::Uuid::from_bytes(operation.into_bytes()), 1024, 32).await.unwrap();
            commands.publish_resolution(CommandResolution::Prompt { text: "prompt".into(), source: None, mentions: Vec::new() }).await.unwrap();
            let request = commands.next_request().await.unwrap();
            let expected = [HeadlessOutcome::Completed, HeadlessOutcome::Failed, HeadlessOutcome::Cancelled, HeadlessOutcome::Blocked, HeadlessOutcome::Indeterminate][kind as usize];
            let result = commands.consume(&request, |actual,_,_| async move {
                match kind {
                    0 => Ok(serde_json::json!({"text":"x".repeat(count)})),
                    1 => Err(Error::Storage("x".repeat(count))),
                    2 => Err(Error::InteractionRejected(crate::InteractionRejection::Cancelled)),
                    3 => Err(Error::InteractionRejected(crate::InteractionRejection::Declined)),
                    _ => Err(Error::Indeterminate(actual)),
                }
            }).await;
            assert_eq!(result.is_ok(), kind == 0);
            let saved = commands.output_page(0).unwrap().unwrap();
            let bytes = serde_json::to_vec(&saved).unwrap();
            assert!(bytes.len() <= 1024);
            assert_eq!(super::super::command_output::completion(&bytes).unwrap(), expected);
        }); }
    }
}

#[tokio::test]
async fn unsaved_indeterminate_owner_result_preserves_exit_class() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = Arc::new(
        crate::test_support::ScriptedModelProvider::new([]).with_context_capacity(
            crate::context::ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: 4096,
            },
        ),
    );
    let model = crate::model::Model::new(
        "test".into(),
        "uncertain".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let local = Arc::new(
        LocalHarness::open(
            crate::test_support::local_harness_config(directory.path(), (model, provider.clone()))?
                .without_default_tools(),
        )
        .await?,
    );
    let operation = OperationId::new();
    let mut client = LocalFileClient::open(local.clone(), operation).await?;
    // The recovery owner cannot establish an outcome. Its original request
    // remains in the outbox; no fabricated terminal execution file is written.
    client.host = Box::pin(async move { Err(Error::Indeterminate(operation)) });
    let request = prepare_local_prompt(operation, "uncertain request")?;
    let outcome =
        super::super::run_headless(&mut client, request, operation, &mut Vec::new()).await?;
    assert_eq!(outcome, HeadlessOutcome::Indeterminate);
    assert_eq!(outcome.exit_code(), 4);
    assert!(client.outcome()?.is_none());
    assert!(provider.requests().is_empty());
    drop(client);
    super::super::disconnect_local(local).await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2))]
    // Follow publishes every fragment, but retains a bounded disposable tail.
    // The identical reducer without eviction is the retention negative control.
    #[test]
    fn fragmented_follow_completes_without_retaining_the_stream(count in 10_001usize..10_017, turns in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            use crate::task_volume::TaskVolume;
            let volume = TaskVolume::open(Arc::new(acyclic_stream::MemoryStream::default()), acyclic_stream::StreamPath::new("fragmented-follow").unwrap()).await.unwrap();
            let operation = OperationId::new();
            let request = prepare_local_prompt(operation, &"p".repeat(64_000)).unwrap();
            let mut generation = volume.snapshot(&Query::Root, 4096).await.unwrap().generation;
            generation = volume.append(generation, &request).await.unwrap();
            let limits = QueryViewLimits { files: 4096, segments: 4096, bytes: OBSERVATION_BYTES };
            let mut query = ClientQuery::open(&volume, selection(operation).unwrap(), None, limits).await.unwrap();
            let mut broken = super::super::file_views::FileViews::snapshot(query.snapshot(), limits).unwrap();
            let mut rejected = false;
            let mut delivered = 0;
            for sequence in 0..count {
                let file = FileValue { requester: None, publication: None,
                    key: FileKey::Task { task: Component::new(crate::runtime::task_id_for_admission(operation).to_string()).unwrap(),
                        file: TaskFile::Chunk { execution: crate::OperationId::from_bytes([0; 16]), turn: if turns { (sequence / 64) as u64 } else { 0 }, sequence: sequence as u64 } },
                    channel: Some(crate::task_volume::Channel::Text), bytes: vec![b'x'].into() };
                generation = volume.append(generation, &file).await.unwrap();
                query.publish_next(async |change| {
                    delivered += 1;
                    if !rejected {
                        match broken.prepare(change) {
                            Ok(apply) => apply(&mut broken).unwrap(),
                            Err(crate::task_volume::Error::LimitExceeded) => rejected = true,
                            Err(error) => panic!("unexpected negative-control failure: {error}"),
                        }
                    }
                    Ok(())
                }).await.unwrap();
                assert!(query.files().map(|head| head.segments.len()).sum::<usize>() <= 129);
                assert!(query.files().map(|head| head.length).sum::<u64>() <= request.bytes.len() as u64 + 128);
                assert_eq!(query.file(&request.key).unwrap().segments[0], request.bytes);
            }
            let result = FileValue::new(key(operation, TaskFile::ExecutionResult).unwrap(), br#"{"sequence":1,"ok":{"text":"done"}}"#.to_vec());
            generation = volume.append(generation, &result).await.unwrap();
            query.publish_next(async |_| Ok(())).await.unwrap();
            assert_eq!(query.file(&result.key).unwrap().segments[0], result.bytes);
            assert_eq!(query.cursor().position, generation.position);
            assert_eq!(delivered, count);
            assert!(rejected, "negative control retaining every segment must exhaust retention");
        });
    }
}

mod restoration;
