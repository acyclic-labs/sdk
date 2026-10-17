use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

async fn framed_result<const EXACT: bool>(extra: usize, task_root: bool) {
    let volume = Arc::new(
        files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("framed-result").unwrap(),
        )
        .await
        .unwrap(),
    );
    let session = uuid::Uuid::from_bytes([1; 16]);
    let operation = OperationId::from_bytes([2; 16]);
    let id = component(operation.to_string()).unwrap();
    let key = |file| {
        if task_root {
            files::FileKey::Task {
                task: component(TaskId::from_bytes([3; 16]).to_string()).unwrap(),
                file,
            }
        } else {
            files::FileKey::Session {
                session: component(session.to_string()).unwrap(),
                file,
            }
        }
    };
    let result_key = key(files::TaskFile::ExecutionResult(id.clone()));
    let framing =
        files::record_bytes(&files::FileValue::new(result_key.clone(), Vec::new())).unwrap();
    let budget = acyclic_stream::MAX_RECORD_BYTES - framing;
    let empty = archive::output_bytes(0, &Ok(json!({"stdout":""})), u64::MAX)
        .unwrap()
        .len();
    let result = json!({"stdout":"x".repeat(budget + extra - empty)});
    let mut commands = CommandSession::open(
        volume.clone(),
        session,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        32,
    )
    .await
    .unwrap();
    let request = files::FileValue::new(
        key(files::TaskFile::Request(id)),
        serde_json::to_vec(&CommandResolution::Prompt {
            text: "hi".into(),
            source: None,
            mentions: Vec::new(),
        })
        .unwrap(),
    );
    let cut = volume.head().await.unwrap();
    volume.append(cut, &request).await.unwrap();
    if EXACT {
        let mut calls = 0;
        commands
            .consume(&request, |_, _, _| {
                calls += 1;
                async { Ok(result) }
            })
            .await
            .unwrap();
        assert_eq!(calls, 1);
        let saved = volume
            .snapshot(&files::Query::File(result_key), 1)
            .await
            .unwrap();
        let saved = &saved.files[0];
        assert!(files::record_bytes(saved).unwrap() <= acyclic_stream::MAX_RECORD_BYTES);
        let body: Value = serde_json::from_slice(&saved.bytes).unwrap();
        assert_eq!(body["output_omitted"], true);
        assert_eq!(body["completion"], "completed");
        drop(commands);
        let mut reopened =
            CommandSession::open(volume, session, acyclic_stream::MAX_RECORD_BYTES as u64, 32)
                .await
                .unwrap();
        assert!(
            reopened
                .consume(&request, |_, _, _| async {
                    panic!("saved outcome dispatched twice")
                })
                .await
                .unwrap()
                .is_none()
        );
    } else {
        // Original policy: the body fits, so omission never runs. Its metadata
        // then overflows the actual record and the terminal publication fails.
        let bytes =
            archive::output_bytes(0, &Ok(result), acyclic_stream::MAX_RECORD_BYTES as u64).unwrap();
        assert!(bytes.len() <= acyclic_stream::MAX_RECORD_BYTES);
        assert!(
            serde_json::from_slice::<Value>(&bytes)
                .unwrap()
                .get("output_omitted")
                .is_none()
        );
        commands
            .publish(result_key, bytes, true)
            .await
            .expect("old payload-only budget stranded the result");
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn near_limit_results_include_record_framing_and_never_replay(extra in 8usize..64, task_root in any::<bool>()) {
        futures::executor::block_on(framed_result::<true>(extra,task_root));
    }
}
#[test]
#[should_panic(expected = "old payload-only budget stranded the result")]
fn payload_only_result_budget_is_detected_negative_control() {
    futures::executor::block_on(framed_result::<false>(8, false));
}
