use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_harness::{
    Error,
    scheduler::LeaseFence,
    search::{
        SearchRequest, SearchResponse,
        files::{SearchOutcome, admit_request_for_test, drive_for_test},
    },
    task_volume::{Component, FileKey, FileValue, Query, TaskVolume},
    test_support::{ScriptedSearch, ScriptedSearchProvider},
};
use acyclic_stream::{MemoryStream, StreamPath};
use serde::Deserialize;
use std::{cell::Cell, sync::Arc};

// Independent request DTO and contract. Do not use production validation here.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    query: String,
    #[serde(default = "default_results")]
    max_results: u32,
}
fn default_results() -> u32 {
    5
}

pub fn expected(bytes: &[u8]) -> Option<SearchRequest> {
    if bytes.len() > 262_144 {
        return None;
    }
    let request: Request = serde_json::from_slice(bytes).ok()?;
    if request.query.chars().count() > 4096
        || !request.query.chars().any(|c| !c.is_whitespace())
        || !(1..=20).contains(&request.max_results)
    {
        return None;
    }
    Some(SearchRequest {
        query: request.query,
        max_results: request.max_results,
    })
}

pub async fn check(bytes: &[u8]) {
    let expected = expected(bytes);
    for native in [false, true] {
        let ledger = NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &CancellationToken::new(),
        )
        .unwrap_or_else(|error| panic!("ledger: {error:?}"));
        let work = native.then_some(&ledger);
        let baseline = ledger.live_allocation_bytes();
        match (admit_request_for_test(bytes, work), &expected) {
            (Ok((request, custody)), Some(expected)) => {
                assert_eq!(&request, expected);
                if native {
                    assert!(
                        custody
                            .as_ref()
                            .unwrap()
                            .work()
                            .shares_allocation_context(&ledger)
                    );
                }
                let canonical = serde_json::to_vec(&request).unwrap();
                let round_trip = admit_request_for_test(&canonical, work).unwrap();
                assert_eq!(round_trip.0, request);
                drop(round_trip);
                drop((request, custody));
            }
            (Err(error), None) => assert!(
                matches!(
                    (&error, native),
                    (Error::ToolRejected { .. }, true) | (Error::Invalid(_), false)
                ),
                "nonterminal admission failure: {error:?}"
            ),
            (actual, expected) => {
                panic!("admission disagrees with independent contract: {actual:?}, {expected:?}")
            }
        }
        assert_eq!(ledger.live_allocation_bytes(), baseline);

        let volume = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("request-fuzz").unwrap(),
        )
        .await
        .unwrap();
        let task = Component::new("request").unwrap();
        let input = FileKey::parse(".tasks/request/requests/search-one.json").unwrap();
        let result = FileKey::parse(".tasks/request/requests/search-one/result.json").unwrap();
        let generation = volume.snapshot(&Query::Root, 0).await.unwrap().generation;
        let metadata = serde_json::to_vec(&serde_json::json!({"key":input,"channel":null}))
            .unwrap()
            .len()
            + 12;
        let admitted = volume
            .append(generation, &FileValue::new(input, bytes.to_vec()))
            .await;
        if bytes.len() + metadata > acyclic_stream::MAX_RECORD_BYTES {
            assert!(matches!(
                admitted,
                Err(acyclic_harness::task_volume::Error::LimitExceeded)
            ));
            assert!(
                volume
                    .snapshot(&Query::Root, 0)
                    .await
                    .unwrap()
                    .files
                    .is_empty()
            );
            continue;
        }
        admitted.unwrap();
        let provider = Arc::new(
            ScriptedSearchProvider::new(expected.iter().cloned().map(|request| ScriptedSearch {
                request,
                response: Some(SearchResponse { results: vec![] }),
                error: None,
            }))
            .unwrap(),
        );
        let resolutions = Cell::new(0);
        let mut fence = LeaseFence {
            reservation_id: "first".into(),
            placement: "request".into(),
        };
        drive_for_test(
            &volume,
            &task,
            &fence,
            || {
                assert!(expected.is_some(), "invalid request resolved a credential");
                resolutions.set(resolutions.get() + 1);
                Ok(Some(provider.clone()))
            },
            vec![],
            work,
            1,
        )
        .await
        .unwrap();
        let before = volume
            .snapshot(&Query::Task(task.clone()), 4)
            .await
            .unwrap();
        assert_eq!(before.files.len(), 3);
        let terminal = before.files.iter().find(|file| file.key == result).unwrap();
        let outcome: SearchOutcome = serde_json::from_slice(&terminal.bytes).unwrap();
        assert_eq!(
            outcome,
            if expected.is_some() {
                SearchOutcome::Succeeded {
                    response: SearchResponse { results: vec![] },
                }
            } else {
                SearchOutcome::Rejected
            }
        );
        assert_eq!(resolutions.get(), usize::from(expected.is_some()));
        assert_eq!(provider.remaining(), 0);
        for replacement in [false, true] {
            if replacement {
                fence.reservation_id = "replacement".into();
            }
            drive_for_test(
                &volume,
                &task,
                &fence,
                || panic!("replay resolved a credential"),
                vec![],
                work,
                1,
            )
            .await
            .unwrap();
            let after = volume
                .snapshot(&Query::Task(task.clone()), 4)
                .await
                .unwrap();
            assert_eq!(after.generation, before.generation);
            assert_eq!(after.files, before.files);
        }
    }
}
