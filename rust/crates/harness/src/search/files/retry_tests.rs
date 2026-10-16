use super::*;
use crate::{
    durable_host::tests::fixture::Fixture,
    test_support::{ScriptedSearch, ScriptedSearchProvider},
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::cell::Cell;

async fn saturated_tail(broken: bool) -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let fixture = Fixture::new(
        stream.clone(),
        acyclic_fs::Fs::memory(),
        serde_json::json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    let owner = fixture.running(1).await?;
    let path = StreamPath::new("bounded-search").unwrap();
    let volume = TaskVolume::open(stream.clone(), path.clone())
        .await
        .map_err(storage)?;
    let task = Component::new(hex::encode(owner.task_id().into_bytes())).unwrap();
    let id = Component::new("search-contended").unwrap();
    let generation = volume
        .snapshot(&Query::Root, 0)
        .await
        .map_err(storage)?
        .generation;
    volume
        .append(
            generation,
            &FileValue::new(
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::Request(id.clone()),
                },
                br#"{"query":"bounded"}"#.as_slice(),
            ),
        )
        .await
        .map_err(storage)?;
    let response = SearchResponse { results: vec![] };
    let provider = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: SearchRequest {
            query: "bounded".into(),
            max_results: 5,
        },
        response: Some(response.clone()),
        error: None,
    }])?);
    let attempts = Cell::new(0);
    let contending = Cell::new(true);
    let state = (&fixture, &owner, &attempts, &contending);
    let authorize = |settlement| async move {
        let (fixture, owner, attempts, contending) = state;
        let conditions = condition(owner, None, settlement).await?;
        if settlement && contending.get() {
            let next = attempts.get() + 1;
            attempts.set(next);
            if next > PUBLICATION_ATTEMPTS {
                return Err(Error::Storage(
                    "test detected an unbounded publisher".into(),
                ));
            }
            fixture.running((next + 1) as u8).await?;
        }
        Ok(conditions)
    };
    let mut result = Ok(());
    for _ in 0..if broken { 2 } else { 1 } {
        result = settle(
            &volume,
            &task,
            &id,
            owner.task_binding().1,
            &mut || Ok(Some(provider.clone() as Arc<dyn SearchProvider>)),
            &authorize,
            None,
        )
        .await;
        if !matches!(result, Err(Error::SearchPublicationDeferred)) {
            break;
        }
    }
    assert!(
        matches!(result, Err(Error::SearchPublicationDeferred)),
        "publication attempt bound was ignored: {result:?}"
    );
    assert_eq!(attempts.get(), PUBLICATION_ATTEMPTS);
    assert_eq!(
        result.unwrap_err().code(),
        crate::wire::ErrorCode::Indeterminate
    );
    assert_eq!(provider.remaining(), 0);
    let completion = volume
        .file_volume(&ready(&task, &id))
        .await
        .map_err(storage)?;
    let saved = completion
        .snapshot(&Query::File(ready(&task, &id)), 1)
        .await
        .map_err(storage)?;
    assert_eq!(saved.files.len(), 1);
    assert_eq!(
        serde_json::from_slice::<SearchOutcome>(&saved.files[0].bytes).unwrap(),
        SearchOutcome::Succeeded {
            response: response.clone()
        }
    );
    assert!(
        volume
            .snapshot(&Query::File(file(&task, &id, true)), 1)
            .await
            .map_err(storage)?
            .files
            .is_empty()
    );

    let denied = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            items_examined: 0,
            ..acyclic_fs::performance::BOUNDARY_BUDGET
        },
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let failure = publish(
        &volume,
        file(&task, &id, true),
        saved.files[0].bytes.clone(),
        &authorize,
        Some(&denied),
    )
    .await
    .unwrap_err();
    assert!(failure.is_native_failure());
    assert_eq!(
        attempts.get(),
        PUBLICATION_ATTEMPTS,
        "unfunded attempt reached authorization"
    );

    contending.set(false);
    let reopened = TaskVolume::open(stream, path).await.map_err(storage)?;
    settle(
        &reopened,
        &task,
        &id,
        owner.task_binding().1,
        &mut || panic!("resume redispatched the completed search"),
        &authorize,
        None,
    )
    .await?;
    let terminal = reopened
        .snapshot(&Query::File(file(&task, &id, true)), 1)
        .await
        .map_err(storage)?;
    assert_eq!(terminal.files.len(), 1);
    assert_eq!(terminal.files[0].bytes, saved.files[0].bytes);
    Ok(())
}

#[tokio::test]
async fn every_attempt_conflicts_but_completion_survives_resume() -> Result<()> {
    saturated_tail(false).await
}

#[tokio::test]
#[should_panic(expected = "publication attempt bound was ignored")]
async fn negative_control_detects_ignoring_the_attempt_bound() {
    saturated_tail(true).await.unwrap();
}

#[tokio::test]
async fn claimed_request_without_completion_returns_typed_deferred() -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let volume = TaskVolume::open(stream, StreamPath::new("missing-completion").unwrap())
        .await
        .map_err(storage)?;
    let task = Component::new("task").unwrap();
    let id = Component::new("search-missing").unwrap();
    let fence = LeaseFence {
        reservation_id: "owner".into(),
        placement: "test".into(),
    };
    let generation = volume
        .snapshot(&Query::Root, 0)
        .await
        .map_err(storage)?
        .generation;
    volume
        .append(
            generation,
            &FileValue::new(file(&task, &id, false), encode(&fence, None)?),
        )
        .await
        .map_err(storage)?;
    let result = settle(
        &volume,
        &task,
        &id,
        &fence,
        &mut || panic!("claimed request was redispatched"),
        &|_| std::future::ready(Ok(vec![])),
        None,
    )
    .await;
    assert!(
        matches!(result, Err(Error::SearchPublicationDeferred)),
        "{result:?}"
    );
    assert_eq!(
        result.unwrap_err().code(),
        crate::wire::ErrorCode::Indeterminate
    );
    Ok(())
}
