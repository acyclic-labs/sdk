use super::*;
use crate::durable_host::tests::fixture::Fixture;
use acyclic_stream::{MemoryStream, StreamPath};
use std::cell::Cell;

async fn funded_attempts(broken: bool) -> Result<()> {
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
    let volume = TaskVolume::open(stream, StreamPath::new("funded-publication").unwrap())
        .await
        .map_err(storage)?;
    let task = Component::new(hex::encode(owner.task_id().into_bytes())).unwrap();
    let id = Component::new("search-funded").unwrap();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let bytes = encode(
        &SearchOutcome::Succeeded {
            response: SearchResponse { results: vec![] },
        },
        Some(&work),
    )?;
    let completion = volume
        .file_volume(&ready(&task, &id))
        .await
        .map_err(storage)?;
    retain_response(&completion, ready(&task, &id), bytes.clone(), Some(&work)).await?;
    let key = file(&task, &id, true);
    let write = FileValue::new(key.clone(), bytes.clone());

    // Measure the real, fixed packing cost separately from attempt admission.
    // Every failed append below packs the identical file on this original ledger.
    let generation = volume
        .snapshot(&Query::File(key.clone()), 1)
        .await
        .map_err(storage)?
        .generation;
    let before = work.receipt().items_examined;
    let rejection = volume
        .append_if(
            generation,
            &write,
            vec![CommitCondition::Tail {
                path: StreamPath::new("absent-calibration").unwrap(),
                expected: 1,
            }],
            None,
            Some(&work),
        )
        .await;
    assert!(matches!(
        rejection,
        Err(crate::task_volume::Error::FenceConflict)
    ));
    let packing = work.receipt().items_examined - before;
    assert!(packing > 0);

    let authorizations = Cell::new(0);
    let authorize = |settlement: bool| {
        let (fixture, owner, authorizations) = (&fixture, &owner, &authorizations);
        async move {
            assert!(settlement);
            let conditions = condition(owner, None, true).await?;
            let next = authorizations.get() + 1;
            authorizations.set(next);
            assert!(next <= PUBLICATION_ATTEMPTS, "unbounded funded publisher");
            fixture.running((next + 1) as u8).await?;
            Ok(conditions)
        }
    };
    let before = work.receipt().items_examined;
    let result = if broken {
        // Deliberately move admission outside the retry loop: omit later charges.
        attempt(Some(&work))?;
        for _ in 0..PUBLICATION_ATTEMPTS {
            let snapshot = volume
                .snapshot(&Query::File(key.clone()), 1)
                .await
                .map_err(storage)?;
            let result = volume
                .append_if(
                    snapshot.generation,
                    &write,
                    authorize(true).await?,
                    None,
                    Some(&work),
                )
                .await;
            assert!(matches!(
                result,
                Err(crate::task_volume::Error::FenceConflict)
            ));
        }
        Err(Error::SearchPublicationDeferred)
    } else {
        publish(&volume, key.clone(), bytes.clone(), &authorize, Some(&work)).await
    };
    assert!(
        matches!(result, Err(Error::SearchPublicationDeferred)),
        "{result:?}"
    );
    assert_eq!(authorizations.get(), PUBLICATION_ATTEMPTS);
    assert_eq!(
        work.receipt().items_examined - before,
        PUBLICATION_ATTEMPTS as u64 * (packing + 1),
        "publication retries were not charged individually"
    );
    assert!(
        work.remaining()?.items_examined > 0,
        "funded ledger was exhausted"
    );

    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let owner = fixture
        .host
        .journal_owner(owner.task_id(), owner.task_binding().1, Some(&io))
        .await?;
    let resume = |settlement| condition(&owner, Some(&work), settlement);
    publish(&volume, key.clone(), bytes.clone(), &resume, Some(&work)).await?;
    let terminal = volume
        .snapshot(&Query::File(key), 1)
        .await
        .map_err(storage)?;
    assert_eq!(terminal.files.len(), 1);
    assert_eq!(terminal.files[0].bytes, bytes);
    assert!(terminal.files[0].bytes.matches_meter(&work.runtime_meter()));
    Ok(())
}

#[tokio::test]
async fn all_four_native_publication_attempts_charge_the_original_ledger() -> Result<()> {
    funded_attempts(false).await
}

#[tokio::test]
#[should_panic(expected = "publication retries were not charged individually")]
async fn negative_control_detects_admission_only_before_the_loop() {
    funded_attempts(true).await.unwrap();
}
