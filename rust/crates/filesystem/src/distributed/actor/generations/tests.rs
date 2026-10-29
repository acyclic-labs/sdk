use super::*;
type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn generation_catalog_separates_cold_unknown_from_verified_fork_absence() -> TestResult {
    let (store, authority) = super::super::super::append::group_tests::fixture().await?;
    let cancel = CancellationToken::new();
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancel);
    let actor = store.actor(authority, &mut work)?;
    let mut held = actor.lease(&mut work).await?;
    let mut projection = store
        .activate(authority, None, &mut held, &mut work)
        .await?;
    let inherited = GenerationId::new(Digest::from_bytes([21; 32]));
    let earlier_ancestor = GenerationId::new(Digest::from_bytes([22; 32]));
    projection.generations = None;
    projection.lineage_tail = 17;
    assert!(matches!(
        projection.generation(earlier_ancestor, &mut work)?,
        GenerationKnowledge::Unknown
    ));
    let (catalog, identity) = GenerationCatalog::creation(Some((inherited, 17)), None, &mut work)?;
    projection.generations = Some(catalog);
    assert!(matches!(
        projection.generation(inherited, &mut work)?,
        GenerationKnowledge::Unknown
    ));
    let committed = acyclic_stream::CommitId::from_bytes([41; 32]);
    identity.seal(committed);
    assert!(
        matches!(projection.generation(inherited, &mut work)?, GenerationKnowledge::Present(value) if value == committed)
    );
    let sealed = GenerationId::from_root(ObjectId {
        storage: Some(crate::storage::ObjectStorage {
            locator: Digest::from_bytes([31; 32]),
            root_file_id: crate::FileId::from_bytes([32; 16]),
            origin_generation: inherited.digest(),
        }),
        ..inherited.object_id()
    });
    assert!(matches!(
        projection.generation(sealed, &mut work)?,
        GenerationKnowledge::Absent
    ));
    let separate = GenerationIdentity::new(&mut work)?;
    projection
        .generations
        .as_ref()
        .unwrap()
        .stage(sealed, 17, &separate, &mut work)?;
    let sealed_commit = acyclic_stream::CommitId::from_bytes([43; 32]);
    separate.seal(sealed_commit);
    assert!(
        matches!(projection.generation(sealed, &mut work)?, GenerationKnowledge::Present(value) if value == sealed_commit)
    );
    assert!(
        matches!(projection.generation(inherited, &mut work)?, GenerationKnowledge::Present(value) if value == committed)
    );
    assert!(
        matches!(
            projection.generation(earlier_ancestor, &mut work)?,
            GenerationKnowledge::Absent
        ),
        "an inherited lineage row was mistaken for a destination locator"
    );
    Ok(())
}

#[tokio::test]
async fn generation_catalog_pending_facts_cannot_escape_their_frontier_or_receipt() -> TestResult {
    let (store, authority) = super::super::super::append::group_tests::fixture().await?;
    let cancel = CancellationToken::new();
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancel);
    let actor = store.actor(authority, &mut work)?;
    let mut held = actor.lease(&mut work).await?;
    let mut projection = store
        .activate(authority, None, &mut held, &mut work)
        .await?;
    let generation = GenerationId::new(Digest::from_bytes([23; 32]));
    let catalog = projection
        .generations
        .as_ref()
        .ok_or("fresh creation lost coverage")?
        .clone();
    let abandoned = GenerationIdentity::new(&mut work)?;
    catalog.stage(generation, 1, &abandoned, &mut work)?;
    assert!(matches!(
        projection.generation(generation, &mut work)?,
        GenerationKnowledge::Absent
    ));
    projection.lineage_tail = 1;
    assert!(matches!(
        projection.generation(generation, &mut work)?,
        GenerationKnowledge::Unknown
    ));
    drop(abandoned);
    assert!(matches!(
        projection.generation(generation, &mut work)?,
        GenerationKnowledge::Unknown
    ));
    let replacement = GenerationIdentity::new(&mut work)?;
    catalog.stage(generation, 1, &replacement, &mut work)?;
    let committed = acyclic_stream::CommitId::from_bytes([42; 32]);
    replacement.seal(committed);
    assert!(
        matches!(projection.generation(generation, &mut work)?, GenerationKnowledge::Present(value) if value == committed)
    );
    projection.lineage_tail = 0;
    assert!(
        matches!(
            projection.generation(generation, &mut work)?,
            GenerationKnowledge::Absent
        ),
        "a successor receipt widened an earlier projection"
    );
    Ok(())
}

#[test]
fn generation_catalog_failed_node_admission_preserves_coverage_and_original_funding() -> TestResult
{
    crate::initialize_test_native_io();
    let cancel = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
            .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let (catalog, identity) = {
        let mut owner = AuthorityWork::native(&original, &cancel);
        GenerationCatalog::creation(None, None, &mut owner)?
    };
    let funded = original.live_allocation_bytes();
    assert!(funded > baseline);
    let generation = GenerationId::new(Digest::from_bytes([24; 32]));
    let mut denied = AuthorityWork::new(
        crate::WorkBudget {
            allocation_operations: 0,
            ..crate::performance::BOUNDARY_BUDGET
        },
        &cancel,
    );
    assert!(
        catalog
            .stage(generation, 1, &identity, &mut denied)
            .is_err()
    );
    assert!(
        catalog
            .facts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(generation.object_id().index_key(), &mut denied.accounting())?
            .is_none()
    );
    assert_eq!(original.live_allocation_bytes(), funded);
    let last = catalog.clone();
    drop(catalog);
    assert_eq!(original.live_allocation_bytes(), funded);
    drop(last);
    drop(identity);
    assert_eq!(
        original.live_allocation_bytes(),
        baseline,
        "catalog backing outlived its original credit"
    );
    Ok(())
}
