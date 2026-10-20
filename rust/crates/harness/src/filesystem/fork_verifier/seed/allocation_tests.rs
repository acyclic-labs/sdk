use super::*;
use acyclic_test_support::differential;

#[test]
fn seed_binding_is_hashed_once_but_each_allocation_is_authenticated() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|cause| Error::Storage(cause.to_string()))?;
    let fixture =
        runtime.block_on(crate::durable_host::tests::created_fixture::CreationFixture::new())?;
    let child = runtime.block_on(fixture.create_filesystem_child_with_grants(&[]))?;
    let verifier = FilesystemForkVerifier::new(fixture.filesystem.clone(), 65_536, None, None)?;
    let observe = |restore_repeated_hash| {
        differential::observe::<_, _, (&'static str, u64)>(
            || PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new()).unwrap(),
            |io| {
                runtime.block_on(async {
                    if restore_repeated_hash {
                        // Original allocation loop recomputed the same binding
                        // for the private volume and the selected project.
                        verifier
                            .allocation(&child.seed, &child.private, &io)
                            .await?;
                        verifier.allocation(&child.seed, &child.project, &io).await
                    } else {
                        verifier.verify_seed(&child.seed, &io).await
                    }
                })
            },
        )
    };
    let count = |trace: &[(&str, u64)], label| {
        trace
            .iter()
            .filter(|(name, _)| *name == label)
            .map(|(_, amount)| amount)
            .sum::<u64>()
    };
    let actual = observe(false);
    actual.0?;
    assert_eq!(count(&actual.1, "fork seed digest"), 1);
    assert_eq!(count(&actual.1, "fork allocation verified"), 2);
    let restored = observe(true);
    restored.0?;
    assert_eq!(count(&restored.1, "fork allocation verified"), 2);
    assert_ne!(
        count(&restored.1, "fork seed digest"),
        1,
        "restoring the repeated full-seed hash escaped the exact guard"
    );
    Ok(())
}
