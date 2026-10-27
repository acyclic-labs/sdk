use super::*;

#[tokio::test]
async fn rebinding_rotates_the_execution_identity_and_discards_cached_authority() -> Result<()> {
    let (mut journal, _issuer) = super::super::test_support::journal().await?;
    let operation = OperationId::new();
    let mut identity = journal.cache_identity();
    assert!(identity.is_some());
    // Populate the real authenticated summary, so this also checks invalidation.
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    assert_eq!(
        journal.verified.get_mut().summary().operation,
        Some(operation)
    );
    journal = journal.with_turn(operation, OperationId::new());
    assert_ne!(journal.cache_identity(), identity);
    assert_eq!(journal.verified.get_mut().summary().operation, None);
    identity = journal.cache_identity();
    journal = journal.with_history_read_limits(crate::store::HistoryReadLimits {
        maximum_events: 8,
        maximum_bytes: 65_536,
    })?;
    assert_ne!(journal.cache_identity(), identity);
    identity = journal.cache_identity();
    let input = crate::filesystem::FilesystemContentVerifier::new(
        journal.host.clone(),
        &journal.verifier,
        &journal.scope,
        65_536,
        None,
    )?;
    journal = journal.with_input_verifier(input);
    assert_ne!(journal.cache_identity(), identity);
    identity = journal.cache_identity();
    let verifier = journal.verifier.copy(&mut None)?;
    let scope = journal.scope.copy(&mut None)?;
    let volume = journal.volume.copy(&mut None)?;
    journal =
        journal.with_interaction_owner(&verifier, &SchemaRegistry::new(), &scope, &volume, None)?;
    assert_ne!(journal.cache_identity(), identity);
    Ok(())
}
