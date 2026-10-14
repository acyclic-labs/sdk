use super::*;

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn prepared_candidate_adoption_rejects_new_observations() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, _) = fixture(config(), &cancel).await?;
    let mut live = volume
        .checkout(GenerationSelector::Head, tracking(), B, &cancel)
        .await?
        .value;
    let base = live.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::standalone(B, &cancel),
        &cancel,
    )?;
    let candidate = base.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::standalone(B, &cancel),
        &cancel,
    )?;
    let prepared = live
        .prepare_candidate_adoption(
            &base,
            &candidate,
            &mut crate::kernel::allocation::KernelAccounting::standalone(B, &cancel),
        )?
        .ok_or("shared proof rejected")?;
    live.lookup_no_follow(&path("later-reader")?, B, &cancel)
        .await?;
    assert!(!live.adopt_prepared_candidate(&base, candidate, prepared));
    let mut candidate = base.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::standalone(B, &cancel),
        &cancel,
    )?;
    let prepared = live
        .prepare_candidate_adoption(
            &base,
            &candidate,
            &mut crate::kernel::allocation::KernelAccounting::standalone(B, &cancel),
        )?
        .ok_or("reader observations rejected")?;
    candidate
        .lookup_no_follow(&path("later-candidate")?, B, &cancel)
        .await?;
    assert!(!live.adopt_prepared_candidate(&base, candidate, prepared));
    Ok(())
}
