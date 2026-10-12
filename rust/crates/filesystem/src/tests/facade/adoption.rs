use super::*;

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn prepared_candidate_adoption_rejects_new_observations_and_changed_base_state()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "candidate-adoption-seal",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let mut live = workspace
        .checkout(
            GenerationSelector::Head,
            tracking(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let base = live
        .private_candidate(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("unbounded private_candidate preserves a validated checkout")
        .value;
    let mut candidate = base
        .private_candidate(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("unbounded private_candidate preserves a validated checkout")
        .value;
    let prepared =
        live.prepare_candidate_adoption(&base, &candidate, WorkBudget::UNBOUNDED, &cancellation)?;
    live.lookup_no_follow(&path("later-reader")?, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert!(
        !live.adopt_prepared_candidate(
            &base,
            candidate
                .private_candidate(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new()
                )
                .expect("unbounded private_candidate preserves a validated checkout")
                .value,
            prepared.value.ok_or("shared proof rejected")?,
        )
    );
    let prepared = live
        .prepare_candidate_adoption(&base, &candidate, WorkBudget::UNBOUNDED, &cancellation)?
        .value
        .ok_or("reader observations rejected")?;
    candidate
        .lookup_no_follow(
            &path("later-candidate")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert!(
        !live.adopt_prepared_candidate(
            &base,
            candidate
                .private_candidate(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new()
                )
                .expect("unbounded private_candidate preserves a validated checkout")
                .value,
            prepared
        )
    );
    let prepared = live
        .prepare_candidate_adoption(&base, &candidate, WorkBudget::UNBOUNDED, &cancellation)?
        .value
        .ok_or("compatible candidate rejected")?;
    live.live_operation_id = Some(OperationId::from_bytes([209; 16]));
    assert!(
        !live.adopt_prepared_candidate(
            &base,
            candidate
                .private_candidate(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new()
                )
                .expect("unbounded private_candidate preserves a validated checkout")
                .value,
            prepared
        )
    );
    assert!(
        live.prepare_candidate_adoption(&base, &candidate, WorkBudget::UNBOUNDED, &cancellation,)?
            .value
            .is_none()
    );
    Ok(())
}
