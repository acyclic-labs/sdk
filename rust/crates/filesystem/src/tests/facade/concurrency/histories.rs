use super::*;
use proptest::prelude::*;

struct Proposal {
    delta: Owned<GenerationDiff>,
    reads: CheckoutDependencies,
    command: (u8, u8),
}

async fn edit(
    checkout: &mut TestCheckout,
    command: (u8, u8),
    round: u64,
    cancel: &CancellationToken,
) -> TestResult {
    // Every proposal changes length, including a generated zero byte. A clean
    // checkout returns NoChanges and is not an authority commit in this model.
    checkout
        .change_content(
            path(&format!("f{}", command.0))?,
            ContentChange::Resize {
                logical_bytes: round + 2,
            },
            ContentTimes::Preserve,
            B,
            cancel,
        )
        .await?;
    checkout
        .change_content(
            path(&format!("f{}", command.0))?,
            ContentChange::Write {
                offset: round + 1,
                bytes: OwnedBytes::from(vec![command.1]),
            },
            ContentTimes::Preserve,
            B,
            cancel,
        )
        .await?;
    Ok(())
}

async fn propose(
    volume: &TestVolume,
    command: (u8, u8),
    round: u64,
    accounting: &mut KernelAccounting<'_>,
    cancel: &CancellationToken,
) -> TestResult<Proposal> {
    let mut checkout = open_checkout(volume, writable_manual(), cancel).await?;
    edit(&mut checkout, command, round, cancel).await?;
    let (delta, allocation) = crate::kernel::diff_generation_file_tables(
        &volume.fs.inner.objects,
        checkout.base_root.file_table,
        checkout.root.file_table,
        128,
        DecodeLimits::for_volume(volume.config),
        accounting,
        cancel,
    )
    .await?;
    let bytes = delta.retained_bytes()?;
    let reads = checkout.dependencies.proof().dependencies.clone();
    Ok(Proposal {
        delta: Owned::new(delta, allocation, bytes),
        reads,
        command,
    })
}

async fn check_history(mut pending: Vec<(u8, u8)>) -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut serial) = fixture(serialized_config(), &cancel).await?;
    for key in 0..4 {
        create(
            &mut serial,
            &format!("f{key}"),
            OwnedBytes::from_static(&[0]),
            &cancel,
        )
        .await?;
    }
    commit(&mut serial, &cancel).await?;
    let mut applied = 0;
    let mut round = 0;
    while !pending.is_empty() {
        let mut accounting = KernelAccounting::standalone(B, &cancel);
        let mut selected: Vec<Proposal> = Vec::new();
        let mut deferred = Vec::new();
        for command in pending {
            let proposal = propose(&volume, command, round, &mut accounting, &cancel).await?;
            let mut independent = true;
            for prior in &selected {
                let query = crate::kernel::CommitFootprint {
                    reads: Some(&proposal.reads),
                    writes: &proposal.delta.value,
                }
                .independent(
                    crate::kernel::CommitFootprint {
                        reads: Some(&prior.reads),
                        writes: &prior.delta.value,
                    },
                    accounting.remaining()?.items_examined,
                );
                accounting.charge_items(query.items_examined)?;
                independent &= query.independent?;
            }
            if independent {
                selected.push(proposal);
            } else {
                deferred.push(command);
                proposal.delta.retire(&mut accounting)?;
            }
        }
        assert!(!selected.is_empty(), "bounded selection made no progress");
        let mut composed = serial.root.file_table;
        for proposal in selected {
            composed = crate::kernel::apply_generation_delta(
                &volume.fs.inner.objects,
                composed,
                &proposal.delta.value,
                DecodeLimits::for_volume(volume.config),
                &mut accounting,
                &cancel,
            )
            .await?;
            edit(&mut serial, proposal.command, round, &cancel).await?;
            applied += 1;
            proposal.delta.retire(&mut accounting)?;
        }
        assert_eq!(
            composed, serial.root.file_table,
            "batched tree differs from its serial history"
        );
        if serial.has_pending_mutations() {
            commit(&mut serial, &cancel).await?;
        }
        pending = deferred;
        round += 1;
    }
    assert!(applied > 0);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn real_delta_batches_equal_a_serial_history(history in prop::collection::vec((0u8..4, any::<u8>()), 1..13)) {
        futures::executor::block_on(check_history(history))
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    }
}
