use super::*;

#[derive(Debug)]
enum SealFailure {
    Owner(u8),
    Filesystem(FsError),
}
impl From<FsError> for SealFailure {
    fn from(error: FsError) -> Self {
        Self::Filesystem(error)
    }
}

#[tokio::test]
async fn native_batch_preserves_the_failing_members_typed_error_and_publishes_no_prefix()
-> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    for name in ["a", "b"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit(&mut seed, &cancel).await?;
    let base = seed.authority_head.ok_or("missing base")?;
    let mut proposals = Vec::new();
    for name in ["a", "b"] {
        let original =
            crate::NativeMaterializationWork::new(B, &cancel).map_err(|(error, _)| error)?;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                writable_manual(),
                &original,
                &cancel,
            )
            .await?;
        checkout
            .change_content(
                path(name)?,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"new"),
                },
                ContentTimes::Preserve,
                &original,
                &cancel,
            )
            .await?;
        proposals.push((checkout, original));
    }
    let outcomes = futures::future::join_all(proposals.iter_mut().enumerate().map(
        |(index, (checkout, original))| {
            let cancel = &cancel;
            async move {
                let mut seal = move |_: &crate::NativeMaterializationWork| {
                    std::future::ready(if index == 0 {
                        Err(SealFailure::Owner(7))
                    } else {
                        Ok(())
                    })
                };
                checkout
                    .publish_pending_against_with_native_work(
                        OperationId::new(),
                        None,
                        None,
                        None,
                        PublicationPermit::Unrestricted,
                        base,
                        original,
                        cancel,
                        &mut seal,
                        None,
                        None,
                    )
                    .await
            }
        },
    ))
    .await;
    let mut owners = 0;
    for outcome in outcomes {
        match outcome {
            Err(SealFailure::Owner(7)) => owners += 1,
            Err(SealFailure::Filesystem(FsError::Publication(
                crate::kernel::PublicationError::GroupAborted,
            ))) => {}
            outcome => return Err(format!("wrong producer seal result: {outcome:?}").into()),
        }
    }
    assert_eq!(
        owners, 1,
        "the producer's typed error was erased or delivered twice"
    );
    let mut accounting = KernelAccounting::standalone(B, &cancel);
    let (_, head) = volume
        .resolve_head_generation(&mut accounting, &cancel)
        .await?;
    assert_eq!(head, base, "a failed member published a batch prefix");
    for (checkout, _) in &proposals {
        assert!(checkout.has_pending_mutations());
    }
    Ok(())
}
