use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn cold_projection_original_cut_and_anchor_survive_later_checkpoint() -> Result<()> {
    use acyclic_fs::NativeMaterializationWork;
    let client = Arc::new(MemoryStream::default());
    let mut aggregate = with_content(&client).await?;
    for identity in 1..=128 {
        let mut next = command(identity)?;
        next.expected_revision = aggregate.reducer().revision();
        aggregate
            .execute(
                next,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let original = checkpoints::load_with_work(
        &client,
        &authority(),
        &issuer().verifier(&mut None).expect("verifier copy"),
        checkpoints::default_projection_read_limits(),
        Some(64),
        &mut None,
    )
    .await?;
    assert_eq!(original.through_revision, 64);
    let original = original.snapshot.expect("the selected checkpoint exists");
    assert_eq!(original.revision, 64);
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(projection_codec_budget(), &cancellation)
        .expect("original finite historical projection ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("original historical projection guard"),
    );
    let loaded = checkpoints::load_with_work(
        &client,
        &authority(),
        &issuer().verifier(&mut None).expect("verifier copy"),
        checkpoints::default_projection_read_limits(),
        Some(64),
        &mut Some(&mut io),
    )
    .await?;
    assert_eq!(loaded.through_revision, 64);
    let snapshot = loaded.snapshot.expect("native selected checkpoint exists");
    assert_eq!(snapshot, original);
    assert_eq!(snapshot.events.last(), original.events.last());
    drop(snapshot);
    assert_eq!(io.into_custody()?.bytes(), 0);
    native
        .admit_retained(
            projection_codec_budget().peak_allocation_bytes
                - NativeMaterializationWork::accounting_state_bytes(),
        )
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn cold_projection_original_audience_and_cancellation_precede_sdk_reads() -> Result<()> {
    use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError};
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    for cancelled in [false, true] {
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(projection_codec_budget(), &cancellation)
            .expect("original preflight ledger");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native)
                .expect("original preflight custody"),
        );
        if cancelled {
            cancellation.cancel();
        }
        let other = crate::core::AuthorityIssuer::new(
            "other-projection",
            [9; 32],
            Authority {
                kind: crate::core::AggregateKind::Task,
                id: "another-audience".into(),
            },
        );
        let verifier = if cancelled {
            issuer().verifier(&mut None).expect("verifier copy")
        } else {
            other.verifier(&mut None).expect("verifier copy")
        };
        let error = checkpoints::load_with_work(
            &client,
            &authority(),
            &verifier,
            checkpoints::default_projection_read_limits(),
            Some(64),
            &mut Some(&mut io),
        )
        .await
        .err()
        .expect("original audience/cancellation rejects before SDK");
        if cancelled {
            assert!(matches!(
                error,
                Error::NativeWork {
                    cause: NativeTreeMaterializationError::Cancelled,
                    ..
                }
            ));
        } else {
            assert!(matches!(
                error,
                Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    ..
                }
            ));
        }
        assert_eq!(native.receipt().authority_records_read, 0);
        assert_eq!(io.into_custody()?.bytes(), 0);
    }
    let sdk = provider.io_snapshot();
    assert_eq!(
        sdk.reads + sdk.tails as u64 + sdk.writes + sdk.inspections + sdk.commit_reads,
        0
    );
    Ok(())
}
