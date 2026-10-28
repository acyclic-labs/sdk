//! A durable receipt, unlike a commit acknowledgement, survives a power cut.
use super::*;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn path() -> StreamPath {
    StreamPath::new("receipts").unwrap()
}

async fn append(provider: &LocalStream, record: &'static [u8]) -> Result {
    let outcome = provider
        .append(AppendRequest {
            path: path(),
            records: vec![Bytes::from_static(record).into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    assert!(matches!(outcome, crate::AppendOutcome::Committed(_)));
    Ok(())
}

/// Journal bytes the device holds: everything only while every frame is synced.
fn synced_bytes(provider: &LocalStream) -> Option<u64> {
    let journal = provider.inner.journal.lock().ok()?;
    (journal.flushed_sequence == Some(journal.appended)).then_some(journal.bytes)
}

/// Drops the provider and keeps only the journal bytes the device confirmed.
async fn power_cut(
    directory: &std::path::Path,
    provider: LocalStream,
    durable_bytes: u64,
) -> Result<LocalStream> {
    drop(provider);
    std::fs::OpenOptions::new()
        .write(true)
        .open(directory.join(JOURNAL_FILE))?
        .set_len(durable_bytes)?;
    Ok(LocalStream::open(directory, LocalStreamLimits::default()).await?)
}

/// `barrier` asks for a receipt after the deferred append; without it the
/// acknowledged record is all the writer holds, which is the negative control.
async fn survives_cut(barrier: bool) -> Result<u64> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    append(&provider, b"base").await?;
    let mut durable_bytes =
        synced_bytes(&provider).ok_or("required append left frames unsynced")?;
    deferring_durability(async {
        append(&provider, b"approval").await?;
        let observed = provider.watermarks(path()).await?;
        assert!(
            crate::DurableReceipt::covering(path(), &observed, 2).is_none(),
            "a deferred acknowledgement observed as durable"
        );
        if barrier {
            let receipt = provider
                .durable_receipt(path(), 2)
                .await?
                .ok_or("the barrier issued no receipt")?;
            assert_eq!((receipt.path(), receipt.through()), (&path(), 2));
            durable_bytes =
                synced_bytes(&provider).ok_or("receipt issued before the device sync")?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await?;
    let provider = power_cut(directory.path(), provider, durable_bytes).await?;
    Ok(provider.tail(path()).await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receipt_covers_a_deferred_record_across_a_power_cut() -> Result {
    assert_eq!(survives_cut(true).await?, 2);
    Ok(())
}

/// Negative control: the acknowledgement alone, made under deferred
/// durability, is lost by the same cut.
#[tokio::test(flavor = "multi_thread")]
async fn negative_control_a_deferred_acknowledgement_is_lost_by_a_power_cut() -> Result {
    assert_eq!(survives_cut(false).await?, 1);
    Ok(())
}

async fn foreign_provider_survives_cut(scoped: bool) -> Result<u64> {
    let selected_directory = tempfile::tempdir()?;
    let foreign_directory = tempfile::tempdir()?;
    let selected = LocalStream::open(selected_directory.path(), Default::default()).await?;
    let foreign = LocalStream::open(foreign_directory.path(), Default::default()).await?;
    append(&selected, b"base").await?;
    append(&foreign, b"base").await?;
    let mut foreign_durable = synced_bytes(&foreign).ok_or("base was not durable")?;
    let cloned = selected.clone();
    let publication = async {
        append(&cloned, b"deferred").await?;
        assert!(synced_bytes(&selected).is_none());
        append(&foreign, b"independent authority").await?;
        if let Some(bytes) = synced_bytes(&foreign) {
            foreign_durable = bytes;
        }
        selected
            .durable_receipt(path(), 2)
            .await?
            .ok_or("selected journal receipt missing")?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    if scoped {
        deferring_provider_durability(&selected, publication).await?;
    } else {
        // Execute the former ambient scope: the selected journal's receipt
        // cannot make another authority's acknowledged append survive a cut.
        deferring_durability(publication).await?;
    }
    append(&selected, b"scope ended").await?;
    assert!(synced_bytes(&selected).is_some());
    let foreign = power_cut(foreign_directory.path(), foreign, foreign_durable).await?;
    Ok(foreign.tail(path()).await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn provider_scoped_deferral_cannot_defer_a_foreign_authority() -> Result {
    assert_eq!(foreign_provider_survives_cut(true).await?, 2);
    assert_eq!(foreign_provider_survives_cut(false).await?, 1);
    Ok(())
}

async fn native_publication(provider: &LocalStream, entry: u8) -> Result<Arc<Meter>> {
    let meter = Arc::new(Meter::local());
    let records = vec![
        if entry == 2 {
            Bytes::new()
        } else {
            Bytes::from_static(b"native publication")
        }
        .into(),
    ];
    if entry == 0 {
        let outcome = provider
            .native_append(
                AppendRequest {
                    path: path(),
                    records,
                    if_tail: Some(1),
                    idempotency_key: crate::IdempotencyKey::unique(),
                },
                meter.original(),
            )
            .await??;
        assert!(matches!(
            outcome.value(),
            crate::AppendOutcome::Committed(_)
        ));
    } else {
        let request = CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![
                crate::CommitCondition::Tail {
                    path: path(),
                    expected: 1,
                },
                crate::CommitCondition::Tail {
                    path: StreamPath::new("native-deferral-owner")?,
                    expected: 1,
                },
            ],
            mutations: vec![crate::CommitMutation::Append {
                path: path(),
                records,
            }],
            idempotency_key: IdempotencyKey::new("native-deferral")?,
        };
        let outcome = if entry == 1 {
            provider
                .native_commit_guarded(request, None, meter.original(), || {
                    std::future::ready(Ok::<_, StreamError>(()))
                })
                .await??
        } else {
            let packet = provider
                .native_prepare_commit(request, 64, meter.original())
                .await??;
            let (packet, allocation) = packet.into_parts();
            let result = packet
                .finish(Bytes::from_static(b"native publication").into())
                .await??;
            drop(allocation);
            result
        };
        assert!(matches!(
            outcome.value(),
            crate::CommitOutcome::Committed(_)
        ));
    }
    Ok(meter)
}

#[tokio::test(flavor = "multi_thread")]
async fn native_publication_workers_preserve_provider_scope_and_both_device_prefixes() -> Result {
    for entry in 0..3 {
        for scoped in [false, true] {
            let selected_directory = tempfile::tempdir()?;
            let foreign_directory = tempfile::tempdir()?;
            let selected = LocalStream::open(selected_directory.path(), Default::default()).await?;
            let foreign = LocalStream::open(foreign_directory.path(), Default::default()).await?;
            append(&selected, b"base").await?;
            append(&foreign, b"base").await?;
            // Prepared ingress owns an exact owner-tail and target-tail pair;
            // both fences exist before the deferred publication begins.
            for provider in [&selected, &foreign] {
                provider
                    .append(AppendRequest {
                        path: StreamPath::new("native-deferral-owner")?,
                        records: vec![Bytes::from_static(b"owner").into()],
                        if_tail: Some(0),
                        idempotency_key: crate::IdempotencyKey::unique(),
                    })
                    .await?;
            }
            let mut foreign_bytes = synced_bytes(&foreign).ok_or("base was not durable")?;
            let selected_bytes = synced_bytes(&selected).ok_or("base was not durable")?;
            let mut meters = Vec::new();
            let publication = async {
                meters.push(native_publication(&selected, entry).await?);
                assert!(
                    synced_bytes(&selected).is_none(),
                    "native worker lost its deferral"
                );
                meters.push(native_publication(&foreign, entry).await?);
                if let Some(bytes) = synced_bytes(&foreign) {
                    foreign_bytes = bytes;
                }
                assert_eq!(synced_bytes(&foreign).is_some(), scoped);
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            };
            if scoped {
                deferring_provider_durability(&selected, publication).await?;
            } else {
                // Former ambient grouping loses a separate authority's write.
                deferring_durability(publication).await?;
            }
            let foreign = power_cut(foreign_directory.path(), foreign, foreign_bytes).await?;
            assert_eq!(foreign.tail(path()).await?, if scoped { 2 } else { 1 });
            if scoped {
                selected
                    .durable_receipt(path(), 2)
                    .await?
                    .ok_or("native receipt missing")?;
                let bytes = synced_bytes(&selected).ok_or("native receipt preceded device sync")?;
                let selected = power_cut(selected_directory.path(), selected, bytes).await?;
                assert_eq!(selected.tail(path()).await?, 2);
                append(&selected, b"scope ended").await?;
                assert!(synced_bytes(&selected).is_some());
            } else {
                let selected =
                    power_cut(selected_directory.path(), selected, selected_bytes).await?;
                assert_eq!(
                    selected.tail(path()).await?,
                    1,
                    "accepted native write survived without a receipt"
                );
            }
            for meter in meters {
                meter.assert_refunded();
            }
        }
    }
    Ok(())
}

#[cfg(target_vendor = "apple")]
async fn survives_barrier_cut(receipt: bool) -> Result<u64> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    append(&provider, b"base").await?;
    let mut flushed_bytes = synced_bytes(&provider).ok_or("base was not flushed")?;
    drop(provider);
    let provider = LocalStream::open(
        directory.path(),
        LocalStreamLimits {
            durability: LocalDurability::Barrier,
            ..LocalStreamLimits::default()
        },
    )
    .await?;
    append(&provider, b"approval").await?;
    assert_eq!(provider.inner.synced.borrow().ordered, 1);
    assert_eq!(provider.inner.synced.borrow().flushed, None);
    let marks = provider.watermarks(path()).await?;
    assert!(crate::DurableReceipt::covering(path(), &marks, 2).is_none());
    if receipt {
        let proof = provider
            .durable_receipt(path(), 2)
            .await?
            .ok_or("missing full flush")?;
        assert_eq!(proof.through(), 2);
        flushed_bytes = synced_bytes(&provider).ok_or("receipt was not flushed")?;
    }
    Ok(power_cut(directory.path(), provider, flushed_bytes)
        .await?
        .tail(path())
        .await?)
}

#[cfg(target_vendor = "apple")]
#[tokio::test(flavor = "multi_thread")]
async fn barrier_needs_a_device_flush_before_a_receipt_survives_power_loss() -> Result {
    assert_eq!(survives_barrier_cut(true).await?, 2);
    Ok(())
}

#[cfg(target_vendor = "apple")]
#[tokio::test(flavor = "multi_thread")]
async fn negative_control_barrier_acknowledgement_is_lost_by_power_loss() -> Result {
    assert_eq!(survives_barrier_cut(false).await?, 1);
    Ok(())
}

/// Model the ordered-only completion available on Apple on every platform.
/// Every phase-barrier adapter must upgrade that cut to device-flushed proof.
#[tokio::test(flavor = "multi_thread")]
async fn phase_barriers_upgrade_ordered_prefixes_to_device_flushed_proofs() -> Result {
    let mut proofs = [false; 3];
    for (entry, proof) in proofs.iter_mut().enumerate() {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        append(&provider, b"ordered approval").await?;
        {
            let mut journal = provider.inner.journal.lock().map_err(|_| "journal")?;
            journal.flushed_sequence = None;
        }
        provider
            .inner
            .synced
            .send_modify(|progress| progress.flushed = None);
        assert!(synced_bytes(&provider).is_none());
        assert_eq!(provider.watermarks(path()).await?.durable, 0);
        let meter = Arc::new(Meter::local());
        match entry {
            0 => provider.durability_barrier().await?,
            1 => drop(
                provider
                    .native_durability_barrier(meter.original())
                    .await??,
            ),
            _ => drop(
                provider
                    .prepare_native_durability_barrier(meter.original())?
                    .await??,
            ),
        }
        *proof =
            synced_bytes(&provider).is_some() && provider.watermarks(path()).await?.durable == 1;
        meter.assert_refunded();
    }
    // Collect every adapter before asserting so the actual old implementation
    // is exercised through all three entry points even when the first is bad.
    assert_eq!(
        proofs, [true; 3],
        "phase barrier returned an ordered-only proof"
    );
    Ok(())
}
