use super::*;
use crate::workspace::ForkOptions;

#[tokio::test]
async fn real_collection_retains_unfinished_children_and_detects_early_root_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    for early_retirement in [false, true] {
        let directory = tempfile::tempdir()?;
        let cancel = CancellationToken::new();
        let fs = crate::Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let store = fs.objects();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        let mut children = Vec::new();
        // Cross both the resident window and the 256-candidate collection page.
        for child in 0_u64..600 {
            let mut bytes = vec![91; 8_192];
            bytes[..8].copy_from_slice(&child.to_le_bytes());
            let object = HashedObject::new(ObjectKind::BlobChunk, bytes.into());
            children.push(object.object_id());
            store.put_hashed(object, &mut work).await?;
        }
        assert!(store.spill_io.read().await.is_some());
        if early_retirement {
            // Negative control: lose custody before the unfinished parent exists.
            work.construction = Default::default();
        }
        let collected = fs.collect_local_garbage(None, &cancel).await?;
        assert_eq!(collected.removed, if early_retirement { 600 } else { 0 });
        for child in [0, 299, 599] {
            let mut reading = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
            let read = store.read(children[child], 8_192, &mut reading).await;
            if early_retirement {
                assert!(matches!(read, Err(ObjectStoreError::Missing)));
            } else {
                let read = read?;
                assert_eq!(&read.bytes[..8], &(child as u64).to_le_bytes());
                assert!(read.bytes[8..].iter().all(|&byte| byte == 91));
            }
        }
        drop(work);
        let collected = fs.collect_local_garbage(None, &cancel).await?;
        if !early_retirement {
            assert_eq!(collected.removed, 600);
        }
        assert!(store.index().objects.is_empty());
        assert!(store.spill_io.read().await.is_none());
        assert!(
            fs.local_account().list(None, 1).await?.digests.is_empty(),
            "uncommitted children reached durable Objects"
        );
    }
    Ok(())
}

#[tokio::test]
async fn live_host_collection_retires_settled_private_forks_without_publishing_or_harming_a_peer()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancel = CancellationToken::new();
    let fs = crate::Fs::local(crate::LocalOptions::new(directory.path())).await?;
    let source = fs
        .create_workspace("collection-source", WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value;
    source.write_text("/base", "durable source").await?;
    let inventory = fs.local_account().list(None, 1_024).await?;
    assert!(inventory.next.is_none());
    let (peer, peer_lease) = source
        .fork_ephemeral(
            "collection-peer",
            ForkOptions::from_generation(
                source.head(WorkBudget::UNBOUNDED, &cancel).await?.value,
                crate::IdempotencyKey::new(),
            ),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let (discarded, discarded_lease) = source
        .fork_ephemeral(
            "collection-discarded",
            ForkOptions::from_generation(
                source.head(WorkBudget::UNBOUNDED, &cancel).await?.value,
                crate::IdempotencyKey::new(),
            ),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let payload = (0..6 * 1_024 * 1_024_u64)
        .map(|offset| (offset.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 32) as u8)
        .collect::<Vec<_>>();
    peer.write("/peer", bytes::Bytes::copy_from_slice(&payload))
        .await?;
    let private = payload.iter().map(|byte| byte ^ 0x5a).collect::<Vec<_>>();
    discarded.write("/discarded", private.into()).await?;
    let store = fs.objects();
    assert!(store.spill_io.read().await.is_some());
    discarded_lease.settle().await?;
    drop(discarded);

    // Both leases were created from the same source, so collection must keep
    // shared base bytes and an idle peer's privately committed head/history.
    let collected = fs.collect_local_garbage(None, &cancel).await?;
    assert!(
        collected.removed > 0,
        "settled private entries were not swept"
    );
    assert_eq!(
        peer.read(
            "/peer",
            payload.len() as u64,
            WorkBudget::UNBOUNDED,
            &cancel
        )
        .await?
        .value
        .as_ref(),
        payload.as_slice()
    );
    assert_eq!(
        peer.read("/base", 64, WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value
            .as_ref(),
        b"durable source"
    );
    assert_eq!(
        fs.local_account().list(None, 1_024).await?.digests,
        inventory.digests
    );

    peer_lease.settle().await?;
    drop(peer);
    // Negative control: opening the cut without its private sweep leaves
    // the actual unlinked-file owner behind after every private fork settles.
    let omitted_private_phase = store.collection.begin().await;
    drop(omitted_private_phase);
    assert!(store.spill_io.read().await.is_some());
    fs.collect_local_garbage(None, &cancel).await?;
    assert!(store.index().objects.is_empty());
    assert!(
        store.spill_io.read().await.is_none(),
        "an empty private area retained its unlinked file"
    );
    assert_eq!(
        source
            .read("/base", 64, WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value
            .as_ref(),
        b"durable source"
    );
    assert_eq!(
        fs.local_account().list(None, 1_024).await?.digests,
        inventory.digests
    );
    Ok(())
}

#[tokio::test]
async fn collection_waits_for_an_accepted_spill_identity_before_offset_reuse()
-> Result<(), Box<dyn std::error::Error>> {
    for early_release in [false, true] {
        let directory = tempfile::tempdir()?;
        let (store, _provider) = open_staged(directory.path()).await?;
        let cancel = CancellationToken::new();
        let (old, expected) = spill_bytes_fixture(&store, &cancel, vec![11; 64].into()).await?;
        let (peer, peer_bytes) = spill_bytes_fixture(&store, &cancel, vec![12; 96].into()).await?;
        let collecting = store.collection.begin().await;
        let mut accepted = Some(store.spill_io.read().await);
        let Lookup::Spilled {
            offset,
            stored_length,
            ..
        } = store.lookup(old)
        else {
            panic!("fixture did not spill the accepted identity");
        };
        if early_release {
            // Negative control: keep a stale accepted offset without its lease.
            drop(accepted.take());
        }
        let marked = HashSet::from([peer.digest]);
        // This proof schedules primitive events, independently of Tokio's task budget.
        let sweep = tokio::task::unconstrained(store.sweep_private(&collecting, &marked, &cancel));
        tokio::pin!(sweep);
        let first = futures::poll!(sweep.as_mut());
        assert_eq!(
            first.is_ready(),
            early_release,
            "collector did not obey the accepted-read lease"
        );
        let actual;
        if let std::task::Poll::Ready(result) = first {
            assert_eq!(result?.removed, 1);
            // Keeping the peer spilled makes this the same real physical file.
            spill_bytes_fixture(&store, &cancel, vec![13; 64].into()).await?;
            let file = store.spill_io.read().await;
            actual = read_spilled(
                file.as_ref().ok_or("missing spill")?,
                &[(old, offset, stored_length)],
            )
            .await?
            .pop()
            .ok_or("missing recovered bytes")?;
        } else {
            let file = accepted.as_ref().ok_or("accepted lease was lost")?;
            actual = read_spilled(
                file.as_ref().ok_or("missing spill")?,
                &[(old, offset, stored_length)],
            )
            .await?
            .pop()
            .ok_or("missing recovered bytes")?;
            drop(accepted.take());
            assert_eq!(sweep.await?.removed, 1);
            spill_bytes_fixture(&store, &cancel, vec![13; 64].into()).await?;
        }
        assert_eq!(
            actual == expected,
            !early_release,
            "physical-byte oracle missed stale-offset negative control"
        );
        let mut reading = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        assert_eq!(store.read(peer, 96, &mut reading).await?.bytes, peer_bytes);
    }
    Ok(())
}
