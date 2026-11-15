use super::*;
use crate::workspace::ForkOptions;

#[tokio::test]
async fn construction_after_the_real_marker_cut_survives_private_collection()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_native_runtime::testing::Schedule;
    for bypass_admission in [false, true] {
        let directory = tempfile::tempdir()?;
        let cancel = CancellationToken::new();
        let fs = crate::Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let store = fs.objects();
        let (schedule, mut events) = Schedule::new();
        let mut collection = Box::pin(schedule.scope(fs.collect_local_garbage(None, &cancel)));
        let held = loop {
            tokio::select! {
                biased;
                boundary = events.recv() => {
                    let boundary = boundary.ok_or("collection marker boundary was lost")?;
                    if boundary.name == "filesystem:collection-marked" { break boundary; }
                    boundary.release();
                }
                result = collection.as_mut() => {
                    result?;
                    return Err("collection ended before its marker cut".into());
                }
            }
        };
        let expected = OwnedBytes::from_static(b"child constructed after the marker snapshot");
        let object = HashedObject::new(ObjectKind::BlobChunk, expected.clone());
        let id = object.object_id();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        if bypass_admission {
            // Negative control: install actual private bytes without the common
            // admission barrier. The already-complete marker cannot see them.
            store.index_mut().admit_resident(object)?;
        } else {
            store.put_hashed(object, &mut work).await?;
        }
        held.release();
        let result = loop {
            tokio::select! {
                biased;
                boundary = events.recv() => boundary.ok_or("collection completion was lost")?.release(),
                result = collection.as_mut() => break result?,
            }
        };
        drop(collection);
        schedule.drained().await;
        assert_eq!(result.removed, u64::from(bypass_admission));
        let mut reading = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        let read = store.read(id, expected.len() as u64, &mut reading).await;
        if bypass_admission {
            assert!(
                matches!(read, Err(ObjectStoreError::Missing)),
                "omitted-admission negative did not lose actual bytes"
            );
        } else {
            assert_eq!(read?.bytes, expected);
            drop(work);
            assert_eq!(fs.collect_local_garbage(None, &cancel).await?.removed, 1);
        }
        assert!(
            fs.local_account().list(None, 1).await?.digests.is_empty(),
            "unfinished construction published durable bytes"
        );
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[tokio::test]
async fn cancelled_collection_keeps_native_deallocation_before_extent_reuse()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_native_runtime::testing::Schedule;
    for (cut, bypass_order) in [
        ("native:submitted", false),
        ("native:completed", false),
        ("native:submitted", true),
    ] {
        let directory = tempfile::tempdir()?;
        let (store, _provider) = open_staged(directory.path()).await?;
        let cancel = CancellationToken::new();
        let (dead, _) = spill_bytes_fixture(&store, &cancel, vec![17; 1024 * 1024].into()).await?;
        let (peer, peer_bytes) =
            spill_bytes_fixture(&store, &cancel, vec![29; 65536].into()).await?;
        let Lookup::Spilled {
            offset,
            stored_length,
            ..
        } = store.lookup(dead)
        else {
            return Err("discarded fixture was not spilled".into());
        };
        let collecting = store.collection.begin().await;
        let marked = HashSet::from([peer.digest]);
        let (schedule, mut events) = Schedule::new();
        let mut sweep =
            Box::pin(schedule.scope(store.sweep_private(&collecting, &marked, &cancel)));
        let held = loop {
            tokio::select! {
                biased;
                boundary = events.recv() => {
                    let boundary = boundary.ok_or("deallocation boundary was lost")?;
                    if boundary.name == cut { break boundary; }
                    boundary.release();
                }
                result = sweep.as_mut() => {
                    result?;
                    return Err("collection ended before its physical cut".into());
                }
            }
        };
        // Withdraw the real collection observer after native acceptance. The
        // accepted physical operation must retain ordering independently.
        drop(sweep);
        let expected = vec![53; stored_length as usize];
        let mut completed_replacement = None;
        let replacement = if bypass_order {
            // Negative control: deliberately write outside the native driver's
            // accepted fence. The delayed hole must erase these real bytes.
            use std::io::{Seek, SeekFrom, Write};
            let spill = store.spill_io.read().await;
            let mut raw = spill
                .as_ref()
                .ok_or("missing spill")?
                .as_file()
                .try_clone()?;
            raw.seek(SeekFrom::Start(offset))?;
            raw.write_all(&expected)?;
            None
        } else {
            let mut replacement = Box::pin(spill_bytes_fixture(
                &store,
                &cancel,
                expected.clone().into(),
            ));
            if cut == "native:submitted" {
                assert!(
                    futures::poll!(replacement.as_mut()).is_pending(),
                    "extent reuse escaped an accepted deallocation"
                );
                Some(replacement)
            } else {
                // Physical completion, rather than delivery to the cancelled
                // observer, releases the original file fence.
                completed_replacement = Some(replacement.await?);
                None
            }
        };
        held.release();
        // Keep driving the original accepted operation after its observer has
        // gone. No time or readiness loop decides when it has completed.
        let drained = schedule.drained();
        tokio::pin!(drained);
        loop {
            tokio::select! {
                biased;
                boundary = events.recv() => boundary.ok_or("native completion was lost")?.release(),
                () = &mut drained => break,
            }
        }
        let replacement = match replacement {
            Some(replacement) => Some(replacement.await?),
            None => completed_replacement,
        };
        let id = if let Some((id, _)) = replacement {
            assert!(
                matches!(store.lookup(id), Lookup::Spilled { offset: actual, .. } if actual == offset)
            );
            id
        } else {
            dead
        };
        let spill = store.spill_io.read().await;
        let actual = read_spilled(
            spill.as_ref().ok_or("missing spill")?,
            &[(id, offset, stored_length)],
        )
        .await?
        .pop()
        .ok_or("missing physical replacement")?;
        assert_eq!(
            actual.as_slice() == expected.as_slice(),
            !bypass_order,
            "physical-byte oracle missed the unfenced-reuse negative control"
        );
        drop(spill);
        let mut reading = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        assert_eq!(
            store.read(peer, 65536, &mut reading).await?.bytes,
            peer_bytes
        );
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[tokio::test]
async fn collection_deallocates_private_blocks_without_moving_a_live_peer()
-> Result<(), Box<dyn std::error::Error>> {
    fn allocation(file: &std::fs::File) -> std::io::Result<u64> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(file.metadata()?.blocks() * 512)
        }
        #[cfg(windows)]
        {
            let stat =
                crate::native_host::stat_file(file, &mut crate::native_host::ordinary_work())
                    .map_err(crate::native_host::ordinary_error)?;
            u64::try_from(stat.raw.AllocationSize).map_err(std::io::Error::other)
        }
    }
    for omit_collection in [false, true] {
        let directory = tempfile::tempdir()?;
        let (store, _provider) = open_staged(directory.path()).await?;
        let cancel = CancellationToken::new();
        let (dead, _) =
            spill_bytes_fixture(&store, &cancel, vec![17; 4 * 1024 * 1024].into()).await?;
        let peer_bytes = vec![29; 1024 * 1024];
        let (peer, _) = spill_bytes_fixture(&store, &cancel, peer_bytes.clone().into()).await?;
        let (before, peer_offset) = {
            let spill = store.spill_io.read().await;
            let offset = match store.index().lookup(peer) {
                Lookup::Spilled { offset, .. } => offset,
                _ => return Err("peer was not spilled".into()),
            };
            (
                allocation(spill.as_ref().ok_or("missing spill")?.as_file())?,
                offset,
            )
        };
        let collecting = store.collection.begin().await;
        if !omit_collection {
            let marked = HashSet::from([peer.digest]);
            assert_eq!(
                store
                    .sweep_private(&collecting, &marked, &cancel)
                    .await?
                    .removed,
                1
            );
            assert!(matches!(store.index().lookup(dead), Lookup::Absent));
        }
        let spill = store.spill_io.read().await;
        let file = spill.as_ref().ok_or("live peer lost its file")?;
        let after = allocation(file.as_file())?;
        assert_eq!(
            after < before,
            !omit_collection,
            "physical allocation oracle missed omitted-collection negative control"
        );
        let recovered = read_spilled(file, &[(peer, peer_offset, peer_bytes.len() as u64)]).await?;
        assert_eq!(recovered[0].as_slice(), peer_bytes.as_slice());
    }
    Ok(())
}

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
    // A fork may restage immutable base metadata already in durable Objects.
    // Those marked source copies are not private fork garbage. Verify the
    // actual durable provider, bypassing both staging and the object cache.
    let remaining = store.index().objects.keys().copied().collect::<Vec<_>>();
    for id in remaining {
        let mut reading = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        assert!(
            store.inner().inner().contains(id, &mut reading).await?,
            "settled fork retained a private-only object: {id:?}"
        );
    }
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
        let actual;
        if early_release {
            assert_eq!(sweep.await?.removed, 1);
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
            assert!(
                futures::poll!(sweep.as_mut()).is_pending(),
                "collector did not obey the accepted-read lease"
            );
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
