use super::*;
use crate::{HeadProjection, HeadsRequest, RevisionCut};
use futures::FutureExt as _;

fn request(paths: &[&str]) -> Result<HeadsRequest, StreamError> {
    Ok(HeadsRequest {
        paths: paths
            .iter()
            .map(StreamPath::new)
            .collect::<Result<_, _>>()?,
        retained: Default::default(),
        projection: HeadProjection::Tail,
        durable_through: None,
        children: None,
        limit: paths
            .len()
            .try_into()
            .map_err(|_| StreamError::InvalidArgument)?,
        at_least: Default::default(),
    })
}

#[tokio::test]
async fn coherent_native_heads_keep_original_credit_and_leave_future_cut_append_runnable()
-> Result<(), Box<dyn std::error::Error>> {
    // The old unique-byte clone really allocates a shared header; the cold
    // native observation below must not defer this cost to its reader.
    let unique = OwnedBytes::from(vec![42; 16]);
    let mut promoted = None;
    let old = acyclic_test_support::measure(|| promoted = Some(unique.clone()));
    assert!(old.count_total > 0);
    drop(promoted);
    for local in [false, true] {
        let directory = tempfile::tempdir()?;
        let local_provider = if local {
            Some(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?)
        } else {
            None
        };
        let (provider, revisions): (Arc<dyn crate::NativeStreamProvider>, _) =
            if let Some(local) = &local_provider {
                (
                    Arc::new(local.clone()),
                    Arc::clone(&local.inner.provider.revisions),
                )
            } else {
                let memory = MemoryStream::default();
                let revisions = Arc::clone(&memory.revisions);
                (Arc::new(memory), revisions)
            };

        // Cold observation must not promote a hidden byte-owner header. Only
        // the exact concrete provider frame is allocated for one inline head.
        let cold = request(&["native/absent"])?;
        let meter = Arc::new(Meter::default());
        let mut cold_result = None;
        let physical = acyclic_test_support::measure(|| {
            cold_result = provider.native_heads(cold, meter.original()).now_or_never();
        });
        let output = cold_result.ok_or("cold capture unexpectedly parked")???;
        assert_eq!(
            physical.count_total,
            meter.allocations.load(Ordering::SeqCst)
        );
        assert_eq!(physical.count_total, 1);
        assert!(output.value().heads[0].is_none());
        drop(output);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);

        provider.append(keyed_append("native/heads", 0)?).await?;
        provider
            .fork(ForkRequest {
                source: StreamPath::new("native/heads")?,
                destination: StreamPath::new("native/empty")?,
                at_tail: Some(0),
                exclude: Vec::new(),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;

        for paths in [
            vec!["native/heads"],
            vec!["native/heads", "native/absent", "native/empty"],
        ] {
            let selected = request(&paths)?;
            let original = provider.heads(selected.clone()).await?;
            let meter = Arc::new(Meter::default());
            let output = provider
                .native_heads(selected.clone(), meter.original())
                .await??;
            output.value().validate_for(&selected)?;
            assert_eq!(output.value(), &original);
            assert_eq!(meter.examined.load(Ordering::SeqCst), paths.len() as u64);
            assert_eq!(
                meter.allocations.load(Ordering::SeqCst),
                if paths.len() == 1 { 1 } else { 2 }
            );
            let expected = if paths.len() == 1 {
                0
            } else {
                output.value().heads.capacity() * std::mem::size_of::<Option<crate::PathHead>>()
            };
            assert_eq!(meter.held.load(Ordering::SeqCst), expected as u64);
            assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
            assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
            drop(output);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            meter.assert_refunded();
        }

        // Original frame admission succeeds; output spill admission then fails
        // before the selected namespace is touched. Inline capture needs no spill.
        for paths in [vec!["native/heads"], vec!["native/heads", "native/absent"]] {
            let meter = Arc::new(Meter::default());
            let future = provider.native_heads(request(&paths)?, meter.original());
            meter.deny_allocation.store(true, Ordering::SeqCst);
            let result = future.await;
            if paths.len() == 1 {
                assert!(result.is_ok_and(|result| result.is_ok()));
            } else {
                let error = result.err().ok_or("unfunded spill was captured")?;
                assert_original_denial(&error, "allocation");
                assert_eq!(meter.examined.load(Ordering::SeqCst), 0);
            }
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            meter.assert_refunded();
        }

        let mut selected = request(&["native/heads", "native/absent"])?;
        let mut next = revisions.current();
        next.index += 1;
        selected.at_least = RevisionCut::of(next.clone());
        let meter = Arc::new(Meter::default());
        let mut parked = provider.native_heads(selected.clone(), meter.original());
        assert!((&mut parked).now_or_never().is_none());
        assert_eq!(meter.examined.load(Ordering::SeqCst), 0);
        if let Some(local) = &local_provider {
            assert!(
                local.inner.publication.try_write().is_ok(),
                "future cut retained the satisfying append's publication fence"
            );
        }
        provider.append(keyed_append("native/heads", 1)?).await?;
        let output = parked.await??;
        output.value().validate_for(&selected)?;
        assert_eq!(output.value().snapshot, next);
        assert_eq!(
            output.value().heads[0].as_ref().map(|head| head.tail),
            Some(2)
        );
        assert!(output.value().heads[1].is_none());
        drop(output);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();

        let mut foreign = selected.clone();
        let mut cut = next.clone();
        cut.incarnation ^= 1;
        foreign.at_least = RevisionCut::of(cut);
        let meter = Arc::new(Meter::default());
        assert!(matches!(
            provider.native_heads(foreign, meter.original()).await?,
            Err(StreamError::RevisionForeign)
        ));
        assert_eq!(meter.examined.load(Ordering::SeqCst), 0);

        next.index += 1;
        selected.at_least = RevisionCut::of(next);
        let mut cancelled = provider.native_heads(selected.clone(), meter.original());
        assert!((&mut cancelled).now_or_never().is_none());
        drop(cancelled);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        let mut lost = provider.native_heads(selected, meter.original());
        assert!((&mut lost).now_or_never().is_none());
        revisions.close();
        assert!(matches!(lost.await?, Err(StreamError::Unavailable)));
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();

        // An unimplemented native projection cannot leak into ordinary work.
        let mut history = request(&["native/heads"])?;
        history.projection = HeadProjection::Watermarks;
        let meter = Arc::new(Meter::default());
        assert!(matches!(
            provider.native_heads(history, meter.original()).await?,
            Err(StreamError::Unsupported)
        ));
        assert_eq!(meter.allocations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.examined.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn native_history_keeps_original_records_provenance_and_funding_at_one_cut()
-> Result<(), Box<dyn std::error::Error>> {
    for local in [false, true] {
        let directory = tempfile::tempdir()?;
        let provider: Arc<dyn crate::NativeStreamProvider> = if local {
            Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?)
        } else {
            Arc::new(MemoryStream::default())
        };
        provider.append(keyed_append("history/source", 0)?).await?;
        provider
            .append(keyed_append("history/source/state", 0)?)
            .await?;
        provider
            .fork(ForkRequest {
                source: StreamPath::new("history/source")?,
                destination: StreamPath::new("history/child")?,
                at_tail: Some(0),
                exclude: Vec::new(),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        for path in [
            "history/source",
            "history/child",
            "history/child/state",
            "history/missing",
        ] {
            let mut selected = request(&[path])?;
            selected.projection = HeadProjection::History;
            let mut expected = provider.heads(selected.clone()).await?;
            // Native observation makes no persistence claim or durability join.
            for head in expected.heads.iter_mut().flatten() {
                head.durable = 0;
            }
            let meter = Arc::new(Meter::default());
            let future = provider.native_heads(selected.clone(), meter.original());
            let mut observed = None;
            let physical = acyclic_test_support::measure(|| {
                observed = future.now_or_never();
            });
            let output = observed.ok_or("history capture unexpectedly parked")???;
            output.value().validate_for(&selected)?;
            assert_eq!(output.value(), &expected);
            // The frame was admitted before measurement. Only reconstructed
            // descendant provenance needs two new physical allocations.
            let extra = u64::from(path == "history/child/state") * 2;
            assert_eq!(physical.count_total, extra);
            assert_eq!(meter.allocations.load(Ordering::SeqCst), extra + 1);
            assert_eq!(
                meter.read.load(Ordering::SeqCst),
                expected.heads[0]
                    .as_ref()
                    .and_then(|head| head.last.as_ref())
                    .map_or(0, |record| record.payload_bytes() as u64)
            );
            assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
            assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
            let retained_path = output.value().heads[0]
                .as_ref()
                .and_then(|head| head.origin.as_ref())
                .map(|origin| origin.source.clone());
            drop(output);
            assert_eq!(meter.held.load(Ordering::SeqCst) != 0, extra != 0);
            drop(retained_path);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            meter.assert_refunded();
        }

        let mut selected = request(&["history/child/state"])?;
        selected.projection = HeadProjection::History;
        // Refuse original record delivery before cloning it, without substituting
        // an ordinary head/read. The provider and original publication survive.
        let denied = Arc::new(Meter {
            read_limit: 0,
            ..Meter::default()
        });
        assert!(
            provider
                .native_heads(selected.clone(), denied.original())
                .await
                .is_err()
        );
        assert_eq!(denied.read.load(Ordering::SeqCst), 0);
        assert_eq!(denied.held.load(Ordering::SeqCst), 0);
        denied.assert_refunded();

        // Admit the frame, then deny the provenance payload allocation. No
        // reconstructed path escapes, and all original work is refunded.
        let denied = Arc::new(Meter::default());
        let future = provider.native_heads(selected.clone(), denied.original());
        denied.deny_allocation.store(true, Ordering::SeqCst);
        let error = future.await.err().ok_or("unfunded provenance escaped")?;
        assert_original_denial(&error, "allocation");
        assert_eq!(denied.allocations.load(Ordering::SeqCst), 1);
        assert_eq!(denied.held.load(Ordering::SeqCst), 0);
        denied.assert_refunded();
        let healthy = Arc::new(Meter::default());
        let output = provider
            .native_heads(selected.clone(), healthy.original())
            .await??;
        let head = output.value().heads[0]
            .as_ref()
            .ok_or("lost inherited state")?;
        assert_eq!(
            head.last.as_ref().ok_or("lost record")?.origin.as_str(),
            "history/source/state"
        );
        assert_eq!(
            head.origin
                .as_ref()
                .ok_or("lost provenance")?
                .source
                .as_str(),
            "history/source/state"
        );
        drop(output);
        healthy.assert_refunded();
        selected.durable_through = Some(1);
        let unsupported = Arc::new(Meter::default());
        assert!(matches!(
            provider
                .native_heads(selected, unsupported.original())
                .await?,
            Err(StreamError::Unsupported)
        ));
        assert_eq!(unsupported.allocations.load(Ordering::SeqCst), 0);
    }
    Ok(())
}
