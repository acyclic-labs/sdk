use super::*;
use crate::v1::memory::persistence::tests::{bucket, put, reopen, seeded};
use crate::v1::tests::get;
use std::sync::atomic::Ordering;

mod meter;

#[tokio::test]
async fn checkpoint_streams_bounded_frames_and_preserves_reopen_and_retry()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let bound = PACK_LIMIT as u64 * 8;
    let mut peaks = Vec::new();
    for count in [1, 10, 100, 1_000] {
        let root = tempfile::tempdir()?;
        let core = seeded(root.path()).await?;
        for start in (0..count).step_by(32) {
            let requests = (start..(start + 32).min(count))
                .map(|number| {
                    let mut query = put(&format!("key-{number:04}"));
                    query.metadata = Some(wire::ObjectMetadata {
                        content_type: "a".repeat(896),
                        ..Default::default()
                    });
                    (query, OwnedBytes::from_static(b"body"))
                })
                .collect();
            for result in core.put_batch(requests).await {
                result?;
            }
        }
        let journal = core.journal.as_ref().ok_or("missing journal")?;
        let mut state = core.lock_state()?.clone();
        journal.materialize_inline(&mut state, None)?;
        let meter = Arc::new(meter::Meter::default());
        journal.compact(&state, Some(meter.clone()))?;
        assert_eq!(
            meter.held.load(Ordering::SeqCst),
            0,
            "completed frames release their original custody"
        );
        let peak = meter.peak.load(Ordering::SeqCst);
        assert!(peak <= bound, "{count} objects: {peak} > {bound}");
        peaks.push(peak);

        if count == 1_000 {
            // Run the removed production shape with the same state and codec.
            let control = Arc::new(meter::Meter::default());
            let mut publication = Publication::new(Some(control.clone()));
            let snapshot = difference(&State::default(), &state, 0, &mut publication)?;
            assert!(
                control.peak.load(Ordering::SeqCst) > bound,
                "whole-snapshot negative control must breach the frame bound"
            );
            drop(snapshot);
            drop(publication);
            assert_eq!(control.held.load(Ordering::SeqCst), 0);
        }
        drop(state);
        drop(core);
        let core = reopen(root.path())?;
        assert_eq!(
            core.get(get(bucket(), "key-0000"), 4).await?.into_body(),
            OwnedBytes::from_static(b"body")
        );
        let mut query = put(&format!("key-{:04}", count - 1));
        query.metadata = Some(wire::ObjectMetadata {
            content_type: "a".repeat(896),
            ..Default::default()
        });
        let before = core
            .put(query.clone(), OwnedBytes::from_static(b"body"))
            .await?;
        assert_eq!(
            core.put(query, OwnedBytes::from_static(b"body")).await?,
            before
        );
    }
    let [_, _, after100, after1000] = peaks.as_slice() else {
        return Err("missing checkpoint samples".into());
    };
    assert!(
        after1000 <= &(after100 * 2),
        "a tenfold history increase must stay within one packing frame"
    );
    eprintln!("checkpoint_native_peak_bytes_1_10_100_1000={peaks:?}");
    Ok(())
}

#[test]
fn checkpoint_packing_length_matches_protobuf_for_every_change_family()
-> Result<(), Box<dyn std::error::Error>> {
    let mut checkpoint = Checkpoint {
        file: tempfile::tempfile()?,
        limits: LocalObjectsLimits::default(),
        operations: 0,
        bytes: 0,
        pending: Pending::new(u64::MAX, None),
    };
    for length in [0, 1, 127, 128, 16_383] {
        let items = [
            Change::Bucket(BucketChange {
                name: "b".repeat(length),
                ..Default::default()
            }),
            Change::Object(ObjectChange {
                key: "o".repeat(length),
                ..Default::default()
            }),
            Change::Upload(UploadChange {
                id: "u".repeat(length),
                ..Default::default()
            }),
            Change::Part(PartChange {
                upload: "p".repeat(length),
                ..Default::default()
            }),
            Change::Receipt(ReceiptChange {
                key: "r".repeat(length),
                ..Default::default()
            }),
        ];
        for item in items {
            checkpoint.append(|_| Ok(item))?;
            assert_eq!(
                checkpoint.pending.encoded_len(),
                checkpoint.pending.delta.encoded_len()
            );
        }
    }
    // Counting only item bodies omits repeated-message tags and lengths.
    assert_ne!(
        checkpoint.pending.encoded_len(),
        checkpoint
            .pending
            .delta
            .receipts
            .iter()
            .map(Message::encoded_len)
            .sum::<usize>()
    );
    Ok(())
}
