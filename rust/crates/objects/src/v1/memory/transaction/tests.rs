use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;

fn query(bucket: &wire::BucketRef, key: &str) -> wire::PutObjectHeader {
    wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: key.into(),
        ..Default::default()
    }
}

fn overwrite_allocations(entries: usize, keep_snapshot: bool) -> AllocationInfo {
    let (core, bucket) = MemoryObjects::with_default_bucket();
    let mut state = core.lock_state().unwrap();
    for index in 0..entries {
        core.put_locked(
            &mut state,
            &query(&bucket, &format!("key-{index}")),
            OwnedBytes::from_static(b"seed"),
        )
        .unwrap();
    }
    let query = query(&bucket, "key-0");
    measure(|| {
        for _ in 0..8 {
            // Production-path negative control: retain the old tree through
            // publication, reproducing snapshot-before-mutation ownership.
            let old = keep_snapshot.then(|| state.clone());
            core.put_locked(&mut state, &query, OwnedBytes::from_static(b"next"))
                .unwrap();
            drop(old);
        }
    })
}

#[test]
fn repeated_writes_do_not_copy_history_branches() {
    let small = overwrite_allocations(1, false);
    let wide = overwrite_allocations(1024, false);
    let broken = overwrite_allocations(1024, true);
    eprintln!("overwrite allocations: small={small:?} wide={wide:?} snapshot-control={broken:?}");
    let constant = |count: AllocationInfo| {
        count.count_total == small.count_total && count.bytes_total == small.bytes_total
    };
    assert!(constant(wide));
    assert!(
        !constant(broken),
        "retaining the old tree must fail the same allocation bound"
    );
    assert!(broken.count_total > wide.count_total);
    assert!(broken.bytes_total > wide.bytes_total);
}

fn contents(state: &State) -> Vec<(String, wire::ObjectInfo, Vec<u8>)> {
    state
        .buckets
        .values()
        .flat_map(|bucket| bucket.objects.iter())
        .map(|(key, stored)| {
            let StoredBody::Memory(bytes) = &stored.body else {
                panic!("memory fixture")
            };
            (
                key.to_string(),
                (*stored.info).clone(),
                bytes.as_slice().to_vec(),
            )
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Each failure phase leaves objects, sequence, byte accounting and receipts
    // unchanged; successful histories equal the independent key/byte model.
    #[test]
    fn staged_histories_keep_atomic_rollback_and_exact_quotas(
        commands in prop::collection::vec((0u8..6, 0usize..17, 0u8..4), 1..25),
    ) {
        let (mut core, bucket) = MemoryObjects::with_default_bucket();
        core.options = MemoryOptions { maximum_bytes: 24, maximum_entries: 12 };
        let mut state = core.lock_state().unwrap();
        let mut model = std::collections::BTreeMap::<String, Vec<u8>>::new();
        let mut receipts = 0;
        let mut sequence = 0;
        for (index, &(key, size, phase)) in commands.iter().enumerate() {
            let key = format!("key-{key}");
            let body = vec![index as u8; size];
            let before = contents(&state);
            let identity = Some(wire::MutationIdentity { idempotency_key: format!("attempt-{index}") });
            let digest = [index as u8; 32];
            let mut expected = model.clone();
            expected.insert(key.clone(), body.clone());
            let bytes: usize = expected.values().map(Vec::len).sum();
            let admitted = phase == 0 && bytes <= 24 && 1 + expected.len() + receipts + 1 <= 12;
            let result: Result<wire::ObjectInfo, Error> = core.mutate_locked_engine(
                &mut state, (digest, &identity), |_| unreachable!("fresh identity"),
                |next| {
                    let result = publish(next, &bucket.name, &key, None, &None, StoredBody::memory(body.clone().into()))?;
                    if phase == 1 { return Err(Unavailable.into()); }
                    Ok(result)
                },
                |response, identity| {
                    if phase == 2 { return Err(Unavailable.into()); }
                    Ok((identity.idempotency_key.clone().into(), Receipt {
                        digest, response: response.encode_to_vec().into(), put_info: Some(response.clone().into()),
                        #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
                        kind: 2,
                    }))
                },
                |previous, next| {
                    // No object-tree mutation, even in the staged candidate,
                    // before the final fallible commit/authority check.
                    assert_eq!(contents(previous), contents(&next.state));
                    assert!(next.object.is_some());
                    assert_eq!(previous.receipts.len(), next.state.receipts.len());
                    assert!(next.receipt.is_some());
                    if phase == 3 { return Err(Unavailable.into()); }
                    Ok(())
                },
            );
            prop_assert_eq!(result.is_ok(), admitted);
            if admitted { model = expected; receipts += 1; sequence += 1; }
            else { prop_assert_eq!(contents(&state), before); }
            let actual: std::collections::BTreeMap<_, _> = contents(&state).into_iter().map(|(key, _, bytes)| (key, bytes)).collect();
            prop_assert_eq!(&actual, &model);
            prop_assert_eq!(state.sequence, sequence);
            prop_assert_eq!(state.receipts.len(), receipts);
            prop_assert_eq!(state.object_bytes, model.values().map(Vec::len).sum::<usize>());
        }
    }
}
