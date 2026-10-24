use super::*;
use acyclic_test_support::measure;
use proptest::prelude::*;
use std::cell::Cell;

thread_local! {
    static ADD_FIRST: Cell<bool> = const { Cell::new(false) };
    static SCAN: Cell<bool> = const { Cell::new(false) };
    static OMIT: Cell<bool> = const { Cell::new(false) };
    static EXAMINED: Cell<u64> = const { Cell::new(0) };
}
pub(super) fn restore_add_first() -> bool {
    ADD_FIRST.get()
}
pub(super) fn restore_scan() -> bool {
    SCAN.get()
}
pub(super) fn omit_update() -> bool {
    OMIT.get()
}
pub(super) fn examined() {
    EXAMINED.set(EXAMINED.get() + 1);
}
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        ADD_FIRST.set(false);
        SCAN.set(false);
        OMIT.set(false);
    }
}

fn bucket(name: &str) -> Bucket {
    Bucket {
        info: wire::Bucket {
            bucket: Some(wire::BucketRef { name: name.into() }),
            created_at: Some(prost_types::Timestamp::default()),
        }
        .into(),
        objects: OrdMap::new(),
    }
}
fn stored(size: u8) -> Stored {
    Stored {
        info: wire::ObjectInfo {
            etag: format!("\"{size}\""),
            size: u64::from(size),
            last_modified: Some(prost_types::Timestamp::default()),
            metadata: None,
        }
        .into(),
        body: StoredBody::memory(vec![size; usize::from(size)].into()),
    }
}
fn upload(name: &str) -> Upload {
    Upload {
        bucket: name.to_owned().into(),
        key: "value".to_owned().into(),
        metadata: None.into(),
        parts: OrdMap::new(),
    }
}
fn part(number: u32, size: u8) -> (SharedInfo<wire::UploadedPart>, StoredBody) {
    (
        wire::UploadedPart {
            part_number: number,
            etag: format!("\"{size}\""),
            size: u64::from(size),
        }
        .into(),
        stored(size).body,
    )
}

fn assert_totals(state: &State, total: usize) {
    // The reference deliberately recounts every collection, independently of
    // persistent-map diff and all cached aggregates.
    let objects: usize = state
        .buckets
        .values()
        .flat_map(|bucket| bucket.objects.values())
        .map(|object| object.body.len())
        .sum();
    let uploads: usize = state
        .uploads
        .values()
        .flat_map(|upload| upload.parts.values())
        .map(|(_, body)| body.len())
        .sum();
    let entries: usize = state
        .buckets
        .values()
        .map(|bucket| bucket.objects.len())
        .sum::<usize>()
        + state
            .uploads
            .values()
            .map(|upload| upload.parts.len())
            .sum::<usize>();
    assert_eq!(state.object_bytes, objects, "object byte aggregate differs");
    assert_eq!(state.upload_bytes, uploads, "upload byte aggregate differs");
    assert_eq!(state.nested_entries, entries, "entry aggregate differs");
    assert_eq!(total, objects + uploads);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn changed_subtree_totals_equal_full_recounts_after_every_transition(
        operations in prop::collection::vec((0_u8..9, 0_u8..5, 0_u8..5, 0_u8..32), 0..65),
    ) {
        let mut state = State::default();
        for (operation, identity, key, size) in operations {
            let before = state.clone();
            let name = format!("bucket-{identity}");
            let id = format!("upload-{identity}");
            let key = format!("key-{key}");
            match operation {
                0 => { state.buckets.insert(name.clone().into(), bucket(&name)); }
                1 => { state.buckets.remove(name.as_str()); }
                2 => { if let Some(bucket) = state.buckets.get_mut(name.as_str()) {
                    bucket.objects.insert(key.into(), stored(size));
                } }
                3 => { if let Some(bucket) = state.buckets.get_mut(name.as_str()) {
                    bucket.objects.remove(key.as_str());
                } }
                4 => { state.uploads.insert(id.clone().into(), upload(&name)); }
                5 => { state.uploads.remove(id.as_str()); }
                6 => { if let Some(upload) = state.uploads.get_mut(id.as_str()) {
                    upload.parts.insert(u32::from(size % 5) + 1, part(u32::from(size % 5) + 1, size));
                } }
                7 => { if let Some(upload) = state.uploads.get_mut(id.as_str()) {
                    upload.parts.remove(&(u32::from(size % 5) + 1));
                } }
                _ => { // Reconstruct an equal-value map with a different root.
                    if let Some(bucket) = state.buckets.get_mut(name.as_str()) {
                        bucket.objects = bucket.objects.iter().map(|(key, value)| (key.clone(), value.clone())).collect();
                    }
                    if let Some(upload) = state.uploads.get_mut(id.as_str()) {
                        upload.parts = upload.parts.iter().map(|(key, value)| (*key, value.clone())).collect();
                    }
                }
            }
            let total = stored_bytes(&before, &mut state).unwrap();
            assert_totals(&state, total);
        }
    }
}

#[test]
#[should_panic(expected = "entry aggregate differs")]
fn omitted_aggregate_update_fails_the_same_recount_oracle() {
    OMIT.set(true);
    let _reset = Reset;
    let before = State::default();
    let mut next = before.clone();
    let mut bucket = bucket("bucket");
    bucket.objects.insert("value".to_owned().into(), stored(3));
    next.buckets.insert("bucket".to_owned().into(), bucket);
    let total = stored_bytes(&before, &mut next).unwrap();
    assert_totals(&next, total);
}

fn put_cost(count: u8, restore: bool) -> (u64, acyclic_test_support::AllocationInfo) {
    let (core, target) = MemoryObjects::with_default_bucket();
    let mut state = core.lock_state().unwrap();
    for index in 0..count {
        let name = format!("bucket-{index}");
        let mut bucket = bucket(&name);
        bucket.objects.insert("value".to_owned().into(), stored(3));
        state.buckets.insert(name.clone().into(), bucket);
        let mut upload = upload(&name);
        upload.parts.insert(1, part(1, 3));
        state
            .uploads
            .insert(format!("upload-{index}").into(), upload);
    }
    let total = stored_bytes(&State::default(), &mut state).unwrap();
    assert_totals(&state, total);
    let query = wire::PutObjectHeader {
        bucket: Some(target),
        object_key: "value".into(),
        ..Default::default()
    };
    // Seed the same target first: insertion versus replacement must not account
    // for any measured difference between retained histories.
    core.put_locked(&mut state, &query, OwnedBytes::from_static(b"old"))
        .unwrap();
    SCAN.set(restore);
    let _reset = Reset;
    EXAMINED.set(0);
    let measured = measure(|| {
        core.put_locked(&mut state, &query, OwnedBytes::from_static(b"new"))
            .unwrap();
    });
    let work = EXAMINED.get();
    assert_totals(&state, state.object_bytes + state.upload_bytes);
    (work, measured)
}

fn exact_put_cost(restore: bool) {
    let small = put_cost(8, restore);
    let wide = put_cost(24, restore);
    eprintln!("put delta K=8 {small:?}; K=24 {wide:?}");
    assert_eq!(small.0, 0, "unchanged state was scanned");
    assert_eq!(wide.0, small.0, "unchanged state was scanned");
    assert_eq!(wide.1.count_total, small.1.count_total);
    assert_eq!(wide.1.bytes_total, small.1.bytes_total);
}

#[test]
fn put_keeps_exact_work_and_allocations_across_buckets_uploads_and_parts() {
    exact_put_cost(false);
}

#[test]
#[should_panic(expected = "unchanged state was scanned")]
fn restored_quota_scan_fails_the_same_production_cost_oracle() {
    exact_put_cost(true);
}

fn assert_adjusted_total(total: usize, removed: usize, added: usize) {
    let expected = (total as u128)
        .checked_sub(removed as u128)
        .and_then(|remaining| remaining.checked_add(added as u128))
        .and_then(|value| usize::try_from(value).ok());
    assert_eq!(
        adjusted_total(total, removed, added),
        expected,
        "delta settlement depends on temporary overflow"
    );
}

#[test]
fn replacement_counters_are_settled_without_temporary_overflow() {
    for total in [0, 1, usize::MAX - 1, usize::MAX] {
        for removed in [0, 1, usize::MAX - 1, usize::MAX] {
            for added in [0, 1, usize::MAX - 1, usize::MAX] {
                assert_adjusted_total(total, removed, added);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn counter_delta_matches_wider_integer_arithmetic(
        total in any::<usize>(), removed in any::<usize>(), added in any::<usize>(),
    ) {
        assert_adjusted_total(total, removed, added);
    }
}

#[test]
#[should_panic(expected = "delta settlement depends on temporary overflow")]
fn restored_add_before_remove_fails_the_same_counter_oracle() {
    ADD_FIRST.set(true);
    let _reset = Reset;
    assert_adjusted_total(usize::MAX, 1, 1);
}
