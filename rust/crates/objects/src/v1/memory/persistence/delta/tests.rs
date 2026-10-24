use super::*;
use acyclic_test_support::{differential, measure};
use proptest::prelude::*;

#[path = "baseline.rs"]
mod baseline;
thread_local! {
    static RESTORE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(super) fn restore_scan() -> bool {
    RESTORE.get()
}
pub(super) fn old_difference(
    before: &State,
    next: &State,
    ordinal: u64,
    publication: &mut Publication,
) -> Result<Delta, NativeObjectsError> {
    baseline::difference(before, next, ordinal, publication)
}
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        RESTORE.set(false);
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
fn info(sequence: u64, size: usize) -> wire::ObjectInfo {
    wire::ObjectInfo {
        etag: format!("\"{sequence}\""),
        size: size as u64,
        metadata: None,
        last_modified: Some(prost_types::Timestamp::default()),
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

fn bodies_for(core: &MemoryObjects) -> Vec<StoredBody> {
    (1_u8..5)
        .map(|size| {
            core.journal
                .as_ref()
                .unwrap()
                .external(
                    &StoredBody::memory(vec![size; usize::from(size)].into()),
                    &mut Publication::new(None),
                )
                .unwrap()
        })
        .collect()
}

fn recount(state: &State) {
    assert_eq!(
        state.object_bytes,
        state
            .buckets
            .values()
            .flat_map(|bucket| bucket.objects.values())
            .map(|object| object.body.len())
            .sum::<usize>()
    );
    assert_eq!(
        state.upload_bytes,
        state
            .uploads
            .values()
            .flat_map(|upload| upload.parts.values())
            .map(|(_, body)| body.len())
            .sum::<usize>()
    );
    assert_eq!(
        state.nested_entries,
        state
            .buckets
            .values()
            .map(|bucket| bucket.objects.len())
            .sum::<usize>()
            + state
                .uploads
                .values()
                .map(|upload| upload.parts.len())
                .sum::<usize>()
    );
}

fn snapshot(state: &State) -> Vec<u8> {
    baseline::difference(&State::default(), state, 0, &mut Publication::new(None))
        .unwrap()
        .encode_to_vec()
}

fn history(commands: &[(u8, u8, u8)], old: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let directory = tempfile::tempdir().unwrap();
    let core = open(
        directory.path().to_owned(),
        LocalObjectsLimits::default(),
        None,
    )
    .unwrap();
    let bodies = bodies_for(&core);
    let root = &core.journal.as_ref().unwrap().root;
    let mut state = State::default();
    let mut output = Vec::new();
    // Deterministic prefix reaches every collection before generated history.
    let prefix = [
        (0, 0, 0),
        (0, 1, 0),
        (2, 0, 0),
        (4, 0, 0),
        (6, 0, 0),
        (7, 0, 0),
    ];
    for (index, &(operation, identity, key)) in prefix.iter().chain(commands).enumerate() {
        let mut next = state.clone();
        next.sequence += 1;
        let name = format!("bucket-{identity}");
        let id = format!("upload-{identity}");
        let number = u32::from(key) + 1;
        let key = format!("key-{key}");
        let body = &bodies[index % bodies.len()];
        match operation {
            0 => {
                if !next.buckets.contains_key(name.as_str()) {
                    next.buckets.insert(name.clone().into(), bucket(&name));
                }
            }
            1 => {
                if next
                    .buckets
                    .get(name.as_str())
                    .is_some_and(|bucket| bucket.objects.is_empty())
                    && !next
                        .uploads
                        .values()
                        .any(|upload| upload.bucket.as_str() == name)
                {
                    next.buckets.remove(name.as_str());
                }
            }
            2 => {
                if let Some(bucket) = next.buckets.get_mut(name.as_str()) {
                    bucket.objects.insert(
                        key.into(),
                        Stored {
                            info: info(next.sequence, body.len()).into(),
                            body: body.clone(),
                        },
                    );
                }
            }
            3 => {
                if let Some(bucket) = next.buckets.get_mut(name.as_str()) {
                    bucket.objects.remove(key.as_str());
                }
            }
            4 => {
                if next.buckets.contains_key(name.as_str())
                    && !next.uploads.contains_key(id.as_str())
                {
                    next.uploads.insert(id.into(), upload(&name));
                }
            }
            5 => {
                next.uploads.remove(id.as_str());
            }
            6 => {
                if let Some(upload) = next.uploads.get_mut(id.as_str()) {
                    upload.parts.insert(
                        number,
                        (
                            wire::UploadedPart {
                                part_number: number,
                                etag: format!("\"{index}\""),
                                size: body.len() as u64,
                            }
                            .into(),
                            body.clone(),
                        ),
                    );
                }
            }
            7 => {
                let info = info(next.sequence, body.len());
                next.receipts.insert(
                    format!("receipt-{index}").into(),
                    Receipt {
                        digest: [identity; 32],
                        response: info.encode_to_vec().into(),
                        put_info: Some(info.into()),
                        kind: 2,
                    },
                );
            }
            _ => {
                // A rebuilt equal-value root must not fabricate wire changes.
                if let Some(bucket) = next.buckets.get_mut(name.as_str()) {
                    bucket.objects = bucket
                        .objects
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect();
                }
            }
        }
        stored_bytes(&state, &mut next).unwrap();
        recount(&next);
        let delta = if old {
            baseline::difference
        } else {
            difference
        }(&state, &next, index as u64 + 1, &mut Publication::new(None))
        .unwrap();
        let bytes = delta.encode_to_vec();
        let mut recovered = state.clone();
        super::super::replay::apply(
            &mut recovered,
            delta,
            root,
            core.options,
            LocalObjectsLimits::default(),
        )
        .unwrap();
        recount(&recovered);
        assert_eq!(snapshot(&recovered), snapshot(&next));
        differential::record((index, bytes.clone()));
        output.push((bytes, snapshot(&recovered)));
        state = next;
    }
    output
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn mixed_deltas_match_frozen_writer_full_bytes_trace_and_replay(
        commands in prop::collection::vec((0_u8..9, 0_u8..3, 0_u8..4), 0..25),
    ) {
        let [(old, old_events), (new, new_events)] = differential::pair::<_, _, (usize, Vec<u8>)>(
            || commands.clone(), |commands, old| history(&commands, old),
        );
        prop_assert_eq!(new, old);
        prop_assert_eq!(new_events, old_events);
    }
}

fn unchanged_cost(count: u8, restore: bool) -> acyclic_test_support::AllocationInfo {
    let directory = tempfile::tempdir().unwrap();
    let core = open(
        directory.path().to_owned(),
        LocalObjectsLimits::default(),
        None,
    )
    .unwrap();
    let bodies = bodies_for(&core);
    let mut state = State::default();
    for index in 0..count {
        let name = format!("bucket-{index}");
        let mut bucket = bucket(&name);
        bucket.objects.insert(
            "value".to_owned().into(),
            Stored {
                info: info(0, bodies[0].len()).into(),
                body: bodies[0].clone(),
            },
        );
        state.buckets.insert(name.clone().into(), bucket);
        let mut upload = upload(&name);
        upload.parts.insert(
            1,
            (
                wire::UploadedPart {
                    etag: "part".into(),
                    part_number: 1,
                    size: bodies[0].len() as u64,
                }
                .into(),
                bodies[0].clone(),
            ),
        );
        state
            .uploads
            .insert(format!("upload-{index}").into(), upload);
    }
    stored_bytes(&State::default(), &mut state).unwrap();
    let next = state.clone();
    RESTORE.set(restore);
    let _reset = Reset;
    let mut result = None;
    let measured = measure(|| {
        result = Some(difference(&state, &next, 1, &mut Publication::new(None)).unwrap())
    });
    assert_eq!(
        result.unwrap(),
        Delta {
            ordinal: 1,
            ..Default::default()
        }
    );
    measured
}

fn exact_unchanged_cost(restore: bool) {
    let small = unchanged_cost(8, restore);
    let wide = unchanged_cost(24, restore);
    eprintln!("journal unchanged K=8 {small:?}; K=24 {wide:?}");
    assert_eq!(
        small.count_total, 0,
        "unchanged multipart bodies were serialized"
    );
    assert_eq!(wide.count_total, small.count_total);
    assert_eq!(wide.bytes_total, small.bytes_total);
}

#[test]
fn unchanged_buckets_uploads_and_parts_have_exact_zero_delta_allocations() {
    exact_unchanged_cost(false);
}

#[test]
#[should_panic(expected = "unchanged multipart bodies were serialized")]
fn restored_full_delta_writer_fails_the_same_allocation_oracle() {
    exact_unchanged_cost(true);
}

fn publication_cost(count: u8, restore: bool) -> acyclic_test_support::AllocationInfo {
    let directory = tempfile::tempdir().unwrap();
    let core = open(
        directory.path().to_owned(),
        LocalObjectsLimits::default(),
        None,
    )
    .unwrap();
    let bodies = bodies_for(&core);
    core.mutate([0; 32], &None, |next| {
        for index in 0..count {
            let name = format!("bucket-{index}");
            let mut bucket = bucket(&name);
            bucket.objects.insert(
                "value".to_owned().into(),
                Stored {
                    info: info(0, bodies[0].len()).into(),
                    body: bodies[0].clone(),
                },
            );
            next.buckets.insert(name.clone().into(), bucket);
            let mut upload = upload(&name);
            upload.parts.insert(
                1,
                (
                    wire::UploadedPart {
                        etag: "part".into(),
                        part_number: 1,
                        size: bodies[0].len() as u64,
                    }
                    .into(),
                    bodies[0].clone(),
                ),
            );
            next.uploads
                .insert(format!("upload-{index}").into(), upload);
        }
        let mut target = bucket("target");
        target.objects.insert(
            "value".to_owned().into(),
            Stored {
                info: info(0, bodies[0].len()).into(),
                body: bodies[0].clone(),
            },
        );
        next.buckets.insert("target".to_owned().into(), target);
        next.sequence += 1;
        Ok(wire::DeleteObjectResponse::default())
    })
    .unwrap();
    drop(core);
    // Validate the independent seed through the actual durable recovery path.
    let core = open(
        directory.path().to_owned(),
        LocalObjectsLimits::default(),
        None,
    )
    .unwrap();
    let mut state = core.lock_state().unwrap();
    recount(&state);
    let query = wire::PutObjectHeader {
        bucket: Some(wire::BucketRef {
            name: "target".into(),
        }),
        object_key: "value".into(),
        ..Default::default()
    };
    RESTORE.set(restore);
    let _reset = Reset;
    let timestamp = prost_types::Timestamp {
        seconds: 1_700_000_000,
        nanos: 300_000_000,
    };
    let measured = measure(|| {
        test_clock::with(&timestamp, || {
            core.put_locked(&mut state, &query, OwnedBytes::from_static(b"new"))
                .unwrap();
        });
    });
    assert_eq!(
        state
            .buckets
            .get("target")
            .unwrap()
            .objects
            .get("value")
            .unwrap()
            .info
            .last_modified,
        Some(timestamp)
    );
    recount(&state);
    let expected = snapshot(&state);
    drop(state);
    drop(core);
    let core = open(
        directory.path().to_owned(),
        LocalObjectsLimits::default(),
        None,
    )
    .unwrap();
    let state = core.lock_state().unwrap();
    recount(&state);
    assert_eq!(snapshot(&state), expected);
    measured
}

fn exact_publication_cost(restore: bool) {
    let small = publication_cost(8, restore);
    let wide = publication_cost(24, restore);
    eprintln!("durable put K=8 {small:?}; K=24 {wide:?}");
    assert_eq!(
        wide.count_total, small.count_total,
        "publication allocations grow with unrelated state"
    );
    assert_eq!(
        wide.bytes_total, small.bytes_total,
        "publication bytes grow with unrelated state"
    );
}

#[test]
fn durable_put_is_flat_with_unrelated_buckets_uploads_and_parts() {
    exact_publication_cost(false);
}

#[test]
#[should_panic(expected = "publication allocations grow with unrelated state")]
fn restored_writer_fails_the_complete_durable_put_allocation_oracle() {
    exact_publication_cost(true);
}
