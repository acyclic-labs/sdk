use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use std::collections::BTreeMap;

mod full_batch;

fn create_upload(core: &MemoryObjects) -> wire::MultipartUpload {
    futures::executor::block_on(core.create_multipart(wire::CreateMultipartRequest {
        bucket: bucket(),
        object_key: "upload".into(),
        ..Default::default()
    }))
    .unwrap()
}

fn part(core: &MemoryObjects, upload: &str, number: u32) -> wire::UploadedPart {
    futures::executor::block_on(core.upload_part(
        wire::UploadPartHeader {
            bucket: bucket(),
            object_key: "upload".into(),
            upload_id: upload.into(),
            part_number: number,
            mutation: None,
        },
        OwnedBytes::from_static(b"part"),
    ))
    .unwrap()
}

// Restore the precise old inline_changes loop on an independently held staged
// state. It calls the real prepare_inline choke point for every retained part.
fn retained_part_rewalk(core: &MemoryObjects) {
    let mut state = core.lock_state().unwrap().clone();
    let journal = core.journal.as_ref().unwrap();
    let mut publication = Publication::new(None);
    let mut bytes = Vec::new();
    for (id, _) in &state.uploads.clone() {
        let upload = state.uploads.get_mut(id).unwrap();
        for (number, _) in &upload.parts.clone() {
            let (_, body) = upload.parts.get_mut(number).unwrap();
            *body = journal
                .prepare_inline(body, &mut bytes, &mut publication)
                .unwrap();
        }
    }
    assert!(bytes.is_empty(), "retained parts must already be persisted");
}

fn prefix_work(prefix: u32, native: bool, rewalk: bool) -> Vec<usize> {
    let root = tempfile::tempdir().unwrap();
    let core = core(root.path(), 64);
    let upload = create_upload(&core);
    for number in 1..=prefix {
        part(&core, &upload.upload_id, number);
    }
    let counts = (0..4)
        .map(|turn| {
            let (result, reads) = count_body_lengths(|| {
                if rewalk {
                    retained_part_rewalk(&core);
                }
                batch(
                    &core,
                    vec![(
                        put(&format!("turn-{turn}")),
                        OwnedBytes::from_static(b"frame"),
                    )],
                    native,
                )
            });
            assert_eq!(result, vec![Ok(5)]);
            reads
        })
        .collect();
    // Extending the upload itself validates and accounts for exactly its delta.
    let (_, reads) = count_body_lengths(|| part(&core, &upload.upload_id, prefix + 1));
    assert_eq!(reads, 2);
    let before = core.lock_state().unwrap().clone();
    let mut next = before.clone();
    let journal = core.journal.as_ref().unwrap();
    let mut publication = Publication::new(None);
    let mut inline = None;
    let (allocations, reads) = count_body_lengths(|| {
        measure(|| {
            inline = Some(journal.inline_changes(&before, &mut next, None, &mut publication));
        })
    });
    assert!(inline.unwrap().unwrap().is_empty());
    assert_eq!(reads, 0);
    assert_eq!(allocations, AllocationInfo::default());
    let bytes = before.object_bytes + before.upload_bytes;
    drop(before);
    drop(next);
    drop(core);
    let reopened = core_reopen(root.path());
    assert_eq!(
        {
            let state = reopened.lock_state().unwrap();
            state.object_bytes + state.upload_bytes
        },
        bytes
    );
    assert_eq!(
        futures::executor::block_on(reopened.list_parts(wire::ListPartsRequest {
            bucket: bucket(),
            object_key: "upload".into(),
            upload_id: upload.upload_id,
            page_size: 100,
            ..Default::default()
        }))
        .unwrap()
        .parts
        .len(),
        (prefix + 1) as usize
    );
    counts
}

fn core_reopen(root: &Path) -> MemoryObjects {
    open(
        root.to_path_buf(),
        LocalObjectsLimits {
            maximum_object_bytes: 64,
            ..Default::default()
        },
        None,
    )
    .unwrap()
}

#[test]
fn multipart_prefixes_do_not_add_work_to_either_batch_api() {
    for native in [false, true] {
        let n = prefix_work(16, native, false);
        let twice = prefix_work(32, native, false);
        let old_n = prefix_work(16, native, true);
        let old_twice = prefix_work(32, native, true);
        eprintln!(
            "multipart body lengths: N={n:?} 2N={twice:?} oldN={old_n:?} old2N={old_twice:?}"
        );
        assert_eq!(n, vec![4; 4]);
        assert_eq!(twice, n);
        assert_eq!(old_n, vec![20; 4]);
        assert_eq!(old_twice, vec![36; 4]);
        assert!(!constant_work(&old_n, 4));
        assert!(!constant_work(&old_twice, 4));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // The cached total equals an independent map of byte lengths after every
    // insert, overwrite, removal, upload removal, and rejected quota transition.
    #[test]
    fn staged_part_byte_totals_match_the_full_independent_model(
        steps in prop::collection::vec((0u8..4, 0u8..3, 1u32..5, 0usize..10), 1..32),
    ) {
        let (core, bucket) = MemoryObjects::with_default_bucket();
        let mut state = core.lock_state().unwrap().clone();
        let mut parts = BTreeMap::<(u8,u32),usize>::new();
        let mut objects = BTreeMap::<u8,usize>::new();
        for (index, (kind, id, number, size)) in steps.into_iter().enumerate() {
            let mut next = state.clone();
            let mut expected_parts = parts.clone();
            let mut expected_objects = objects.clone();
            let upload_key = format!("upload-{id}");
            match kind {
                0 => {
                    expected_parts.insert((id,number), size);
                    if !next.uploads.contains_key(upload_key.as_str()) {
                        next.uploads.insert(upload_key.clone().into(), Upload {
                            bucket:bucket.name.clone().into(), key:upload_key.clone().into(),
                            metadata:None.into(), parts:OrdMap::new(),
                        });
                    }
                    next.uploads.get_mut(upload_key.as_str()).unwrap().parts.insert(number, (
                        wire::UploadedPart {etag:index.to_string(), part_number:number, size:size as u64}.into(),
                        StoredBody::memory(vec![7;size].into()),
                    ));
                }
                1 => {
                    expected_parts.remove(&(id,number));
                    if let Some(upload) = next.uploads.get_mut(upload_key.as_str()) {
                        upload.parts.remove(&number);
                    }
                }
                2 => {
                    expected_parts.retain(|(key,_),_| *key != id);
                    next.uploads.remove(upload_key.as_str());
                }
                _ => {
                    expected_objects.insert(id,size);
                    next.buckets.get_mut(bucket.name.as_str()).unwrap().objects.insert(id.to_string().into(), Stored {
                        info:wire::ObjectInfo {size:size as u64, ..Default::default()}.into(),
                        body:StoredBody::memory(vec![7;size].into()),
                    });
                }
            }
            let expected = expected_parts.values().chain(expected_objects.values()).sum::<usize>();
            prop_assert_eq!(stored_bytes(&state, &mut next), Some(expected));
            if expected <= 24 {
                state = next; parts = expected_parts; objects = expected_objects;
            }
            prop_assert_eq!(state.object_bytes + state.upload_bytes, parts.values().chain(objects.values()).sum::<usize>());
        }
    }
}

#[test]
fn dropping_part_accounting_is_rejected_by_the_same_total_oracle() {
    let mut before = State::default();
    before.upload_bytes = 7;
    before.nested_entries = 1;
    before.uploads.insert(
        "upload".to_owned().into(),
        Upload {
            bucket: "bucket".to_owned().into(),
            key: "key".to_owned().into(),
            metadata: None.into(),
            parts: OrdMap::from_iter([(
                1_u32,
                (
                    wire::UploadedPart {
                        part_number: 1,
                        size: 7,
                        ..Default::default()
                    }
                    .into(),
                    StoredBody::memory(OwnedBytes::from_static(b"1234567")),
                ),
            )]),
        },
    );
    let mut next = before.clone();
    next.uploads.clear();
    assert_eq!(stored_bytes(&before, &mut next), Some(0));
    assert_ne!(
        (before.object_bytes + before.upload_bytes),
        (next.object_bytes + next.upload_bytes),
        "omitting the removed upload delta preserves the wrong total"
    );
}

#[tokio::test]
async fn multipart_quota_is_atomic_across_overwrite_abort_completion_and_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    for native in [false, true] {
        let root = tempfile::tempdir()?;
        let limits = LocalObjectsLimits {
            maximum_object_bytes: 8,
            maximum_bytes: 12,
            ..Default::default()
        };
        let core = open(root.path().to_path_buf(), limits, None)?;
        core.create_bucket(wire::CreateBucketRequest {
            name: "recovery".into(),
            mutation: None,
        })
        .await?;
        let upload = create_upload(&core);
        let query = wire::UploadPartHeader {
            bucket: bucket(),
            object_key: "upload".into(),
            upload_id: upload.upload_id.clone(),
            part_number: 1,
            mutation: None,
        };
        core.upload_part(query.clone(), OwnedBytes::from_static(b"12345678"))
            .await?;
        let before = fs::read(root.path().join("mutations.log"))?;
        assert_eq!(
            batch(
                &core,
                vec![(put("object"), OwnedBytes::from_static(b"12345"))],
                native
            ),
            vec![Err(QuotaExceeded)]
        );
        assert_eq!(
            {
                let state = core.lock_state()?;
                state.object_bytes + state.upload_bytes
            },
            8
        );
        assert_eq!(fs::read(root.path().join("mutations.log"))?, before);
        core.upload_part(query, OwnedBytes::from_static(b"part"))
            .await?;
        assert_eq!(
            batch(
                &core,
                vec![(put("object"), OwnedBytes::from_static(b"12345678"))],
                native
            ),
            vec![Ok(8)]
        );
        assert_eq!(
            {
                let state = core.lock_state()?;
                state.object_bytes + state.upload_bytes
            },
            12
        );
        core.abort_multipart(wire::AbortMultipartRequest {
            bucket: bucket(),
            object_key: "upload".into(),
            upload_id: upload.upload_id,
            ..Default::default()
        })
        .await?;
        assert_eq!(
            {
                let state = core.lock_state()?;
                state.object_bytes + state.upload_bytes
            },
            8
        );
        let upload = create_upload(&core);
        let receipt = part(&core, &upload.upload_id, 1);
        core.complete_multipart(wire::CompleteMultipartRequest {
            bucket: bucket(),
            object_key: "upload".into(),
            upload_id: upload.upload_id,
            parts: vec![receipt],
            ..Default::default()
        })
        .await?;
        assert_eq!(
            {
                let state = core.lock_state()?;
                state.object_bytes + state.upload_bytes
            },
            12
        );
        drop(core);
        let reopened = open(root.path().to_path_buf(), limits, None)?;
        assert_eq!(
            {
                let state = reopened.lock_state()?;
                state.object_bytes + state.upload_bytes
            },
            12
        );
        assert_eq!(
            reopened
                .get(get(bucket(), "object"), 8)
                .await?
                .into_body()
                .as_slice(),
            b"12345678"
        );
        assert_eq!(
            reopened
                .get(get(bucket(), "upload"), 4)
                .await?
                .into_body()
                .as_slice(),
            b"part"
        );
    }
    Ok(())
}
