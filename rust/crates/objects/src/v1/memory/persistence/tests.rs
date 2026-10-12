use super::*;

#[test]
fn native_composite_controls_retire_after_the_last_physical_alias()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering::SeqCst;
    let observed = Arc::new(crate::v1::tests::NativeTransportMeter::new());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let mut publication = Publication::new(Some(meter));
    let mut parts = Vec::new();
    publication.reserve(&mut parts, 1)?;
    parts.push(StoredBody::memory(OwnedBytes::from_static(b"x")));
    let body = composite_publication(parts, 1, &mut publication)?;
    let alias = body.clone();
    drop(publication);
    assert!(observed.memory.load(SeqCst) > 0);
    drop(body);
    assert!(observed.memory.load(SeqCst) > 0);
    drop(alias);
    assert_eq!(observed.memory.load(SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_journal_control_denial_keeps_original_cause_and_unpoisoned_tail()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering::SeqCst;
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    let path = root.path().join("mutations.log");
    let durable = fs::read(&path)?;
    let before = core.lock_state()?.clone();
    let mut next = before.clone();
    next.buckets.get_mut("recovery").ok_or("missing bucket")?
        .objects.get_mut("retained").ok_or("missing object")?
        .body = StoredBody::memory(OwnedBytes::from_static(b"after!"));
    let observed = Arc::new(crate::v1::tests::NativeTransportMeter::new());
    observed.limit.store(0, SeqCst);
    let journal = core.journal.as_ref().ok_or("missing journal")?;
    let failed = journal.commit(&before, &mut next, Some(observed.clone()));
    assert!(matches!(failed, Err(NativeObjectsError::Admission(error)) if error.kind() == std::io::ErrorKind::OutOfMemory));
    assert_eq!(observed.allocations.load(SeqCst), 0);
    assert_eq!(observed.memory.load(SeqCst), 0);
    assert_eq!(fs::read(path)?, durable);
    journal.check()?;
    assert_eq!(core.get(get("retained"), 6).await?.into_body(), OwnedBytes::from_static(b"before"));
    Ok(())
}

#[test]
fn header_entropy_failure_preserves_empty_journal_and_retry_key()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("mutations.log");
    let limits = LocalObjectsLimits::default();
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let failed = load_header(&mut file, root.path(), limits, |key| {
        key.fill(1);
        Err(getrandom::Error::UNSUPPORTED)
    });
    assert!(matches!(failed, Err(LocalOpenError::Unavailable)));
    assert_eq!(file.metadata()?.len(), 0);
    assert_eq!(file.stream_position()?, 0);
    assert!(fs::read(&path)?.is_empty());

    let header = load_header(&mut file, root.path(), limits, |key| {
        key.fill(2);
        Ok(())
    })?;
    assert_eq!(header.cursor_key, vec![2; 32]);
    let published = fs::read(&path)?;
    assert!(!published.is_empty());
    drop(file);

    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
    let recovered = load_header(&mut file, root.path(), limits, |_| {
        panic!("a valid existing header must not request entropy")
    })?;
    assert_eq!(recovered.encode_to_vec(), header.encode_to_vec());
    assert_eq!(fs::read(&path)?, published);
    drop(file);
    let provider = reopen(root.path())?;
    assert_eq!(
        *provider
            .token_key
            .lock()
            .map_err(|_| "poisoned cursor key")?,
        Some([2; 32]),
    );
    Ok(())
}

// Provider closure must release its lock across concurrent Unix spawning.
#[cfg(unix)]
#[test]
fn dropping_the_last_owner_unlocks_an_inherited_file_description()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let core = reopen(root.path())?;
    let inherited = core
        .journal
        .as_ref()
        .ok_or("missing journal")?
        ._owner
        .try_clone()?;
    let live = core.clone();
    drop(core);
    assert!(matches!(
        reopen(root.path()),
        Err(LocalOpenError::AlreadyOwned)
    ));
    drop(live);
    let next = reopen(root.path())?;
    // Closing the old duplicate cannot release the new owner's lock.
    drop(inherited);
    assert!(matches!(
        reopen(root.path()),
        Err(LocalOpenError::AlreadyOwned)
    ));
    drop(next);
    drop(reopen(root.path())?);
    Ok(())
}

fn bucket() -> Option<wire::BucketRef> {
    Some(wire::BucketRef {
        name: "recovery".into(),
    })
}
pub(super) fn put(key: &str) -> wire::PutObjectHeader {
    wire::PutObjectHeader {
        bucket: bucket(),
        object_key: key.into(),
        mutation: Some(wire::MutationIdentity {
            idempotency_key: key.into(),
        }),
        ..Default::default()
    }
}
pub(super) fn get(key: &str) -> wire::GetObjectRequest {
    wire::GetObjectRequest {
        bucket: bucket(),
        object_key: key.into(),
        ..Default::default()
    }
}
pub(super) fn reopen(root: &Path) -> Result<MemoryObjects, LocalOpenError> {
    open(root.to_path_buf(), LocalObjectsLimits::default(), None)
}
pub(super) async fn seeded(root: &Path) -> Result<MemoryObjects, Box<dyn std::error::Error>> {
    let core = reopen(root)?;
    core.create_bucket(wire::CreateBucketRequest {
        name: "recovery".into(),
        mutation: None,
    })
    .await?;
    core.put(put("retained"), OwnedBytes::from_static(b"before"))
        .await?;
    Ok(core)
}
async fn assert_poisoned(core: &MemoryObjects) {
    assert_eq!(
        core.get(get("retained"), 6).await.err().map(|e| e.code),
        Some(Unavailable)
    );
    assert_eq!(
        core.head_bucket(wire::HeadBucketRequest { bucket: bucket() })
            .await
            .err()
            .map(|e| e.code),
        Some(Unavailable)
    );
    assert_eq!(
        core.list(wire::ListObjectsRequest {
            bucket: bucket(),
            page_size: 1,
            ..Default::default()
        })
        .await
        .err()
        .map(|e| e.code),
        Some(Unavailable)
    );
    assert_eq!(
        core.put(put("retained"), OwnedBytes::from_static(b"before"))
            .await
            .err()
            .map(|e| e.code),
        Some(Unavailable)
    );
}

#[tokio::test]
async fn incomplete_final_frames_repair_without_publishing_a_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    for cut in [0, 2, 35, 40] {
        let root = tempfile::tempdir()?;
        let core = seeded(root.path()).await?;
        let path = root.path().join("mutations.log");
        let before = fs::read(&path)?;
        core.journal
            .as_ref()
            .ok_or("missing journal")?
            .fault_bytes
            .store(cut, Ordering::Release);
        assert_eq!(
            core.put(put("interrupted"), OwnedBytes::from_static(b"after"))
                .await
                .err()
                .map(|e| e.code),
            Some(Unavailable)
        );
        assert_poisoned(&core).await;
        drop(core);
        let core = reopen(root.path())?;
        assert_eq!(fs::read(&path)?, before);
        assert_eq!(
            core.get(get("retained"), 6).await?.into_body(),
            OwnedBytes::from_static(b"before")
        );
        assert_eq!(
            core.get(get("interrupted"), 5).await.err().map(|e| e.code),
            Some(NotFound)
        );
        core.put(put("interrupted"), OwnedBytes::from_static(b"after"))
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn complete_uncertain_frame_recovers_its_exact_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    core.journal
        .as_ref()
        .ok_or("missing journal")?
        .fault_sync
        .store(true, Ordering::Release);
    let query = put("uncertain");
    assert_eq!(
        core.put(query.clone(), OwnedBytes::from_static(b"after"))
            .await
            .err()
            .map(|e| e.code),
        Some(Unavailable)
    );
    assert_poisoned(&core).await;
    drop(core);
    let core = reopen(root.path())?;
    let recovered = core
        .head(wire::HeadObjectRequest {
            bucket: bucket(),
            object_key: "uncertain".into(),
            ..Default::default()
        })
        .await?
        .object
        .ok_or("missing recovered object")?;
    let before = fs::read(root.path().join("mutations.log"))?;
    assert_eq!(
        core.put(query, OwnedBytes::from_static(b"after")).await?,
        recovered
    );
    assert_eq!(fs::read(root.path().join("mutations.log"))?, before);
    Ok(())
}

#[tokio::test]
async fn obsolete_v2_magic_fails_closed_without_rewriting_the_journal()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    drop(seeded(root.path()).await?);
    let path = root.path().join("mutations.log");
    let mut bytes = fs::read(&path)?;
    let obsolete = b"ACYCLIC-OBJECTS-V2-LOCAL\0\x01";
    assert_eq!(obsolete.len(), MAGIC.len());
    let magic = bytes.get_mut(..MAGIC.len()).ok_or("short journal header")?;
    assert_eq!(magic, MAGIC);
    magic.copy_from_slice(obsolete);
    fs::write(&path, &bytes)?;
    assert!(matches!(reopen(root.path()), Err(LocalOpenError::Corrupt)));
    assert_eq!(fs::read(&path)?, bytes);
    Ok(())
}

#[tokio::test]
async fn complete_corruption_fails_closed_without_truncating_the_journal()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    drop(seeded(root.path()).await?);
    let path = root.path().join("mutations.log");
    let mut bytes = fs::read(&path)?;
    let last = bytes.last_mut().ok_or("empty journal")?;
    *last ^= 1;
    fs::write(&path, &bytes)?;
    assert!(matches!(reopen(root.path()), Err(LocalOpenError::Corrupt)));
    assert_eq!(fs::read(&path)?, bytes);
    Ok(())
}

#[tokio::test]
async fn multipart_recovery_preserves_parts_composite_ranges_and_completion_retries()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    let upload = core
        .create_multipart(wire::CreateMultipartRequest {
            bucket: bucket(),
            object_key: "multipart".into(),
            ..Default::default()
        })
        .await?;
    let mut parts = Vec::new();
    for (number, bytes) in [
        (1, OwnedBytes::from(vec![b'a'; 5 * 1024 * 1024])),
        (2, OwnedBytes::from_static(b"end")),
    ] {
        parts.push(
            core.upload_part(
                wire::UploadPartHeader {
                    bucket: bucket(),
                    object_key: "multipart".into(),
                    upload_id: upload.upload_id.clone(),
                    part_number: number,
                    mutation: None,
                },
                bytes,
            )
            .await?,
        );
    }
    drop(core);
    let core = reopen(root.path())?;
    assert_eq!(
        core.list_parts(wire::ListPartsRequest {
            bucket: bucket(),
            object_key: "multipart".into(),
            upload_id: upload.upload_id.clone(),
            page_size: 10,
            ..Default::default()
        })
        .await?
        .parts,
        parts
    );
    let query = wire::CompleteMultipartRequest {
        bucket: bucket(),
        object_key: "multipart".into(),
        upload_id: upload.upload_id,
        parts,
        mutation: Some(wire::MutationIdentity {
            idempotency_key: "completion".into(),
        }),
        ..Default::default()
    };
    let receipt = core.complete_multipart(query.clone()).await?;
    drop(core);
    let core = reopen(root.path())?;
    let range = wire::GetObjectRequest {
        range: Some(wire::ByteRange {
            selection: Some(wire::byte_range::Selection::Bytes(wire::InclusiveRange {
                start: 5 * 1024 * 1024 - 2,
                end: Some(5 * 1024 * 1024 + 1),
            })),
        }),
        ..get("multipart")
    };
    assert_eq!(core.get(range, 4).await?.into_body(), OwnedBytes::from_static(b"aaen"));
    core.delete(wire::DeleteObjectRequest {
        bucket: bucket(),
        object_key: "multipart".into(),
        ..Default::default()
    })
    .await?;
    drop(core);
    let core = reopen(root.path())?;
    assert_eq!(core.complete_multipart(query).await?, receipt);
    assert_eq!(
        core.get(get("multipart"), 10).await.err().map(|e| e.code),
        Some(NotFound)
    );
    Ok(())
}

#[tokio::test]
async fn logical_quota_failure_does_not_persist_a_body_or_retry_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let options = LocalObjectsLimits {
        maximum_object_bytes: 5,
        maximum_bytes: 5,
        ..Default::default()
    };
    let core = open(root.path().to_path_buf(), options, None)?;
    core.create_bucket(wire::CreateBucketRequest {
        name: "recovery".into(),
        mutation: None,
    })
    .await?;
    core.put(put("retained"), OwnedBytes::from_static(b"1234"))
        .await?;
    let before = fs::read(root.path().join("mutations.log"))?;
    let count = fs::read_dir(root.path().join("segments"))?.count();
    assert_eq!(
        core.put(put("new"), OwnedBytes::from_static(b"ab"))
            .await
            .err()
            .map(|e| e.code),
        Some(QuotaExceeded)
    );
    assert_eq!(fs::read(root.path().join("mutations.log"))?, before);
    assert_eq!(fs::read_dir(root.path().join("segments"))?.count(), count);
    drop(core);
    let core = open(root.path().to_path_buf(), options, None)?;
    core.delete(wire::DeleteObjectRequest {
        bucket: bucket(),
        object_key: "retained".into(),
        ..Default::default()
    })
    .await?;
    core.put(put("new"), OwnedBytes::from_static(b"ab")).await?;
    assert_eq!(
        core.get(get("new"), 2).await?.into_body(),
        OwnedBytes::from_static(b"ab")
    );
    Ok(())
}

#[tokio::test]
async fn uncertain_batch_item_prevents_later_exact_retries_in_the_same_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    core.journal
        .as_ref()
        .ok_or("missing journal")?
        .fault_bytes
        .store(2, Ordering::Release);
    let results = core
        .put_batch(vec![
            (put("interrupted"), OwnedBytes::from_static(b"after")),
            (put("retained"), OwnedBytes::from_static(b"before")),
            (put("later"), OwnedBytes::from_static(b"later")),
        ])
        .await;
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|result| {
        result
            .as_ref()
            .err()
            .is_some_and(|error| error.code == Unavailable)
    }));
    drop(core);
    let core = reopen(root.path())?;
    assert_eq!(
        core.get(get("retained"), 6).await?.into_body(),
        OwnedBytes::from_static(b"before")
    );
    assert_eq!(
        core.get(get("later"), 5).await.err().map(|e| e.code),
        Some(NotFound)
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_interruption_recovers_old_or_new_state_without_losing_receipts()
-> Result<(), Box<dyn std::error::Error>> {
    for stage in 1..=4 {
        let root = tempfile::tempdir()?;
        let core = seeded(root.path()).await?;
        let original = core
            .put(put("retained"), OwnedBytes::from_static(b"before"))
            .await?;
        let mut replacement = put("retained");
        replacement.mutation = Some(wire::MutationIdentity {
            idempotency_key: "replacement".into(),
        });
        core.put(replacement, OwnedBytes::from_static(b"after")).await?;
        core.journal
            .as_ref()
            .ok_or("missing journal")?
            .fault_checkpoint
            .store(stage, Ordering::Release);
        assert!(matches!(
            core.collect_local_garbage(10),
            Err(LocalOpenError::Unavailable)
        ));
        assert_poisoned(&core).await;
        drop(core);
        let core = reopen(root.path())?;
        assert_eq!(
            core.get(get("retained"), 5).await?.into_body(),
            OwnedBytes::from_static(b"after")
        );
        assert_eq!(
            core.put(put("retained"), OwnedBytes::from_static(b"before"))
                .await?,
            original
        );
        core.collect_local_garbage(10)?;
        core.put(put("next"), OwnedBytes::from_static(b"next")).await?;
        drop(core);
        assert_eq!(
            reopen(root.path())?.get(get("next"), 4).await?.into_body(),
            OwnedBytes::from_static(b"next")
        );
    }
    Ok(())
}

#[tokio::test]
async fn checkpoint_packs_multiple_frames_and_preserves_live_pagination()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    for number in 0..100 {
        let mut query = put(&format!("key-{number:03}"));
        query.metadata = Some(wire::ObjectMetadata {
            content_type: "a".repeat(800),
            ..Default::default()
        });
        core.put(query, OwnedBytes::new()).await?;
    }
    let mut query = wire::ListObjectsRequest {
        bucket: bucket(),
        page_size: 1,
        ..Default::default()
    };
    let page = core.list(query.clone()).await?;
    query.continuation_token = page.continuation_token;
    let expected = core.list(query.clone()).await?;
    core.collect_local_garbage(200)?;
    drop(core);
    let core = reopen(root.path())?;
    assert_eq!(core.list(query).await?, expected);
    core.put(put("after-checkpoint"), OwnedBytes::new()).await?;
    Ok(())
}

#[derive(Clone, Debug)]
enum Step {
    CreateBucket(u8),
    DeleteBucket(u8),
    Put(u8, u8, usize),
    Delete(u8, u8),
}

fn step() -> impl proptest::strategy::Strategy<Value = Step> {
    use proptest::prelude::*;
    let bucket = prop_oneof![3 => Just(0_u8), 1 => Just(1_u8)];
    let size = prop_oneof![8 => 0..16_usize, 1 => Just(inline::LIMIT + 1)];
    prop_oneof![
        2 => (0..2_u8).prop_map(Step::CreateBucket),
        1 => (0..2_u8).prop_map(Step::DeleteBucket),
        4 => (bucket.clone(), 0..2_u8, size).prop_map(|(b, k, n)| Step::Put(b, k, n)),
        2 => (bucket, 0..2_u8).prop_map(|(b, k)| Step::Delete(b, k)),
    ]
}

type View = (
    u64,
    Vec<(
        String,
        wire::Bucket,
        Vec<(String, wire::ObjectInfo, Vec<BodyRecord>)>,
    )>,
    Vec<(String, [u8; 32], Vec<u8>, u32, Option<wire::ObjectInfo>)>,
);

fn view(state: &State) -> Result<View, Error> {
    // The incrementally maintained byte total must equal a full recount.
    let bytes = state
        .buckets
        .values()
        .flat_map(|bucket| bucket.objects.values())
        .map(|object| object.body.len())
        .sum::<usize>();
    if bytes != state.object_bytes {
        return Err(Unavailable.into());
    }
    let mut buckets = Vec::new();
    for (name, bucket) in &state.buckets {
        let mut objects = Vec::new();
        for (key, value) in &bucket.objects {
            objects.push((key.to_string(), (*value.info).clone(), bodies(&value.body, &mut Publication::new(None)).map_err(service_error)?));
        }
        buckets.push((name.to_string(), (*bucket.info).clone(), objects));
    }
    let receipts = state
        .receipts
        .iter()
        .map(|(key, value)| {
            (
                key.to_string(),
                value.digest,
                value.response.as_slice().to_vec(),
                value.kind,
                value.put_info.as_ref().map(|info| (**info).clone()),
            )
        })
        .collect();
    Ok((state.sequence, buckets, receipts))
}

/// Full-scan reference for the bucket and object part of `difference`.
fn naive_changes(
    before: &State,
    next: &State,
) -> Result<(Vec<BucketChange>, Vec<ObjectChange>), Error> {
    let (mut buckets, mut objects, mut removed) = (Vec::new(), Vec::new(), Vec::new());
    for (name, bucket) in &next.buckets {
        let old = before.buckets.get(name);
        if old.is_none_or(|old| old.info != bucket.info) {
            buckets.push(BucketChange {
                name: name.to_string(),
                info: Some((*bucket.info).clone()),
            });
        }
        for (key, value) in &bucket.objects {
            let refs = bodies(&value.body, &mut Publication::new(None)).map_err(service_error)?;
            let unchanged = match old.and_then(|old| old.objects.get(key)) {
                Some(prior) => prior.info == value.info && bodies(&prior.body, &mut Publication::new(None)).map_err(service_error)? == refs,
                None => false,
            };
            if !unchanged {
                objects.push(ObjectChange {
                    bucket: name.to_string(),
                    key: key.to_string(),
                    info: Some((*value.info).clone()),
                    bodies: refs,
                });
            }
        }
        for key in old.iter().flat_map(|old| old.objects.keys()) {
            if !bucket.objects.contains_key(key) {
                removed.push(ObjectChange {
                    bucket: name.to_string(),
                    key: key.to_string(),
                    ..Default::default()
                });
            }
        }
    }
    objects.extend(removed);
    for name in before
        .buckets
        .keys()
        .filter(|name| !next.buckets.contains_key(*name))
    {
        buckets.push(BucketChange {
            name: name.to_string(),
            info: None,
        });
    }
    Ok((buckets, objects))
}

async fn run(core: &MemoryObjects, index: usize, step: &Step) {
    let bucket = |b: &u8| {
        Some(wire::BucketRef {
            name: format!("bucket-{b}"),
        })
    };
    let mutation = Some(wire::MutationIdentity {
        idempotency_key: format!("step-{index}"),
    });
    // Rejections (absent bucket, non-empty bucket, ...) leave the state unchanged.
    let _ = match step {
        Step::CreateBucket(b) => core
            .create_bucket(wire::CreateBucketRequest {
                name: format!("bucket-{b}"),
                mutation,
            })
            .await
            .map(drop),
        Step::DeleteBucket(b) => core
            .delete_bucket(wire::DeleteBucketRequest {
                bucket: bucket(b),
                mutation,
            })
            .await
            .map(drop),
        Step::Put(b, k, size) => core
            .put(
                wire::PutObjectHeader {
                    bucket: bucket(b),
                    object_key: format!("key-{k}"),
                    mutation,
                    ..Default::default()
                },
                OwnedBytes::from(vec![u8::try_from(index % 251).unwrap_or_default(); *size]),
            )
            .await
            .map(drop),
        Step::Delete(b, k) => core
            .delete(wire::DeleteObjectRequest {
                bucket: bucket(b),
                object_key: format!("key-{k}"),
                mutation,
                ..Default::default()
            })
            .await
            .map(drop),
    };
}

fn snapshot(core: &MemoryObjects) -> Result<State, proptest::test_runner::TestCaseError> {
    core.state
        .lock()
        .map(|state| state.clone())
        .map_err(|_| proptest::test_runner::TestCaseError::fail("poisoned state"))
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig {
        rng_seed: proptest::test_runner::RngSeed::Fixed(246),
        failure_persistence: None,
        ..proptest::prelude::ProptestConfig::with_cases(16)
    })]

    /// Every committed delta equals a full-scan diff, replays onto its predecessor including the immutable decoded put acknowledgement, and recovers the exact state and retry metadata at every frame boundary.
    #[test]
    fn journal_deltas_replay_and_recover_prefix_states(
        steps in proptest::collection::vec(step(), 1..8),
    ) {
        // Most interesting histories need a bucket, so every case starts with one.
        let steps: Vec<_> = std::iter::once(Step::CreateBucket(0)).chain(steps).collect();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let root = tempfile::tempdir()?;
        let journal = root.path().join("mutations.log");
        let core = reopen(root.path())?;
        let mut states = vec![snapshot(&core)?];
        let mut lengths = vec![fs::metadata(&journal)?.len()];
        for (index, step) in steps.iter().enumerate() {
            runtime.block_on(run(&core, index, step));
            let next = snapshot(&core)?;
            let before = states.last().ok_or(Error::from(Unavailable))?;
            let delta = difference(before, &next, 1, &mut Publication::new(None))?;
            proptest::prop_assert_eq!(
                (delta.buckets.clone(), delta.objects.clone()),
                naive_changes(before, &next)?
            );
            let mut replayed = before.clone();
            apply(&mut replayed, delta, &Arc::new(crate::physical::LocalRoot::new(root.path().to_path_buf())), core.options, LocalObjectsLimits::default())?;
            proptest::prop_assert_eq!(view(&replayed)?, view(&next)?);
            states.push(next);
            lengths.push(fs::metadata(&journal)?.len());
        }
        drop(core);
        for (state, length) in states.iter().zip(&lengths).rev() {
            OpenOptions::new().write(true).open(&journal)?.set_len(*length)?;
            let recovered = reopen(root.path())?;
            proptest::prop_assert_eq!(view(&snapshot(&recovered)?)?, view(state)?);
            proptest::prop_assert_eq!(fs::metadata(&journal)?.len(), *length);
        }
    }
}

#[tokio::test]
async fn store_spans_omit_customer_names()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    // Two dispatchers prevent concurrent callsite interest from being cached away.
    let _second = tracing::Dispatch::new(tracing_subscriber::registry());
    let seen = crate::body::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(seen.clone()),
    );
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    assert_eq!(
        core.get(get("missing"), 1).await.err().map(|e| e.code),
        Some(NotFound)
    );
    core.collect_local_garbage(10)?;
    drop(core);
    drop(reopen(root.path())?);
    let seen = seen.0.lock().map_err(|_| "capture poisoned")?;
    assert!(!seen.is_empty());
    let path = root.path().to_string_lossy();
    for span in seen.iter() {
        for (field, value) in &span.fields {
            assert!(!["path", "token", "content", "body", "authorization"].contains(&field.as_str()));
            for leak in ["recovery", "retained", "missing", path.as_ref()] {
                assert!(!value.contains(leak), "{}.{}={value}", span.name, field);
            }
        }
    }
    Ok(())
}
