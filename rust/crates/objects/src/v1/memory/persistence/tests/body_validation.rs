use super::*;
use crate::body::count_body_lengths;
use proptest::prelude::*;

mod differential;
mod meter;
mod multipart;

fn batch(
    core: &MemoryObjects,
    requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    native: bool,
) -> Vec<Result<u64, wire::ErrorCode>> {
    if !native {
        return futures::executor::block_on(core.put_batch(requests))
            .into_iter()
            .map(|result| result.map(|info| info.size).map_err(|error| error.code))
            .collect();
    }
    let meter: Arc<dyn NativeReadMeter> = Arc::new(meter::Meter);
    let request = ObjectsNativeRequest::new(requests, (), &meter).unwrap();
    futures::executor::block_on(core.put_batch_native(request, meter).unwrap())
        .unwrap()
        .map(|result| {
            result
                .map(|info| info.info().size)
                .map_err(|error| service_error(error).code)
        })
        .collect()
}

fn core(root: &Path, limit: u64) -> MemoryObjects {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let core = open(
        root.to_path_buf(),
        LocalObjectsLimits {
            maximum_object_bytes: Some(limit),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    futures::executor::block_on(core.create_bucket(wire::CreateBucketRequest {
        name: "recovery".into(),
        mutation: None,
    }))
    .unwrap();
    core
}

// The removed validation algorithm, independently retained as the revert control.
fn previous_validation(
    journal: &Journal,
    state: &State,
    pending: Option<&StoredBody>,
) -> Result<(), Error> {
    if state
        .buckets
        .values()
        .flat_map(|bucket| bucket.objects.values())
        .map(|object| &object.body)
        .chain(
            state
                .uploads
                .values()
                .flat_map(|upload| upload.parts.values())
                .map(|(_, body)| body),
        )
        .chain(pending)
        .any(|body| {
            journal
                .limits
                .maximum_object_bytes
                .is_some_and(|limit| body.len() as u64 > limit)
        })
    {
        return Err(QuotaExceeded.into());
    }
    Ok(())
}

fn continuation_work(turns: usize, native: bool, rewalk: bool) -> Vec<usize> {
    let root = tempfile::tempdir().unwrap();
    let core = core(root.path(), 64);
    (0..turns)
        .map(|turn| {
            let (result, reads) = count_body_lengths(|| {
                if rewalk {
                    previous_validation(
                        core.journal.as_ref().unwrap(),
                        &core.lock_state().unwrap(),
                        None,
                    )
                    .unwrap();
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
            assert_eq!(result.len(), 1);
            assert!(result[0].is_ok(), "{result:?}");
            reads
        })
        .collect()
}

fn constant_work(work: &[usize], expected: usize) -> bool {
    expected != 0 && work.iter().all(|&count| count == expected)
}

#[test]
fn continuation_body_validation_is_independent_of_the_validated_prefix() {
    for native in [false, true] {
        let small = continuation_work(16, native, false);
        let large = continuation_work(32, native, false);
        let rewalk = continuation_work(32, native, true);
        // Publish metadata, aggregate quota, pending body, persisted body.
        let expected = 4;
        eprintln!("body lengths per continuation: N={small:?} 2N={large:?} rewalk={rewalk:?}");
        assert!(constant_work(&small, expected));
        assert!(constant_work(&large, expected));
        assert!(!constant_work(&rewalk, expected));
        assert_eq!(large.iter().sum::<usize>(), small.iter().sum::<usize>() * 2);
    }
}

#[tokio::test]
async fn inline_and_external_upload_parts_establish_the_same_validated_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    for limit in [4, inline::LIMIT - 1, inline::LIMIT, inline::LIMIT + 1] {
        let root = tempfile::tempdir()?;
        let core = core(root.path(), limit as u64);
        let upload = core
            .create_multipart(wire::CreateMultipartRequest {
                bucket: bucket(),
                object_key: "upload".into(),
                ..Default::default()
            })
            .await?;
        let query = wire::UploadPartHeader {
            bucket: bucket(),
            object_key: "upload".into(),
            upload_id: upload.upload_id.clone(),
            part_number: 1,
            mutation: None,
        };
        let original = core
            .upload_part(query.clone(), OwnedBytes::from(vec![42; limit]))
            .await?;
        let retained = core.lock_state()?.uploads[upload.upload_id.as_str()].parts[&1]
            .1
            .clone();
        let composite = StoredBody::Composite {
            parts: Arc::new(crate::v1::Guarded {
                value: vec![retained.clone(), retained],
                _allocation: None,
            }),
            length: limit * 2,
        };
        assert!(!unpersisted(&composite));
        let mut inline_data = Vec::new();
        assert_eq!(
            core.journal
                .as_ref()
                .unwrap()
                .prepare_inline(&composite, &mut inline_data, &mut Publication::new(None),)
                .err()
                .map(|error| service_error(error).code),
            Some(QuotaExceeded)
        );
        assert!(inline_data.is_empty());
        let before = fs::read(root.path().join("mutations.log"))?;
        let sequence = core.lock_state()?.sequence;
        assert_eq!(
            core.upload_part(query, OwnedBytes::from(vec![0; limit + 1]))
                .await
                .err()
                .map(|error| error.code),
            Some(QuotaExceeded)
        );
        assert_eq!(core.lock_state()?.sequence, sequence);
        assert_eq!(fs::read(root.path().join("mutations.log"))?, before);
        drop(core);
        let core = open(
            root.path().to_path_buf(),
            LocalObjectsLimits {
                maximum_object_bytes: Some(limit as u64),
                ..Default::default()
            },
            None,
        )?;
        core.complete_multipart(wire::CompleteMultipartRequest {
            bucket: bucket(),
            object_key: "upload".into(),
            upload_id: upload.upload_id,
            parts: vec![original],
            ..Default::default()
        })
        .await?;
        assert_eq!(
            core.get(get(bucket(), "upload"), limit as u64)
                .await?
                .into_body(),
            OwnedBytes::from(vec![42; limit])
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn batches_preserve_individual_size_rejections_and_reopened_bytes(
        limit in 1u64..17,
        lengths in prop::collection::vec(0usize..25, 1..17),
        native in any::<bool>(),
    ) {
        let root = tempfile::tempdir().unwrap();
        let core = core(root.path(), limit);
        let requests: Vec<_> = lengths.iter().enumerate().map(|(index, &length)| (
            put(&format!("item-{index}")), OwnedBytes::from(vec![index as u8; length]),
        )).collect();
        let results = batch(&core, requests, native);
        for (&length, result) in lengths.iter().zip(&results) {
            prop_assert_eq!(*result,
                if length as u64 <= limit { Ok(length as u64) } else { Err(QuotaExceeded) });
        }
        drop(core);
        let core = open(root.path().to_path_buf(), LocalObjectsLimits {
            maximum_object_bytes: Some(limit), ..Default::default()
        }, None).unwrap();
        for (index, &length) in lengths.iter().enumerate() {
            let result = futures::executor::block_on(core.get(get(bucket(), &format!("item-{index}")), 25));
            if length as u64 <= limit {
                let body = result.unwrap().into_body();
                prop_assert_eq!(body.as_slice(), vec![index as u8; length]);
            } else {
                prop_assert_eq!(result.err().map(|error| error.code), Some(NotFound));
            }
        }
    }
}
