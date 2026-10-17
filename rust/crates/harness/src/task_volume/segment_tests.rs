use super::*;
use crate::task_volume::{Query, TaskVolume};
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;
use std::sync::Arc;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Attribution survives both the persisted segment and public file codecs.
    #[test]
    fn child_request_attribution_round_trips(
        requester in proptest::option::of(any::<[u8; 16]>()),
        bytes in proptest::collection::vec(any::<u8>(), 0..128),
    ) {
        let mut write = Write::new(FileKey::parse("agents/worker/request.md").unwrap(), bytes);
        write.requester = requester.map(crate::TaskId::from_bytes);
        let (encoded, _) = encode(&write, None).unwrap();
        let decoded = decode(&encoded).unwrap();
        prop_assert_eq!(&decoded, &write);
        prop_assert_eq!(decoded.requester(), write.requester());
        let json = serde_json::to_vec(&decoded).unwrap();
        prop_assert_eq!(serde_json::from_slice::<Write>(&json).unwrap(), write);
    }
}

#[test]
fn request_attribution_corpus_rejects_non_request_claims() {
    for (bytes, authored) in [
        (
            include_bytes!("../../../harness-fuzz/corpus/child_control/authored-segment")
                .as_slice(),
            Some(true),
        ),
        (
            include_bytes!("../../../harness-fuzz/corpus/child_control/legacy-segment").as_slice(),
            Some(false),
        ),
        (
            include_bytes!("../../../harness-fuzz/corpus/child_control/forged-control-segment")
                .as_slice(),
            None,
        ),
    ] {
        match authored {
            Some(authored) => assert_eq!(
                decode(&bytes.to_vec().into())
                    .unwrap()
                    .requester()
                    .is_some(),
                authored
            ),
            None => assert!(matches!(
                decode(&bytes.to_vec().into()),
                Err(Error::InvalidSegment)
            )),
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn goal_file_publication_survives_segment_and_query_json(
        publication in prop::option::of(any::<[u8; 16]>()), bytes in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        let mut file = FileValue::new(FileKey::Goal, bytes);
        file.publication = publication.map(crate::OperationId::from_bytes);
        let (encoded, accepted) = encode(&file, None).unwrap();
        prop_assert_eq!(record_bytes(&file).unwrap(), encoded.len());
        prop_assert_eq!(&accepted, &file);
        prop_assert_eq!(decode(&encoded).unwrap(), file.clone());
        let json = serde_json::to_vec(&file).unwrap();
        prop_assert_eq!(serde_json::from_slice::<FileValue>(&json).unwrap(), file);
    }
}

#[tokio::test]
async fn native_publication_funds_packing_and_retains_original_custody()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let payload = vec![b'x'; 32_768];
    let write = Write::new(
        FileKey::parse(".tasks/t/requests/search-one/result.json")?,
        payload.clone(),
    );
    for denial in [None, Some("copy"), Some("memory")] {
        let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
        if denial == Some("copy") {
            budget.bytes_copied = payload.len() as u64 - 1;
        }
        if denial == Some("memory") {
            budget.peak_allocation_bytes = 8192;
        }
        let work = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let stream = Arc::new(MemoryStream::default());
        let volume =
            TaskVolume::open(stream.clone(), StreamPath::new("native-publication")?).await?;
        let before = volume.snapshot(&Query::Root, 1).await?;
        let result = volume
            .append_if(before.generation, &write, vec![], None, Some(&work))
            .await;
        let after = volume.snapshot(&Query::Root, 1).await?;
        if denial.is_some() {
            assert!(
                matches!(result, Err(Error::FileRead(ref error)) if error.is_native_failure()),
                "{denial:?}: {result:?}"
            );
            assert_eq!(after.generation, before.generation);
            assert!(after.files.is_empty());
            assert_eq!(work.live_allocation_bytes(), baseline);
            assert!(work.receipt().bytes_copied < payload.len() as u64);
        } else {
            assert_eq!(result?, after.generation);
            let file = after.files.first().ok_or("missing published file")?;
            assert_eq!(file, &write);
            assert!(file.bytes.matches_meter(&work.runtime_meter()));
            assert!(file.bytes.retained_capacity().unwrap() > payload.len());
            assert!(work.receipt().bytes_copied > payload.len() as u64);
            assert!(work.receipt().bytes_encoded > 0);
            assert!(work.live_allocation_bytes() > baseline + payload.len() as u64);
        }
        drop(after);
        drop(volume);
        drop(stream);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

// Original packing is initialized independently and calls neither the candidate
// encoder nor its decoder. It retains the old temporary payload allocation.
fn original_chunk_record() -> Vec<u8> {
    #[derive(Serialize)]
    struct OriginalEntry {
        key: FileKey,
        channel: Option<Channel>,
    }
    let key = FileKey::parse(".tasks/task/turns/0/chunks/02020202-0202-0202-0202-020202020202/2")
        .unwrap();
    let payload: OwnedBytes = b"authenticated delta".to_vec().into();
    let metadata = serde_json::to_vec(&OriginalEntry {
        key,
        channel: Some(Channel::Text),
    })
    .unwrap();
    let mut record = Vec::with_capacity(12 + metadata.len() + payload.len());
    record.extend_from_slice(b"ACYTASK1");
    record.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    record.extend_from_slice(&metadata);
    record.extend_from_slice(&payload);
    record
}

#[test]
fn borrowed_chunk_packing_has_exact_allocations_with_copy_negative_control() {
    let key = FileKey::Task {
        task: crate::task_volume::Component::new("task").unwrap(),
        file: crate::task_volume::TaskFile::Chunk {
            turn: 0,
            execution: crate::OperationId::from_bytes([2; 16]),
            sequence: 2,
        },
    };
    let payload = b"authenticated delta";
    for native in [false, true] {
        let run = |copy: bool| {
            crate::test_support::accounted::accounted(native, 1 << 24, false, |io| {
                let before = io.receipt();
                let mut output = None;
                let allocations = acyclic_test_support::measure(|| {
                    let copied = copy.then(|| OwnedBytes::copy_from_slice(payload));
                    let bytes = copied.as_deref().unwrap_or(payload);
                    output = Some(
                        encode_parts(
                            &key,
                            Some(Channel::Text),
                            None,
                            None,
                            bytes,
                            if native {
                                io.original_native_work()
                            } else {
                                None
                            },
                        )
                        .unwrap()
                        .0,
                    );
                });
                let after = io.receipt();
                (
                    output.unwrap().to_vec(),
                    allocations.count_total,
                    after.allocation_operations - before.allocation_operations,
                )
            })
            .unwrap()
            .0
        };
        let candidate = run(false);
        let baseline = run(true);
        assert_eq!(candidate.0, original_chunk_record());
        assert_eq!(candidate.0, baseline.0);
        eprintln!(
            "packing native={native} candidate={} native_ops={} copied={}",
            candidate.1, candidate.2, baseline.1
        );
        assert_eq!(candidate.1, if native { 6 } else { 5 });
        assert_eq!(candidate.2, if native { 4 } else { 0 });
        assert_eq!(
            baseline.1,
            candidate.1 + 1,
            "redundant OwnedBytes payload copy escaped exact allocation assertion"
        );
    }
}
