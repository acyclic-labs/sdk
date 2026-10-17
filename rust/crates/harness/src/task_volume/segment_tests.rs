use super::*;
use crate::task_volume::{Query, TaskVolume};
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;
use std::sync::Arc;

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
