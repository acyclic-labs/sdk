use super::*;
use crate::kernel::{FileKind, FilePayload, FileRecord, FileTablePage, encode_file_table_page};

async fn scenario(share_narrowed_cache: bool) -> Result<bool, Box<dyn std::error::Error>> {
    let store = store(4096)?;
    let value = FileRecord {
        file_id: crate::FileId::from_bytes([7; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: crate::ObjectId {
            kind: ObjectKind::Metadata,
            digest: crate::Digest::ZERO,
        },
        payload: FilePayload::Empty,
    };
    let bytes = encode_file_table_page(
        &FileTablePage::Leaf(vec![value]),
        8,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let object = HashedObject::new(ObjectKind::FileTablePage, bytes.into());
    let cancellation = CancellationToken::new();
    store
        .put_hashed(
            object.clone(),
            &mut AuthorityWork::new(BUDGET, &cancellation),
        )
        .await?;
    for _ in 0..2 {
        let result = crate::kernel::lookup_file_record(
            &store,
            object.object_id(),
            value.file_id,
            Default::default(),
            BUDGET,
            &cancellation,
        )
        .await?;
        assert_eq!(result.value, Some(value));
    }
    assert!(store.decoded.stats()?.decoded_hits > 0);
    let mut narrowed = store.clone().with_object_limit(1)?;
    if share_narrowed_cache {
        narrowed.decoded = store.decoded.clone();
    }
    let result = crate::kernel::lookup_file_record(
        &narrowed,
        object.object_id(),
        value.file_id,
        Default::default(),
        BUDGET,
        &cancellation,
    )
    .await;
    Ok(result.is_err())
}
#[tokio::test]
async fn logical_adapter_reuses_decodes_without_bypassing_a_narrowed_limit() {
    assert!(
        scenario(false).await.unwrap(),
        "decoded cache bypassed narrowed object bound"
    );
}
#[tokio::test]
async fn narrowed_cache_negative_control() {
    let rejected = scenario(true).await.unwrap();
    let fault = std::panic::catch_unwind(|| {
        assert!(rejected, "decoded cache bypassed narrowed object bound")
    })
    .expect_err("shared narrowed cache survived");
    let text = fault
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| fault.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("decoded cache bypassed narrowed object bound"),
        "{text}"
    );
}
