use super::*;
use crate::foundation::FileId;
use crate::kernel::{FileKind, NameEncoding, TreePage};
use crate::memory::MemoryObjectStore;

use crate::kernel::Child;
fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
    )?)
}

use crate::test_support::put_tree_page as put_page;

#[tokio::test]
async fn batch_lookup_reads_each_shared_frontier_once_and_preserves_input_order()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let left = put_page(
        &store,
        &TreePage::Leaf(vec![
            TreeEntry {
                name: name("a")?,
                file_id: FileId::from_bytes([1; 16]),
                kind: FileKind::Regular,
            },
            TreeEntry {
                name: name("b")?,
                file_id: FileId::from_bytes([2; 16]),
                kind: FileKind::Regular,
            },
        ]),
        16,
    )
    .await?;
    let right = put_page(
        &store,
        &TreePage::Leaf(vec![
            TreeEntry {
                name: name("m")?,
                file_id: FileId::from_bytes([3; 16]),
                kind: FileKind::Regular,
            },
            TreeEntry {
                name: name("n")?,
                file_id: FileId::from_bytes([4; 16]),
                kind: FileKind::Regular,
            },
        ]),
        16,
    )
    .await?;
    let root = put_page(
        &store,
        &TreePage::Internal(vec![
            Child {
                first: name("a")?,
                page: left,
            },
            Child {
                first: name("m")?,
                page: right,
            },
        ]),
        16,
    )
    .await?;
    let queries = vec![name("n")?, name("a")?, name("n")?, name("z")?];
    let result = lookup_tree_entries(
        &store,
        root,
        &queries,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        result
            .value
            .iter()
            .map(|entry| entry.as_ref().map(|entry| entry.file_id))
            .collect::<Vec<_>>(),
        vec![
            Some(FileId::from_bytes([4; 16])),
            Some(FileId::from_bytes([1; 16])),
            Some(FileId::from_bytes([4; 16])),
            None,
        ]
    );
    assert_eq!(result.work.page_reads, 3);
    assert_eq!(result.work.backend_read_operations, 2);
    assert_eq!(result.value.len(), 4);

    Ok(())
}

#[test]
fn batch_error_translation_is_total() {
    use persistent_batch::Error as BatchError;

    assert!(matches!(
        map_batch_error(BatchError::Cancelled),
        TreeReadError::Cancelled
    ));
    assert!(matches!(
        map_batch_error(BatchError::Empty),
        TreeReadError::EmptyBatch
    ));

    assert!(matches!(
        map_batch_error(BatchError::WrongRootKind),
        TreeReadError::WrongRootKind
    ));
    assert!(matches!(
        map_batch_error(BatchError::InvalidLimits),
        TreeReadError::InvalidHeightLimit
    ));
    assert!(matches!(
        map_batch_error(BatchError::HeightExceeded),
        TreeReadError::HeightExceeded
    ));
    assert!(matches!(
        map_batch_error(BatchError::CycleOrAlias),
        TreeReadError::Cycle
    ));
    assert!(matches!(
        map_batch_error(BatchError::ChildBoundsMismatch),
        TreeReadError::ChildBoundsMismatch
    ));
    assert!(matches!(
        map_batch_error(BatchError::InvalidRouting),
        TreeReadError::InvalidRouting
    ));
    assert!(matches!(
        map_batch_error(BatchError::AllocationFailed),
        TreeReadError::AllocationFailed
    ));
    assert!(matches!(
        map_batch_error(BatchError::Storage(ObjectStoreError::Missing)),
        TreeReadError::Storage(ObjectStoreError::Missing)
    ));
    assert!(matches!(
        map_batch_error(BatchError::Decode(CanonicalDecodeError::LengthOverflow)),
        TreeReadError::Decode(CanonicalDecodeError::LengthOverflow)
    ));
    assert!(matches!(
        map_batch_error(BatchError::Work(WorkError::Overflow)),
        TreeReadError::Work(WorkError::Overflow)
    ));
}
