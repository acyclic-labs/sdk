use super::*;
use crate::cache::{CachedObjectStore, ObjectCacheOptions};
use crate::foundation::{Digest, FileId};
use crate::kernel::file_table_mutation::FileTableFormat;
use crate::kernel::tree_mutation::TreeFormat;
use crate::kernel::{
    FileKind, FilePayload, FileRecord, FileTableChild, FileTablePage, LogicalName, NameEncoding,
    TreeEntry, encode_file_table_page, file_table_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectKind, ObjectStore};
use bytes::Bytes;

fn record(byte: u8) -> FileRecord {
    FileRecord {
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([byte; 32]),
        },
        payload: FilePayload::Empty,
    }
}

fn put(
    store: &MemoryObjectStore,
    page: &FileTablePage,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = file_table_page_id(page, 8)?;
    ObjectStore::put(
        store,
        id,
        Bytes::from(encode_file_table_page(page, 8)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(id)
}

fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
        255,
    )?)
}

#[test]
fn authenticated_leaf_and_internal_bounds_are_total() -> Result<(), Box<dyn std::error::Error>> {
    let (first, second) = (record(1), record(2));
    assert!(
        validate_leaf::<FileTableFormat>(&[first], Some(&first.file_id), Some(&second.file_id))
            .is_ok()
    );
    assert!(matches!(
        validate_leaf::<FileTableFormat>(&[first], Some(&second.file_id), None),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_leaf::<FileTableFormat>(&[first], Some(&first.file_id), Some(&first.file_id)),
        Err(Error::ChildBoundsMismatch)
    ));
    let child = Child {
        first: first.file_id,
        page: first.metadata,
    };
    assert!(
        validate_children::<FileTableFormat>(
            std::slice::from_ref(&child),
            Some(&first.file_id),
            Some(&second.file_id)
        )
        .is_ok()
    );
    assert!(matches!(
        validate_children::<FileTableFormat>(
            std::slice::from_ref(&child),
            Some(&second.file_id),
            None
        ),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_children::<FileTableFormat>(
            std::slice::from_ref(&child),
            None,
            Some(&first.file_id)
        ),
        Err(Error::ChildBoundsMismatch)
    ));
    let entries = [
        TreeEntry {
            name: name("b")?,
            file_id: first.file_id,
            kind: FileKind::Regular,
        },
        TreeEntry {
            name: name("c")?,
            file_id: second.file_id,
            kind: FileKind::Regular,
        },
    ];
    assert!(validate_leaf::<TreeFormat>(&entries, Some(&name("b")?), Some(&name("d")?)).is_ok());
    assert!(matches!(
        validate_leaf::<TreeFormat>(&entries, None, Some(&name("c")?)),
        Err(Error::ChildBoundsMismatch)
    ));
    Ok(())
}

#[test]
fn cached_pages_answer_identically_without_backend_reads() -> Result<(), Box<dyn std::error::Error>>
{
    let memory = MemoryObjectStore::default();
    let low = put(&memory, &FileTablePage::Leaf(vec![record(1), record(2)]))?;
    let high = put(&memory, &FileTablePage::Leaf(vec![record(5), record(6)]))?;
    let root = put(
        &memory,
        &FileTablePage::Internal(vec![
            FileTableChild {
                first_file_id: record(1).file_id,
                page: low,
            },
            FileTableChild {
                first_file_id: record(5).file_id,
                page: high,
            },
        ]),
    )?;
    let store = CachedObjectStore::new(memory, ObjectCacheOptions::default())?;
    let cancellation = CancellationToken::new();
    let lookup = |file_id: FileId| {
        crate::async_storage::poll_immediate(lookup_async::<_, FileTableFormat>(
            &store,
            root,
            &file_id,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
    };
    let cold = lookup(record(6).file_id)?;
    assert_eq!(cold.value, Some(record(6)));
    assert_eq!(cold.work.page_reads, 2);
    assert!(cold.work.backend_read_operations > 0);
    let warm = lookup(record(6).file_id)?;
    assert_eq!(warm.value, Some(record(6)));
    assert_eq!(warm.work.page_reads, 2);
    assert_eq!(
        warm.work.backend_read_operations, 0,
        "decoded pages are shared between lookups"
    );
    assert_eq!(lookup(record(3).file_id)?.value, None);
    Ok(())
}
