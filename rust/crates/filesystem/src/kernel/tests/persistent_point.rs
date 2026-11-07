use super::*;
use crate::foundation::{Digest, FileId};
use crate::kernel::file_table_mutation::FileTableFormat;
use crate::kernel::persistent_btree::Child;
use crate::kernel::tree_mutation::TreeFormat;
use crate::kernel::{
    FileKind, FilePayload, FileRecord, FileTablePage, LogicalName, NameEncoding, TreeEntry,
};
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectKind;

fn record(byte: u8) -> FileRecord {
    FileRecord {
        bindings: None,
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: ObjectId {
            storage: None,
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([byte; 32]),
        },
        payload: FilePayload::Empty,
    }
}

use crate::test_support::put_file_table_page as put;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn projected_point_retires_original_scratch_on_close_and_withdrawal()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::retained::Retained;
    use std::sync::Arc;

    let store = crate::test_support::PausedStore::new(Arc::new(MemoryObjectStore::default()), None);
    let first = put(&store, &FileTablePage::Leaf(vec![record(1), record(2)]), 8).await?;
    let last = put(&store, &FileTablePage::Leaf(vec![record(3)]), 8).await?;
    let root = put(
        &store,
        &FileTablePage::Internal(vec![
            Child {
                first: record(1).file_id,
                page: first,
            },
            Child {
                first: record(3).file_id,
                page: last,
            },
        ]),
        8,
    )
    .await?;
    let budget = crate::performance::BOUNDARY_BUDGET;
    for native in [false, true] {
        for outcome in 0..4 {
            let cancellation = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let mut accounting = if native {
                KernelAccounting::native(&original, &cancellation)
            } else {
                KernelAccounting::standalone(budget, &cancellation)
            };
            let live = |accounting: &mut KernelAccounting<'_>| {
                accounting.standalone_parts().map_or_else(
                    || original.live_allocation_bytes(),
                    |(ledger, ..)| ledger.live_bytes(),
                )
            };
            let held = Owned::<Vec<u8>>::allocate(4096, &mut accounting)?;
            let held = Retained::from_owned(held, &mut accounting)?;
            // The ordinary ledger's shared control lives until the ledger drops.
            let ledger_baseline = live(&mut accounting) - held.value.capacity() as u64;
            let mut earlier = Owned::<Vec<u8>>::allocate(128, &mut accounting)?;
            earlier.value.push(7);
            accounting.charge_items(1)?;
            let baseline = live(&mut accounting);
            let before = accounting.spent();
            let selected = record(if outcome == 1 { 7 } else { 2 }).file_id;
            let (producer, completion) = tokio::sync::oneshot::channel();
            *store.release.lock().unwrap() = Some(completion);
            let mut operation = Box::pin(lookup_projected_with_accounting::<
                _,
                FileTableFormat,
                _,
                _,
                _,
            >(
                &store,
                root,
                |candidate| candidate.cmp(&selected),
                |value, accounting| {
                    assert_eq!(*value, record(2));
                    let mut output = Owned::<Vec<u8>>::allocate(37, accounting)?;
                    output.value.push(2);
                    Ok(output)
                },
                DecodeLimits::default(),
                &mut accounting,
                &cancellation,
                None,
            ));
            assert!(futures::poll!(operation.as_mut()).is_pending());
            assert!(store.release.lock().unwrap().is_none());
            match outcome {
                0 | 1 => {
                    producer.send(()).map_err(|_| "point read receiver lost")?;
                    let output = operation.await?;
                    if outcome == 0 {
                        let output = output.ok_or("selected point is absent")?;
                        assert_eq!(output.value, [2]);
                        assert_eq!(
                            live(&mut accounting),
                            baseline + output.value.capacity() as u64
                        );
                        output.retire(&mut accounting)?;
                    } else {
                        assert!(output.is_none());
                    }
                }
                2 => {
                    drop(producer);
                    assert!(matches!(
                        operation.await,
                        Err(crate::FsError::Object(crate::ObjectStoreError::Rejected(message)))
                            if message == "cache fill producer lost"
                    ));
                }
                _ => {
                    cancellation.cancel();
                    drop(operation);
                    drop(producer);
                }
            }
            accounting.spent().verify(budget)?;
            let pages = if outcome < 2 { 2 } else { 1 };
            assert_eq!(accounting.spent().page_reads, before.page_reads + pages);
            assert!(accounting.spent().items_examined >= before.items_examined);
            assert_eq!(live(&mut accounting), baseline);
            assert_eq!(earlier.value, [7]);
            earlier.retire(&mut accounting)?;
            drop(held);
            assert_eq!(live(&mut accounting), ledger_baseline);
        }
    }
    Ok(())
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
    assert!(leaf_within::<FileTableFormat>(
        &[first],
        Some(&first.file_id),
        Some(&second.file_id)
    ));
    assert!(!leaf_within::<FileTableFormat>(
        &[first],
        Some(&second.file_id),
        None
    ));
    assert!(!leaf_within::<FileTableFormat>(
        &[first],
        Some(&first.file_id),
        Some(&first.file_id)
    ));
    let child = Child {
        first: first.file_id,
        page: first.metadata,
    };
    assert!(children_within(
        std::slice::from_ref(&child),
        Some(&first.file_id),
        Some(&second.file_id)
    ));
    assert!(!children_within(
        std::slice::from_ref(&child),
        Some(&second.file_id),
        None
    ));
    assert!(!children_within(
        std::slice::from_ref(&child),
        None,
        Some(&first.file_id)
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
    assert!(leaf_within::<TreeFormat>(
        &entries,
        Some(&name("b")?),
        Some(&name("d")?)
    ));
    assert!(!leaf_within::<TreeFormat>(
        &entries,
        None,
        Some(&name("c")?)
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_point_uses_the_original_counters_and_retires_scratch()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &FileTablePage::Leaf(vec![record(1), record(2)]), 8).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let (value, allocation) = lookup_async_with_accounting::<_, FileTableFormat>(
        &store,
        root,
        &record(2).file_id,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
        None,
    )
    .await?;
    assert_eq!(value, Some(record(2)));
    assert_eq!(original.receipt().page_reads, 1);
    assert!(original.receipt().backend_read_operations > 0);
    drop(allocation);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_point_denies_scratch_before_provider_work() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let root = put(&store, &FileTablePage::Leaf(vec![record(1)]), 8).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = lookup_async_with_accounting::<_, FileTableFormat>(
        &store,
        root,
        &record(1).file_id,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
        None,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(original.receipt(), before);
    assert_eq!(original.receipt().backend_read_operations, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_owned_reserve_preserves_the_actual_allocator_error()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = crate::kernel::persistent_point::Owned::<Vec<u8>>::allocate(
        isize::MAX as usize + 1,
        &mut accounting,
    );
    assert!(matches!(
        result,
        Err(crate::FsError::NativeKernel(
            crate::NativeTreeMaterializationError::AllocationFailed(_)
        ))
    ));
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn owned_push_respects_the_logical_limit_with_spare_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut values =
        crate::kernel::persistent_point::Owned::<Vec<u8>>::allocate(2, &mut accounting)?;
    values.value.push(1);
    assert!(values.ensure_for_push(1, &mut accounting).is_err());
    values.retire(&mut accounting)?;
    Ok(())
}
