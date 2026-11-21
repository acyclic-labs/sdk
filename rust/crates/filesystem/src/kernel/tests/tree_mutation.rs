use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::kernel::decode_tree_page;
use crate::kernel::{FileKind, TreePage, encode_tree_page, tree_page_id};
use crate::memory::MemoryObjectStore;

#[path = "tree_mutation/canonical_history.rs"]
mod canonical_history;
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_mutation_refuses_index_copy_before_allocating_or_reading()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let original = entry("a", 1, FileKind::Regular)?;
    let root = put(&store, &TreePage::Leaf(vec![original.clone()]), 8).await?;
    let mutations = [TreeMutation::Replace {
        expected_file_id: original.file_id,
        entry: original,
    }];
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::root(
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
        &crate::services::ServicesInitialized::for_kernel_tests(),
    )
    .map_err(|(error, _)| error)?;
    let before = work.receipt().unwrap();
    let live = work.live_allocation_bytes().unwrap();
    let mut accounting = super::super::allocation::KernelAccounting::native(&work, &cancellation);
    let error = apply_tree_mutations_with_accounting(
        &store,
        root,
        &mutations,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    )
    .await
    .expect_err("index copy must be admitted before allocating");
    assert!(matches!(
        error,
        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_))
            | crate::FsError::Work(_)
    ));
    assert_eq!(work.receipt().unwrap(), before);
    assert_eq!(work.live_allocation_bytes().unwrap(), live);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_mutation_write_denial_preserves_paid_orphan_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = two_leaf_tree(&store).await?;
    let first = entry("a", 1, FileKind::Fifo)?;
    let first_leaf = TreePage::Leaf(vec![first.clone(), entry("m", 2, FileKind::Regular)?]);
    let first_page = tree_page_id(&first_leaf, 8)?;
    let second = entry("x", 4, FileKind::Fifo)?;
    let second_leaf = TreePage::Leaf(vec![entry("n", 3, FileKind::Regular)?, second.clone()]);
    let second_page = tree_page_id(&second_leaf, 8)?;
    let mutations = [
        TreeMutation::Replace {
            entry: first,
            expected_file_id: FileId::from_bytes([1; 16]),
        },
        TreeMutation::Replace {
            entry: second,
            expected_file_id: FileId::from_bytes([4; 16]),
        },
    ];
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::root(
        WorkBudget {
            page_writes: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
        &crate::services::ServicesInitialized::for_kernel_tests(),
    )
    .map_err(|(error, _)| error)?;
    let mut accounting = super::super::allocation::KernelAccounting::native(&work, &cancellation);
    let error = apply_tree_mutations_with_accounting(
        &store,
        root,
        &mutations,
        pair_limits(),
        &mut accounting,
        &cancellation,
    )
    .await
    .expect_err("second changed leaf must refuse on original page-write limit");
    let error = match error {
        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(error))
        | crate::FsError::Work(error) => error,
        error => return Err(error.into()),
    };
    assert!(matches!(
        error,
        WorkError::BudgetExceeded {
            counter: crate::WorkCounter::PageWrites,
            observed: 2,
            maximum: 1,
        }
    ));
    assert_eq!(work.receipt().unwrap().page_writes, 1);
    assert_eq!(work.receipt().unwrap().backend_write_operations, 1);
    assert!(
        store
            .read(
                first_page,
                u64::MAX,
                &mut crate::AuthorityWork::new(
                    WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                ),
            )
            .await
            .is_ok()
    );
    let first_page_bytes = encode_tree_page(
        &first_leaf,
        8,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let second_page_bytes = encode_tree_page(
        &second_leaf,
        8,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    // Both identities must be encoded before resynchronization can distinguish
    // an existing page from a write needing the caller's remaining credit.
    assert_eq!(
        work.receipt().unwrap().bytes_encoded,
        (first_page_bytes.len() + second_page_bytes.len()) as u64
    );
    assert_eq!(
        work.receipt().unwrap().object_bytes_written,
        first_page_bytes.len() as u64
    );
    assert!(matches!(
        store
            .read(
                second_page,
                u64::MAX,
                &mut crate::AuthorityWork::new(
                    WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                ),
            )
            .await,
        Err(crate::storage::ObjectStoreError::Missing)
    ));
    drop(store);
    assert_eq!(
        work.live_allocation_bytes().unwrap(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}
use crate::test_support::put_tree_page as put;
use crate::test_support::tree_entries::{entry, name};

#[cfg(not(target_arch = "wasm32"))]
#[path = "../../../tests/support/two_leaf_tree.rs"]
mod pair;
#[cfg(not(target_arch = "wasm32"))]
use crate::test_support::put_tree_page;
#[cfg(not(target_arch = "wasm32"))]
use pair::{pair_limits, two_leaf_tree};

#[tokio::test]
async fn point_inserts_rewrite_bounded_pages_however_large_the_directory()
-> Result<(), Box<dyn std::error::Error>> {
    const ENTRIES: u32 = 3_000;
    let store = MemoryObjectStore::default();
    let mut root = put(&store, &TreePage::Leaf(Vec::new()), 1_024).await?;
    let mut largest_late_rewrite = 0;
    let mut late_rewrites = 0;
    for step in 0..ENTRIES {
        // A fixed permutation spreads inserts across every leaf.
        let ordinal = step * 7_919 % ENTRIES;
        let receipt = apply_tree_mutations(
            &store,
            root,
            vec![TreeMutation::Insert(TreeEntry {
                name: name(&format!("entry-{ordinal:06}"))?,
                file_id: FileId::from_bytes([u8::try_from(ordinal % 251)?; 16]),
                kind: FileKind::Regular,
            })],
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
        root = receipt.value;
        if step >= ENTRIES / 2 {
            largest_late_rewrite = largest_late_rewrite.max(receipt.work.bytes_encoded);
            late_rewrites += receipt.work.bytes_encoded;
        }
    }
    // Only the touched leaf and its ancestors are re-encoded. Content-defined
    // page sizes are geometric around the soft target, so an insert lands in
    // a page of twice the target on average and rarely past a few times it,
    // however many entries the directory already holds.
    let target = u64::try_from(crate::kernel::persistent_btree::TARGET_PAGE_BYTES)?;
    let mean = late_rewrites / u64::from(ENTRIES - ENTRIES / 2);
    assert!(
        mean <= 6 * target,
        "an insert re-encoded {mean} bytes on average"
    );
    assert!(
        largest_late_rewrite <= 16 * target,
        "one insert re-encoded {largest_late_rewrite} bytes"
    );
    let listing = crate::kernel::list_tree_entries(
        &store,
        root,
        None,
        ENTRIES,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?;
    let names = listing
        .entries
        .iter()
        .map(|entry| entry.name.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let expected = (0..ENTRIES)
        .map(|ordinal| format!("entry-{ordinal:06}").into_bytes())
        .collect::<Vec<_>>();
    assert_eq!(names, expected);
    Ok(())
}
