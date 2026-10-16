use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::Digest;
use crate::kernel::{decode_extent_page, encode_extent_page, extent_page_id};
use crate::memory::MemoryObjectStore;

use crate::storage::object_digest;

async fn put(
    store: &MemoryObjectStore,
    page: &ExtentPage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = extent_page_id(page, maximum_items)?;
    {
        let __object_args = (
            id,
            acyclic_stream::OwnedBytes::from(encode_extent_page(
                page,
                maximum_items,
                |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
                |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
            )?),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    Ok(id)
}

fn hole(offset: u64, length: u64) -> Extent {
    Extent {
        offset,
        length,
        kind: ExtentKind::Hole,
    }
}

fn allocated(offset: u64, length: u64) -> Extent {
    Extent {
        offset,
        length,
        kind: ExtentKind::AllocatedZero,
    }
}

async fn three_leaf_tree(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, [ObjectId; 3]), Box<dyn std::error::Error>> {
    let leaves = [
        put(store, &ExtentPage::Leaf(vec![hole(0, 100)]), 8).await?,
        put(store, &ExtentPage::Leaf(vec![hole(100, 100)]), 8).await?,
        put(store, &ExtentPage::Leaf(vec![hole(200, 100)]), 8).await?,
    ];
    let root = put(
        store,
        &ExtentPage::Internal(vec![
            ExtentChild {
                first_offset: 0,
                end_offset: 100,
                page: leaves[0],
            },
            ExtentChild {
                first_offset: 100,
                end_offset: 200,
                page: leaves[1],
            },
            ExtentChild {
                first_offset: 200,
                end_offset: 300,
                page: leaves[2],
            },
        ]),
        8,
    )
    .await?;
    Ok((root, leaves))
}

async fn root_children(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<ExtentChild>, Box<dyn std::error::Error>> {
    let bytes = {
        let __object_args = (
            root,
            u64::MAX,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?
    .value;
    let ExtentPage::Internal(children) =
        decode_extent_page(&bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("extent root is not internal".into());
    };
    Ok(children)
}

async fn leaf_extents(
    store: &MemoryObjectStore,
    page: ObjectId,
) -> Result<Vec<Extent>, Box<dyn std::error::Error>> {
    let bytes = {
        let __object_args = (
            page,
            u64::MAX,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?
    .value;
    let ExtentPage::Leaf(extents) =
        decode_extent_page(&bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("extent page is not a leaf".into());
    };
    Ok(extents)
}

#[tokio::test]
async fn rewrites_only_intersecting_leaf_and_shared_root() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let (root, leaves) = three_leaf_tree(&store).await?;
    let receipt = apply_extent_mutations(
        &store,
        (root, 300),
        &[ExtentMutation::Replace {
            offset: 120,
            length: 10,
            kind: ExtentKind::AllocatedZero,
            extend: false,
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let children = root_children(&store, receipt.root).await?;
    assert_eq!(children[0].page, leaves[0]);
    assert_ne!(children[1].page, leaves[1]);
    assert_eq!(children[2].page, leaves[2]);
    assert_eq!(
        leaf_extents(&store, children[1].page).await?,
        vec![hole(100, 20), allocated(120, 10), hole(130, 70)]
    );
    Ok(())
}

#[tokio::test]
async fn semantic_noop_preserves_root_without_writes() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let receipt = apply_extent_mutations(
        &store,
        (root, 300),
        &[ExtentMutation::Replace {
            offset: 120,
            length: 10,
            kind: ExtentKind::Hole,
            extend: false,
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.root, root);
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 0);
    assert_eq!(receipt.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_extent_mutation_performs_zero_work() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let root = put(&store, &ExtentPage::Leaf(vec![hole(0, 100)]), 8).await?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = apply_extent_mutations(
        &store,
        (root, 100),
        &[ExtentMutation::Resize { logical_bytes: 50 }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .err()
    .ok_or("cancelled extent mutation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentMutationError::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn decoded_extent_allocation_is_admitted_after_shape_read_before_decode()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(
        &store,
        &ExtentPage::Leaf(vec![hole(0, 25), allocated(25, 25), hole(50, 50)]),
        8,
    )
    .await?;
    let mutation = [ExtentMutation::Replace {
        offset: 25,
        length: 25,
        kind: ExtentKind::AllocatedZero,
        extend: false,
    }];
    let baseline = apply_extent_mutations(
        &store,
        (root, 100),
        &mutation,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let maximum = baseline.work.peak_allocation_bytes - 1;
    let failure = apply_extent_mutations(
        &store,
        (root, 100),
        &mutation,
        1,
        DecodeLimits::default(),
        WorkBudget {
            peak_allocation_bytes: maximum,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("underbudgeted decoded extent page unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentMutationError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum: observed_maximum,
        }) if observed == baseline.work.peak_allocation_bytes && observed_maximum == maximum
    ));
    assert_eq!(failure.work.backend_read_operations, 1);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn truncate_drops_later_subtree_without_reading_it() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let (root, leaves) = three_leaf_tree(&store).await?;
    let receipt = apply_extent_mutations(
        &store,
        (root, 300),
        &[ExtentMutation::Resize { logical_bytes: 150 }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let children = root_children(&store, receipt.root).await?;
    assert_eq!(children.len(), 2);
    assert_eq!(children[0].page, leaves[0]);
    assert_eq!(children[1].end_offset, 150);
    Ok(())
}

#[tokio::test]
async fn extension_reads_only_last_frontier_and_appends_hole()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, leaves) = three_leaf_tree(&store).await?;
    let receipt = apply_extent_mutations(
        &store,
        (root, 300),
        &[ExtentMutation::Resize { logical_bytes: 500 }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let children = root_children(&store, receipt.root).await?;
    assert_eq!(children[0].page, leaves[0]);
    assert_eq!(children[1].page, leaves[1]);
    assert_eq!(children[2].end_offset, 500);
    assert_eq!(
        leaf_extents(&store, children[2].page).await?,
        vec![hole(200, 300)]
    );
    Ok(())
}

#[tokio::test]
async fn content_offsets_advance_across_leaf_boundaries() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([7; 32]),
    };
    let receipt = apply_extent_mutations(
        &store,
        (root, 300),
        &[ExtentMutation::Replace {
            offset: 90,
            length: 30,
            kind: ExtentKind::Content {
                object: blob,
                object_offset: 50,
            },
            extend: false,
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let children = root_children(&store, receipt.root).await?;
    assert_eq!(
        leaf_extents(&store, children[0].page).await?[1].kind,
        ExtentKind::Content {
            object: blob,
            object_offset: 50,
        }
    );
    assert_eq!(
        leaf_extents(&store, children[1].page).await?[0].kind,
        ExtentKind::Content {
            object: blob,
            object_offset: 60,
        }
    );
    Ok(())
}

#[tokio::test]
async fn shrink_then_growth_does_not_resurrect_discarded_extents()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let leaves = [
        put(&store, &ExtentPage::Leaf(vec![allocated(0, 100)]), 8).await?,
        put(&store, &ExtentPage::Leaf(vec![allocated(100, 100)]), 8).await?,
        put(&store, &ExtentPage::Leaf(vec![allocated(200, 100)]), 8).await?,
    ];
    let root = put(
        &store,
        &ExtentPage::Internal(vec![
            ExtentChild {
                first_offset: 0,
                end_offset: 100,
                page: leaves[0],
            },
            ExtentChild {
                first_offset: 100,
                end_offset: 200,
                page: leaves[1],
            },
            ExtentChild {
                first_offset: 200,
                end_offset: 300,
                page: leaves[2],
            },
        ]),
        8,
    )
    .await?;
    let receipt = apply_extent_mutations(
        &store,
        (root, 300),
        &[
            ExtentMutation::Resize { logical_bytes: 150 },
            ExtentMutation::Resize { logical_bytes: 300 },
        ],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let children = root_children(&store, receipt.root).await?;
    assert_eq!(
        leaf_extents(&store, children[1].page).await?[1],
        hole(150, 50)
    );
    assert_eq!(
        leaf_extents(&store, children[2].page).await?,
        vec![hole(200, 100)]
    );
    Ok(())
}

#[tokio::test]
async fn rejected_nonextending_range_performs_no_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let failure = apply_extent_mutations(
        &store,
        (root, 300),
        &[ExtentMutation::Replace {
            offset: 299,
            length: 2,
            kind: ExtentKind::Hole,
            extend: false,
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("out-of-range mutation unexpectedly succeeded")?;
    assert!(matches!(failure.error, ExtentMutationError::OutsideFile));
    assert_eq!(failure.work.items_examined, 1);
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn invalid_tail_is_rejected_during_allocation_free_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let failure = apply_extent_mutations(
        &store,
        (root, 300),
        &[
            ExtentMutation::Replace {
                offset: 10,
                length: 5,
                kind: ExtentKind::AllocatedZero,
                extend: false,
            },
            ExtentMutation::Resize { logical_bytes: 250 },
            ExtentMutation::Replace {
                offset: 249,
                length: 2,
                kind: ExtentKind::Hole,
                extend: false,
            },
        ],
        3,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("invalid tail unexpectedly succeeded")?;
    assert!(matches!(failure.error, ExtentMutationError::OutsideFile));
    assert_eq!(failure.work.items_examined, 3);
    assert_eq!(failure.work.allocation_operations, 0);
    assert_eq!(failure.work.peak_allocation_bytes, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn admission_work_budget_stops_before_allocation_or_backend_access()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let budget = WorkBudget {
        items_examined: 1,
        ..WorkBudget::UNBOUNDED
    };
    let failure = apply_extent_mutations(
        &store,
        (root, 300),
        &[
            ExtentMutation::Resize { logical_bytes: 250 },
            ExtentMutation::Resize { logical_bytes: 200 },
        ],
        2,
        DecodeLimits::default(),
        budget,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("underbudgeted admission unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentMutationError::Work(WorkError::BudgetExceeded {
            counter: "items_examined",
            observed: 2,
            maximum: 1,
        })
    ));
    assert_eq!(failure.work.items_examined, 1);
    assert_eq!(failure.work.allocation_operations, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn radix_copy_budget_stops_before_unadmitted_copy_or_backend_access()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let failure = apply_extent_mutations(
        &store,
        (root, 300),
        &[
            ExtentMutation::Replace {
                offset: 10,
                length: 5,
                kind: ExtentKind::AllocatedZero,
                extend: false,
            },
            ExtentMutation::Replace {
                offset: 20,
                length: 5,
                kind: ExtentKind::AllocatedZero,
                extend: false,
            },
        ],
        2,
        DecodeLimits::default(),
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("zero-copy budget unexpectedly admitted radix copying")?;
    assert!(matches!(
        failure.error,
        ExtentMutationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            observed: 32,
            maximum: 0,
        })
    ));
    assert_eq!(failure.work.bytes_copied, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn page_encoding_and_hashing_are_separately_admitted_before_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let page = ExtentPage::Leaf(Vec::new());
    let encoded_length = u64::try_from(extent_page_encoded_length(&page, 1)?)?;
    let mut encode_limited = KernelAccounting::standalone(
        WorkBudget {
            bytes_encoded: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let encode_initial_work = encode_limited.spent();
    let encode_error = (put_extent_page_with_accounting(
        &store,
        &page,
        DecodeLimits::default(),
        &mut encode_limited,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("zero encoding budget unexpectedly succeeded")?;
    assert!(matches!(project_extent_error(encode_error),
        ExtentMutationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_encoded", observed, maximum: 0,
        }) if observed == encoded_length
    ));
    assert_eq!(encode_limited.spent(), encode_initial_work);

    let mut hash_limited = KernelAccounting::standalone(
        WorkBudget {
            bytes_hashed: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let hash_error = (put_extent_page_with_accounting(
        &store,
        &page,
        DecodeLimits::default(),
        &mut hash_limited,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("zero hash budget unexpectedly succeeded")?;
    assert!(matches!(project_extent_error(hash_error),
        ExtentMutationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_hashed", observed, maximum: 0,
        }) if observed == encoded_length + OBJECT_DIGEST_ENVELOPE_BYTES
    ));
    assert_eq!(hash_limited.spent().bytes_encoded, encoded_length);
    assert_eq!(hash_limited.spent().bytes_hashed, 0);
    assert_eq!(hash_limited.spent().backend_write_operations, 0);

    let mut byte_limited = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let byte_error = (put_extent_page_with_accounting(
        &store,
        &page,
        DecodeLimits {
            maximum_page_bytes: 1,
            ..DecodeLimits::default()
        },
        &mut byte_limited,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("one-byte page ceiling unexpectedly admitted an extent page")?;
    assert!(matches!(project_extent_error(byte_error),
        ExtentMutationError::Decode(CanonicalDecodeError::ObjectTooLarge {
            observed, maximum: 1,
        }) if observed == encoded_length
    ));
    assert_eq!(byte_limited.spent().backend_write_operations, 0);

    Ok(())
}

#[tokio::test]
async fn shape_valid_malformed_page_releases_decoded_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let mut malformed = encode_extent_page(
        &ExtentPage::Leaf(vec![hole(0, 1)]),
        8,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    malformed[23..31].fill(0);
    let malformed_id = ObjectId {
        kind: ObjectKind::ExtentPage,
        digest: object_digest(ObjectKind::ExtentPage, &malformed),
    };
    {
        let __object_args = (
            malformed_id,
            acyclic_stream::OwnedBytes::from(malformed),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::put(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?;
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut context = defensive_context(&store, &mut accounting, &cancellation)?;
    let initial_live = standalone_live(context.accounting);
    let error = (context.read_page(malformed_id))
        .await
        .err()
        .ok_or("shape-valid malformed extent page unexpectedly decoded")?;
    assert!(matches!(
        project_extent_error(error),
        ExtentMutationError::Decode(CanonicalDecodeError::Invariant(_))
    ));
    assert_eq!(standalone_live(context.accounting), initial_live);
    Ok(())
}

#[tokio::test]
async fn extent_pages_split_on_byte_bound_before_item_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &ExtentPage::Leaf(vec![hole(0, 100)]), 8).await?;
    let blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([6; 32]),
    };
    let limits = DecodeLimits {
        maximum_page_bytes: 111,
        maximum_page_items: 8,
        ..DecodeLimits::default()
    };
    let receipt = apply_extent_mutations(
        &store,
        (root, 100),
        &[
            ExtentMutation::Replace {
                offset: 0,
                length: 10,
                kind: ExtentKind::Content {
                    object: blob,
                    object_offset: 0,
                },
                extend: false,
            },
            ExtentMutation::Replace {
                offset: 20,
                length: 10,
                kind: ExtentKind::Content {
                    object: blob,
                    object_offset: 20,
                },
                extend: false,
            },
        ],
        2,
        limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.work.page_writes, 3);
    let root_bytes = {
        let __object_args = (
            receipt.root,
            u64::MAX,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?
    .value;
    assert_eq!(root_bytes.len(), 111);
    let ExtentPage::Internal(children) = decode_extent_page(&root_bytes, limits, &mut |_| {
        Ok::<_, crate::kernel::CanonicalDecodeError>(())
    })?
    else {
        return Err("byte-bounded extent root is not internal".into());
    };
    assert_eq!(children.len(), 2);
    for child in children {
        let bytes = {
            let __object_args = (
                child.page,
                u64::MAX,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }?
        .value;
        assert!(bytes.len() <= 111);
    }
    Ok(())
}

#[test]
fn overlapping_ordered_edits_normalize_content_offsets_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    let blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([8; 32]),
    };
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let plan = compile_patch_plan(
        100,
        &[
            ExtentMutation::Replace {
                offset: 10,
                length: 50,
                kind: ExtentKind::Content {
                    object: blob,
                    object_offset: 100,
                },
                extend: false,
            },
            ExtentMutation::Replace {
                offset: 20,
                length: 10,
                kind: ExtentKind::AllocatedZero,
                extend: false,
            },
        ],
        &mut accounting,
    )?;
    let patches = &plan.patches.value;
    assert_eq!(plan.final_size, 100);
    assert_eq!(patches.len(), 3);
    assert_eq!((patches[0].offset, patches[0].end), (10, 20));
    assert_eq!(
        patches[0].kind,
        ExtentKind::Content {
            object: blob,
            object_offset: 100,
        }
    );
    assert_eq!(patches[1].kind, ExtentKind::AllocatedZero);
    assert_eq!((patches[2].offset, patches[2].end), (30, 60));
    assert_eq!(
        patches[2].kind,
        ExtentKind::Content {
            object: blob,
            object_offset: 120,
        }
    );
    Ok(())
}

#[tokio::test]
async fn truncate_to_zero_reads_no_old_pages_and_emits_canonical_empty_leaf()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let receipt = apply_extent_mutations(
        &store,
        (root, 300),
        &[ExtentMutation::Resize { logical_bytes: 0 }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.work.page_reads, 0);
    assert_eq!(receipt.work.page_writes, 1);
    assert_eq!(leaf_extents(&store, receipt.root).await?, Vec::new());
    Ok(())
}

#[tokio::test]
async fn truncate_to_zero_stops_at_first_child_independent_of_root_width()
-> Result<(), Box<dyn std::error::Error>> {
    async fn root_with_width(
        store: &MemoryObjectStore,
        width: u32,
    ) -> Result<ObjectId, Box<dyn std::error::Error>> {
        let mut children = Vec::with_capacity(usize::try_from(width)?);
        for index in 0..width {
            let first = u64::from(index);
            let page = put(store, &ExtentPage::Leaf(vec![hole(first, 1)]), width).await?;
            children.push(ExtentChild {
                first_offset: first,
                end_offset: first + 1,
                page,
            });
        }
        put(store, &ExtentPage::Internal(children), width).await
    }

    let narrow_store = MemoryObjectStore::default();
    let narrow = root_with_width(&narrow_store, 2).await?;
    let narrow_receipt = apply_extent_mutations(
        &narrow_store,
        (narrow, 2),
        &[ExtentMutation::Resize { logical_bytes: 0 }],
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;

    let wide_store = MemoryObjectStore::default();
    let wide = root_with_width(&wide_store, 1_024).await?;
    let wide_receipt = apply_extent_mutations(
        &wide_store,
        (wide, 1_024),
        &[ExtentMutation::Resize { logical_bytes: 0 }],
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(wide_receipt.work.page_reads, 0);
    assert_eq!(
        wide_receipt.work.items_examined,
        narrow_receipt.work.items_examined
    );
    Ok(())
}

fn standalone_live(accounting: &KernelAccounting<'_>) -> u64 {
    match accounting {
        KernelAccounting::Standalone { allocations, .. } => allocations.live_bytes(),
        _ => unreachable!("standalone fixture"),
    }
}

fn defensive_context<'a, 'b>(
    store: &'a MemoryObjectStore,
    accounting: &'a mut KernelAccounting<'b>,
    cancellation: &'a CancellationToken,
) -> Result<Context<'a, 'b, MemoryObjectStore>, FsError> {
    let visited = VisitedObjectSet::new_with_accounting(8, accounting)?;
    let patches = Owned::allocate(0, accounting)?;
    Ok(Context {
        store,
        limits: DecodeLimits::default(),
        accounting,
        visited,
        patches,
        maximum_seen_height: 0,
        cancellation,
    })
}

#[path = "extent_mutation/histories.rs"]
mod histories;
