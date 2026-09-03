use super::*;
use crate::async_storage::poll_ready;
use crate::foundation::Digest;
use crate::kernel::extent_page_id;
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectStore;

fn put(
    store: &MemoryObjectStore,
    page: &ExtentPage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = extent_page_id(page, maximum_items)?;
    store.put(
        id,
        Bytes::from(encode_extent_page(page, maximum_items)?),
        WorkBudget::UNBOUNDED,
    )?;
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

fn three_leaf_tree(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, [ObjectId; 3]), Box<dyn std::error::Error>> {
    let leaves = [
        put(store, &ExtentPage::Leaf(vec![hole(0, 100)]), 8)?,
        put(store, &ExtentPage::Leaf(vec![hole(100, 100)]), 8)?,
        put(store, &ExtentPage::Leaf(vec![hole(200, 100)]), 8)?,
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
    )?;
    Ok((root, leaves))
}

fn root_children(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<ExtentChild>, Box<dyn std::error::Error>> {
    let bytes = store.read(root, u64::MAX, WorkBudget::UNBOUNDED)?.value;
    let ExtentPage::Internal(children) = decode_extent_page(&bytes, DecodeLimits::default())?
    else {
        return Err("extent root is not internal".into());
    };
    Ok(children)
}

fn leaf_extents(
    store: &MemoryObjectStore,
    page: ObjectId,
) -> Result<Vec<Extent>, Box<dyn std::error::Error>> {
    let bytes = store.read(page, u64::MAX, WorkBudget::UNBOUNDED)?.value;
    let ExtentPage::Leaf(extents) = decode_extent_page(&bytes, DecodeLimits::default())? else {
        return Err("extent page is not a leaf".into());
    };
    Ok(extents)
}

#[test]
fn rewrites_only_intersecting_leaf_and_shared_root() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, leaves) = three_leaf_tree(&store)?;
    let receipt = apply_extent_mutations(
        &store,
        root,
        300,
        &[ExtentMutation::Replace {
            offset: 120,
            length: 10,
            kind: ExtentKind::AllocatedZero,
            extend: false,
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let children = root_children(&store, receipt.root)?;
    assert_eq!(children[0].page, leaves[0]);
    assert_ne!(children[1].page, leaves[1]);
    assert_eq!(children[2].page, leaves[2]);
    assert_eq!(
        leaf_extents(&store, children[1].page)?,
        vec![hole(100, 20), allocated(120, 10), hole(130, 70)]
    );
    Ok(())
}

#[test]
fn semantic_noop_preserves_root_without_writes() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store)?;
    let receipt = apply_extent_mutations(
        &store,
        root,
        300,
        &[ExtentMutation::Replace {
            offset: 120,
            length: 10,
            kind: ExtentKind::Hole,
            extend: false,
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(receipt.root, root);
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 0);
    assert_eq!(receipt.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn async_and_sync_extent_mutation_share_exact_results_and_work()
-> Result<(), Box<dyn std::error::Error>> {
    let sync_store = MemoryObjectStore::default();
    let async_store = MemoryObjectStore::default();
    let (sync_root, _) = three_leaf_tree(&sync_store)?;
    let (async_root, _) = three_leaf_tree(&async_store)?;
    assert_eq!(sync_root, async_root);
    let mutations = [ExtentMutation::Replace {
        offset: 125,
        length: 25,
        kind: ExtentKind::AllocatedZero,
        extend: false,
    }];
    let synchronous = apply_extent_mutations(
        &sync_store,
        sync_root,
        300,
        &mutations,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let asynchronous = poll_ready(apply_extent_mutations_async(
        &async_store,
        async_root,
        300,
        &mutations,
        ExtentMutationOptions {
            maximum_mutations: 8,
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed asynchronous extent mutation unexpectedly blocked")??;
    assert_eq!(asynchronous, synchronous);
    Ok(())
}

#[test]
fn pre_cancelled_async_extent_mutation_performs_zero_work() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let root = put(&store, &ExtentPage::Leaf(vec![hole(0, 100)]), 8)?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = poll_ready(apply_extent_mutations_async(
        &store,
        root,
        100,
        &[ExtentMutation::Resize { logical_bytes: 50 }],
        ExtentMutationOptions {
            maximum_mutations: 8,
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &cancellation,
    ))
    .ok_or("memory-backed asynchronous extent mutation unexpectedly blocked")?
    .err()
    .ok_or("cancelled extent mutation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentMutationError::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn decoded_extent_allocation_is_admitted_after_shape_read_before_decode()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(
        &store,
        &ExtentPage::Leaf(vec![hole(0, 25), allocated(25, 25), hole(50, 50)]),
        8,
    )?;
    let mutation = [ExtentMutation::Replace {
        offset: 25,
        length: 25,
        kind: ExtentKind::AllocatedZero,
        extend: false,
    }];
    let baseline = apply_extent_mutations(
        &store,
        root,
        100,
        &mutation,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let maximum = baseline.work.peak_allocation_bytes - 1;
    let failure = apply_extent_mutations(
        &store,
        root,
        100,
        &mutation,
        1,
        DecodeLimits::default(),
        WorkBudget {
            peak_allocation_bytes: maximum,
            ..WorkBudget::UNBOUNDED
        },
    )
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

#[test]
fn truncate_drops_later_subtree_without_reading_it() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, leaves) = three_leaf_tree(&store)?;
    let receipt = apply_extent_mutations(
        &store,
        root,
        300,
        &[ExtentMutation::Resize { logical_bytes: 150 }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let children = root_children(&store, receipt.root)?;
    assert_eq!(children.len(), 2);
    assert_eq!(children[0].page, leaves[0]);
    assert_eq!(children[1].end_offset, 150);
    Ok(())
}

#[test]
fn extension_reads_only_last_frontier_and_appends_hole() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, leaves) = three_leaf_tree(&store)?;
    let receipt = apply_extent_mutations(
        &store,
        root,
        300,
        &[ExtentMutation::Resize { logical_bytes: 500 }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let children = root_children(&store, receipt.root)?;
    assert_eq!(children[0].page, leaves[0]);
    assert_eq!(children[1].page, leaves[1]);
    assert_eq!(children[2].end_offset, 500);
    assert_eq!(
        leaf_extents(&store, children[2].page)?,
        vec![hole(200, 300)]
    );
    Ok(())
}

#[test]
fn content_offsets_advance_across_leaf_boundaries() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store)?;
    let blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([7; 32]),
    };
    let receipt = apply_extent_mutations(
        &store,
        root,
        300,
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
    )?;
    let children = root_children(&store, receipt.root)?;
    assert_eq!(
        leaf_extents(&store, children[0].page)?[1].kind,
        ExtentKind::Content {
            object: blob,
            object_offset: 50,
        }
    );
    assert_eq!(
        leaf_extents(&store, children[1].page)?[0].kind,
        ExtentKind::Content {
            object: blob,
            object_offset: 60,
        }
    );
    Ok(())
}

#[test]
fn shrink_then_growth_does_not_resurrect_discarded_extents()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let leaves = [
        put(&store, &ExtentPage::Leaf(vec![allocated(0, 100)]), 8)?,
        put(&store, &ExtentPage::Leaf(vec![allocated(100, 100)]), 8)?,
        put(&store, &ExtentPage::Leaf(vec![allocated(200, 100)]), 8)?,
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
    )?;
    let receipt = apply_extent_mutations(
        &store,
        root,
        300,
        &[
            ExtentMutation::Resize { logical_bytes: 150 },
            ExtentMutation::Resize { logical_bytes: 300 },
        ],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let children = root_children(&store, receipt.root)?;
    assert_eq!(leaf_extents(&store, children[1].page)?[1], hole(150, 50));
    assert_eq!(
        leaf_extents(&store, children[2].page)?,
        vec![hole(200, 100)]
    );
    Ok(())
}

#[test]
fn rejected_nonextending_range_performs_no_backend_work() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store)?;
    let failure = apply_extent_mutations(
        &store,
        root,
        300,
        &[ExtentMutation::Replace {
            offset: 299,
            length: 2,
            kind: ExtentKind::Hole,
            extend: false,
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("out-of-range mutation unexpectedly succeeded")?;
    assert!(matches!(failure.error, ExtentMutationError::OutsideFile));
    assert_eq!(failure.work.items_examined, 1);
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn invalid_tail_is_rejected_during_allocation_free_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store)?;
    let failure = apply_extent_mutations(
        &store,
        root,
        300,
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
    )
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

#[test]
fn admission_work_budget_stops_before_allocation_or_backend_access()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store)?;
    let budget = WorkBudget {
        items_examined: 1,
        ..WorkBudget::UNBOUNDED
    };
    let failure = apply_extent_mutations(
        &store,
        root,
        300,
        &[
            ExtentMutation::Resize { logical_bytes: 250 },
            ExtentMutation::Resize { logical_bytes: 200 },
        ],
        2,
        DecodeLimits::default(),
        budget,
    )
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

#[test]
fn radix_copy_budget_stops_before_unadmitted_copy_or_backend_access()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store)?;
    let failure = apply_extent_mutations(
        &store,
        root,
        300,
        &[ExtentMutation::Replace {
            offset: 10,
            length: 5,
            kind: ExtentKind::AllocatedZero,
            extend: false,
        }],
        1,
        DecodeLimits::default(),
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
    )
    .err()
    .ok_or("zero-copy budget unexpectedly admitted radix copying")?;
    assert!(matches!(
        failure.error,
        ExtentMutationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            observed: 16,
            maximum: 0,
        })
    ));
    assert_eq!(failure.work.bytes_copied, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn page_encoding_and_hashing_are_separately_admitted_before_storage()
-> Result<(), Box<dyn std::error::Error>> {
    fn context<'a>(
        store: &'a MemoryObjectStore,
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> Result<Context<'a, MemoryObjectStore>, AllocationError> {
        let mut allocations = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let visited = VisitedObjectSet::new(1, &mut allocations, &mut work, budget)?;
        Ok(Context {
            store,
            limits: DecodeLimits::default(),
            budget,
            work,
            allocations,
            visited,
            patches: Vec::new(),
            maximum_seen_height: 0,
            cancellation,
        })
    }

    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let page = ExtentPage::Leaf(Vec::new());
    let encoded_length = u64::try_from(extent_page_encoded_length(&page, 1)?)?;
    let mut encode_limited = context(
        &store,
        WorkBudget {
            bytes_encoded: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )?;
    let encode_initial_work = encode_limited.work;
    let encode_error = poll_ready(encode_limited.write_page(&page))
        .ok_or("synchronous adapter unexpectedly blocked")?
        .err()
        .ok_or("zero encoding budget unexpectedly succeeded")?;
    assert!(matches!(
        encode_error,
        ExtentMutationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_encoded",
            observed,
            maximum: 0,
        }) if observed == encoded_length
    ));
    assert_eq!(encode_limited.work, encode_initial_work);

    let mut hash_limited = context(
        &store,
        WorkBudget {
            bytes_hashed: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )?;
    let hash_error = poll_ready(hash_limited.write_page(&page))
        .ok_or("synchronous adapter unexpectedly blocked")?
        .err()
        .ok_or("zero hash budget unexpectedly succeeded")?;
    assert!(matches!(
        hash_error,
        ExtentMutationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_hashed",
            observed,
            maximum: 0,
        }) if observed == encoded_length + OBJECT_DIGEST_ENVELOPE_BYTES
    ));
    assert_eq!(hash_limited.work.bytes_encoded, encoded_length);
    assert_eq!(hash_limited.work.bytes_hashed, 0);
    assert_eq!(hash_limited.work.backend_write_operations, 0);

    let mut byte_limited = context(&store, WorkBudget::UNBOUNDED, &cancellation)?;
    byte_limited.limits.maximum_page_bytes = 1;
    let byte_error = poll_ready(byte_limited.write_page(&page))
        .ok_or("byte-limited page write blocked")?
        .err()
        .ok_or("one-byte page ceiling unexpectedly admitted an extent page")?;
    assert!(matches!(
        byte_error,
        ExtentMutationError::Decode(CanonicalDecodeError::ObjectTooLarge {
            observed,
            maximum: 1,
        }) if observed == encoded_length
    ));
    assert_eq!(byte_limited.work.backend_write_operations, 0);

    Ok(())
}

#[test]
fn shape_valid_malformed_page_releases_decoded_allocation() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let mut malformed = encode_extent_page(&ExtentPage::Leaf(vec![hole(0, 1)]), 8)?;
    malformed[23..31].fill(0);
    let malformed_id = ObjectId {
        kind: ObjectKind::ExtentPage,
        digest: object_digest(ObjectKind::ExtentPage, &malformed),
    };
    ObjectStore::put(
        &store,
        malformed_id,
        Bytes::from(malformed),
        WorkBudget::UNBOUNDED,
    )?;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let visited = VisitedObjectSet::new(1, &mut allocations, &mut work, WorkBudget::UNBOUNDED)?;
    let mut context = Context {
        store: &store,
        limits: DecodeLimits::default(),
        budget: WorkBudget::UNBOUNDED,
        work,
        allocations,
        visited,
        patches: Vec::new(),
        maximum_seen_height: 0,
        cancellation: &cancellation,
    };
    let initial_live = context.allocations.live_bytes();
    let error = poll_ready(context.read_page(malformed_id))
        .ok_or("malformed page read blocked")?
        .err()
        .ok_or("shape-valid malformed extent page unexpectedly decoded")?;
    assert!(matches!(
        error,
        ExtentMutationError::Decode(CanonicalDecodeError::Invariant(_))
    ));
    assert_eq!(context.allocations.live_bytes(), initial_live);
    Ok(())
}

#[test]
fn extent_pages_split_on_byte_bound_before_item_bound() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &ExtentPage::Leaf(vec![hole(0, 100)]), 8)?;
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
        root,
        100,
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
    )?;
    assert_eq!(receipt.work.page_writes, 3);
    let root_bytes = store
        .read(receipt.root, u64::MAX, WorkBudget::UNBOUNDED)?
        .value;
    assert_eq!(root_bytes.len(), 111);
    let ExtentPage::Internal(children) = decode_extent_page(&root_bytes, limits)? else {
        return Err("byte-bounded extent root is not internal".into());
    };
    assert_eq!(children.len(), 2);
    for child in children {
        let bytes = store
            .read(child.page, u64::MAX, WorkBudget::UNBOUNDED)?
            .value;
        assert!(bytes.len() <= 111);
    }
    Ok(())
}

#[test]
fn same_leaf_patch_replay_examines_linear_items() -> Result<(), Box<dyn std::error::Error>> {
    const EXTENTS: u64 = 1_024;
    let extents: Vec<_> = (0..EXTENTS)
        .map(|offset| Extent {
            offset,
            length: 1,
            kind: if offset.is_multiple_of(2) {
                ExtentKind::Hole
            } else {
                ExtentKind::AllocatedZero
            },
        })
        .collect();
    let patches: Vec<_> = (0..EXTENTS)
        .step_by(2)
        .map(|offset| Patch {
            offset,
            end: offset + 1,
            kind: ExtentKind::AllocatedZero,
        })
        .collect();
    let patch_count = u64::try_from(patches.len())?;
    let (mut result, examined) = apply_patches_linear(extents, &patches)?;
    assert!(examined <= EXTENTS + patch_count.saturating_mul(3));
    coalesce(&mut result)?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], allocated(0, EXTENTS));
    Ok(())
}

#[test]
fn overlapping_ordered_edits_normalize_content_offsets_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    let blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([8; 32]),
    };
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
        WorkBudget::UNBOUNDED,
    )?;
    let patches = plan.patches;
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

#[test]
fn truncate_to_zero_reads_no_old_pages_and_emits_canonical_empty_leaf()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store)?;
    let receipt = apply_extent_mutations(
        &store,
        root,
        300,
        &[ExtentMutation::Resize { logical_bytes: 0 }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(receipt.work.page_reads, 0);
    assert_eq!(receipt.work.page_writes, 1);
    assert_eq!(leaf_extents(&store, receipt.root)?, Vec::new());
    Ok(())
}

#[test]
fn truncate_to_zero_stops_at_first_child_independent_of_root_width()
-> Result<(), Box<dyn std::error::Error>> {
    fn root_with_width(
        store: &MemoryObjectStore,
        width: u32,
    ) -> Result<ObjectId, Box<dyn std::error::Error>> {
        let mut children = Vec::with_capacity(usize::try_from(width)?);
        for index in 0..width {
            let first = u64::from(index);
            let page = put(store, &ExtentPage::Leaf(vec![hole(first, 1)]), width)?;
            children.push(ExtentChild {
                first_offset: first,
                end_offset: first + 1,
                page,
            });
        }
        put(store, &ExtentPage::Internal(children), width)
    }

    let narrow_store = MemoryObjectStore::default();
    let narrow = root_with_width(&narrow_store, 2)?;
    let narrow_receipt = apply_extent_mutations(
        &narrow_store,
        narrow,
        2,
        &[ExtentMutation::Resize { logical_bytes: 0 }],
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;

    let wide_store = MemoryObjectStore::default();
    let wide = root_with_width(&wide_store, 1_024)?;
    let wide_receipt = apply_extent_mutations(
        &wide_store,
        wide,
        1_024,
        &[ExtentMutation::Resize { logical_bytes: 0 }],
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(wide_receipt.work.page_reads, 0);
    assert_eq!(
        wide_receipt.work.items_examined,
        narrow_receipt.work.items_examined
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ByteKind {
    Hole,
    AllocatedZero,
    Content { object: ObjectId, offset: u64 },
}

fn bytes_to_extents(bytes: &[ByteKind]) -> Result<Vec<Extent>, ExtentMutationError> {
    let mut extents = Vec::new();
    for (index, byte) in bytes.iter().enumerate() {
        let offset = u64::try_from(index).map_err(|_| ExtentMutationError::RangeOverflow)?;
        let kind = match byte {
            ByteKind::Hole => ExtentKind::Hole,
            ByteKind::AllocatedZero => ExtentKind::AllocatedZero,
            ByteKind::Content { object, offset } => ExtentKind::Content {
                object: *object,
                object_offset: *offset,
            },
        };
        extents.push(Extent {
            offset,
            length: 1,
            kind,
        });
    }
    coalesce(&mut extents)?;
    Ok(extents)
}

fn extents_to_bytes(extents: &[Extent]) -> Result<Vec<ByteKind>, ExtentMutationError> {
    let mut bytes = Vec::new();
    for extent in extents {
        for relative in 0..extent.length {
            bytes.push(match extent.kind {
                ExtentKind::Hole => ByteKind::Hole,
                ExtentKind::AllocatedZero => ByteKind::AllocatedZero,
                ExtentKind::Content {
                    object,
                    object_offset,
                } => ByteKind::Content {
                    object,
                    offset: object_offset
                        .checked_add(relative)
                        .ok_or(ExtentMutationError::RangeOverflow)?,
                },
            });
        }
    }
    Ok(bytes)
}

fn build_extent_tree(
    store: &MemoryObjectStore,
    extents: &[Extent],
    width: usize,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    if extents.is_empty() {
        return put(store, &ExtentPage::Leaf(Vec::new()), u32::try_from(width)?);
    }
    let mut level = Vec::new();
    for chunk in extents.chunks(width) {
        level.push(Summary {
            first: chunk[0].offset,
            end: extent_end(&chunk[chunk.len() - 1])?,
            page: put(
                store,
                &ExtentPage::Leaf(chunk.to_vec()),
                u32::try_from(width)?,
            )?,
        });
    }
    while level.len() > 1 {
        let mut parent = Vec::new();
        for chunk in level.chunks(width) {
            let children: Vec<_> = chunk
                .iter()
                .map(|summary| ExtentChild {
                    first_offset: summary.first,
                    end_offset: summary.end,
                    page: summary.page,
                })
                .collect();
            parent.push(Summary {
                first: chunk[0].first,
                end: chunk[chunk.len() - 1].end,
                page: put(
                    store,
                    &ExtentPage::Internal(children),
                    u32::try_from(width)?,
                )?,
            });
        }
        level = parent;
    }
    Ok(level[0].page)
}

fn flatten_extent_tree(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<Extent>, Box<dyn std::error::Error>> {
    let bytes = store.read(root, u64::MAX, WorkBudget::UNBOUNDED)?.value;
    Ok(match decode_extent_page(&bytes, DecodeLimits::default())? {
        ExtentPage::Leaf(extents) => extents,
        ExtentPage::Internal(children) => {
            let mut extents = Vec::new();
            for child in children {
                extents.extend(flatten_extent_tree(store, child.page)?);
            }
            extents
        }
    })
}

fn apply_reference(bytes: &mut Vec<ByteKind>, mutation: &ExtentMutation) {
    match mutation {
        ExtentMutation::Resize { logical_bytes } => {
            bytes.resize(
                usize::try_from(*logical_bytes).unwrap_or(usize::MAX),
                ByteKind::Hole,
            );
        }
        ExtentMutation::Replace {
            offset,
            length,
            kind,
            ..
        } => {
            let start = usize::try_from(*offset).unwrap_or(usize::MAX);
            let end = usize::try_from(offset.saturating_add(*length)).unwrap_or(usize::MAX);
            if start > bytes.len() {
                bytes.resize(start, ByteKind::Hole);
            }
            bytes.resize(bytes.len().max(end), ByteKind::Hole);
            for (relative, byte) in bytes[start..end].iter_mut().enumerate() {
                *byte = match kind {
                    ExtentKind::Hole => ByteKind::Hole,
                    ExtentKind::AllocatedZero => ByteKind::AllocatedZero,
                    ExtentKind::Content {
                        object,
                        object_offset,
                    } => ByteKind::Content {
                        object: *object,
                        offset: object_offset
                            .saturating_add(u64::try_from(relative).unwrap_or(u64::MAX)),
                    },
                };
            }
        }
    }
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

#[test]
fn generated_sparse_histories_match_independent_byte_model()
-> Result<(), Box<dyn std::error::Error>> {
    let blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([9; 32]),
    };
    for history in 0..256_u64 {
        let store = MemoryObjectStore::default();
        let mut seed = history.saturating_add(1);
        let initial_size = usize::try_from(next(&mut seed) % 33)?;
        let mut expected = Vec::with_capacity(initial_size);
        for index in 0..initial_size {
            expected.push(match next(&mut seed) % 3 {
                0 => ByteKind::Hole,
                1 => ByteKind::AllocatedZero,
                _ => ByteKind::Content {
                    object: blob,
                    offset: u64::try_from(index)?,
                },
            });
        }
        let root = build_extent_tree(&store, &bytes_to_extents(&expected)?, 4)?;
        let old_size = u64::try_from(expected.len())?;
        let mutation_count = usize::try_from(next(&mut seed) % 8 + 1)?;
        let mut mutations = Vec::with_capacity(mutation_count);
        for _ in 0..mutation_count {
            let mutation = if next(&mut seed).is_multiple_of(4) {
                ExtentMutation::Resize {
                    logical_bytes: next(&mut seed) % 65,
                }
            } else {
                let offset = next(&mut seed) % 65;
                let length = next(&mut seed) % 16 + 1;
                let kind = match next(&mut seed) % 3 {
                    0 => ExtentKind::Hole,
                    1 => ExtentKind::AllocatedZero,
                    _ => ExtentKind::Content {
                        object: blob,
                        object_offset: next(&mut seed) % 128,
                    },
                };
                ExtentMutation::Replace {
                    offset,
                    length,
                    kind,
                    extend: true,
                }
            };
            apply_reference(&mut expected, &mutation);
            mutations.push(mutation);
        }
        let limits = DecodeLimits {
            maximum_page_items: 4,
            ..DecodeLimits::default()
        };
        let receipt = apply_extent_mutations(
            &store,
            root,
            old_size,
            &mutations,
            8,
            limits,
            WorkBudget::UNBOUNDED,
        )?;
        assert_eq!(receipt.logical_bytes, u64::try_from(expected.len())?);
        assert_eq!(
            extents_to_bytes(&flatten_extent_tree(&store, receipt.root)?)?,
            expected,
            "history {history} diverged"
        );
    }
    Ok(())
}

fn defensive_context<'a>(
    store: &'a MemoryObjectStore,
    cancellation: &'a CancellationToken,
) -> Result<Context<'a, MemoryObjectStore>, AllocationError> {
    let budget = WorkBudget::UNBOUNDED;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let visited = VisitedObjectSet::new(8, &mut allocations, &mut work, budget)?;
    Ok(Context {
        store,
        limits: DecodeLimits::default(),
        budget,
        work,
        allocations,
        visited,
        patches: Vec::new(),
        maximum_seen_height: 0,
        cancellation,
    })
}

#[test]
#[allow(clippy::too_many_lines)]
fn defensive_extent_helpers_and_frames_are_total() -> Result<(), Box<dyn std::error::Error>> {
    let root = ObjectId {
        kind: ObjectKind::ExtentPage,
        digest: Digest::ZERO,
    };
    let mutation = ExtentMutation::Resize { logical_bytes: 0 };
    let invalid_limits = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    for (candidate, mutations, maximum, limits, expected) in [
        (
            ObjectId {
                kind: ObjectKind::Blob,
                digest: Digest::ZERO,
            },
            vec![mutation.clone()],
            1,
            DecodeLimits::default(),
            ExtentMutationError::WrongRootKind,
        ),
        (
            root,
            Vec::new(),
            1,
            DecodeLimits::default(),
            ExtentMutationError::Empty,
        ),
        (
            root,
            vec![mutation.clone()],
            0,
            DecodeLimits::default(),
            ExtentMutationError::TooManyMutations,
        ),
        (
            root,
            vec![mutation.clone()],
            1,
            invalid_limits,
            ExtentMutationError::InvalidLimits,
        ),
    ] {
        let failure = validate_request(candidate, &mutations, maximum, limits)
            .err()
            .ok_or("invalid extent request unexpectedly succeeded")?;
        assert_eq!(
            std::mem::discriminant(&failure.error),
            std::mem::discriminant(&expected)
        );
        assert_eq!(*failure.work, WorkCounters::default());
    }
    validate_request(
        root,
        std::slice::from_ref(&mutation),
        1,
        DecodeLimits::default(),
    )?;

    let no_op = compile_patch_plan(0, std::slice::from_ref(&mutation), WorkBudget::UNBOUNDED)?;
    assert!(no_op.patches.is_empty());
    assert_eq!(no_op.final_size, 0);
    assert_eq!(no_op.live_allocation_bytes, 0);

    let mut work = WorkCounters::default();
    let missing_coordinate = coordinate_index(&[1, 3], 2, &mut work, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("missing patch coordinate unexpectedly resolved")?;
    assert!(matches!(
        missing_coordinate.error,
        ExtentMutationError::PatchInvariant
    ));
    assert!(!patch_continues(
        &Patch {
            offset: 0,
            end: 1,
            kind: ExtentKind::Hole,
        },
        &Patch {
            offset: 2,
            end: 3,
            kind: ExtentKind::Hole,
        }
    )?);
    assert!(matches!(
        validate_kind(
            &ExtentKind::Content {
                object: root,
                object_offset: 0,
            },
            1
        ),
        Err(ExtentMutationError::WrongContentKind)
    ));
    assert!(matches!(
        validate_kind(
            &ExtentKind::Content {
                object: ObjectId {
                    kind: ObjectKind::Blob,
                    digest: Digest::ZERO,
                },
                object_offset: u64::MAX,
            },
            1
        ),
        Err(ExtentMutationError::RangeOverflow)
    ));
    assert!(matches!(
        extent_chunk_end(
            &[0_u8],
            0,
            DecodeLimits {
                maximum_page_items: 1,
                maximum_page_bytes: 16,
                ..DecodeLimits::default()
            },
            |_| 2
        ),
        Err(ExtentMutationError::PageItemTooLarge)
    ));
    assert!(matches!(
        extent_chunk_end(
            &[0_u8],
            0,
            DecodeLimits {
                maximum_page_items: 0,
                ..DecodeLimits::default()
            },
            |_| 1
        ),
        Err(ExtentMutationError::PageItemTooLarge)
    ));
    assert!(matches!(
        validate_leaf(&[], 0, 1),
        Err(ExtentMutationError::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_children(&[], 0, 1),
        Err(ExtentMutationError::ChildBoundsMismatch)
    ));

    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let mut context = defensive_context(&store, &cancellation)?;
    let complete = poll_ready(context.enter_node(NodeRequest {
        page: root,
        original_first: 4,
        original_end: 8,
        output_end: 4,
        height: 1,
    }))
    .ok_or("empty extent request unexpectedly suspended")??;
    assert!(matches!(complete, EnteredNode::Complete(values) if values.is_empty()));
    let height = poll_ready(context.enter_node(NodeRequest {
        page: root,
        original_first: 0,
        original_end: 1,
        output_end: 1,
        height: context.limits.maximum_page_height.saturating_add(1),
    }))
    .ok_or("height guard unexpectedly suspended")?
    .err()
    .ok_or("excessive height unexpectedly succeeded")?;
    assert!(matches!(height, ExtentMutationError::HeightExceeded));

    let original = ObjectId {
        kind: ObjectKind::ExtentPage,
        digest: Digest::from_bytes([1; 32]),
    };
    let empty = poll_ready(context.finish_frame(InternalFrame {
        original,
        children: Vec::new(),
        original_first: 0,
        original_end: 0,
        output_end: 0,
        height: 1,
        next_child: 0,
        rewritten: Vec::new(),
        logical_bytes: 0,
    }))
    .ok_or("empty frame unexpectedly suspended")??;
    assert!(empty.is_empty());
    let rewritten = vec![Summary {
        first: 0,
        end: 1,
        page: original,
    }];
    let single = poll_ready(context.finish_frame(InternalFrame {
        original,
        children: vec![ExtentChild {
            first_offset: 0,
            end_offset: 2,
            page: original,
        }],
        original_first: 0,
        original_end: 2,
        output_end: 1,
        height: 1,
        next_child: 1,
        rewritten: rewritten.clone(),
        logical_bytes: 0,
    }))
    .ok_or("single frame unexpectedly suspended")??;
    assert_eq!(single.len(), 1);
    assert_eq!(single[0].end, 1);

    for allocation in [
        AllocationError::Work(WorkError::Overflow),
        AllocationError::Overflow,
        AllocationError::ReleaseInvariant,
        AllocationError::InvalidCapacity,
        AllocationError::CapacityExceeded,
        AllocationError::AllocationFailed,
    ] {
        let mapped = ExtentMutationError::from(allocation);
        assert!(matches!(
            mapped,
            ExtentMutationError::Work(WorkError::Overflow) | ExtentMutationError::AllocationFailed
        ));
    }
    Ok(())
}

#[test]
fn patch_admission_and_coalescing_reject_every_boundary_before_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let empty = admit_patch_plan(
        8,
        &[ExtentMutation::Replace {
            offset: 0,
            length: 0,
            kind: ExtentKind::Hole,
            extend: false,
        }],
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("empty replacement unexpectedly admitted")?;
    assert!(matches!(empty.error, ExtentMutationError::EmptyRange));

    let overflow = admit_patch_plan(
        u64::MAX,
        &[ExtentMutation::Replace {
            offset: u64::MAX,
            length: 1,
            kind: ExtentKind::AllocatedZero,
            extend: true,
        }],
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("overflowing replacement unexpectedly admitted")?;
    assert!(matches!(overflow.error, ExtentMutationError::RangeOverflow));

    assert!(!equivalent_continuation(&hole(0, 1), &hole(2, 1))?);
    assert!(equivalent_continuation(&hole(0, 1), &hole(1, 1))?);
    assert!(equivalent_continuation(&allocated(0, 1), &allocated(1, 1))?);
    assert!(!equivalent_continuation(&hole(0, 1), &allocated(1, 1))?);
    Ok(())
}
