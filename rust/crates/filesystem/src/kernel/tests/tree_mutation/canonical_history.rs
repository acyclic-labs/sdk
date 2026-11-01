use super::*;

/// Both forks start at the same empty tree and install the same stable
/// identities. Only publication boundaries differ; metadata is not regenerated.
#[tokio::test]
async fn identical_fork_contents_have_identical_directory_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = crate::CancellationToken::new();
    let limits = DecodeLimits::default();
    let base = put(&store, &TreePage::Leaf(Vec::new()), 1_024).await?;
    let entries = (0..192_u16)
        .map(|value| wide_entry(value, FileKind::Regular))
        .collect::<Result<Vec<_>, _>>()?;
    let one_batch = apply_tree_mutations(
        &store,
        base,
        entries.iter().cloned().map(TreeMutation::Insert).collect(),
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?
    .value;
    let mut incremental = base;
    for batch in entries.chunks(17) {
        incremental = apply_tree_mutations(
            &store,
            incremental,
            batch.iter().cloned().map(TreeMutation::Insert).collect(),
            limits,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    }
    assert_eq!(flatten_tree(&store, one_batch).await?, entries);
    assert_eq!(flatten_tree(&store, incremental).await?, entries);
    assert_eq!(
        one_batch, incremental,
        "identical fork contents must have history-independent B+tree roots"
    );
    Ok(())
}

/// Pages far smaller than many entries: names of 1 to 64 extra bytes make
/// some entries share pages, some close the page they join, and some exceed
/// the page bound outright.
fn tiny_pages() -> (DecodeLimits, bool) {
    (
        DecodeLimits {
            maximum_page_items: 6,
            maximum_page_bytes: 72,
            ..DecodeLimits::default()
        },
        true,
    )
}

fn varied_entry(value: u16) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    let padding = "p".repeat(usize::from(value.wrapping_mul(37) % 64));
    entry(
        &format!("v{value:04}{padding}"),
        value.to_le_bytes()[0],
        FileKind::Regular,
    )
}

fn limits_with(maximum_page_items: u32) -> DecodeLimits {
    DecodeLimits {
        maximum_page_items,
        ..DecodeLimits::default()
    }
}

async fn apply(
    store: &MemoryObjectStore,
    root: ObjectId,
    mutations: Vec<TreeMutation>,
    limits: DecodeLimits,
) -> Result<OperationReceipt<ObjectId>, Box<dyn std::error::Error>> {
    Ok(apply_tree_mutations(
        store,
        root,
        mutations,
        limits,
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?)
}

/// Splits `items` into batches of 1..=`widest` drawn from `seed`.
fn batches<T: Clone>(items: &[T], widest: u64, seed: &mut u64) -> Vec<Vec<T>> {
    let mut result = Vec::new();
    let mut start = 0;
    while start < items.len() {
        let width = usize::try_from(next(seed) % widest + 1).unwrap_or(1);
        let end = items.len().min(start + width);
        result.push(items[start..end].to_vec());
        start = end;
    }
    result
}

fn shuffle<T>(items: &mut [T], seed: &mut u64) {
    for index in (1..items.len()).rev() {
        let bound = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        let other = usize::try_from(next(seed) % bound).unwrap_or(0);
        items.swap(index, other);
    }
}

/// Builds the kept part of `universe` entries through a seeded history: kept
/// and extra entries are inserted in shuffled batches, then the extras are
/// removed in shuffled batches. Returns the history's root and the root of
/// one batch inserting the kept entries into an empty tree.
async fn history_and_one_batch_roots(
    store: &MemoryObjectStore,
    universe: u16,
    seed: u64,
    (limits, varied): (DecodeLimits, bool),
) -> Result<(ObjectId, ObjectId, Vec<TreeEntry>), Box<dyn std::error::Error>> {
    let mut seed = seed.wrapping_mul(2).wrapping_add(1);
    let empty = put(store, &TreePage::Leaf(Vec::new()), 1_024).await?;
    let mut kept = Vec::new();
    let mut extra = Vec::new();
    for value in 0..universe {
        let entry = if varied {
            varied_entry(value)?
        } else {
            wide_entry(value, FileKind::Regular)?
        };
        if next(&mut seed).is_multiple_of(3) {
            extra.push(entry);
        } else {
            kept.push(entry);
        }
    }
    let mut inserts = kept.iter().chain(&extra).cloned().collect::<Vec<_>>();
    shuffle(&mut inserts, &mut seed);
    let mut root = empty;
    for batch in batches(&inserts, 48, &mut seed) {
        root = apply(
            store,
            root,
            batch.into_iter().map(TreeMutation::Insert).collect(),
            limits,
        )
        .await?
        .value;
    }
    let mut removals = extra;
    shuffle(&mut removals, &mut seed);
    for batch in batches(&removals, 24, &mut seed) {
        let mutations = batch
            .into_iter()
            .map(|entry| TreeMutation::Remove {
                name: entry.name,
                expected_file_id: Some(entry.file_id),
            })
            .collect();
        root = apply(store, root, mutations, limits).await?.value;
    }
    let one_batch = apply(
        store,
        empty,
        kept.iter().cloned().map(TreeMutation::Insert).collect(),
        limits,
    )
    .await?
    .value;
    Ok((root, one_batch, kept))
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(48))]

    /// Any batching and order of inserts and removals reaching one entry set
    /// gives the root of inserting that set at once, at every tree height.
    #[test]
    fn every_history_of_one_entry_set_has_one_root(
        seed in proptest::prelude::any::<u64>(),
        universe in 1_u16..600,
        shape in 0_u8..3,
    ) {
        let limits = match shape {
            0 => (DecodeLimits::default(), false),
            1 => (limits_with(6), false),
            _ => tiny_pages(),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        let (history, one_batch, flattened, kept) = runtime
            .block_on(async {
                let store = MemoryObjectStore::default();
                let (history, one_batch, kept) =
                    history_and_one_batch_roots(&store, universe, seed, limits).await?;
                let flattened = flatten_tree(&store, history).await?;
                Ok::<_, Box<dyn std::error::Error>>((history, one_batch, flattened, kept))
            })
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        proptest::prop_assert_eq!(flattened, kept);
        proptest::prop_assert_eq!(history, one_batch);
    }
}

/// Negative control: chunking each rewritten page on its own, closing its
/// last page instead of carrying it right, makes roots depend on history.
#[tokio::test]
async fn chunking_rewritten_pages_locally_makes_roots_history_dependent()
-> Result<(), Box<dyn std::error::Error>> {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            crate::kernel::persistent_btree::LOCAL_REWRITE.with(|local| local.set(false));
        }
    }
    for shape in [(limits_with(6), false), tiny_pages()] {
        let mut diverged = 0;
        for seed in 0..16 {
            let store = MemoryObjectStore::default();
            let (history, one_batch, _) =
                history_and_one_batch_roots(&store, 400, seed, shape).await?;
            assert_eq!(history, one_batch, "seed {seed}");
            let _reset = Reset;
            crate::kernel::persistent_btree::LOCAL_REWRITE.with(|local| local.set(true));
            let store = MemoryObjectStore::default();
            let (history, one_batch, kept) =
                history_and_one_batch_roots(&store, 400, seed, shape).await?;
            assert_eq!(flatten_tree(&store, history).await?, kept);
            if history != one_batch {
                diverged += 1;
            }
        }
        assert!(
            diverged > 0,
            "local chunking never diverged at {:?}",
            shape.0
        );
    }
    Ok(())
}

async fn height(
    store: &MemoryObjectStore,
    mut page: ObjectId,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut height = 1;
    loop {
        let bytes = store
            .read(
                page,
                u64::MAX,
                &mut crate::AuthorityWork::new(
                    WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                ),
            )
            .await?;
        match decode_tree_page(&bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })? {
            TreePage::Leaf(_) => return Ok(height),
            TreePage::Internal(children) => {
                page = children.first().ok_or("empty internal page")?.page;
                height += 1;
            }
        }
    }
}

fn ordinal_entry(ordinal: u32, suffix: &str) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    Ok(TreeEntry {
        name: name(&format!("e{ordinal:06}{suffix}"))?,
        file_id: FileId::from_bytes([u8::try_from(ordinal % 251)?; 16]),
        kind: FileKind::Regular,
    })
}

/// A point insert or removal reads and writes O(height) pages: growing the
/// directory sixteenfold adds levels, not per-operation work.
#[tokio::test]
async fn point_operations_cost_pages_proportional_to_height()
-> Result<(), Box<dyn std::error::Error>> {
    let mut heights = Vec::new();
    for entries in [1_024_u32, 16_384] {
        let store = MemoryObjectStore::default();
        let limits = limits_with(16);
        let empty = put(&store, &TreePage::Leaf(Vec::new()), 1_024).await?;
        let mut root = apply(
            &store,
            empty,
            (0..entries)
                .map(|ordinal| ordinal_entry(ordinal, "").map(TreeMutation::Insert))
                .collect::<Result<_, _>>()?,
            limits,
        )
        .await?
        .value;
        let height = height(&store, root).await?;
        let mut writes = 0_u64;
        let mut reads = 0_u64;
        for step in 0..64_u32 {
            let inserted = ordinal_entry(step * 7_919 % entries, "x")?;
            let insert = apply(
                &store,
                root,
                vec![TreeMutation::Insert(inserted.clone())],
                limits,
            )
            .await?;
            let remove = apply(
                &store,
                insert.value,
                vec![TreeMutation::Remove {
                    name: inserted.name,
                    expected_file_id: Some(inserted.file_id),
                }],
                limits,
            )
            .await?;
            // Removing the insert restores the original root.
            assert_eq!(remove.value, root);
            for receipt in [&insert, &remove] {
                writes = writes.max(receipt.work.page_writes);
                reads = reads.max(receipt.work.page_reads);
            }
            root = remove.value;
        }
        // Per level, a changed boundary splits or joins the touched page, and
        // a carry or a shifted item-bound cut reaches its right neighbours.
        assert!(
            writes <= 3 * height,
            "{entries}: {writes} writes at height {height}"
        );
        assert!(
            reads <= 3 * height,
            "{entries}: {reads} reads at height {height}"
        );
        heights.push(height);
    }
    assert!(heights[1] > heights[0], "sixteenfold entries add a level");
    Ok(())
}

/// Removing all but a few entries at either end, at once or one at a time,
/// leaves the root one batch of the survivors builds: no single-child pages
/// stay above them, even where the shrink never touched those pages.
#[tokio::test]
async fn shrinking_to_either_end_leaves_the_one_batch_root()
-> Result<(), Box<dyn std::error::Error>> {
    for maximum_page_items in [2, 4] {
        let limits = limits_with(maximum_page_items);
        for entries in [5_u32, 9, 33, 130] {
            let all = (0..entries)
                .map(|ordinal| ordinal_entry(ordinal, ""))
                .collect::<Result<Vec<_>, _>>()?;
            for (kept, from_left, one_at_a_time) in (1..=3_usize)
                .flat_map(|kept| [(kept, true), (kept, false)])
                .flat_map(|(kept, left)| [(kept, left, false), (kept, left, true)])
            {
                let store = MemoryObjectStore::default();
                let empty = put(&store, &TreePage::Leaf(Vec::new()), 1_024).await?;
                let insert = |entries: &[TreeEntry]| {
                    entries
                        .iter()
                        .cloned()
                        .map(TreeMutation::Insert)
                        .collect::<Vec<_>>()
                };
                let mut root = apply(&store, empty, insert(all.as_slice()), limits)
                    .await?
                    .value;
                let (removed, survivors) = if from_left {
                    all.split_at(all.len() - kept)
                } else {
                    let (survivors, removed) = all.split_at(kept);
                    (removed, survivors)
                };
                let removals = removed
                    .iter()
                    .cloned()
                    .map(|entry| TreeMutation::Remove {
                        name: entry.name,
                        expected_file_id: Some(entry.file_id),
                    })
                    .collect::<Vec<_>>();
                if one_at_a_time {
                    for removal in removals {
                        root = apply(&store, root, vec![removal], limits).await?.value;
                    }
                } else {
                    root = apply(&store, root, removals, limits).await?.value;
                }
                let one_batch = apply(&store, empty, insert(survivors), limits).await?.value;
                assert_eq!(flatten_tree(&store, root).await?, survivors);
                assert_eq!(
                    root, one_batch,
                    "{entries} entries at {maximum_page_items} items keeping {kept}, \
                     left {from_left}, one at a time {one_at_a_time}"
                );
            }
        }
    }
    Ok(())
}

/// At small item limits no item is a certain boundary, so a run of inserts
/// before every entry costs pages proportional to height. Were every page a
/// positional pair, each insert would shift every page after it.
#[tokio::test]
async fn left_edge_inserts_at_small_item_limits_cost_pages_proportional_to_height()
-> Result<(), Box<dyn std::error::Error>> {
    let entries = 4_096_u32;
    for maximum_page_items in [3, 4, 6] {
        let limits = limits_with(maximum_page_items);
        let store = MemoryObjectStore::default();
        let empty = put(&store, &TreePage::Leaf(Vec::new()), 1_024).await?;
        let mut root = apply(
            &store,
            empty,
            (0..entries)
                .map(|ordinal| ordinal_entry(ordinal, "").map(TreeMutation::Insert))
                .collect::<Result<_, _>>()?,
            limits,
        )
        .await?
        .value;
        let height = height(&store, root).await?;
        let mut writes = 0_u64;
        let mut reads = 0_u64;
        // Names below "e" sort before every ordinal entry; each step inserts
        // a new first entry.
        for step in (0..32_u8).rev() {
            let first = entry(&format!("a{step:03}"), step, FileKind::Regular)?;
            let receipt = apply(&store, root, vec![TreeMutation::Insert(first)], limits).await?;
            writes = writes.max(receipt.work.page_writes);
            reads = reads.max(receipt.work.page_reads);
            root = receipt.value;
        }
        assert!(
            writes <= 8 * height,
            "{maximum_page_items} items: {writes} writes at height {height}"
        );
        assert!(
            reads <= 8 * height,
            "{maximum_page_items} items: {reads} reads at height {height}"
        );
    }
    Ok(())
}
