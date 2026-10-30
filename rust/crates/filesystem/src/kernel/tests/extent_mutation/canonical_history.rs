use super::*;

/// The original range producer must refuse before touching output, reuse
/// unchanged pages with no write credit, and carry only a bounded right tail.
#[tokio::test]
async fn range_rewrites_admit_original_work_and_publish_only_completed_chunks()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let edit = ExtentEdit {
        route: 0,
        change: Change::Write(Patch {
            offset: 0,
            end: 8,
            kind: ExtentKind::AllocatedZero,
        }),
    };
    let mut output = Vec::with_capacity(8);
    let mut denied = KernelAccounting::standalone(
        WorkBudget {
            items_examined: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let refused = <ExtentEdit as persistent_btree::Mutation<ExtentFormat>>::rewrite_range(
        &[],
        &[hole(0, 8)],
        &[(0, &edit)],
        Some(&0),
        None,
        &mut output,
        &mut denied,
    );
    assert!(matches!(refused, Err(FsError::Work(_))));
    assert!(
        output.is_empty(),
        "work refusal must precede the first output change"
    );
    assert_eq!(denied.spent().items_examined, 0);

    let mut copied = Vec::new();
    for count in [64_u64, 256] {
        let store = MemoryObjectStore::default();
        let limits = shape(1);
        let empty = (put(&store, &ExtentPage::Leaf(Vec::new()), 8).await?, 0);
        let writes = (0..count)
            .map(|index| ExtentMutation::Replace {
                offset: index * 8,
                length: 8,
                kind: ExtentKind::Content {
                    object: blob(1),
                    object_offset: index * 24,
                },
                extend: true,
            })
            .collect::<Vec<_>>();
        let root = apply_batch(&store, empty, &writes, limits).await?;
        let unchanged = apply_extent_mutations(
            &store,
            root,
            &writes,
            u32::try_from(writes.len())?,
            limits,
            WorkBudget {
                page_writes: 0,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .await?;
        assert_eq!(unchanged.root, root.0);
        assert_eq!(unchanged.work.page_writes, 0);
        let replacements = writes
            .iter()
            .map(|write| match write {
                ExtentMutation::Replace { offset, length, .. } => ExtentMutation::Replace {
                    offset: *offset,
                    length: *length,
                    kind: ExtentKind::Content {
                        object: blob(2),
                        object_offset: offset * 3,
                    },
                    extend: false,
                },
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        let changed = apply_extent_mutations(
            &store,
            root,
            &replacements,
            u32::try_from(replacements.len())?,
            limits,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let rebuilt = apply_batch(
            &store,
            empty,
            &replacements
                .iter()
                .map(|write| match write {
                    ExtentMutation::Replace {
                        offset,
                        length,
                        kind,
                        ..
                    } => ExtentMutation::Replace {
                        offset: *offset,
                        length: *length,
                        kind: kind.clone(),
                        extend: true,
                    },
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
            limits,
        )
        .await?;
        assert_eq!((changed.root, changed.logical_bytes), rebuilt);
        copied.push(changed.work.bytes_copied);
        println!(
            "entries={count} copied={} reads={} writes={}",
            changed.work.bytes_copied, changed.work.page_reads, changed.work.page_writes
        );
    }
    assert!(
        copied[1] <= copied[0] * 8,
        "four times the input must not recopy its growing prefix quadratically: {copied:?}"
    );

    for shape_index in 0..3 {
        let store = MemoryObjectStore::default();
        let [single, batched, rebuilt] =
            history_roots(&store, 2_006_604_908_912_946_961, shape(shape_index), true).await?;
        assert_eq!(single, batched);
        assert_eq!(single, rebuilt);
    }
    Ok(())
}

fn next_value(seed: &mut u64) -> u64 {
    next(seed) >> 16
}

/// Page shapes from wide pages down to pages of a few items, and pages whose
/// byte bound makes every content extent close the page it joins.
fn shape(index: u8) -> DecodeLimits {
    match index {
        0 => DecodeLimits::default(),
        1 => DecodeLimits {
            maximum_page_items: 4,
            ..DecodeLimits::default()
        },
        _ => DecodeLimits {
            maximum_page_items: 6,
            maximum_page_bytes: 64,
            ..DecodeLimits::default()
        },
    }
}

fn blob(tag: u8) -> ObjectId {
    ObjectId {
        storage: None,
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([tag; 32]),
    }
}

/// Seeded writes and resizes over a file of up to a few thousand bytes.
fn generated_history(seed: &mut u64, count: usize) -> Vec<ExtentMutation> {
    (0..count)
        .map(|_| {
            if next_value(seed).is_multiple_of(8) {
                return ExtentMutation::Resize {
                    logical_bytes: next_value(seed) % 3_000,
                };
            }
            let kind = match next_value(seed) % 4 {
                0 => ExtentKind::Hole,
                1 => ExtentKind::AllocatedZero,
                tag => ExtentKind::Content {
                    object: blob(u8::try_from(tag).unwrap_or(0)),
                    object_offset: next_value(seed) % 4_000,
                },
            };
            ExtentMutation::Replace {
                offset: next_value(seed) % 2_800,
                length: next_value(seed) % 120 + 1,
                kind,
                extend: true,
            }
        })
        .collect()
}

async fn apply_batch(
    store: &MemoryObjectStore,
    root: (ObjectId, u64),
    mutations: &[ExtentMutation],
    limits: DecodeLimits,
) -> Result<(ObjectId, u64), Box<dyn std::error::Error>> {
    let receipt = apply_extent_mutations(
        store,
        root,
        mutations,
        u32::try_from(mutations.len())?,
        limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    Ok((receipt.root, receipt.logical_bytes))
}

/// The root of writing `root`'s extents into an empty file in one batch.
async fn bulk_root(
    store: &MemoryObjectStore,
    empty: (ObjectId, u64),
    root: (ObjectId, u64),
    limits: DecodeLimits,
) -> Result<(ObjectId, u64), Box<dyn std::error::Error>> {
    let writes = flatten_extent_tree(store, root.0)
        .await?
        .into_iter()
        .map(|extent| ExtentMutation::Replace {
            offset: extent.offset,
            length: extent.length,
            kind: extent.kind,
            extend: true,
        })
        .collect::<Vec<_>>();
    if writes.is_empty() {
        return Ok(empty);
    }
    apply_batch(store, empty, &writes, limits).await
}

/// Fails at the first step whose root differs from writing its own extents
/// at once, printing the mutations and both trees' extents.
async fn check_step(
    store: &MemoryObjectStore,
    empty: (ObjectId, u64),
    root: (ObjectId, u64),
    step: &str,
    mutations: &[ExtentMutation],
    limits: DecodeLimits,
) -> Result<(), Box<dyn std::error::Error>> {
    let bulk = bulk_root(store, empty, root, limits).await?;
    if bulk == root {
        return Ok(());
    }
    Err(format!(
        "{step} diverged from writing its extents at once
         mutations: {mutations:?}
         incremental: {:?}
         bulk: {:?}",
        flatten_extent_tree(store, root.0).await?,
        flatten_extent_tree(store, bulk.0).await?,
    )
    .into())
}

/// Three roots for one file: the history applied one mutation at a time, the
/// history applied in seeded batches, and the file's final extents written
/// into an empty file at once in shuffled order. Every intermediate root is
/// checked against writing its extents at once when `checked`, so a
/// divergence names the first mutation that caused it.
async fn history_roots(
    store: &MemoryObjectStore,
    seed: u64,
    limits: DecodeLimits,
    checked: bool,
) -> Result<[(ObjectId, u64); 3], Box<dyn std::error::Error>> {
    let mut seed = seed.wrapping_mul(2).wrapping_add(1);
    let empty = (put(store, &ExtentPage::Leaf(Vec::new()), 8).await?, 0);
    let history = generated_history(&mut seed, 40);
    let mut single = empty;
    for (index, mutation) in history.iter().enumerate() {
        let step = std::slice::from_ref(mutation);
        single = apply_batch(store, single, step, limits).await?;
        if !checked {
            continue;
        }
        check_step(
            store,
            empty,
            single,
            &format!("mutation {index}"),
            step,
            limits,
        )
        .await?;
    }
    let mut batched = empty;
    let mut start = 0;
    while start < history.len() {
        let width = usize::try_from(next_value(&mut seed) % 6 + 1)?;
        let end = history.len().min(start + width);
        let batch = &history[start..end];
        batched = apply_batch(store, batched, batch, limits).await?;
        let step = format!("batch of mutations {start}..{end}");
        if checked {
            check_step(store, empty, batched, &step, batch, limits).await?;
        }
        start = end;
    }
    let mut writes = flatten_extent_tree(store, single.0)
        .await?
        .into_iter()
        .map(|extent| ExtentMutation::Replace {
            offset: extent.offset,
            length: extent.length,
            kind: extent.kind,
            extend: true,
        })
        .collect::<Vec<_>>();
    for index in (1..writes.len()).rev() {
        let other = usize::try_from(next_value(&mut seed) % (u64::try_from(index)? + 1))?;
        writes.swap(index, other);
    }
    let rebuilt = if writes.is_empty() {
        empty
    } else {
        apply_batch(store, empty, &writes, limits).await?
    };
    Ok([single, batched, rebuilt])
}

/// Cases per property run: `PROPTEST_CASES` when set, else 48.
fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(48)
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(cases()))]

    /// Every history reaching one file gives the root of writing its extents
    /// at once, at every page shape.
    #[test]
    fn every_history_of_one_extent_map_has_one_root(
        seed in proptest::prelude::any::<u64>(),
        shape_index in 0_u8..3,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        let [single, batched, rebuilt] = runtime
            .block_on(async {
                let store = MemoryObjectStore::default();
                history_roots(&store, seed, shape(shape_index), true).await
            })
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        proptest::prop_assert_eq!(single, batched);
        proptest::prop_assert_eq!(single, rebuilt);
    }
}

/// A history the property test once found at four-item pages: a shrink and
/// regrowth left the file one leaf, which the rightmost path wrapped in
/// single-child pages instead of making it the root.
#[tokio::test]
async fn pinned_four_item_page_history_has_one_root() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let [single, batched, rebuilt] =
        history_roots(&store, 2_006_604_908_912_946_961, shape(1), true).await?;
    assert_eq!(single, batched);
    assert_eq!(single, rebuilt);
    Ok(())
}

/// Shrinking a file to one page makes that page the root, never a chain of
/// single-child pages above it, whichever leaf the surviving extents meet
/// and whether the file then regrows.
#[tokio::test]
async fn shrinking_to_one_page_leaves_no_single_child_root()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = shape(1);
    let store = MemoryObjectStore::default();
    let empty = (put(&store, &ExtentPage::Leaf(Vec::new()), 8).await?, 0);
    let distinct = |index: u64| ExtentKind::Content {
        object: blob(9),
        object_offset: index * 30,
    };
    let file = file_of(&store, 64, distinct, limits).await?;
    for size in (5..640).step_by(15) {
        for regrow in [false, true] {
            let mut mutations = vec![ExtentMutation::Resize {
                logical_bytes: size,
            }];
            if regrow {
                mutations.push(ExtentMutation::Replace {
                    offset: 900,
                    length: 50,
                    kind: ExtentKind::AllocatedZero,
                    extend: true,
                });
            }
            let root = apply_batch(&store, file, &mutations, limits).await?;
            check_step(
                &store,
                empty,
                root,
                &format!("size {size}"),
                &mutations,
                limits,
            )
            .await?;
        }
    }
    Ok(())
}

/// Negative control: chunking each rewritten page on its own, closing its
/// last page instead of carrying it right, makes extent roots depend on
/// history.
#[tokio::test]
async fn chunking_rewritten_extent_pages_locally_makes_roots_history_dependent()
-> Result<(), Box<dyn std::error::Error>> {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            crate::kernel::persistent_btree::LOCAL_REWRITE.with(|local| local.set(false));
        }
    }
    for shape_index in [1, 2] {
        let limits = shape(shape_index);
        let mut diverged = 0;
        for seed in 0..16 {
            let store = MemoryObjectStore::default();
            let [single, batched, rebuilt] = history_roots(&store, seed, limits, true).await?;
            assert_eq!(single, batched, "seed {seed}");
            assert_eq!(single, rebuilt, "seed {seed}");
            let _reset = Reset;
            crate::kernel::persistent_btree::LOCAL_REWRITE.with(|local| local.set(true));
            let store = MemoryObjectStore::default();
            // Unchecked: the control counts divergence instead of failing.
            let [single, batched, rebuilt] = history_roots(&store, seed, limits, false).await?;
            if single != batched || single != rebuilt {
                diverged += 1;
            }
        }
        assert!(
            diverged > 0,
            "local chunking never diverged at shape {shape_index}"
        );
    }
    Ok(())
}

async fn decode(
    store: &MemoryObjectStore,
    page: ObjectId,
) -> Result<ExtentPage, Box<dyn std::error::Error>> {
    let bytes = store
        .read(
            page,
            u64::MAX,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
    Ok(decode_extent_page(
        &bytes,
        DecodeLimits::default(),
        &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
    )?)
}

/// Pages a path copy of `[offset, end)` must rewrite: every page whose range
/// meets it, and the root. This is what rewriting only intersecting pages,
/// without joining across them, read and wrote.
async fn intersecting_pages(
    store: &MemoryObjectStore,
    root: (ObjectId, u64),
    offset: u64,
    end: u64,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let mut pages = 0;
    let mut height = 0;
    let mut level = vec![(root.0, 0, root.1)];
    while !level.is_empty() {
        height += 1;
        let mut below = Vec::new();
        for (page, first, last) in level {
            pages += 1;
            if let ExtentPage::Internal(children) = decode(store, page).await? {
                for (index, child) in children.iter().enumerate() {
                    let child_end = children.get(index + 1).map_or(last, |next| next.first);
                    if child.first < end && child_end > offset && child.first >= first {
                        below.push((child.page, child.first, child_end));
                    }
                }
            }
        }
        level = below;
    }
    Ok((pages, height))
}

/// Leaves `[first, end)` with a sibling leaf on each side under one parent.
async fn inner_sibling_leaves(
    store: &MemoryObjectStore,
    root: (ObjectId, u64),
) -> Result<Vec<(u64, u64)>, Box<dyn std::error::Error>> {
    let mut leaves = Vec::new();
    let mut level = vec![(root.0, root.1)];
    while let Some((page, last)) = level.pop() {
        let ExtentPage::Internal(children) = decode(store, page).await? else {
            continue;
        };
        let ends = (0..children.len())
            .map(|index| children.get(index + 1).map_or(last, |next| next.first))
            .collect::<Vec<_>>();
        if matches!(decode(store, children[0].page).await?, ExtentPage::Leaf(_)) {
            for index in 1..children.len().saturating_sub(1) {
                leaves.push((children[index].first, ends[index]));
            }
        } else {
            level.extend(
                children
                    .iter()
                    .zip(ends)
                    .map(|(child, end)| (child.page, end)),
            );
        }
    }
    leaves.sort_unstable();
    Ok(leaves)
}

/// Page reads and writes of content writes of `ranges` at `root`.
async fn write_cost(
    store: &MemoryObjectStore,
    root: (ObjectId, u64),
    ranges: &[(u64, u64)],
    limits: DecodeLimits,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let writes = ranges
        .iter()
        .map(|&(offset, end)| ExtentMutation::Replace {
            offset,
            length: end - offset,
            // Offsets that never continue each other, so adjacent probes
            // stay separate extents.
            kind: ExtentKind::Content {
                object: blob(9),
                object_offset: offset * 3,
            },
            extend: false,
        })
        .collect::<Vec<_>>();
    let receipt = apply_extent_mutations(
        store,
        root,
        &writes,
        u32::try_from(writes.len())?,
        limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    Ok((receipt.work.page_reads, receipt.work.page_writes))
}

/// A file of `extents` ten-byte extents, built in one batch from empty.
async fn file_of(
    store: &MemoryObjectStore,
    extents: u64,
    kind: impl Fn(u64) -> ExtentKind,
    limits: DecodeLimits,
) -> Result<(ObjectId, u64), Box<dyn std::error::Error>> {
    let empty = (put(store, &ExtentPage::Leaf(Vec::new()), 8).await?, 0);
    let build = (0..extents)
        .map(|index| ExtentMutation::Replace {
            offset: index * 10,
            length: 10,
            kind: kind(index),
            extend: true,
        })
        .collect::<Vec<_>>();
    apply_batch(store, empty, &build, limits).await
}

/// The tree height, the mean pages a path copy of each written range
/// touches, the mean page reads and writes, and the most reads and writes,
/// over 256 seeded writes.
async fn random_write_cost(
    extents: u64,
    limits: DecodeLimits,
) -> Result<(u64, [f64; 3], [u64; 2]), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    // Alternating kinds never join, so the file holds `extents` extents.
    let alternating = |index: u64| {
        if index.is_multiple_of(2) {
            ExtentKind::Hole
        } else {
            ExtentKind::AllocatedZero
        }
    };
    let mut root = file_of(&store, extents, alternating, limits).await?;
    let mut seed = extents;
    let (mut reads, mut writes, mut path, mut height) = (0_u64, 0_u64, 0_u64, 0_u64);
    let (mut most_reads, mut most_writes) = (0_u64, 0_u64);
    let operations = 256_u64;
    for _ in 0..operations {
        let offset = next_value(&mut seed) % (extents * 10 - 40);
        let length = next_value(&mut seed) % 30 + 1;
        let (pages, levels) = intersecting_pages(&store, root, offset, offset + length).await?;
        path += pages;
        height = height.max(levels);
        let receipt = apply_extent_mutations(
            &store,
            root,
            &[ExtentMutation::Replace {
                offset,
                length,
                kind: ExtentKind::Content {
                    object: blob(9),
                    object_offset: offset,
                },
                extend: false,
            }],
            1,
            limits,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        reads += receipt.work.page_reads;
        writes += receipt.work.page_writes;
        most_reads = most_reads.max(receipt.work.page_reads);
        most_writes = most_writes.max(receipt.work.page_writes);
        root = (receipt.root, receipt.logical_bytes);
    }
    let mean = |total: u64| total as f64 / operations as f64;
    Ok((
        height,
        [mean(path), mean(reads), mean(writes)],
        [most_reads, most_writes],
    ))
}

/// An extent write reads and writes O(height) pages: growing the file
/// sixteenfold adds levels, not per-operation work. Prints the cost beside
/// the pages a path copy of the written range touches, for small pages that
/// make the tree tall and for the default page shape.
#[tokio::test]
async fn extent_writes_cost_pages_proportional_to_height() -> Result<(), Box<dyn std::error::Error>>
{
    let small = DecodeLimits {
        maximum_page_items: 16,
        ..DecodeLimits::default()
    };
    let mut heights = Vec::new();
    for (shape, limits) in [("small", small), ("default", DecodeLimits::default())] {
        for extents in [1_024_u64, 16_384] {
            let (height, [path, reads, writes], [most_reads, most_writes]) =
                random_write_cost(extents, limits).await?;
            println!(
                "EXTENT_COST shape={shape} extents={extents} height={height} \
                 path_copy_pages_mean={path:.2} reads_mean={reads:.2} writes_mean={writes:.2} \
                 reads_max={most_reads} writes_max={most_writes}"
            );
            // Per level, a changed boundary splits or joins the touched page,
            // and a carry or a join reaches its right neighbours.
            assert!(
                most_writes <= 3 * height,
                "{shape} {extents}: {most_writes} writes"
            );
            assert!(
                most_reads <= 3 * height,
                "{shape} {extents}: {most_reads} reads"
            );
            if shape == "small" {
                heights.push(height);
            }
        }
    }
    assert!(heights[1] > heights[0], "sixteenfold extents add a level");
    Ok(())
}

/// A write that starts or ends exactly on a leaf boundary reads the
/// neighbour leaf on that side, so content that continues across the
/// boundary joins into one extent. That neighbour is the whole extra cost:
/// at most one leaf read per aligned side over the pages a path copy of the
/// written range touches, and no extra write, since a neighbour that does
/// not change keeps its page. Every extent here is distinct content of one
/// size, so a probe that replaces whole extents moves no page boundary and
/// isolates the alignment cost from boundary shifts.
#[tokio::test]
async fn boundary_aligned_writes_read_at_most_one_neighbour_leaf_per_side()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = DecodeLimits {
        maximum_page_items: 16,
        ..DecodeLimits::default()
    };
    for extents in [1_024_u64, 16_384] {
        let store = MemoryObjectStore::default();
        let distinct = |index: u64| ExtentKind::Content {
            object: blob(1),
            object_offset: index * 100,
        };
        let root = file_of(&store, extents, distinct, limits).await?;
        let leaves = inner_sibling_leaves(&store, root).await?;
        let step = (leaves.len() / 32).max(1);
        let (mut probes, mut extra_reads, mut extra_writes) = (0_u64, 0_u64, 0_u64);
        for &(first, end) in leaves.iter().step_by(step) {
            // Starts on the leaf's first boundary, ends on its last, and both.
            let cases: [(&[(u64, u64)], u64); 3] = [
                (&[(first, first + 10)], 1),
                (&[(end - 10, end)], 1),
                (&[(first, first + 10), (end - 10, end)], 2),
            ];
            for (ranges, sides) in cases {
                let (path, _) = intersecting_pages(&store, root, first, end).await?;
                let (reads, writes) = write_cost(&store, root, ranges, limits).await?;
                assert!(
                    reads <= path + sides && writes <= path,
                    "{extents}: {ranges:?} read {reads} and wrote {writes} pages; \
                     the path copy touches {path}, aligned sides {sides}"
                );
                extra_reads = extra_reads.max(reads.saturating_sub(path));
                extra_writes = extra_writes.max(writes.saturating_sub(path));
                probes += 1;
            }
        }
        assert!(probes > 0, "{extents}: no inner leaf probed");
        println!(
            "EXTENT_BOUNDARY_COST extents={extents} probes={probes} \
             extra_reads_max={extra_reads} extra_writes_max={extra_writes}"
        );
    }
    Ok(())
}
