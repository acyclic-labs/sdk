use super::*;
use crate::kernel::{FileMetadata, TreePage};
use crate::memory::MemoryObjectStore;
use crate::test_support::{put_file_metadata, put_tree_page};
use crate::{Lifecycle, WorkBudget};

fn original_frame(
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(KernelAccounting<'_>, u64), FsError> {
    let mut work = KernelAccounting::standalone(budget, cancellation);
    drop(super::super::super::allocation::retained::RetainedAllocation::claim(0, 0, &mut work)?);
    let baseline = work
        .standalone_parts()
        .ok_or(FsError::InvalidDiff)?
        .0
        .live_bytes();
    Ok((work, baseline))
}

fn identity(ordinal: u128) -> FileId {
    // Include every possible branch coordinate, then unrelated exact keys.
    let key = match ordinal {
        0 => 0,
        1..=128 => 1 << (ordinal - 1),
        _ => u128::MAX - ordinal,
    };
    FileId::from_bytes(key.to_be_bytes())
}

fn directory(ordinal: u128, metadata: ObjectId, entries: ObjectId) -> FileRecord {
    FileRecord {
        file_id: identity(ordinal),
        kind: FileKind::Directory,
        link_count: 1,
        metadata,
        bindings: None,
        payload: FilePayload::Directory { entries },
    }
}

#[tokio::test]
async fn active_ancestry_bounds_membership_and_retires_refused_or_cancelled_frames()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let entries = put_tree_page(&store, &TreePage::Leaf(Vec::new()), 8).await?;
    let metadata = put_file_metadata(&store, FileMetadata::default()).await?;
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.limits.maximum_path_depth = 10_001;
    // Three traversals plus full-key comparisons are bounded by the key width,
    // independently of stack depth. Frame growth is measured separately below.
    let membership_bound = 4 * (u64::from(u128::BITS) + 16 + 1);
    for depth in [1_u128, 1_000, 10_000] {
        let cancel = CancellationToken::new();
        let (mut work, baseline) = original_frame(WorkBudget::UNBOUNDED, &cancel)?;
        let mut walk = Descendants::new(directory(0, metadata, entries), config, &mut work)?;
        for ordinal in 1..depth {
            let before = work.spent();
            walk.push(directory(ordinal, metadata, entries), config, &mut work)?;
            assert!(work.spent().items_examined - before.items_examined <= membership_bound);
        }
        let before = work.spent();
        let live = work
            .standalone_parts()
            .ok_or(FsError::InvalidDiff)?
            .0
            .live_bytes();
        assert!(matches!(
            walk.push(directory(0, metadata, entries), config, &mut work),
            Err(FsError::InvalidDiff)
        ));
        assert!(work.spent().items_examined - before.items_examined <= membership_bound);
        assert_eq!(
            work.spent().allocation_operations,
            before.allocation_operations
        );
        assert_eq!(work.spent().backend_read_operations, 0);
        assert_eq!(
            work.standalone_parts()
                .ok_or(FsError::InvalidDiff)?
                .0
                .live_bytes(),
            live
        );
        assert_eq!(walk.frames.value.len() as u128, depth);

        // The removed frame scan performs depth comparisons for this absent key.
        // Count that same scan without constructing or emitting any path bytes.
        let absent = identity(depth);
        let mut old_comparisons = 0;
        let old_found = walk.frames.value.iter().any(|frame| {
            old_comparisons += 1;
            frame.file_id == absent
        });
        assert!(!old_found);
        assert_eq!(old_comparisons, depth);
        if depth > u128::from(membership_bound) {
            assert!(old_comparisons > u128::from(membership_bound));
        }

        // A sibling may reuse a departed identity; an active ancestor may not.
        let before = work.spent();
        walk.pop(&mut work)?;
        assert_eq!(work.spent(), before);
        walk.push(directory(depth - 1, metadata, entries), config, &mut work)?;
        let before = work.spent();
        cancel.cancel();
        walk.retire(&mut work)?;
        assert_eq!(work.spent(), before, "cleanup admits no fresh work");
        assert_eq!(
            work.standalone_parts()
                .ok_or(FsError::InvalidDiff)?
                .0
                .live_bytes(),
            baseline,
            "the original ledger header stays funded through caller retirement"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancel = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut work = KernelAccounting::native(&original, &cancel);
        let mut walk = Descendants::new(directory(0, metadata, entries), config, &mut work)?;
        for ordinal in 1..=u128::from(u128::BITS) {
            walk.push(directory(ordinal, metadata, entries), config, &mut work)?;
        }
        assert!(original.live_allocation_bytes() > baseline);
        let before = work.spent();
        cancel.cancel();
        walk.retire(&mut work)?;
        assert_eq!(work.spent(), before);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }

    // Refuse every original allocation during an enter that also grows frames.
    let cancel = CancellationToken::new();
    let (mut probe, _) = original_frame(WorkBudget::UNBOUNDED, &cancel)?;
    let mut walk = Descendants::new(directory(0, metadata, entries), config, &mut probe)?;
    for ordinal in 1..4 {
        walk.push(directory(ordinal, metadata, entries), config, &mut probe)?;
    }
    let baseline = probe.spent().allocation_operations;
    walk.push(directory(4, metadata, entries), config, &mut probe)?;
    let admitted = probe.spent().allocation_operations - baseline;
    walk.retire(&mut probe)?;
    assert!(admitted > 0);
    for allowance in 0..admitted {
        let (mut work, retained_baseline) = original_frame(
            WorkBudget {
                allocation_operations: baseline + allowance,
                ..WorkBudget::UNBOUNDED
            },
            &cancel,
        )?;
        let mut walk = Descendants::new(directory(0, metadata, entries), config, &mut work)?;
        for ordinal in 1..4 {
            walk.push(directory(ordinal, metadata, entries), config, &mut work)?;
        }
        assert!(matches!(
            walk.push(directory(4, metadata, entries), config, &mut work),
            Err(FsError::Work(crate::WorkError::BudgetExceeded {
                counter: "allocation_operations",
                ..
            }))
        ));
        assert_eq!(walk.frames.value.len(), 4);
        assert_eq!(walk.active.get(identity(4).into_bytes(), &mut work)?, None);
        for ordinal in 0..4 {
            assert_eq!(
                walk.active.get(identity(ordinal).into_bytes(), &mut work)?,
                Some(&())
            );
        }
        walk.retire(&mut work)?;
        assert_eq!(
            work.standalone_parts()
                .ok_or(FsError::InvalidDiff)?
                .0
                .live_bytes(),
            retained_baseline
        );
    }
    Ok(())
}
