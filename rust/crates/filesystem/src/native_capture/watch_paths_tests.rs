use super::*;
use proptest::prelude::*;

fn path(value: &str) -> NamespacePath {
    let limits = crate::model::VolumeLimits::default();
    NamespacePath::from_portable(
        &crate::path::PortablePath::parse(value, limits).unwrap(),
        limits,
    )
    .unwrap()
}
fn meter() -> NativeMaterializationWork {
    NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap()
}
fn record() -> FileRecord {
    FileRecord {
        file_id: crate::FileId::from_bytes([1; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: crate::ObjectId {
            kind: crate::ObjectKind::Metadata,
            digest: crate::Digest::from_bytes([2; 32]),
        },
        payload: crate::kernel::FilePayload::Empty,
    }
}
fn ordinary(
    count: usize,
    work: &NativeMaterializationWork,
) -> Vec<(NamespacePath, (CurrentRecord, CaptureIntent))> {
    (0..count)
        .flat_map(|index| [format!("/d{index:04}"), format!("/d{index:04}/file")])
        .map(|name| {
            (
                clone_capture_path(&path(&name), work).unwrap(),
                (CurrentRecord::Lookup, CaptureIntent::Complete),
            )
        })
        .collect()
}
fn missing(count: usize) -> Vec<(NamespacePath, Option<FileRecord>)> {
    (0..count)
        .map(|index| (path(&format!("/d{index:04}")), None))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // The indexed implementation agrees with exhaustive proper-ancestor search
    // for every generated path, replacement set and mix of present/absent parents.
    #[test]
    fn indexed_ancestry_matches_exhaustive_search(
        parents in prop::collection::btree_map(prop::collection::vec(0u8..8, 1..6), any::<bool>(), 0..40),
        components in prop::collection::vec(0u8..8, 1..8),
    ) {
        let name = |parts: &[u8]| format!("/{}", parts.iter().map(|n| format!("d{n}")).collect::<Vec<_>>().join("/"));
        let parents: Vec<_> = parents.into_iter().map(|(parts, present)| (path(&name(&parts)), present.then(record))).collect();
        let path = path(&name(&components));
        let expected = parents.iter().any(|(parent, record)| record.is_some() && path != *parent && path.is_within(parent));
        prop_assert_eq!(below_replaced_parent(&path, &parents, &meter()).unwrap(), expected);
    }

    #[test]
    fn retirement_preserves_unrelated_entries_and_classifies_replaced_ancestors(
        replaced in prop::collection::btree_map(0usize..40, any::<bool>(), 0..30),
    ) {
        let work = meter();
        let mut entries = ordinary(40, &work);
        let missing: Vec<_> = replaced.iter().map(|(index, present)| (path(&format!("/d{index:04}")), present.then(record))).collect();
        retire_hydrated(&mut entries, &missing, &work).unwrap();
        let expected: Vec<_> = (0..40).flat_map(|index| {
            let parent = format!("/d{index:04}");
            let mut rows = Vec::new();
            if !replaced.contains_key(&index) { rows.push((parent.clone(), false)); }
            rows.push((format!("{parent}/file"), replaced.get(&index) == Some(&true)));
            rows
        }).collect();
        prop_assert_eq!(entries.len(), expected.len());
        for ((actual, (current, _)), (expected, absent)) in entries.iter().zip(expected) {
            prop_assert_eq!(actual, &path(&expected));
            prop_assert_eq!(matches!(current, CurrentRecord::Known(None)), absent);
        }
    }

    // N fresh sibling directories and their children require O(N log N) item
    // work, and each surviving entry moves at most once. Neither bound permits
    // the former repeated Vec::remove or exhaustive sibling scan.
    #[test]
    fn sibling_retirement_and_ancestry_scale_without_quadratic_work(count in 64usize..256) {
        let work = meter();
        let mut entries = ordinary(count, &work);
        let missing = missing(count);
        let before = work.receipt();
        retire_hydrated(&mut entries, &missing, &work).unwrap();
        let after = work.receipt();
        prop_assert_eq!(entries.len(), count);
        prop_assert!(entries.iter().all(|(path, _)| path.depth() == 2));
        let entry_bytes = std::mem::size_of::<(NamespacePath, (CurrentRecord, CaptureIntent))>() as u64;
        let copy_bound = count as u64 * entry_bytes;
        let item_bound = 8 * count as u64 * (count.ilog2() as u64 + 1);
        prop_assert!(after.bytes_copied - before.bytes_copied <= copy_bound);
        prop_assert!(after.items_examined - before.items_examined <= item_bound);

        let broken = meter();
        let mut entries = ordinary(count, &broken);
        let before = broken.receipt();
        // Negative control: execute the former production removal helper.
        for (path, _) in &missing {
            let (key, _) = capture_remove(&mut entries, path, &broken).unwrap().unwrap();
            capture_drop_path(key, &broken).unwrap();
        }
        prop_assert!(broken.receipt().bytes_copied - before.bytes_copied > copy_bound);
        // Negative control: the former ancestry loop examines every missing
        // sibling even when all records are absent.
        let before = broken.receipt();
        for _ in &entries {
            for _ in &missing {
                capture_charge(&broken, crate::materializer::NativeResourceCredit::Items.delta(1)).unwrap();
            }
        }
        prop_assert!(broken.receipt().items_examined - before.items_examined > item_bound);
    }
}
