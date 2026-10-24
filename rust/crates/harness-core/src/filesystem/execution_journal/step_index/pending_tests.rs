//! A recovered tail indexes unrelated steps without scanning them per query.
use super::*;

#[test]
fn recovered_tail_step_lookup_is_bounded_through_one_thousand_unrelated_steps() -> Result<()> {
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let scope = crate::filesystem::PreparationScope::new(Some(&io))?;
        let mut pending = Pending::default();
        for step in 0..1_000 {
            pending.push(
                u64::from(step) + 1,
                [1; 32],
                [2; 32],
                Some(step),
                None,
                scope.io(),
            )?;
            if ![0, 9, 99, 999].contains(&step) {
                continue;
            }
            let mut selected = Funded::new(Some(&io))?;
            let before = io.receipt();
            pending.collect(Some(0), 1, &mut selected, Some(&io))?;
            let examined = io.receipt().items_examined - before.items_examined;
            assert_eq!(selected.values.len(), 1);
            assert_eq!(selected.values[0].sequence, 1);
            // At most 256 radix branches, one root, one leaf, 32 key
            // bytes and the selected entry; no journal-length term.
            assert!(
                examined <= 291,
                "tail lookup scanned unrelated steps: {examined}"
            );
            let before = io.receipt();
            let mut found = 0;
            for entry in &pending.entries {
                crate::model::charge_items(Some(&io), 1)?;
                found += usize::from(entry.entry.step == Some(0));
            }
            assert_eq!(found, selected.values.len());
            let scanned = io.receipt().items_examined - before.items_examined;
            assert_eq!(scanned, u64::from(step) + 1);
            if step == 999 {
                assert!(
                    scanned > 291,
                    "the fixed-width lookup oracle accepted the linear scan control"
                );
            }
            println!(
                "pending tail native={native} steps={} indexed_items={examined} scan_items={scanned}",
                step + 1
            );
        }
        // Interleaved entries for a step retain reverse journal order, and
        // the combined step bound includes every entry rather than each page.
        pending.push(1_001, [3; 32], [4; 32], Some(0), None, scope.io())?;
        let mut selected = Funded::new(Some(&io))?;
        assert!(
            pending
                .collect(Some(0), 1, &mut selected, Some(&io))
                .is_err()
        );
        drop(selected);
        let mut selected = Funded::new(Some(&io))?;
        pending.collect(Some(0), 2, &mut selected, Some(&io))?;
        assert_eq!(
            selected
                .values
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            [1_001, 1]
        );
        drop(selected);
        pending.clear();
        pending.push(1_002, [5; 32], [6; 32], None, None, scope.io())?;
        let mut selected = Funded::new(Some(&io))?;
        pending.collect(Some(0), 1, &mut selected, Some(&io))?;
        assert!(
            selected.values.is_empty(),
            "cleared tail retained an old head"
        );
        pending.collect(None, 1, &mut selected, Some(&io))?;
        assert_eq!(selected.values.len(), 1);
        assert_eq!(selected.values[0].sequence, 1_002);
        drop(selected);
        drop(pending);
        scope.finish(0)?;
        assert_eq!(io.owned_bytes()?, 0);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
