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
        let mut single_allocations = 0;
        for step in 0..1_000 {
            let before = io.receipt();
            pending.push(
                u64::from(step) + 1,
                [1; 32],
                [2; 32],
                Some(step),
                None,
                scope.io(),
            )?;
            let after = io.receipt();
            assert_eq!(
                after.bytes_copied - before.bytes_copied,
                if step == 1 {
                    std::mem::size_of::<Entry>() as u64
                } else {
                    0
                },
                "pending insertion exceeded its one inline promotion copy"
            );
            if step == 0 {
                single_allocations = after.allocation_operations - before.allocation_operations;
            }
            if ![0, 9, 99, 999].contains(&step) {
                continue;
            }
            let mut selected = Funded::new(Some(&io))?;
            let before = io.receipt();
            pending.collect(Some(0), None, Some(1), &mut selected, Some(&io))?;
            let examined = io.receipt().items_examined - before.items_examined;
            assert_eq!(selected.values.len(), 1);
            assert_eq!(selected.values[0].sequence, 1);
            // At most 256 radix branches, one root, one leaf, 32 key
            // bytes and the selected entry; no journal-length term.
            assert!(
                examined <= 291,
                "tail lookup scanned unrelated steps: {examined}"
            );
            println!(
                "pending tail native={native} steps={} indexed_items={examined}",
                step + 1
            );
        }
        assert_eq!(
            single_allocations, 0,
            "single pending entry allocated backing"
        );
        // Interleaved entries for a step retain reverse journal order, and
        // the combined step bound includes every entry rather than each page.
        pending.push(1_001, [3; 32], [4; 32], Some(0), None, scope.io())?;
        let mut selected = Funded::new(Some(&io))?;
        assert!(
            pending
                .collect(Some(0), None, Some(1), &mut selected, Some(&io))
                .is_err()
        );
        drop(selected);
        let mut selected = Funded::new(Some(&io))?;
        pending.collect(Some(0), None, Some(2), &mut selected, Some(&io))?;
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
        pending.collect(Some(0), None, Some(1), &mut selected, Some(&io))?;
        assert!(
            selected.values.is_empty(),
            "cleared tail retained an old head"
        );
        pending.collect(None, None, Some(1), &mut selected, Some(&io))?;
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
