use super::*;
use std::os::unix::fs::MetadataExt as _;

#[derive(Clone, Copy)]
enum Control {
    Original,
    FreshLedger,
    ParentScan,
}

// The independent expectation describes the four physical mutations: remove
// the exact empty directory, lock the registry, unlink the fence, unlock.
pub(super) fn expected(before: crate::WorkCounters, live: u64, leaf: &str) -> crate::WorkCounters {
    let mut expected = before;
    expected.items_examined += leaf.len() as u64;
    expected.bytes_copied += leaf.len() as u64;
    expected.allocation_operations += 1;
    expected.peak_allocation_bytes = expected
        .peak_allocation_bytes
        .max(live + leaf.len() as u64 + 1);
    expected.backend_write_operations += 4;
    expected
}

fn retirement(control: Control, authored: usize) -> Result<bool, Box<dyn std::error::Error>> {
    registry::initialize()?;
    let root = tempfile::tempdir()?;
    let destination = root.path().join("owned-mount");
    std::fs::create_dir(&destination)?;
    for index in 0..authored {
        std::fs::write(
            root.path().join(format!("user-{index}")),
            index.to_le_bytes(),
        )?;
    }
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 128,
            backend_write_operations: 128,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let mut guard = None;
    MountDestinationGuard::acquire_with_work(&destination, false, &work, &mut guard, false)?;
    let registry_before = std::fs::metadata(registry::path()?)?;
    let mut packet = crate::native_mount::startup::NativeMountRetirement {
        destination_guard: guard,
        retained_source: None,
        startup_source_watch: None,
    };
    let before = work.receipt();
    let live = work.live_allocation_bytes();
    let guard = packet.destination_guard.as_mut().unwrap();
    let (fence, _) = MountDestinationGuard::paths_for_canonical(&destination.canonicalize()?)?;
    assert!(fence.is_file(), "fixture did not acquire its actual fence");
    guard.prepare_destination_cleanup(&destination, &work)?;
    if matches!(control, Control::ParentScan) {
        // Restore the real old ingress, not a synthetic counter increment.
        let (reader, allocation) = crate::native_host::read_dir_held(
            guard.parent_directory.as_ref().unwrap(),
            &mut &work,
        )?;
        drop(reader);
        drop(allocation);
    }
    cancellation.cancel();
    if matches!(control, Control::FreshLedger) {
        guard.remove_destination()?;
        let fresh = NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| error)?;
        guard.release(&mut &fresh)?;
        drop(packet.destination_guard.take());
    } else {
        assert!(packet.retire(None)?);
        let completed = work.receipt();
        assert!(!packet.retire(None)?);
        assert_eq!(
            work.receipt(),
            completed,
            "retirement retried physical work"
        );
    }
    assert!(packet.is_retired());
    assert!(!destination.exists());
    assert!(!fence.exists());
    let registry_after = std::fs::metadata(registry::path()?)?;
    assert_eq!(
        (registry_before.dev(), registry_before.ino()),
        (registry_after.dev(), registry_after.ino()),
    );
    assert_eq!(work.live_allocation_bytes(), live);
    for index in 0..authored {
        assert_eq!(
            std::fs::read(root.path().join(format!("user-{index}")))?,
            index.to_le_bytes(),
        );
    }
    Ok(work.receipt() == expected(before, live, "owned-mount"))
}

#[test]
fn cancelled_retirement_uses_exact_original_work_and_preserves_authored_siblings()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(retirement(Control::Original, 4097)?);
    assert!(retirement(Control::Original, 0)?);
    assert!(
        !retirement(Control::FreshLedger, 0)?,
        "fresh cleanup ledger escaped the original-work oracle",
    );
    assert!(
        !retirement(Control::ParentScan, 0)?,
        "parent scan escaped the exact-work oracle",
    );
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn retirement_work_is_independent_of_authored_sibling_count(authored in 0usize..24) {
        proptest::prop_assert!(retirement(Control::Original, authored).unwrap());
    }
}
