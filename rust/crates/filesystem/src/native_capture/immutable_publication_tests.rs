use super::*;
use crate::native_host::publish_probe;

#[test]
fn immutable_bytes_survive_each_publication_kill_and_reject_changed_content()
-> Result<(), DemandError> {
    for (durability, step) in [NativeDurability::Visibility, NativeDurability::Durable]
        .into_iter()
        .flat_map(|durability| (0..publish_probe::STEPS).map(move |step| (durability, step)))
    {
        let directory = tempfile::tempdir()?;
        let held = hold_root(directory.path())?;
        let leaf = Path::new("request.json");
        let bytes = br#"{"request":"complete"}"#;
        publish_probe::kill_at(Some(step));
        assert!(held.publish_immutable(leaf, bytes, durability).is_err());
        publish_probe::kill_at(None);
        if let Ok(retained) = std::fs::read(directory.path().join(leaf)) {
            assert_eq!(retained, bytes);
        }
        held.publish_immutable(leaf, bytes, durability)?;
        held.publish_immutable(leaf, bytes, durability)?;
        assert!(
            held.publish_immutable(leaf, b"changed", durability)
                .is_err()
        );
        assert!(
            held.publish_immutable(leaf, b"{\"request\":\"complete\"}extra", durability)
                .is_err()
        );
        assert_eq!(std::fs::read(directory.path().join(leaf))?, bytes);
    }
    publication_barriers_follow_the_requested_guarantee()
}

/// Use the original finite work frame to observe barriers on the physical
/// path. In particular, promoting existing visible bytes must flush the file.
fn publication_barriers_follow_the_requested_guarantee() -> Result<(), DemandError> {
    use crate::demand::DemandWork;
    let directory = tempfile::tempdir()?;
    let held = hold_root(directory.path())?;
    let stop = CancellationToken::new();
    let budget = WorkBudget {
        durability_operations: 0,
        ..WorkBudget::UNBOUNDED
    };
    let mut visible = DemandWork::new(budget, &stop);
    let leaf = Path::new("projection");
    for _ in 0..2 {
        held.root.publish_immutable(
            leaf,
            b"complete",
            NativeDurability::Visibility,
            &mut visible,
        )?;
    }
    let before = std::fs::metadata(directory.path().join(leaf))?;
    let mut durable = DemandWork::new(
        WorkBudget {
            durability_operations: 2,
            ..budget
        },
        &stop,
    );
    held.root
        .publish_immutable(leaf, b"complete", NativeDurability::Durable, &mut durable)?;
    assert_eq!(
        durable.work.durability_operations, 2,
        "settle existing file and name"
    );
    use std::os::unix::fs::MetadataExt as _;
    assert_eq!(
        std::fs::metadata(directory.path().join(leaf))?.ino(),
        before.ino()
    );
    held.root
        .remove_record_file(leaf, NativeDurability::Visibility, &mut visible)?;
    assert!(!directory.path().join(leaf).exists());

    let relative = Path::new("one/two");
    let child =
        held.root
            .create_directories(relative, NativeDurability::Visibility, &mut visible)?;
    let identity = child.identity();
    drop(child);
    let mut durable = DemandWork::new(
        WorkBudget {
            durability_operations: 2,
            ..budget
        },
        &stop,
    );
    assert_eq!(
        held.root
            .create_directories(relative, NativeDurability::Durable, &mut durable)?
            .identity(),
        identity
    );
    assert_eq!(
        durable.work.durability_operations, 2,
        "settle both existing parent names"
    );
    assert_eq!(visible.work.durability_operations, 0);

    // The former unconditional barriers refuse on exactly this zero-barrier
    // frame. A failed file barrier must never expose the private partial file.
    let rejected = Path::new("durable");
    assert!(matches!(
        held.root.publish_immutable(
            rejected,
            b"complete",
            NativeDurability::Durable,
            &mut DemandWork::new(budget, &stop)
        ),
        Err(DemandError::Work(WorkError::BudgetExceeded {
            counter: "durability_operations",
            maximum: 0,
            ..
        }))
    ));
    assert!(!directory.path().join(rejected).exists());
    let mut publish = DemandWork::new(
        WorkBudget {
            durability_operations: 2,
            ..budget
        },
        &stop,
    );
    held.root.publish_immutable(
        rejected,
        b"complete",
        NativeDurability::Durable,
        &mut publish,
    )?;
    assert_eq!(publish.work.durability_operations, 2);
    let mut remove = DemandWork::new(
        WorkBudget {
            durability_operations: 1,
            ..budget
        },
        &stop,
    );
    held.root
        .remove_record_file(rejected, NativeDurability::Durable, &mut remove)?;
    assert_eq!(remove.work.durability_operations, 1);
    assert!(!directory.path().join(rejected).exists());
    Ok(())
}

#[test]
fn immutable_publication_stays_in_held_directory_and_preserves_existing_leaf()
-> Result<(), DemandError> {
    let fixture = tempfile::tempdir()?;
    let path = fixture.path().join("workspace");
    let moved = fixture.path().join("moved");
    let target = fixture.path().join("target");
    std::fs::create_dir(&path)?;
    let held = hold_root(&path)?;
    std::fs::rename(&path, &moved)?;
    std::fs::create_dir(&path)?;
    held.publish_immutable(
        Path::new("request"),
        b"retained",
        crate::NativeDurability::Durable,
    )?;
    assert_eq!(std::fs::read(moved.join("request"))?, b"retained");
    assert!(!path.join("request").exists());
    std::fs::write(&target, b"unchanged")?;
    std::fs::hard_link(&target, moved.join("link"))?;
    assert!(
        held.publish_immutable(
            Path::new("link"),
            b"replaced",
            crate::NativeDurability::Durable
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&target)?, b"unchanged");
    held.remove_file(Path::new("request"), crate::NativeDurability::Durable)?;
    held.remove_file(Path::new("link"), crate::NativeDurability::Durable)?;
    assert!(!moved.join("request").exists());
    assert!(!moved.join("link").exists());
    assert!(!path.join("request").exists());
    assert_eq!(std::fs::read(&target)?, b"unchanged");
    assert!(
        held.remove_file(Path::new("../target"), crate::NativeDurability::Durable)
            .is_err()
    );
    Ok(())
}

#[test]
fn negative_control_publication_in_place_leaves_a_torn_immutable_record() -> Result<(), DemandError>
{
    let directory = tempfile::tempdir()?;
    let held = hold_root(directory.path())?;
    let leaf = Path::new("request.json");
    let bytes = br#"{"request":"complete"}"#;
    publish_probe::tear(true);
    publish_probe::kill_at(Some(1));
    let killed = held.publish_immutable(leaf, bytes, crate::NativeDurability::Durable);
    publish_probe::kill_at(None);
    publish_probe::tear(false);
    assert!(killed.is_err());
    assert_eq!(
        std::fs::read(directory.path().join(leaf))?,
        bytes[..bytes.len() / 2]
    );
    assert!(
        held.publish_immutable(leaf, bytes, crate::NativeDurability::Durable)
            .is_err()
    );
    Ok(())
}
