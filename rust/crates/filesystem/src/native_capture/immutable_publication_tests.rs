use super::*;
use crate::native_host::publish_probe;

#[test]
fn immutable_bytes_survive_each_publication_kill_and_reject_changed_content()
-> Result<(), DemandError> {
    for step in 0..publish_probe::STEPS {
        let directory = tempfile::tempdir()?;
        let held = hold_root(directory.path())?;
        let leaf = Path::new("request.json");
        let bytes = br#"{"request":"complete"}"#;
        publish_probe::kill_at(Some(step));
        assert!(held.publish_immutable(leaf, bytes).is_err());
        publish_probe::kill_at(None);
        if let Ok(retained) = std::fs::read(directory.path().join(leaf)) {
            assert_eq!(retained, bytes);
        }
        held.publish_immutable(leaf, bytes)?;
        held.publish_immutable(leaf, bytes)?;
        assert!(held.publish_immutable(leaf, b"changed").is_err());
        assert!(
            held.publish_immutable(leaf, b"{\"request\":\"complete\"}extra")
                .is_err()
        );
        assert_eq!(std::fs::read(directory.path().join(leaf))?, bytes);
    }
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
    held.publish_immutable(Path::new("request"), b"retained")?;
    assert_eq!(std::fs::read(moved.join("request"))?, b"retained");
    assert!(!path.join("request").exists());
    std::fs::write(&target, b"unchanged")?;
    std::fs::hard_link(&target, moved.join("link"))?;
    assert!(
        held.publish_immutable(Path::new("link"), b"replaced")
            .is_err()
    );
    assert_eq!(std::fs::read(&target)?, b"unchanged");
    held.remove_file(Path::new("request"))?;
    held.remove_file(Path::new("link"))?;
    assert!(!moved.join("request").exists());
    assert!(!moved.join("link").exists());
    assert!(!path.join("request").exists());
    assert_eq!(std::fs::read(&target)?, b"unchanged");
    assert!(held.remove_file(Path::new("../target")).is_err());
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
    let killed = held.publish_immutable(leaf, bytes);
    publish_probe::kill_at(None);
    publish_probe::tear(false);
    assert!(killed.is_err());
    assert_eq!(
        std::fs::read(directory.path().join(leaf))?,
        bytes[..bytes.len() / 2]
    );
    assert!(held.publish_immutable(leaf, bytes).is_err());
    Ok(())
}
