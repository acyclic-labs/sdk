use super::*;
use crate::AgentId;
use crate::conversation::{VolumeClass, VolumeOwner};
use crate::resources::ProviderRef;

fn store(maximum_bytes: u64, maximum_files: u64) -> Result<MemoryStore> {
    let provider = ProviderRef::new("local".into(), "memory".into(), "2".into(), &mut None)?;
    let volume = VolumeRef::new(
        provider,
        "test-volume".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::new()),
        &mut None,
    )?;
    MemoryStore::new(volume, 4_096, 4_096, maximum_bytes, maximum_files)
}

#[test]
fn stages_deduplicated_bytes_under_file_and_byte_bounds() -> Result<()> {
    let mut store = store(3, 16)?;
    let first = store.stage("notes/one.txt", b"abc", "text/plain", "one.txt", true)?;
    for index in 0..15 {
        let path = format!("notes/copy-{index}.txt");
        let same = store.stage(&path, b"abc", "text/plain", "copy.txt", true)?;
        assert_eq!(first.descriptor(), same.descriptor());
    }
    assert_eq!(store.generation(), 16);
    assert!(
        store
            .stage(
                "notes/different.txt",
                b"d",
                "text/plain",
                "different.txt",
                true
            )
            .is_err()
    );
    assert_eq!(store.read(&first)?, b"abc");
    Ok(())
}

#[test]
fn provider_version_rejects_changed_file_contract() -> Result<()> {
    let mut store = store(32, 8)?;
    let first = store.stage("notes/current.txt", b"one", "text/plain", "one.txt", true)?;
    let changed_display = FileRef::new(
        store.volume.clone(),
        first.path().into(),
        first.version().into(),
        first.descriptor().clone(),
        "renamed.txt".into(),
        &mut None,
    )?;
    assert!(
        store
            .retain_reference(&changed_display, b"one", false, None)
            .is_err()
    );
    Ok(())
}

#[test]
fn path_history_is_generation_pinned_and_rejects_conflicts() -> Result<()> {
    let mut store = store(32, 8)?;
    let first = store.stage("notes/current.txt", b"one", "text/plain", "one.txt", true)?;
    let generation = store.generation();
    let second = store.stage("notes/current.txt", b"two", "text/plain", "two.txt", true)?;
    assert!(store.path_conflicts("notes"));
    assert_eq!(
        store.read_path("notes/current.txt", Some(generation))?,
        first
    );
    assert_eq!(store.read_path("notes/current.txt", None)?, second);
    let page = store.list("notes", Some(generation), None, 16)?;
    assert_eq!(
        page.entries,
        vec![MemoryStoreEntry {
            name: "current.txt".into(),
            kind: MemoryStoreEntryKind::File,
        }]
    );
    Ok(())
}
