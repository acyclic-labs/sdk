use acyclic_harness::task_volume::FileKey;

fn assert_identity(path: &str, key: FileKey) {
    assert_eq!(key.path(), path, "accepted paths must already be canonical");
    assert_eq!(FileKey::parse(&key.path()).unwrap(), key);
    let json = serde_json::to_vec(&key).unwrap();
    assert_eq!(serde_json::from_slice::<FileKey>(&json).unwrap(), key);
}

pub fn exercise(data: &[u8]) {
    if let Ok(path) = std::str::from_utf8(data) {
        if let Ok(key) = FileKey::parse(path) {
            assert_identity(path, key);
        }
    }
    if let Ok(key) = serde_json::from_slice::<FileKey>(data) {
        assert_identity(&key.path(), key.clone());
    }
}

#[cfg(test)]
pub fn broken_alias_acceptance() {
    // A broken normalizing parser accepts an alias instead of rejecting it.
    assert_identity("workspace/a/../b", FileKey::parse("workspace/b").unwrap());
}
