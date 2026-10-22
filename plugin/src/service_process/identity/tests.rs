use super::*;
use std::sync::{Arc, Barrier, Mutex};

thread_local! {
    pub(super) static READ_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[test]
fn concurrent_service_roots_hash_one_settled_artifact_and_persist_each_cache() {
    let root = tempfile::tempdir().unwrap();
    let artifact = root.path().join("artifact");
    fs::write(&artifact, b"the same artifact bytes").unwrap();
    let (_, changed) = executable_fingerprint(&fs::File::open(&artifact).unwrap()).unwrap();
    // Advance the policy's clock, without sleeping or changing file timestamps.
    let now = changed + SETTLED_EXECUTABLE_AGE + std::time::Duration::from_secs(1);
    let cache = Arc::new(Mutex::new(None));
    let start = Arc::new(Barrier::new(4));
    let threads = (0..4)
        .map(|index| {
            let data = root.path().join(index.to_string());
            fs::create_dir(&data).unwrap();
            let artifact = artifact.clone();
            let cache = Arc::clone(&cache);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                READ_BYTES.set(0);
                let identity = service_identity_using(&data, &artifact, &cache, || now).unwrap();
                let reads = READ_BYTES.get();
                // A new process can use this root's saved cache as before.
                let cold = Mutex::new(None);
                assert_eq!(
                    service_identity_using(&data, &artifact, &cold, || now).unwrap(),
                    identity
                );
                assert_eq!(READ_BYTES.get(), reads);
                assert!(
                    cold.lock().unwrap().is_none(),
                    "disk entries must not seed another root's memory cache"
                );
                (identity, reads)
            })
        })
        .collect::<Vec<_>>();
    let mut total = 0;
    let expected = service_identity_from_digest(&blake3::hash(b"the same artifact bytes").to_hex());
    for thread in threads {
        let (identity, reads) = thread.join().unwrap();
        assert_eq!(identity, expected);
        total += reads;
    }
    assert_eq!(total, b"the same artifact bytes".len());
}

#[test]
fn identity_reuse_rejects_rewrites_replacements_and_unsettled_bytes() {
    let root = tempfile::tempdir().unwrap();
    let artifact = root.path().join("artifact");
    fs::write(&artifact, b"original artifact").unwrap();
    let cache = Mutex::new(None);
    let identity = |name: &str, settled: bool| {
        let data = root.path().join(name);
        fs::create_dir(&data).unwrap();
        let (_, changed) = executable_fingerprint(&fs::File::open(&artifact).unwrap()).unwrap();
        let now = if settled {
            changed + SETTLED_EXECUTABLE_AGE + std::time::Duration::from_secs(1)
        } else {
            changed
        };
        service_identity_using(&data, &artifact, &cache, || now).unwrap()
    };
    let original = identity("original", true);
    let metadata = fs::metadata(&artifact).unwrap();
    fs::write(&artifact, b"rewritten bytes of a different length").unwrap();
    fs::File::options()
        .write(true)
        .open(&artifact)
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(metadata.modified().unwrap()))
        .unwrap();
    let rewritten = identity("rewritten", true);
    assert_ne!(original, rewritten);
    fs::remove_file(&artifact).unwrap();
    fs::write(&artifact, b"replaced").unwrap();
    let replaced = identity("replaced", true);
    assert_ne!(replaced, rewritten);
    fs::write(&artifact, b"not settled").unwrap();
    READ_BYTES.set(0);
    let first = identity("unsettled-a", false);
    let second = identity("unsettled-b", false);
    assert_eq!(first, second);
    assert_eq!(READ_BYTES.get(), 2 * b"not settled".len());
    assert_eq!(cache.lock().unwrap().as_ref().unwrap().1, replaced);
}
