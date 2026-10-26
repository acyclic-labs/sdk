//! Every Rust source in the repository takes OS file locks through
//! [`super::FileLock`]. A raw lock released by closing its file stays held
//! while any concurrently spawned process shares the description.
//!
//! This scan catches every textual spelling of a raw lock: fs2, `flock` calls,
//! Win32 lock calls, and std's lock methods reached by path. Method calls such
//! as `file.try_lock()` are type-checked instead by the `disallowed-methods`
//! entries in each `clippy.toml`.
use std::collections::BTreeMap;

/// Raw file-lock spellings.
const RAW_LOCKS: [&str; 12] = [
    "fs2",
    "flock(",
    "FlockOperation",
    "LockFileEx",
    "UnlockFile",
    "try_lock_shared",
    "lock_shared",
    "try_lock_exclusive",
    "lock_exclusive",
    "File::lock",
    "File::try_lock",
    "File::unlock",
];

/// Exact remaining raw spellings: (file, spelling, count).
const ALLOWED: [(&str, &str, usize); 4] = [
    // The guard itself.
    (
        "rust/crates/native-runtime/src/file_lock.rs",
        "LockFileEx",
        1,
    ),
    (
        "rust/crates/native-runtime/src/file_lock.rs",
        "try_lock_shared",
        1,
    ),
    (
        "rust/crates/native-runtime/src/file_lock.rs",
        "lock_shared",
        2,
    ),
    // A Perl contender's lock, taken and released by its own process.
    (
        "rust/crates/filesystem/src/native_mount/darwin_mount/tests/mod.rs",
        "flock(",
        1,
    ),
];

/// Compares raw-lock counts with the exact allowance, in both directions.
fn violations<'a>(files: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<String> {
    let mut found = BTreeMap::new();
    for (path, text) in files {
        for spelling in RAW_LOCKS {
            let count = text.matches(spelling).count();
            if count > 0 {
                found.insert((path, spelling), count);
            }
        }
    }
    let mut violations = Vec::new();
    for (path, spelling, count) in ALLOWED {
        match found.remove(&(path, spelling)) {
            Some(actual) if actual == count => (),
            actual => violations.push(format!(
                "{path}: {spelling} expected {count}, found {}",
                actual.unwrap_or(0)
            )),
        }
    }
    violations.extend(
        found
            .into_iter()
            .map(|((path, spelling), count)| format!("{path}: raw {spelling} x{count}")),
    );
    violations
}

fn sources() -> &'static [(&'static str, &'static str)] {
    static FILES: std::sync::OnceLock<Vec<(&str, &str)>> = std::sync::OnceLock::new();
    FILES.get_or_init(|| {
        acyclic_test_support::repository_rust_sources()
            .iter()
            .copied()
            // This policy spells its forbidden inputs; every other Rust source
            // in every original root remains subject to the exact allowance.
            .filter(|(path, _)| {
                *path != "rust/crates/native-runtime/src/file_lock_sources_tests.rs"
            })
            .collect()
    })
}

#[test]
fn compiled_source_policy_borrows_every_input_without_target_file_opens() {
    let files = sources();
    assert!(files.len() > 3000, "source catalog lost its roots");
    for (path, text) in files {
        let original = acyclic_test_support::repository_rust_source(std::path::Path::new(path))
            .expect("compiled source exists");
        assert!(std::ptr::eq(text.as_ptr(), original.as_ptr()));
        assert_eq!(*text, original);
    }
}

#[test]
fn every_file_lock_goes_through_the_explicitly_releasing_guard() {
    let files = sources();
    assert!(files.len() > 3000, "source scan lost its roots");
    let found = violations(files.iter().copied());
    assert!(found.is_empty(), "raw file locks: {found:#?}");
}

#[test]
fn planted_raw_file_locks_fail_the_scan() {
    let files = sources();
    let scan = |target: &str, plant: &str| {
        assert!(
            files.iter().any(|(path, _)| *path == target),
            "planted source target is absent: {target}"
        );
        violations(files.iter().copied().map(|(path, text)| {
            if path == target {
                (path, plant)
            } else {
                (path, text)
            }
        }))
    };
    let target = "rust/crates/filesystem/src/native_mount/fence_io.rs";
    for (plant, spelling) in [
        ("fs2::FileExt::try_lock_exclusive(&file)", "fs2"),
        ("unsafe { libc::flock(fd, libc::LOCK_EX) }", "flock("),
        (
            "rustix::fs::flock(&file, FlockOperation::LockExclusive)",
            "FlockOperation",
        ),
        (
            "LockFileEx(handle, flags, 0, 1, 0, overlapped)",
            "LockFileEx",
        ),
        ("UnlockFile(handle, 0, 0, 1, 0)", "UnlockFile"),
        ("std::fs::File::try_lock(&file)", "File::try_lock"),
        ("File::lock_shared(&file)", "lock_shared"),
        ("File::unlock(&file)", "File::unlock"),
    ] {
        let found = scan(target, plant);
        assert!(
            found.contains(&format!("{target}: raw {spelling} x1")),
            "{plant}: {found:?}"
        );
    }
    // A removed allowance is stale rather than silently passing.
    let guard = "rust/crates/native-runtime/src/file_lock.rs";
    assert!(scan(guard, "").contains(&format!("{guard}: lock_shared expected 2, found 0")));
}
