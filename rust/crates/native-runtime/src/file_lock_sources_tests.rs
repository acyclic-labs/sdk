//! Every Rust source in the repository takes OS file locks through
//! [`super::FileLock`]. A raw lock released by closing its file stays held
//! while any concurrently spawned process shares the description.
//!
//! This scan catches every textual spelling of a raw lock: fs2, `flock` calls,
//! Win32 lock calls, and std's lock methods reached by path. Method calls such
//! as `file.try_lock()` are type-checked instead by the `disallowed-methods`
//! entries in each `clippy.toml`.
use std::collections::BTreeMap;

/// Source trees scanned from the repository root.
const ROOTS: [&str; 7] = [
    "rust/crates",
    "rust/fuzz",
    "rust/verification",
    "plugin",
    "examples",
    "compatibility",
    "fuzz",
];

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
fn violations<'a>(files: impl IntoIterator<Item = (String, &'a str)>) -> Vec<String> {
    let mut found = BTreeMap::new();
    for (path, text) in files {
        for spelling in RAW_LOCKS {
            let count = text.matches(spelling).count();
            if count > 0 {
                found.insert((path.clone(), spelling), count);
            }
        }
    }
    let mut violations = Vec::new();
    for (path, spelling, count) in ALLOWED {
        match found.remove(&(path.to_owned(), spelling)) {
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

fn sources() -> &'static [(String, String)] {
    static FILES: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();
    FILES.get_or_init(|| {
        let root = acyclic_test_support::source_path("");
        let mut files = Vec::new();
        let mut pending = ROOTS.map(|name| root.join(name)).to_vec();
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory).expect("source directory") {
                let path = entry.expect("source entry").path();
                if path.is_dir() {
                    let name = path.file_name().and_then(|name| name.to_str());
                    if !matches!(name, Some("target" | "node_modules" | "vendor" | ".git")) {
                        pending.push(path);
                    }
                    continue;
                }
                if path.extension().is_none_or(|extension| extension != "rs") {
                    continue;
                }
                let relative = path
                    .strip_prefix(&root)
                    .expect("repository source")
                    .to_str()
                    .expect("UTF-8 source path")
                    .replace('\\', "/");
                // This scan spells every raw lock it forbids.
                if relative == "rust/crates/native-runtime/src/file_lock_sources_tests.rs" {
                    continue;
                }
                files.push((relative, std::fs::read_to_string(&path).expect("source")));
            }
        }
        files
    })
}

#[test]
fn every_file_lock_goes_through_the_explicitly_releasing_guard() {
    let files = sources();
    assert!(files.len() > 3000, "source scan lost its roots");
    let found = violations(
        files
            .iter()
            .map(|(path, text)| (path.clone(), text.as_str())),
    );
    assert!(found.is_empty(), "raw file locks: {found:#?}");
}

#[test]
fn planted_raw_file_locks_fail_the_scan() {
    let files = sources();
    let scan = |target: &str, plant: &str| {
        assert!(
            files.iter().any(|(path, _)| path == target),
            "planted source target is absent: {target}"
        );
        violations(files.iter().map(|(path, text)| {
            if path == target {
                (path.clone(), plant)
            } else {
                (path.clone(), text.as_str())
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
