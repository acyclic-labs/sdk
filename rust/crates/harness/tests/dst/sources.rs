//! Mount callbacks read time through the captured clock seam.
use std::collections::BTreeMap;

/// Host clock reads in every spelling: an associated or free `now` reached
/// through any path, alias, re-export or import (called or passed as a
/// function, including `tokio::time::Instant`), `elapsed`, a renamed clock type,
/// and direct OS clock calls. `.now()` on a value is the captured seam method.
fn host_clock_reads(text: &str) -> usize {
    let bytes = text.as_bytes();
    let identifier = |byte: &u8| byte.is_ascii_alphanumeric() || *byte == b'_';
    let mut count = 0;
    for (at, _) in text.match_indices("now") {
        let before = at.checked_sub(1).map(|index| bytes[index]);
        let after = bytes.get(at + 3);
        if before.as_ref().is_some_and(identifier) || after.is_some_and(identifier) {
            continue;
        }
        let path = text[..at].ends_with("::");
        let free_call = after == Some(&b'(') && !matches!(before, Some(b'.' | b':'));
        count += usize::from(path || free_call);
    }
    count += text.matches(".elapsed(").count();
    for source in [
        "Instant as ",
        "SystemTime as ",
        "clock_gettime",
        "gettimeofday",
        "mach_absolute_time",
        "mach_continuous_time",
        "QueryPerformanceCounter",
        "GetTickCount",
        "GetSystemTime",
        "timeGetTime",
    ] {
        count += text.matches(source).count();
    }
    count
}

// Only the captured seam contains host fallbacks. Callback and lifecycle
// helpers must use it, including helpers added after this check was written.
const MOUNT_CLOCK_ALLOWED: [(&str, usize); 1] = [("filesystem/src/native_mount/clock.rs", 4)];

fn mount_clock_violations<'a>(files: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<String> {
    let mut found: BTreeMap<&str, usize> = files
        .into_iter()
        .filter(|(path, _)| path.starts_with("filesystem/src/native_mount/"))
        .map(|(path, text)| (path, host_clock_reads(text)))
        .filter(|(_, count)| *count > 0)
        .collect();
    let mut violations = Vec::new();
    for (path, count) in MOUNT_CLOCK_ALLOWED {
        match found.remove(path) {
            Some(actual) if actual == count => (),
            actual => violations.push(format!(
                "{path}: host clock reads expected {count}, found {}",
                actual.unwrap_or(0)
            )),
        }
    }
    violations.extend(
        found
            .into_iter()
            .map(|(path, count)| format!("{path}: unseeded host clock x{count}")),
    );
    violations
}

#[test]
fn mount_callbacks_cannot_read_host_clocks_directly() {
    // Scan the whole native_mount tree on every build platform, so callback
    // helpers in any file, existing or new, are covered without a hand-kept
    // callback list. Lifecycle helper subprocesses are refused/scripted by Driver.
    let files: Vec<_> = acyclic_test_support::repository_rust_sources()
        .iter()
        .filter_map(|(path, text)| {
            let path = path.strip_prefix("rust/crates/")?;
            let parts: Vec<_> = path.split('/').collect();
            let name = parts.last()?;
            (path.starts_with("filesystem/src/native_mount/")
                && !parts
                    .iter()
                    .any(|part| *part == "tests" || part.ends_with("_tests"))
                && *name != "tests.rs"
                && !name.ends_with("_tests.rs")
                && !name.starts_with("tests_"))
            .then(|| (path.to_owned(), (*text).to_owned()))
        })
        .collect();
    let scan = |planted: Option<(&str, &str)>| {
        let added = planted.filter(|(target, _)| files.iter().all(|(path, _)| path != target));
        mount_clock_violations(
            files
                .iter()
                .map(|(path, text)| match planted {
                    Some((target, plant)) if path == target => (path.as_str(), plant),
                    _ => (path.as_str(), text.as_str()),
                })
                .chain(added),
        )
    };
    let found = scan(None);
    assert!(found.is_empty(), "mount host clock reads: {found:#?}");
    // Every escape form must fail, in an existing callback and in a new helper.
    let callback = "filesystem/src/native_mount/fuse/attributes.rs";
    let helper = "filesystem/src/native_mount/fuse/clock_helper.rs";
    let original = &files.iter().find(|(path, _)| path == callback).unwrap().1;
    for plant in [
        "let started = std::time::Instant::now();",
        "let started = tokio::time::Instant::now();",
        "let now = stamp.unwrap_or_else(std::time::SystemTime::now);",
        "use std::time::SystemTime as Wall; let now = Wall::now;",
        "use std::time::Instant as Tick;",
        "use crate::clock_helpers::now; let at = now();",
        "let age = std::time::UNIX_EPOCH.elapsed();",
        "let age = started.elapsed();",
        "unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &raw mut now) };",
    ] {
        let text = format!("{original}\n{plant}");
        assert_eq!(scan(Some((callback, &text))).len(), 1, "{plant}");
        assert_eq!(scan(Some((helper, plant))).len(), 1, "new helper: {plant}");
    }
    // The seeded seam method stays legal; an extra lifecycle read is not.
    let seam = format!("{original}\nlet now = clock.now();");
    assert!(scan(Some((callback, &seam))).is_empty());
    let lifecycle = "filesystem/src/native_mount/fuse/lifecycle.rs";
    let text = &files.iter().find(|(path, _)| path == lifecycle).unwrap().1;
    assert_eq!(
        scan(Some((lifecycle, &format!("{text}\nInstant::now();")))),
        [format!("{lifecycle}: unseeded host clock x1")]
    );
}
