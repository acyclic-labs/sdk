//! Exact allowances for the listed identity, wall-clock and entropy sources
//! in seven crates. New or stale direct-source counts fail this scan.
//!
//! Mount clock reads are checked separately below over the whole native_mount
//! tree in every spelling. Host fallbacks live in the captured clock seam and
//! the macOS uptime fallback; mount start/stop deadlines have exact counts.
use std::{collections::BTreeMap, path::Path};

/// Crates linked into the DST closure whose sources reach simulated paths.
const CRATES: [&str; 7] = [
    "harness",
    "harness-core",
    "filesystem",
    "stream",
    "objects",
    "native-runtime",
    "actors",
];

/// Direct nondeterministic sources. Patterns omit the call parenthesis so a
/// source passed as a function (`unwrap_or_else(SystemTime::now)`) still counts;
/// identity constructors match through any type path or alias, and renaming a
/// clock or identity type is itself a source.
const SOURCES: [&str; 24] = [
    "::new_v4",
    "::now_v7",
    "::new_v7",
    "SystemTime::now",
    "SystemTime as ",
    "= SystemTime;",
    "= std::time::SystemTime;",
    "Uuid as ",
    "getrandom::fill",
    "getrandom::getrandom",
    "getrandom::u32",
    "getrandom::u64",
    "thread_rng",
    "rand::rng",
    "rand::random",
    "OsRng",
    "fastrand::",
    "Timestamp::now",
    "Utc::now",
    "Local::now",
    "OffsetDateTime::now",
    "Utc as ",
    "OffsetDateTime as ",
    "RandomState",
];

/// Exact remaining direct sources: (file, source, count).
const ALLOWED: [(&str, &str, usize); 12] = [
    // Seam fallbacks, reached only when no seeded scope is active.
    ("harness-core/src/identity.rs", "::new_v4", 1),
    ("harness-core/src/identity.rs", "getrandom::fill", 1),
    ("filesystem/src/identity.rs", "::new_v4", 1),
    ("filesystem/src/identity.rs", "::now_v7", 1),
    ("stream/src/lib/domain.rs", "SystemTime::now", 1),
    ("stream/src/lib/stream.rs", "::new_v4", 1),
    ("objects/src/v1/memory/mod.rs", "getrandom::fill", 1),
    ("objects/src/v1/memory/mod.rs", "SystemTime::now", 1),
    // cfg(test) poll-sweep fixture; wasm32 builds compile no simulation.
    (
        "harness/src/local/admission/poll_sweep/selected_history.rs",
        "::new_v4",
        1,
    ),
    ("stream/src/wasm/remote.rs", "::new_v4", 1),
    // The sole mount clock fallback is unreachable through a captured seeded clock.
    ("filesystem/src/native_mount/clock.rs", "SystemTime::now", 1),
    // Unseeded per-process hash keys. They vary only staged-batch probe
    // charges, never replay traces; seeding them is an open finding.
    ("filesystem/src/logical_objects/index.rs", "RandomState", 6),
];

// In-crate unit-test modules by repository naming convention.
fn test_only(relative: &str) -> bool {
    let mut parts = relative.rsplit('/');
    let name = parts.next().unwrap_or_default();
    name == "tests.rs"
        || name.ends_with("_tests.rs")
        || name.starts_with("tests_")
        || parts.any(|part| part == "tests" || part.ends_with("_tests"))
}

/// Compares direct source counts with the exact allowance, in both directions.
fn violations<'a>(files: impl IntoIterator<Item = (String, &'a str)>) -> Vec<String> {
    let mut found = BTreeMap::new();
    for (path, text) in files {
        for source in SOURCES {
            let count = text.matches(source).count();
            if count > 0 {
                found.insert((path.clone(), source), count);
            }
        }
    }
    let mut violations = Vec::new();
    for (path, source, count) in ALLOWED {
        match found.remove(&(path.to_owned(), source)) {
            Some(actual) if actual == count => (),
            actual => violations.push(format!(
                "{path}: {source} expected {count}, found {}",
                actual.unwrap_or(0)
            )),
        }
    }
    violations.extend(
        found
            .into_iter()
            .map(|((path, source), count)| format!("{path}: unseeded {source} x{count}")),
    );
    violations
}

// Both scans share one read of the tree.
fn sources() -> &'static [(String, String)] {
    static FILES: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();
    FILES.get_or_init(|| read_sources(&Path::new(env!("CARGO_MANIFEST_DIR")).join("..")))
}

fn read_sources(root: &Path) -> Vec<(String, String)> {
    let mut files = Vec::new();
    for name in CRATES {
        let mut pending = vec![root.join(name).join("src")];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory).expect("source directory") {
                let path = entry.expect("source entry").path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                if path.extension().is_none_or(|extension| extension != "rs") {
                    continue;
                }
                let relative = path
                    .strip_prefix(root)
                    .expect("crate source")
                    .to_str()
                    .expect("UTF-8 source path")
                    .replace('\\', "/");
                if !test_only(&relative) {
                    files.push((relative, std::fs::read_to_string(&path).expect("source")));
                }
            }
        }
    }
    files
}

#[test]
fn unseeded_sources_stay_behind_their_seams() {
    let files = sources();
    assert!(files.len() > 1000, "source scan lost its roots");
    let found = violations(
        files
            .iter()
            .map(|(path, text)| (path.clone(), text.as_str())),
    );
    assert!(found.is_empty(), "unseeded sources: {found:#?}");
}

#[test]
fn planted_unseeded_sources_fail_the_scan() {
    let files = sources();
    let scan = |planted: Option<(&str, &str)>| {
        if let Some((target, _)) = planted {
            assert!(
                files.iter().any(|(path, _)| path == target),
                "planted source target is absent: {target}"
            );
        }
        violations(files.iter().map(|(path, text)| match planted {
            Some((target, plant)) if path == target => (path.clone(), plant),
            _ => (path.clone(), text.as_str()),
        }))
    };
    // A new production file drawing host entropy directly.
    let planted = violations(
        files
            .iter()
            .map(|(path, text)| (path.clone(), text.as_str()))
            .chain([(
                "harness/src/planted.rs".into(),
                "let id = uuid::Uuid::new_v4();",
            )]),
    );
    assert_eq!(planted, ["harness/src/planted.rs: unseeded ::new_v4 x1"]);
    // A second direct read beside an allowed seam fallback.
    let identity = files
        .iter()
        .find(|(path, _)| path == "filesystem/src/identity.rs")
        .expect("filesystem identity seam");
    let doubled = format!(
        "{}\nstd::time::SystemTime::now();uuid::Uuid::now_v7()",
        identity.1
    );
    assert_eq!(
        scan(Some(("filesystem/src/identity.rs", &doubled))),
        [
            "filesystem/src/identity.rs: ::now_v7 expected 1, found 2",
            "filesystem/src/identity.rs: unseeded SystemTime::now x1",
        ]
    );
    // Restoring a routed wall-clock read in production admission code.
    let admission = "let now = std::time::SystemTime::now();";
    assert_eq!(
        scan(Some(("harness-core/src/store/admission.rs", admission))),
        ["harness-core/src/store/admission.rs: unseeded SystemTime::now x1"]
    );
    // The same reads passed as functions or through renamed types. Each form
    // escaped the earlier call-parenthesis patterns.
    for (plant, expected) in [
        (
            "let now = cached.unwrap_or_else(std::time::SystemTime::now);",
            "SystemTime::now",
        ),
        (
            "use std::time::SystemTime as Wall; let now = Wall::now();",
            "SystemTime as ",
        ),
        (
            "type Wall = std::time::SystemTime;",
            "= std::time::SystemTime;",
        ),
        ("use uuid::Uuid as Id; let id = Id::new_v4();", "Uuid as "),
        (
            "type Id = uuid::Uuid; let id = cached.unwrap_or_else(Id::new_v4);",
            "::new_v4",
        ),
        ("let fill = getrandom::fill;", "getrandom::fill"),
        (
            "let keys = std::collections::hash_map::RandomState::new();",
            "RandomState",
        ),
    ] {
        let found = scan(Some(("harness-core/src/store/admission.rs", plant)));
        assert!(
            found.contains(&format!(
                "harness-core/src/store/admission.rs: unseeded {expected} x1"
            )),
            "{plant}: {found:?}"
        );
    }
    // A removed seam leaves a stale allowance rather than silently passing.
    assert_eq!(
        scan(Some(("stream/src/lib/stream.rs", ""))),
        ["stream/src/lib/stream.rs: ::new_v4 expected 1, found 0"]
    );
    assert!(test_only("harness/src/client/tests/history.rs"));
    assert!(!test_only("harness/src/local/owner.rs"));
}

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

/// Every native mount source with a host clock read, and its exact count. Any
/// other file under native_mount, including a new callback helper, must read
/// time only through its provider's captured clock.
const MOUNT_CLOCK_ALLOWED: [(&str, usize); 6] = [
    // The seam: seeded reads, the test host override, and both host fallbacks.
    ("filesystem/src/native_mount/clock.rs", 4),
    // Seeded `clock.elapsed()`, else CLOCK_UPTIME_RAW outside simulation.
    ("filesystem/src/native_mount/darwin_mount/context.rs", 2),
    // Mount start/stop deadlines on the mounting thread; never kernel callbacks.
    ("filesystem/src/native_mount/darwin_mount/cleanup.rs", 7),
    ("filesystem/src/native_mount/darwin_mount/session.rs", 2),
    ("filesystem/src/native_mount/darwin_mount/startup.rs", 2),
    ("filesystem/src/native_mount/fuse/lifecycle.rs", 2),
];

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
    let files = sources();
    let mounts = files
        .iter()
        .filter(|(path, _)| path.starts_with("filesystem/src/native_mount/"))
        .count();
    assert!(mounts > 150, "mount clock scan lost its roots");
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
    // The previous detector (`Instant::now(`/`SystemTime::now(` over a fixed
    // callback file list) missed all but the first two forms.
    let previous = |text: &str| text.contains("Instant::now(") || text.contains("SystemTime::now(");
    let callback = "filesystem/src/native_mount/fuse/attributes.rs";
    let helper = "filesystem/src/native_mount/fuse/clock_helper.rs";
    let original = &files.iter().find(|(path, _)| path == callback).unwrap().1;
    let mut escaped_previous = 0;
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
        escaped_previous += usize::from(!previous(plant));
    }
    assert_eq!(escaped_previous, 7);
    // The seeded seam method stays legal; an extra lifecycle read is not.
    let seam = format!("{original}\nlet now = clock.now();");
    assert!(scan(Some((callback, &seam))).is_empty());
    let lifecycle = "filesystem/src/native_mount/fuse/lifecycle.rs";
    let text = &files.iter().find(|(path, _)| path == lifecycle).unwrap().1;
    assert_eq!(
        scan(Some((lifecycle, &format!("{text}\nInstant::now();")))),
        [format!("{lifecycle}: host clock reads expected 2, found 3")]
    );
    // Each provider constructor must capture time before handing callbacks to
    // its driver. Defaulting the field would compile but lose the seed.
    for path in [
        "filesystem/src/native_mount/adapter/runtime.rs",
        "filesystem/src/native_mount/fuse/core.rs",
        "filesystem/src/native_mount/darwin_mount/context.rs",
        "filesystem/src/native_mount/projfs/projector.rs",
    ] {
        let text = &files.iter().find(|(name, _)| name == path).unwrap().1;
        let captured = "clock: super::super::clock::Clock::capture(),";
        let seeded = |text: &str| text.matches(captured).count() == 1;
        assert!(seeded(text), "provider failed to capture its clock: {path}");
        assert!(!seeded(
            &text.replace(captured, "clock: Default::default(),")
        ));
    }
}

#[test]
fn fresh_transport_identities_do_not_copy_domain_keys() {
    // This is a structural allocation proof: the transport owns the generator's
    // Bytes directly. A domain key is only constructed for the domain consumer.
    let routes = [
        (
            include_str!("../../../stream/src/grpc/client.rs"),
            "crate::new_identity_bytes,",
            "|| Bytes::copy_from_slice(crate::new_idempotency_key().as_bytes()),",
        ),
        (
            include_str!("../../../stream/src/session/client.rs"),
            "identity: crate::new_identity_bytes(),",
            "identity: Bytes::copy_from_slice(crate::new_idempotency_key().as_bytes()),",
        ),
    ];
    let owns_generated_bytes =
        |text: &str, route: &str| text.contains(route) && !text.contains("new_idempotency_key()");
    for (source, route, copying_mutant) in routes {
        assert!(owns_generated_bytes(source, route));
        // Restoring either original call must fail the same allocation gate.
        let broken = source.replace(route, copying_mutant);
        assert!(!owns_generated_bytes(&broken, route));
    }
}
