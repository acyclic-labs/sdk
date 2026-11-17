//! Product code never special-cases a workspace entry by name. A workspace
//! holds whatever its user put there: `.git`, `target/`, `Cargo.lock` and
//! `.env` are ordinary files, captured, forked and merged like any other.
//!
//! The one exception is Git ignore rules, read by the `acyclic git` facade
//! and the filesystem's Git compatibility layer and applied by their rules.
//! The materializer's private operation directory also names its own staging
//! subtrees; those names never address a workspace entry.
//!
//! The scan covers each crate's product sources, everything under `src`
//! outside test files and inline test modules. Repository build tooling and
//! qualification fixtures inspect this repository itself, not a workspace.
use std::collections::BTreeMap;

/// Workspace-name spellings: literals, and the bare cache name as a path.
const SPELLINGS: [&str; 14] = [
    "\".git\"",
    "\"/.git\"",
    "\".gitignore\"",
    "\"/.gitignore\"",
    "\"Cargo.lock\"",
    "\".env\"",
    "\"node_modules\"",
    "\"/target\"",
    "\"target/\"",
    "\"vendor\"",
    "join(\"target\")",
    "Path::new(\"target\")",
    "OsStr::new(\"target\")",
    "is_named(\"target\")",
];

/// Exact remaining spellings: (file, spelling, count).
const ALLOWED: [(&str, &str, usize); 12] = [
    // Git ignore rules, applied by the Git compatibility layer.
    (
        "rust/crates/filesystem/src/git_compat/capture/incremental.rs",
        "\".gitignore\"",
        1,
    ),
    (
        "rust/crates/filesystem/src/git_compat/capture/traversal.rs",
        "\"/.gitignore\"",
        1,
    ),
    (
        "rust/crates/filesystem/src/git_compat/capture/traversal.rs",
        "\".gitignore\"",
        1,
    ),
    // Git ignore rules, applied by the `acyclic git` facade.
    (
        "rust/crates/harness-core/src/filesystem/agent_publication.rs",
        "\"/.gitignore\"",
        1,
    ),
    (
        "rust/crates/harness-core/src/filesystem/workspace_policy.rs",
        "\"/.gitignore\"",
        1,
    ),
    // The materializer's staging subtree in its private operation directory.
    (
        "rust/crates/filesystem/src/materializer/backend.rs",
        "Path::new(\"target\")",
        2,
    ),
    (
        "rust/crates/filesystem/src/materializer/directory_preparation.rs",
        "Path::new(\"target\")",
        1,
    ),
    (
        "rust/crates/filesystem/src/materializer/directory_preparation.rs",
        "is_named(\"target\")",
        1,
    ),
    (
        "rust/crates/filesystem/src/materializer/publication.rs",
        "Path::new(\"target\")",
        1,
    ),
    (
        "rust/crates/harness-core/src/filesystem/workspace_publication.rs",
        "join(\"target\")",
        1,
    ),
    (
        "rust/crates/harness-core/src/tools/git/materialize.rs",
        "join(\"target\")",
        1,
    ),
    // A Windows test filesystem helper compiled only for tests.
    (
        "rust/crates/filesystem/src/windows_test_filesystems.rs",
        "join(\"target\")",
        1,
    ),
];

/// Crates whose sources inspect this repository rather than a workspace.
const REPOSITORY_TOOLING: [&str; 5] = [
    "conformance",
    "proto-codegen",
    "sdk-docs",
    "sdk-generation",
    "source-policies",
];

/// The product part of a crate source, or `None` for a test-only file.
fn product(path: &str) -> Option<&str> {
    let relative = path.strip_prefix("rust/crates/")?;
    let (krate, rest) = relative.split_once('/')?;
    let rest = rest.strip_prefix("src/")?;
    if REPOSITORY_TOOLING.contains(&krate) || krate.contains("test") || krate.ends_with("-fuzz") {
        return None;
    }
    let mut parts = rest.rsplit('/');
    let name = parts.next()?;
    if parts.any(|part| part == "tests" || part.ends_with("_tests") || part == "test_support")
        || name == "tests.rs"
        || name.ends_with("_tests.rs")
        || name.starts_with("tests_")
        || name == "test_support.rs"
    {
        return None;
    }
    Some(path)
}

/// Drops each inline test module: a module whose `#[cfg(..)]` predicate
/// requires a test build, through the formatted `}` at that item's
/// indentation. Product code around it is kept, including modules gated on
/// `not(test)` or on `test` as one alternative. Out-of-line test modules are
/// excluded by path.
fn without_inline_tests(text: &str) -> String {
    let mut kept = String::with_capacity(text.len());
    let mut attributes = String::new();
    let mut test_only = false;
    let mut closing = None;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_end();
        let item = trimmed.trim_start();
        if let Some(end) = &closing {
            if trimmed == end {
                closing = None;
            }
        } else if item.starts_with("#[") {
            test_only |= item
                .strip_prefix("#[cfg(")
                .and_then(|rest| rest.strip_suffix(")]"))
                .is_some_and(requires_test);
            attributes.push_str(line);
        } else if test_only && item.starts_with("mod ") && trimmed.ends_with('{') {
            let indent = &trimmed[..trimmed.len() - item.len()];
            closing = Some(format!("{indent}}}"));
            attributes.clear();
            test_only = false;
        } else {
            kept.push_str(&attributes);
            kept.push_str(line);
            attributes.clear();
            test_only = false;
        }
    }
    kept.push_str(&attributes);
    kept
}

/// Whether a `cfg` predicate holds only in a test build: `test` or the
/// `test-support` feature, a conjunction with such a member, or a disjunction
/// whose every member is one. `not(..)` never requires a test build.
fn requires_test(predicate: &str) -> bool {
    let predicate = predicate.trim();
    if let Some(members) = predicate
        .strip_prefix("all(")
        .and_then(|p| p.strip_suffix(')'))
    {
        return split_members(members).any(requires_test);
    }
    if let Some(members) = predicate
        .strip_prefix("any(")
        .and_then(|p| p.strip_suffix(')'))
    {
        return split_members(members).all(requires_test);
    }
    predicate == "test" || predicate == "feature = \"test-support\""
}

/// The top-level comma-separated members of a `cfg` predicate list.
fn split_members(members: &str) -> impl Iterator<Item = &str> {
    let mut depth = 0usize;
    let mut start = 0;
    let mut parts = Vec::new();
    for (index, byte) in members.bytes().enumerate() {
        match byte {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(&members[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&members[start..]);
    parts.into_iter().filter(|part| !part.trim().is_empty())
}

/// Compares workspace-name counts with the exact allowance, in both directions.
fn violations<'a>(files: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<String> {
    let mut found = BTreeMap::new();
    for (path, text) in files {
        let text = without_inline_tests(text);
        for spelling in SPELLINGS {
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
            .map(|((path, spelling), count)| format!("{path}: workspace name {spelling} x{count}")),
    );
    violations
}

fn sources() -> Vec<(&'static str, &'static str)> {
    crate::repository_rust_sources()
        .iter()
        .filter_map(|&(path, text)| product(path).map(|path| (path, text)))
        .collect()
}

#[test]
fn product_code_names_no_workspace_entry() {
    let files = sources();
    assert!(files.len() > 1000, "source scan lost its roots");
    let found = violations(files.iter().copied());
    assert!(
        found.is_empty(),
        "workspace names in product code: {found:#?}"
    );
}

#[test]
fn planted_workspace_names_fail_the_scan() {
    let target = "rust/crates/harness-local/src/local/workspace.rs";
    let facade = "rust/crates/harness-core/src/filesystem/workspace_policy.rs";
    let files = sources()
        .into_iter()
        .filter(|(path, _)| {
            *path == target
                || *path == facade
                || ALLOWED.iter().any(|(allowed, _, _)| path == allowed)
        })
        .collect::<Vec<_>>();
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
    assert!(scan(target, "").is_empty(), "the control scan must pass");
    for (plant, spelling) in [
        (
            "const SOURCE_EXCLUDED_ROOTS: &[&str] = &[\".git\"];",
            "\".git\"",
        ),
        ("let lock = root.join(\"Cargo.lock\");", "\"Cargo.lock\""),
        ("if name == \".env\" { continue; }", "\".env\""),
        ("watch.exclude(root.join(\"target\"));", "join(\"target\")"),
        ("Path::new(\"target\")", "Path::new(\"target\")"),
        ("view.optional(\"/.gitignore\", 1)", "\"/.gitignore\""),
        ("skip(\"node_modules\")", "\"node_modules\""),
    ] {
        let found = scan(target, plant);
        assert!(
            found.contains(&format!("{target}: workspace name {spelling} x1")),
            "{plant}: {found:?}"
        );
    }
    // A test module at the end of a product file is not product code, but a
    // name before it is.
    assert!(
        scan(
            target,
            "fn f() {}\n#[cfg(test)]\nmod tests {\n\".env\"\n}\n"
        )
        .is_empty()
    );
    assert!(!scan(target, "\".env\"\n#[cfg(test)]\nmod tests {\n}\n").is_empty());
    // Modules that need a test build are skipped, whatever else they require.
    for gate in [
        "all(test, not(loom))",
        "any(test, feature = \"test-support\")",
        "all(any(test, feature = \"test-support\"), unix)",
    ] {
        let plant = format!("#[cfg({gate})]\nmod tests {{\n\".env\"\n}}\n");
        assert!(scan(target, &plant).is_empty(), "{plant}");
    }
    // Product code after a test module, nested or not, is still scanned, and
    // so is a module built without tests or with tests as one alternative.
    for plant in [
        "#[cfg(test)]\nmod tests {\n}\nfn f() -> &'static str { \".env\" }\n",
        "mod outer {\n    #[cfg(test)]\n    mod tests {\n    }\n    fn f() -> &'static str { \".env\" }\n}\n",
        "#[cfg(not(test))]\nmod production {\n    fn f() -> &'static str { \".env\" }\n}\n",
        "#[cfg(any(test, unix))]\nmod native {\n    fn f() -> &'static str { \".env\" }\n}\n",
        "#[cfg(all(unix, not(any(test, loom))))]\nmod native {\n    fn f() -> &'static str { \".env\" }\n}\n",
    ] {
        let found = scan(target, plant);
        assert!(
            found.contains(&format!("{target}: workspace name \".env\" x1")),
            "{plant}: {found:?}"
        );
    }
    // A removed allowance is stale rather than silently passing.
    assert!(scan(facade, "").contains(&format!("{facade}: \"/.gitignore\" expected 1, found 0")));
}
