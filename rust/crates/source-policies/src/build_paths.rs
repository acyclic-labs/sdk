//! A test runs where its build system puts it, not where it was compiled, and its
//! binary's directory may be read-only.
//!
//! - The compile-time manifest directory is valid only while compiling. At run time
//!   a source file is found through `acyclic_test_support::crate_path!` or
//!   `source_path`. The spelling is allowed only as the argument of an
//!   `include!`, `include_str!` or `include_bytes!`, which read at compile time.
//! - A temporary directory is never created beside the running executable.
use std::collections::BTreeMap;

/// The compile-time manifest directory, assembled so that this file never spells it.
fn manifest() -> String {
    format!("env!(\"{}\")", ["CARGO", "MANIFEST", "DIR"].join("_"))
}

/// Compile-time readers that may take the manifest directory.
const READERS: [&str; 3] = [
    "include!(concat!(",
    "include_str!(concat!(",
    "include_bytes!(concat!(",
];

/// Exact remaining run-time spellings: (file, count).
const ALLOWED: [(&str, usize); 3] = [
    // The fallback root for ordinary Cargo runs, and `crate_path!` itself.
    ("rust/crates/test-support/src/fixtures.rs", 2),
    // A compiler-policy fixture for admitted compile-time metadata; it is never run.
    (
        "rust/crates/harness/tests/ui/native-environment/src/lib.rs",
        1,
    ),
    // A wasm32 test: node imports the module from the tree the test compiled from.
    ("rust/crates/stream/tests/credential_javascript.rs", 1),
];

/// Run-time uses of the manifest directory in `text`.
fn manifest_uses(text: &str) -> usize {
    let needle = manifest();
    text.match_indices(&needle)
        .filter(|(at, _)| {
            let before: String = text[..*at]
                .chars()
                .rev()
                .filter(|c| !c.is_whitespace())
                .take(32)
                .collect();
            let before: String = before.chars().rev().collect();
            !READERS.iter().any(|reader| before.ends_with(reader))
        })
        .count()
}

/// `tempdir_in` calls whose argument names the running executable.
fn executable_tempdirs(text: &str) -> usize {
    let call = "current_exe()";
    // Names bound to the running executable's path, as in `let name = ...current_exe()`.
    let mut names = vec!["current_exe".to_owned()];
    for (at, _) in text.match_indices(call) {
        let line = text[..at].rsplit('\n').next().unwrap_or_default();
        if let Some(binding) = line.trim_start().strip_prefix("let ") {
            let name: String = binding
                .trim_start_matches("mut ")
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                names.push(name);
            }
        }
    }
    text.match_indices("tempdir_in(")
        .filter(|(at, open)| {
            let mut depth = 1;
            let argument: String = text[at + open.len()..]
                .chars()
                .take_while(|c| {
                    depth += match c {
                        '(' => 1,
                        ')' => -1,
                        _ => 0,
                    };
                    depth > 0
                })
                .collect();
            names.iter().any(|name| {
                argument.match_indices(name.as_str()).any(|(start, _)| {
                    let word = |c: char| c.is_alphanumeric() || c == '_';
                    !argument[..start].ends_with(word)
                        && !argument[start + name.len()..].starts_with(word)
                })
            })
        })
        .count()
}

/// Compares both rules with their exact allowance, in both directions.
fn violations<'a>(files: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<String> {
    let mut found = BTreeMap::new();
    let mut violations = Vec::new();
    for (path, text) in files {
        let count = manifest_uses(text);
        if count > 0 {
            found.insert(path, count);
        }
        let count = executable_tempdirs(text);
        if count > 0 {
            violations.push(format!(
                "{path}: temporary directory beside the executable x{count}"
            ));
        }
    }
    for (path, count) in ALLOWED {
        match found.remove(path) {
            Some(actual) if actual == count => (),
            actual => violations.push(format!(
                "{path}: manifest directory expected {count}, found {}",
                actual.unwrap_or(0)
            )),
        }
    }
    violations.extend(
        found
            .into_iter()
            .map(|(path, count)| format!("{path}: run-time manifest directory x{count}")),
    );
    violations
}

fn sources() -> Vec<(&'static str, &'static str)> {
    crate::repository_rust_sources()
        .iter()
        .copied()
        .filter(|(path, _)| *path != "rust/crates/source-policies/src/build_paths.rs")
        .collect()
}

#[test]
fn tests_find_sources_and_temporary_directories_where_they_run() {
    let files = sources();
    assert!(files.len() > 3000, "source scan lost its roots");
    let found = violations(files);
    assert!(found.is_empty(), "build-location paths: {found:#?}");
}

#[test]
fn planted_build_location_paths_fail_the_scan() {
    let target = "rust/crates/markdown/tests/whitespace.rs";
    let fixtures = "rust/crates/test-support/src/fixtures.rs";
    let files = sources()
        .into_iter()
        .filter(|(path, _)| *path == target || ALLOWED.iter().any(|(allowed, _)| path == allowed))
        .collect::<Vec<_>>();
    assert!(
        files.iter().any(|(path, _)| *path == target),
        "planted source target is absent"
    );
    let scan = |target: &str, plant: &str| {
        violations(files.iter().map(|&(path, text)| {
            if path == target {
                (path, plant)
            } else {
                (path, text)
            }
        }))
    };
    let manifest = manifest();
    for plant in [
        format!("let root = Path::new({manifest});"),
        format!("let file = concat!({manifest}, \"/tests/x.txt\");"),
        format!("let x = option_{manifest};"),
    ] {
        assert_eq!(
            scan(target, &plant),
            [format!("{target}: run-time manifest directory x1")],
            "{plant}"
        );
    }
    for plant in [
        format!("include_str!(concat!({manifest}, \"/x.txt\"))"),
        format!("include_bytes!(concat!(\n    {manifest},\n    \"/x.bin\"\n))"),
    ] {
        assert!(scan(target, &plant).is_empty(), "{plant}");
    }
    for plant in [
        "tempfile::tempdir_in(std::env::current_exe().unwrap().parent().unwrap())",
        "let executable = std::env::current_exe()?;\nlet d = Builder::new().tempdir_in(executable.parent().expect(\"parent\"));",
        "let mut exe = env::current_exe()?; tempfile::tempdir_in(exe.parent().unwrap())",
    ] {
        assert_eq!(
            scan(target, plant),
            [format!(
                "{target}: temporary directory beside the executable x1"
            )],
            "{plant}"
        );
    }
    for plant in [
        "let executable = std::env::current_exe()?; let d = tempfile::tempdir_in(base)?;",
        "let executable_dir = x; tempfile::tempdir_in(executable_dir)",
    ] {
        assert!(scan(target, plant).is_empty(), "{plant}");
    }
    // A removed allowance is stale rather than silently passing.
    assert!(scan(fixtures, "").contains(&format!(
        "{fixtures}: manifest directory expected 2, found 0"
    )));
}
