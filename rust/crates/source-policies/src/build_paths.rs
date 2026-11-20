//! A test runs where its build system puts it, not where it was compiled, and its
//! binary's directory may be read-only.
//!
//! - The compile-time manifest directory is valid only while compiling. At run time
//!   a source file is found through `acyclic_test_support::crate_path!` or
//!   `source_path`. The spelling is allowed only as the argument of an
//!   `include!`, `include_str!` or `include_bytes!`, which read at compile time.
//! - A temporary directory is never created beside the running executable or in the
//!   working directory, which a runner may set to a read-only checkout.
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
    // The fixtures resolve the source root at run time; no compile-time manifest
    // directory remains, so the allowance is zero.
    ("rust/crates/test-support/src/fixtures.rs", 0),
    // A compiler-policy fixture for admitted compile-time metadata; it is never run.
    (
        "rust/crates/harness/tests/ui/native-environment/src/lib.rs",
        1,
    ),
    // A wasm32 test: node imports the module from the tree the test compiled from.
    ("rust/crates/stream/tests/credential_javascript.rs", 1),
];

/// `text` with Rust's insignificant spacing and comments removed: whitespace survives only as
/// one space between two word characters, and a comment counts as whitespace, so each token
/// sequence has one spelling. String and character literals are kept whole, so a `//` inside
/// one starts no comment.
fn squeeze(text: &str) -> String {
    let word = |c: char| c.is_alphanumeric() || c == '_';
    let mut squeezed = String::with_capacity(text.len());
    let mut spaced = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match (c, chars.peek()) {
            ('/', Some(&'/')) => {
                chars.by_ref().take_while(|c| *c != '\n').for_each(drop);
                spaced = true;
                continue;
            }
            ('/', Some(&'*')) => {
                chars.next();
                let (mut depth, mut last) = (1, ' ');
                for c in chars.by_ref() {
                    match (last, c) {
                        ('/', '*') => (depth, last) = (depth + 1, ' '),
                        ('*', '/') => (depth, last) = (depth - 1, ' '),
                        _ => last = c,
                    }
                    if depth == 0 {
                        break;
                    }
                }
                spaced = true;
                continue;
            }
            _ if c.is_whitespace() => {
                spaced = true;
                continue;
            }
            _ => {}
        }
        if spaced && squeezed.ends_with(word) && word(c) {
            squeezed.push(' ');
        }
        spaced = false;
        squeezed.push(c);
        // A string or character literal is copied through its closing quote. A lifetime has
        // no closing quote within two characters and is copied as written.
        if c == '"' {
            let before = &squeezed[..squeezed.len() - 1];
            let stem = before.trim_end_matches('#');
            let (opened, hashes) = (squeezed.len(), before.len() - stem.len());
            let raw = matches!(
                &stem[stem.trim_end_matches(word).len()..],
                "r" | "br" | "cr"
            );
            if raw {
                // A raw string ends at a quote followed by its own number of hashes.
                let close = format!("\"{}", "#".repeat(hashes));
                for c in chars.by_ref() {
                    squeezed.push(c);
                    if squeezed.len() > opened + hashes && squeezed.ends_with(&close) {
                        break;
                    }
                }
                continue;
            }
        }
        let quote = match c {
            '"' => Some('"'),
            '\'' if chars.clone().take(3).any(|c| c == '\'') || chars.peek() == Some(&'\\') => {
                Some('\'')
            }
            _ => None,
        };
        if let Some(quote) = quote {
            let mut escaped = false;
            for c in chars.by_ref() {
                squeezed.push(c);
                match c {
                    _ if escaped => escaped = false,
                    '\\' => escaped = true,
                    _ if c == quote => break,
                    _ => {}
                }
            }
        }
    }
    squeezed
}

/// Run-time uses of the manifest directory in `text`.
fn manifest_uses(text: &str) -> usize {
    let text = squeeze(text);
    text.match_indices(&manifest())
        .filter(|(at, _)| !READERS.iter().any(|reader| text[..*at].ends_with(reader)))
        .count()
}

/// A relative path literal, bare or as `Path::new`/`PathBuf::from`: it resolves against the
/// working directory, which a test runner may set to the checkout.
fn relative_literal(argument: &str) -> bool {
    let argument = argument.strip_prefix('&').unwrap_or(argument);
    let inner = [
        "std::path::Path::new(",
        "Path::new(",
        "std::path::PathBuf::from(",
        "PathBuf::from(",
    ]
    .iter()
    .find_map(|call| argument.strip_prefix(call)?.strip_suffix(')'))
    .unwrap_or(argument);
    inner
        .strip_prefix('"')
        .and_then(|literal| literal.strip_suffix('"'))
        .is_some_and(|path| !path.contains('"') && !path.starts_with('/'))
}

/// Temporary directories created inside the build or checkout tree: a `tempdir_in` or
/// `TempDir::new_in` whose argument names the running executable or the working
/// directory, or is a relative path literal.
fn tree_tempdirs(text: &str) -> usize {
    let text = squeeze(text);
    let word = |c: char| c.is_alphanumeric() || c == '_';
    // Names bound to either path, as in `let name = ...current_exe()`.
    let mut names = vec!["current_exe".to_owned(), "current_dir".to_owned()];
    for (at, _) in text
        .match_indices("current_exe()")
        .chain(text.match_indices("current_dir()"))
    {
        let statement = &text[text[..at].rfind([';', '{', '}']).map_or(0, |end| end + 1)..at];
        // Attributes may precede the `let`; comments are already gone.
        let binding = statement
            .match_indices("let ")
            .find(|(start, _)| !statement[..*start].ends_with(word))
            .map(|(start, keyword)| &statement[start + keyword.len()..]);
        if let Some(binding) = binding {
            let binding = binding.strip_prefix("mut ").unwrap_or(binding);
            let name: String = binding.chars().take_while(|c| word(*c)).collect();
            if !name.is_empty() {
                names.push(name);
            }
        }
    }
    text.match_indices("tempdir_in(")
        .chain(text.match_indices("TempDir::new_in("))
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
            relative_literal(&argument)
                || names.iter().any(|name| {
                    argument.match_indices(name.as_str()).any(|(start, _)| {
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
        let count = tree_tempdirs(text);
        if count > 0 {
            violations.push(format!(
                "{path}: temporary directory in the build or checkout tree x{count}"
            ));
        }
    }
    for (path, count) in ALLOWED {
        let actual = found.remove(path).unwrap_or(0);
        if actual != count {
            violations.push(format!(
                "{path}: manifest directory expected {count}, found {actual}"
            ));
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
        // Spacing that Rust ignores is no way around the rule.
        format!(
            "let root = Path::new({});",
            manifest.replace("!(", " ! (\n    ").replace("\")", "\" )")
        ),
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
        format!("// Not {manifest} at run time."),
    ] {
        assert!(scan(target, &plant).is_empty(), "{plant}");
    }
    for plant in [
        "tempfile::tempdir_in(std::env::current_exe().unwrap().parent().unwrap())",
        "let executable = std::env::current_exe()?;\nlet d = Builder::new().tempdir_in(executable.parent().expect(\"parent\"));",
        "let mut exe = env::current_exe()?; tempfile::tempdir_in(exe.parent().unwrap())",
        "let executable =\n    std::env::current_exe()?;\ntempfile::tempdir_in (\n    executable.parent().unwrap())",
        // Comments and attributes before the binding decide nothing.
        "// The running binary.\nlet executable = std::env::current_exe()?;\ntempfile::tempdir_in(executable.parent().unwrap())",
        "/* the binary; its */ let executable = std::env::current_exe()?;\ntempfile::tempdir_in(executable.parent().unwrap())",
        "#[cfg(unix)]\nlet executable = std::env::current_exe()?;\ntempfile::tempdir_in(executable.parent().unwrap())",
        "let quote = '\"'; let raw = r#\"\" // \"#; let executable = std::env::current_exe()?;
tempfile::tempdir_in(executable.parent().unwrap())",
        "let url = \"https://x\"; let executable = std::env::current_exe()?;\ntempfile::tempdir_in(executable.parent().unwrap())",
        // The working directory may be the read-only checkout.
        "let root = tempfile::tempdir_in(\".\").map_err(storage)?;",
        "let d = tempfile::tempdir_in(std::env::current_dir()?)?;",
        "let cwd = std::env::current_dir()?; tempfile::TempDir::new_in(&cwd)",
        "Builder::new().tempdir_in(Path::new(\"target/tmp\"))",
        "tempfile::tempdir_in(&PathBuf::from(\"scratch\"))",
    ] {
        assert_eq!(
            scan(target, plant),
            [format!(
                "{target}: temporary directory in the build or checkout tree x1"
            )],
            "{plant}"
        );
    }
    for plant in [
        "let executable = std::env::current_exe()?; let d = tempfile::tempdir_in(base)?;",
        "let executable_dir = x; tempfile::tempdir_in(executable_dir)",
        // An absolute or computed root is outside the tree.
        "tempfile::tempdir_in(\"/dev/shm\")",
        "tempfile::tempdir_in(format!(\"{}/x\", root))",
        "let current_directory = x; tempfile::tempdir_in(current_directory)",
        // A commented-out use is no use.
        "// tempfile::tempdir_in(std::env::current_exe()?.parent().unwrap())",
    ] {
        assert!(scan(target, plant).is_empty(), "{plant}");
    }
    // A use in a file whose allowance is zero is a violation, not a silent pass.
    assert_eq!(
        scan(fixtures, &format!("let root = Path::new({manifest});")),
        [format!(
            "{fixtures}: manifest directory expected 0, found 1"
        )]
    );
}
