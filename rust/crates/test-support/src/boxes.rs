//! Finds heap moves on budgeted paths that state no reason.
//!
//! A source file is budgeted when it names a live allocation budget. Every
//! heap move in it is made through a charged box, or is preceded within three
//! lines by an `// Unbudgeted:` or a `// Charged above:` comment that says why
//! it needs no charge. A function that returns a fresh `Box<T>` (one that is
//! not pinned and not a trait object) is a heap move too: deserializing,
//! cloning or converting into it allocates with no call to `Box::new` in
//! sight, so its signature carries the reason.

use std::path::Path;

/// Names that mark a source file as holding a live allocation budget.
pub const BUDGET_MARKERS: [&str; 7] = [
    "PreparationIo",
    "HistoryIo",
    "OwnedShared",
    "BoundedOperationFuture",
    "ChargedBox",
    "charged_box",
    "charge_box",
];

/// Calls that move a value or a frame to the heap.
///
/// `traced(` is harness-core's span wrapper, which builds its frame through
/// `in_heap` on the caller's behalf.
const HEAP_CALLS: [&str; 8] = [
    "Box::new(",
    "Box::pin(",
    "(Box::new)",
    "Box::from(",
    "Box::default(",
    "into_boxed_",
    "in_heap(",
    "traced(",
];

fn identifier(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

/// Whether `code` names `token` with no identifier character before it, so
/// `ChargedBox::new(` is not `Box::new(`.
fn names(code: &str, token: &str) -> bool {
    // A token that opens with punctuation, like `(Box::new)`, is whole as found.
    let bounded = token.chars().next().is_some_and(identifier);
    code.match_indices(token).any(|(at, _)| {
        !bounded
            || !code
                .get(..at)
                .and_then(|before| before.chars().last())
                .is_some_and(identifier)
    })
}

/// Whether the return type on `code` is a fresh box: `Box<T>` that is neither
/// `Pin<Box<..>>` nor `Box<dyn ..>`, each of which is made by a call above.
fn returns_box(code: &str) -> bool {
    let Some((_, returned)) = code.split_once("->") else {
        return false;
    };
    returned.match_indices("Box<").any(|(at, _)| {
        let before = returned.get(..at).unwrap_or_default();
        let after = returned.get(at + "Box<".len()..).unwrap_or_default();
        !before.chars().last().is_some_and(identifier)
            && !before.ends_with("Pin<")
            && !after.starts_with("dyn ")
    })
}

fn justified(lines: &[&str]) -> bool {
    lines
        .iter()
        .any(|line| line.contains("// Unbudgeted:") || line.contains("// Charged above:"))
}

/// Whether `line` is an attribute that builds the next item only under test.
fn test_attribute(line: &str) -> bool {
    let line = line.trim();
    line == "#[cfg(test)]" || line.starts_with("#[cfg(all(test,")
}

/// The lines of `source` with every item under a test attribute blanked, so
/// line numbers hold and production code after a test item is still read.
fn production(source: &str) -> Vec<&str> {
    let lines: Vec<&str> = source.lines().collect();
    let mut kept = lines.clone();
    let mut at = 0;
    while at < lines.len() {
        if !test_attribute(lines[at]) {
            at += 1;
            continue;
        }
        let mut depth = 0_i64;
        let mut opened = false;
        while let Some(line) = lines.get(at) {
            let code = line.split("//").next().unwrap_or_default();
            let opens = code.matches('{').count() as i64;
            depth += opens - code.matches('}').count() as i64;
            opened |= opens != 0;
            kept[at] = "";
            at += 1;
            let code = code.trim();
            if !code.starts_with("#[")
                && ((opened && depth <= 0) || (!opened && code.ends_with(';')))
            {
                break;
            }
        }
    }
    kept
}

/// The one-based lines of `source`, outside its test items, that make a raw
/// box or heap frame without a stated reason.
#[must_use]
pub fn unpaid(source: &str) -> Vec<usize> {
    let lines = production(source);
    let mut unpaid = Vec::new();
    let mut signature = 0;
    for (number, line) in lines.iter().enumerate() {
        let code = line.split("//").next().unwrap_or_default();
        if names(code, "fn ") {
            signature = number;
        }
        let near = lines
            .get(number.saturating_sub(3)..number)
            .unwrap_or_default();
        if HEAP_CALLS.iter().any(|call| names(code, call)) && !justified(near) {
            unpaid.push(number + 1);
        } else if returns_box(code) {
            // A signature's reason sits above its `fn`, not above its return type.
            let above = lines
                .get(signature.saturating_sub(3)..signature)
                .unwrap_or_default();
            if !justified(near) && !justified(above) {
                unpaid.push(number + 1);
            }
        }
    }
    unpaid
}

/// Fails with every unpaid line of each named source.
///
/// # Panics
///
/// When any source makes an unpaid box.
pub fn assert_all_paid(sources: &[(&str, &str)]) {
    let unpaid: Vec<(&str, Vec<usize>)> = sources
        .iter()
        .map(|(name, source)| (*name, unpaid(source)))
        .filter(|(_, lines)| !lines.is_empty())
        .collect();
    assert!(
        unpaid.is_empty(),
        "boxes with no charge or reason: {unpaid:?}"
    );
}

/// Whether `source`, outside its test items, names a live budget.
#[must_use]
pub fn budgeted(source: &str) -> bool {
    production(source)
        .iter()
        .any(|line| BUDGET_MARKERS.iter().any(|marker| names(line, marker)))
}

/// The modules `source` declares under a test attribute, which build only
/// under test.
fn test_modules(source: &str) -> Vec<String> {
    let mut modules = Vec::new();
    let mut under_test = false;
    for line in source.lines() {
        let line = line.trim();
        if line.starts_with("#[") {
            under_test |= test_attribute(line);
            continue;
        }
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        if under_test
            && let Some(declared) = line
                .trim_start_matches("pub(crate) ")
                .trim_start_matches("pub(super) ")
                .trim_start_matches("pub ")
                .strip_prefix("mod ")
                .and_then(|declared| declared.strip_suffix(';'))
        {
            modules.push(declared.trim().to_owned());
        }
        under_test = false;
    }
    modules
}

/// Every source file under `root` that builds outside tests, with its path
/// relative to `root` and its text. Test directories, `*tests.rs` files and
/// modules declared after a `#[cfg(test)]` are left out.
///
/// # Panics
///
/// When `root` cannot be read.
#[must_use]
pub fn sources(root: &Path) -> Vec<(String, String)> {
    let mut pending = vec![root.to_path_buf()];
    let mut all = Vec::new();
    while let Some(directory) = pending.pop() {
        let entries = std::fs::read_dir(&directory)
            .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()));
        for entry in entries {
            let path = entry.expect("source directory entry").path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            if path.is_dir() {
                if name != "tests" && !name.ends_with("_tests") {
                    pending.push(path);
                }
            } else if name.ends_with(".rs") && !name.ends_with("tests.rs") {
                let source = std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
                all.push((path, source));
            }
        }
    }
    // A module's children sit beside `mod.rs`, `lib.rs` or `main.rs`, and in
    // the directory named for any other file.
    let mut excluded = Vec::new();
    for (path, source) in &all {
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default();
        let base = if matches!(stem, "mod" | "lib" | "main") {
            path.parent().map(Path::to_path_buf).unwrap_or_default()
        } else {
            path.with_extension("")
        };
        excluded.extend(
            test_modules(source)
                .into_iter()
                .map(|module| base.join(module)),
        );
    }
    let mut found: Vec<(String, String)> = all
        .into_iter()
        .filter(|(path, _)| {
            !excluded
                .iter()
                .any(|module| path.with_extension("") == *module || path.starts_with(module))
        })
        .map(|(path, source)| {
            let relative = path.strip_prefix(root).unwrap_or(&path);
            (relative.display().to_string(), source)
        })
        .collect();
    found.sort();
    found
}

/// Fails with every unpaid line of `sources`, and when fewer than
/// `at_least` are given, so a moved tree cannot pass by scanning nothing.
///
/// # Panics
///
/// When any source makes an unpaid box, or too few are given.
pub fn assert_sources_paid(sources: &[(String, String)], at_least: usize) {
    assert!(
        sources.len() >= at_least,
        "only {} sources scanned for unpaid boxes",
        sources.len()
    );
    let named: Vec<(&str, &str)> = sources
        .iter()
        .map(|(name, source)| (name.as_str(), source.as_str()))
        .collect();
    assert_all_paid(&named);
}

#[cfg(test)]
mod tests {
    use super::{budgeted, test_modules, unpaid};

    #[test]
    fn only_modules_under_a_test_attribute_are_test_modules() {
        let source = "#[cfg(test)]
mod tests;
mod host;
#[cfg(test)]
#[cfg(loom)]
pub(crate) mod models;
";
        assert_eq!(
            test_modules(source),
            vec!["tests".to_owned(), "models".to_owned()]
        );
    }

    #[test]
    fn rejects_each_raw_form_and_accepts_each_reason() {
        for raw in [
            "    let value = Box::new(value);",
            "    future: Box::pin(build()),",
            "    .map(Box::new)",
            "    acyclic_fs::in_heap(|| run()).await",
            "    let value = std::boxed::Box::new(value);",
            "    let bytes: Box<[u8]> = Box::from(slice);",
            "    let bytes = vec.into_boxed_slice();",
            "fn decode(value: Value) -> Result<Box<Request>> {",
        ] {
            assert_eq!(unpaid(raw), vec![1], "{raw}");
        }
        assert!(unpaid("    ChargedBox::new(Some(io), value)?").is_empty());
        assert!(unpaid("    // Unbudgeted: nothing is live.\n    Box::new(value)").is_empty());
        assert!(unpaid("    // Charged above: frame.\n\n    Box::pin(f)").is_empty());
        assert!(unpaid("fn a() {}\n#[cfg(test)]\nfn b() { Box::new(1); }").is_empty());
        assert!(unpaid("fn f() -> Pin<Box<dyn Future>> {").is_empty());
        assert!(unpaid("fn f() -> Result<Box<dyn ProjectJoinPlan>> {").is_empty());
        assert!(unpaid("fn f() -> ChargedBox<Request> {").is_empty());
        assert!(
            unpaid(
                "// Unbudgeted: the budget is made here.\nfn decode(\n    a: u8,\n    b: u8,\n) -> Result<Box<Request>> {"
            )
            .is_empty()
        );
    }

    #[test]
    fn a_named_budget_marks_the_file() {
        assert!(budgeted("fn run(io: Option<&PreparationIo>) {}"));
        assert!(budgeted("use crate::charged_box::ChargedBox;"));
        assert!(!budgeted(
            "fn run() {}\n#[cfg(test)]\nfn t(io: PreparationIo) {}"
        ));
        assert!(!budgeted("fn run(io: Option<&MyPreparationIo>) {}"));
    }
}
