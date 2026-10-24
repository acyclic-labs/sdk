//! Every production process start in a crate that links into a host goes
//! through [`super::Command`], so no Windows child inherits a host handle
//! it was not given.
//!
//! Coverage is computed, not listed: a workspace crate is covered when it
//! depends on this crate, or when a covered crate depends on it, so a new
//! host crate is covered the moment it is linked. The remaining crates are
//! build and test tooling that never runs inside a host, and must match
//! [`EXEMPT`] exactly, so a crate leaving coverage fails here too.
//!
//! Production code is what a crate's library and binary targets compile
//! outside tests: modules and items under a `cfg` that implies `test` are
//! skipped, as is the workspace's `test-support` fixture feature that only
//! tests and test tooling enable, and comments and string literals. Tests and tooling may use
//! `std` directly, since they never start a host's children. The scan
//! locates the workspace from the working directory, never from this
//! crate's build-time path, so it scans whatever checkout it runs in.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// The crate that owns the command type.
const OWNER: &str = "acyclic-native-runtime";

/// Workspace crates that no host links: build, generation and test tooling.
const EXEMPT: [&str; 7] = [
    "acyclic-auth-wasm",
    "acyclic-inference",
    "acyclic-inference-wasm",
    "acyclic-machines-daytona",
    "acyclic-machines-wasm",
    "acyclic-test-support",
    "sdk-proto-codegen",
];

/// Spellings that start a process other than through the command type.
const STARTS: [&str; 8] = [
    "process::Command",
    "process::*",
    "process as ",
    "CreateProcess",
    "ShellExecute",
    "WinExec",
    "portable_pty",
    "duct::",
];

/// Where the command type itself starts processes.
const OWNERS: [&str; 2] = ["src/command/", "src/windows/command/"];

/// One workspace crate.
struct Member {
    directory: PathBuf,
    manifest: toml::Table,
}

fn read_manifest(path: &Path) -> toml::Table {
    let text =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    text.parse()
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// The nearest ancestor of the working directory whose manifest declares
/// a workspace.
fn workspace_root() -> PathBuf {
    let start = std::env::current_dir().expect("working directory");
    start
        .ancestors()
        .find(|directory| {
            fs::read_to_string(directory.join("Cargo.toml"))
                .is_ok_and(|text| text.lines().any(|line| line.trim() == "[workspace]"))
        })
        .unwrap_or_else(|| panic!("no workspace above {}", start.display()))
        .to_owned()
}

fn normalized(path: &Path) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                normal.pop();
            }
            std::path::Component::CurDir => {}
            other => normal.push(other),
        }
    }
    normal
}

/// Workspace members by package name.
fn members(root: &Path) -> (toml::Table, BTreeMap<String, Member>) {
    let manifest = read_manifest(&root.join("Cargo.toml"));
    let workspace = manifest
        .get("workspace")
        .and_then(toml::Value::as_table)
        .expect("[workspace]");
    let mut members = BTreeMap::new();
    for member in workspace
        .get("members")
        .and_then(toml::Value::as_array)
        .expect("workspace members")
    {
        let member = member.as_str().expect("member path");
        let directories: Vec<PathBuf> = if let Some(parent) = member.strip_suffix("/*") {
            fs::read_dir(root.join(parent))
                .expect("member glob")
                .flatten()
                .map(|entry| entry.path())
                .filter(|directory| directory.join("Cargo.toml").is_file())
                .collect()
        } else {
            assert!(!member.contains('*'), "unsupported member pattern {member}");
            vec![root.join(member)]
        };
        for directory in directories {
            let manifest = read_manifest(&directory.join("Cargo.toml"));
            let name = manifest
                .get("package")
                .and_then(|package| package.get("name"))
                .and_then(toml::Value::as_str)
                .expect("package name")
                .to_owned();
            members.insert(
                name,
                Member {
                    directory: normalized(&directory),
                    manifest,
                },
            );
        }
    }
    (workspace.clone(), members)
}

/// Every dependency table that links into the crate's own targets: plain
/// and per-target, never development or build dependencies.
fn linked_tables(manifest: &toml::Table) -> Vec<&toml::Table> {
    let mut tables: Vec<&toml::Table> = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .into_iter()
        .collect();
    if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
        tables.extend(
            targets
                .values()
                .filter_map(|target| target.get("dependencies"))
                .filter_map(toml::Value::as_table),
        );
    }
    tables
}

/// The workspace members each member links.
fn links(
    root: &Path,
    workspace: &toml::Table,
    members: &BTreeMap<String, Member>,
) -> BTreeMap<String, BTreeSet<String>> {
    let by_directory: BTreeMap<&Path, &str> = members
        .iter()
        .map(|(name, member)| (member.directory.as_path(), name.as_str()))
        .collect();
    let shared = workspace
        .get("dependencies")
        .and_then(toml::Value::as_table);
    members
        .iter()
        .map(|(name, member)| {
            let mut linked = BTreeSet::new();
            for table in linked_tables(&member.manifest) {
                for (key, value) in table {
                    let path = match value.get("path").and_then(toml::Value::as_str) {
                        Some(path) => Some(member.directory.join(path)),
                        None if value.get("workspace").and_then(toml::Value::as_bool)
                            == Some(true) =>
                        {
                            shared
                                .and_then(|shared| shared.get(key))
                                .and_then(|shared| shared.get("path"))
                                .and_then(toml::Value::as_str)
                                .map(|path| root.join(path))
                        }
                        None => None,
                    };
                    if let Some(linked_name) =
                        path.and_then(|path| by_directory.get(normalized(&path).as_path()).copied())
                    {
                        linked.insert(linked_name.to_owned());
                    }
                }
            }
            (name.clone(), linked)
        })
        .collect()
}

/// Members that depend on [`OWNER`], and every member those link.
fn covered(links: &BTreeMap<String, BTreeSet<String>>) -> BTreeSet<String> {
    let mut hosts: BTreeSet<String> = BTreeSet::from([OWNER.to_owned()]);
    loop {
        let before = hosts.len();
        for (name, linked) in links {
            if linked.iter().any(|linked| hosts.contains(linked)) {
                hosts.insert(name.clone());
            }
        }
        if hosts.len() == before {
            break;
        }
    }
    let mut covered = hosts.clone();
    let mut pending: Vec<String> = hosts.into_iter().collect();
    while let Some(name) = pending.pop() {
        for linked in links.get(&name).into_iter().flatten() {
            if covered.insert(linked.clone()) {
                pending.push(linked.clone());
            }
        }
    }
    covered
}

/// Source text with comments, string and character literals blanked, so
/// only code remains; line structure is kept.
fn code_only(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut code = String::with_capacity(text.len());
    let mut index = 0;
    // Bracket depth inside an attribute, whose literals are kept.
    let mut attribute = 0usize;
    while let Some(&byte) = bytes.get(index) {
        let rest = bytes.get(index..).unwrap_or_default();
        if rest.starts_with(b"//") {
            while bytes.get(index).is_some_and(|&byte| byte != b'\n') {
                index += 1;
            }
        } else if rest.starts_with(b"/*") {
            let mut depth = 0usize;
            while index < bytes.len() {
                let rest = bytes.get(index..).unwrap_or_default();
                if rest.starts_with(b"/*") {
                    depth += 1;
                    index += 2;
                } else if rest.starts_with(b"*/") {
                    depth -= 1;
                    index += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    if rest.first() == Some(&b'\n') {
                        code.push('\n');
                    }
                    index += 1;
                }
            }
        } else if let Some(hashes) = raw_string_start(rest) {
            index += hashes + 2 + usize::from(rest.first() == Some(&b'b'));
            let mut closing = vec![b'"'];
            closing.extend(std::iter::repeat_n(b'#', hashes));
            while index < bytes.len()
                && !bytes.get(index..).unwrap_or_default().starts_with(&closing)
            {
                if bytes.get(index) == Some(&b'\n') {
                    code.push('\n');
                }
                index += 1;
            }
            index += closing.len();
            code.push_str("\"\"");
        } else if byte == b'"' && attribute > 0 {
            // Attributes keep their literals: `#[path]` files, `cfg` features.
            let start = index;
            index += 1;
            while let Some(&byte) = bytes.get(index) {
                index += 1;
                match byte {
                    b'\\' => index += 1,
                    b'"' => break,
                    _ => {}
                }
            }
            code.push_str(text.get(start..index).unwrap_or_default());
        } else if byte == b'"' {
            index += 1;
            while let Some(&byte) = bytes.get(index) {
                index += 1;
                match byte {
                    b'\\' => index += 1,
                    b'"' => break,
                    b'\n' => code.push('\n'),
                    _ => {}
                }
            }
            code.push_str("\"\"");
        } else if byte == b'\'' && is_char_literal(rest) {
            index += 1;
            while let Some(&byte) = bytes.get(index) {
                index += 1;
                match byte {
                    b'\\' => index += 1,
                    b'\'' => break,
                    _ => {}
                }
            }
            code.push_str("' '");
        } else {
            if byte == b'[' && (attribute > 0 || code.ends_with('#') || code.ends_with("#!")) {
                attribute += 1;
            } else if byte == b']' && attribute > 0 {
                attribute -= 1;
            }
            let length = text
                .get(index..)
                .and_then(|rest| rest.chars().next())
                .map_or(1, char::len_utf8);
            code.push_str(text.get(index..index + length).unwrap_or_default());
            index += length;
        }
    }
    code
}

/// The number of `#` in a raw string starting here, if one does.
fn raw_string_start(rest: &[u8]) -> Option<usize> {
    let rest = rest.strip_prefix(b"b").unwrap_or(rest);
    let rest = rest.strip_prefix(b"r")?;
    let hashes = rest.iter().take_while(|&&byte| byte == b'#').count();
    (rest.get(hashes) == Some(&b'"')).then_some(hashes)
}

/// Whether a `'` starts a character literal rather than a lifetime.
fn is_char_literal(rest: &[u8]) -> bool {
    match rest {
        [b'\'', b'\\', ..] | [b'\'', _, b'\'', ..] => true,
        [b'\'', first, ..] if *first >= 0x80 => {
            let text = String::from_utf8_lossy(rest.get(1..).unwrap_or_default());
            let mut chars = text.chars();
            chars.next();
            chars.next() == Some('\'')
        }
        _ => false,
    }
}

/// Tokens of a `cfg` predicate.
fn tokens(predicate: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for character in predicate.chars() {
        if character.is_alphanumeric()
            || character == '_'
            || character == '"'
            || character == '-'
            || character == '.'
        {
            current.push(character);
        } else {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            if "(),=".contains(character) {
                tokens.push(character.to_string());
            }
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

/// Whether the predicate starting at `tokens[*at]` holds only in tests.
fn implies_test(tokens: &[String], at: &mut usize) -> bool {
    let Some(head) = tokens.get(*at).cloned() else {
        return false;
    };
    *at += 1;
    if tokens.get(*at).map(String::as_str) == Some("=") {
        let value = tokens.get(*at + 1).cloned();
        *at += 2;
        // The workspace's test-fixture feature: only tests and test tooling
        // enable it.
        return head == "feature" && value.as_deref() == Some("\"test-support\"");
    }
    if tokens.get(*at).map(String::as_str) != Some("(") {
        return head == "test";
    }
    *at += 1;
    let mut children = Vec::new();
    while tokens.get(*at).is_some_and(|token| token != ")") {
        children.push(implies_test(tokens, at));
        if tokens.get(*at).map(String::as_str) == Some(",") {
            *at += 1;
        }
    }
    *at += 1;
    match head.as_str() {
        "all" => children.iter().any(|child| *child),
        "any" => !children.is_empty() && children.iter().all(|child| *child),
        _ => false,
    }
}

/// Whether attribute text (without `#[`/`]`) is a `cfg` that implies `test`.
fn test_only(attribute: &str) -> bool {
    let attribute = attribute.trim();
    let Some(predicate) = attribute
        .strip_prefix("cfg")
        .map(str::trim_start)
        .and_then(|rest| rest.strip_prefix('('))
        .and_then(|rest| rest.strip_suffix(')'))
    else {
        return false;
    };
    let tokens = tokens(predicate);
    implies_test(&tokens, &mut 0)
}

/// The index just past the bracket matching the opener at `open`.
fn matching(code: &[u8], open: usize, opener: u8, closer: u8) -> usize {
    let mut depth = 0usize;
    for (index, &byte) in code.iter().enumerate().skip(open) {
        if byte == opener {
            depth += 1;
        } else if byte == closer {
            depth -= 1;
            if depth == 0 {
                return index + 1;
            }
        }
    }
    code.len()
}

/// A module declared by `mod name;`, with its `#[path]` if any.
struct Declared {
    name: String,
    path: Option<String>,
    /// Inline module directory nesting at the declaration.
    nesting: Vec<String>,
}

/// Production code of one file, and the modules it declares there.
fn production(code: &str) -> (String, Vec<Declared>) {
    let bytes = code.as_bytes();
    let mut kept = String::with_capacity(code.len());
    let mut declared = Vec::new();
    let mut nesting: Vec<(String, usize)> = Vec::new();
    let mut depth = 0usize;
    let mut index = 0;
    let mut attributes: Vec<String> = Vec::new();
    let mut path_attribute: Option<String> = None;
    while index < bytes.len() {
        let rest = code.get(index..).unwrap_or_default();
        if rest.starts_with("#![") {
            let end = matching(bytes, index + 2, b'[', b']');
            let attribute = code.get(index + 3..end - 1).unwrap_or_default();
            if test_only(attribute) && depth == 0 {
                return (String::new(), Vec::new());
            }
            index = end;
            continue;
        }
        if rest.starts_with("#[") {
            let end = matching(bytes, index + 1, b'[', b']');
            let attribute = code.get(index + 2..end - 1).unwrap_or_default().to_owned();
            if let Some(path) = attribute
                .trim()
                .strip_prefix("path")
                .map(str::trim_start)
                .and_then(|rest| rest.strip_prefix('='))
            {
                path_attribute = Some(path.trim().to_owned());
            }
            attributes.push(attribute);
            index = end;
            continue;
        }
        let Some(&byte) = bytes.get(index) else { break };
        if byte.is_ascii_whitespace() {
            kept.push(byte as char);
            index += 1;
            continue;
        }
        // The next item starts here; its attributes decide whether it counts.
        let skip = attributes.iter().any(|attribute| test_only(attribute));
        let item_path = path_attribute.take();
        attributes.clear();
        let item_end = item_end(bytes, index);
        let item = code.get(index..item_end).unwrap_or_default();
        if skip {
            for _ in item.matches('\n') {
                kept.push('\n');
            }
            index = item_end;
            continue;
        }
        let words: Vec<&str> = item
            .split(|character: char| !(character.is_alphanumeric() || character == '_'))
            .filter(|word| !word.is_empty())
            .take(4)
            .collect();
        let module = words
            .iter()
            .position(|word| *word == "mod")
            .filter(|position| {
                words.get(..*position).is_some_and(|prefix| {
                    prefix
                        .iter()
                        .all(|word| ["pub", "crate", "super", "self", "in"].contains(word))
                })
            })
            .and_then(|position| words.get(position + 1));
        if let Some(name) = module {
            if item.trim_end().ends_with(';') {
                declared.push(Declared {
                    name: (*name).to_owned(),
                    path: item_path.map(|path| path.trim_matches('"').to_owned()),
                    nesting: nesting.iter().map(|(name, _)| name.clone()).collect(),
                });
                index = item_end;
                continue;
            }
            // An inline module: descend into its body.
            let open = code
                .get(index..)
                .and_then(|rest| rest.find('{'))
                .map_or(item_end, |offset| index + offset);
            kept.push_str(code.get(index..=open).unwrap_or_default());
            depth += 1;
            nesting.push(((*name).to_owned(), depth));
            index = open + 1;
            continue;
        }
        if byte == b'}' && nesting.last().is_some_and(|(_, level)| *level == depth) {
            nesting.pop();
            depth -= 1;
            kept.push('}');
            index += 1;
            continue;
        }
        kept.push_str(item);
        index = item_end;
    }
    (kept, declared)
}

/// The end of the item starting at `start`: past its first top-level `;`,
/// or past the `{ }` body that ends it.
fn item_end(code: &[u8], start: usize) -> usize {
    let mut index = start;
    while let Some(&byte) = code.get(index) {
        match byte {
            b';' => return index + 1,
            b'{' => {
                let end = matching(code, index, b'{', b'}');
                // A body followed by `;` (a `static` or `let`) continues.
                let after = code.get(end..).unwrap_or_default();
                let next = after.iter().position(|byte| !byte.is_ascii_whitespace());
                return match next.and_then(|offset| after.get(offset)) {
                    Some(b';') => end + next.unwrap_or(0) + 1,
                    Some(b'.' | b',' | b')' | b'?') => {
                        index = end;
                        continue;
                    }
                    _ => end,
                };
            }
            b'(' => index = matching(code, index, b'(', b')'),
            b'[' => index = matching(code, index, b'[', b']'),
            b'}' if index == start => return index + 1,
            b'}' => return index,
            _ => index += 1,
        }
    }
    code.len()
}

/// Production code of every source file a crate's library and binaries
/// compile, by path.
fn crate_sources(directory: &Path, manifest: &toml::Table) -> Vec<(PathBuf, String)> {
    let mut roots: Vec<PathBuf> = Vec::new();
    let lib = manifest
        .get("lib")
        .and_then(|lib| lib.get("path"))
        .and_then(toml::Value::as_str)
        .map_or_else(|| directory.join("src/lib.rs"), |path| directory.join(path));
    roots.push(lib);
    roots.push(directory.join("src/main.rs"));
    for bin in manifest
        .get("bin")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(path) = bin.get("path").and_then(toml::Value::as_str) {
            roots.push(directory.join(path));
        }
    }
    if let Ok(entries) = fs::read_dir(directory.join("src/bin")) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|extension| extension == "rs") {
                roots.push(path);
            } else if path.join("main.rs").is_file() {
                roots.push(path.join("main.rs"));
            }
        }
    }
    let mut sources = Vec::new();
    let mut seen = BTreeSet::new();
    let mut pending: Vec<(PathBuf, bool)> = roots
        .into_iter()
        .filter(|root| root.is_file())
        .map(|root| (root, true))
        .collect();
    while let Some((file, root)) = pending.pop() {
        if !seen.insert(file.clone()) {
            continue;
        }
        let text =
            fs::read_to_string(&file).unwrap_or_else(|error| panic!("{}: {error}", file.display()));
        let (code, declared) = production(&code_only(&text));
        let owns_directory = root || file.file_name().is_some_and(|name| name == "mod.rs");
        let parent = file.parent().expect("source directory").to_owned();
        let base = if owns_directory {
            parent.clone()
        } else {
            parent.join(file.file_stem().expect("source stem"))
        };
        for module in declared {
            let mut directory = base.clone();
            for inline in &module.nesting {
                directory.push(inline);
            }
            let candidates = match &module.path {
                Some(path) if module.nesting.is_empty() => vec![parent.join(path)],
                Some(path) => vec![directory.join(path)],
                None => vec![
                    directory.join(format!("{}.rs", module.name)),
                    directory.join(&module.name).join("mod.rs"),
                ],
            };
            let found = candidates
                .iter()
                .find(|candidate| candidate.is_file())
                .unwrap_or_else(|| {
                    panic!(
                        "{}: module {} not found in {candidates:?}",
                        file.display(),
                        module.name
                    )
                });
            pending.push((normalized(found), false));
        }
        sources.push((file, code));
    }
    sources
}

/// Starts outside the command type in `code`.
fn starts(code: &str) -> Vec<&'static str> {
    let mut found: Vec<&'static str> = STARTS
        .into_iter()
        .filter(|spelling| {
            code.match_indices(spelling).any(|(at, _)| {
                let identifier = |character: char| character.is_alphanumeric() || character == '_';
                // `service_process::*` names another module.
                let joined_before = code
                    .get(..at)
                    .and_then(|before| before.chars().next_back())
                    .is_some_and(identifier);
                // `process::CommandExt` names a trait, not a start.
                let joined_after = spelling.ends_with("Command")
                    && code
                        .get(at + spelling.len()..)
                        .and_then(|rest| rest.chars().next())
                        .is_some_and(identifier);
                !joined_before && !joined_after
            })
        })
        .collect();
    // `process::{..., Command, ...}` imports.
    let mut rest = code;
    while let Some(start) = rest.find("process::{") {
        let group = rest.get(start..).unwrap_or_default();
        let end = group.find('}').unwrap_or(group.len());
        if group
            .get(..end)
            .unwrap_or_default()
            .split(|character: char| !(character.is_alphanumeric() || character == '_'))
            .any(|word| word == "Command")
        {
            found.push("process::{Command}");
        }
        rest = group.get(end..).unwrap_or_default();
    }
    found
}

#[test]
fn every_host_process_start_uses_the_command_type() {
    let root = workspace_root();
    let (workspace, members) = members(&root);
    let links = links(&root, &workspace, &members);
    let covered = covered(&links);
    let exempt: BTreeSet<&str> = members
        .keys()
        .map(String::as_str)
        .filter(|name| !covered.contains(*name))
        .collect();
    assert_eq!(
        exempt,
        EXEMPT.into_iter().collect(),
        "the crates no host links changed; a host crate must start processes through the command type"
    );
    let mut violations = Vec::new();
    let mut scanned = 0;
    for name in &covered {
        let member = members.get(name).expect("covered member");
        for (file, code) in crate_sources(&member.directory, &member.manifest) {
            scanned += 1;
            let relative = file
                .strip_prefix(&member.directory)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            if name == OWNER && OWNERS.iter().any(|owner| relative.starts_with(owner)) {
                continue;
            }
            for spelling in starts(&code) {
                violations.push(format!("{name}/{relative}: {spelling}"));
            }
        }
    }
    assert!(scanned > 100, "only {scanned} sources scanned");
    assert!(
        violations.is_empty(),
        "process starts outside the command type:\n{}",
        violations.join("\n")
    );
}

/// Negative control: a planted workspace's host crate, the crate it links
/// and its production spawns are found; its tests and tooling are not.
#[test]
fn the_scan_finds_planted_starts_and_skips_tests_and_tooling() {
    let root = tempfile::tempdir().expect("root");
    let write = |path: &str, text: &str| {
        let path = root.path().join(path);
        fs::create_dir_all(path.parent().expect("parent")).expect("directory");
        fs::write(path, text).expect("write");
    };
    write(
        "Cargo.toml",
        "[workspace]\nmembers = [\"runtime\", \"host\", \"linked\", \"tool\"]\n\n[workspace.dependencies]\nlinked = { path = \"linked\" }\n",
    );
    write(
        "runtime/Cargo.toml",
        &format!("[package]\nname = \"{OWNER}\"\n"),
    );
    write("runtime/src/lib.rs", "");
    write(
        "host/Cargo.toml",
        &format!(
            "[package]\nname = \"host\"\n\n[target.'cfg(windows)'.dependencies]\n{OWNER} = {{ path = \"../runtime\" }}\nlinked.workspace = true\n\n[dev-dependencies]\ntool = {{ path = \"../tool\" }}\n"
        ),
    );
    write(
        "host/src/lib.rs",
        concat!(
            "//! std::process::Command in a comment\n",
            "mod spawn;\n",
            "#[cfg(all(test, windows))]\nmod tests;\n",
            "#[cfg(test)]\nfn helper() { std::process::Command::new(\"x\"); }\n",
            "#[cfg(any(test, feature = \"x\"))]\nfn not_only_tests() { let _ = \"no\"; }\n",
            "mod inline {\n    #[path = \"elsewhere.rs\"]\n    mod moved;\n}\n",
            "const TEXT: &str = \"std::process::Command\";\n",
            "#[cfg(all(unix, feature = \"test-support\"))]\nmod fixtures;\n",
            "use std::os::unix::process::CommandExt as _;\n",
            "use service_process::*;\n",
        ),
    );
    write("host/src/spawn.rs", "use std::process::{Child, Command};\n");
    write("host/src/tests.rs", "use std::process::Command;\n");
    write("host/src/fixtures.rs", "use std::process::Command;\n");
    write(
        "host/src/inline/elsewhere.rs",
        "fn f() { tokio::process::Command::new(\"x\"); }\n",
    );
    write("linked/Cargo.toml", "[package]\nname = \"linked\"\n");
    write(
        "linked/src/lib.rs",
        "pub fn f() { std::process::Command::new(\"x\"); }\n",
    );
    write("tool/Cargo.toml", "[package]\nname = \"tool\"\n");
    write(
        "tool/src/main.rs",
        "fn main() { std::process::Command::new(\"x\"); }\n",
    );
    let (workspace, members) = members(root.path());
    let links = links(root.path(), &workspace, &members);
    let covered = covered(&links);
    assert_eq!(
        covered,
        [OWNER, "host", "linked"]
            .map(str::to_owned)
            .into_iter()
            .collect()
    );
    let mut found = BTreeMap::new();
    for name in ["host", "linked", "tool"] {
        let member = members.get(name).expect("member");
        for (file, code) in crate_sources(&member.directory, &member.manifest) {
            let relative = file
                .strip_prefix(root.path())
                .expect("planted")
                .to_string_lossy()
                .replace('\\', "/");
            found.insert(relative, starts(&code));
        }
    }
    assert_eq!(found.get("host/src/lib.rs"), Some(&Vec::new()), "{found:?}");
    assert_eq!(
        found.get("host/src/spawn.rs"),
        Some(&vec!["process::{Command}"])
    );
    assert_eq!(
        found.get("host/src/inline/elsewhere.rs"),
        Some(&vec!["process::Command"])
    );
    assert_eq!(
        found.get("linked/src/lib.rs"),
        Some(&vec!["process::Command"])
    );
    assert!(!found.contains_key("host/src/tests.rs"), "{found:?}");
    assert!(!found.contains_key("host/src/fixtures.rs"), "{found:?}");
}
