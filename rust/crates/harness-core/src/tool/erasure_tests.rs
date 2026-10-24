//! Every tool, effect and task-command dispatch returns one erased future.
//!
//! A dispatch boundary that returned `impl Future` (or was an `async fn`)
//! would inline the dispatched tool or command's whole state machine into its
//! caller's, so the caller's poll frame would grow with every command it can
//! reach. The dispatch traits return only `BoxFuture` (or a
//! `NativeFutureBundle`, which owns one); the checks below fail if any
//! dispatch trait, implementation or the task command router stops doing so.

use crate::effects::EffectProvider;
use crate::tool::ToolExecutor;
use std::path::Path;

// A trait with an `async fn` or `-> impl Future` method is not dyn-compatible:
// these compile only while every dispatch method returns a nameable, erased type.
const _: Option<&dyn ToolExecutor> = None;
const _: Option<&dyn EffectProvider> = None;
#[cfg(feature = "filesystem")]
const _: Option<&dyn crate::filesystem::TaskCommandHost> = None;

/// The dispatch traits and every implementation of them.
const DISPATCH: [(&str, &str); 3] = [
    ("pub trait ToolExecutor", "ToolExecutor for "),
    ("pub trait EffectProvider", "EffectProvider for "),
    ("pub trait TaskCommandHost", "TaskCommandHost for "),
];

/// Replaces comments, string and character literals with spaces, so braces
/// and keywords inside them never count.
fn code_only(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut out = String::with_capacity(source.len());
    let mut index = 0;
    while index < bytes.len() {
        let rest = source.get(index..).unwrap_or_default();
        if rest.starts_with("//") {
            let end = rest.find('\n').unwrap_or(rest.len());
            out.extend(std::iter::repeat_n(' ', end));
            index += end;
        } else if rest.starts_with("/*") {
            let end = rest.find("*/").map_or(rest.len(), |end| end + 2);
            out.extend(
                rest.get(..end)
                    .unwrap_or(rest)
                    .chars()
                    .map(|c| if c == '\n' { '\n' } else { ' ' }),
            );
            index += end;
        } else if let Some(hashes) = raw_string_hashes(rest) {
            let open = 2 + hashes;
            let close = format!("\"{}", "#".repeat(hashes));
            let end = rest
                .get(open..)
                .and_then(|after| after.find(&close))
                .map_or(rest.len(), |end| open + end + close.len());
            out.extend(
                rest.get(..end)
                    .unwrap_or(rest)
                    .chars()
                    .map(|c| if c == '\n' { '\n' } else { ' ' }),
            );
            index += end;
        } else if rest.starts_with('"') {
            let mut end = 1;
            while end < rest.len() {
                match rest.as_bytes()[end] {
                    b'\\' => end += 2,
                    b'"' => {
                        end += 1;
                        break;
                    }
                    _ => end += 1,
                }
            }
            let end = end.min(rest.len());
            out.extend(
                rest.get(..end)
                    .unwrap_or(rest)
                    .chars()
                    .map(|c| if c == '\n' { '\n' } else { ' ' }),
            );
            index += end;
        } else if let Some(end) = char_literal(rest) {
            out.extend(std::iter::repeat_n(' ', end));
            index += end;
        } else {
            let c = rest.chars().next().unwrap_or(' ');
            out.push(c);
            index += c.len_utf8();
        }
    }
    out
}

fn raw_string_hashes(rest: &str) -> Option<usize> {
    let after = rest.strip_prefix('r')?;
    let hashes = after.len() - after.trim_start_matches('#').len();
    after.get(hashes..)?.starts_with('"').then_some(hashes)
}

/// The length of a character literal at the start of `rest`, if one is
/// there (a lifetime such as `'a` is not one).
fn char_literal(rest: &str) -> Option<usize> {
    let body = rest.strip_prefix('\'')?;
    if let Some(escaped) = body.strip_prefix('\\') {
        let first = escaped.chars().next()?.len_utf8();
        return escaped.get(first..)?.find('\'').map(|end| end + first + 3);
    }
    let c = body.chars().next()?;
    body.get(c.len_utf8()..)?
        .starts_with('\'')
        .then_some(c.len_utf8() + 2)
}

/// The item text at brace depth one of the block opened after `start`: the
/// method signatures of a trait or impl, without any method body.
fn signatures(code: &str, start: usize) -> String {
    let Some(open) = code
        .get(start..)
        .and_then(|rest| rest.find('{'))
        .map(|open| start + open)
    else {
        return String::new();
    };
    let mut depth = 0_usize;
    let mut out = String::new();
    for c in code.get(open..).unwrap_or_default().chars() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            _ if depth == 1 => out.push(c),
            _ => {}
        }
    }
    out
}

/// Every dispatch method signature in `source` that does not return an
/// erased future: an `async fn`, or a signature returning `impl ...`.
fn violations(file: &str, source: &str) -> Vec<String> {
    let code = code_only(source);
    let mut found = Vec::new();
    for (definition, implementation) in DISPATCH {
        for anchor in [definition, implementation] {
            for (start, _) in code.match_indices(anchor) {
                let items = signatures(&code, start);
                let items = items.split_whitespace().collect::<Vec<_>>().join(" ");
                for item in items.split(';') {
                    if item.contains("async fn ") || item.contains("-> impl ") {
                        found.push(format!("{file}: {anchor}: {}", item.trim()));
                    }
                }
            }
        }
    }
    found
}

/// The two command routers and the erased future each must return: the
/// task command router (`task_commands/dispatch.rs`) and the wire command
/// router every client owner awaits (`local/commands.rs`).
const ROUTERS: [(&str, &str); 2] = [
    ("fn dispatch_command<'a>(", "Result<BoundedContentFuture<"),
    ("fn dispatch_configured_command<'a>(", "BoxProviderFuture<"),
];

/// A router's signature, up to its body.
fn router_signature(code: &str, name: &str) -> Option<String> {
    let start = code.find(name)?;
    let end = code.get(start..)?.find('{')?;
    Some(
        code.get(start..start + end)?
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn sources(directory: &Path, files: &mut Vec<(String, String)>) {
    let mut entries = std::fs::read_dir(directory)
        .expect("source directory")
        .map(|entry| entry.expect("source entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            sources(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let source = std::fs::read_to_string(&path).expect("source file");
            files.push((path.display().to_string(), source));
        }
    }
}

#[test]
fn every_dispatch_returns_an_erased_future() {
    // Dispatch implementations live in both this crate and the harness crate
    // built on it.
    let mut files = Vec::new();
    let manifest = Path::new(crate::build_metadata::MANIFEST_DIR);
    sources(&manifest.join("src"), &mut files);
    sources(&manifest.join("../harness/src"), &mut files);
    let mut found = Vec::new();
    let mut anchors = [0_usize; 3];
    let mut routers = [0_usize; 2];
    for (file, source) in &files {
        found.extend(violations(file, source));
        let code = code_only(source);
        for (count, (definition, implementation)) in anchors.iter_mut().zip(DISPATCH) {
            *count += code.matches(definition).count() + code.matches(implementation).count();
        }
        for (seen, (name, erased)) in routers.iter_mut().zip(ROUTERS) {
            if let Some(signature) = router_signature(&code, name) {
                *seen += 1;
                assert!(
                    signature.contains(erased) && !signature.contains("impl "),
                    "{file}: the command router must return the erased {erased}..>: {signature}"
                );
            }
        }
    }
    assert!(
        found.is_empty(),
        "dispatch paths return an unerased future:\n{}",
        found.join("\n")
    );
    // Each trait and at least one implementation was seen, so the scan is live.
    for (count, (definition, _)) in anchors.iter().zip(DISPATCH) {
        assert!(
            *count >= 2,
            "{definition} and its implementations were not scanned"
        );
    }
    assert_eq!(
        routers,
        [1, 1],
        "each command router must be scanned exactly once"
    );
}

/// Negative control: the same scan rejects every unerased dispatch form and
/// accepts the erased one, without being fooled by comments or literals.
#[test]
fn the_scan_rejects_unerased_dispatch() {
    let async_impl =
        "impl ToolExecutor for Tool {\n    async fn execute(&self) -> Result<()> { Ok(()) }\n}";
    let opaque_impl = "impl ToolExecutor for Tool {\n    fn execute<'a>(&'a self) -> impl Future<Output = ()> + 'a { async {} }\n}";
    let opaque_trait =
        "pub trait EffectProvider {\n    fn dispatch(&self) -> impl Future<Output = ()>;\n}";
    let async_host = "impl TaskCommandHost for Host<'_> {\n    async fn execute(&self) {}\n}";
    for unerased in [async_impl, opaque_impl, opaque_trait, async_host] {
        assert_eq!(violations("fixture", unerased).len(), 1, "{unerased}");
    }
    let erased = "impl ToolExecutor for Tool {\n    // async fn execute(&self) -> impl Future\n    fn execute<'a>(&'a self) -> BoxFuture<'a, ()> {\n        let brace = '{';\n        let text = \"-> impl Future }\";\n        Box::pin(async move { let _ = (brace, text); helper().await })\n    }\n    fn name(&self) -> &str { \"}\" }\n}\nasync fn helper() {}";
    assert_eq!(violations("fixture", erased), Vec::<String>::new());
    let router = code_only("fn dispatch_command<'a>(&'a self) -> impl Future<Output = ()> {}");
    assert!(
        router_signature(&router, ROUTERS[0].0).is_some_and(|signature| signature
            .contains("impl ")
            && !signature.contains(ROUTERS[0].1))
    );
}
