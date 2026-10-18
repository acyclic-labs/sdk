#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::{
    Error, Result,
    client::{CommandCatalog, CommandResolution},
    conversation::ContentResidencyVerifier,
};
use std::collections::BTreeMap;

// These are the production parser functions used by catalog resolution,
// command/skill discovery and pinned template expansion, without replicas.
#[path = "../../harness/src/context/discovery/frontmatter.rs"]
mod frontmatter;
#[path = "../../harness/src/client/commands/parse.rs"]
mod parse;
#[path = "../../harness/src/client/commands/syntax.rs"]
mod syntax;

struct NoReads;
impl ContentResidencyVerifier for NoReads {}

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let mut parts = bytes.splitn(3, |byte| *byte == 0);
    let command = parts.next().unwrap_or_default();
    let template = parts.next().unwrap_or(command);
    let arguments = parts.next().unwrap_or(command);
    {
        let fuzz_input = fuzz_input
            .view(command)
            .unwrap_or_else(|| unreachable!("input command slice"));
        if let Some(input) = fuzz_input.text() {
            if let Some(Some((name, arguments))) =
                fuzz_support::FuzzInput::parse_text_bounded(fuzz_input, 4096, |input, maximum| {
                    syntax::split(input, maximum)
                })
            {
                let canonical = format!("/{name} {arguments}");
                let _ = fuzz_support::round_trip(
                    &Some((name, arguments)),
                    syntax::split(&canonical, 8192),
                );
                let _ = parse::validate_name(name);
            }
            if let Some(resolved) = fuzz_support::FuzzInput::parse_text(fuzz_input, |input| {
                let catalog = CommandCatalog::builtins().unwrap();
                futures::executor::block_on(catalog.resolve(input, &NoReads, 4096))
            }) {
                assert!(
                    !input.trim_start().starts_with('/')
                        || !matches!(&resolved, CommandResolution::Prompt { .. })
                );
                let encoded = serde_json::to_vec(&resolved).unwrap();
                let _ = fuzz_support::round_trip(
                    &resolved,
                    serde_json::from_slice::<CommandResolution>(&encoded),
                );
            }
        }
    }
    {
        let fuzz_input = fuzz_input
            .view(template)
            .unwrap_or_else(|| unreachable!("input template slice"));
        if let Some(fields) = fuzz_support::FuzzInput::parse(
            fuzz_input,
            frontmatter::parse_frontmatter::<BTreeMap<String, serde_json::Value>>,
        ) {
            let rewritten = format!("---\n{}---\n", serde_saphyr::to_string(&fields).unwrap());
            let _ = fuzz_support::round_trip(
                &fields,
                frontmatter::parse_frontmatter::<BTreeMap<String, serde_json::Value>>(
                    rewritten.as_bytes(),
                ),
            );
        }
    }
    let body = if parse::has_frontmatter(template) {
        let fuzz_input = fuzz_input
            .view(template)
            .unwrap_or_else(|| unreachable!("input frontmatter slice"));
        let Some((_, _, start)) =
            fuzz_support::FuzzInput::parse(fuzz_input, frontmatter::frontmatter_end)
        else {
            return;
        };
        &template[start..]
    } else {
        template
    };
    if let (Some(body), Some(arguments)) = (
        fuzz_input
            .view(body)
            .and_then(fuzz_support::FuzzInput::text),
        fuzz_input
            .view(arguments)
            .and_then(fuzz_support::FuzzInput::text),
    ) {
        for bound in [0, body.len() as u64, 4096] {
            let expected = if body.contains("$ARGUMENTS") {
                body.replace("$ARGUMENTS", arguments)
            } else if !arguments.is_empty() {
                format!("{body}\n{arguments}")
            } else {
                body.into()
            };
            if let Some(expanded) = fuzz_support::accepts(
                !body.contains('\0') && expected.len() as u64 <= bound,
                parse::expand(body, arguments, bound),
            ) {
                assert_eq!(expanded, expected);
            }
        }
    }
});
