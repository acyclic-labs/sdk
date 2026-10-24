#![no_main]

use acyclic_harness::{
    Error, Result,
    client::{CommandCatalog, CommandResolution},
    conversation::ContentResidencyVerifier,
};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

// These are the production parser functions used by catalog resolution,
// command/skill discovery and pinned template expansion, without replicas.
#[path = "../../harness-core/src/context/discovery/frontmatter.rs"]
mod frontmatter;
#[path = "../../harness/src/client/commands/parse.rs"]
mod parse;
#[path = "../../harness/src/client/commands/syntax.rs"]
mod syntax;

struct NoReads;
impl ContentResidencyVerifier for NoReads {}

fuzz_target!(|bytes: &[u8]| {
    let mut parts = bytes.splitn(3, |byte| *byte == 0);
    let command = parts.next().unwrap_or_default();
    let template = parts.next().unwrap_or(command);
    let arguments = parts.next().unwrap_or(command);
    if let Ok(input) = std::str::from_utf8(command) {
        if let Ok(Some((name, arguments))) = syntax::split(input, 4096) {
            let canonical = format!("/{name} {arguments}");
            assert_eq!(
                syntax::split(&canonical, 8192).unwrap(),
                Some((name, arguments))
            );
            let _ = parse::validate_name(name);
        }
        if let Ok(resolved) = futures::executor::block_on(
            CommandCatalog::builtins()
                .unwrap()
                .resolve(input, &NoReads, 4096),
        ) {
            assert!(
                !input.trim_start().starts_with('/')
                    || !matches!(&resolved, CommandResolution::Prompt { .. })
            );
            let encoded = serde_json::to_vec(&resolved).unwrap();
            assert_eq!(
                serde_json::from_slice::<CommandResolution>(&encoded).unwrap(),
                resolved
            );
        }
    }
    if let Ok(fields) =
        frontmatter::parse_frontmatter::<BTreeMap<String, serde_json::Value>>(template)
    {
        let rewritten = format!("---\n{}---\n", serde_saphyr::to_string(&fields).unwrap());
        assert_eq!(
            frontmatter::parse_frontmatter::<BTreeMap<String, serde_json::Value>>(
                rewritten.as_bytes()
            )
            .unwrap(),
            fields
        );
    }
    let body = if parse::has_frontmatter(template) {
        match frontmatter::frontmatter_end(template) {
            Ok((_, _, start)) => &template[start..],
            Err(_) => return,
        }
    } else {
        template
    };
    if let (Ok(body), Ok(arguments)) = (std::str::from_utf8(body), std::str::from_utf8(arguments)) {
        for bound in [0, body.len() as u64, 4096] {
            let expected = if body.contains("$ARGUMENTS") {
                body.replace("$ARGUMENTS", arguments)
            } else if !arguments.is_empty() {
                format!("{body}\n{arguments}")
            } else {
                body.into()
            };
            let result = parse::expand(body, arguments, bound);
            assert_eq!(
                result.is_ok(),
                !body.contains('\0') && expected.len() as u64 <= bound
            );
            if let Ok(expanded) = result {
                assert_eq!(expanded, expected);
            }
        }
    }
});
