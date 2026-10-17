use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;

fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        Just("\n".to_owned()),
        Just("\0".to_owned()),
        Just("$ARGUMENTS".to_owned()),
        "[a-z \n$]{0,32}",
        proptest::collection::vec(any::<char>(), 0..64)
            .prop_map(|chars| chars.into_iter().collect()),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Canonical slash syntax round-trips every split, including empty names,
    // Unicode whitespace and arguments. Accepted names are stable under JSON.
    #[test]
    fn syntax_and_names_round_trip(input in text()) {
        let input = format!("/{input}");
        let (name, arguments) = command_syntax::split(&input, 1024).expect("bound").expect("slash");
        let normalized = format!("/{name} {arguments}");
        prop_assert_eq!(command_syntax::split(&normalized, 1024).expect("normalized"), Some((name, arguments)));
        if command_parse::validate_name(name).is_ok() {
            let encoded = serde_json::to_vec(name).expect("encode");
            let decoded: String = serde_json::from_slice(&encoded).expect("decode");
            prop_assert_eq!(&decoded, name);
            prop_assert!(command_parse::validate_name(&decoded).is_ok());
        }
    }

    // Frontmatter encoding, decoding and rewriting preserves all fields,
    // including escaped Unicode, delimiters and placeholder text.
    #[test]
    fn frontmatter_round_trips(
        fields in proptest::collection::btree_map("[a-z]{1,12}", text(), 0..8), crlf in any::<bool>(),
    ) {
        let newline = if crlf { "\r\n" } else { "\n" };
        let header = serde_saphyr::to_string(&fields).expect("header");
        let source = format!("---{newline}{header}---{newline}body $ARGUMENTS");
        prop_assert!(command_parse::has_frontmatter(source.as_bytes()));
        let decoded: BTreeMap<String, String> = frontmatter::parse_frontmatter(source.as_bytes()).expect("parse header");
        prop_assert_eq!(&decoded, &fields);
        let (_, _, body) = frontmatter::frontmatter_end(source.as_bytes()).expect("body offset");
        prop_assert_eq!(&source[body..], "body $ARGUMENTS");
        let rewritten = format!("---\n{}---\n", serde_saphyr::to_string(&decoded).expect("rewrite"));
        prop_assert_eq!(frontmatter::parse_frontmatter::<BTreeMap<String, String>>(rewritten.as_bytes()).expect("reparse"), fields);
    }

    // Expansion succeeds iff its exact, single-pass result fits and the body
    // contains no NUL. Rejection cannot silently make this property vacuous.
    #[test]
    fn substitutions_obey_the_exact_bound(
        parts in proptest::collection::vec(text(), 0..8), arguments in text(), bound in 0_u64..8192,
    ) {
        let body = parts.join("$ARGUMENTS");
        let expected = if body.contains("$ARGUMENTS") {
            body.replace("$ARGUMENTS", &arguments)
        } else if !arguments.is_empty() {
            format!("{body}\n{arguments}")
        } else { body.clone() };
        let result = command_parse::expand(&body, &arguments, bound);
        prop_assert_eq!(result.is_ok(), !body.contains('\0') && expected.len() as u64 <= bound);
        if let Ok(actual) = result { prop_assert_eq!(actual, expected); }
    }

    // Builtins and unknown slash inputs never become model text. Successful
    // command resolutions preserve their typed operation through the JSON codec.
    #[test]
    fn resolution_round_trips_without_forwarding_slash_syntax(input in text(), command in 0_usize..7) {
        let names = ["compact", "model", "diff", "plan", "goal", "mcp", "unknown"];
        let input = format!("/{} {input}", names[command]);
        let catalog = CommandCatalog::builtins().expect("catalog");
        if let Ok(resolved) = futures::executor::block_on(catalog.resolve(&input, &DeniedReader, 1024)) {
            prop_assert!(!matches!(&resolved, CommandResolution::Prompt { .. }), "slash syntax must not become chat");
            let encoded = serde_json::to_vec(&resolved).expect("encode");
            prop_assert_eq!(serde_json::from_slice::<CommandResolution>(&encoded).expect("decode"), resolved);
        }
    }
}

#[test]
fn keep_chomp_newline_is_not_extended_by_frontmatter_framing() {
    let fields = BTreeMap::from([("a".to_owned(), "\n".to_owned())]);
    let header = serde_saphyr::to_string(&fields).expect("serialize keep-chomp scalar");
    for newline in ["\n", "\r\n"] {
        let source = format!("---{newline}{header}---{newline}body");
        let decoded: BTreeMap<String, String> =
            frontmatter::parse_frontmatter(source.as_bytes()).expect("parse keep-chomp scalar");
        assert_eq!(decoded, fields);
        let (_, _, body) = frontmatter::frontmatter_end(source.as_bytes()).expect("body offset");
        assert_eq!(&source[body..], "body");
        let rewritten = format!(
            "---\n{}---\n",
            serde_saphyr::to_string(&decoded).expect("rewrite")
        );
        assert_eq!(
            frontmatter::parse_frontmatter::<BTreeMap<String, String>>(rewritten.as_bytes())
                .expect("reparse"),
            fields
        );
    }
}
