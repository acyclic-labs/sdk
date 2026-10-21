use syn::ext::IdentExt;
use syn::{Meta, Token, parse::Parser, punctuated::Punctuated, visit::Visit};
#[path = "feature_policy/modules.rs"]
mod modules;

fn named(path: &syn::Path, name: &str) -> bool {
    path.get_ident().is_some_and(|ident| ident.unraw() == name)
}

// Possible predicate values with test disabled (and optionally test-support
// disabled). Unknown platform/feature predicates may have either value.
fn possible(meta: &Meta, without_support: bool) -> [bool; 2] {
    match meta {
        Meta::Path(path) if named(path, "test") => [true, false],
        Meta::NameValue(_) if without_support && contains_test_support(meta) => [true, false],
        Meta::List(list) => {
            let args = Punctuated::<Meta, Token![,]>::parse_terminated
                .parse2(list.tokens.clone())
                .expect("valid cfg predicate");
            let values: Vec<_> = args
                .iter()
                .map(|meta| possible(meta, without_support))
                .collect();
            if named(&list.path, "all") {
                [values.iter().any(|v| v[0]), values.iter().all(|v| v[1])]
            } else if named(&list.path, "any") {
                [values.iter().all(|v| v[0]), values.iter().any(|v| v[1])]
            } else if named(&list.path, "not") && values.len() == 1 {
                [values[0][1], values[0][0]]
            } else {
                [true, true]
            }
        }
        _ => [true, true],
    }
}

fn gated(attrs: &[syn::Attribute], without_support: bool) -> bool {
    attrs
        .iter()
        .filter(|attr| named(attr.path(), "cfg"))
        .any(|attr| {
            !possible(
                &attr.parse_args::<Meta>().expect("valid cfg predicate"),
                without_support,
            )[1]
        })
}

// Macro bodies are opaque and may substitute arguments into cfg predicates.
// Reject the feature literal anywhere in runtime macro tokens, independently
// of its argument position or macro name, rather than interpreting expansion.
fn macro_predicates(tokens: proc_macro2::TokenStream) -> usize {
    tokens
        .into_iter()
        .map(|token| match token {
            proc_macro2::TokenTree::Literal(value) => usize::from(
                syn::parse_str::<syn::LitStr>(&value.to_string())
                    .is_ok_and(|value| value.value() == "test-support"),
            ),
            proc_macro2::TokenTree::Group(group) => macro_predicates(group.stream()),
            _ => 0,
        })
        .sum()
}

fn contains_test_support(meta: &Meta) -> bool {
    match meta {
        Meta::NameValue(value) if named(&value.path, "feature") => {
            matches!(&value.value, syn::Expr::Lit(value)
                if matches!(&value.lit, syn::Lit::Str(value) if value.value() == "test-support"))
        }
        Meta::List(list) => Punctuated::<Meta, Token![,]>::parse_terminated
            .parse2(list.tokens.clone())
            .expect("valid cfg predicate")
            .iter()
            .any(contains_test_support),
        _ => false,
    }
}

#[derive(Clone, Copy, Default)]
enum Fixtures {
    #[default]
    None,
    Local,
    Settings,
}
#[derive(Default)]
struct FeaturePolicy {
    violations: usize,
    fixtures: Fixtures,
    exemptions: [usize; 3],
    owner: String,
}
impl<'ast> Visit<'ast> for FeaturePolicy {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if !gated(&item.attrs, false) {
            syn::visit::visit_item_mod(self, item);
        }
    }
    fn visit_attribute(&mut self, attr: &'ast syn::Attribute) {
        if (named(attr.path(), "cfg") || named(attr.path(), "cfg_attr"))
            && contains_test_support(&attr.meta)
        {
            self.violations += 1;
        }
        syn::visit::visit_attribute(self, attr);
    }
    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        self.violations += macro_predicates(mac.tokens.clone());
        syn::visit::visit_macro(self, mac);
    }
    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        let owner = std::mem::take(&mut self.owner);
        if let syn::Type::Path(path) = &*item.self_ty {
            self.owner = path.path.segments.last().unwrap().ident.to_string();
        }
        syn::visit::visit_item_impl(self, item);
        self.owner = owner;
    }
    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        let fixture = match self.fixtures {
            Fixtures::Local => self.owner == "LocalHarness" && item.sig.ident == "task_stream",
            Fixtures::Settings => self.owner == "SettingsConfig" && item.sig.ident == "for_test",
            Fixtures::None => false,
        };
        if fixture && gated(&item.attrs, true) {
            self.exemptions[0] += 1;
        } else {
            syn::visit::visit_impl_item_fn(self, item);
        }
    }
    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        let owner = std::mem::replace(&mut self.owner, item.ident.to_string());
        syn::visit::visit_item_struct(self, item);
        self.owner = owner;
    }
    fn visit_field(&mut self, field: &'ast syn::Field) {
        if matches!(self.fixtures, Fixtures::Settings)
            && self.owner == "SettingsConfig"
            && field
                .ident
                .as_ref()
                .is_some_and(|name| name == "_test_home")
        {
            self.exemptions[1] += 1;
            self.visit_type(&field.ty);
        } else {
            syn::visit::visit_field(self, field);
        }
    }
    fn visit_field_value(&mut self, field: &'ast syn::FieldValue) {
        // Only the inert None initializer is exempt; a feature-gated runtime
        // computation in this field must be rejected like every other field.
        if matches!(self.fixtures, Fixtures::Settings)
            && self.owner == "SettingsConfig"
            && matches!(&field.member, syn::Member::Named(name) if name == "_test_home")
            && matches!(&field.expr, syn::Expr::Path(path) if path.path.is_ident("None") && path.attrs.is_empty())
        {
            self.exemptions[2] += 1;
        } else {
            syn::visit::visit_field_value(self, field);
        }
    }
}
fn policy(source: &str, fixtures: Fixtures) -> FeaturePolicy {
    let ast = syn::parse_file(source).expect("valid Rust source");
    let mut policy = FeaturePolicy {
        fixtures,
        ..FeaturePolicy::default()
    };
    policy.visit_file(&ast);
    policy
}
fn violations(source: &str) -> usize {
    policy(source, Fixtures::None).violations
}

#[test]
fn enclosing_predicates_are_rejected() {
    for source in [
        r#"#![cfg(feature = "test-support")] fn open() {}"#,
        r#"#[cfg(feature = "test-support")] impl Runtime { fn open() {} }"#,
        r#"#[cfg(feature = "test-support")] mod runtime { fn open() {} }"#,
        r#"mod runtime { #![cfg(feature = "test-support")] fn open() {} }"#,
    ] {
        assert_eq!(violations(source), 1);
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn every_enclosing_predicate_and_macro_is_rejected(
        modules in proptest::collection::vec(proptest::bool::ANY, 0..5),
        implementation in proptest::bool::ANY,
        file in proptest::bool::ANY,
        wrappers in 0usize..6,
        alias in "[a-z]{1,6}",
    ) {
        let attribute = r#"#[cfg(feature = "test-support")]"#;
        let mut expression = format!(r#"predicate_{alias}!(feature = "test-support")"#);
        for _ in 0..wrappers {
            expression = format!("matches!({expression}, true)");
        }
        let mut source = format!("{} impl Runtime {{ fn open() {{ let _ = {expression}; }} }}", if implementation { attribute } else { "" });
        for gated in &modules {
            source = format!("{} mod runtime {{ {source} }}", if *gated { attribute } else { "" });
        }
        if file {
            source = format!(r#"#![cfg(feature = "test-support")] {source}"#);
        }
        let expected = modules.iter().filter(|gated| **gated).count()
            + usize::from(implementation) + usize::from(file) + 1;
        proptest::prop_assert_eq!(violations(&source), expected);
    }
}

fn read_runtime(path: &std::path::Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))
}

fn runtime_files(
    read: impl Fn(&std::path::Path) -> Result<String, String>,
) -> Result<std::collections::BTreeMap<std::path::PathBuf, (String, Fixtures)>, String> {
    let root = std::path::Path::new(crate::build_metadata::MANIFEST_DIR);
    let core = std::path::Path::new(acyclic_harness_core::build_metadata::MANIFEST_DIR);
    modules::sources(
        &[
            (root.join("src/local/mod.rs"), Fixtures::Local),
            (core.join("src/native_settings.rs"), Fixtures::Settings),
            (core.join("src/native_environment.rs"), Fixtures::None),
        ],
        read,
    )
}

#[test]
fn runtime_configuration_is_independent_of_test_support() {
    for (path, (source, fixtures)) in
        runtime_files(read_runtime).expect("every runtime module must be scanned")
    {
        let policy = policy(&source, fixtures);
        assert_eq!(policy.violations, 0, "{}", path.display());
        assert_eq!(
            policy.exemptions,
            match fixtures {
                Fixtures::None => [0, 0, 0],
                Fixtures::Local => [1, 0, 0],
                Fixtures::Settings => [1, 1, 1],
            }
        );
    }
}

#[test]
fn planted_violations_in_every_runtime_unit_are_rejected() {
    for (_, (source, fixtures)) in
        runtime_files(read_runtime).expect("every runtime module must be scanned")
    {
        let planted = format!("{source}\n#[cfg(feature = \"test-support\")]\nfn planted() {{}}\n");
        assert_eq!(policy(&planted, fixtures).violations, 1);
    }
    let settings = read_runtime(
        &std::path::Path::new(acyclic_harness_core::build_metadata::MANIFEST_DIR)
            .join("src/native_settings.rs"),
    )
    .expect("native settings source must be readable");
    let settings = settings.as_str();
    for before in [
        "fn with_roots(",
        "fn configured_codex_home(",
        "fn codex_directory(",
    ] {
        assert!(settings.contains(before));
        let planted = settings.replacen(
            before,
            &format!("#[cfg(feature = \"test-support\")] {before}"),
            1,
        );
        assert_eq!(policy(&planted, Fixtures::Settings).violations, 1);
    }
    let planted = settings.replacen("_test_home: None", "_test_home: side_effect()", 1);
    assert_ne!(planted, settings);
    assert_eq!(policy(&planted, Fixtures::Settings).violations, 1);
}

#[test]
fn negative_controls_reject_feature_attributes_and_expressions() {
    for source in [
        r#"#[cfg(any(test, feature = "test-support"))] fn resolve() {}"#,
        r#"fn resolve() { #[cfg(feature = "test-support")] let _state = (); }"#,
        r##"#[cfg_attr(feature = r#"test-support"#, inline)] fn resolve() {}"##,
        r#"fn resolve() { let _temporary = cfg!(all(feature = "test-support", unix)); }"#,
        r#"fn resolve() { let _temporary = matches!(cfg!(feature = "test-support"), true); }"#,
        r#"fn resolve() { let _temporary = vec![cfg!(feature = "test-support")]; }"#,
        r#"items! { #[cfg(feature = "test-support")] fn resolve() {} }"#,
        r#"use std::cfg as enabled; fn resolve() { let _temporary = enabled!(feature = "test-support"); }"#,
        r#"macro_rules! flag { ($value:literal) => { cfg!(feature = $value) } } fn resolve() { let _ = flag!("test-support"); }"#,
        r#"#[cfg(r#feature = "test-support")] fn resolve() {}"#,
        r#"#[r#cfg(feature = "test-support")] fn resolve() {}"#,
        r#"fn resolve() { let _temporary = vec![cfg!(r#feature = "test-support")]; }"#,
    ] {
        assert_eq!(violations(source), 1);
    }
    assert_eq!(
        violations(r#"#[cfg(feature = "filesystem-local")] fn resolve() {}"#),
        0
    );
}

#[test]
fn fixture_exemptions_require_an_actual_test_only_gate() {
    for (owner, method, fixtures) in [
        ("LocalHarness", "task_stream", Fixtures::Local),
        ("SettingsConfig", "for_test", Fixtures::Settings),
    ] {
        for predicate in [
            "not(feature = \"test-support\")",
            "any(unix, feature = \"test-support\")",
            "all(not(test), any(unix, feature = \"test-support\"))",
        ] {
            let source = format!("impl {owner} {{ #[cfg({predicate})] fn {method}() {{}} }}");
            let found = policy(&source, fixtures);
            assert_eq!(found.exemptions[0], 0, "{source}");
            assert!(found.violations > 0, "{source}");
            // Reverted predicate: mentioning the literal admits this bypass.
            let ast = syn::parse_file(&source).unwrap();
            let syn::Item::Impl(item) = &ast.items[0] else {
                unreachable!()
            };
            let syn::ImplItem::Fn(item) = &item.items[0] else {
                unreachable!()
            };
            assert!(
                item.attrs
                    .iter()
                    .any(|attr| contains_test_support(&attr.meta))
            );
        }
        for predicate in [
            "test",
            "feature = \"test-support\"",
            "any(test, feature = \"test-support\")",
            "all(unix, feature = \"test-support\")",
        ] {
            let source = format!("impl {owner} {{ #[cfg({predicate})] fn {method}() {{}} }}");
            assert_eq!(policy(&source, fixtures).exemptions[0], 1, "{source}");
        }
    }
}

#[test]
fn planted_children_are_checked_through_the_actual_runtime_roots() {
    for declaration in [
        "mod policy_probe;",
        "#[path = \"native_environment/policy_probe.rs\"] mod policy_probe;",
        "#[r#path = \"native_environment/policy_probe.rs\"] mod policy_probe;",
        "include!(\"native_environment/policy_probe.rs\");",
        "r#include!(\"native_environment/policy_probe.rs\");",
    ] {
        let found = runtime_files(|path| {
            if path.ends_with("native_environment/policy_probe.rs") {
                Ok("#[cfg(feature = \"test-support\")] fn resolve() {}".into())
            } else {
                read_runtime(path).map(|source| {
                    if path.ends_with("src/native_environment.rs") {
                        format!("{source}\n{declaration}")
                    } else {
                        source
                    }
                })
            }
        });
        if declaration.contains("include!") {
            assert!(
                found.is_err(),
                "an unexpanded source inclusion must not pass"
            );
        } else {
            assert_eq!(
                found
                    .unwrap()
                    .values()
                    .map(|(source, fixtures)| policy(source, *fixtures).violations)
                    .sum::<usize>(),
                1,
                "a root-only file list silently omits the planted child"
            );
        }
    }
}
