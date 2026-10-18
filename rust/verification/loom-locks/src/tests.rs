use super::scan;

fn constructs(body: &str) -> bool {
    match scan(&format!("fn model() {{ {body} }}")) {
        Ok(found) => found,
        Err(error) if error.to_string().starts_with("indeterminate:") => true,
        Err(error) => panic!("{body}: {error}"),
    }
}

#[test]
fn every_lock_constructor_spelling_is_found() {
    for body in [
        "let a = SyncMutex::new(0);",
        "let a = crate::sync::RwLock::default();",
        "let a = SyncMutex::<Vec<u8>>::new(Vec::new());",
        "let a = Mutex::<BTreeMap<u8, Vec<Option<u8>>>>::default();",
        "let a = SyncMutex::<()>::new(());",
        "let a = SyncMutex::<(u8, Vec<u8>)>::new((0, Vec::new()));",
        "let a = Mutex::<[u8; 4]>::default();",
        "let a = Mutex::<Box<dyn Fn() -> u8>>::new(Box::new(|| 0));",
        // The five forms that escaped the text matcher.
        "let a = SyncMutex::<[u8; { 4 }]>::new([0; 4]);",
        "let a = SyncMutex::<[u8; (8 >> 1)]>::new([0; 4]);",
        "let a = SyncMutex::<Vec</* > */ u8>>::new(Vec::new());",
        "let a = <SyncMutex<()>>::new(());",
        "let a = SyncMutex /* x */ ::<u8>::new(0);",
        "let a = <SyncMutex<u8> as Default>::default();",
        "let a: SyncMutex<u8> = Default::default();",
        "let a = Arc::new(loom::sync::Condvar::new());",
        "let a = vec![SyncMutex::<u8>::new(0)];",
        // A closure argument, as in a Loom model entry.
        "run(|| { let a = SyncMutex::<[u8; (8 >> 1)]>::new([0; 4]); });",
    ] {
        assert!(constructs(body), "missed {body}");
    }
}

#[test]
fn lock_types_inside_other_constructors_are_not_locks() {
    for body in [
        "let a = Vec::<SyncMutex::<Vec<u8>>>::new();",
        "let a = Option::<SyncMutex::<Vec<u8>>>::default();",
        "let a = Vec::<SyncMutex<u8>>::new();",
        "let a: Option<SyncMutex<u8>> = Default::default();",
        "// SyncMutex::new(0)\nlet a = 1;",
        "let a = \"SyncMutex::new(0)\";",
    ] {
        assert!(!constructs(body), "flagged {body}");
    }
    assert!(indeterminate("fn p() { production::Component::new(); }"));
    let identity =
        "const _: fn(crate::sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;";
    assert_eq!(scan(identity).ok(), Some(false));
    // Only the identity closure is an assertion; any other body is a value.
    let converting = "const _: fn(u8) -> SyncMutex<u8> = |value| value.into();";
    assert!(scan(converting).is_err());
}

#[test]
fn aliases_and_loom_imports_are_resolved() {
    let renamed = "use crate::sync::SyncMutex as DataMutex;\nfn m() { DataMutex::new(0); }";
    assert_eq!(scan(renamed).ok(), Some(true));
    let chained =
        "type Lock<T> = crate::sync::Mutex<T>;\ntype Cell<T> = Lock<T>;\nfn m() { Cell::new(0); }";
    assert_eq!(scan(chained).ok(), Some(true));
    let unrelated = "use std::cell::Cell as Slot;\nfn m() { Slot::new(0); }";
    assert_eq!(scan(unrelated).ok(), Some(false));
    let imported = "use loom::{sync::Mutex as DataMutex, thread};";
    assert_eq!(scan(imported).ok(), Some(true));
    let threads = "use loom::{sync::Arc, thread};";
    assert_eq!(scan(threads).ok(), Some(false));
}

#[test]
fn alias_parameters_follow_the_target_container() {
    for container in [
        "Option<T>",
        "Vec<T>",
        "std::marker::PhantomData<T>",
        "[T; 0]",
    ] {
        let source = format!(
            "type Slot<T> = {container}; type Other<U> = Slot<U>; fn p() {{ let _: Other<std::sync::Mutex<u8>> = Default::default(); }}"
        );
        assert_eq!(scan(&source).ok(), Some(false), "{source}");
    }
    for container in ["(T,)", "std::sync::Arc<T>", "[T; 1]"] {
        let source = format!(
            "type Slot<T> = {container}; type Other<U> = Slot<U>; fn p() {{ let _: Other<std::sync::Mutex<u8>> = Default::default(); }}"
        );
        assert_eq!(scan(&source).ok(), Some(true), "{source}");
    }
}

#[test]
fn source_corpus_survives_parse_print_and_keeps_the_expected_verdict() {
    for source in [
        include_str!("../fuzz/corpus/source/external-module"),
        include_str!("../fuzz/corpus/source/shadowed-function"),
        include_str!("../fuzz/corpus/source/local-container"),
        include_str!("../fuzz/corpus/source/raw-type"),
        include_str!("../fuzz/corpus/source/raw-method"),
        include_str!("../fuzz/corpus/source/alias"),
        include_str!("../fuzz/corpus/source/take"),
        include_str!("../fuzz/corpus/source/projection"),
        include_str!("../fuzz/corpus/source/proptest"),
        include_str!("../fuzz/corpus/source/json-key"),
    ] {
        let file = syn::parse_file(source).unwrap_or_else(|error| panic!("{source}: {error}"));
        let rendered = quote::quote!(#file).to_string();
        assert!(
            matches!(scan(source), Ok(true)) || indeterminate(source),
            "{source}: {:?}",
            verdict(source)
        );
        assert_eq!(scan(source).ok(), scan(&rendered).ok());
    }
    for source in [
        include_str!("../fuzz/corpus/source/empty-alias"),
        include_str!("../fuzz/corpus/source/non-lock"),
    ] {
        assert_eq!(scan(source).ok(), Some(false), "{source}");
    }
}

#[test]
fn unknown_explicit_owners_cannot_adopt_an_unrelated_expected_type() {
    for source in [
        "fn plain() { production::Component::new() }",
        "fn plain() { unknown_factory(); }",
        "const A: SyncMutex<u8> = missing_factory();",
        "fn plain() { let value = Unknown {}; }",
        "fn plain() { external::VALUE; }",
        "use external::*; fn plain() { VALUE; }",
        "mod a { use std::cell::Cell as Value; } mod b { type Value = std::sync::Mutex<u8>; fn p() { Value::default(); } }",
        "mod a { type Value = u8; } mod b { type Value = std::sync::Mutex<u8>; fn p() { Value::default(); } }",
        "fn plain() { production::Component::build(); }",
        "fn plain() { value.unknown_factory(); }",
        "type A = <() as Factory>::Lock; fn plain() { A::default() }",
        "type A = Missing; fn plain() -> u8 { A::default() }",
        "use B as A; use A as B; fn plain() -> u8 { A::default() }",
    ] {
        assert!(indeterminate(source), "{source}: {:?}", verdict(source));
    }
}

fn verdict(source: &str) -> Result<bool, String> {
    scan(source).map_err(|error| error.to_string())
}

fn indeterminate(source: &str) -> bool {
    verdict(source).is_err_and(|error| error.starts_with("indeterminate"))
}

// Generated code cannot be seen, so it cannot appear: the r4 probes that
// generated alias names and constructions through macro_rules! are hard
// errors, as are unknown macros, attributes, derives and rebound macro names.
#[test]
fn generated_code_is_rejected() {
    for source in [
        "macro_rules! alias { ($n:ident) => { type $n = std::sync::Mutex<()>; }; }\nalias!(Cell);\nfn m() { Cell::new(()); }",
        "macro_rules! m { () => { let lock: std::sync::Mutex<()> = (Default::default()); }; }",
        "macro_rules! m { () => {}; }",
        "fn m() { my_macro!(x => 1); }",
        "fn m() { crate::vec![1]; }",
        "fn m() { other::json!(1); }",
        "#[tokio::test]\nasync fn m() {}",
        "#[derive(serde::Deserialize)]\nstruct S;",
        "use crate::locks::make as vec;\nfn m() { vec![]; }",
        "use crate::locks::vec;",
    ] {
        assert!(indeterminate(source), "{source}: {:?}", verdict(source));
    }
    let definition = verdict("macro_rules! m { () => {}; }");
    assert!(definition.is_err_and(|error| error.contains("a macro_rules! definition")));
    for source in [
        "use serde_json::json;\nfn m() { json!({\"a\": 1}); }",
        "fn p() { matches!(value, Some(_) | None); }",
        "use std::vec;\n#[derive(Clone, Debug, Default)]\nstruct S;\n#[test]\nfn m() { vec![1]; }",
    ] {
        assert_eq!(verdict(source), Ok(false), "{source}");
    }
}

// Allowed macros are parsed with their own grammar, so constructions inside
// their arguments are found, and arguments outside it are indeterminate.
#[test]
fn allowed_macro_arguments_are_parsed_with_their_grammar() {
    for body in [
        "assert!(SyncMutex::new(0).lock().is_ok(), \"{}\", 1);",
        "assert_eq!(1, { let a: SyncMutex<u8> = Default::default(); 1 });",
        "let a = vec![SyncMutex::new(0); 2];",
        "let a = format!(\"{x}\", x = SyncMutex::new(0));",
        "let a = matches!(x, Some(_) if SyncMutex::new(0).lock().is_ok());",
        "let a = proptest::prop_oneof![1 => SyncMutex::new(0)];",
        "let a = serde_json::json!({\"k\": [1, {\"n\": SyncMutex::new(0)}]});",
        "let a = serde_json::json!({ (SyncMutex::new(0)): 1 });",
    ] {
        assert!(constructs(body), "missed {body}");
    }
    let statics = "loom::thread_local! { static A: SyncMutex<u8> = SyncMutex::new(0) }";
    assert_eq!(verdict(statics), Ok(true));
    let typed = "thread_local! { static A: SyncMutex<u8> = const { Default::default() }; }";
    assert_eq!(verdict(typed), Ok(true));
    let proptest = "proptest::proptest! {\n#![proptest_config(Config::with_cases(4))]\n#[test]\nfn p(a in 0..4u8, b: u8) { let lock: SyncMutex<u8> = (a.into()); }\n}";
    assert!(indeterminate(proptest));
    for body in [
        "assert!(a b c);",
        "let a = matches!(x);",
        "let a = vec![a b];",
        "proptest::proptest! { fn p(a: ) {} }",
        "let a = serde_json::json!({ \"k\" 1 });",
    ] {
        let source = format!("fn m() {{ {body} }}");
        assert!(indeterminate(&source), "{body}: {:?}", verdict(&source));
    }
}

// A binding declared as a lock constructs one whatever the initializer's
// shape, including the r4 wrapped-default probes.
#[test]
fn bindings_declared_as_locks_construct_whatever_the_initializer() {
    for body in [
        "let lock: std::sync::Mutex<()> = (Default::default());",
        "let lock: std::sync::Mutex<()> = { Default::default() };",
        "let lock: std::sync::Mutex<()> = unsafe { { (Default::default()) } };",
        "let lock: SyncMutex<u8> = From::from(0);",
        "let lock: SyncMutex<u8> = 0.into();",
        "let lock: SyncMutex<u8> = make();",
        "let lock: SyncMutex<u8>;",
        "let pair: (SyncMutex<u8>, u8) = (Default::default(), 0);",
        "let locks: [SyncMutex<u8>; 2] = Default::default();",
        "let shared: Arc<SyncMutex<u8>> = Default::default();",
        "let shared = Arc::<SyncMutex<u8>>::default();",
        "let lock = <(SyncMutex<u8>, u8)>::default();",
    ] {
        assert!(constructs(body), "missed {body}");
    }
    for source in [
        "static A: SyncMutex<u8> = SyncMutex::new(0);",
        "const fn make() -> SyncMutex<u8> { SyncMutex::new(0) } const A: SyncMutex<u8> = make();",
        "struct Holder { lock: SyncMutex<u8> }",
        "fn make() -> SyncMutex<u8> { if a { (Default::default()) } else { 0.into() } }",
        "fn take(_: SyncMutex<u8>) {}\nfn m() { take({ Default::default() }); }",
        "struct Holder<T>(T);\nfn m() { let h: Holder<SyncMutex<u8>> = Default::default(); }",
        "type Pair = (SyncMutex<u8>, u8);\nfn m() { let p: Pair = Default::default(); }",
        "trait Make { fn make() -> Self; }\nimpl Make for SyncMutex<u8> { fn make() -> Self { Self::new(0) } }",
    ] {
        assert_eq!(verdict(source), Ok(true), "{source}");
    }
}

// In a file that writes a lock type by value, a context-typed constructor
// whose type the file does not determine is indeterminate; one whose type it
// determines as no lock is clean. A file that writes no lock type leaves such
// types to signatures in other files.
#[test]
fn inferred_types_are_resolved_or_indeterminate() {
    const MENTION: &str = concat!(
        "fn mention(_: Arc<SyncMutex<u8>>) {}\n",
        "use std::default::Default as Fresh;\n",
        "use std::convert::From as Convert;\n",
    );
    for body in [
        "let a = Default::default();",
        "let a: _ = Default::default();",
        "take(Default::default());",
        "let a = Arc::new(Into::into(0));",
        "let a = values.map(Into::into);",
        "let a = <_>::default();",
        "x = Default::default();",
        "let h = Holder { lock: Default::default() };",
        "let a = Fresh::default();",
        "let a = Convert::from(0);",
        "let a = std::mem::take(&mut slot);",
        "let a = slot.unwrap_or_default();",
    ] {
        let source = format!("{MENTION}fn m() {{ {body} }}");
        assert!(indeterminate(&source), "{body}: {:?}", verdict(&source));
        let elsewhere = format!("fn m() {{ {body} }}");
        assert!(
            indeterminate(&elsewhere),
            "{elsewhere}: {:?}",
            verdict(&elsewhere)
        );
    }
    for source in [
        "fn m() { let a: Vec<SyncMutex<u8>> = (0..2).map(|_| 0.into()).collect(); }",
        "fn make<T: Default>() -> T { T::default() }\nfn m() { make::<SyncMutex<u8>>(); }",
        "type Lock = SyncMutex<u8>;\nfn m() { let a = Default::default(); }",
        "fn m() { Mutex::new(()); let unresolved = Default::default(); }",
        "struct S { a: u8 } fn m() { let t = Other { a: 1, ..Default::default() }; }",
    ] {
        assert!(indeterminate(source), "{source}: {:?}", verdict(source));
    }
    for source in [
        "fn m() { let a: u8 = Default::default(); let s: String = \"x\".into(); }",
        "fn take(_: String, _: &str) {}\nfn m() { take(\"x\".into(), &Default::default()); }",
        "struct S { a: u8 }\nfn m() { let s = S { a: 0.into() }; let t = S { a: 1, ..Default::default() }; }",
        "fn make() -> u8 { if a { return 1.into(); } Default::default() }",
        "fn m() { let f = || -> u8 { Default::default() }; }",
        "fn m() { let a: Option<SyncMutex<u8>> = Default::default(); }",
    ] {
        let source = format!("{MENTION}{source}");
        assert_eq!(verdict(&source), Ok(false), "{source}");
    }
    // A borrowed lock is not a lock type written by value.
    let borrowed = "fn m(lock: &SyncMutex<u8>) { let a = Default::default(); }";
    assert!(indeterminate(borrowed));
}

#[test]
fn unseen_item_bodies_are_indeterminate_at_every_nesting_level() {
    for source in [
        "mod hidden; fn check() {}",
        "mod visible { mod hidden; }",
        "#[cfg(any())] #[path = \"elsewhere.rs\"] mod hidden;",
        "unsafe extern \"C\" { fn unknown(); }",
        "extern crate hidden as std;",
    ] {
        assert!(indeterminate(source), "{source}: {:?}", verdict(source));
    }
    assert_eq!(scan("mod visible { fn p() -> u8 { 0 } }").ok(), Some(false));
    assert_eq!(
        scan("mod visible { fn p() { std::sync::Mutex::new(0); } }").ok(),
        Some(true)
    );
}
#[test]
fn lexical_values_and_local_containers_do_not_inherit_library_bindings() {
    for source in [
        "fn consume(_: u8) {} fn p(consume: fn(std::sync::Mutex<()>)) { consume(Default::default()); }",
        "fn helper(_: u8) {} use helper as consume; fn p(consume: fn(std::sync::Mutex<()>)) { consume(Default::default()); }",
        "struct Consume(u8); fn p(Consume: fn(std::sync::Mutex<()>)) { Consume(Default::default()); }",
        "trait Default { fn default() -> std::sync::Mutex<()>; } impl Default for () { fn default() -> std::sync::Mutex<()> { std::default::Default::default() } } fn p() { <() as Default>::default(); }",
        "#[derive(Default)] struct Vec(std::sync::Arc<std::sync::Mutex<()>>); fn p() { Vec::default(); }",
        "#[derive(Default)] struct Option(std::sync::Arc<std::sync::Mutex<()>>); fn p() { Option::default(); }",
        "mod std { pub mod vec { pub struct Vec; } } fn p() { std::vec::Vec::default(); }",
    ] {
        assert!(matches!(scan(source), Ok(true) | Err(_)), "{source}");
    }
    assert!(!scan("#[derive(Default)] struct Vec(u8); fn p() { Vec::default(); }").unwrap());
}
