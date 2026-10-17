//! Shrinking constructor, alias and wrapper programs must report a lock or
//! explicit uncertainty. Their non-lock counterparts use resolved local
//! functions and standard Cell types and must report zero. Parse/print must
//! preserve the verdict. Regression seeds are checked in alongside the suite.
use super::scan;
use proptest::prelude::*;

const PREFIXES: [&str; 5] = [
    "",
    "crate::sync::",
    "std::sync::",
    "loom::sync::",
    "tokio::sync::",
];
const LOCKS: [&str; 5] = ["Mutex", "RwLock", "Condvar", "SyncMutex", "AsyncMutex"];

#[derive(Clone, Debug)]
enum Link {
    /// `type A<T> = Current<T>;`
    Alias,
    /// `use Current as R;`
    Rename,
}

#[derive(Clone, Debug)]
enum Wrap {
    Paren,
    Block,
    Unsafe,
    If,
    Match,
    Statement,
}

#[derive(Clone, Debug)]
enum Form {
    New,
    QualifiedDefault,
    TraitDefault,
    /// `let lock: T = init;` with each context-typed initializer.
    Typed(Init),
    /// The tail of `fn make() -> T`.
    Returned(Init),
    /// An argument of `fn take(_: T)`.
    Argument(Init),
    /// A field of `struct Holder { lock: T }`.
    Field(Init),
    /// The first element of a `(T, u8)` binding.
    Tuple(Init),
    /// `let shared: Arc<T> = init;`: `Default` of an `Arc` makes its lock.
    Shared,
}

#[derive(Clone, Copy, Debug)]
enum Init {
    Default,
    From,
    Into,
    MethodInto,
}

#[derive(Clone, Debug)]
struct Case {
    prefix: usize,
    lock: usize,
    chain: Vec<Link>,
    form: Form,
    wraps: Vec<Wrap>,
}

fn init() -> impl Strategy<Value = Init> {
    prop_oneof![
        Just(Init::Default),
        Just(Init::From),
        Just(Init::Into),
        Just(Init::MethodInto),
    ]
}

fn case() -> impl Strategy<Value = Case> {
    let link = prop_oneof![Just(Link::Alias), Just(Link::Rename)];
    let wrap = prop_oneof![
        Just(Wrap::Paren),
        Just(Wrap::Block),
        Just(Wrap::Unsafe),
        Just(Wrap::If),
        Just(Wrap::Match),
        Just(Wrap::Statement),
    ];
    let form = prop_oneof![
        Just(Form::New),
        Just(Form::QualifiedDefault),
        Just(Form::TraitDefault),
        init().prop_map(Form::Typed),
        init().prop_map(Form::Returned),
        init().prop_map(Form::Argument),
        init().prop_map(Form::Field),
        init().prop_map(Form::Tuple),
        Just(Form::Shared),
    ];
    (
        0..PREFIXES.len(),
        0..LOCKS.len(),
        prop::collection::vec(link, 0..4),
        form,
        prop::collection::vec(wrap, 0..4),
    )
        .prop_map(|(prefix, lock, chain, form, wraps)| Case {
            prefix,
            lock,
            chain,
            form,
            wraps,
        })
}

impl Case {
    /// The program over `name` (a lock, or a non-lock stand-in).
    fn render(&self, name: &str) -> String {
        let mut items = "fn ready() -> bool { true }\n".to_string();
        let mut current = if name == "Cell" {
            "std::cell::Cell".to_string()
        } else {
            format!("{}{name}", PREFIXES[self.prefix])
        };
        for (index, link) in self.chain.iter().enumerate() {
            match link {
                Link::Alias => {
                    items.push_str(&format!("type Alias{index}<T> = {current}<T>;\n"));
                    current = format!("Alias{index}");
                }
                Link::Rename => {
                    let path = if current.contains("::") {
                        current.clone()
                    } else {
                        format!("self::{current}")
                    };
                    items.push_str(&format!("use {path} as Renamed{index};\n"));
                    current = format!("Renamed{index}");
                }
            }
        }
        let ty = format!("{current}<u8>");
        let wrap = |value: &str| {
            self.wraps
                .iter()
                .fold(value.to_string(), |value, wrap| match wrap {
                    Wrap::Paren => format!("({value})"),
                    Wrap::Block => format!("{{ {value} }}"),
                    Wrap::Unsafe => format!("unsafe {{ {value} }}"),
                    Wrap::If => format!("if ready() {{ {value} }} else {{ {value} }}"),
                    Wrap::Match => format!("match 0 {{ 0 => {value}, _ => {value} }}"),
                    Wrap::Statement => format!("{{ let _unrelated = 1; {value} }}"),
                })
        };
        let made = |init: Init| {
            wrap(match init {
                Init::Default => "Default::default()",
                Init::From => "From::from(0)",
                Init::Into => "Into::into(0)",
                Init::MethodInto => "0.into()",
            })
        };
        let body = match &self.form {
            Form::New => format!("let _lock = {};", wrap(&format!("{current}::new(0)"))),
            Form::QualifiedDefault => {
                format!("let _lock = {};", wrap(&format!("<{ty}>::default()")))
            }
            Form::TraitDefault => {
                format!(
                    "let _lock = {};",
                    wrap(&format!("<{ty} as Default>::default()"))
                )
            }
            Form::Typed(init) => format!("let _lock: {ty} = {};", made(*init)),
            Form::Returned(init) => {
                items.push_str(&format!("fn make() -> {ty} {{ {} }}\n", made(*init)));
                "let _lock = make();".into()
            }
            Form::Argument(init) => {
                items.push_str(&format!("fn take(_lock: {ty}) {{}}\n"));
                format!("take({});", made(*init))
            }
            Form::Field(init) => {
                items.push_str(&format!("struct Holder {{ lock: {ty} }}\n"));
                format!("let _holder = Holder {{ lock: {} }};", made(*init))
            }
            Form::Tuple(init) => {
                format!(
                    "let _pair: ({ty}, u8) = {};",
                    wrap(&format!("({}, 0)", made(*init)))
                )
            }
            Form::Shared => {
                format!(
                    "let _shared: std::sync::Arc<{ty}> = {};",
                    made(Init::Default)
                )
            }
        };
        format!("{items}fn model() {{ {body} }}\n")
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn every_generated_construction_is_found(case in case()) {
        let source = case.render(LOCKS[case.lock]);
        let file = syn::parse_file(&source).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let rendered = quote::quote!(#file).to_string();
        let verdict = scan(&source).map_err(|error| error.to_string());
        prop_assert!(verdict == Ok(true) || verdict.as_ref().is_err_and(|error| error.starts_with("indeterminate:")), "missed:\n{}: {:?}", source, verdict);
        prop_assert_eq!(scan(&source).ok(), scan(&rendered).ok(), "round trip changed the verdict");
    }

    #[test]
    fn the_same_programs_over_a_non_lock_construct_nothing(case in case()) {
        let source = case.render("Cell");
        let verdict = scan(&source).map_err(|error| error.to_string());
        prop_assert_eq!(verdict, Ok(false), "flagged:\n{}", source);
    }

    #[test]
    fn constructor_spellings_never_become_a_lock_free_verdict(
        raw_type in any::<bool>(), raw_method in any::<bool>(),
        aliases in 0usize..5, wrapper in 0usize..7,
    ) {
        let lock = if raw_type { "r#Mutex" } else { "Mutex" };
        let method = if raw_method { "r#default" } else { "default" };
        let ty = format!("std::sync::{lock}<u8>");
        let mut prefix = "use std::default::Default as D0;\n".to_string();
        for index in 0..aliases {
            prefix.push_str(&format!("use D{index} as D{};\n", index + 1));
        }
        let constructor = format!("D{aliases}::{method}()");
        let source = match wrapper {
            0 => format!("{prefix}fn make() -> {ty} {{ {constructor} }}"),
            1 => format!("proptest::proptest! {{ #[test] fn p(lock: {ty}) {{}} }}"),
            2 => format!("fn p() {{ serde_json::json!({{ {{ let lock = std::sync::{lock}::new(0u8); \"key\" }}: 0 }}); }}"),
            3 => format!("trait Factory {{ type Lock; }} impl Factory for () {{ type Lock = {ty}; }} fn p() {{ <() as Factory>::Lock::{method}(); }}"),
            4 => format!("proptest::proptest! {{ #[test] fn p(lock: Vec<{ty}>) {{}} }}"),
            5 => format!("mod a {{ use std::cell::Cell as Value; }} mod b {{ type Value = {ty}; fn p() {{ Value::{method}(); }} }}"),
            _ => format!("use std::mem::take as T0; {} fn make(slot: &mut {ty}) -> {ty} {{ T{aliases}(slot) }}",
                (0..aliases).map(|index| format!("use T{index} as T{};", index + 1)).collect::<String>()),
        };
        match scan(&source) {
            Ok(true) => {},
            Err(error) if error.to_string().starts_with("indeterminate:") => {},
            other => prop_assert!(false, "false clean or unexpected error: {:?}\n{}", other, source),
        }
        prop_assert_eq!(scan("fn plain() -> u8 { let value: u8 = Default::default(); value }").ok(), Some(false));
    }
}
