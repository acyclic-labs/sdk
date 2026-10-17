//! Generated code is outside the scan, so a real model may only use macros and
//! attributes whose expansion is known: a `macro_rules!` definition, or any
//! macro or attribute not in [`MACROS`] or [`ATTRIBUTES`], is a hard error.
//! Each allowed macro's arguments are parsed with that macro's own grammar
//! into statements the alias and construction analysis visits like source;
//! arguments that do not parse with it are indeterminate, never lock free.
use proc_macro2::{Delimiter, Spacing, Span, TokenStream, TokenTree};
use syn::{
    Attribute, Block, Expr, ExprBlock, Item, Lit, Macro, Pat, Stmt, Token, Type,
    parse::{ParseStream, Parser},
    punctuated::Punctuated,
    visit::{self, Visit},
    visit_mut::VisitMut,
};

#[derive(Clone, Copy)]
enum Grammar {
    /// Comma-separated expressions (format arguments parse as assignments).
    Exprs,
    /// `expr; count` or comma-separated expressions.
    Vec,
    /// `expr, pattern (if guard)?`.
    Matches,
    /// `static NAME: Type = init;` items.
    Statics,
    /// Comma-separated literals.
    Literals,
    /// `#![proptest_config(expr)]` then `#[attr] fn name(pat in strategy, ..) { .. }`.
    Proptest,
    /// `(weight =>)? strategy` list.
    OneOf,
    /// JSON with Rust expressions as keys in parentheses and as values.
    Json,
}

/// The macros a real model may invoke, by path with any `std::`, `core::` or
/// `alloc::` prefix removed. A single-segment invocation may also name the
/// last segment of a listed path; a `use` that binds such a name to any other
/// path is an error, so the name cannot be redirected to another macro.
const MACROS: [(&str, Grammar); 27] = [
    ("assert", Grammar::Exprs),
    ("assert_eq", Grammar::Exprs),
    ("assert_ne", Grammar::Exprs),
    ("debug_assert", Grammar::Exprs),
    ("debug_assert_eq", Grammar::Exprs),
    ("debug_assert_ne", Grammar::Exprs),
    ("format", Grammar::Exprs),
    ("print", Grammar::Exprs),
    ("println", Grammar::Exprs),
    ("eprint", Grammar::Exprs),
    ("eprintln", Grammar::Exprs),
    ("write", Grammar::Exprs),
    ("writeln", Grammar::Exprs),
    ("panic", Grammar::Exprs),
    ("unreachable", Grammar::Exprs),
    ("todo", Grammar::Exprs),
    ("vec", Grammar::Vec),
    ("matches", Grammar::Matches),
    ("thread_local", Grammar::Statics),
    ("loom::thread_local", Grammar::Statics),
    ("compile_error", Grammar::Literals),
    ("proptest::proptest", Grammar::Proptest),
    ("proptest::prop_assert", Grammar::Exprs),
    ("proptest::prop_assert_eq", Grammar::Exprs),
    ("proptest::prop_assert_ne", Grammar::Exprs),
    ("proptest::prop_oneof", Grammar::OneOf),
    ("serde_json::json", Grammar::Json),
];

/// Inert or known attributes. `derive` is limited to [`DERIVES`]; a derived
/// `Default` constructs its fields, which the construction analysis covers.
const ATTRIBUTES: [&str; 12] = [
    "test",
    "should_panic",
    "ignore",
    "cfg",
    "path",
    "doc",
    "derive",
    "inline",
    "must_use",
    "track_caller",
    "ts",
    "async_trait::async_trait",
];
const DERIVES: [&str; 9] = [
    "Clone",
    "Copy",
    "Debug",
    "PartialEq",
    "Eq",
    "Hash",
    "PartialOrd",
    "Ord",
    "Default",
];
/// Derives that generate metadata only.
const METADATA_DERIVES: [&str; 1] = ["TS"];

/// Statements parsed from the arguments of every macro in `file`, including
/// macros nested in those arguments, after checking every macro, attribute
/// and macro-name import against the allow-lists.
pub fn fragments(file: &syn::File) -> syn::Result<Vec<Vec<Stmt>>> {
    let mut collector = Collector::default();
    collector.visit_file(file);
    let mut next = 0;
    loop {
        if let Some(error) = collector.error.take() {
            return Err(error);
        }
        let Some(fragment) = collector.fragments.get_mut(next) else {
            return Ok(collector.fragments);
        };
        // Taken out while visited: the visit appends to the same list.
        let stmts = std::mem::take(fragment);
        stmts.iter().for_each(|stmt| collector.visit_stmt(stmt));
        collector.fragments[next] = stmts;
        next += 1;
    }
}

/// Whether a `use` binding `name` to `path` redirects an allowed macro name.
pub fn redirects(path: &[String], name: &str) -> bool {
    let joined = normalized(path);
    MACROS
        .iter()
        .any(|(allowed, _)| allowed.rsplit("::").next() == Some(name))
        && !MACROS.iter().any(|(allowed, _)| *allowed == joined)
}

fn normalized(path: &[String]) -> String {
    let skip = usize::from(path.len() > 1 && ["std", "core", "alloc"].contains(&path[0].as_str()));
    path[skip..].join("::")
}

fn grammar(path: &[String]) -> Option<Grammar> {
    let joined = normalized(path);
    MACROS.iter().find_map(|(allowed, grammar)| {
        let matches = *allowed == joined
            || (path.len() == 1 && allowed.rsplit("::").next() == Some(joined.as_str()));
        matches.then_some(*grammar)
    })
}

#[derive(Default)]
struct Collector {
    fragments: Vec<Vec<Stmt>>,
    error: Option<syn::Error>,
}

impl Collector {
    fn fail(&mut self, message: String) {
        self.error.get_or_insert_with(|| indeterminate(&message));
    }
}

impl Visit<'_> for Collector {
    fn visit_macro(&mut self, mac: &Macro) {
        let path = segments(&mac.path);
        let name = path.join("::");
        if mac.path.is_ident("macro_rules") {
            self.fail(
                "a macro_rules! definition in a real model is generated code the scan cannot see"
                    .into(),
            );
        } else if let Some(grammar) = grammar(&path) {
            match parse(grammar, mac.tokens.clone()) {
                Ok(mut stmts) => {
                    stmts
                        .iter_mut()
                        .for_each(|stmt| crate::Normalize.visit_stmt_mut(stmt));
                    self.fragments.push(stmts);
                }
                Err(error) => {
                    self.error.get_or_insert_with(|| {
                        indeterminate(&format!(
                            "{name}! arguments do not parse with its grammar: {error}"
                        ))
                    });
                }
            }
        } else {
            self.fail(format!(
                "{name}! is not an allow-listed macro for real models; write the code it would generate"
            ));
        }
        visit::visit_macro(self, mac);
    }

    fn visit_attribute(&mut self, attribute: &Attribute) {
        let name = segments(attribute.path()).join("::");
        if !ATTRIBUTES.contains(&name.as_str()) {
            self.fail(format!(
                "#[{name}] is not an allow-listed attribute for real models"
            ));
        } else if name == "derive" {
            let derived =
                attribute.parse_args_with(Punctuated::<syn::Path, Token![,]>::parse_terminated);
            match derived {
                Ok(paths) => {
                    for path in paths {
                        let derive = segments(&path).join("::");
                        if !DERIVES.contains(&derive.as_str())
                            && !METADATA_DERIVES.contains(&derive.as_str())
                        {
                            self.fail(format!(
                                "derive({derive}) is not an allow-listed derive for real models"
                            ));
                        }
                    }
                }
                Err(error) => self.fail(format!("derive does not parse: {error}")),
            }
        }
        visit::visit_attribute(self, attribute);
    }
}

fn segments(path: &syn::Path) -> Vec<String> {
    path.segments
        .iter()
        .map(|segment| crate::name(&segment.ident))
        .collect()
}

fn hard(message: &str) -> syn::Error {
    syn::Error::new(Span::call_site(), message)
}

pub fn indeterminate(message: &str) -> syn::Error {
    hard(&format!("indeterminate: {message}"))
}

fn expr(expr: Expr) -> Stmt {
    Stmt::Expr(expr, None)
}

fn exprs(input: ParseStream<'_>) -> syn::Result<Vec<Stmt>> {
    let list = Punctuated::<Expr, Token![,]>::parse_terminated(input)?;
    Ok(list.into_iter().map(expr).collect())
}

fn parse(grammar: Grammar, tokens: TokenStream) -> syn::Result<Vec<Stmt>> {
    match grammar {
        Grammar::Exprs => exprs.parse2(tokens),
        Grammar::Vec => (|input: ParseStream<'_>| {
            let first: Expr = input.parse()?;
            input.parse::<Token![;]>()?;
            let count: Expr = input.parse()?;
            Ok(vec![expr(first), expr(count)])
        })
        .parse2(tokens.clone())
        .or_else(|_| exprs.parse2(tokens)),
        Grammar::Matches => (|input: ParseStream<'_>| {
            let scrutinee: Expr = input.parse()?;
            input.parse::<Token![,]>()?;
            let pattern = Pat::parse_multi_with_leading_vert(input)?;
            let mut stmts = vec![syn::parse2(quote::quote!(let (#pattern) = #scrutinee;))?];
            if input.parse::<Option<Token![if]>>()?.is_some() {
                stmts.push(expr(input.parse()?));
            }
            input.parse::<Option<Token![,]>>()?;
            Ok(stmts)
        })
        .parse2(tokens),
        Grammar::Statics => statics(tokens),
        Grammar::Literals => {
            Punctuated::<Lit, Token![,]>::parse_terminated.parse2(tokens)?;
            Ok(Vec::new())
        }
        Grammar::Proptest => proptest.parse2(tokens),
        Grammar::OneOf => (|input: ParseStream<'_>| {
            let mut stmts = Vec::new();
            while !input.is_empty() {
                stmts.push(expr(input.parse()?));
                if input.parse::<Option<Token![=>]>>()?.is_some() {
                    stmts.push(expr(input.parse()?));
                }
                if input.parse::<Option<Token![,]>>()?.is_none() {
                    break;
                }
            }
            Ok(stmts)
        })
        .parse2(tokens),
        Grammar::Json => {
            let mut stmts = Vec::new();
            json(tokens, &mut stmts)?;
            Ok(stmts)
        }
    }
}

// `thread_local!` accepts a final item without its semicolon.
fn statics(tokens: TokenStream) -> syn::Result<Vec<Stmt>> {
    let items = |input: ParseStream<'_>| {
        let mut stmts = Vec::new();
        while !input.is_empty() {
            let item: Item = input.parse()?;
            if !matches!(item, Item::Static(_)) {
                return Err(input.error("expected a static item"));
            }
            stmts.push(Stmt::Item(item));
        }
        Ok(stmts)
    };
    items.parse2(tokens.clone()).or_else(|_| {
        let mut terminated = tokens;
        terminated.extend(quote_semicolon());
        items.parse2(terminated)
    })
}

fn quote_semicolon() -> TokenStream {
    TokenTree::Punct(proc_macro2::Punct::new(';', Spacing::Alone)).into()
}

fn proptest(input: ParseStream<'_>) -> syn::Result<Vec<Stmt>> {
    let mut stmts = Vec::new();
    for attribute in Attribute::parse_inner(input)? {
        if !attribute.path().is_ident("proptest_config") {
            return Err(input.error("only #![proptest_config(..)] configures proptest!"));
        }
        stmts.push(expr(attribute.parse_args()?));
    }
    while !input.is_empty() {
        let attrs = Attribute::parse_outer(input)?;
        input.parse::<Token![fn]>()?;
        input.parse::<syn::Ident>()?;
        let arguments;
        syn::parenthesized!(arguments in input);
        while !arguments.is_empty() {
            let pattern = Pat::parse_single(&arguments)?;
            if arguments.parse::<Option<Token![in]>>()?.is_some() {
                stmts.push(expr(arguments.parse()?));
            } else {
                arguments.parse::<Token![:]>()?;
                let ty: Type = arguments.parse()?;
                stmts.push(syn::parse2(quote::quote!(let #pattern: #ty;))?);
            }
            if arguments.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
        }
        let block: Block = input.parse()?;
        stmts.push(expr(Expr::Block(ExprBlock {
            attrs,
            label: None,
            block,
        })));
    }
    Ok(stmts)
}

// Splits at top-level commas; groups keep their contents.
fn split(tokens: TokenStream) -> Vec<Vec<TokenTree>> {
    let mut parts = vec![Vec::new()];
    for tree in tokens {
        match &tree {
            TokenTree::Punct(comma) if comma.as_char() == ',' => parts.push(Vec::new()),
            _ => parts
                .last_mut()
                .into_iter()
                .for_each(|part| part.push(tree.clone())),
        }
    }
    parts.retain(|part| !part.is_empty());
    parts
}

fn json(tokens: TokenStream, stmts: &mut Vec<Stmt>) -> syn::Result<()> {
    let trees: Vec<TokenTree> = tokens.clone().into_iter().collect();
    match trees.as_slice() {
        [TokenTree::Group(array)] if array.delimiter() == Delimiter::Bracket => {
            for element in split(array.stream()) {
                json(element.into_iter().collect(), stmts)?;
            }
        }
        [TokenTree::Group(object)] if object.delimiter() == Delimiter::Brace => {
            for entry in split(object.stream()) {
                // The key ends at the first lone `:` (a `::` is joint).
                let colon = entry.iter().enumerate().position(|(at, tree)| {
                    matches!(tree, TokenTree::Punct(p) if p.as_char() == ':' && p.spacing() == Spacing::Alone)
                        && !matches!(at.checked_sub(1).and_then(|before| entry.get(before)),
                            Some(TokenTree::Punct(p)) if p.as_char() == ':' && p.spacing() == Spacing::Joint)
                });
                let Some(colon) = colon else {
                    return Err(hard("a json! object entry has no `key: value`"));
                };
                let key: TokenStream = entry[..colon].iter().cloned().collect();
                if let [TokenTree::Group(group)] = &entry[..colon]
                    && group.delimiter() == Delimiter::Parenthesis
                {
                    stmts.push(expr(syn::parse2(group.stream())?));
                } else {
                    stmts.push(expr(syn::parse2::<Expr>(key)?));
                }
                json(entry[colon + 1..].iter().cloned().collect(), stmts)?;
            }
        }
        _ => stmts.push(expr(syn::parse2(tokens)?)),
    }
    Ok(())
}
