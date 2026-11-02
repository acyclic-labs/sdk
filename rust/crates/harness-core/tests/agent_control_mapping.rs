//! Structural drift check; execution traces must separately check shell effects.
use std::collections::{BTreeMap, BTreeSet};
use syn::{Expr, Item, Member, visit::Visit};

#[derive(Default)]
struct Calls {
    paths: BTreeSet<Vec<String>>,
    actions: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for Calls {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        match &*call.func {
            Expr::Path(path) => {
                self.paths.insert(
                    path.path
                        .segments
                        .iter()
                        .map(|s| s.ident.to_string())
                        .collect(),
                );
            }
            Expr::Paren(paren) => {
                if let Expr::Field(field) = &*paren.expr
                    && let Expr::Path(base) = &*field.base
                    && base.path.is_ident("kernel")
                    && let Member::Named(name) = &field.member
                {
                    self.actions.insert(name.to_string());
                }
            }
            _ => {}
        }
        syn::visit::visit_expr_call(self, call);
    }
}

fn mapping(kernel: &str, model: &str, shell: &str, harness: &str) -> Result<(), String> {
    let parse = |source| syn::parse_file(source).map_err(|error| error.to_string());
    let kernel = parse(kernel)?;
    let model = parse(model)?;
    let shell = parse(shell)?;
    let harness = parse(harness)?;
    let original = harness.items.iter().any(|item| {
        let Item::Mod(module) = item else {
            return false;
        };
        module.ident == "production"
            && module.attrs.iter().any(|attr| {
                let syn::Meta::NameValue(value) = &attr.meta else {
                    return false;
                };
                let Expr::Lit(literal) = &value.value else {
                    return false;
                };
                let syn::Lit::Str(path) = &literal.lit else {
                    return false;
                };
                value.path.is_ident("path") && path.value() == "../../src/agent/kernel.rs"
            })
    });
    if !original {
        return Err("proof does not include the production kernel".into());
    }
    let original_model = harness.items.iter().any(|item| {
        let Item::Macro(item) = item else {
            return false;
        };
        item.mac.path.is_ident("include")
            && syn::parse2::<syn::LitStr>(item.mac.tokens.clone())
                .is_ok_and(|path| path.value() == "agent_control_model.rs")
    });
    if !original_model {
        return Err("proof does not include the mapped model".into());
    }
    let mut functions = BTreeMap::new();
    let mut public = BTreeSet::new();
    for item in &kernel.items {
        if let Item::Fn(function) = item {
            let name = function.sig.ident.to_string();
            if matches!(function.vis, syn::Visibility::Public(_)) {
                public.insert(name.clone());
            }
            let mut calls = Calls::default();
            calls.visit_block(&function.block);
            functions.insert(name, calls.paths);
        }
    }
    let fields: BTreeSet<_> = model
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "Kernel" => Some(item),
            _ => None,
        })
        .flat_map(|item| {
            item.fields
                .iter()
                .filter_map(|f| f.ident.as_ref().map(ToString::to_string))
        })
        .collect();
    let mut bindings = BTreeSet::new();
    for item in &model.items {
        let Item::Const(item) = item else { continue };
        if item.ident != "PRODUCTION" {
            continue;
        }
        let Expr::Struct(value) = &*item.expr else {
            return Err("production binding is not explicit".into());
        };
        for field in &value.fields {
            let Member::Named(name) = &field.member else {
                return Err("unnamed action".into());
            };
            let Expr::Path(target) = &field.expr else {
                return Err("action is not a production function".into());
            };
            let path: Vec<_> = target
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            if path != ["production".to_owned(), name.to_string()]
                || !public.contains(&name.to_string())
            {
                return Err("action binding differs from its production function".into());
            }
            if !bindings.insert(name.to_string()) {
                return Err("duplicate action binding".into());
            }
        }
    }
    let mut calls = Calls::default();
    calls.visit_file(&model);
    if fields.is_empty() || fields != bindings || fields != calls.actions {
        return Err("model action declaration, binding and invocation differ".into());
    }
    let mut reachable = bindings;
    loop {
        let mut next = reachable.clone();
        for name in &reachable {
            for path in &functions[name] {
                if path.len() == 1 && functions.contains_key(&path[0]) {
                    next.insert(path[0].clone());
                }
            }
        }
        if next == reachable {
            break;
        }
        reachable = next;
    }
    let mut calls = Calls::default();
    calls.visit_file(&shell);
    let shipping: BTreeSet<_> = calls
        .paths
        .iter()
        .filter(|p| p.len() == 2 && p[0] == "kernel")
        .map(|p| p[1].clone())
        .collect();
    if shipping != public || !shipping.is_subset(&reachable) {
        return Err("shipping kernel calls and model transition coverage differ".into());
    }
    Ok(())
}

#[test]
fn agent_control_mapping_is_bidirectional() -> Result<(), String> {
    let kernel = include_str!("../src/agent/kernel.rs");
    let model = include_str!("formal/agent_control_model.rs");
    let shell = include_str!("../src/agent/machine.rs");
    let harness = include_str!("formal/agent_control.rs");
    mapping(kernel, model, shell, harness)?;
    // Mutate actual source, independently breaking each mapping edge. These
    // are structural negatives, not fabricated execution or tool fixtures.
    let changed = model.replacen("alarm: production::alarm,", "alarm: production::pick,", 1);
    assert_ne!(changed, model);
    assert_eq!(
        mapping(kernel, &changed, shell, harness).unwrap_err(),
        "action binding differs from its production function"
    );
    let changed = model.replacen("(kernel.satisfy)", "(kernel.admit)", 1);
    assert_ne!(changed, model);
    assert_eq!(
        mapping(kernel, &changed, shell, harness).unwrap_err(),
        "model action declaration, binding and invocation differ"
    );
    let changed = shell.replacen("kernel::alarm(", "kernel::unmapped_alarm(", 1);
    assert_ne!(changed, shell);
    assert_eq!(
        mapping(kernel, model, &changed, harness).unwrap_err(),
        "shipping kernel calls and model transition coverage differ"
    );
    let changed = format!("{kernel}\npub const fn unmapped() -> bool {{ false }}");
    assert_eq!(
        mapping(&changed, model, shell, harness).unwrap_err(),
        "shipping kernel calls and model transition coverage differ"
    );
    let changed = harness.replacen("../../src/agent/kernel.rs", "parallel_kernel.rs", 1);
    assert_ne!(changed, harness);
    assert_eq!(
        mapping(kernel, model, shell, &changed).unwrap_err(),
        "proof does not include the production kernel"
    );
    let changed = harness.replacen("agent_control_model.rs", "parallel_model.rs", 1);
    assert_ne!(changed, harness);
    assert_eq!(
        mapping(kernel, model, shell, &changed).unwrap_err(),
        "proof does not include the mapped model"
    );
    Ok(())
}
