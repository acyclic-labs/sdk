//! Structural drift check; execution traces must separately check shell effects.
use acyclic_test_support::correspondence::{self, Inventory, Mapping};
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

fn extract(
    kernel: &str,
    model: &str,
    shell: &str,
    harness: &str,
) -> Result<(Inventory, Mapping), String> {
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
    let mut bindings = BTreeMap::new();
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
            if bindings
                .insert(name.to_string(), name.to_string())
                .is_some()
            {
                return Err("duplicate action binding".into());
            }
        }
    }
    let mut calls = Calls::default();
    calls.visit_file(&model);
    let invoked = calls.actions;
    let mut calls = Calls::default();
    calls.visit_file(&shell);
    let shipping: BTreeSet<_> = calls
        .paths
        .iter()
        .filter(|p| p.len() == 2 && p[0] == "kernel")
        .map(|p| p[1].clone())
        .collect();
    if shipping != public {
        return Err("shipping kernel calls and model transition coverage differ".into());
    }
    let functions = functions
        .iter()
        .map(|(name, paths)| {
            let internal = paths
                .iter()
                .filter(|path| path.len() == 1 && functions.contains_key(&path[0]))
                .map(|path| path[0].clone())
                .collect();
            (name.clone(), internal)
        })
        .collect();
    Ok((
        Inventory {
            actions: fields,
            invoked,
            functions,
            shipping,
        },
        Mapping {
            bindings,
            exclusions: BTreeMap::new(),
        },
    ))
}

fn mapping(kernel: &str, model: &str, shell: &str, harness: &str) -> Result<(), String> {
    let (inventory, mapping) = extract(kernel, model, shell, harness)?;
    correspondence::validate(&inventory, &mapping)
        .map(|_| ())
        .map_err(|error| match error {
            correspondence::Error::ActionCoverage => {
                "model action declaration, binding and invocation differ".into()
            }
            _ => error.to_string(),
        })
}

#[cfg(feature = "test-support")]
pub(crate) fn original() -> Result<(Inventory, Mapping), String> {
    let (inventory, mapping) = extract(
        include_str!("../src/agent/kernel.rs"),
        include_str!("formal/agent_control_model.rs"),
        include_str!("../src/agent/machine.rs"),
        include_str!("formal/agent_control.rs"),
    )?;
    trace_mapping(include_str!("../src/agent/control_trace.rs"), &inventory)?;
    Ok((inventory, mapping))
}

fn trace_mapping(source: &str, inventory: &Inventory) -> Result<(), String> {
    let file = syn::parse_file(source).map_err(|error| error.to_string())?;
    let variants: BTreeSet<_> = file
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Enum(item) if item.ident == "Decision" => Some(item),
            _ => None,
        })
        .flat_map(|item| {
            item.variants
                .iter()
                .map(|variant| variant.ident.to_string().to_lowercase())
        })
        .collect();
    if variants != inventory.actions {
        return Err("trace decisions and model actions differ".into());
    }
    let mut methods = BTreeSet::new();
    for item in &file.items {
        let Item::Impl(item) = item else { continue };
        let syn::Type::Path(target) = &*item.self_ty else {
            continue;
        };
        if !target.path.is_ident("Decision") {
            continue;
        }
        for item in &item.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            let name = method.sig.ident.to_string();
            if name != "action" && name != "conforms" {
                continue;
            }
            methods.insert(name.clone());
            let Some(syn::Stmt::Expr(Expr::Match(arms), _)) = method.block.stmts.last() else {
                return Err("trace adapter is not an explicit action match".into());
            };
            let mut matched = BTreeSet::new();
            for arm in &arms.arms {
                let syn::Pat::TupleStruct(pattern) = &arm.pat else {
                    return Err("trace adapter has an implicit action".into());
                };
                let action = pattern
                    .path
                    .segments
                    .last()
                    .ok_or("trace action missing")?
                    .ident
                    .to_string()
                    .to_lowercase();
                if !matched.insert(action.clone()) {
                    return Err("trace adapter repeats an action".into());
                }
                if name == "action" {
                    let Expr::Lit(value) = &*arm.body else {
                        return Err("trace action name missing".into());
                    };
                    if !matches!(&value.lit, syn::Lit::Str(value) if value.value() == action) {
                        return Err("trace action name differs from its decision".into());
                    }
                } else {
                    let mut calls = Calls::default();
                    calls.visit_expr(&arm.body);
                    let kernels: BTreeSet<_> = calls
                        .paths
                        .into_iter()
                        .filter(|path| path.first().is_some_and(|name| name == "kernel"))
                        .collect();
                    if kernels != BTreeSet::from([vec!["kernel".into(), action]]) {
                        return Err("trace replay differs from its proof kernel binding".into());
                    }
                }
            }
            if matched != inventory.actions {
                return Err("trace adapter does not cover every action".into());
            }
        }
    }
    if methods != BTreeSet::from(["action".into(), "conforms".into()]) {
        return Err("trace adapter is missing its original action or replay method".into());
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
    // Exercise the shared checker with mutations of the independently extracted
    // shipping graph, rather than a second handwritten model of that graph.
    let (inventory, mapped) = extract(kernel, model, shell, harness)?;
    let trace = include_str!("../src/agent/control_trace.rs");
    trace_mapping(trace, &inventory)?;
    let changed = trace.replacen("=> \"admit\"", "=> \"pick\"", 1);
    assert_ne!(changed, trace);
    assert_eq!(
        trace_mapping(&changed, &inventory).unwrap_err(),
        "trace action name differs from its decision"
    );
    let changed = trace.replacen("kernel::satisfy(control)", "kernel::admit(control)", 1);
    assert_ne!(changed, trace);
    assert_eq!(
        trace_mapping(&changed, &inventory).unwrap_err(),
        "trace replay differs from its proof kernel binding"
    );
    let mut changed = mapped.clone();
    changed.bindings.insert("alarm".into(), "absent".into());
    assert_eq!(
        correspondence::validate(&inventory, &changed),
        Err(correspondence::Error::UnknownSymbol("absent".into()))
    );
    let mut changed = inventory.clone();
    changed
        .functions
        .get_mut("settle")
        .unwrap()
        .insert("absent".into());
    assert_eq!(
        correspondence::validate(&changed, &mapped),
        Err(correspondence::Error::MissingCallee {
            caller: "settle".into(),
            callee: "absent".into()
        })
    );
    let mut changed = inventory.clone();
    changed.functions.insert("unmapped".into(), BTreeSet::new());
    assert_eq!(
        correspondence::validate(&changed, &mapped),
        Err(correspondence::Error::UncoveredSymbol("unmapped".into()))
    );
    let mut excluded = mapped.clone();
    excluded.exclusions.insert("unmapped".into(), String::new());
    assert_eq!(
        correspondence::validate(&changed, &excluded),
        Err(correspondence::Error::InvalidExclusion("unmapped".into()))
    );
    excluded.exclusions.insert(
        "unmapped".into(),
        "not a transition; reviewed explicitly".into(),
    );
    correspondence::validate(&changed, &excluded).map_err(|error| error.to_string())?;
    let mut changed = inventory.clone();
    for callees in changed.functions.values_mut() {
        callees.remove("report");
    }
    assert_eq!(
        correspondence::validate(&changed, &mapped),
        Err(correspondence::Error::UnobservedEntry("report".into()))
    );
    let mut changed = mapped;
    changed
        .exclusions
        .insert("alarm".into(), "stale exclusion".into());
    assert_eq!(
        correspondence::validate(&inventory, &changed),
        Err(correspondence::Error::InvalidExclusion("alarm".into()))
    );
    Ok(())
}
