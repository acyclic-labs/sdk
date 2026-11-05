//! Structural drift check; execution traces must separately check shell effects.
use acyclic_test_support::correspondence::{self, Inventory, Mapping};
use std::collections::{BTreeMap, BTreeSet};
use syn::{Expr, Item, Member, visit::Visit};

fn call_path(call: &syn::ExprCall) -> Vec<String> {
    match &*call.func {
        Expr::Path(path) => path
            .path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect(),
        _ => Vec::new(),
    }
}

#[derive(Default)]
struct Calls {
    paths: BTreeSet<Vec<String>>,
    actions: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for Calls {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        match &*call.func {
            Expr::Path(_) => {
                self.paths.insert(call_path(call));
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
    trace_call_sites(include_str!("../src/agent/machine.rs"), &inventory)?;
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

/// Each original call must record its own arguments and bound result before
/// another statement can lower or change them. Set coverage alone cannot show
/// this: a second call to an already mapped helper can otherwise go unrecorded.
/// The recorded agent must also own the control snapshot passed to that call.
fn trace_call_sites(source: &str, inventory: &Inventory) -> Result<(), String> {
    fn name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Path(path) => path.path.get_ident().map(ToString::to_string),
            _ => None,
        }
    }
    fn bound(pattern: &syn::Pat) -> Option<Vec<String>> {
        match pattern {
            syn::Pat::Ident(id) if id.subpat.is_none() => Some(vec![id.ident.to_string()]),
            syn::Pat::Tuple(tuple) => tuple
                .elems
                .iter()
                .map(bound)
                .collect::<Option<Vec<_>>>()
                .map(|parts| parts.into_iter().flatten().collect()),
            _ => None,
        }
    }
    fn result(expr: &Expr) -> Option<Vec<String>> {
        match expr {
            Expr::Tuple(tuple) => tuple.elems.iter().map(name).collect(),
            expr => name(expr).map(|name| vec![name]),
        }
    }
    fn owner(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Reference(reference) => owner(&reference.expr),
            expr => name(expr),
        }
    }
    fn binds(pattern: &syn::Pat, name: &str) -> bool {
        struct Binding<'a> {
            name: &'a str,
            found: bool,
        }
        impl<'ast> Visit<'ast> for Binding<'_> {
            fn visit_pat_ident(&mut self, pattern: &'ast syn::PatIdent) {
                self.found |= pattern.ident == self.name;
                syn::visit::visit_pat_ident(self, pattern);
            }
            fn visit_pat(&mut self, pattern: &'ast syn::Pat) {
                if matches!(pattern, syn::Pat::Macro(_)) {
                    // An unexpanded binding cannot establish source correspondence.
                    self.found = true;
                } else {
                    syn::visit::visit_pat(self, pattern);
                }
            }
        }
        let mut binding = Binding { name, found: false };
        binding.visit_pat(pattern);
        binding.found
    }
    #[derive(Default)]
    struct Sites {
        calls: usize,
        records: usize,
        paired: usize,
        actions: BTreeSet<String>,
        error: Option<String>,
    }
    impl<'ast> Visit<'ast> for Sites {
        fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
            let names = call_path(call);
            if names.len() == 2 && names[0] == "kernel" {
                self.calls += 1;
            } else if names == ["control_trace", "record"] {
                self.records += 1;
            }
            syn::visit::visit_expr_call(self, call);
        }

        fn visit_block(&mut self, block: &'ast syn::Block) {
            for (index, statement) in block.stmts.iter().enumerate() {
                let syn::Stmt::Local(local) = statement else {
                    continue;
                };
                let Some(initializer) = &local.init else {
                    continue;
                };
                let Expr::Call(call) = &*initializer.expr else {
                    continue;
                };
                let names = call_path(call);
                if names.len() != 2 || names[0] != "kernel" {
                    continue;
                }
                let action = &names[1];
                let paired = (|| {
                    let syn::Stmt::Expr(Expr::Call(record), _) = block.stmts.get(index + 1)? else {
                        return None;
                    };
                    if call_path(record) != ["control_trace", "record"] || record.args.len() != 2 {
                        return None;
                    }
                    let control = name(call.args.first()?)?;
                    let (snapshot_index, snapshot) =
                        block.stmts[..index].iter().enumerate().rev().find_map(
                            |(index, statement)| {
                                let syn::Stmt::Local(local) = statement else {
                                    return None;
                                };
                                binds(&local.pat, &control).then_some((index, local))
                            },
                        )?;
                    let Expr::Call(snapshot) = &*snapshot.init.as_ref()?.expr else {
                        return None;
                    };
                    let recorded_owner = owner(record.args.first()?)?;
                    if call_path(snapshot) != ["control"]
                        || snapshot.args.len() != 1
                        || owner(snapshot.args.first()?).as_deref() != Some(recorded_owner.as_str())
                        || block.stmts[snapshot_index + 1..=index]
                            .iter()
                            .any(|statement| {
                                matches!(statement, syn::Stmt::Local(local) if binds(&local.pat, &recorded_owner))
                            })
                    {
                        return None;
                    }
                    let Expr::Call(decision) = record.args.last()? else {
                        return None;
                    };
                    let recorded = call_path(decision);
                    if recorded.len() != 2
                        || recorded[0] != "Decision"
                        || recorded[1].to_lowercase() != *action
                    {
                        return None;
                    }
                    if decision.args.len() != call.args.len() + 1 {
                        return None;
                    }
                    let expected: Option<Vec<_>> = call.args.iter().map(name).collect();
                    let actual: Option<Vec<_>> = decision
                        .args
                        .iter()
                        .take(call.args.len())
                        .map(name)
                        .collect();
                    if expected.is_none()
                        || actual != expected
                        || result(decision.args.last()?)? != bound(&local.pat)?
                    {
                        return None;
                    }
                    Some(())
                })();
                if paired.is_some() {
                    self.paired += 1;
                    self.actions.insert(action.clone());
                } else if self.error.is_none() {
                    self.error = Some(format!(
                        "shipping {action} call lost its adjacent original input/result record"
                    ));
                }
            }
            syn::visit::visit_block(self, block);
        }
    }
    let file = syn::parse_file(source).map_err(|error| error.to_string())?;
    let mut sites = Sites::default();
    sites.visit_file(&file);
    if let Some(error) = sites.error {
        return Err(error);
    }
    if sites.calls != sites.paired
        || sites.records != sites.paired
        || sites.actions != inventory.actions
    {
        return Err("shipping call sites and original trace records differ".into());
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
    trace_call_sites(shell, &inventory)?;
    for (before, after, action) in [
        (
            "Decision::Alarm(before, queued, approval_ready, pending, alarm)",
            "Decision::Alarm(before, queued, queued, pending, alarm)",
            "alarm",
        ),
        (
            "Decision::Satisfy(before, next)",
            "Decision::Satisfy(before, before)",
            "satisfy",
        ),
        (
            "control_trace::record(&state, Decision::Admit(before, kind, admitted));",
            "control_trace::lost(&state, Decision::Admit(before, kind, admitted));",
            "admit",
        ),
        (
            "control_trace::record(state, Decision::Report(before, (next, send)));",
            "control_trace::lost(state, Decision::Report(before, (next, send)));",
            "report",
        ),
        (
            "control_trace::record(&state, Decision::Admit(before, kind, admitted));",
            "control_trace::record(&foreign, Decision::Admit(before, kind, admitted));",
            "admit",
        ),
        (
            "control_trace::record(state, Decision::Report(before, (next, send)));",
            "control_trace::record(foreign, Decision::Report(before, (next, send)));",
            "report",
        ),
        (
            "let before = control(&state);",
            "let before = control(&foreign);",
            "alarm",
        ),
        (
            "let before = control(&state);",
            "let before = control(&state); let mut state = foreign;",
            "alarm",
        ),
        (
            "let before = control(&state);",
            "let before = control(&state); let (mut state, _) = (foreign, ());",
            "alarm",
        ),
        (
            "let before = control(&state);",
            "let before = control(&state); let state: AgentState = foreign;",
            "alarm",
        ),
        (
            "let before = control(&state);",
            "let before = control(&state); let before: Control = foreign;",
            "alarm",
        ),
    ] {
        let changed = shell.replacen(before, after, 1);
        assert_ne!(changed, shell);
        assert_eq!(
            trace_call_sites(&changed, &inventory).unwrap_err(),
            format!("shipping {action} call lost its adjacent original input/result record")
        );
    }
    let record = "control_trace::record(state, Decision::Satisfy(before, next));";
    let changed = shell.replacen(record, &format!("{record}\n{record}"), 1);
    assert_ne!(changed, shell);
    assert_eq!(
        trace_call_sites(&changed, &inventory).unwrap_err(),
        "shipping call sites and original trace records differ"
    );
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
    changed
        .functions
        .insert("unmodeled_entry".into(), BTreeSet::new());
    changed.shipping.insert("unmodeled_entry".into());
    assert_eq!(
        correspondence::validate(&changed, &mapped),
        Err(correspondence::Error::UnobservedEntry(
            "unmodeled_entry".into()
        ))
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
