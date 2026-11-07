use acyclic_grpc_observability::codegen::observe_clients;
use std::io;
use syn::visit::Visit;

/// Every accepted transformation is reversible without changing any original
/// item, signature or body. This checks the production transformer itself.
pub(super) fn check(source: &str) -> io::Result<Option<usize>> {
    let Ok(observed) = observe_clients(source, "actors") else {
        return Ok(None);
    };
    let original = syn::parse_file(source).map_err(io::Error::other)?;
    let mut restored = syn::parse_file(&observed).map_err(io::Error::other)?;
    assert_eq!(original.items.len(), restored.items.len());
    let mut methods = 0;
    for (before, after) in original.items.iter().zip(&mut restored.items) {
        let syn::Item::Mod(before) = before else {
            continue;
        };
        if !before.ident.to_string().ends_with("_service_client") {
            continue;
        }
        let Some((_, before_items)) = &before.content else {
            continue;
        };
        let syn::Item::Mod(after) = after else {
            return Err(io::Error::other(
                "observer changed a client module's item kind",
            ));
        };
        let (_, after_items) = after
            .content
            .as_mut()
            .ok_or_else(|| io::Error::other("observer removed client contents"))?;
        assert!(observed.contains("acyclic.actors.grpc.call"));
        assert_eq!(after_items.len(), before_items.len() + 1);
        let helper = after_items
            .pop()
            .ok_or_else(|| io::Error::other("observer omitted its span factory"))?;
        assert!(
            matches!(helper, syn::Item::Fn(function) if function.sig.ident == "observed_call_span")
        );
        for (before, after) in before_items.iter().zip(after_items) {
            let (syn::Item::Impl(before), syn::Item::Impl(after)) = (before, after) else {
                continue;
            };
            assert_eq!(before.items.len(), after.items.len());
            for (before, after) in before.items.iter().zip(&mut after.items) {
                let syn::ImplItem::Fn(before) = before else {
                    continue;
                };
                if before.sig.asyncness.is_none() || before.sig.ident == "connect" {
                    continue;
                }
                let syn::ImplItem::Fn(after) = after else {
                    return Err(io::Error::other(
                        "observer changed a client method's item kind",
                    ));
                };
                after.block = original_body(&after.block, &before.block)?;
                methods += 1;
            }
        }
    }
    assert_eq!(
        prettyplease::unparse(&restored),
        prettyplease::unparse(&original)
    );
    Ok(Some(methods))
}

fn original_body(observed: &syn::Block, original: &syn::Block) -> io::Result<syn::Block> {
    let [syn::Stmt::Expr(syn::Expr::Await(awaited), None)] = observed.stmts.as_slice() else {
        return Err(io::Error::other(
            "observer must emit exactly one awaited call",
        ));
    };
    let syn::Expr::Call(call) = awaited.base.as_ref() else {
        return Err(io::Error::other("observer lost the call boundary"));
    };
    assert!(path(
        &call.func,
        &["acyclic_grpc_observability", "observe_call"]
    ));
    assert_eq!(call.args.len(), 2);
    let mut arguments = call.args.iter();
    let Some(syn::Expr::Call(span)) = arguments.next() else {
        return Err(io::Error::other("observer lost the span factory"));
    };
    assert!(path(&span.func, &["observed_call_span"]));
    assert_eq!(span.args.len(), 1);
    let Some(syn::Expr::Lit(syn::ExprLit {
        lit: syn::Lit::Str(name),
        ..
    })) = span.args.first()
    else {
        return Err(io::Error::other("observer lost the original RPC name"));
    };
    let mut names = RpcNames::default();
    names.visit_block(original);
    assert!(
        names.0.contains(&name.value()),
        "observer changed RPC identity"
    );
    let Some(syn::Expr::Async(body)) = arguments.next() else {
        return Err(io::Error::other("observer lost the original async body"));
    };
    assert!(
        body.capture.is_none(),
        "observer changed async capture semantics"
    );
    Ok(body.block.clone())
}

fn path(expression: &syn::Expr, names: &[&str]) -> bool {
    matches!(expression, syn::Expr::Path(path)
        if path.path.segments.iter().map(|segment| segment.ident.to_string()).eq(names.iter().copied()))
}

#[derive(Default)]
struct RpcNames(Vec<String>);

impl<'ast> Visit<'ast> for RpcNames {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if path(&call.func, &["GrpcMethod", "new"])
            && let Some(syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(name),
                ..
            })) = call.args.iter().nth(1)
        {
            self.0.push(name.value());
        }
        syn::visit::visit_expr_call(self, call);
    }
}
