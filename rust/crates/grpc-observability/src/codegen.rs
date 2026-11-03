//! Structural native client observation for maintained Tonic build output.

use std::io;

/// Observe unary clients while retaining server bodies unchanged.
///
/// # Errors
/// Rejects invalid Rust or an unrecognized unary client shape.
pub fn observe_clients(source: &str, family: &str) -> io::Result<String> {
    let mut file = syn::parse_file(source).map_err(io::Error::other)?;
    let span_name = format!("acyclic.{family}.grpc.call");
    for item in &mut file.items {
        let syn::Item::Mod(module) = item else {
            continue;
        };
        if !module.ident.to_string().ends_with("_service_client") {
            continue;
        }
        let Some((_, items)) = &mut module.content else {
            continue;
        };
        for item in &mut *items {
            let syn::Item::Impl(implementation) = item else {
                continue;
            };
            for item in &mut implementation.items {
                let syn::ImplItem::Fn(method) = item else {
                    continue;
                };
                if method.sig.asyncness.is_none() || method.sig.ident == "connect" {
                    continue;
                }
                let mut rpc = RpcShape::default();
                syn::visit::Visit::visit_block(&mut rpc, &method.block);
                if !rpc.unary || rpc.name.is_none() {
                    return Err(io::Error::other(
                        "unrecognized observable unary client method",
                    ));
                }
                let rpc_name = rpc
                    .name
                    .ok_or_else(|| io::Error::other("missing unary RPC name"))?;
                let body = &method.block;
                method.block = syn::parse_quote!({
                    acyclic_grpc_observability::observe_call(
                        observed_call_span(#rpc_name),
                        async #body,
                    ).await
                });
            }
        }
        // One metadata callsite and field schema per family, not per method.
        items.push(syn::parse_quote! {
            fn observed_call_span(rpc: &str) -> tracing::Span {
                tracing::info_span!(
                    #span_name, rpc,
                    rpc.code = tracing::field::Empty,
                    outcome = tracing::field::Empty,
                    outcome.scope = "typed_rpc",
                    error.kind = tracing::field::Empty,
                )
            }
        });
    }
    Ok(format!("// @generated\n{}", prettyplease::unparse(&file)))
}

#[derive(Default)]
struct RpcShape {
    unary: bool,
    name: Option<syn::LitStr>,
}
impl<'ast> syn::visit::Visit<'ast> for RpcShape {
    fn visit_expr_method_call(&mut self, expression: &'ast syn::ExprMethodCall) {
        self.unary |= expression.method == "unary";
        syn::visit::visit_expr_method_call(self, expression);
    }
    fn visit_expr_call(&mut self, expression: &'ast syn::ExprCall) {
        if let syn::Expr::Path(path) = expression.func.as_ref() {
            let names: Vec<_> = path
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            if names == ["GrpcMethod", "new"]
                && let Some(syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(name),
                    ..
                })) = expression.args.iter().nth(1)
            {
                self.name = Some(name.clone());
            }
        }
        syn::visit::visit_expr_call(self, expression);
    }
}
