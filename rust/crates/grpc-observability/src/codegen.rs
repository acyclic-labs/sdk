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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_unary_client_bodies_are_observed_and_unknown_shapes_reject() -> io::Result<()> {
        let source = r#"
            pub mod actors_service_client {
                impl<T> Client<T> {
                    pub async fn connect() { connect().await }
                    pub async fn inspect(&mut self) {
                        let metadata = GrpcMethod::new("acyclic.actors.v1.ActorsService", "InspectActor");
                        self.inner.unary(request, path, codec).await
                    }
                }
            }
            pub mod actors_service_server { async fn inspect() { server().await } }
        "#;
        let output = observe_clients(source, "actors")?;
        assert_eq!(
            output
                .matches("acyclic_grpc_observability::observe_call")
                .count(),
            1
        );
        assert!(output.contains("acyclic.actors.grpc.call"));
        assert!(output.contains("observed_call_span(\"InspectActor\")"));
        let output = syn::parse_file(&output).map_err(io::Error::other)?;
        let original = syn::parse_file(source).map_err(io::Error::other)?;
        assert_eq!(
            prettyplease::unparse(&syn::File {
                items: vec![
                    output
                        .items
                        .get(1)
                        .ok_or_else(|| io::Error::other("missing server module"))?
                        .clone()
                ],
                ..output.clone()
            }),
            prettyplease::unparse(&syn::File {
                items: vec![
                    original
                        .items
                        .get(1)
                        .ok_or_else(|| io::Error::other("missing server module"))?
                        .clone()
                ],
                ..original.clone()
            })
        );
        assert!(observe_clients(&source.replace(".unary(", ".streaming("), "actors").is_err());
        assert!(
            observe_clients(&source.replace("GrpcMethod::new", "Other::new"), "actors").is_err()
        );
        Ok(())
    }
}
