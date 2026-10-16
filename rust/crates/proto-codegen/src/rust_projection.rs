//! Split generated Rust at syntax boundaries without changing its AST.

use quote::ToTokens;
use syn::{Expr, Item, visit_mut::VisitMut};

const CHUNK_LINES: usize = 700;

struct Projection {
    stem: String,
    files: Vec<(String, String)>,
    error: Option<syn::Error>,
}

fn render(items: &[Item]) -> String {
    prettyplease::unparse(&syn::File {
        shebang: None,
        attrs: Vec::new(),
        items: items.to_vec(),
    })
}

fn render_expr(expr: &Expr) -> syn::Result<String> {
    let source = render(&[syn::parse_quote!(const _: () = #expr;)]);
    source
        .strip_prefix("const _: () = ")
        .and_then(|body| body.strip_suffix(";\n"))
        .map(|body| format!("{body}\n"))
        .ok_or_else(|| syn::Error::new_spanned(expr, "unexpected expression formatting"))
}

impl Projection {
    fn save(&mut self, source: String) -> String {
        let name = format!("{}.part{}.rs", self.stem, self.files.len());
        self.files.push((name.clone(), source));
        name.rsplit_once('/')
            .map_or(name.as_str(), |(_, base)| base)
            .to_owned()
    }

    fn split_items(&mut self, items: &mut Vec<Item>) {
        if render(items).lines().count() <= CHUNK_LINES {
            return;
        }
        let mut chunks = Vec::new();
        let mut chunk = Vec::new();
        for item in std::mem::take(items) {
            chunk.push(item);
            if render(&chunk).lines().count() > CHUNK_LINES
                && chunk.len() > 1
                && let Some(last) = chunk.pop()
            {
                let name = self.save(render(&chunk));
                chunks.push(syn::parse_quote!(include!(#name);));
                chunk = vec![last];
            }
        }
        if !chunk.is_empty() {
            let name = self.save(render(&chunk));
            chunks.push(syn::parse_quote!(include!(#name);));
        }
        *items = chunks;
    }
}

impl VisitMut for Projection {
    fn visit_item_mod_mut(&mut self, module: &mut syn::ItemMod) {
        syn::visit_mut::visit_item_mod_mut(self, module);
        if let Some((_, items)) = &mut module.content {
            self.split_items(items);
        }
    }

    fn visit_expr_match_mut(&mut self, expr: &mut syn::ExprMatch) {
        syn::visit_mut::visit_expr_match_mut(self, expr);
        let source = match render_expr(&Expr::Match(expr.clone())) {
            Ok(source) => source,
            Err(error) => {
                self.error = Some(error);
                return;
            }
        };
        if source.lines().count() > CHUNK_LINES {
            for arm in &mut expr.arms {
                match render_expr(&arm.body) {
                    Ok(source) => {
                        let name = self.save(source);
                        *arm.body = syn::parse_quote!(include!(#name));
                    }
                    Err(error) => self.error = Some(error),
                }
            }
        }
    }
}

/// Preserve the generated AST while splitting large files into sibling includes.
pub fn project(name: &str, source: &str) -> syn::Result<Vec<(String, String)>> {
    let mut file = syn::parse_file(source)?;
    if !name.ends_with(".tonic.rs") {
        for item in &mut file.items {
            if let Item::Macro(item) = item
                && item.mac.path.is_ident("include")
            {
                let gate = if name.contains("objects") {
                    syn::parse_quote!(#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))])
                } else {
                    syn::parse_quote!(#[cfg(feature = "grpc")])
                };
                item.attrs.push(gate);
            }
        }
    }
    let mut projection = Projection {
        stem: name.strip_suffix(".rs").unwrap_or(name).to_owned(),
        files: Vec::new(),
        error: None,
    };
    projection.visit_file_mut(&mut file);
    if let Some(error) = projection.error {
        return Err(error);
    }
    projection.split_items(&mut file.items);
    projection
        .files
        .push((name.to_owned(), prettyplease::unparse(&file)));
    for (_, source) in &projection.files {
        if source.lines().count() >= 800 {
            return Err(syn::Error::new_spanned(
                file.to_token_stream(),
                "generated Rust item exceeds the source-file bound",
            ));
        }
    }
    Ok(projection.files)
}

#[cfg(test)]
mod tests;
