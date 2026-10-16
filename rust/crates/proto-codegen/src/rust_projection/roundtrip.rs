use std::collections::BTreeMap;
use syn::{Expr, Item, visit_mut::VisitMut};

struct Expand<'a> {
    files: &'a BTreeMap<String, String>,
    error: Option<syn::Error>,
}

impl VisitMut for Expand<'_> {
    fn visit_file_mut(&mut self, file: &mut syn::File) {
        self.items(&mut file.items);
    }

    fn visit_item_mod_mut(&mut self, module: &mut syn::ItemMod) {
        if let Some((_, items)) = &mut module.content {
            self.items(items);
        }
    }

    fn visit_expr_mut(&mut self, expr: &mut Expr) {
        if let Expr::Macro(mac) = expr
            && mac.mac.path.is_ident("include")
        {
            match self.included(&mac.mac).and_then(syn::parse_str) {
                Ok(expanded) => *expr = expanded,
                Err(error) => self.error = Some(error),
            }
        }
        syn::visit_mut::visit_expr_mut(self, expr);
    }
}

impl Expand<'_> {
    fn included(&self, mac: &syn::Macro) -> syn::Result<&str> {
        let name = syn::parse2::<syn::LitStr>(mac.tokens.clone())?;
        self.files
            .get(&name.value())
            .map(String::as_str)
            .ok_or_else(|| syn::Error::new_spanned(mac, "missing generated include"))
    }

    fn items(&mut self, items: &mut Vec<Item>) {
        let mut expanded = Vec::new();
        for mut item in std::mem::take(items) {
            if let Item::Macro(mac) = &item
                && mac.mac.path.is_ident("include")
            {
                match self.included(&mac.mac).and_then(syn::parse_file) {
                    Ok(mut file) => {
                        self.visit_file_mut(&mut file);
                        expanded.extend(file.items);
                    }
                    Err(error) => self.error = Some(error),
                }
                continue;
            }
            self.visit_item_mut(&mut item);
            expanded.push(item);
        }
        *items = expanded;
    }
}

/// Expanding every emitted include must reproduce the original Rust AST.
pub fn assert_roundtrip(
    source: &str,
    files: Vec<(String, String)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (name, _) = files.last().ok_or("missing root output")?;
    let name = name
        .rsplit_once('/')
        .map_or(name.as_str(), |(_, base)| base)
        .to_owned();
    assert!(files.iter().all(|(_, text)| text.lines().count() < 800));
    let files: BTreeMap<_, _> = files
        .into_iter()
        .map(|(name, text)| {
            (
                name.rsplit_once('/')
                    .map_or(name.as_str(), |(_, base)| base)
                    .to_owned(),
                text,
            )
        })
        .collect();
    let mut expanded = syn::parse_file(files.get(&name).ok_or("missing root output")?)?;
    let mut expand = Expand {
        files: &files,
        error: None,
    };
    expand.visit_file_mut(&mut expanded);
    if let Some(error) = expand.error {
        return Err(error.into());
    }
    let original = syn::parse_file(source)?;
    // Canonical printing removes optional punctuation as well as whitespace.
    assert!(
        prettyplease::unparse(&expanded) == prettyplease::unparse(&original),
        "expanded Rust syntax changed"
    );
    Ok(())
}
