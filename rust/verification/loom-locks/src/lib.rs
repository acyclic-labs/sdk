//! Reports which Rust sources construct or import a lock, for
//! `scripts/check-loom-models.mjs`. Sources are parsed, not pattern matched,
//! so comments, const-generic blocks, shifts inside array lengths, qualified
//! paths and nested generics cannot hide or fake a constructor.
//!
//! The scan is decided on types, not expression shapes ([`finder`]): a
//! binding declared as a lock, or a context-typed constructor
//! (`Default::default()`, `From::from`, `.into()`, ...) whose expected type
//! holds a lock, is a construction however it is wrapped, and one whose
//! expected type the file does not determine is indeterminate. Generated code
//! is excluded structurally ([`macros`]): only allow-listed macros and
//! attributes may appear, and their arguments are parsed with their grammar.
//!
//! stdin: records `path\0source\0`. stdout: one `path\t1`, `path\t0` or
//! `path\t!error` line per record.
mod finder;
mod items;
mod macros;

use std::collections::{BTreeMap, BTreeSet};
use syn::{
    Stmt, Type, UseTree,
    visit::{self, Visit},
    visit_mut::VisitMut,
};

const LOCKS: [&str; 5] = ["Mutex", "RwLock", "Condvar", "SyncMutex", "AsyncMutex"];
const LOOM_LOCKS: [&str; 3] = ["Mutex", "RwLock", "Condvar"];

fn name(ident: &syn::Ident) -> String {
    use syn::ext::IdentExt;
    ident.unraw().to_string()
}

struct Normalize;

impl VisitMut for Normalize {
    fn visit_ident_mut(&mut self, ident: &mut syn::Ident) {
        use syn::ext::IdentExt;
        *ident = ident.unraw();
    }
}

struct Substitute<'a>(&'a BTreeMap<String, Type>);

impl VisitMut for Substitute<'_> {
    fn visit_type_mut(&mut self, ty: &mut Type) {
        if let Type::Path(path) = ty
            && path.qself.is_none()
            && path.path.segments.len() == 1
            && let Some(replacement) = self.0.get(&name(&path.path.segments[0].ident))
        {
            *ty = replacement.clone();
        } else {
            syn::visit_mut::visit_type_mut(self, ty);
        }
    }
}

fn substitute(parameters: &syn::Generics, arguments: &syn::PathArguments, target: &Type) -> Type {
    let mut supplied = match arguments {
        syn::PathArguments::AngleBracketed(angle) => angle
            .args
            .iter()
            .filter_map(|arg| match arg {
                syn::GenericArgument::Type(ty) => Some(ty),
                _ => None,
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    }
    .into_iter();
    let mut bindings = BTreeMap::new();
    for parameter in parameters.type_params() {
        let mut argument = supplied
            .next()
            .cloned()
            .or_else(|| parameter.default.clone())
            .unwrap_or_else(|| syn::parse_quote!(_));
        Substitute(&bindings).visit_type_mut(&mut argument);
        bindings.insert(name(&parameter.ident), argument);
    }
    let mut expanded = target.clone();
    Substitute(&bindings).visit_type_mut(&mut expanded);
    expanded
}

/// Whether `source` constructs a lock or imports a Loom lock; an error when
/// it uses generated code or a construction whose type it does not determine.
pub fn scan(source: &str) -> syn::Result<bool> {
    let mut file = syn::parse_file(source)?;
    Normalize.visit_file_mut(&mut file);
    let fragments = macros::fragments(&file)?;
    let stmts: Vec<&Stmt> = fragments.iter().flatten().collect();
    let mut names = Names::default();
    names.visit_file(&file);
    stmts.iter().for_each(|stmt| names.visit_stmt(stmt));
    if let Some(error) = names.error {
        return Err(error);
    }
    let found = finder::find(&file, &stmts, &names)?;
    Ok(found || names.loom_import)
}

/// Lock names, renames and type aliases visible anywhere in the file.
#[derive(Default)]
struct Names<'ast> {
    renames: Vec<(String, String)>,
    declarations: BTreeSet<String>,
    /// `type` items: name, parameters and target.
    aliases: Vec<(String, &'ast syn::Generics, &'ast Type)>,
    loom_import: bool,
    error: Option<syn::Error>,
}

impl<'ast> Names<'ast> {
    fn declare(&mut self, name: &str) {
        if !self.declarations.insert(name.to_string()) {
            self.error.get_or_insert_with(|| {
                macros::indeterminate(&format!(
                    "multiple bindings of `{name}` require scope resolution"
                ))
            });
        }
    }
    fn instantiated(&self, ty: &Type) -> Option<Type> {
        let Type::Path(path) = ty else {
            return None;
        };
        if path.qself.is_some() {
            return None;
        }
        let path = self.path(&path.path)?;
        let spelling = path
            .segments
            .iter()
            .map(|segment| name(&segment.ident))
            .collect::<Vec<_>>()
            .join("::");
        let (parameters, target) = self.alias(&spelling)?;
        Some(substitute(
            parameters,
            &path.segments.last()?.arguments,
            target,
        ))
    }
    fn path(&self, path: &syn::Path) -> Option<syn::Path> {
        let spelling = path
            .segments
            .iter()
            .map(|segment| name(&segment.ident))
            .collect::<Vec<_>>()
            .join("::");
        let mut resolved: syn::Path = syn::parse_str(&self.resolve(&spelling)?).ok()?;
        for (segment, original) in resolved
            .segments
            .iter_mut()
            .rev()
            .zip(path.segments.iter().rev())
        {
            segment.arguments = original.arguments.clone();
        }
        Some(resolved)
    }
    fn resolve(&self, name: &str) -> Option<String> {
        let mut current = name.to_string();
        let mut visited = BTreeSet::new();
        loop {
            current = current
                .strip_prefix("self::")
                .unwrap_or(&current)
                .to_string();
            let (head, tail) = current.split_once("::").unwrap_or((&current, ""));
            if !visited.insert(head.to_string()) {
                return None;
            }
            let mut bindings = self.renames.iter().filter(|(_, alias)| alias == head);
            let Some((target, _)) = bindings.next() else {
                return Some(current);
            };
            if bindings.any(|(other, _)| other != target) {
                return None;
            }
            current = if tail.is_empty() {
                target.clone()
            } else {
                format!("{target}::{tail}")
            };
        }
    }
    /// The parameters and target of the alias `name`.
    fn alias(&self, name: &str) -> Option<(&'ast syn::Generics, &'ast Type)> {
        let name = self.resolve(name)?;
        self.aliases
            .iter()
            .find(|(alias, _, _)| alias == &name)
            .map(|(_, parameters, target)| (*parameters, *target))
    }

    fn walk(&mut self, tree: &UseTree, prefix: &mut Vec<String>) {
        let loom = prefix.first().is_some_and(|first| first == "loom")
            && prefix.get(1).is_some_and(|second| second == "sync");
        match tree {
            UseTree::Path(path) => {
                prefix.push(crate::name(&path.ident));
                self.walk(&path.tree, prefix);
                prefix.pop();
            }
            UseTree::Name(name) => {
                let target = crate::name(&name.ident);
                self.loom_import |= loom && LOOM_LOCKS.contains(&target.as_str());
                self.bind(prefix, &target, &target);
            }
            UseTree::Rename(rename) => {
                let target = crate::name(&rename.ident);
                self.loom_import |= loom && LOOM_LOCKS.contains(&target.as_str());
                self.bind(prefix, &target, &crate::name(&rename.rename));
            }
            UseTree::Glob(_) => {
                self.loom_import |= loom;
                self.error.get_or_insert_with(|| {
                    macros::indeterminate(
                        "glob imports leave value and constructor bindings unresolved",
                    )
                });
            }
            UseTree::Group(group) => {
                for tree in &group.items {
                    self.walk(tree, prefix);
                }
            }
        }
    }

    fn bind(&mut self, prefix: &[String], target: &str, name: &str) {
        self.declare(name);
        let mut path = prefix.to_vec();
        path.push(target.to_string());
        if !prefix.is_empty() || target != name {
            self.renames.push((path.join("::"), name.to_string()));
        }
        if macros::redirects(&path, name) && self.error.is_none() {
            self.error = Some(macros::indeterminate(&format!(
                "`use {} as {name}` rebinds an allow-listed macro name",
                path.join("::")
            )));
        }
    }
}

impl<'ast> Visit<'ast> for Names<'ast> {
    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        self.walk(&item.tree, &mut Vec::new());
    }

    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        self.declare(&crate::name(&item.ident));
        visit::visit_item_struct(self, item);
    }
    fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
        self.declare(&crate::name(&item.ident));
        visit::visit_item_enum(self, item);
    }
    fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
        self.declare(&crate::name(&item.ident));
        self.aliases
            .push((crate::name(&item.ident), &item.generics, &item.ty));
        visit::visit_item_type(self, item);
    }
}

#[cfg(test)]
mod properties;
#[cfg(test)]
mod tests;
