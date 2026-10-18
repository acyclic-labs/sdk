use crate::finder::type_name;
use std::collections::{BTreeMap, BTreeSet};
use syn::{
    Expr, Pat, Signature, Type,
    visit::{self, Visit},
};

/// Free functions, associated functions and structs defined in the file. A
/// name defined twice is ambiguous and resolves to nothing.
#[derive(Default)]
pub(super) struct Items<'ast> {
    pub(super) functions: BTreeMap<String, Option<&'ast Signature>>,
    pub(super) structs: BTreeMap<String, Option<&'ast syn::ItemStruct>>,
    pub(super) values: BTreeSet<String>,
    owner: Vec<Option<String>>,
}

impl<'ast> Items<'ast> {
    fn function(&mut self, name: String, signature: &'ast Signature) {
        self.functions
            .entry(name)
            .and_modify(|seen| *seen = None)
            .or_insert(Some(signature));
    }
}

impl<'ast> Visit<'ast> for Items<'ast> {
    fn visit_pat_ident(&mut self, pattern: &'ast syn::PatIdent) {
        self.values.insert(crate::name(&pattern.ident));
        visit::visit_pat_ident(self, pattern);
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.function(crate::name(&item.sig.ident), &item.sig);
        visit::visit_item_fn(self, item);
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        self.owner.push(type_name(&item.self_ty));
        visit::visit_item_impl(self, item);
        self.owner.pop();
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if let Some(Some(owner)) = self.owner.last() {
            self.function(format!("{owner}::{}", item.sig.ident), &item.sig);
        }
        visit::visit_impl_item_fn(self, item);
    }

    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        self.structs
            .entry(crate::name(&item.ident))
            .and_modify(|seen| *seen = None)
            .or_insert(Some(item));
        visit::visit_item_struct(self, item);
    }
}

/// `const _: fn(A) -> B = |lock| lock;`, which asserts at compile time that
/// `A` is `B` and constructs nothing; its types are not written values.
pub(super) fn identity(item: &syn::ItemConst) -> bool {
    let Expr::Closure(closure) = &*item.expr else {
        return false;
    };
    let parameter = match closure.inputs.iter().collect::<Vec<_>>().as_slice() {
        [Pat::Ident(parameter)] => crate::name(&parameter.ident),
        _ => return false,
    };
    item.ident == "_"
        && matches!(*item.ty, Type::BareFn(_))
        && matches!(&*closure.body, Expr::Path(body) if body.path.is_ident(&parameter))
}
