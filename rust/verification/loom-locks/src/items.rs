use crate::finder::type_name;
use std::collections::BTreeMap;
use syn::{
    Signature,
    visit::{self, Visit},
};

/// Free functions, associated functions and structs defined in the file. A
/// name defined twice is ambiguous and resolves to nothing.
#[derive(Default)]
pub(super) struct Items<'ast> {
    pub(super) functions: BTreeMap<String, Option<&'ast Signature>>,
    pub(super) structs: BTreeMap<String, Option<&'ast syn::ItemStruct>>,
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
