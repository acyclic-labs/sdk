//! Read the method inventory of an original Rust implementation.

use syn::{Attribute, ImplItem, ImplItemFn, ItemImpl};

/// Direct method items of the implementation, in declaration order.
pub fn methods(implementation: &ItemImpl) -> impl Iterator<Item = &ImplItemFn> {
    implementation.items.iter().filter_map(|item| match item {
        ImplItem::Fn(method) => Some(method),
        _ => None,
    })
}

/// Attributes whose configuration can change which methods are emitted.
pub fn conditions(attributes: &[Attribute]) -> impl Iterator<Item = &Attribute> {
    attributes.iter().filter(|attribute| {
        attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
    })
}
