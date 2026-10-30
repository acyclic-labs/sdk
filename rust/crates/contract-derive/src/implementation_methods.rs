//! Inventory the original implementation before async-trait lowers its methods.
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, ImplItem, ItemImpl};

pub(crate) fn expand(name: Ident, implementation: ItemImpl) -> TokenStream {
    let owner = &implementation.self_ty;
    let (generics, _, condition) = implementation.generics.split_for_impl();
    let methods = implementation.items.iter().filter_map(|item| {
        let ImplItem::Fn(method) = item else {
            return None;
        };
        let name = &method.sig.ident;
        let conditions = method.attrs.iter().filter(|attribute| {
            attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
        });
        Some(quote!(#(#conditions)* stringify!(#name)))
    });
    quote! {
        #implementation
        impl #generics #owner #condition {
            /// Methods emitted by the compiler-checked provider implementation.
            pub const #name: &'static [&'static str] = &[#(#methods),*];
        }
    }
}
