//! Inventory the original implementation before async-trait lowers its methods.
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, ItemImpl};

pub(crate) fn expand(name: &Ident, implementation: &ItemImpl) -> TokenStream {
    let owner = &implementation.self_ty;
    let (generics, _, condition) = implementation.generics.split_for_impl();
    let methods = acyclic_implementation_inventory::methods(implementation).map(|method| {
        let name = &method.sig.ident;
        let conditions = acyclic_implementation_inventory::conditions(&method.attrs);
        quote!(#(#conditions)* stringify!(#name))
    });
    quote! {
        #implementation
        impl #generics #owner #condition {
            /// Methods emitted by the compiler-checked provider implementation.
            pub const #name: &[&str] = &[#(#methods),*];
        }
    }
}
