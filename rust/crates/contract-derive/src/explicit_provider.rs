//! Generate the no-default adapter contract from the provider declaration.
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemTrait, Pat, TraitItem};

pub(crate) fn expand(contract: ItemTrait) -> syn::Result<TokenStream> {
    if !contract.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(&contract.generics, "provider contract must not be generic"));
    }
    let mut explicit = contract.clone();
    explicit.ident = format_ident!("Explicit{}", contract.ident);
    let mut forwarding = Vec::new();
    for item in &mut explicit.items {
        let TraitItem::Fn(method) = item else {
            return Err(syn::Error::new_spanned(item, "provider contract must contain only methods"));
        };
        method.default = None;
        method.semi_token = Some(Default::default());
        let mut args = Vec::new();
        for arg in &method.sig.inputs {
            match arg {
                FnArg::Receiver(_) => args.push(quote!(self)),
                FnArg::Typed(arg) => {
                    let Pat::Ident(name) = &*arg.pat else {
                        return Err(syn::Error::new_spanned(&arg.pat, "provider argument must be named"));
                    };
                    let name = &name.ident;
                    args.push(quote!(#name));
                }
            }
        }
        let attrs = &method.attrs;
        let sig = &method.sig;
        let name = &sig.ident;
        let target = &explicit.ident;
        let wait = sig.asyncness.as_ref().map(|_| quote!(.await));
        forwarding.push(quote! {
            #(#attrs)*
            #sig { <Self as #target>::#name(#(#args),*) #wait }
        });
    }
    let attrs: Vec<_> = contract.attrs.iter().filter(|attr| attr.path().is_ident("cfg_attr")).collect();
    let name = &contract.ident;
    let required = &explicit.ident;
    Ok(quote! {
        #contract
        #explicit
        #(#attrs)*
        impl<__Provider: #required> #name for __Provider {
            #(#forwarding)*
        }
    })
}
