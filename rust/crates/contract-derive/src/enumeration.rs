use crate::options::Options;
use heck::ToShoutySnakeCase;
use proc_macro2::TokenStream;
use quote::quote;

pub fn expand(input: &syn::ItemEnum, options: Options) -> syn::Result<TokenStream> {
    let error = options
        .error
        .ok_or_else(|| syn::Error::new_spanned(&input.ident, "enum error type is required"))?;
    let unknown = options.unknown.ok_or_else(|| {
        syn::Error::new_spanned(&input.ident, "unknown enum error constructor is required")
    })?;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "generic contract declarations are unsupported",
        ));
    }
    let name = &input.ident;
    let wire_name = options
        .name
        .map_or_else(|| crate::options::identifier(name), |name| name.value());
    let mut schema = format!("enum {wire_name} {{\n");
    let mut seen = std::collections::BTreeSet::new();
    let mut cases = Vec::new();
    let mut default = None;
    for variant in &input.variants {
        if !matches!(variant.fields, syn::Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "contract enum variants must be unit variants",
            ));
        }
        let Some((_, syn::Expr::Lit(literal))) = &variant.discriminant else {
            return Err(syn::Error::new_spanned(
                variant,
                "explicit enum integer is required",
            ));
        };
        let syn::Lit::Int(value) = &literal.lit else {
            return Err(syn::Error::new_spanned(
                literal,
                "explicit enum integer is required",
            ));
        };
        let value = value.base10_parse::<i32>()?;
        if !seen.insert(value) {
            return Err(syn::Error::new_spanned(variant, "duplicate enum integer"));
        }
        if default.is_none() {
            if value != 0 {
                return Err(syn::Error::new_spanned(
                    variant,
                    "first protobuf enum value must be zero",
                ));
            }
            default = Some(&variant.ident);
        }
        let ident = &variant.ident;
        cases.push(quote!(#value => Ok(Self::#ident)));
        schema.push_str(&format!(
            "  {}_{} = {value};\n",
            wire_name.to_shouty_snake_case(),
            crate::options::identifier(ident).to_shouty_snake_case()
        ));
    }
    let default =
        default.ok_or_else(|| syn::Error::new_spanned(name, "contract enum must not be empty"))?;
    schema.push_str("}\n");
    Ok(quote! {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        #[repr(i32)]
        #input
        impl #name {
            #[doc = "The protobuf name of this semantic enum."]
            pub const PROTO_NAME: &'static str = #wire_name;
            #[doc = "Protobuf declaration derived from semantic cases."]
            pub fn schema() -> String { #schema.to_owned() }
        }
        impl ::core::default::Default for #name { fn default() -> Self { Self::#default } }
        impl ::core::convert::TryFrom<i32> for #name {
            type Error = #error;
            fn try_from(value: i32) -> Result<Self, Self::Error> { match value { #(#cases,)* _ => Err(#unknown(value)) } }
        }
        impl ::core::convert::From<#name> for i32 { fn from(value: #name) -> Self { value as Self } }
    })
}
