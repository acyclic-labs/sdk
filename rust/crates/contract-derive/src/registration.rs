use heck::ToSnakeCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemEnum, ItemStruct, LitStr, Path, parse::Parser};

#[derive(Default)]
struct File {
    family: Option<LitStr>,
    messages: Vec<Path>,
    enums: Vec<Path>,
    services: Vec<Path>,
}

pub fn file(args: TokenStream, input: &ItemStruct) -> syn::Result<TokenStream> {
    let mut file = File::default();
    let mut seen = std::collections::BTreeSet::new();
    let parser = syn::meta::parser(|meta| {
        let key = meta
            .path
            .get_ident()
            .ok_or_else(|| meta.error("file options must be identifiers"))?
            .to_string();
        if !seen.insert(key) {
            return Err(meta.error("duplicate file option"));
        }
        if meta.path.is_ident("family") {
            file.family = Some(meta.value()?.parse()?);
        } else {
            let paths = if meta.path.is_ident("messages") {
                &mut file.messages
            } else if meta.path.is_ident("enums") {
                &mut file.enums
            } else if meta.path.is_ident("services") {
                &mut file.services
            } else {
                return Err(meta.error("unknown file option"));
            };
            let content;
            syn::parenthesized!(content in meta.input);
            *paths = content
                .parse_terminated(Path::parse_mod_style, syn::Token![,])?
                .into_iter()
                .collect();
        }
        Ok(())
    });
    parser.parse2(args)?;
    file.expand(input)
}

impl File {
    fn expand(self, input: &ItemStruct) -> syn::Result<TokenStream> {
        if !input.generics.params.is_empty() || !matches!(input.fields, syn::Fields::Unit) {
            return Err(syn::Error::new_spanned(
                input,
                "contract file requires a nongeneric unit struct",
            ));
        }
        let family = self.family.ok_or_else(|| {
            syn::Error::new_spanned(&input.ident, "first-party family is required")
        })?;
        let family_name = family.value();
        if family_name.is_empty()
            || !family_name.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase() || (index > 0 && (byte.is_ascii_digit() || byte == b'_'))
            })
        {
            return Err(syn::Error::new_spanned(
                family,
                "first-party family must be a lowercase identifier",
            ));
        }
        let package = format!("acyclic.{family_name}.v1");
        let path = format!("{family_name}/v1/{family_name}.proto");
        let go_package =
            format!("github.com/acyclic-labs/sdk/go/gen/{family_name}/v1;{family_name}v1");
        let name = &input.ident;
        let messages = self.messages;
        let enums = self.enums;
        let services = self.services;
        let members = format_ident!(
            "__{}_members",
            crate::options::identifier(name).to_snake_case()
        );
        let checks = messages
            .iter()
            .chain(services.iter())
            .map(|ty| quote!(const _: #name = <#ty>::FILE;))
            .collect::<Vec<_>>();
        Ok(quote! {
            #input
            #(#checks)*
            mod #members {
                pub trait Member {}
            }
            #(impl #members::Member for #messages {})*
            #(impl #members::Member for #enums {})*
            #(impl #members::Member for #services {})*
            #(impl #enums {
                #[doc = "The unique compiler-linked file owner."]
                pub const FILE: #name = #name;
            })*
            impl #name {
                #[doc(hidden)]
                pub const fn require_member<T: #members::Member>() {}
                #[doc = "Canonical protobuf package shared by wire names and rendering."]
                pub const PACKAGE: &'static str = #package;
                #[doc = "Render this file from its compiler-linked semantic type inventory."]
                pub fn render(root: &::std::path::Path) -> ::std::io::Result<()> {
                    let mut names = ::std::collections::BTreeSet::new();
                    let mut text = format!("syntax = \"proto3\";\n\npackage {};\n\noption go_package = \"{}\";\n\n", Self::PACKAGE, #go_package);
                    for mut group in [
                        vec![#((#enums::PROTO_NAME, #enums::schema())),*],
                        vec![#((<#messages as ::prost::Name>::NAME, #messages::schema())),*],
                        vec![#((#services::PROTO_NAME, #services::schema())),*],
                    ] {
                        group.sort_by(|left, right| left.0.cmp(right.0));
                        for (name, declaration) in group {
                            if !names.insert(name) { return Err(::std::io::Error::new(::std::io::ErrorKind::InvalidData, "duplicate protobuf declaration name")); }
                            text.push_str(&declaration);
                            text.push('\n');
                        }
                    }
                    let path = root.join(#path);
                    if let Some(parent) = path.parent() { ::std::fs::create_dir_all(parent)?; }
                    ::std::fs::write(path, text)
                }
            }
        })
    }
}

pub fn service(mut input: ItemEnum, file: &Path) -> syn::Result<TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "generic services are unsupported",
        ));
    }
    let name = &input.ident;
    let name_text = crate::options::identifier(name);
    let attrs = &input.attrs;
    let visibility = &input.vis;
    let mut methods = Vec::new();
    let mut checks = vec![crate::options::owner_check(file, &quote!(#name), true)];
    let mut method_names = std::collections::BTreeSet::new();
    for variant in &mut input.variants {
        if !method_names.insert(crate::options::identifier(&variant.ident)) {
            return Err(syn::Error::new_spanned(variant, "duplicate service method"));
        }
        let mut client_stream = "";
        let mut server_stream = "";
        let mut seen = std::collections::BTreeSet::new();
        for attr in variant
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("wire"))
        {
            attr.parse_nested_meta(|meta| {
                if !seen.insert(meta.path.get_ident().map(ToString::to_string)) {
                    return Err(meta.error("duplicate streaming option"));
                }
                if meta.path.is_ident("client_streaming") {
                    client_stream = "stream ";
                } else if meta.path.is_ident("server_streaming") {
                    server_stream = "stream ";
                } else {
                    return Err(meta.error("unknown service option"));
                }
                Ok(())
            })?;
        }
        variant.attrs.retain(|attr| !attr.path().is_ident("wire"));
        let syn::Fields::Named(fields) = &mut variant.fields else {
            return Err(syn::Error::new_spanned(
                variant,
                "service methods require request and response types",
            ));
        };
        let mut request = None;
        let mut response = None;
        let mut fields_seen = std::collections::BTreeSet::new();
        for field in &mut fields.named {
            if !fields_seen.insert(field.ident.clone()) {
                return Err(syn::Error::new_spanned(field, "duplicate service field"));
            }
            if field.ident.as_ref().is_some_and(|ident| ident == "request") {
                request = Some(&field.ty);
            } else if field
                .ident
                .as_ref()
                .is_some_and(|ident| ident == "response")
            {
                response = Some(&field.ty);
            } else {
                return Err(syn::Error::new_spanned(field, "unknown service field"));
            }
            field
                .attrs
                .push(syn::parse_quote!(#[doc = "Compiler-linked service message."]));
        }
        let request = request
            .ok_or_else(|| syn::Error::new_spanned(&variant.ident, "request type is required"))?;
        let response = response
            .ok_or_else(|| syn::Error::new_spanned(&variant.ident, "response type is required"))?;
        checks.push(crate::options::owner_check(file, &quote!(#request), true));
        checks.push(crate::options::owner_check(file, &quote!(#response), true));
        let method = crate::options::identifier(&variant.ident);
        methods.push(quote!(schema.push_str(&format!("  rpc {}({}{}) returns ({}{});\n", #method, #client_stream, <#request as ::prost::Name>::NAME, #server_stream, <#response as ::prost::Name>::NAME));));
    }
    if methods.is_empty() {
        return Err(syn::Error::new_spanned(
            name,
            "first-party services require a method",
        ));
    }
    Ok(quote! {
        #(#checks)*
        #(#attrs)*
        #visibility struct #name;
        impl #name {
            #[doc = "The unique compiler-linked file owner."]
            pub const FILE: #file = #file;
            #[doc = "Canonical protobuf service name."]
            pub const PROTO_NAME: &'static str = #name_text;
            #[doc = "Service schema derived from compiler-linked message types."]
            pub fn schema() -> String { let mut schema = format!("service {} {{\n", #name_text); #(#methods)* schema.push_str("}\n"); schema }
        }
    })
}
