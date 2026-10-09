use crate::options::{self, Options};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemStruct, Type};

struct Field {
    ident: syn::Ident,
    ty: Type,
    wire: Type,
    options: Options,
    shape: Shape,
    kind: String,
}

#[derive(Clone, Copy)]
enum Shape {
    Scalar,
    Optional,
    Repeated,
    Required,
}

impl Field {
    fn new(field: &mut syn::Field, variant: bool) -> syn::Result<Self> {
        let ident = field.ident.clone().ok_or_else(|| {
            syn::Error::new_spanned(&field.ty, "contract messages require named fields")
        })?;
        let options = Options::take_fields(&mut field.attrs)?;
        if usize::from(options.kind.is_some())
            + usize::from(options.enumeration.is_some())
            + usize::from(options.oneof.is_some())
            > 1
        {
            return Err(syn::Error::new_spanned(
                &ident,
                "wire kinds are mutually exclusive",
            ));
        }
        if options.oneof.is_some() && options.tag.is_some() {
            return Err(syn::Error::new_spanned(
                &ident,
                "oneof tags belong to its cases",
            ));
        }
        let optional = options::inner(&field.ty, "Option");
        let repeated =
            options::inner(&field.ty, "Vec").filter(|_| options.kind.as_deref() == Some("message"));
        let ty = optional
            .clone()
            .or(repeated.clone())
            .unwrap_or_else(|| field.ty.clone());
        let kind = if options.oneof.is_some() {
            "oneof".into()
        } else if options.enumeration.is_some() {
            "enumeration".into()
        } else {
            options
                .kind
                .clone()
                .map_or_else(|| options::primitive(&ty), Ok)?
        };
        let base: Type = match kind.as_str() {
            "message" | "oneof" => options::shadow(&ty)?,
            "enumeration" => syn::parse_quote!(i32),
            "string" => syn::parse_quote!(::std::string::String),
            "bytes" => syn::parse_quote!(::std::vec::Vec<u8>),
            "uint32" => syn::parse_quote!(u32),
            "uint64" => syn::parse_quote!(u64),
            "bool" => syn::parse_quote!(bool),
            _ => return Err(syn::Error::new_spanned(&ty, "unsupported wire kind")),
        };
        if variant && (optional.is_some() || repeated.is_some() || options.oneof.is_some()) {
            return Err(syn::Error::new_spanned(
                &ty,
                "oneof variants require one present payload",
            ));
        }
        let shape = if repeated.is_some() {
            Shape::Repeated
        } else if optional.is_some() {
            Shape::Optional
        } else if !variant && (kind == "message" || kind == "oneof") {
            Shape::Required
        } else {
            Shape::Scalar
        };
        let wire = match shape {
            Shape::Scalar => base,
            Shape::Optional | Shape::Required => syn::parse_quote!(::core::option::Option<#base>),
            Shape::Repeated => syn::parse_quote!(::std::vec::Vec<#base>),
        };
        Ok(Self {
            ident,
            ty,
            wire,
            options,
            shape,
            kind,
        })
    }

    fn declaration(&self, seen: &mut std::collections::BTreeSet<u32>) -> syn::Result<TokenStream> {
        let ident = &self.ident;
        let wire = &self.wire;
        let attribute = if let Some(tags) = &self.options.oneof {
            for tag in tags.value().split(',') {
                options::tag(
                    tag.trim().parse().map_err(|_| {
                        syn::Error::new_spanned(tags, "oneof tags must be integers")
                    })?,
                    seen,
                    ident,
                )?;
            }
            let ty = options::shadow(&self.ty)?;
            let path = quote!(#ty).to_string();
            quote!(oneof = #path, tags = #tags)
        } else {
            let tag = self
                .options
                .tag
                .ok_or_else(|| syn::Error::new_spanned(ident, "wire tag is required"))?;
            options::tag(tag, seen, ident)?;
            let kind = syn::Ident::new(&self.kind, ident.span());
            let kind = if let Some(enumeration) = &self.options.enumeration {
                let path = quote!(#enumeration).to_string();
                quote!(enumeration = #path)
            } else {
                quote!(#kind)
            };
            let shape = match self.shape {
                Shape::Optional | Shape::Required => quote!(optional,),
                Shape::Repeated => quote!(repeated,),
                Shape::Scalar => quote!(),
            };
            quote!(#kind, #shape tag = #tag)
        };
        Ok(
            quote! { #[doc = "Wire field derived from its semantic declaration."] #[prost(#attribute)] pub #ident: #wire },
        )
    }

    fn conversions(&self, error: &syn::Path, value: &TokenStream) -> (TokenStream, TokenStream) {
        let from = if let Some(from) = &self.options.from {
            quote!(#from(#value)?)
        } else {
            match self.shape {
                Shape::Scalar => quote!(::core::convert::TryInto::try_into(#value)?),
                Shape::Required => {
                    quote!(::core::convert::TryInto::try_into(#value.ok_or_else(<#error as ::core::default::Default>::default)?)?)
                }
                Shape::Optional => {
                    quote!(#value.map(::core::convert::TryInto::try_into).transpose()?)
                }
                Shape::Repeated => {
                    quote!(#value.into_iter().map(::core::convert::TryInto::try_into).collect::<Result<_, _>>()?)
                }
            }
        };
        let into = if let Some(into) = &self.options.into {
            quote!(#into(#value))
        } else {
            match self.shape {
                Shape::Scalar => quote!(::core::convert::Into::into(#value)),
                Shape::Required => quote!(Some(::core::convert::Into::into(#value))),
                Shape::Optional => quote!(#value.map(::core::convert::Into::into)),
                Shape::Repeated => {
                    quote!(#value.into_iter().map(::core::convert::Into::into).collect())
                }
            }
        };
        (from, into)
    }

    fn schema(&self) -> syn::Result<TokenStream> {
        let ident = self.ident.to_string();
        if self.kind == "oneof" {
            let ty = options::shadow(&self.ty)?;
            return Ok(quote!(schema.push_str(&<#ty>::schema(#ident));));
        }
        let name = if let Some(enumeration) = &self.options.enumeration {
            quote!(#enumeration::PROTO_NAME)
        } else if self.kind == "message" {
            let ty = options::shadow(&self.ty)?;
            quote!(<#ty as ::prost::Name>::NAME)
        } else {
            let kind = &self.kind;
            quote!(#kind)
        };
        let prefix = match self.shape {
            Shape::Repeated => "repeated ",
            Shape::Optional if self.kind != "message" => "optional ",
            _ => "",
        };
        let tag = self
            .options
            .tag
            .ok_or_else(|| syn::Error::new_spanned(&self.ident, "wire tag is required"))?;
        Ok(quote!(schema.push_str(&format!("  {}{} {} = {};\n", #prefix, #name, #ident, #tag));))
    }

    fn oneof_check(&self) -> syn::Result<TokenStream> {
        let Some(tags) = &self.options.oneof else {
            return Ok(quote!());
        };
        let mut tags = tags
            .value()
            .split(',')
            .map(|tag| tag.trim().parse::<u32>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| syn::Error::new_spanned(tags, "oneof tags must be integers"))?;
        tags.sort_unstable();
        let ty = options::shadow(&self.ty)?;
        Ok(quote! {
            #[allow(clippy::panic, reason = "This const must fail compilation when prost's duplicated tag list disagrees with the oneof authority.")]
            const _: () = match <#ty>::TAGS { &[#(#tags),*] => (), _ => panic!("oneof tags disagree with semantic cases") };
        })
    }
}

pub fn expand(mut input: ItemStruct, options: Options) -> syn::Result<TokenStream> {
    let error = options
        .error
        .ok_or_else(|| syn::Error::new_spanned(&input.ident, "contract error type is required"))?;
    let package = options
        .package
        .ok_or_else(|| syn::Error::new_spanned(&input.ident, "contract package is required"))?;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "generic contract declarations are unsupported",
        ));
    }
    let name = &input.ident;
    let wire = format_ident!("{}Proto", name);
    let visibility = &input.vis;
    let wire_name = options
        .name
        .map_or_else(|| name.to_string(), |name| name.value());
    let mut seen = std::collections::BTreeSet::new();
    let fields: Vec<Field> = input
        .fields
        .iter_mut()
        .map(|f| Field::new(f, false))
        .collect::<Result<_, _>>()?;
    let declarations = fields
        .iter()
        .map(|field| field.declaration(&mut seen))
        .collect::<Result<Vec<_>, _>>()?;
    let (ingress, egress): (Vec<_>, Vec<_>) = fields
        .iter()
        .map(|field| {
            let ident = &field.ident;
            let (from, into) = field.conversions(&error, &quote!(value.#ident));
            (quote!(#ident: #from), quote!(#ident: #into))
        })
        .unzip();
    let schema = fields
        .iter()
        .map(Field::schema)
        .collect::<Result<Vec<_>, _>>()?;
    let oneof_checks = fields
        .iter()
        .map(Field::oneof_check)
        .collect::<Result<Vec<_>, _>>()?;
    let package_checks = fields
        .iter()
        .filter_map(|field| {
            let optional = field.kind == "oneof";
            let actual = if let Some(enumeration) = &field.options.enumeration {
                Ok(quote!(#enumeration::PACKAGE))
            } else if field.kind == "message" || optional {
                options::shadow(&field.ty).map(|ty| {
                    if optional {
                        quote!(<#ty>::MESSAGE_PACKAGE)
                    } else {
                        quote!(<#ty as ::prost::Name>::PACKAGE)
                    }
                })
            } else {
                return None;
            };
            Some(actual.map(|actual| options::package_check(&quote!(#package), actual, optional)))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let post = options.post.map(|post| quote!(#post(&result)?;));
    Ok(quote! {
        #input
        #(#oneof_checks)*
        #(#package_checks)*
        #[doc = "Wire shadow derived from the semantic declaration."]
        #[derive(Clone, PartialEq, ::prost::Message)]
        #visibility struct #wire { #(#declarations,)* }
        impl #wire {
            #[doc = "Protobuf declaration derived from the semantic fields."]
            pub fn schema() -> String { let mut schema = format!("message {} {{\n", #wire_name); #(#schema)* schema.push_str("}\n"); schema }
        }
        impl ::prost::Name for #wire { const NAME: &'static str = #wire_name; const PACKAGE: &'static str = #package; }
        impl ::core::convert::TryFrom<#wire> for #name {
            type Error = #error;
            fn try_from(value: #wire) -> Result<Self, Self::Error> { let result = Self { #(#ingress,)* }; #post Ok(result) }
        }
        impl ::core::convert::From<#name> for #wire {
            fn from(value: #name) -> Self { Self { #(#egress,)* } }
        }
    })
}

pub fn oneof(mut input: syn::ItemEnum, options: Options) -> syn::Result<TokenStream> {
    use heck::ToSnakeCase;
    let error = options
        .error
        .ok_or_else(|| syn::Error::new_spanned(&input.ident, "contract error type is required"))?;
    let name = &input.ident;
    let wire = format_ident!("{}Proto", name);
    let visibility = &input.vis;
    let mut declarations = Vec::new();
    let mut ingress = Vec::new();
    let mut egress = Vec::new();
    let mut schema = Vec::new();
    let mut tags = std::collections::BTreeSet::new();
    let mut packages = Vec::new();
    for variant in &mut input.variants {
        let syn::Fields::Unnamed(payload) = &variant.fields else {
            return Err(syn::Error::new_spanned(
                variant,
                "oneof variants require a tuple payload",
            ));
        };
        if payload.unnamed.len() != 1 {
            return Err(syn::Error::new_spanned(
                variant,
                "oneof variants require exactly one payload",
            ));
        }
        let mut field =
            payload.unnamed.first().cloned().ok_or_else(|| {
                syn::Error::new_spanned(&variant.ident, "oneof payload is absent")
            })?;
        field.ident = Some(variant.ident.clone());
        field.attrs = variant.attrs.clone();
        let field = Field::new(&mut field, true)?;
        variant.attrs.retain(|attr| !attr.path().is_ident("wire"));
        let tag = field
            .options
            .tag
            .ok_or_else(|| syn::Error::new_spanned(&variant.ident, "wire tag is required"))?;
        options::tag(tag, &mut tags, &variant.ident)?;
        let ident = &variant.ident;
        let ty = &field.wire;
        if field.kind == "message" {
            packages.push(quote!(<#ty as ::prost::Name>::PACKAGE));
        }
        let kind = syn::Ident::new(&field.kind, ident.span());
        declarations
            .push(quote!(#[doc = "Wire oneof case."] #[prost(#kind, tag = #tag)] #ident(#ty)));
        let (from, into) = field.conversions(&error, &quote!(value));
        ingress.push(quote!(#wire::#ident(value) => Self::#ident(#from)));
        egress.push(quote!(#name::#ident(value) => Self::#ident(#into)));
        let field_name = ident.to_string().to_snake_case();
        let kind = if field.kind == "message" {
            quote!(<#ty as ::prost::Name>::NAME)
        } else {
            let kind = field.kind;
            quote!(#kind)
        };
        schema.push(
            quote!(schema.push_str(&format!("    {} {} = {};\n", #kind, #field_name, #tag));),
        );
    }
    let tags: Vec<u32> = tags.into_iter().collect();
    let package = packages
        .first()
        .map_or_else(|| quote!(None), |package| quote!(Some(#package)));
    let package_checks = packages
        .first()
        .map(|first| {
            packages
                .iter()
                .skip(1)
                .map(|actual| options::package_check(first, actual.clone(), false))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(quote! {
        #input
        #[doc = "Oneof wire shadow derived from the semantic cases."]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        #visibility enum #wire { #(#declarations,)* }
        #(#package_checks)*
        impl #wire {
            #[doc = "The package of nested cases; primitive-only oneofs have no package requirement."]
            pub const MESSAGE_PACKAGE: Option<&'static str> = #package;
            #[doc = "The sole authoritative oneof tag inventory."]
            pub const TAGS: &'static [u32] = &[#(#tags),*];
            #[doc = "Protobuf oneof declaration derived from its semantic cases."]
            pub fn schema(name: &str) -> String { let mut schema = format!("  oneof {} {{\n", name); #(#schema)* schema.push_str("  }\n"); schema }
        }
        impl ::core::convert::TryFrom<#wire> for #name {
            type Error = #error;
            fn try_from(value: #wire) -> Result<Self, Self::Error> { Ok(match value { #(#ingress,)* }) }
        }
        impl ::core::convert::From<#name> for #wire {
            fn from(value: #name) -> Self { match value { #(#egress,)* } }
        }
    })
}
