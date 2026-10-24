//! `#[derive(HeapLayout)]`: the JSON heap layout of a serde decode.
//!
//! The layout mirrors what `#[derive(Deserialize)]` builds from the same serde
//! attributes: field and variant names after renaming, the enum
//! representation, flattened members and skipped fields. A field decoded by a
//! custom function has no layout serde can describe, so it must name one with
//! `#[heap_layout(as = Type)]` or `#[heap_layout(scalar)]`.
use proc_macro2::TokenStream;
use quote::quote;
use syn::ext::IdentExt;
use syn::{Data, DeriveInput, Fields, LitStr, Type};

#[derive(Clone, Copy)]
enum Rule {
    Lower,
    Upper,
    Pascal,
    Camel,
    Snake,
    ScreamingSnake,
    Kebab,
    ScreamingKebab,
}
impl Rule {
    fn parse(text: &LitStr) -> syn::Result<Self> {
        Ok(match text.value().as_str() {
            "lowercase" => Self::Lower,
            "UPPERCASE" => Self::Upper,
            "PascalCase" => Self::Pascal,
            "camelCase" => Self::Camel,
            "snake_case" => Self::Snake,
            "SCREAMING_SNAKE_CASE" => Self::ScreamingSnake,
            "kebab-case" => Self::Kebab,
            "SCREAMING-KEBAB-CASE" => Self::ScreamingKebab,
            _ => return Err(syn::Error::new_spanned(text, "unknown serde rename rule")),
        })
    }
    /// serde's rule for a `snake_case` field name.
    fn field(self, field: &str) -> String {
        match self {
            Self::Lower | Self::Snake => field.to_owned(),
            Self::Upper | Self::ScreamingSnake => field.to_ascii_uppercase(),
            Self::Pascal => {
                let mut pascal = String::new();
                let mut capitalize = true;
                for ch in field.chars() {
                    if ch == '_' {
                        capitalize = true;
                    } else if capitalize {
                        pascal.push(ch.to_ascii_uppercase());
                        capitalize = false;
                    } else {
                        pascal.push(ch);
                    }
                }
                pascal
            }
            Self::Camel => lower_first(&Self::Pascal.field(field)),
            Self::Kebab => field.replace('_', "-"),
            Self::ScreamingKebab => field.to_ascii_uppercase().replace('_', "-"),
        }
    }
    /// serde's rule for a `PascalCase` variant name.
    fn variant(self, variant: &str) -> String {
        match self {
            Self::Pascal => variant.to_owned(),
            Self::Lower => variant.to_ascii_lowercase(),
            Self::Upper => variant.to_ascii_uppercase(),
            Self::Camel => lower_first(variant),
            Self::Snake => {
                let mut snake = String::new();
                for (index, ch) in variant.char_indices() {
                    if index > 0 && ch.is_uppercase() {
                        snake.push('_');
                    }
                    snake.push(ch.to_ascii_lowercase());
                }
                snake
            }
            Self::ScreamingSnake => Self::Snake.variant(variant).to_ascii_uppercase(),
            Self::Kebab => Self::Snake.variant(variant).replace('_', "-"),
            Self::ScreamingKebab => Self::ScreamingSnake.variant(variant).replace('_', "-"),
        }
    }
}
fn lower_first(text: &str) -> String {
    let mut chars = text.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_ascii_lowercase().to_string() + chars.as_str()
    })
}

/// The serde and layout attributes of one item, variant or field.
#[derive(Default)]
struct Attrs {
    rename: Option<String>,
    aliases: Vec<String>,
    rename_all: Option<Rule>,
    rename_all_fields: Option<Rule>,
    tag: Option<String>,
    content: Option<String>,
    untagged: bool,
    transparent: bool,
    flatten: bool,
    skip: bool,
    custom: bool,
    from: Option<Type>,
    layout: Option<TokenStream>,
    krate: Option<syn::Path>,
}
/// A `name = "value"` or `name(deserialize = "value")` serde attribute value.
fn deserialize_name(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<Option<LitStr>> {
    if meta.input.peek(syn::Token![=]) {
        return Ok(Some(meta.value()?.parse()?));
    }
    let mut name = None;
    meta.parse_nested_meta(|side| {
        let value: LitStr = side.value()?.parse()?;
        if side.path.is_ident("deserialize") {
            name = Some(value);
        }
        Ok(())
    })?;
    Ok(name)
}
/// Consumes the value of an attribute this derive does not read.
fn skip_value(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
    if meta.input.peek(syn::Token![=]) {
        meta.value()?.parse::<syn::Expr>()?;
    } else if meta.input.peek(syn::token::Paren) {
        meta.parse_nested_meta(|nested| skip_value(&nested))?;
    }
    Ok(())
}
impl Attrs {
    fn parse(attrs: &[syn::Attribute]) -> syn::Result<Self> {
        let mut parsed = Self::default();
        for attr in attrs {
            if attr.path().is_ident("serde") {
                attr.parse_nested_meta(|meta| parsed.serde(&meta))?;
            } else if attr.path().is_ident("heap_layout") {
                attr.parse_nested_meta(|meta| parsed.heap_layout(&meta))?;
            }
        }
        Ok(parsed)
    }
    fn serde(&mut self, meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
        let key = meta
            .path
            .get_ident()
            .map(ToString::to_string)
            .unwrap_or_default();
        match key.as_str() {
            "rename" => {
                if let Some(name) = deserialize_name(meta)? {
                    self.rename = Some(name.value());
                }
            }
            "alias" => self.aliases.push(meta.value()?.parse::<LitStr>()?.value()),
            "rename_all" => {
                if let Some(rule) = deserialize_name(meta)? {
                    self.rename_all = Some(Rule::parse(&rule)?);
                }
            }
            "rename_all_fields" => {
                if let Some(rule) = deserialize_name(meta)? {
                    self.rename_all_fields = Some(Rule::parse(&rule)?);
                }
            }
            "tag" => self.tag = Some(meta.value()?.parse::<LitStr>()?.value()),
            "content" => self.content = Some(meta.value()?.parse::<LitStr>()?.value()),
            "untagged" => self.untagged = true,
            "transparent" => self.transparent = true,
            "flatten" => self.flatten = true,
            "skip" | "skip_deserializing" => self.skip = true,
            "with" | "deserialize_with" => {
                self.custom = true;
                skip_value(meta)?;
            }
            "from" | "try_from" => {
                self.from = Some(meta.value()?.parse::<LitStr>()?.parse()?);
            }
            _ => skip_value(meta)?,
        }
        Ok(())
    }
    fn heap_layout(&mut self, meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
        if meta.path.is_ident("as") {
            let ty: Type = meta.value()?.parse()?;
            self.layout = Some(quote!(<#ty as __layout::HeapLayout>::LAYOUT));
        } else if meta.path.is_ident("scalar") {
            self.layout = Some(quote!(&__layout::JsonLayout::Scalar));
        } else if meta.path.is_ident("crate") {
            self.krate = Some(meta.value()?.parse()?);
        } else {
            return Err(meta.error("expected `as = Type`, `scalar` or `crate = path`"));
        }
        Ok(())
    }
}

/// One decoded field: its JSON names, or a flattened layout.
struct Member {
    names: Vec<String>,
    layout: TokenStream,
    flatten: bool,
}
fn members(fields: &Fields, rule: Option<Rule>) -> syn::Result<Vec<Member>> {
    let mut members = Vec::new();
    for (index, field) in fields.iter().enumerate() {
        let attrs = Attrs::parse(&field.attrs)?;
        if attrs.skip {
            continue;
        }
        let layout = match (attrs.layout, attrs.custom) {
            (Some(layout), _) => layout,
            (None, true) => {
                return Err(syn::Error::new_spanned(
                    field,
                    "a field decoded by a custom function needs #[heap_layout(as = Type)] or #[heap_layout(scalar)]",
                ));
            }
            (None, false) => {
                let ty = &field.ty;
                quote!(<#ty as __layout::HeapLayout>::LAYOUT)
            }
        };
        let name = field
            .ident
            .as_ref()
            .map_or_else(|| index.to_string(), |ident| ident.unraw().to_string());
        let mut names = vec![attrs.rename.unwrap_or_else(|| match rule {
            Some(rule) => rule.field(&name),
            None => name,
        })];
        names.extend(attrs.aliases);
        members.push(Member {
            names,
            layout,
            flatten: attrs.flatten,
        });
    }
    Ok(members)
}
/// A layout: a `JsonLayout` value, or a reference to another type's layout.
enum Layout {
    Value(TokenStream),
    Ref(TokenStream),
}
impl Layout {
    /// A `&'static JsonLayout` expression.
    fn reference(&self) -> TokenStream {
        match self {
            Self::Value(value) => quote!(&#value),
            Self::Ref(reference) => reference.clone(),
        }
    }
}
fn scalar() -> TokenStream {
    quote!(&__layout::JsonLayout::Scalar)
}
/// The layout of a struct body or enum variant's content.
fn body(fields: &Fields, rule: Option<Rule>, transparent: bool) -> syn::Result<Layout> {
    let members = members(fields, rule)?;
    if transparent {
        let [member] = members.as_slice() else {
            return Err(syn::Error::new_spanned(
                fields,
                "a transparent record decodes exactly one field",
            ));
        };
        return Ok(Layout::Ref(member.layout.clone()));
    }
    Ok(match fields {
        Fields::Unit => Layout::Ref(scalar()),
        Fields::Unnamed(unnamed) if unnamed.unnamed.len() == 1 => match members.as_slice() {
            [member] => Layout::Ref(member.layout.clone()),
            _ => Layout::Ref(scalar()),
        },
        Fields::Unnamed(_) => {
            let elements = members.iter().map(|member| &member.layout);
            let scalar = scalar();
            Layout::Value(quote!(__layout::JsonLayout::Node {
                seq: ::core::option::Option::Some(__layout::Alloc::INLINE),
                map: ::core::option::Option::None,
                tagged: false,
                fields: &[],
                elements: &[#(#elements,)* #scalar],
                flatten: &[],
            }))
        }
        Fields::Named(_) => Layout::Value(record(&members, None)),
    })
}
/// A struct object, also decodable from an array of its fields unless a
/// flattened member makes serde buffer the object. An internally tagged
/// enum's object adds its `tag`, is buffered before the variant is known,
/// and is refused as an array.
fn record(members: &[Member], tag: Option<&str>) -> TokenStream {
    let flattened: Vec<_> = members.iter().filter(|member| member.flatten).collect();
    let named: Vec<_> = members.iter().filter(|member| !member.flatten).collect();
    let scalar = scalar();
    let fields = tag
        .map(|tag| quote!((#tag, #scalar)))
        .into_iter()
        .chain(named.iter().flat_map(|member| {
            let layout = &member.layout;
            member
                .names
                .iter()
                .map(move |name| quote!((#name, #layout)))
        }));
    let flatten = flattened.iter().map(|member| &member.layout);
    let tagged = tag.is_some() || !flattened.is_empty();
    let (seq, elements) = if tagged {
        (quote!(::core::option::Option::None), Vec::new())
    } else {
        let mut elements: Vec<_> = named.iter().map(|member| member.layout.clone()).collect();
        elements.push(scalar);
        (
            quote!(::core::option::Option::Some(__layout::Alloc::INLINE)),
            elements,
        )
    };
    quote!(__layout::JsonLayout::Node {
        seq: #seq,
        map: ::core::option::Option::Some(__layout::Alloc::INLINE),
        tagged: #tagged,
        fields: &[#(#fields,)*],
        elements: &[#(#elements,)*],
        flatten: &[#(#flatten,)*],
    })
}

fn enumeration(data: &syn::DataEnum, attrs: &Attrs) -> syn::Result<Layout> {
    // Externally tagged members, adjacently tagged contents, internally
    // tagged members, and variants decoded without a tag.
    let mut external = Vec::new();
    let mut contents = Vec::new();
    let mut internal = Vec::new();
    let mut untagged = Vec::new();
    for variant in &data.variants {
        let variant_attrs = Attrs::parse(&variant.attrs)?;
        if variant_attrs.skip {
            continue;
        }
        let rule = variant_attrs.rename_all.or(attrs.rename_all_fields);
        let layout = match (variant_attrs.layout, variant_attrs.custom) {
            (Some(layout), _) => layout,
            (None, true) => {
                return Err(syn::Error::new_spanned(
                    variant,
                    "a variant decoded by a custom function needs #[heap_layout(as = Type)] or #[heap_layout(scalar)]",
                ));
            }
            (None, false) => body(&variant.fields, rule, false)?.reference(),
        };
        if variant_attrs.untagged || attrs.untagged {
            untagged.push(layout);
            continue;
        }
        if attrs.tag.is_some() && attrs.content.is_none() {
            match &variant.fields {
                Fields::Named(_) => internal.extend(members(&variant.fields, rule)?),
                Fields::Unnamed(_) => internal.push(Member {
                    names: Vec::new(),
                    layout: layout.clone(),
                    flatten: true,
                }),
                Fields::Unit => {}
            }
        }
        let name = variant.ident.unraw().to_string();
        let mut names = vec![
            variant_attrs
                .rename
                .unwrap_or_else(|| match attrs.rename_all {
                    Some(rule) => rule.variant(&name),
                    None => name,
                }),
        ];
        names.extend(variant_attrs.aliases);
        for name in names {
            external.push(quote!((#name, #layout)));
        }
        contents.push(layout);
    }
    let scalar = scalar();
    let tagged = match (&attrs.tag, &attrs.content) {
        _ if attrs.untagged => None,
        (Some(tag), None) => Some(record(&internal, Some(tag))),
        (Some(tag), Some(content)) => {
            let content_layout = quote!(&__layout::JsonLayout::Any {
                tagged: false,
                of: &[#(#contents,)*],
            });
            Some(quote!(__layout::JsonLayout::Node {
                seq: ::core::option::Option::Some(__layout::Alloc::INLINE),
                map: ::core::option::Option::Some(__layout::Alloc::INLINE),
                tagged: true,
                fields: &[(#tag, #scalar), (#content, #content_layout)],
                elements: &[#scalar, #content_layout, #scalar],
                flatten: &[],
            }))
        }
        _ => Some(quote!(__layout::JsonLayout::Node {
            seq: ::core::option::Option::None,
            map: ::core::option::Option::Some(__layout::Alloc::INLINE),
            tagged: false,
            fields: &[#(#external,)*],
            elements: &[],
            flatten: &[],
        })),
    };
    Ok(match (tagged, untagged.is_empty()) {
        (Some(tagged), true) => Layout::Value(tagged),
        (tagged, _) => {
            let of = tagged
                .map(|tagged| quote!(&#tagged))
                .into_iter()
                .chain(untagged);
            Layout::Value(quote!(__layout::JsonLayout::Any {
                tagged: true,
                of: &[#(#of,)*],
            }))
        }
    })
}

pub fn expand(input: &DeriveInput) -> syn::Result<TokenStream> {
    let attrs = Attrs::parse(&input.attrs)?;
    let layout = if let Some(from) = &attrs.from {
        Layout::Ref(quote!(<#from as __layout::HeapLayout>::LAYOUT))
    } else {
        match &input.data {
            Data::Struct(data) => body(&data.fields, attrs.rename_all, attrs.transparent)?,
            Data::Enum(data) => enumeration(data, &attrs)?,
            Data::Union(_) => {
                return Err(syn::Error::new_spanned(
                    input,
                    "unions have no serde layout",
                ));
            }
        }
    };
    let krate = attrs
        .krate
        .unwrap_or_else(|| syn::parse_quote!(::acyclic_harness_core));
    let name = &input.ident;
    let mut generics = input.generics.clone();
    let generic = generics.type_params().next().is_some();
    for param in generics.type_params_mut() {
        param
            .bounds
            .push(syn::parse_quote!(#krate::heap_layout::HeapLayout));
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    // A non-generic layout lives in a static, so recursive types can refer to
    // themselves; a generic one is promoted per instantiation.
    let value = match layout {
        Layout::Value(value) if !generic => quote!({
            static LAYOUT: __layout::JsonLayout = #value;
            &LAYOUT
        }),
        layout => layout.reference(),
    };
    Ok(quote! {
        const _: () = {
            use #krate::heap_layout as __layout;
            #[automatically_derived]
            impl #impl_generics __layout::HeapLayout for #name #ty_generics #where_clause {
                const LAYOUT: &'static __layout::JsonLayout = #value;
            }
        };
    })
}
