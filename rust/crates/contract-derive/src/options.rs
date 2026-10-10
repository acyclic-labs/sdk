use syn::ext::IdentExt;
use syn::{LitInt, LitStr, Path, Type, meta::ParseNestedMeta};

#[derive(Default)]
pub struct Options {
    seen: std::collections::BTreeSet<String>,
    pub error: Option<Path>,
    pub file: Option<Path>,
    pub post: Option<Path>,
    pub name: Option<LitStr>,
    pub unknown: Option<Path>,
    pub from: Option<Path>,
    pub into: Option<Path>,
    pub tag: Option<u32>,
    pub kind: Option<String>,
    pub oneof: Option<LitStr>,
}

impl Options {
    fn claim(&mut self, meta: &ParseNestedMeta<'_>) -> syn::Result<()> {
        let key = meta
            .path
            .get_ident()
            .ok_or_else(|| meta.error("contract options must be identifiers"))?
            .to_string();
        if !self.seen.insert(key) {
            return Err(meta.error("duplicate contract option"));
        }
        Ok(())
    }

    pub fn message(&mut self, meta: &ParseNestedMeta<'_>) -> syn::Result<()> {
        self.claim(meta)?;
        if meta.path.is_ident("error") {
            self.error = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("file") {
            self.file = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("post") {
            self.post = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("name") {
            self.name = Some(meta.value()?.parse()?);
        } else {
            return Err(meta.error("unknown contract declaration option"));
        }
        Ok(())
    }

    pub fn field(&mut self, meta: &ParseNestedMeta<'_>) -> syn::Result<()> {
        self.claim(meta)?;
        if meta.path.is_ident("tag") {
            self.tag = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("from") {
            self.from = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("into") {
            self.into = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("oneof") {
            self.oneof = Some(meta.value()?.parse()?);
        } else if [
            "string",
            "bytes",
            "uint32",
            "uint64",
            "bool",
            "message",
            "enumeration",
        ]
        .iter()
        .any(|kind| meta.path.is_ident(kind))
        {
            if self.kind.is_some() {
                return Err(meta.error("multiple wire kinds"));
            }
            self.kind = meta.path.get_ident().map(ToString::to_string);
        } else {
            return Err(meta.error("unknown contract field option"));
        }
        Ok(())
    }

    pub fn enumeration(&mut self, meta: &ParseNestedMeta<'_>) -> syn::Result<()> {
        if meta.path.is_ident("unknown") {
            self.claim(meta)?;
            self.unknown = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("error") || meta.path.is_ident("name") {
            self.message(meta)
        } else {
            Err(meta.error("unknown enum option"))
        }
    }

    pub fn take_fields(attrs: &mut Vec<syn::Attribute>) -> syn::Result<Self> {
        let mut options = Self::default();
        for attr in attrs.iter().filter(|a| a.path().is_ident("wire")) {
            attr.parse_nested_meta(|meta| options.field(&meta))?;
        }
        attrs.retain(|a| !a.path().is_ident("wire"));
        Ok(options)
    }
}

pub fn inner(ty: &Type, wrapper: &str) -> Option<Type> {
    if let Type::Path(path) = ty
        && let Some(segment) = path.path.segments.last()
        && segment.ident == wrapper
        && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
        && args.args.len() == 1
        && let Some(syn::GenericArgument::Type(ty)) = args.args.first()
    {
        Some(ty.clone())
    } else {
        None
    }
}

pub fn shadow(ty: &Type) -> syn::Result<Type> {
    let Type::Path(mut path) = ty.clone() else {
        return Err(syn::Error::new_spanned(ty, "message type must be a path"));
    };
    let last = path
        .path
        .segments
        .last_mut()
        .ok_or_else(|| syn::Error::new_spanned(ty, "empty message path"))?;
    if !matches!(last.arguments, syn::PathArguments::None) {
        return Err(syn::Error::new_spanned(
            ty,
            "generic message types are unsupported",
        ));
    }
    last.ident = quote::format_ident!("{}Proto", last.ident);
    Ok(Type::Path(path))
}

pub fn primitive(ty: &Type) -> syn::Result<String> {
    if let Type::Path(path) = ty {
        for (name, kind) in [
            ("String", "string"),
            ("bool", "bool"),
            ("u32", "uint32"),
            ("u64", "uint64"),
        ] {
            if path.path.is_ident(name) {
                return Ok(kind.into());
            }
        }
    }
    if let Some(Type::Path(path)) = inner(ty, "Vec")
        && path.path.is_ident("u8")
    {
        return Ok("bytes".into());
    }
    Err(syn::Error::new_spanned(
        ty,
        "wire kind must be explicit for nominal or nested types",
    ))
}

pub fn tag(
    value: u32,
    seen: &mut std::collections::BTreeSet<u32>,
    at: impl quote::ToTokens,
) -> syn::Result<()> {
    if value == 0 || value >= (1 << 29) || (19000..20000).contains(&value) || !seen.insert(value) {
        return Err(syn::Error::new_spanned(
            at,
            "invalid or duplicate protobuf tag",
        ));
    }
    Ok(())
}

/// Require exact file ownership and sealed inventory membership at compile time.
pub fn owner_check(
    file: &Path,
    ty: &proc_macro2::TokenStream,
    member: bool,
) -> proc_macro2::TokenStream {
    let membership = member.then(|| quote::quote!(const _: () = #file::require_member::<#ty>();));
    quote::quote! {
        const _: #file = <#ty>::FILE;
        #membership
    }
}

/// Map a Rust identifier to its literal protobuf name, without raw-token syntax.
pub fn identifier(ident: &syn::Ident) -> String {
    ident.unraw().to_string()
}
