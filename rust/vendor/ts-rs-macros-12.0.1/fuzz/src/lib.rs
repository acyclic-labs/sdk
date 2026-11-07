//! Fuzz the actual derivation parser against Serde's serialization field selection.

#[macro_use]
#[path = "../../src/utils.rs"]
mod utils;
#[path = "../../src/attr/mod.rs"]
mod attr;
#[path = "../../src/deps.rs"]
mod deps;
#[path = "../../src/optional.rs"]
mod optional;

pub use attr::{EnumAttr, FieldAttr, Inflection, Repr, StructAttr, Tagged, VariantAttr};
pub use deps::Dependencies;
pub use optional::{Optional, apply, parse_optional};
pub use utils::{format_generics, make_string_literal, raw_name_to_ts_field, to_ts_ident};

use attr::{Attr, ContainerAttr, Serde};
use serde_derive_internals::{Ctxt, Derive, ast};

/// Exercises raw metadata and full declarations, comparing accepted transparent
/// containers with the same checked AST used by Serde's serializer derivation.
pub fn exercise(source: &str) {
    let _ = syn::parse_str::<Serde<StructAttr>>(source);
    let _ = syn::parse_str::<Serde<EnumAttr>>(source);
    let _ = syn::parse_str::<Serde<FieldAttr>>(source);
    let _ = syn::parse_str::<Serde<VariantAttr>>(source);
    let Ok(input) = syn::parse_str::<syn::DeriveInput>(source) else {
        return;
    };
    match &input.data {
        syn::Data::Struct(data) => {
            if let Ok(attrs) = StructAttr::from_attrs(&input.attrs) {
                let _ = attrs.crate_rename();
                let _ = attrs.assert_validity(&data.fields);
                if attrs.transparent {
                    let _ = StructAttr::transparent_field(&data.fields);
                }
            }
            for field in &data.fields {
                if let Ok(attrs) = FieldAttr::from_attrs(&field.attrs) {
                    let _ = attrs.assert_validity(field);
                }
            }
        }
        syn::Data::Enum(data) => {
            if let Ok(item) = syn::parse_str::<syn::ItemEnum>(source) {
                if let Ok(attrs) = EnumAttr::from_attrs(&input.attrs) {
                    let _ = attrs.assert_validity(&item);
                }
            }
            for variant in &data.variants {
                if let Ok(attrs) = VariantAttr::from_attrs(&variant.attrs) {
                    let _ = attrs.assert_validity(variant);
                }
            }
        }
        syn::Data::Union(_) => return,
    }
    let cx = Ctxt::new();
    let container = ast::Container::from_ast(
        &cx,
        &input,
        Derive::Serialize,
        &syn::Ident::new("__private", proc_macro2::Span::call_site()),
    );
    if cx.check().is_err() {
        return;
    }
    let Some(container) = container else {
        return;
    };
    if !container.attrs.transparent() {
        return;
    }
    let (syn::Data::Struct(data), ast::Data::Struct(_, fields)) = (&input.data, &container.data)
    else {
        return;
    };
    let expected = fields.iter().find(|field| field.attrs.transparent());
    let actual = StructAttr::transparent_field(&data.fields);
    match (expected, actual) {
        (Some(expected), Ok(actual)) => assert_eq!(expected.original, actual),
        _ => panic!("transparent serialization differs from the checked Serde AST"),
    }
}
