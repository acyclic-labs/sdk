use super::*;
use syn::{ItemEnum, ItemStruct, parse_quote};

#[test]
fn deserialization_metadata_preserves_serialization() -> syn::Result<()> {
    let ordinary: ItemStruct = parse_quote! {
        #[serde(rename_all = "camelCase")]
        struct Metadata { protocol_version: u32, handle: String }
    };
    let mut validated = ordinary.clone();
    validated
        .attrs
        .push(parse_quote!(#[serde(try_from = "RawMetadata")]));
    assert_eq!(
        crate::types::struct_def(&ordinary)?.inline.to_string(),
        crate::types::struct_def(&validated)?.inline.to_string()
    );

    let ordinary: ItemEnum = parse_quote! {
        #[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
        enum Event { Open { actor_id: String }, Unknown }
    };
    let mut extensible = ordinary.clone();
    let unknown = extensible.variants.last_mut().ok_or_else(|| {
        syn::Error::new(proc_macro2::Span::call_site(), "missing catch-all variant")
    })?;
    unknown.attrs.push(parse_quote!(#[serde(other)]));
    extensible
        .attrs
        .push(parse_quote!(#[serde(try_from = "RawEvent")]));
    assert_eq!(
        crate::types::enum_def(&ordinary)?.inline.to_string(),
        crate::types::enum_def(&extensible)?.inline.to_string()
    );

    // A recognized attribute must consume its actual grammar. A generic
    // unknown-attribute skip would accept each of these malformed forms.
    for malformed in [quote!(try_from = 7), quote!(try_from = "not a type!")] {
        assert!(syn::parse2::<Serde<StructAttr>>(malformed.clone()).is_err());
        assert!(syn::parse2::<Serde<EnumAttr>>(malformed).is_err());
    }
    assert!(syn::parse2::<Serde<VariantAttr>>(quote!(other = "ignored")).is_err());
    Ok(())
}
