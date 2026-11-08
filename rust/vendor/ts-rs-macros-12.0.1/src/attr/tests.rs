use super::*;
use syn::{parse_quote, ItemEnum, ItemStruct};

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

#[test]
fn container_serialization_preserves_transparent_fields() -> syn::Result<()> {
    let newtype: ItemStruct = parse_quote!(
        struct Wrapper(String);
    );
    let expected = crate::types::struct_def(&newtype)?.inline.to_string();
    for declaration in [
        quote!(
            #[serde(transparent)]
            struct Wrapper {
                value: String,
            }
        ),
        quote!(
            #[serde(transparent)]
            struct Wrapper(#[serde(skip)] (), String);
        ),
        quote!(
            #[serde(transparent)]
            struct Wrapper {
                #[serde(skip_serializing)]
                cache: (),
                value: String,
            }
        ),
        quote!(
            #[serde(transparent)]
            struct Wrapper(String, std::marker::PhantomData<()>);
        ),
    ] {
        let item: ItemStruct = syn::parse2(declaration)?;
        assert!(StructAttr::from_attrs(&item.attrs)?.transparent);
        assert_eq!(
            crate::types::struct_def(&item)?.inline.to_string(),
            expected
        );
    }
    for declaration in [
        quote!(
            #[serde(transparent)]
            struct Empty;
        ),
        quote!(
            #[serde(transparent)]
            struct Empty(#[serde(skip)] ());
        ),
        quote!(
            #[serde(transparent)]
            struct Multiple(String, String);
        ),
    ] {
        let item: ItemStruct = syn::parse2(declaration)?;
        assert!(crate::types::struct_def(&item).is_err());
    }
    let ordinary: ItemEnum = parse_quote! {
        #[serde(tag = "kind", rename_all = "camelCase")]
        enum Reply { Ready { file_name: String }, Closed }
    };
    let mut restricted = ordinary.clone();
    restricted
        .attrs
        .push(parse_quote!(#[serde(deny_unknown_fields)]));
    assert_eq!(
        crate::types::enum_def(&ordinary)?.inline.to_string(),
        crate::types::enum_def(&restricted)?.inline.to_string()
    );
    assert!(syn::parse2::<Serde<StructAttr>>(quote!(transparent = true)).is_err());
    assert!(syn::parse2::<Serde<StructAttr>>(quote!(deny_unknown_fields = true)).is_err());
    assert!(syn::parse2::<Serde<EnumAttr>>(quote!(deny_unknown_fields = true)).is_err());
    Ok(())
}
