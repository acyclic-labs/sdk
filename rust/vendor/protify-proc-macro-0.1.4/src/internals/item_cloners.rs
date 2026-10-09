use crate::*;

pub fn create_shadow_struct(item: &ItemStruct) -> ItemStruct {
	let item_fields = if let Fields::Named(fields) = &item.fields {
		fields.named.iter().map(|f| Field {
			attrs: vec![],
			// Proxied structs are the public wire shadow of a private-field
			// semantic model.  Keep the semantic fields encapsulated while making
			// the generated protobuf fields accessible to transport adapters.
			vis: Visibility::Public(token::Pub::default()),
			mutability: syn::FieldMutability::None,
			ident: f.ident.clone(),
			colon_token: f.colon_token,
			ty: f.ty.clone(),
		})
	} else {
		unreachable!()
	};

	ItemStruct {
		attrs: vec![],
		vis: Visibility::Public(token::Pub::default()),
		struct_token: token::Struct::default(),
		ident: format_ident!("{}Proto", item.ident),
		generics: item.generics.clone(),
		fields: Fields::Named(syn::FieldsNamed {
			brace_token: token::Brace::default(),
			named: item_fields.collect(),
		}),
		semi_token: None,
	}
}

pub fn create_shadow_enum(item: &ItemEnum) -> ItemEnum {
	let variants = item.variants.iter().map(|variant| Variant {
		attrs: vec![],
		ident: variant.ident.clone(),
		discriminant: variant.discriminant.clone(),
		fields: { let mut fields = variant.fields.clone(); for field in fields.iter_mut() { field.attrs.retain(|attr| !attr.path().is_ident("ts")); } fields },
	});

	ItemEnum {
		attrs: vec![],
		vis: Visibility::Public(token::Pub::default()),
		enum_token: token::Enum::default(),
		ident: format_ident!("{}Proto", item.ident),
		generics: item.generics.clone(),
		brace_token: token::Brace::default(),
		variants: variants.collect(),
	}
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::ToTokens;

    #[test]
    fn wire_shadow_keeps_original_payload_and_isolates_only_ts_attributes() {
        let source: ItemEnum = parse_quote! {
            pub enum Source { Inline(#[ts(type = "Uint8Array")] #[doc = "retained"] Vec<u8>) }
        };
        let original = source.to_token_stream().to_string();
        let wire = create_shadow_enum(&source);
        let field = wire.variants.first().unwrap().fields.iter().next().unwrap();
        assert!(!field.attrs.iter().any(|attr| attr.path().is_ident("ts")));
        assert!(field.attrs.iter().any(|attr| attr.path().is_ident("doc")));
        assert_eq!(source.to_token_stream().to_string(), original);
    }
}
