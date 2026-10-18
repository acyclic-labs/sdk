//! Shared authorization-option parser, exercised directly by properties/fuzzing.
pub fn fields(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<String> {
    let mut fields = Vec::<(String, String)>::new();
    meta.parse_nested_meta(|field| {
        let key = field
            .path
            .get_ident()
            .ok_or_else(|| field.error("expected authorization field"))?
            .to_string();
        if !matches!(key.as_str(), "action" | "resource" | "extractor") {
            return Err(field.error("unknown authorization field"));
        }
        let value = field.value()?.parse::<syn::LitStr>()?.value();
        if value.is_empty() || (key != "action" && fields.iter().any(|(k, _)| k == &key)) {
            return Err(field.error("empty or duplicate authorization field"));
        }
        fields.push((key, value));
        Ok(())
    })?;
    let has = |key: &str| fields.iter().any(|(k, _)| k == key);
    if has("resource") == has("extractor") || (has("resource") && !has("action")) {
        return Err(meta.error("authorization requires actions with one resource resolver, or a registered dynamic extractor"));
    }
    // Protobuf octal escapes preserve arbitrary UTF-8 and control bytes;
    // Rust Debug strings use escapes that protobuf does not understand.
    Ok(fields
        .into_iter()
        .map(|(key, value)| {
            let encoded = value
                .bytes()
                .map(|b| {
                    if (32..127).contains(&b) && !matches!(b, b'"' | b'\\') {
                        char::from(b).to_string()
                    } else {
                        format!("\\{b:03o}")
                    }
                })
                .collect::<String>();
            format!("{key}: \"{encoded}\"")
        })
        .collect::<Vec<_>>()
        .join(" "))
}
