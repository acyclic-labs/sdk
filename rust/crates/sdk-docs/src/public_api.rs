//! Checked bridge from rustdoc JSON format 60 to public-api 0.52.2's format-59 parser.
//!
//! This accepts only normal public rustdoc JSON. Format 60's optional
//! `default_unstable` metadata is rejected when populated because public-api 0.52.2
//! cannot represent it; null fields are removed only from the three typed slots
//! where rustdoc-types 0.60 defines them.

use crate::Error;
use public_api::tokens::Token;
use rustdoc_types::{Crate, FORMAT_VERSION, Id, ItemEnum};
use serde_json::Value;
use std::fs;
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicItemSignature {
    pub id: Id,
    pub parent_id: Option<Id>,
    pub display: String,
    /// The exported path represented by this public-api occurrence.
    pub path: Vec<String>,
}

pub fn extract(json_path: &Path) -> Result<Vec<PublicItemSignature>, Error> {
    let raw = fs::read(json_path)?;
    let value: Value = serde_json::from_slice(&raw)?;
    let krate: Crate = serde_json::from_value(value.clone())?;
    if krate.format_version != FORMAT_VERSION {
        return Err(Error::Invalid(format!(
            "{} uses unsupported rustdoc format {}; expected {}",
            json_path.display(),
            krate.format_version,
            FORMAT_VERSION
        )));
    }
    if krate.includes_private {
        return Err(Error::Invalid(format!(
            "{} includes private rustdoc items; public-api requires normal public JSON",
            json_path.display()
        )));
    }
    let adapted = adapt(value, &krate)?;
    let mut temp = tempfile::NamedTempFile::new()?;
    serde_json::to_writer(temp.as_file_mut(), &adapted)?;
    let api = public_api::Builder::from_rustdoc_json(temp.path())
        .include_function_parameter_names(true)
        .build()
        .map_err(|error| Error::Invalid(format!("public-api: {error}")))?;
    api.items()
        .map(|item| {
            Ok(PublicItemSignature {
                id: Id(item.id().0),
                parent_id: item.parent_id().map(|id| Id(id.0)),
                display: item.to_string(),
                path: exported_path(item.tokens())?,
            })
        })
        .collect()
}
fn adapt(mut value: Value, krate: &Crate) -> Result<Value, Error> {
    if value.get("format_version").and_then(Value::as_u64) != Some(60) {
        return Err(Error::Invalid("adapter requires rustdoc format 60".into()));
    }
    if value.get("includes_private").and_then(Value::as_bool) != Some(false) {
        return Err(Error::Invalid(
            "adapter requires includes_private=false".into(),
        ));
    }

    let slots = krate.index.iter().filter_map(|(id, item)| {
        let field = match &item.inner {
            ItemEnum::Function(_) => "function",
            ItemEnum::AssocConst { .. } => "assoc_const",
            ItemEnum::AssocType { .. } => "assoc_type",
            _ => return None,
        };
        Some((id.0, field))
    });
    remove_known_fields(&mut value, slots)?;
    value["format_version"] = Value::from(59);
    Ok(value)
}

fn exported_path<'a>(tokens: impl Iterator<Item = &'a Token>) -> Result<Vec<String>, Error> {
    let tokens = tokens.collect::<Vec<_>>();
    let Some(start) = tokens.iter().enumerate().find_map(|(index, token)| {
        if matches!(token, Token::Kind(_)) {
            Some(index + 1)
        } else if matches!(token, Token::Keyword(kind) if kind == "impl") {
            tokens
                .iter()
                .enumerate()
                .skip(index + 1)
                .find_map(|(index, token)| {
                    matches!(token, Token::Keyword(kind) if kind == "for").then_some(index + 1)
                })
                .or(Some(index + 1))
        } else if matches!(
            token,
            Token::Identifier(_) | Token::Function(_) | Token::Type(_)
        ) {
            Some(index)
        } else {
            None
        }
    }) else {
        return Err(Error::Invalid(
            "public-api item has no supported exported path token".into(),
        ));
    };

    let mut path = Vec::new();
    let mut need_component = true;
    let mut generic_depth = 0usize;
    for token in tokens.into_iter().skip(start) {
        if generic_depth > 0 {
            match token {
                Token::Symbol(open) if open == "<" => generic_depth += 1,
                Token::Symbol(close) if close == ">" => generic_depth -= 1,
                _ => {}
            }
            continue;
        }
        match token {
            Token::Whitespace if need_component => {}
            Token::Identifier(name) | Token::Function(name) | Token::Type(name)
                if need_component =>
            {
                path.push(name.clone());
                need_component = false;
            }
            Token::Symbol(open) if open == "<" && !need_component => generic_depth = 1,
            Token::Symbol(separator) if separator == "::" && !need_component => {
                need_component = true;
            }
            _ => break,
        }
    }
    if path.is_empty() || need_component || generic_depth != 0 {
        return Err(Error::Invalid(
            "public-api item has an unsupported exported path token sequence".into(),
        ));
    }
    Ok(path)
}
fn remove_known_fields(
    value: &mut Value,
    slots: impl IntoIterator<Item = (u32, &'static str)>,
) -> Result<(), Error> {
    for (id, field) in slots {
        let Some(slot) = value
            .get_mut("index")
            .and_then(Value::as_object_mut)
            .and_then(|index| index.get_mut(&id.to_string()))
            .and_then(Value::as_object_mut)
            .and_then(|item| item.get_mut("inner"))
            .and_then(Value::as_object_mut)
            .and_then(|inner| inner.get_mut(field))
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        if let Some(metadata) = slot.get("default_unstable") {
            if !metadata.is_null() {
                return Err(Error::Invalid(format!(
                    "unsupported non-null rustdoc 0.60 field: index.{id}.inner.{field}.default_unstable"
                )));
            }
            slot.remove("default_unstable");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{exported_path, remove_known_fields};
    use public_api::tokens::Token;
    use serde_json::json;

    fn base() -> serde_json::Value {
        json!({
            "format_version": 60,
            "includes_private": false,
            "default_unstable": {"user_defined": true},
            "index": {
                "7": {"inner": {"function": {
                    "default_unstable": null,
                    "user_defined": {"default_unstable": "keep"}
                }}},
                "8": {"inner": {"module": {
                    "default_unstable": "keep"
                }}}
            }
        })
    }

    #[test]
    fn adapter_removes_only_typed_nullable_metadata() {
        let mut value = base();
        remove_known_fields(&mut value, [(7, "function")]).expect("nullable metadata is supported");
        assert!(
            value["index"]["7"]["inner"]["function"]
                .get("default_unstable")
                .is_none()
        );
        assert_eq!(value["default_unstable"]["user_defined"], true);
        assert_eq!(
            value["index"]["7"]["inner"]["function"]["user_defined"]["default_unstable"],
            "keep"
        );
        assert_eq!(
            value["index"]["8"]["inner"]["module"]["default_unstable"],
            "keep"
        );
    }

    #[test]
    fn token_path_preserves_exported_alias_and_qualified_method() {
        let tokens = [
            Token::Kind("fn".into()),
            Token::Whitespace,
            Token::Identifier("crate_name".into()),
            Token::Symbol("::".into()),
            Token::Type("Parent".into()),
            Token::Symbol("::".into()),
            Token::Function("method".into()),
            Token::Symbol("<".into()),
            Token::Generic("T".into()),
            Token::Symbol(">".into()),
        ];
        assert_eq!(
            exported_path(tokens.iter()).expect("qualified method path"),
            ["crate_name", "Parent", "method"]
        );

        let alias = [
            Token::Kind("use".into()),
            Token::Whitespace,
            Token::Identifier("crate_name".into()),
            Token::Symbol("::".into()),
            Token::Identifier("exported_alias".into()),
        ];
        assert_eq!(
            exported_path(alias.iter()).expect("alias path"),
            ["crate_name", "exported_alias"]
        );
    }
    #[test]
    fn token_path_handles_generic_receivers_and_enum_variants() {
        let method = [
            Token::Kind("fn".into()),
            Token::Whitespace,
            Token::Identifier("crate_name".into()),
            Token::Symbol("::".into()),
            Token::Type("Generic".into()),
            Token::Symbol("<".into()),
            Token::Generic("T".into()),
            Token::Symbol(">".into()),
            Token::Symbol("::".into()),
            Token::Function("method".into()),
            Token::Symbol("<".into()),
            Token::Generic("U".into()),
            Token::Symbol(">".into()),
        ];
        assert_eq!(
            exported_path(method.iter()).expect("generic method path"),
            ["crate_name", "Generic", "method"]
        );

        let variant = [
            Token::Qualifier("pub".into()),
            Token::Whitespace,
            Token::Identifier("crate_name".into()),
            Token::Symbol("::".into()),
            Token::Type("Choice".into()),
            Token::Symbol("::".into()),
            Token::Identifier("First".into()),
        ];
        assert_eq!(
            exported_path(variant.iter()).expect("variant path"),
            ["crate_name", "Choice", "First"]
        );
    }
    #[test]
    fn token_path_rejects_unknown_sequences() {
        let unknown = [Token::Keyword("unknown".into())];
        assert!(exported_path(unknown.iter()).is_err());

        let broken = [
            Token::Kind("fn".into()),
            Token::Whitespace,
            Token::Identifier("crate_name".into()),
            Token::Symbol("::".into()),
        ];
        assert!(exported_path(broken.iter()).is_err());
    }
    #[test]
    fn token_path_handles_impl_targets() {
        let implementation = [
            Token::Keyword("impl".into()),
            Token::Whitespace,
            Token::Identifier("crate_name".into()),
            Token::Symbol("::".into()),
            Token::Type("Parent".into()),
        ];
        assert_eq!(
            exported_path(implementation.iter()).expect("impl path"),
            ["crate_name", "Parent"]
        );

        let trait_implementation = [
            Token::Keyword("impl".into()),
            Token::Whitespace,
            Token::Type("Trait".into()),
            Token::Whitespace,
            Token::Keyword("for".into()),
            Token::Whitespace,
            Token::Identifier("crate_name".into()),
            Token::Symbol("::".into()),
            Token::Type("Parent".into()),
        ];
        assert_eq!(
            exported_path(trait_implementation.iter()).expect("trait impl path"),
            ["crate_name", "Parent"]
        );
    }
    #[test]
    fn adapter_rejects_populated_typed_metadata() {
        let mut value = base();
        value["index"]["7"]["inner"]["function"]["default_unstable"] = json!({"feature": "x"});
        let error = remove_known_fields(&mut value, [(7, "function")]).unwrap_err();
        assert!(format!("{error:?}").contains("index.7.inner.function.default_unstable"));
    }
}
