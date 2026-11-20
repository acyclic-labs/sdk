//! Checked bridge from rustdoc JSON format 60 to public-api 0.52.2's format-59 parser.
//!
//! This accepts only normal public rustdoc JSON. Format 60's optional
//! `default_unstable` metadata is rejected when populated because public-api 0.52.2
//! cannot represent it; null fields are removed only from the three typed slots
//! where rustdoc-types 0.60 defines them.

use crate::Error;
use public_api::tokens::Token;
use rustdoc_types::{Crate, Id, ItemEnum, FORMAT_VERSION};
use serde_json::Value;
use std::{io::Write, path::Path};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicItemSignature {
    pub id: Id,
    pub parent_id: Option<Id>,
    pub display: String,
    /// The exported path represented by this public-api occurrence.
    pub path: Vec<String>,
}

pub fn extract(
    json_path: &Path,
    raw: &std::rc::Rc<[u8]>,
    krate: &Crate,
) -> Result<Vec<PublicItemSignature>, Error> {
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
    crate::render_cache::extract(raw, || extract_uncached(json_path, raw, krate))
}

fn extract_uncached(
    json_path: &Path,
    raw: &std::rc::Rc<[u8]>,
    krate: &Crate,
) -> Result<Vec<PublicItemSignature>, Error> {
    let value = (*crate::render_cache::json(raw)?).clone();
    let adapted = adapt(value, krate)?;
    let api = crate::render_cache::with_adapter(|temp| {
        {
            let mut writer = std::io::BufWriter::new(temp.as_file_mut());
            serde_json::to_writer(&mut writer, &adapted)?;
            writer.flush()?;
        }
        public_api::Builder::from_rustdoc_json(temp.path())
            .include_function_parameter_names(true)
            .build()
            .map_err(|error| Error::Invalid(format!("public-api: {error}")))
    })?;
    api.items()
        .map(|item| {
            let item_id = Id(item.id().0);
            let is_impl = krate
                .index
                .get(&item_id)
                .is_some_and(|rustdoc_item| matches!(rustdoc_item.inner, ItemEnum::Impl(_)));
            let path = match exported_path_for_item(item.tokens(), is_impl) {
                Ok(path) => path,
                Err(reason) => {
                    return Err(Error::Invalid(format!(
                        "{} public-api item {} ({}) has no supported exported path: {reason}",
                        json_path.display(),
                        item.id().0,
                        item
                    )));
                }
            };
            Ok(PublicItemSignature {
                id: item_id,
                parent_id: item.parent_id().map(|id| Id(id.0)),
                display: item.to_string(),
                path,
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

fn exported_path<'a>(tokens: impl Iterator<Item = &'a Token>) -> Result<Vec<String>, String> {
    let tokens = tokens.collect::<Vec<_>>();
    let proc_macro = tokens.windows(3).any(|tokens| {
        matches!(
            tokens,
            [Token::Kind(proc_kind), Token::Whitespace, Token::Kind(macro_kind)]
                if proc_kind == "proc" && macro_kind == "macro"
        )
    });
    let Some(start) = tokens.iter().enumerate().find_map(|(index, token)| {
        if proc_macro && matches!(token, Token::Kind(kind) if kind == "proc") {
            None
        } else if matches!(token, Token::Kind(_)) {
            Some(index + 1)
        } else if matches!(token, Token::Keyword(kind) if kind == "impl") {
            tokens
                .iter()
                .enumerate()
                .skip(index + 1)
                .find_map(|(index, token)| {
                    matches!(token, Token::Keyword(kind) if kind == "for").then_some(index + 1)
                })
                .or(Some(skip_impl_generics(&tokens, index + 1)))
        } else if matches!(
            token,
            Token::Identifier(_)
                | Token::Generic(_)
                | Token::Function(_)
                | Token::Type(_)
                | Token::Primitive(_)
                | Token::Self_(_)
        ) || matches!(token, Token::Symbol(open) if open == "[")
        {
            Some(index)
        } else {
            None
        }
    }) else {
        return Err(format!("no path-start token in {tokens:?}"));
    };

    let mut path = Vec::new();
    let mut need_component = true;
    let mut generic_depth = 0usize;
    let mut index = start;
    while let Some(token) = tokens.get(index).copied() {
        if generic_depth > 0 {
            match token {
                Token::Symbol(open) if open == "<" => generic_depth += 1,
                Token::Symbol(close) if close == ">" => generic_depth -= 1,
                _ => {}
            }
            index += 1;
            continue;
        }
        match token {
            Token::Whitespace if need_component => {
                index += 1;
            }
            // public-api wraps attribute and derive macro names after the
            // crate path; these delimiters are not exported path components.
            Token::Symbol(open)
                if proc_macro && need_component && (open == "#[" || open == "#[derive(") =>
            {
                index += 1;
            }
            // public-api renders receiver-qualified methods whose receiver is
            // borrowed as `fn &crate::Type::method(...)`. The reference is a
            // signature qualifier, not part of the exported path. Preserve
            // the path so binding-only getters and cancellation helpers are
            // projected with their exact rendered signature.
            Token::Symbol(qualifier)
                if need_component && (qualifier == "&" || qualifier == "*") =>
            {
                index += 1;
            }
            Token::Keyword(qualifier) if need_component && qualifier == "mut" => {
                index += 1;
            }
            Token::Lifetime(_) if need_component => {
                index += 1;
            }
            Token::Identifier(name)
            | Token::Generic(name)
            | Token::Function(name)
            | Token::Type(name)
            | Token::Primitive(name)
            | Token::Self_(name)
                if need_component =>
            {
                path.push(name.clone());
                need_component = false;
                index += 1;
            }
            Token::Symbol(open) if open == "[" && need_component => {
                let mut component = String::from("[");
                let mut depth = 1usize;
                index += 1;
                while let Some(token) = tokens.get(index).copied() {
                    component.push_str(token.text());
                    match token {
                        Token::Symbol(open) if open == "[" => depth += 1,
                        Token::Symbol(close) if close == "]" => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    index += 1;
                }
                if depth != 0 {
                    break;
                }
                path.push(component);
                need_component = false;
                index += 1;
            }
            Token::Symbol(open) if open == "<" && !need_component => {
                generic_depth = 1;
                index += 1;
            }
            Token::Symbol(separator) if separator == "::" && !need_component => {
                need_component = true;
                index += 1;
            }
            _ => break,
        }
    }
    if path.is_empty() || need_component || generic_depth != 0 {
        return Err(format!("unsupported token sequence: {tokens:?}"));
    }
    Ok(path)
}

fn exported_path_for_item<'a>(
    tokens: impl Iterator<Item = &'a Token>,
    is_impl: bool,
) -> Result<Vec<String>, String> {
    let tokens = tokens.collect::<Vec<_>>();
    // Compound receiver impls (for example `impl &'a crate::Type`) are
    // public-api occurrences without a stable exported item path. They were
    // intentionally retained as signature occurrences but must not be
    // mistaken for the ordinary `crate::Type` path when the reference
    // qualifier support below is enabled for binding methods.
    if is_impl {
        let mut index = 1;
        if matches!(tokens.first(), Some(Token::Keyword(kind)) if kind == "impl") {
            while matches!(tokens.get(index), Some(Token::Whitespace)) {
                index += 1;
            }
            let target = tokens.get(index).copied();
            if matches!(target, Some(Token::Symbol(symbol)) if symbol == "&" || symbol == "(")
                || matches!(target, Some(Token::Keyword(kind)) if kind == "dyn")
            {
                return Ok(Vec::new());
            }
        }
    }
    match exported_path(tokens.into_iter()) {
        Ok(path) => Ok(path),
        Err(_) if is_impl => Ok(Vec::new()),
        Err(reason) => Err(reason),
    }
}

fn skip_impl_generics(tokens: &[&Token], start: usize) -> usize {
    let mut index = start;
    while matches!(tokens.get(index), Some(Token::Whitespace)) {
        index += 1;
    }
    if !matches!(tokens.get(index), Some(Token::Symbol(open)) if open == "<") {
        return index;
    }
    let mut depth = 0usize;
    for (offset, token) in tokens.iter().enumerate().skip(index) {
        match token {
            Token::Symbol(open) if open == "<" => depth += 1,
            Token::Symbol(close) if close == ">" => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return offset + 1;
                }
            }
            _ => {}
        }
    }
    start
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
    use super::{exported_path, exported_path_for_item, remove_known_fields};
    use proptest::prelude::*;
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
        assert!(value["index"]["7"]["inner"]["function"]
            .get("default_unstable")
            .is_none());
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

    /// One token of the rendered signature vocabulary public-api emits.
    fn token() -> impl Strategy<Value = Token> {
        let name = "[A-Za-z_][A-Za-z0-9_]{0,8}";
        prop_oneof![
            name.prop_map(Token::Identifier),
            name.prop_map(Token::Type),
            name.prop_map(Token::Function),
            name.prop_map(Token::Generic),
            name.prop_map(Token::Primitive),
            name.prop_map(Token::Qualifier),
            name.prop_map(Token::Kind),
            name.prop_map(Token::Keyword),
            name.prop_map(Token::Annotation),
            "'[a-z]{1,4}".prop_map(Token::Lifetime),
            Just(Token::Self_("Self".into())),
            Just(Token::Whitespace),
            prop::sample::select(
                [
                    "::",
                    "<",
                    ">",
                    "&",
                    "*",
                    "[",
                    "]",
                    ";",
                    "(",
                    ")",
                    "#[",
                    "#[derive(",
                    ")]"
                ]
                .map(|symbol| Token::Symbol(symbol.into()))
                .to_vec(),
            ),
        ]
    }

    /// Render an exported path as the token sequence public-api emits for it.
    fn path_tokens(path: &[String]) -> Vec<Token> {
        let mut tokens = Vec::new();
        for (index, component) in path.iter().enumerate() {
            if index != 0 {
                tokens.push(Token::Symbol("::".into()));
            }
            tokens.push(Token::Identifier(component.clone()));
        }
        tokens
    }

    fn token_sequences() -> impl Strategy<Value = Vec<Token>> {
        prop_oneof![
            prop::collection::vec(token(), 0..24),
            prop::collection::vec("[A-Za-z0-9_ .:<>\\[\\]&*;()#]{1,8}", 1..5)
                .prop_map(|path| path_tokens(&path)),
        ]
    }

    proptest! {
        // Every accepted sequence is a fixed point of its own rendering, every
        // rejection names the tokens it could not place, and an impl occurrence
        // is retained even when it has no exported path.
        #[test]
        fn exported_paths_are_fixed_points_and_rejections_name_their_tokens(
            tokens in token_sequences(),
        ) {
            match exported_path(tokens.iter()) {
                Ok(path) => {
                    prop_assert!(!path.is_empty());
                    let rendered = path_tokens(&path);
                    prop_assert_eq!(
                        exported_path(rendered.iter()).expect("rendered path must parse"),
                        path
                    );
                }
                Err(reason) => prop_assert!(
                    reason.contains(&format!("{tokens:?}")),
                    "diagnostic must name the offending tokens: {reason}"
                ),
            }
            prop_assert!(exported_path_for_item(tokens.iter(), true).is_ok());
        }
    }

    #[test]
    fn adapter_rejects_populated_typed_metadata() {
        let mut value = base();
        value["index"]["7"]["inner"]["function"]["default_unstable"] = json!({"feature": "x"});
        let error = remove_known_fields(&mut value, [(7, "function")]).unwrap_err();
        assert!(format!("{error:?}").contains("index.7.inner.function.default_unstable"));
    }
}
