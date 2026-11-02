use crate::{Family, ToolDeclaration, ToolDeclarations};
use proptest::prelude::*;
use serde::Serialize;
use serde_json::{Value, json};
use std::cell::Cell;

// Generated declarations and schemas, not captured external provider output.
// The counter observes actual leaf serialization, including a refusing leaf.
struct Schema<'a> {
    value: &'a Value,
    visited: &'a Cell<usize>,
    refuse: bool,
}
impl Serialize for Schema<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.visited.set(self.visited.get() + 1);
        if self.refuse {
            return Err(serde::ser::Error::custom("schema boundary refused"));
        }
        self.value.serialize(serializer)
    }
}

// Inspect the public wire grammar independently of the declaration serializer.
fn order(family: Family, wire: &Value) -> Vec<String> {
    let mut names = Vec::new();
    for tool in wire.as_array().unwrap() {
        if let Some(functions) = tool.get("functionDeclarations") {
            names.extend(
                functions
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|tool| tool["name"].as_str().unwrap().to_owned()),
            );
        } else if tool.get("fixture_server").is_some() {
            names.push("provider_tool".into());
        } else {
            names.push(
                match family {
                    Family::Chat => &tool["function"]["name"],
                    _ => &tool["name"],
                }
                .as_str()
                .unwrap()
                .to_owned(),
            );
        }
    }
    names
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn declarations_preserve_order_and_visit_each_borrowed_schema_once(
        texts in prop::collection::vec(".{0,32}", 0..16),
        provider_at in prop::option::of(0usize..17),
    ) {
        let schemas: Vec<_> = texts.iter().map(|text| json!({"const":text})).collect();
        let provider = json!({"fixture_server": {"unknown": texts, "name": "web_search"}});
        let provider = provider.as_object().unwrap();
        let names: Vec<_> = (0..texts.len()).map(|index| format!("tool_{index}")).collect();
        for family in [Family::Responses, Family::Chat, Family::Anthropic, Family::Gemini] {
            let visits = Cell::new(0);
            let build = |refuse| {
                let mut declarations: Vec<_> = schemas.iter().zip(&names).map(|(value, name)| {
                    ToolDeclaration::Function {
                        name: name.as_str().into(), description:"original guidance",
                        parameters:Schema { value, visited:&visits, refuse },
                    }
                }).collect();
                if let Some(index) = provider_at {
                    declarations.insert(index.min(declarations.len()), ToolDeclaration::Provider { definition: provider, server_tool: true });
                }
                ToolDeclarations::new(family, declarations)
            };
            let declarations = build(false).unwrap();
            let wire = serde_json::to_value(&declarations).unwrap();
            prop_assert_eq!(visits.get(), schemas.len());
            let mut expected = names.clone();
            if let Some(index) = provider_at {
                expected.insert(index.min(expected.len()), "provider_tool".into());
            }
            let actual = order(family, &wire);
            prop_assert_eq!(&actual, &expected);
            if expected.len() > 1 {
                expected.reverse();
                prop_assert_ne!(&actual, &expected, "oracle must distinguish reordered tools");
            }
            let actual_schemas: Vec<_> = wire.as_array().unwrap().iter().flat_map(|tool| {
                if tool.get("fixture_server").is_some() {
                    Vec::new()
                } else if family == Family::Gemini {
                    tool["functionDeclarations"].as_array().into_iter().flatten()
                        .map(|function| &function["parametersJsonSchema"]).collect::<Vec<_>>()
                } else {
                    let schema = match family {
                        Family::Responses => tool.get("parameters"),
                        Family::Anthropic => tool.get("input_schema"),
                        Family::Chat => tool["function"].get("parameters"),
                        Family::Gemini => unreachable!(),
                    };
                    schema.into_iter().collect()
                }
            }).collect();
            prop_assert_eq!(actual_schemas, schemas.iter().collect::<Vec<_>>());
            visits.set(0);
            let refusing = build(true).unwrap();
            if schemas.is_empty() {
                prop_assert!(serde_json::to_value(&refusing).is_ok());
            } else {
                prop_assert!(serde_json::to_value(&refusing).is_err());
                prop_assert_eq!(visits.get(), 1, "first refusal must end serialization");
            }
        }
    }

    #[test]
    fn freeform_grammar_uses_the_original_borrowed_format_once(grammar in ".{0,128}") {
        let format = json!({"type":"grammar","syntax":"lark","definition":grammar});
        let visits = Cell::new(0);
        for family in [Family::Responses, Family::Chat, Family::Anthropic, Family::Gemini] {
            for refuse in [false, true] {
                let declaration = ToolDeclaration::Freeform {
                    name:"apply_patch".into(), description:"native guidance",
                    format:Schema {value:&format, visited:&visits, refuse},
                };
                let result = ToolDeclarations::new(family, vec![declaration]);
                if family != Family::Responses {
                    prop_assert!(result.is_err());
                    prop_assert_eq!(visits.replace(0), 0, "unsupported families refuse before touching the format");
                    continue;
                }
                let wire = serde_json::to_value(result.unwrap());
                prop_assert_eq!(visits.replace(0), 1);
                if refuse {
                    prop_assert!(wire.is_err());
                } else {
                    let wire = wire.unwrap();
                    prop_assert_eq!(&wire[0]["format"], &format);
                    prop_assert_eq!(&wire[0]["type"], &json!("custom"));
                    prop_assert!(wire[0].get("parameters").is_none());
                }
            }
        }
    }
}

#[test]
fn provider_data_is_replaceable_without_name_or_duplicate_gates() {
    // Generated settings operands, not captured provider response fixtures.
    let native = json!({"name":"web_search", "future":{"version":17, "settings":["a","b"]}});
    let schema = json!({"type":"object"});
    let visits = Cell::new(0);
    for family in [
        Family::Responses,
        Family::Chat,
        Family::Anthropic,
        Family::Gemini,
    ] {
        let definition = native.as_object().unwrap();
        let declarations = ToolDeclarations::new(
            family,
            vec![
                ToolDeclaration::Provider {
                    definition,
                    server_tool: true,
                },
                ToolDeclaration::Function {
                    name: "web_search".into(),
                    description: "original",
                    parameters: Schema {
                        value: &schema,
                        visited: &visits,
                        refuse: false,
                    },
                },
                ToolDeclaration::Provider {
                    definition,
                    server_tool: false,
                },
            ],
        )
        .unwrap();
        assert_eq!(visits.get(), 0, "admission never serializes the schema");
        let wire = serde_json::to_value(&declarations).unwrap();
        assert_eq!(visits.replace(0), 1);
        assert_eq!(wire[0], native);
        assert_eq!(wire[2], native);
        assert!(declarations.has_server_tools());
        let mut changed = definition.clone();
        changed.insert("future".into(), json!({"version":18}));
        let replacement = ToolDeclarations::<Value>::new(
            family,
            vec![ToolDeclaration::Provider {
                definition: &changed,
                server_tool: false,
            }],
        )
        .unwrap();
        let replacement = serde_json::to_value(&replacement).unwrap();
        assert_ne!(
            replacement[0], wire[0],
            "replacing settings changes the actual declaration"
        );
        assert_eq!(wire[0], native, "the admitted source is never edited");
    }
}
