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
        } else if tool.get("googleSearch").is_some()
            || tool["type"] == "web_search"
            || tool["type"] == "web_search_20250305"
        {
            names.push("web_search".into());
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
        search_at in prop::option::of(0usize..17),
    ) {
        let schemas: Vec<_> = texts.iter().map(|text| json!({"const":text})).collect();
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
                if let Some(index) = search_at {
                    declarations.insert(index.min(declarations.len()), ToolDeclaration::WebSearch);
                }
                ToolDeclarations::new(family, declarations)
            };
            if family == Family::Chat && search_at.is_some() {
                prop_assert!(build(false).is_err());
                prop_assert_eq!(visits.get(), 0);
                continue;
            }
            let declarations = build(false).unwrap();
            let wire = serde_json::to_value(&declarations).unwrap();
            prop_assert_eq!(visits.get(), schemas.len());
            let mut expected = names.clone();
            if let Some(index) = search_at {
                expected.insert(index.min(expected.len()), "web_search".into());
            }
            let actual = order(family, &wire);
            prop_assert_eq!(&actual, &expected);
            if expected.len() > 1 {
                expected.reverse();
                prop_assert_ne!(&actual, &expected, "oracle must distinguish reordered tools");
            }
            let actual_schemas: Vec<_> = wire.as_array().unwrap().iter().flat_map(|tool| {
                if family == Family::Gemini {
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
}

#[test]
fn duplicate_server_declarations_refuse_without_schema_visits() {
    let visits = Cell::new(0);
    let value = json!({});
    for family in [
        Family::Responses,
        Family::Chat,
        Family::Anthropic,
        Family::Gemini,
    ] {
        let declarations = vec![
            ToolDeclaration::Function {
                name: "original".into(),
                description: "original",
                parameters: Schema {
                    value: &value,
                    visited: &visits,
                    refuse: false,
                },
            },
            ToolDeclaration::WebSearch,
            ToolDeclaration::WebSearch,
        ];
        assert!(ToolDeclarations::new(family, declarations).is_err());
        assert_eq!(visits.get(), 0);
    }
    for search_first in [false, true] {
        let function = || ToolDeclaration::Function {
            name: "web_search".into(),
            description: "original",
            parameters: Schema {
                value: &value,
                visited: &visits,
                refuse: false,
            },
        };
        let mut declarations = vec![function(), ToolDeclaration::WebSearch];
        if search_first {
            declarations.reverse();
        }
        assert!(ToolDeclarations::new(Family::Anthropic, declarations).is_err());
        assert_eq!(visits.get(), 0);
        let ordinary = ToolDeclarations::new(Family::Anthropic, vec![function()]).unwrap();
        assert!(serde_json::to_value(ordinary).is_ok());
        assert_eq!(visits.replace(0), 1);
    }
}
