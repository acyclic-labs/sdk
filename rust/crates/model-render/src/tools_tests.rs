use crate::{
    Family, Fields, Header, Provenance, RENDERER_VERSION, SummaryHeader, Tail, ToolDeclaration,
    ToolDeclarations, WireItem, render, render_summary,
};
use proptest::prelude::*;
use serde::Serialize;
use serde_json::{Value, json};
use std::cell::Cell;

// Generated declarations and schemas, not captured external provider output.
// The counter observes actual leaf serialization, including a refusing leaf.
struct Schema<'a> {
    value: &'a Value,
    visited: &'a Cell<usize>,
    refuse: Option<usize>,
}
impl Serialize for Schema<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.visited.set(self.visited.get() + 1);
        if self.refuse.is_some_and(|visit| self.visited.get() >= visit) {
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
                prop_assert!(build(None).is_err());
                prop_assert_eq!(visits.get(), 0);
                continue;
            }
            let declarations = build(None).unwrap();
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
            // The same admitted declarations fund both headers. Leaf visits
            // occur at admission only, independent of the number of renders.
            let mut ordinary_prefix: Option<Vec<u8>> = None;
            for choice in [SummaryHeader::Keep, SummaryHeader::StripExecutableBuiltins] {
                visits.set(0);
                let mut fields = Fields::default();
                fields.set_tools(family, &build(None).unwrap().with_summary_header(choice)).unwrap();
                let stripped = choice == SummaryHeader::StripExecutableBuiltins && search_at.is_some();
                prop_assert_eq!(visits.get(), schemas.len() * if stripped { 2 } else { 1 });
                let header = Header::new(Provenance {
                    family, provider:"provider".into(), model:"model".into(),
                    endpoint:"https://example.invalid".into(), credential_scope:None,
                }, RENDERER_VERSION, &fields, None).unwrap();
                if let Some(prefix) = &ordinary_prefix {
                    prop_assert_eq!(header.bytes(), prefix.as_slice(), "summary choice changed frozen ordinary bytes");
                } else {
                    ordinary_prefix = Some(header.bytes().to_vec());
                }
                for _ in 0..2 {
                    let mut ordinary = Vec::new();
                    render(&header, std::iter::empty::<&WireItem>(), &Tail::new(&fields).unwrap(), &mut ordinary).unwrap();
                    let ordinary: Value = serde_json::from_slice(&ordinary).unwrap();
                    prop_assert_eq!(&ordinary["tools"], &wire, "summary choice changed ordinary declarations");
                    let mut summary = Vec::new();
                    let result = render_summary(&header, std::iter::empty::<&WireItem>(), "summarize", &Tail::summary(&fields, family).unwrap(), &mut summary);
                    if family == Family::Gemini && search_at.is_some() && !stripped {
                        prop_assert!(result.is_err());
                        prop_assert!(summary.is_empty());
                    } else {
                        result.unwrap();
                        let summary: Value = serde_json::from_slice(&summary).unwrap();
                        let mut expected = wire.clone();
                        if stripped {
                            expected.as_array_mut().unwrap().retain(|tool| {
                                tool.get("googleSearch").is_none()
                                    && tool["type"] != "web_search"
                                    && tool["type"] != "web_search_20250305"
                            });
                        }
                        prop_assert_eq!(&summary["tools"], &expected, "summary retained executable builtins or changed client declarations");
                        let input = match family { Family::Gemini => "contents", Family::Responses => "input", _ => "messages" };
                        prop_assert_eq!(summary[input].as_array().unwrap().len(), 1);
                    }
                }
                prop_assert_eq!(visits.get(), schemas.len() * if stripped { 2 } else { 1 }, "rendering repeated schema work");
                if stripped && !schemas.is_empty() {
                    let admitted = fields.clone();
                    visits.set(0);
                    let refusing = build(Some(schemas.len() + 1)).unwrap().with_summary_header(choice);
                    prop_assert!(fields.set_tools(family, &refusing).is_err());
                    prop_assert_eq!(visits.get(), schemas.len() + 1, "summary admission must stop at the first refusing leaf");
                    prop_assert_eq!(&fields, &admitted, "summary refusal partially replaced the admitted fields");
                }
                // Dropping and replacing tools must also drop the alternate header.
                fields.retain(|field| field != "tools");
                prop_assert_eq!(&fields, &Fields::default());
                fields.set_tools(family, &build(None).unwrap()).unwrap();
                let mut kept = Fields::default();
                kept.set_tools(family, &build(None).unwrap()).unwrap();
                prop_assert_eq!(fields, kept);
            }
            visits.set(0);
            let refusing = build(Some(1)).unwrap();
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
                    format:Schema {value:&format, visited:&visits, refuse:refuse.then_some(1)},
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
                    let tools = ToolDeclarations::new(family, vec![
                        ToolDeclaration::WebSearch,
                        ToolDeclaration::Freeform {
                            name:"apply_patch".into(), description:"native guidance",
                            format:Schema {value:&format, visited:&visits, refuse:None},
                        },
                    ]).unwrap().with_summary_header(SummaryHeader::StripExecutableBuiltins);
                    let mut fields = Fields::default();
                    fields.set_tools(family, &tools).unwrap();
                    let header = Header::new(Provenance {
                        family, provider:"provider".into(), model:"model".into(),
                        endpoint:"https://example.invalid".into(), credential_scope:None,
                    }, RENDERER_VERSION, &fields, None).unwrap();
                    let mut summary = Vec::new();
                    render_summary(&header, std::iter::empty::<&WireItem>(), "summarize", &Tail::summary(&fields, family).unwrap(), &mut summary).unwrap();
                    let summary: Value = serde_json::from_slice(&summary).unwrap();
                    prop_assert_eq!(&summary["tools"], &wire, "summary changed the freeform executor declaration");
                    prop_assert_eq!(visits.replace(0), 2, "both admitted headers visit the borrowed format exactly once");
                }
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
                    refuse: None,
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
                refuse: None,
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
