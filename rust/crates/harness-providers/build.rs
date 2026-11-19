//! Register model-checking configuration and validate the embedded counter catalog.
use serde::Deserialize;
use std::{collections::BTreeMap, fmt::Write as _, path::Path};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
enum Adapter {
    Anthropic,
    Chat,
    Gemini,
    Responses,
}

#[derive(Debug, Deserialize)]
enum ResponseField {
    #[serde(rename = "input_tokens")]
    InputTokens,
    #[serde(rename = "totalTokens")]
    TotalTokens,
    #[serde(rename = "count")]
    Count,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Counter {
    path: String,
    adapter: Adapter,
    response_field: ResponseField,
    framing_tokens: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Without a rerun-if line Cargo reruns this script on any package change.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(loom)");
    // ACYCLIC_LOOM=1 (CONTRIBUTING.md) opts Loom models in; the feature alone never does.
    println!("cargo:rerun-if-env-changed=ACYCLIC_LOOM");
    if std::env::var_os("ACYCLIC_LOOM").is_some()
        && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some()
    {
        println!("cargo:rustc-cfg=loom");
    }
    println!("cargo:rerun-if-changed=src/token_counters.json");
    let counters: BTreeMap<Adapter, Option<Counter>> =
        serde_json::from_str(include_str!("src/token_counters.json"))?;
    let mut source = String::from(
        "fn token_counter(adapter: Adapter) -> Option<TokenCounterSpec> { match adapter {\n",
    );
    for adapter in [
        Adapter::Anthropic,
        Adapter::Chat,
        Adapter::Gemini,
        Adapter::Responses,
    ] {
        if let Some(Some(counter)) = counters.get(&adapter) {
            if counter.adapter != adapter {
                return Err(
                    format!("counter dialect differs from its catalog key {adapter:?}").into(),
                );
            }
            writeln!(
                source,
                "Adapter::{adapter:?} => Some(TokenCounterSpec {{ path: {:?}.into(), adapter, response_field: crate::counting::wire::TokenCountField::{:?}, framing_tokens: {} }}),",
                counter.path, counter.response_field, counter.framing_tokens,
            )?;
        } else {
            writeln!(source, "Adapter::{adapter:?} => None,")?;
        }
    }
    source.push_str("} }\n");
    let output = std::env::var_os("OUT_DIR").ok_or("missing build output directory")?;
    std::fs::write(Path::new(&output).join("token_counters.rs"), source)?;
    Ok(())
}
