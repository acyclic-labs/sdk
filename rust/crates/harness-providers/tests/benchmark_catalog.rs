//! Benchmark output must satisfy GraphCoder's production catalog parser offline.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_harness_providers::{Adapter, Credentials, Registry, default_catalog};
use std::{collections::BTreeMap, process::Command};

#[test]
fn generated_benchmark_catalog_adapters_and_negative_controls()
-> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(if cfg!(windows) { "python" } else { "python3" })
        .args([
            "-c",
            r#"
import json
from scripts.benchmarks.contracts import catalog
cases = [
    ('anthropic/claude-haiku-5-5', '/anthropic'),
    ('gemini/gemini-3.8-flash', '/google'),
    ('deepseek/deepseek-flash', '/deepseek/v1'),
    ('fireworks/qwen3p8-max', '/fireworks/v1'),
    ('openai/gpt-6-luna', '/v1'),
]
results = []
for model, path in cases:
    results.append(catalog(model, 'http://127.0.0.1:9' + path, 100, 10))
# Explicit selection overrides both provider and proxy-route inference.
for adapter in ('anthropic', 'gemini', 'chat', 'responses'):
    results.append(catalog('openai/fixture', 'http://127.0.0.1:9/anthropic',
                           100, 10, adapter=adapter))
# Proxy path also works when the provider alias is not known.
for model, path in cases[:4]:
    results.append(catalog('public/' + model.split('/', 1)[1],
                           'http://127.0.0.1:9' + path, 100, 10))
# Provider names infer the family even without a proxy route.
for model, _ in cases[:4]:
    results.append(catalog(model, 'http://127.0.0.1:9/v1', 100, 10))
try:
    catalog('public/fixture', 'http://127.0.0.1:9/v1', 100, 10, adapter='invalid')
except ValueError:
    pass
else:
    raise AssertionError('unknown adapter accepted')
print(json.dumps(results))
"#,
        ])
        .current_dir(acyclic_test_support::source_path(""))
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()?;
    assert!(output.status.success(), "generator: {output:?}");
    let documents: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout)?;
    let expected = [
        (Adapter::Anthropic, "anthropic"),
        (Adapter::Gemini, "gemini"),
        (Adapter::Chat, "openrouter"),
        (Adapter::Chat, "openrouter"),
        (Adapter::Responses, "openai"),
        (Adapter::Anthropic, "anthropic"),
        (Adapter::Gemini, "gemini"),
        (Adapter::Chat, "openrouter"),
        (Adapter::Responses, "openai"),
        (Adapter::Anthropic, "anthropic"),
        (Adapter::Gemini, "gemini"),
        (Adapter::Chat, "openrouter"),
        (Adapter::Chat, "openrouter"),
        (Adapter::Anthropic, "anthropic"),
        (Adapter::Gemini, "gemini"),
        (Adapter::Chat, "openrouter"),
        (Adapter::Chat, "openrouter"),
    ];
    assert_eq!(documents.len(), expected.len());
    let stock = default_catalog();
    let file = tempfile::NamedTempFile::new()?;
    for (document, (adapter, provider)) in documents.iter().zip(expected) {
        std::fs::write(file.path(), serde_json::to_vec(document)?)?;
        let mut registry = Registry::new(Credentials::explicit(BTreeMap::new()))?;
        registry.load_catalog(file.path())?;
        let spec = registry
            .catalog()
            .get("benchmark")
            .ok_or("missing provider")?;
        assert_eq!(spec.adapter, adapter);
        assert_eq!(spec.token_counter, stock[provider].token_counter);
        let (name, model) = spec.models.iter().next().ok_or("missing model")?;
        assert!(model.features.tools);
        assert!(!model.features.images);
        assert_eq!(model.capacity.context_tokens, 100);
        assert_eq!(model.capacity.output_tokens, Some(10));
        let name = name.clone();
        registry.resolve(&format!("benchmark/{name}"))?;

        // These corruptions must fail the actual parser/registration path.
        for (field, value) in [
            ("adapter", serde_json::json!("invalid")),
            ("token_counter", serde_json::json!({"path": "count"})),
        ] {
            let mut broken = document.clone();
            broken["benchmark"][field] = value;
            std::fs::write(file.path(), serde_json::to_vec(&broken)?)?;
            assert!(
                registry.load_catalog(file.path()).is_err(),
                "accepted {field}"
            );
        }
        let mut incomplete = document.clone();
        incomplete["benchmark"]["models"][&name]["features"]
            .as_object_mut()
            .ok_or("missing features")?
            .remove("images");
        std::fs::write(file.path(), serde_json::to_vec(&incomplete)?)?;
        assert!(
            registry.load_catalog(file.path()).is_err(),
            "accepted incomplete features"
        );
    }
    Ok(())
}
