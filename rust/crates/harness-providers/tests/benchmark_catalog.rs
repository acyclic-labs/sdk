//! Benchmark output must satisfy GraphCoder's production catalog parser offline.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_harness_providers::{Adapter, Credentials, Registry, default_catalog};
use std::{collections::BTreeMap, process::Command};

#[test]
fn mixed_benchmark_providers_resolve_independently_and_reject_invalid_worker()
-> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(if cfg!(windows) { "python" } else { "python3" })
        .args([
            "-c",
            r#"
import json
from scripts.benchmarks.contracts import catalog
workers = [
    dict(model_name=provider + '/shared', base_url='http://127.0.0.1:9' + path,
         context_tokens=200, output_tokens=20, adapter=adapter)
    for provider, path, adapter in [
        ('deepseek', '/deepseek/v1', 'chat'),
        ('fireworks', '/fireworks/v1', 'chat'),
        ('anthropic', '/anthropic', 'anthropic'),
        ('google', '/google', 'gemini'),
    ]
]
workers.append(dict(workers[0], model_name='deepseek/reviewer',
                    context_tokens=300, output_tokens=30))
print(json.dumps(catalog('openai/shared', 'http://127.0.0.1:9/v1', 100, 10,
                         adapter='responses', providers=workers)))
"#,
        ])
        .current_dir(acyclic_test_support::source_path(""))
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()?;
    assert!(output.status.success(), "generator: {output:?}");
    let document: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(document.as_object().ok_or("missing providers")?.len(), 5);
    let file = tempfile::NamedTempFile::new()?;
    std::fs::write(file.path(), serde_json::to_vec(&document)?)?;
    let mut registry = Registry::new(Credentials::explicit(BTreeMap::new()))?;
    registry.load_catalog(file.path())?;
    let stock = default_catalog();
    for (provider, path, adapter, counter_provider) in [
        ("openai", "/v1", Adapter::Responses, "openai"),
        ("deepseek", "/deepseek/v1", Adapter::Chat, "openrouter"),
        ("fireworks", "/fireworks/v1", Adapter::Chat, "openrouter"),
        ("anthropic", "/anthropic", Adapter::Anthropic, "anthropic"),
        ("google", "/google", Adapter::Gemini, "gemini"),
    ] {
        let spec = &registry.catalog()[provider];
        assert_eq!(spec.adapter, adapter);
        assert_eq!(spec.base_url, format!("http://127.0.0.1:9{path}"));
        assert_eq!(spec.token_counter, stock[counter_provider].token_counter);
        assert_eq!(
            spec.models["shared"].capacity.context_tokens,
            if provider == "openai" { 100 } else { 200 }
        );
        registry.resolve(&format!("{provider}/shared"))?;
    }
    assert_eq!(registry.catalog()["deepseek"].models.len(), 2);
    assert_eq!(
        registry.catalog()["deepseek"].models["reviewer"]
            .capacity
            .output_tokens,
        Some(30)
    );
    registry.resolve("deepseek/reviewer")?;

    // A valid root cannot hide an invalid worker in a multi-provider document.
    let mut broken = document;
    broken["deepseek"]["adapter"] = serde_json::json!("invalid");
    std::fs::write(file.path(), serde_json::to_vec(&broken)?)?;
    assert!(registry.load_catalog(file.path()).is_err());
    Ok(())
}

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
        let id = document
            .as_object()
            .and_then(|providers| providers.keys().next())
            .ok_or("missing provider identity")?;
        std::fs::write(file.path(), serde_json::to_vec(document)?)?;
        let mut registry = Registry::new(Credentials::explicit(BTreeMap::new()))?;
        registry.load_catalog(file.path())?;
        let spec = registry.catalog().get(id).ok_or("missing provider")?;
        assert_eq!(spec.adapter, adapter);
        assert_eq!(spec.token_counter, stock[provider].token_counter);
        let (name, model) = spec.models.iter().next().ok_or("missing model")?;
        assert!(model.features.tools);
        assert!(!model.features.images);
        assert_eq!(model.capacity.context_tokens, 100);
        assert_eq!(model.capacity.output_tokens, Some(10));
        let name = name.clone();
        registry.resolve(&format!("{id}/{name}"))?;

        // These corruptions must fail the actual parser/registration path.
        for (field, value) in [
            ("adapter", serde_json::json!("invalid")),
            ("token_counter", serde_json::json!({"path": "count"})),
        ] {
            let mut broken = document.clone();
            broken[id][field] = value;
            std::fs::write(file.path(), serde_json::to_vec(&broken)?)?;
            assert!(
                registry.load_catalog(file.path()).is_err(),
                "accepted {field}"
            );
        }
        let mut incomplete = document.clone();
        incomplete[id]["models"][&name]["features"]
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
