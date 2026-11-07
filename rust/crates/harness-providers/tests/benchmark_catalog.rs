//! The benchmark generator must satisfy the shipping `GraphCoder` catalogue parser.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_harness_providers::{Credentials, Registry};
use std::{collections::BTreeMap, process::Command};

#[test]
fn generated_benchmark_catalog_loads_and_old_incomplete_features_fail()
-> Result<(), Box<dyn std::error::Error>> {
    let root = acyclic_test_support::source_path("");
    let output = Command::new(if cfg!(windows) { "python" } else { "python3" })
        .args([
            "-c",
            "import json; from scripts.benchmarks.acyclic_graphcoder_harbor.contracts import catalog; \
             print(json.dumps(catalog('public/fixture','http://172.17.0.1:9/t/trial/openai/v1',100,10,allow_http_host='172.17.0.1')))",
        ])
        .current_dir(root)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()?;
    assert!(output.status.success(), "generator: {output:?}");
    let file = tempfile::NamedTempFile::new()?;
    std::fs::write(file.path(), &output.stdout)?;
    // This is the same production composition as GraphCoder::open, with an
    // explicit empty credential set and no request/model execution.
    let mut registry = Registry::new(Credentials::explicit(BTreeMap::new()))?;
    registry.load_catalog(file.path())?;
    registry.resolve("benchmark/fixture")?;
    let model = registry
        .catalog()
        .get("benchmark")
        .ok_or("missing benchmark provider")?
        .models
        .get("fixture")
        .ok_or("missing benchmark model")?;
    assert!(model.features.tools);
    assert!(!model.features.images);
    assert_eq!(model.capacity.context_tokens, 100);
    assert_eq!(model.capacity.output_tokens, Some(10));

    let generated: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    for (host, base, count_path) in [
        (None, "http://172.17.0.1:9/v1", "responses/input_tokens"),
        (
            Some("172.17.0.2"),
            "http://172.17.0.1:9/v1",
            "responses/input_tokens",
        ),
        (
            Some("*.example.test"),
            "http://one.example.test/v1",
            "responses/input_tokens",
        ),
        (
            Some("172.17.0.1"),
            "http://172.17.0.2:9/v1",
            "responses/input_tokens",
        ),
        (
            Some("172.17.0.1"),
            "http://user@172.17.0.1:9/v1",
            "responses/input_tokens",
        ),
        (
            Some("172.17.0.1"),
            "http://172.17.0.1:9/v1?token=value",
            "responses/input_tokens",
        ),
        (
            Some("172.17.0.1"),
            "http://172.17.0.1:9/v1",
            "http://172.17.0.2:9/count",
        ),
    ] {
        let mut invalid = generated.clone();
        invalid["benchmark"]["allow_http_host"] = host.into();
        invalid["benchmark"]["base_url"] = base.into();
        invalid["benchmark"]["token_counter"]["path"] = count_path.into();
        std::fs::write(file.path(), serde_json::to_vec(&invalid)?)?;
        let mut candidate = Registry::new(Credentials::explicit(BTreeMap::new()))?;
        assert!(
            candidate.load_catalog(file.path()).is_err(),
            "accepted {host:?} {base} {count_path}"
        );
    }
    for base in [
        "https://provider.example/v1",
        "http://localhost:9/v1",
        "http://127.0.0.1:9/v1",
        "http://[::1]:9/v1",
    ] {
        let mut default = generated.clone();
        default["benchmark"]
            .as_object_mut()
            .ok_or("missing provider")?
            .remove("allow_http_host");
        default["benchmark"]["base_url"] = base.into();
        std::fs::write(file.path(), serde_json::to_vec(&default)?)?;
        let mut candidate = Registry::new(Credentials::explicit(BTreeMap::new()))?;
        candidate.load_catalog(file.path())?;
        candidate.resolve("benchmark/fixture")?;
    }

    // Preserve the exact old generator shape as a production-parser negative.
    let mut incomplete: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    incomplete
        .pointer_mut("/benchmark/models/fixture/features")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("missing generated feature object")?
        .remove("images");
    std::fs::write(file.path(), serde_json::to_vec(&incomplete)?)?;
    let Err(error) = registry.load_catalog(file.path()) else {
        return Err("old shape must fail".into());
    };
    assert!(error.to_string().contains("images"), "{error}");
    Ok(())
}
