//! The benchmark generator must satisfy GraphCoder's shipping catalogue parser.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_harness_providers::{Credentials, Registry};
use std::{collections::BTreeMap, process::Command};

#[test]
fn generated_benchmark_catalog_loads_and_old_incomplete_features_fail()
-> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let output = Command::new("python3")
        .args([
            "-c",
            "import json; from scripts.benchmarks.contracts import catalog; \
             print(json.dumps(catalog('public/fixture','http://127.0.0.1:9/v1',100,10)))",
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
    let model = &registry.catalog()["benchmark"].models["fixture"];
    assert!(model.features.tools);
    assert!(!model.features.images);
    assert_eq!(model.capacity.context_tokens, 100);
    assert_eq!(model.capacity.output_tokens, Some(10));

    // Preserve the exact old generator shape as a production-parser negative.
    let mut incomplete: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    incomplete["benchmark"]["models"]["fixture"]["features"]
        .as_object_mut()
        .expect("generated feature object")
        .remove("images");
    std::fs::write(file.path(), serde_json::to_vec(&incomplete)?)?;
    let error = registry.load_catalog(file.path()).err().expect("old shape must fail");
    assert!(error.to_string().contains("images"), "{error}");
    Ok(())
}
