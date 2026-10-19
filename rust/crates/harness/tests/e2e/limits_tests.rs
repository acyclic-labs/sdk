use super::{TestResult, drivers, scenario};
use serde_json::json;
use std::path::Path;

#[tokio::test]
async fn limit_oracles_reject_changed_allowances_counts_and_deltas() -> TestResult {
    let root = Path::new(acyclic_harness::build_metadata::MANIFEST_DIR).join("tests/scenarios");
    for (file, pointer, replacement) in [
        ("input-utf8-at", "/limits/render_bytes", json!(3)),
        ("output-utf8-at", "/limits/file_bytes", json!(3)),
        ("accumulated-at", "/turns/0/0/delta", json!("")),
        ("context-1-at", "/limits/context_messages", json!(1)),
        ("invalid/zero-file-bytes", "/limits/file_bytes", json!(1)),
        ("input-utf8-over", "/expect/request_count", json!(1)),
    ] {
        let path = root.join(format!("limits/bounds/{file}.json"));
        let mut value = scenario::load_value(&root, &path)?;
        drivers::run(value.clone()).await?;
        *value
            .pointer_mut(pointer)
            .ok_or("missing mutation target")? = replacement;
        assert!(
            drivers::run(value).await.is_err(),
            "accepted changed {file}"
        );
    }
    Ok(())
}
