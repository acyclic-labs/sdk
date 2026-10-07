use sdk_docs::scenarios;
use sha2::{Digest, Sha256};
use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let output = root.join("research/docs-scenarios-c8-20261007/generated");
    std::fs::create_dir_all(&output).unwrap();
    let sources = scenarios::validate(&root).unwrap();
    scenarios::compile_all(&root, &sources, None).unwrap();
    let executions = scenarios::execute_all(&root, &sources, None, None).unwrap();
    let snippets = scenarios::render_typescript(&executions).unwrap();
    write_json(
        &output.join("scenario-catalog.json"),
        &scenarios::catalog(&sources),
    );
    write_json(
        &output.join("execution-catalog.json"),
        &scenarios::execution_catalog(&executions),
    );
    let projections =
        scenarios::projection_catalog(&snippets, |snippet| digest(snippet.source.as_bytes()));
    write_json(&output.join("projection-catalog.json"), &projections);
    for snippet in snippets {
        let path = output.join(&snippet.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, snippet.source).unwrap();
    }
}

fn write_json(path: &std::path::Path, value: &impl serde::Serialize) {
    std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}
