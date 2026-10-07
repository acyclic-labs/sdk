use std::{fs, path::Path};

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let sources = sdk_docs::scenarios::validate(&root).expect("scenario source closure");
    let executions = sdk_docs::scenarios::execute_local(&root, &sources, None).expect("local scenarios");
    let snippets = sdk_docs::scenarios::render_typescript(&executions).expect("render snippets");
    let output = root.join("research/docs-scenarios-c8-20261007/generated");
    fs::create_dir_all(&output).expect("create output");
    for snippet in snippets {
        let path = output.join(snippet.path);
        if let Some(parent) = path.parent() { fs::create_dir_all(parent).expect("create parent"); }
        fs::write(path, snippet.source).expect("write snippet");
    }
    println!("executed={} rendered={}", executions.len(),
        fs::read_dir(&output).map(|_| "all").unwrap_or("none"));
}
