use serde_json::Value;

pub const TARGET_PREFIX: &str = "@fixture-target@/";

pub enum Kind {
    Source,
    Dependency,
}

pub fn visit(doc: &mut Value, mut visit: impl FnMut(Kind, &mut String)) {
    if let Some(index) = doc.get_mut("index").and_then(Value::as_object_mut) {
        for item in index.values_mut() {
            if let Some(Value::String(filename)) = item
                .get_mut("span")
                .and_then(|span| span.get_mut("filename"))
            {
                visit(Kind::Source, filename);
            }
        }
    }
    if let Some(crates) = doc
        .get_mut("external_crates")
        .and_then(Value::as_object_mut)
    {
        for record in crates.values_mut() {
            if let Some(Value::String(path)) = record.get_mut("path") {
                visit(Kind::Dependency, path);
            }
        }
    }
}
