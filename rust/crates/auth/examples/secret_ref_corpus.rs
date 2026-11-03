//! Capture reference metadata only, through the original JSON and protobuf writers.
use acyclic_auth::secrets::{SecretRef, SecretRefProto};
use prost::Message;
use std::error::Error;
mod corpus;

fn capture() -> Result<(), Box<dyn Error>> {
    let mut records = Vec::new();
    for (json_name, wire_name, scope, name, version) in [
        ("json-reference", "wire-reference", "scope", "token", 1),
        (
            "json-max-version",
            "wire-max-version",
            "scope/name",
            "token%name",
            u64::MAX,
        ),
        ("json-unicode", "wire-unicode", "組織", "鍵", 42),
    ] {
        let reference = SecretRef::new(scope.into(), name.into(), version)?;
        records.push((
            json_name,
            serde_json::to_vec(&reference)?,
            format!("SecretRef::new({scope:?},{name:?},{version}) -> original Serialize"),
        ));
        records.push((
            wire_name,
            SecretRefProto::from(reference).encode_to_vec(),
            format!(
                "SecretRefProto::from -> original prost::Message::encode_to_vec; version={version}"
            ),
        ));
    }
    corpus::write(
        "fuzz/corpus/secret_ref",
        "fuzz/corpus/secret-reference-sources.tsv",
        records,
    )
}

fn main() {
    if let Err(error) = capture() {
        eprintln!(
            "STEP capture original secret reference metadata failed: {error}; recovery from rust/crates/auth: cargo run --locked --example secret_ref_corpus"
        );
        std::process::exit(1);
    }
}
