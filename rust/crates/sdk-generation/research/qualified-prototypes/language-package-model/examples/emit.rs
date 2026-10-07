use language_package_model::{
    build_record_from_source, validate_language_metadata, GeneratorIdentity, Language,
    QualificationMetadata, QualificationStatus,
};
use std::{env, path::PathBuf};

#[derive(serde::Deserialize)]
struct ExecutedQualification {
    status: String,
    operations: Vec<String>,
    checks: Vec<String>,
}

fn value(args: &[String], name: &str) -> String {
    let flag = format!("--{name}");
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
        .unwrap_or_else(|| panic!("missing {flag}"))
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let manifest = PathBuf::from(value(&args, "manifest-path"));
    let package = value(&args, "package");
    let language = match value(&args, "language").as_str() {
        "python" => Language::Python,
        "kotlin" => Language::Jvm,
        "swift" => Language::Swift,
        "ruby" => Language::Ruby,
        "csharp" => Language::Dotnet,
        other => panic!("unsupported prototype language {other}"),
    };
    let generator_version = value(&args, "generator");
    let generator_source_sha256 = value(&args, "generator-source-sha256");
    let artifact = PathBuf::from(value(&args, "artifact"));
    let receipt = PathBuf::from(value(&args, "receipt"));
    let executed: ExecutedQualification =
        serde_json::from_slice(&std::fs::read(&receipt).unwrap()).unwrap();
    assert_eq!(
        executed.status, "PASS",
        "receipt must report execution success"
    );
    let receipt_marker = value(&args, "receipt-marker");
    let source_root = PathBuf::from(value(&args, "source-root"));
    if args.iter().any(|arg| arg == "--language-metadata") {
        let metadata_path = PathBuf::from(value(&args, "language-metadata"));
        let patch_sha256 = value(&args, "generator-patch-sha256");
        validate_language_metadata(&metadata_path, language, &patch_sha256).unwrap();
    }
    let record = build_record_from_source(
        &source_root,
        &manifest,
        &package,
        language,
        GeneratorIdentity {
            family: "uniffi".into(),
            version: generator_version,
            source: "mozilla/uniffi-rs".into(),
            source_sha256: generator_source_sha256,
        },
        &artifact,
        &receipt,
        &receipt_marker,
        QualificationMetadata {
            status: QualificationStatus::Passed,
            operations: executed.operations,
            checks: executed.checks,
            receipt: language_package_model::sha256_file(&receipt).unwrap(),
        },
    )
    .unwrap();
    println!("{}", record.evidence_json().unwrap());
}
