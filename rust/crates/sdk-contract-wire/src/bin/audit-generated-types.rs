//! Executable acceptance check over generated public SDK type surfaces.
use acyclic_sdk_contract_wire::type_policy::{
    audit_generated_type_features, audit_required_generated_descriptor_shape_coverage,
    audit_required_generated_public_surfaces, resolved_enum_fields, resolved_oneof_members,
    resolved_presence_fields, REQUIRED_PRODUCT_SURFACES,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

const ARTIFACT_CLOSURE_SCHEMA: &str = "acyclic.sdk.generated-artifact-closure.v2";
const TARGET_LANGUAGE_REGISTRY: &[&str] = &[
    "rust",
    "typescript",
    "python",
    "go",
    "java",
    "csharp",
    "swift",
    "cpp",
    "ruby",
    "php",
    "dart",
    "kotlin",
    "scala",
    "elixir",
    "ballerina",
    "objective-c",
    "erlang",
    "ocaml",
    "common-lisp",
    "ada",
    "c",
    "clojure",
    "crystal",
    "elm",
    "gdscript",
    "julia",
    "nim",
    "perl",
    "powershell",
    "r",
    "bash",
    "haskell",
    "lua",
];

fn artifact_hashes(root: &Path) -> Result<BTreeMap<String, String>, String> {
    fn visit(
        root: &Path,
        directory: &Path,
        hashes: &mut BTreeMap<String, String>,
    ) -> Result<(), String> {
        for entry in fs::read_dir(directory)
            .map_err(|error| format!("read {}: {error}", directory.display()))?
        {
            let entry = entry.map_err(|error| error.to_string())?;
            let path = entry.path();
            let kind = entry.file_type().map_err(|error| error.to_string())?;
            if kind.is_symlink() {
                return Err(format!(
                    "artifact closure contains a symbolic link: {}",
                    path.display()
                ));
            }
            if kind.is_dir() {
                visit(root, &path, hashes)?;
            } else if kind.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|error| error.to_string())?
                    .to_string_lossy()
                    .replace('\\', "/");
                let bytes =
                    fs::read(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
                hashes.insert(relative, format!("{:x}", Sha256::digest(bytes)));
            }
        }
        Ok(())
    }
    let mut hashes = BTreeMap::new();
    visit(root, root, &mut hashes)?;
    Ok(hashes)
}

fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<(PathBuf, Vec<String>, Option<String>), String> {
    let mut root = None;
    let mut required = Vec::new();
    let mut source_revision = None;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--artifact-root" => {
                if root.is_some() {
                    return Err("--artifact-root must occur once".into());
                }
                root = Some(PathBuf::from(
                    args.next().ok_or("--artifact-root requires a directory")?,
                ));
            }
            "--required-language" => {
                let language = args
                    .next()
                    .ok_or("--required-language requires a language")?;
                if language.is_empty() || required.contains(&language) {
                    return Err("required languages must be nonempty and distinct".into());
                }
                required.push(language);
            }
            "--source-revision" => {
                if source_revision.is_some() {
                    return Err("--source-revision must occur once".into());
                }
                let revision = args
                    .next()
                    .ok_or("--source-revision requires a git revision")?;
                if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(
                        "--source-revision must be a 40-character hexadecimal git revision".into(),
                    );
                }
                source_revision = Some(revision);
            }
            _ => return Err(format!("unknown argument: {argument}")),
        }
    }
    if required.is_empty() {
        required = REQUIRED_PRODUCT_SURFACES
            .iter()
            .map(|value| (*value).to_owned())
            .collect();
    }
    Ok((
        root.ok_or("--artifact-root is required")?,
        required,
        source_revision,
    ))
}

#[cfg(test)]
fn audit(root: &Path, required: &[String]) -> Result<(Value, bool), String> {
    audit_with_source(root, required, None)
}

fn source_revision_from_metadata(value: &Value) -> Option<String> {
    value
        .get("source_revision")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("source")
                .and_then(|source| source.get("revision"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            value
                .get("source_identity")
                .and_then(|source| source.get("revision"))
                .and_then(Value::as_str)
        })
        .map(ToOwned::to_owned)
}

fn metadata_source_revisions(
    root: &Path,
    hashes: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let mut observations = Vec::new();
    for relative in hashes.keys() {
        let path = root.join(relative.replace('/', std::path::MAIN_SEPARATOR_STR));
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let is_authority_metadata = name.ends_with(".json")
            && (name.contains("receipt")
                || name.contains("manifest")
                || name.contains("authority")
                || name == "type-policy.json"
                || name == "docs.json");
        if !is_authority_metadata {
            continue;
        }
        let Ok(bytes) = fs::read(&path) else {
            continue;
        };
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
            continue;
        };
        if let Some(revision) = source_revision_from_metadata(&value) {
            observations.push((relative.clone(), revision));
        }
    }
    observations.sort();
    observations.dedup();
    observations
}

fn source_binding_report(
    root: &Path,
    hashes: &BTreeMap<String, String>,
    expected: Option<&str>,
) -> (Value, Option<String>) {
    let observations = metadata_source_revisions(root, hashes);
    let observed_revisions = observations
        .iter()
        .map(|(_, revision)| revision.as_str())
        .collect::<Vec<_>>();
    let status = match expected {
        None => "not_requested",
        Some(expected)
            if observations
                .iter()
                .any(|(_, revision)| revision == expected)
                && observations
                    .iter()
                    .all(|(_, revision)| revision == expected) =>
        {
            "passed"
        }
        Some(_) if observations.is_empty() => "missing",
        Some(_) => "mismatch",
    };
    let error = expected.and_then(|expected| {
        if status == "passed" {
            None
        } else if observations.is_empty() {
            Some(format!(
                "artifact closure has no source authority metadata; expected Rust revision {expected}"
            ))
        } else {
            Some(format!(
                "artifact closure source revision does not match expected Rust revision {expected}: observed {}",
                observed_revisions.join(", ")
            ))
        }
    });
    (
        json!({
            "schema": "acyclic.sdk.source-binding.v1",
            "status": status,
            "expected_revision": expected,
            "observations": observations.iter().map(|(path, revision)| json!({
                "path": path,
                "source_revision": revision,
            })).collect::<Vec<_>>(),
        }),
        error,
    )
}

fn surface_language(relative: &str) -> Option<&'static str> {
    let name = relative.rsplit('/').next()?;
    match name {
        name if name.ends_with("-metadata.ts") || name == "RustTypedClients.ts" => {
            Some("typescript")
        }
        "remote.py" => Some("python"),
        "client.go" => Some("go"),
        "RustTypedResponses.java"
        | "RustTypedResponses.kt"
        | "RustTypedResponses.scala"
        | "RustSemanticTypes.java"
        | "RustSemanticTypes.kt"
        | "RustSemanticTypes.scala" => Some("jvm"),
        "RustTypedClients.swift" => Some("swift"),
        "rust_typed_clients.hpp" => Some("cpp"),
        "RustTypedClients.cs" => Some("csharp"),
        "generated_typed.rb" => Some("ruby"),
        "RustTyped.php" => Some("php"),
        "generated_typed.dart" => Some("dart"),
        "RustTypedClients.hs"
        | "RustSemanticTypes.hs"
        | "generated_typed.hs"
        | "RustTypedClients.lhs"
        | "RustSemanticTypes.lhs" => Some("haskell"),
        _ => None,
    }
}

fn surface_registry(hashes: &BTreeMap<String, String>, source_revision: Option<&str>) -> Value {
    let mut by_language: BTreeMap<&str, Vec<(&str, &str)>> = BTreeMap::new();
    for (relative, hash) in hashes {
        if let Some(language) = surface_language(relative) {
            by_language
                .entry(language)
                .or_default()
                .push((relative, hash));
        }
    }
    json!(TARGET_LANGUAGE_REGISTRY
        .iter()
        .map(|language| json!({
            "language": language,
            "present": by_language.contains_key(language),
            "source_revision": source_revision,
            "files": by_language
                .get(language)
                .into_iter()
                .flatten()
                .map(|(path, sha256)| json!({"path": path, "sha256": sha256}))
                .collect::<Vec<_>>(),
        }))
        .collect::<Vec<_>>())
}

fn audit_with_source(
    root: &Path,
    required: &[String],
    expected_source_revision: Option<&str>,
) -> Result<(Value, bool), String> {
    let root = root
        .canonicalize()
        .map_err(|error| format!("resolve artifact root: {error}"))?;
    if !root.is_dir() {
        return Err("artifact root must be a directory".into());
    }
    let before = artifact_hashes(&root)?;
    let (source_binding, source_binding_error) =
        source_binding_report(&root, &before, expected_source_revision);
    let languages = required.iter().map(String::as_str).collect::<Vec<_>>();
    let mut errors = Vec::new();
    if let Some(error) = source_binding_error {
        errors.push(error);
    }
    let mut violations = Vec::new();
    let descriptor_shape_inventory = match (
        resolved_enum_fields(),
        resolved_oneof_members(),
        resolved_presence_fields(),
    ) {
        (Ok(enums), Ok(oneofs), Ok(presence)) => json!({
            "rust_owned": true,
            "enum_fields": enums.len(),
            "oneof_members": oneofs.len(),
            "presence_fields": presence.len(),
            "known_message_oneof_arms": oneofs.iter().filter(|member| {
                matches!(format!("{:?}", member.payload_kind).as_str(), "Message" | "Group")
            }).count(),
        }),
        (enum_result, oneof_result, presence_result) => {
            if let Err(error) = enum_result {
                errors.push(error);
            }
            if let Err(error) = oneof_result {
                errors.push(error);
            }
            if let Err(error) = presence_result {
                errors.push(error);
            }
            Value::Null
        }
    };
    let surfaces_present = match audit_required_generated_public_surfaces(&root, &languages) {
        Ok(found) => {
            violations.extend(found);
            true
        }
        Err(error) => {
            errors.push(error);
            false
        }
    };
    if surfaces_present {
        match audit_required_generated_descriptor_shape_coverage(&root, &languages) {
            Ok(found) => violations.extend(found),
            Err(error) => errors.push(error),
        }
    }
    match audit_generated_type_features(&root) {
        Ok(found) => violations.extend(found),
        Err(error) => errors.push(error),
    }
    if before != artifact_hashes(&root)? {
        return Err("generated artifacts changed during the public-type audit".into());
    }
    let executable = env::current_exe().map_err(|error| error.to_string())?;
    let verifier_hash = format!(
        "{:x}",
        Sha256::digest(fs::read(executable).map_err(|error| error.to_string())?)
    );
    let passed = errors.is_empty() && violations.is_empty();
    let violations = violations
        .into_iter()
        .map(|violation| {
            json!({
                "language": violation.language, "path": violation.path,
                "line": violation.line, "reason": violation.reason,
            })
        })
        .collect::<Vec<_>>();
    Ok((
        json!({
            "schema": "acyclic.sdk.generated-public-type-audit.v1",
            "artifact_closure_schema": ARTIFACT_CLOSURE_SCHEMA,
            "check": "generated public source types",
            "passed": passed, "required_languages": required,
            "verifier_sha256": verifier_hash, "artifact_sha256": before,
            "source_binding": source_binding,
            "language_registry": surface_registry(&before, expected_source_revision),
            "descriptor_shape_inventory": descriptor_shape_inventory,
            "errors": errors, "violations": violations,
        }),
        passed,
    ))
}

fn main() -> ExitCode {
    let result =
        parse_args(env::args().skip(1)).and_then(|(root, required, source_revision)| {
            let source_revision = source_revision.ok_or(
                "--source-revision is required for executable audits; bind generated artifacts to the Rust checkout revision",
            )?;
            audit_with_source(&root, &required, Some(&source_revision))
        });
    match result {
        Ok((report, passed)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("audit report serializes")
            );
            if passed {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(error) => {
            eprintln!("public type audit: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arguments_cannot_disable_required_surfaces() {
        assert!(parse_args(Vec::<String>::new()).is_err());
        assert!(
            parse_args(["--artifact-root", "x", "--required-language", ""].map(str::to_owned))
                .is_err()
        );
        assert!(
            parse_args(["--artifact-root", "x", "--artifact-root", "y"].map(str::to_owned))
                .is_err()
        );
        let (_, required, source_revision) =
            parse_args(["--artifact-root", "x"].map(str::to_owned)).unwrap();
        assert_eq!(required, REQUIRED_PRODUCT_SURFACES);
        assert_eq!(source_revision, None);
    }
    #[test]
    fn empty_artifact_directory_reports_missing_surface() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = env::temp_dir().join(format!(
            "acyclic-empty-type-audit-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        let result = audit(&root, &["typescript".into()]);
        fs::remove_dir(&root).unwrap();
        let (report, passed) = result.unwrap();
        assert!(!passed);
        assert_eq!(report["passed"], false);
        assert!(report["errors"][0]
            .as_str()
            .unwrap()
            .contains("missing generated public SDK surfaces"));
        assert_eq!(report["artifact_sha256"], json!({}));
    }
    #[test]
    fn missing_artifact_directory_cannot_pass() {
        let root =
            env::temp_dir().join(format!("acyclic-missing-type-audit-{}", std::process::id()));
        assert!(audit(&root, &["typescript".into()]).is_err());
    }

    #[test]
    fn generic_markers_cannot_bypass_descriptor_shape_gate() {
        let root = env::temp_dir().join(format!(
            "acyclic-generic-shape-audit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join("jvm")).unwrap();
        fs::write(
            root.join("jvm").join("RustSemanticTypes.java"),
            "public final class Unknown {}\n".to_owned()
                + "public final class Optional {}\n"
                + "public final class Oneof {}\n"
                + "public final class IdempotencyKey {}\n",
        )
        .unwrap();

        let (report, passed) = audit(&root, &["jvm".into()]).unwrap();
        assert!(!passed);
        assert!(report["violations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|finding| {
                finding["reason"] == "generated facade omits a Rust descriptor enum identity"
            }));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn source_binding_rejects_a_stale_generated_cohort() {
        let root = env::temp_dir().join(format!(
            "acyclic-source-binding-audit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("qualification-receipt.json"),
            r#"{"schema":"acyclic.sdk.qualification.receipt.v1","source_revision":"0000000000000000000000000000000000000000"}"#,
        )
        .unwrap();
        let hashes = artifact_hashes(&root).unwrap();
        let (_, error) = source_binding_report(
            &root,
            &hashes,
            Some("1111111111111111111111111111111111111111"),
        );
        assert!(error
            .unwrap()
            .contains("does not match expected Rust revision"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn source_binding_requires_authority_metadata_when_requested() {
        let root = env::temp_dir().join(format!(
            "acyclic-source-binding-missing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let hashes = artifact_hashes(&root).unwrap();
        let (report, error) = source_binding_report(
            &root,
            &hashes,
            Some("1111111111111111111111111111111111111111"),
        );
        assert_eq!(report["status"], "missing");
        assert!(error.unwrap().contains("no source authority metadata"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn language_registry_exposes_non_core_generated_surfaces() {
        let root = env::temp_dir().join(format!(
            "acyclic-language-registry-audit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        for (directory, file) in [
            ("haskell", "generated_typed.hs"),
            ("php", "RustTyped.php"),
            ("dart", "generated_typed.dart"),
            ("ruby", "generated_typed.rb"),
        ] {
            fs::create_dir_all(root.join(directory)).unwrap();
            fs::write(root.join(directory).join(file), "generated").unwrap();
        }
        let registry = surface_registry(
            &artifact_hashes(&root).unwrap(),
            Some("1111111111111111111111111111111111111111"),
        );
        for language in ["haskell", "php", "dart", "ruby"] {
            let entry = registry
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["language"] == language)
                .unwrap();
            assert_eq!(entry["present"], true);
            assert_eq!(entry["files"].as_array().unwrap().len(), 1);
        }
        fs::remove_dir_all(root).unwrap();
    }
}
