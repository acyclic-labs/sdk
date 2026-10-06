//! Executable acceptance check over generated public SDK type surfaces.
use acyclic_sdk_contract_wire::type_policy::{
    GeneratedTypeAuditScope, PRIMARY_GENERATED_TYPE_AUDIT_LANGUAGES,
    audit_generated_type_features_for_languages,
    audit_language_producer_source_bindings_for_languages, discover_language_producer_surfaces,
    audit_required_generated_descriptor_shape_coverage_for_languages,
    audit_required_generated_public_surfaces_for_languages, resolved_enum_fields,
    resolved_oneof_members, resolved_presence_fields,
};
use serde_json::{Value, json};
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

struct AuditArgs {
    root: PathBuf,
    required: Vec<String>,
    scope: GeneratedTypeAuditScope,
    source_revision: Option<String>,
    source_digest: Option<String>,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<AuditArgs, String> {
    let mut root = None;
    let mut required = Vec::new();
    let mut scope = GeneratedTypeAuditScope::Full;
    let mut scope_seen = false;
    let mut source_revision = None;
    let mut source_digest = None;
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
            "--scope" => {
                if scope_seen {
                    return Err("--scope must occur once".into());
                }
                scope_seen = true;
                scope = match args
                    .next()
                    .ok_or("--scope requires rust-typescript or full")?
                    .as_str()
                {
                    "rust-typescript" | "primary" => GeneratedTypeAuditScope::RustTypescript,
                    "full" => GeneratedTypeAuditScope::Full,
                    value => return Err(format!(
                        "unknown generated type audit scope {value}; expected rust-typescript or full"
                    )),
                };
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
            "--model-digest" => {
                if source_digest.is_some() {
                    return Err("--model-digest must occur once".into());
                }
                let digest = args
                    .next()
                    .ok_or("--model-digest requires a 64-character model digest")?;
                if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    return Err("--model-digest must be 64 hexadecimal characters".into());
                }
                source_digest = Some(digest);
            }
            _ => return Err(format!("unknown argument: {argument}")),
        }
    }
    if required.is_empty() {
        required = scope
            .default_required_languages()
            .iter()
            .map(|value| (*value).to_owned())
            .collect();
    } else if scope == GeneratedTypeAuditScope::RustTypescript
        && required
            .iter()
            .any(|language| !PRIMARY_GENERATED_TYPE_AUDIT_LANGUAGES.contains(&language.as_str()))
    {
        return Err(
            "--scope rust-typescript only permits --required-language rust or typescript".into(),
        );
    }
    Ok(AuditArgs {
        root: root.ok_or("--artifact-root is required")?,
        required,
        scope,
        source_revision,
        source_digest,
    })
}

#[cfg(test)]
fn audit(root: &Path, required: &[String]) -> Result<(Value, bool), String> {
    audit_with_source(
        root,
        required,
        GeneratedTypeAuditScope::Full,
        None,
        None,
    )
}

fn source_revision_from_metadata(value: &Value) -> Option<String> {
    fn git_revision(value: Option<&Value>) -> Option<String> {
        let value = value.and_then(Value::as_str)?;
        (value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .then(|| value.to_owned())
    }

    let schema = value.get("schema").and_then(Value::as_str).unwrap_or("");
    let authority = value.get("authority");

    // A Rust authority has two different identities: source_git_sha is the
    // checkout revision, while source_revision is the contract-model digest.
    // Never treat the latter as a Git revision.  The same distinction applies
    // to the generated source-authority manifest whose source_digest is a
    // whole-checkout digest rather than a contract digest.
    match schema {
        "acyclic.sdk.rust-authority.v1" => {
            return git_revision(value.get("source_git_sha"));
        }
        "acyclic.sdk.generation.source-authority.v1"
        | "acyclic.sdk.examples.source-authority.v1" => {
            return git_revision(value.get("source_revision"));
        }
        "acyclic.sdk.language-toolchain-receipt.v1" => {
            return git_revision(value.get("source_git_sha"))
                .or_else(|| git_revision(value.get("source_revision")));
        }
        "acyclic.sdk.qualification.receipt.v1" => {
            if matches!(
                value.get("source_revision_kind").and_then(Value::as_str),
                Some("git-oid" | "git-revision")
            ) {
                return git_revision(value.get("source_revision"));
            }
            return None;
        }
        _ => {}
    }

    // Live qualification receipts place the authority document under an
    // `authority` object.  They are accepted only when their schema declares
    // the RPD family and the nested value is explicitly a Git revision.
    if schema.starts_with("acyclic.sdk.rpd.") {
        return git_revision(authority.and_then(|item| item.get("source_git_sha")))
            .or_else(|| git_revision(value.get("source_git_sha")));
    }

    // Other source-bound receipts are schema-scoped by their explicit Git
    // kind.  Unrelated JSON with a convenient `source_revision` field must
    // never participate in the artifact-cohort decision.
    if value.get("source_git_sha_kind").and_then(Value::as_str) == Some("git-revision") {
        return git_revision(value.get("source_git_sha"));
    }
    if matches!(
        value.get("source_revision_kind").and_then(Value::as_str),
        Some("git-oid" | "git-revision")
    ) {
        return git_revision(value.get("source_revision"));
    }
    None
}

fn metadata_source_revisions(
    root: &Path,
    hashes: &BTreeMap<String, String>,
    audit_languages: Option<&[&str]>,
) -> Vec<(String, String)> {
    let mut observations = Vec::new();
    for relative in hashes.keys() {
        let path = root.join(relative.replace('/', std::path::MAIN_SEPARATOR_STR));
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        if audit_languages.is_some_and(|languages| {
            match metadata_language(relative) {
                Some(language) => !languages.contains(&language.as_str()),
                // Root authority metadata is shared by the selected scope;
                // an unclassified nested receipt belongs to a deferred
                // producer and must not block the primary docs gate.
                None => relative.contains('/'),
            }
        }) {
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

fn metadata_language(relative: &str) -> Option<String> {
    let normalized = relative.replace('\\', "/");
    let parts = normalized.split('/').collect::<Vec<_>>();
    if parts.first() == Some(&"generated") && parts.get(1) == Some(&"rust") {
        return Some("rust".to_owned());
    }
    if parts.first() == Some(&"generated")
        && parts.get(1) == Some(&"rust-source-authority.json")
    {
        return Some("rust".to_owned());
    }
    if parts.first() == Some(&"typescript") {
        return Some("typescript".to_owned());
    }
    if parts.first() == Some(&"language-producers") {
        return parts.get(1).map(|language| (*language).to_owned());
    }
    match parts.first().copied() {
        Some("python" | "go" | "jvm" | "csharp" | "dotnet" | "swift" | "cpp" | "ruby"
        | "php" | "dart" | "haskell") => parts.first().map(|language| (*language).to_owned()),
        _ => None,
    }
}

fn source_binding_report_for_languages(
    root: &Path,
    hashes: &BTreeMap<String, String>,
    expected: Option<&str>,
    audit_languages: Option<&[&str]>,
) -> (Value, Option<String>) {
    let observations = metadata_source_revisions(root, hashes, audit_languages);
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
    let normalized = relative.replace('\\', "/");
    if normalized.ends_with(".rs")
        && normalized
            .split('/')
            .collect::<Vec<_>>()
            .windows(2)
            .any(|parts| parts == ["generated", "rust"])
    {
        return Some("rust");
    }
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
        "generated_typed.rb" | "generated_typed.rbs" | "rust_generated.rbi" => Some("ruby"),
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
    json!(
        TARGET_LANGUAGE_REGISTRY
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
            .collect::<Vec<_>>()
    )
}

fn audit_with_source(
    root: &Path,
    required: &[String],
    scope: GeneratedTypeAuditScope,
    expected_source_revision: Option<&str>,
    expected_model_digest: Option<&str>,
) -> Result<(Value, bool), String> {
    let root = root
        .canonicalize()
        .map_err(|error| format!("resolve artifact root: {error}"))?;
    if !root.is_dir() {
        return Err("artifact root must be a directory".into());
    }
    let before = artifact_hashes(&root)?;
    let audit_languages = match scope {
        GeneratedTypeAuditScope::RustTypescript => Some(PRIMARY_GENERATED_TYPE_AUDIT_LANGUAGES),
        GeneratedTypeAuditScope::Full => None,
    };
    let (source_binding, source_binding_error) = source_binding_report_for_languages(
        &root,
        &before,
        expected_source_revision,
        audit_languages,
    );
    let languages = required.iter().map(String::as_str).collect::<Vec<_>>();
    let mut errors = Vec::new();
    if let Some(error) = source_binding_error {
        errors.push(error);
    }
    let producer_source_binding = if root.join("language-producers").is_dir() {
        let surface_files = discover_language_producer_surfaces(&root)?;
        let producer_findings = audit_language_producer_source_bindings_for_languages(
            &root,
            expected_source_revision,
            expected_model_digest,
            audit_languages,
        )?;
        let findings = producer_findings
            .iter()
            .map(|finding| {
                json!({
                    "producer": finding.producer,
                    "path": finding.path,
                    "reason": finding.reason,
                })
            })
            .collect::<Vec<_>>();
        json!({
            "status": if findings.is_empty() { "passed" } else { "mismatch" },
            "surface_files": surface_files.iter().map(|path| {
                path.strip_prefix(&root)
                    .unwrap_or(path)
                    .to_string_lossy()
                    .replace('\\', "/")
            }).collect::<Vec<_>>(),
            "violations": findings,
        })
    } else {
        json!({
            "status": "not_present",
            "surface_files": [],
            "violations": [],
        })
    };
    let producer_binding_failed = producer_source_binding["status"] == "mismatch";
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
    let surfaces_present = match audit_required_generated_public_surfaces_for_languages(
        &root,
        &languages,
        audit_languages,
    ) {
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
        match audit_required_generated_descriptor_shape_coverage_for_languages(
            &root,
            &languages,
            audit_languages,
        ) {
            Ok(found) => violations.extend(found),
            Err(error) => errors.push(error),
        }
    }
    match audit_generated_type_features_for_languages(&root, audit_languages) {
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
    let passed = errors.is_empty() && violations.is_empty() && !producer_binding_failed;
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
            "scope": scope.id(),
            "passed": passed, "required_languages": required,
            "verifier_sha256": verifier_hash, "artifact_sha256": before,
            "source_binding": source_binding,
            "producer_source_binding": producer_source_binding,
            "language_registry": surface_registry(&before, expected_source_revision),
            "descriptor_shape_inventory": descriptor_shape_inventory,
            "errors": errors, "violations": violations,
        }),
        passed,
    ))
}

fn main() -> ExitCode {
    let result = parse_args(env::args().skip(1)).and_then(|arguments| {
        let source_revision = arguments.source_revision.ok_or(
            "--source-revision is required for executable audits; bind generated artifacts to the Rust checkout revision",
        )?;
        audit_with_source(
            &arguments.root,
            &arguments.required,
            arguments.scope,
            Some(&source_revision),
            arguments.source_digest.as_deref(),
        )
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
    use acyclic_sdk_contract_wire::type_policy::REQUIRED_PRODUCT_SURFACES;
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
        let arguments = parse_args(["--artifact-root", "x"].map(str::to_owned)).unwrap();
        assert_eq!(arguments.required, REQUIRED_PRODUCT_SURFACES);
        assert_eq!(arguments.scope, GeneratedTypeAuditScope::Full);
        assert_eq!(arguments.source_revision, None);
        assert_eq!(arguments.source_digest, None);
        let arguments = parse_args(
            [
                "--artifact-root",
                "x",
                "--source-revision",
                "a".repeat(40).as_str(),
                "--model-digest",
                "b".repeat(64).as_str(),
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(arguments.scope, GeneratedTypeAuditScope::Full);
        assert_eq!(arguments.source_revision, Some("a".repeat(40)));
        assert_eq!(arguments.source_digest, Some("b".repeat(64)));
        assert!(parse_args(
            ["--artifact-root", "x", "--model-digest", "not-a-digest"]
                .map(str::to_owned),
        )
        .is_err());
        let arguments = parse_args(
            ["--artifact-root", "x", "--scope", "rust-typescript"]
                .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(arguments.scope, GeneratedTypeAuditScope::RustTypescript);
        assert_eq!(arguments.required, PRIMARY_GENERATED_TYPE_AUDIT_LANGUAGES);
        assert!(parse_args(
            [
                "--artifact-root",
                "x",
                "--scope",
                "rust-typescript",
                "--required-language",
                "python",
            ]
            .map(str::to_owned),
        )
        .is_err());
    }

    #[test]
    fn primary_scope_requires_and_filters_rust_typescript_surfaces() {
        let root = env::temp_dir().join(format!(
            "acyclic-primary-type-audit-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("generated/rust")).unwrap();
        fs::create_dir_all(root.join("typescript")).unwrap();
        fs::create_dir_all(root.join("python")).unwrap();
        fs::write(root.join("generated/rust/contract.rs"), "pub struct RustSurface;\n").unwrap();
        fs::write(
            root.join("typescript/actors-metadata.ts"),
            "import { FooRequest, FooResponse } from \"./generated/proto/foo_pb\";\nraw(request: FooRequest): Promise<FooResponse> {}\n",
        )
        .unwrap();
        fs::write(
            root.join("python/remote.py"),
            "IdempotencyKeyValue: TypeAlias = object\n",
        )
        .unwrap();

        let required = PRIMARY_GENERATED_TYPE_AUDIT_LANGUAGES;
        let findings = acyclic_sdk_contract_wire::type_policy::
            audit_required_generated_public_surfaces_for_languages(&root, required, Some(required))
            .unwrap();
        assert!(findings.iter().any(|finding| finding.language == "typescript"));
        assert!(findings.iter().all(|finding| finding.language != "python"));
        let full_findings = acyclic_sdk_contract_wire::type_policy::
            audit_required_generated_public_surfaces_for_languages(&root, &["python"], None)
            .unwrap();
        assert!(full_findings.iter().any(|finding| finding.language == "python"));
        fs::remove_dir_all(root).unwrap();
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
        assert!(
            report["errors"][0]
                .as_str()
                .unwrap()
                .contains("missing generated public SDK surfaces")
        );
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
        assert!(
            report["violations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|finding| {
                    finding["reason"] == "generated facade omits a Rust descriptor enum identity"
                })
        );
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
            r#"{"schema":"acyclic.sdk.qualification.receipt.v1","source_revision_kind":"git-oid","source_revision":"0000000000000000000000000000000000000000"}"#,
        )
        .unwrap();
        let hashes = artifact_hashes(&root).unwrap();
        let (_, error) = source_binding_report_for_languages(
            &root,
            &hashes,
            Some("1111111111111111111111111111111111111111"),
            None,
        );
        assert!(
            error
                .unwrap()
                .contains("does not match expected Rust revision")
        );
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
        let (report, error) = source_binding_report_for_languages(
            &root,
            &hashes,
            Some("1111111111111111111111111111111111111111"),
            None,
        );
        assert_eq!(report["status"], "missing");
        assert!(error.unwrap().contains("no source authority metadata"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn source_binding_accepts_a_matching_generated_cohort() {
        let root = env::temp_dir().join(format!(
            "acyclic-source-binding-match-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("sdk-qualification-receipt.json"),
            r#"{"schema":"acyclic.sdk.qualification.receipt.v1","source_revision_kind":"git-oid","source_revision":"1111111111111111111111111111111111111111"}"#,
        )
        .unwrap();
        let hashes = artifact_hashes(&root).unwrap();
        let (report, error) = source_binding_report_for_languages(
            &root,
            &hashes,
            Some("1111111111111111111111111111111111111111"),
            None,
        );
        assert_eq!(report["status"], "passed");
        assert!(error.is_none());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn source_binding_rejects_mixed_generated_revisions() {
        let root = env::temp_dir().join(format!(
            "acyclic-source-binding-mixed-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("sdk-qualification-receipt.json"),
            r#"{"schema":"acyclic.sdk.qualification.receipt.v1","source_revision_kind":"git-oid","source_revision":"1111111111111111111111111111111111111111"}"#,
        )
        .unwrap();
        fs::write(
            root.join("docs.json"),
            r#"{"schema":"acyclic.sdk.qualification.receipt.v1","source_revision_kind":"git-oid","source_revision":"2222222222222222222222222222222222222222"}"#,
        )
        .unwrap();
        let hashes = artifact_hashes(&root).unwrap();
        let (report, error) = source_binding_report_for_languages(
            &root,
            &hashes,
            Some("1111111111111111111111111111111111111111"),
            None,
        );
        assert_eq!(report["status"], "mismatch");
        assert!(
            error
                .unwrap()
                .contains("does not match expected Rust revision")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn source_binding_ignores_contract_digest_and_unscoped_nested_json() {
        let root = env::temp_dir().join(format!(
            "acyclic-source-binding-schema-scope-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join("nested/cache")).unwrap();
        let git = "1111111111111111111111111111111111111111";
        let digest = "2222222222222222222222222222222222222222222222222222222222222222";
        fs::write(
            root.join("rust-authority.json"),
            format!(
                r#"{{"schema":"acyclic.sdk.rust-authority.v1","source_git_sha":"{git}","source_revision":"{digest}","source_revision_kind":"rust-model-sha256"}}"#
            ),
        )
        .unwrap();
        fs::write(
            root.join("nested/cache/random.json"),
            format!(r#"{{"source_revision":"{digest}","source":{{"revision":"{git}"}}}}"#),
        )
        .unwrap();
        fs::write(
            root.join("nested/cache/python-receipt.json"),
            r#"{"schema":"acyclic.sdk.qualification.receipt.v1","source_revision_kind":"git-oid","source_revision":"3333333333333333333333333333333333333333"}"#,
        )
        .unwrap();
        let hashes = artifact_hashes(&root).unwrap();
        let (report, error) =
            source_binding_report_for_languages(&root, &hashes, Some(git), None);
        assert_eq!(report["status"], "mismatch");
        assert!(error.is_some());
        assert_eq!(report["observations"].as_array().unwrap().len(), 2);
        let (primary_report, primary_error) = source_binding_report_for_languages(
            &root,
            &hashes,
            Some(git),
            Some(PRIMARY_GENERATED_TYPE_AUDIT_LANGUAGES),
        );
        assert_eq!(primary_report["status"], "passed");
        assert!(primary_error.is_none());
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
        fs::create_dir_all(root.join("generated/rust")).unwrap();
        fs::write(root.join("generated/rust/contract.rs"), "generated").unwrap();
        let registry = surface_registry(
            &artifact_hashes(&root).unwrap(),
            Some("1111111111111111111111111111111111111111"),
        );
        for language in ["rust", "haskell", "php", "dart", "ruby"] {
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
