//! Executable acceptance check over generated public SDK type surfaces.
use acyclic_sdk_contract_wire::type_policy::{
    REQUIRED_PRODUCT_SURFACES, audit_generated_type_features,
    audit_required_generated_descriptor_shape_coverage,
    audit_required_generated_public_surfaces,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

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

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<(PathBuf, Vec<String>), String> {
    let mut root = None;
    let mut required = Vec::new();
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
            _ => return Err(format!("unknown argument: {argument}")),
        }
    }
    if required.is_empty() {
        required = REQUIRED_PRODUCT_SURFACES
            .iter()
            .map(|value| (*value).to_owned())
            .collect();
    }
    Ok((root.ok_or("--artifact-root is required")?, required))
}

fn audit(root: &Path, required: &[String]) -> Result<(Value, bool), String> {
    let root = root
        .canonicalize()
        .map_err(|error| format!("resolve artifact root: {error}"))?;
    if !root.is_dir() {
        return Err("artifact root must be a directory".into());
    }
    let before = artifact_hashes(&root)?;
    let languages = required.iter().map(String::as_str).collect::<Vec<_>>();
    let mut errors = Vec::new();
    let mut violations = Vec::new();
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
            "check": "generated public source types",
            "passed": passed, "required_languages": required,
            "verifier_sha256": verifier_hash, "artifact_sha256": before,
            "errors": errors, "violations": violations,
        }),
        passed,
    ))
}

fn main() -> ExitCode {
    let result =
        parse_args(env::args().skip(1)).and_then(|(root, required)| audit(&root, &required));
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
        let (_, required) = parse_args(["--artifact-root", "x"].map(str::to_owned)).unwrap();
        assert_eq!(required, REQUIRED_PRODUCT_SURFACES);
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
            "public final class Unknown {}\n"
                .to_owned()
                + "public final class Optional {}\n"
                + "public final class Oneof {}\n"
                + "public final class IdempotencyKey {}\n",
        )
        .unwrap();

        let (report, passed) = audit(&root, &["jvm".into()]).unwrap();
        assert!(!passed);
        assert!(report["violations"].as_array().unwrap().iter().any(|finding| {
            finding["reason"] == "generated facade omits a Rust descriptor enum identity"
        }));
        fs::remove_dir_all(root).unwrap();
    }
}
