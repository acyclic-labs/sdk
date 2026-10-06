//! Rust-owned orchestration for the Python transport target.
//!
//! The Python helper is intentionally a thin `grpcio-tools` invocation. This
//! binary owns the authority boundary, pinned tool versions, generated import
//! normalization, and drift checking.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

const EXPECTED_GRPCIO_TOOLS: &str = "1.83.0";
const EXPECTED_PROTOBUF: &str = "7.36.0";
const AUTHORITY_MARKER: &str = "rust-authority.json";

#[derive(Debug, Deserialize)]
struct AuthorityManifest {
    schema: String,
    authority: String,
    source_git_sha: Option<String>,
    source_git_sha_kind: Option<String>,
    source_revision: Option<String>,
    source_revision_kind: Option<String>,
    exporter: Option<String>,
    families: Vec<AuthorityFamily>,
}

#[derive(Debug, Deserialize)]
struct AuthorityFamily {
    source: String,
    source_sha256: String,
    descriptor: String,
    descriptor_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandMode {
    Generate,
    Check,
    Package,
}

#[derive(Debug)]
struct Config {
    mode: CommandMode,
    schema_root: PathBuf,
    output: PathBuf,
    python_script: PathBuf,
    python: String,
    source_root: Option<PathBuf>,
    generated_root: Option<PathBuf>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("sdk-python-generator: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut config = parse_args(env::args().skip(1))?;
    if config.mode == CommandMode::Package {
        return package_python(&config);
    }
    if !config.schema_root.is_absolute() {
        config.schema_root = env::current_dir()
            .map_err(|error| format!("resolve current directory: {error}"))?
            .join(&config.schema_root);
    }
    validate_schema_root(&config.schema_root)?;
    let versions = probe_python_versions(&config.python)?;
    ensure_versions(&versions.0, &versions.1)?;

    let temporary = temporary_directory("acyclic-sdk-python");
    if temporary.exists() {
        fs::remove_dir_all(&temporary)
            .map_err(|error| format!("remove temporary output: {error}"))?;
    }
    fs::create_dir_all(&temporary).map_err(|error| format!("create temporary output: {error}"))?;

    let result = (|| {
        invoke_python(&config, &temporary)?;
        normalize_generated(&temporary)?;
        write_generation_metadata(&temporary, &config.schema_root)?;
        match config.mode {
            CommandMode::Generate => {
                replace_output(&temporary, &config.output)?;
                Ok(())
            }
            CommandMode::Check => {
                if same_tree(&temporary, &config.output)? {
                    Ok(())
                } else {
                    Err(format!(
                        "generated Python output is stale at {}; run the Rust generator in generate mode",
                        config.output.display()
                    ))
                }
            }
            CommandMode::Package => unreachable!("package mode returned before generation"),
        }
    })();
    let _ = fs::remove_dir_all(&temporary);
    result
}

fn parse_args<I>(arguments: I) -> Result<Config, String>
where
    I: IntoIterator<Item = String>,
{
    let mut arguments = arguments.into_iter();
    let mode = match arguments.next().as_deref() {
        Some("generate") => CommandMode::Generate,
        Some("check") => CommandMode::Check,
        Some("package") => CommandMode::Package,
        Some("--help") | Some("-h") => {
            println!(
                "sdk-python-generator <generate|check> --schema-root ROOT --output DIR [--python-script FILE] [--python BIN]\n\
                 sdk-python-generator package --source-root ROOT --generated-root DIR --output DIR [--python BIN]"
            );
            std::process::exit(0);
        }
        Some(other) => return Err(format!("unknown mode {other}")),
        None => return Err("mode is required: generate or check".to_owned()),
    };

    let mut schema_root = None;
    let mut output = None;
    let mut python_script = None;
    let mut python = env::var("PYTHON").unwrap_or_else(|_| "python".to_owned());
    let mut source_root = None;
    let mut generated_root = None;
    while let Some(argument) = arguments.next() {
        let mut value = || {
            arguments
                .next()
                .ok_or_else(|| format!("missing value for {argument}"))
        };
        match argument.as_str() {
            "--schema-root" => schema_root = Some(PathBuf::from(value()?)),
            "--output" => output = Some(PathBuf::from(value()?)),
            "--python-script" => python_script = Some(PathBuf::from(value()?)),
            "--python" => python = value()?,
            "--source-root" => source_root = Some(PathBuf::from(value()?)),
            "--generated-root" => generated_root = Some(PathBuf::from(value()?)),
            unknown => return Err(format!("unknown argument {unknown}")),
        }
    }

    if mode == CommandMode::Package {
        let output = output.ok_or("--output is required")?;
        return Ok(Config {
            mode,
            schema_root: PathBuf::new(),
            output,
            python_script: PathBuf::new(),
            python,
            source_root,
            generated_root,
        });
    }

    let schema_root = schema_root.ok_or("--schema-root is required")?;
    let output = output.ok_or("--output is required")?;
    let script = python_script.unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("..")
            .join("python")
            .join("generate.py")
    });
    Ok(Config {
        mode,
        schema_root,
        output,
        python_script: script,
        python,
        source_root: None,
        generated_root: None,
    })
}

fn validate_schema_root(schema_root: &Path) -> Result<(), String> {
    if !schema_root.is_dir() {
        return Err(format!(
            "schema root is not a directory: {}",
            schema_root.display()
        ));
    }
    let marker = schema_root.join(AUTHORITY_MARKER);
    let marker_contents = fs::read_to_string(&marker)
        .map_err(|error| format!("read Rust authority marker {}: {error}", marker.display()))?;
    let manifest: AuthorityManifest = serde_json::from_str(&marker_contents)
        .map_err(|error| format!("parse Rust authority marker {}: {error}", marker.display()))?;
    if manifest.schema != "acyclic.sdk.rust-authority.v1" || manifest.authority != "rust" {
        return Err(format!(
            "{} is not a Rust authority export",
            marker.display()
        ));
    }
    if manifest.families.is_empty() {
        return Err(format!(
            "{} contains no Rust authority families",
            marker.display()
        ));
    }
    let mut seen_sources = BTreeSet::new();
    let mut seen_descriptors = BTreeSet::new();
    for family in &manifest.families {
        if !seen_sources.insert(family.source.as_str()) {
            return Err(format!(
                "{} contains duplicate Rust authority source {}",
                marker.display(),
                family.source
            ));
        }
        if !seen_descriptors.insert(family.descriptor.as_str()) {
            return Err(format!(
                "{} contains duplicate Rust authority descriptor {}",
                marker.display(),
                family.descriptor
            ));
        }
        validate_hashed_file(schema_root, &family.source, &family.source_sha256)?;
        validate_hashed_file(schema_root, &family.descriptor, &family.descriptor_sha256)?;
    }
    Ok(())
}

fn validate_hashed_file(root: &Path, relative: &str, expected: &str) -> Result<(), String> {
    if !is_sha256(expected) {
        return Err(format!(
            "Rust authority hash for {relative} is not a lowercase SHA-256 digest"
        ));
    }
    let relative_path = Path::new(relative);
    if relative_path.is_absolute()
        || relative_path
            .components()
            .any(|component| component == std::path::Component::ParentDir)
    {
        return Err(format!(
            "Rust authority path escapes schema root: {relative}"
        ));
    }
    let path = root.join(relative);
    let bytes = fs::read(&path)
        .map_err(|error| format!("read Rust authority schema {}: {error}", path.display()))?;
    let actual = format!("{:x}", Sha256::digest(&bytes));
    if actual != expected {
        return Err(format!(
            "Rust authority content hash mismatch for {}: expected {}, got {}",
            path.display(),
            expected,
            actual
        ));
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn probe_python_versions(python: &str) -> Result<(String, String), String> {
    let script = "import importlib.metadata as m; print(m.version('grpcio-tools')); print(m.version('protobuf'))";
    let output = Command::new(python)
        .args(["-c", script])
        .output()
        .map_err(|error| format!("run {python} to inspect pinned generator versions: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{python} could not inspect grpcio-tools/protobuf: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let versions = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if versions.len() != 2 {
        return Err("Python version probe returned an unexpected number of versions".to_owned());
    }
    Ok((versions[0].clone(), versions[1].clone()))
}

fn ensure_versions(grpcio_tools: &str, protobuf: &str) -> Result<(), String> {
    if grpcio_tools != EXPECTED_GRPCIO_TOOLS || protobuf != EXPECTED_PROTOBUF {
        return Err(format!(
            "incompatible Python generator versions: grpcio-tools={grpcio_tools}, protobuf={protobuf}; expected grpcio-tools={EXPECTED_GRPCIO_TOOLS}, protobuf={EXPECTED_PROTOBUF}"
        ));
    }
    Ok(())
}

fn invoke_python(config: &Config, output: &Path) -> Result<(), String> {
    let status = Command::new(&config.python)
        .arg(&config.python_script)
        .args([
            "--schema-root",
            config.schema_root.to_string_lossy().as_ref(),
        ])
        .args(["--output", output.to_string_lossy().as_ref()])
        .status()
        .map_err(|error| format!("invoke Python grpcio-tools helper: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("Python grpcio-tools helper failed with {status}"))
    }
}

/// Build the installable Python artifacts from a Rust-generated package tree.
///
/// This is deliberately a Rust entrypoint rather than a second package recipe
/// in CI.  The source package is copied to an isolated staging directory,
/// stale generated output is omitted, and the exact generated subtree from the
/// preceding Rust authority stage is installed before invoking the pinned
/// Python build frontend.  In particular, `python/dist` is never used as an
/// input or output, so a wheel left by an older checkout cannot be qualified.
fn package_python(config: &Config) -> Result<(), String> {
    let source_root = config
        .source_root
        .as_deref()
        .ok_or("package mode requires --source-root")?;
    let generated_root = config
        .generated_root
        .as_deref()
        .ok_or("package mode requires --generated-root")?;
    let source_root = absolute_path(source_root)?;
    let generated_root = absolute_path(generated_root)?;
    let output_root = absolute_path(&config.output)?;
    require_directory(&source_root, "Python package source")?;
    require_directory(&generated_root, "Rust-generated Python subtree")?;
    require_file(
        &source_root.join("src/acyclic_sdk/remote.py"),
        "Rust-owned Python remote facade",
    )?;
    require_file(
        &source_root.join("src/acyclic_sdk/py.typed"),
        "Python typing marker",
    )?;
    require_file(
        &generated_root.join("generation.json"),
        "Rust Python generation metadata",
    )?;
    if let Some(parent) = output_root.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create Python package output parent: {error}"))?;
    }
    ensure_disjoint(&source_root, &generated_root, &output_root)?;
    prepare_empty_directory(&output_root)?;

    let stage = temporary_directory("acyclic-sdk-python-package");
    if stage.exists() {
        fs::remove_dir_all(&stage)
            .map_err(|error| format!("remove stale Python package staging directory: {error}"))?;
    }
    fs::create_dir_all(&stage)
        .map_err(|error| format!("create Python package staging directory: {error}"))?;
    let result = (|| {
        copy_package_source(&source_root, &stage)?;
        let generated_destination = stage.join("src/acyclic_sdk/generated");
        copy_tree(&generated_root, &generated_destination)
            .map_err(|error| format!("copy Rust-generated Python subtree: {error}"))?;
        require_file(
            &stage.join("src/acyclic_sdk/remote.py"),
            "staged Python remote facade",
        )?;
        require_file(
            &stage.join("src/acyclic_sdk/py.typed"),
            "staged Python typing marker",
        )?;
        let staged_source_sha256 = tree_sha256(&stage)?;

        let build_version = probe_package_build(&config.python)?;
        let source_date_epoch =
            env::var("SOURCE_DATE_EPOCH").unwrap_or_else(|_| "1735689600".to_owned());
        let artifact_root = output_root.join("artifacts");
        fs::create_dir_all(&artifact_root)
            .map_err(|error| format!("create Python artifact directory: {error}"))?;
        let status = Command::new(&config.python)
            .args([
                "-m",
                "build",
                "--sdist",
                "--wheel",
                "--no-isolation",
                "--outdir",
                artifact_root.to_string_lossy().as_ref(),
            ])
            .env("SOURCE_DATE_EPOCH", &source_date_epoch)
            .current_dir(&stage)
            .status()
            .map_err(|error| format!("invoke Python package builder: {error}"))?;
        if !status.success() {
            return Err(format!("Python package builder failed with {status}"));
        }

        let artifacts = collect_python_artifacts(&artifact_root)?;
        if artifacts.wheel.is_none() || artifacts.sdist.is_none() {
            return Err(format!(
                "Python package builder must produce exactly one wheel and one sdist in {}",
                artifact_root.display()
            ));
        }
        for artifact in [&artifacts.wheel, &artifacts.sdist].into_iter().flatten() {
            if artifact.extension().and_then(|value| value.to_str()) == Some("gz") {
                normalize_python_sdist(&config.python, artifact, &source_date_epoch)?;
            }
            verify_python_archive(&config.python, artifact)?;
        }

        let generation_metadata = fs::read(generated_root.join("generation.json"))
            .map_err(|error| format!("read Rust Python generation metadata: {error}"))?;
        let generation_value: serde_json::Value = serde_json::from_slice(&generation_metadata)
            .map_err(|error| format!("parse Rust Python generation metadata: {error}"))?;
        let source_git_sha = generation_value
            .get("source_git_sha")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or("Rust Python generation metadata is missing source_git_sha")?;
        let source_git_sha_kind = generation_value
            .get("source_git_sha_kind")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or("Rust Python generation metadata is missing source_git_sha_kind")?;
        let receipt = json!({
            "schema": "acyclic.sdk.python-package-receipt.v1",
            "authority": "rust",
            "builder": {
                "generator": format!("sdk-python-generator@{}", env!("CARGO_PKG_VERSION")),
                "build": build_version,
                "source_date_epoch": source_date_epoch,
                "source_root": "python",
                "generated_root": "rust-authority/python/generated",
            },
            "source_revision": generation_value.get("source_revision"),
            "source_revision_kind": generation_value.get("source_revision_kind"),
            "source_git_sha": source_git_sha,
            "source_git_sha_kind": source_git_sha_kind,
            "generation_metadata_sha256": sha256_bytes(&generation_metadata),
            "staged_source_sha256": staged_source_sha256,
            "artifacts": {
                "wheel": artifact_receipt(output_root.as_path(), artifacts.wheel.as_deref().unwrap()),
                "sdist": artifact_receipt(output_root.as_path(), artifacts.sdist.as_deref().unwrap()),
            },
        });
        let receipt_path = output_root.join("package-receipt.json");
        let receipt_text = serde_json::to_string_pretty(&receipt)
            .map_err(|error| format!("serialize Python package receipt: {error}"))?;
        fs::write(&receipt_path, format!("{receipt_text}\n"))
            .map_err(|error| format!("write Python package receipt: {error}"))?;
        Ok(())
    })();
    let _ = fs::remove_dir_all(&stage);
    result
}

fn absolute_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(env::current_dir()
            .map_err(|error| format!("resolve current directory: {error}"))?
            .join(path))
    }
}

fn require_directory(path: &Path, label: &str) -> Result<(), String> {
    if path.is_dir() {
        Ok(())
    } else {
        Err(format!("{label} directory is missing: {}", path.display()))
    }
}

fn require_file(path: &Path, label: &str) -> Result<(), String> {
    if path.is_file() {
        Ok(())
    } else {
        Err(format!("{label} is missing: {}", path.display()))
    }
}

fn path_is_within(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

fn ensure_disjoint(source: &Path, generated: &Path, output: &Path) -> Result<(), String> {
    let source =
        fs::canonicalize(source).map_err(|error| format!("canonicalize source: {error}"))?;
    let generated = fs::canonicalize(generated)
        .map_err(|error| format!("canonicalize generated output: {error}"))?;
    let output = if output.exists() {
        fs::canonicalize(output).map_err(|error| format!("canonicalize package output: {error}"))?
    } else {
        let parent = output
            .parent()
            .ok_or("package output has no parent directory")?;
        fs::canonicalize(parent)
            .map_err(|error| format!("canonicalize package output parent: {error}"))?
            .join(
                output
                    .file_name()
                    .ok_or("package output has no file name")?,
            )
    };
    if path_is_within(&output, &source)
        || path_is_within(&source, &output)
        || path_is_within(&output, &generated)
        || path_is_within(&generated, &output)
    {
        return Err(
            "Python package source, generated subtree, and artifact output must be disjoint"
                .to_owned(),
        );
    }
    Ok(())
}

fn prepare_empty_directory(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|error| format!("create package output: {error}"))?;
    for entry in fs::read_dir(path).map_err(|error| format!("read package output: {error}"))? {
        let entry = entry.map_err(|error| format!("read package output entry: {error}"))?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            fs::remove_dir_all(&entry_path)
                .map_err(|error| format!("clear package output: {error}"))?;
        } else {
            fs::remove_file(&entry_path)
                .map_err(|error| format!("clear package output: {error}"))?;
        }
    }
    Ok(())
}

fn copy_package_source(source: &Path, destination: &Path) -> Result<(), String> {
    fs::create_dir_all(destination).map_err(|error| format!("create package stage: {error}"))?;
    for entry in fs::read_dir(source).map_err(|error| format!("read package source: {error}"))? {
        let entry = entry.map_err(|error| format!("read package source entry: {error}"))?;
        let name = entry.file_name();
        let name_text = name.to_string_lossy();
        if matches!(
            name_text.as_ref(),
            "dist" | "build" | ".pytest_cache" | "__pycache__" | ".git"
        ) || name_text.ends_with(".egg-info")
        {
            continue;
        }
        let from = entry.path();
        let to = destination.join(&name);
        if from.is_dir() {
            copy_package_source(&from, &to)?;
        } else if from.is_file() {
            fs::copy(&from, &to).map_err(|error| {
                format!("copy Python package source {}: {error}", from.display())
            })?;
        }
    }
    Ok(())
}

fn probe_package_build(python: &str) -> Result<String, String> {
    let output = Command::new(python)
        .args([
            "-c",
            "import importlib.metadata as m; print(m.version('build'))",
        ])
        .output()
        .map_err(|error| format!("probe Python build frontend: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "Python package build requires pinned build; install build before packaging: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if version.is_empty() {
        return Err("Python build frontend returned no version".to_owned());
    }
    Ok(version)
}

#[derive(Debug, Default)]
struct PythonArtifacts {
    wheel: Option<PathBuf>,
    sdist: Option<PathBuf>,
}

fn collect_python_artifacts(root: &Path) -> Result<PythonArtifacts, String> {
    let mut artifacts = PythonArtifacts::default();
    for entry in fs::read_dir(root).map_err(|error| format!("read Python artifacts: {error}"))? {
        let path = entry
            .map_err(|error| format!("read Python artifact entry: {error}"))?
            .path();
        if !path.is_file() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if name.ends_with(".whl") {
            if artifacts.wheel.replace(path).is_some() {
                return Err("Python package builder produced multiple wheels".to_owned());
            }
        } else if name.ends_with(".tar.gz") {
            if artifacts.sdist.replace(path).is_some() {
                return Err("Python package builder produced multiple sdists".to_owned());
            }
        } else {
            return Err(format!("unexpected file in Python artifact output: {name}"));
        }
    }
    Ok(artifacts)
}

fn verify_python_archive(python: &str, path: &Path) -> Result<(), String> {
    let script = r#"
import pathlib, sys, tarfile, zipfile
path = pathlib.Path(sys.argv[1])
if path.name.endswith('.whl'):
    names = zipfile.ZipFile(path).namelist()
else:
    names = tarfile.open(path, 'r:gz').getnames()
def has(suffix):
    return any(name == suffix or name.endswith('/' + suffix) for name in names)
required = ('acyclic_sdk/remote.py', 'acyclic_sdk/py.typed', 'acyclic_sdk/generated/generation.json')
missing = [item for item in required if not has(item)]
if missing:
    raise SystemExit('archive is missing required Rust-owned package files: ' + ', '.join(missing))
"#;
    let status = Command::new(python)
        .args(["-c", script, path.to_string_lossy().as_ref()])
        .status()
        .map_err(|error| format!("verify Python artifact {}: {error}", path.display()))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "Python artifact verification failed: {}",
            path.display()
        ))
    }
}

fn normalize_python_sdist(
    python: &str,
    path: &Path,
    source_date_epoch: &str,
) -> Result<(), String> {
    let script = r#"
import gzip, io, os, pathlib, tarfile
source = pathlib.Path(os.environ['ACYCLIC_PYTHON_SDIST'])
temporary = source.with_name(source.name + '.normalized')
epoch = int(os.environ['SOURCE_DATE_EPOCH'])
with tarfile.open(source, 'r:gz') as source_tar, open(temporary, 'wb') as raw:
    with gzip.GzipFile(fileobj=raw, mode='wb', mtime=epoch) as compressed:
        with tarfile.open(fileobj=compressed, mode='w', format=tarfile.PAX_FORMAT) as target_tar:
            for member in sorted(source_tar.getmembers(), key=lambda item: item.name):
                payload = None
                if member.isfile():
                    extracted = source_tar.extractfile(member)
                    payload = io.BytesIO(extracted.read() if extracted else b'')
                member.mtime = epoch
                member.uid = 0
                member.gid = 0
                member.uname = ''
                member.gname = ''
                member.pax_headers = {}
                target_tar.addfile(member, payload)
os.replace(temporary, source)
"#;
    let status = Command::new(python)
        .args(["-c", script])
        .env("ACYCLIC_PYTHON_SDIST", path)
        .env("SOURCE_DATE_EPOCH", source_date_epoch)
        .status()
        .map_err(|error| format!("normalize Python sdist {}: {error}", path.display()))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "Python sdist normalization failed: {}",
            path.display()
        ))
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn tree_sha256(root: &Path) -> Result<String, String> {
    let mut hasher = Sha256::new();
    for relative in relative_files(root)? {
        hasher.update(relative.to_string_lossy().replace('\\', "/").as_bytes());
        hasher.update([0]);
        let bytes = fs::read(root.join(&relative))
            .map_err(|error| format!("read staged package file: {error}"))?;
        hasher.update(bytes);
        hasher.update([0]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn artifact_receipt(output_root: &Path, artifact: &Path) -> serde_json::Value {
    let relative = artifact
        .strip_prefix(output_root)
        .map(|path| path.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| artifact.to_string_lossy().replace('\\', "/"));
    let bytes = fs::read(artifact).unwrap_or_default();
    json!({
        "path": relative,
        "sha256": sha256_bytes(&bytes),
        "remote_py": true,
        "py_typed": true,
        "generation_json": true,
    })
}

fn normalize_generated(root: &Path) -> Result<(), String> {
    let directories = collect_directories(root)?;
    for directory in directories {
        let initializer = directory.join("__init__.py");
        if !initializer.exists() {
            fs::write(initializer, b"")
                .map_err(|error| format!("write Python package initializer: {error}"))?;
        }
    }
    for path in collect_files(root)? {
        let is_generated = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with("_pb2.py") || name.ends_with("_pb2_grpc.py"));
        if !is_generated {
            continue;
        }
        let package = path
            .parent()
            .and_then(|parent| parent.strip_prefix(root).ok())
            .map(|relative| {
                relative
                    .components()
                    .filter_map(|component| component.as_os_str().to_str())
                    .collect::<Vec<_>>()
                    .join(".")
            })
            .unwrap_or_default();
        let text = fs::read_to_string(&path)
            .map_err(|error| format!("read generated Python file: {error}"))?;
        let normalized = text
            .lines()
            .map(|line| normalize_python_import(line, &package))
            .collect::<Vec<_>>()
            .join("\n")
            + if text.ends_with('\n') { "\n" } else { "" };
        fs::write(path, normalized)
            .map_err(|error| format!("normalize generated Python import: {error}"))?;
    }
    Ok(())
}

fn normalize_python_import(line: &str, package: &str) -> String {
    let Some(rest) = line.strip_prefix("from ") else {
        return line.to_owned();
    };
    let Some((module, imported)) = rest.split_once(" import ") else {
        return line.to_owned();
    };
    if module.starts_with("google.") || module.starts_with("acyclic_sdk.") {
        return line.to_owned();
    }
    if module == package {
        format!("from . import {imported}")
    } else if module.contains('.') {
        format!("from acyclic_sdk.generated.{module} import {imported}")
    } else {
        line.to_owned()
    }
}

fn write_generation_metadata(root: &Path, schema_root: &Path) -> Result<(), String> {
    let marker = schema_root.join(AUTHORITY_MARKER);
    let marker_contents = fs::read_to_string(&marker)
        .map_err(|error| format!("read Rust authority marker for generation receipt: {error}"))?;
    let manifest: AuthorityManifest = serde_json::from_str(&marker_contents)
        .map_err(|error| format!("parse Rust authority marker for generation receipt: {error}"))?;
    let families = manifest
        .families
        .iter()
        .map(|family| {
            json!({
                "source": family.source,
                "source_sha256": family.source_sha256,
                "descriptor": family.descriptor,
                "descriptor_sha256": family.descriptor_sha256,
            })
        })
        .collect::<Vec<_>>();
    let metadata = json!({
        "authority": manifest.authority,
        "generator": format!("sdk-python-generator@{}", env!("CARGO_PKG_VERSION")),
        "grpcio-tools": EXPECTED_GRPCIO_TOOLS,
        "protobuf": EXPECTED_PROTOBUF,
        "source_git_sha": manifest.source_git_sha,
        "source_git_sha_kind": manifest.source_git_sha_kind,
        "source_revision": manifest.source_revision,
        "source_revision_kind": manifest.source_revision_kind,
        "exporter": manifest.exporter,
        "families": families,
    });
    let metadata = serde_json::to_string_pretty(&metadata)
        .map_err(|error| format!("serialize Python generation metadata: {error}"))?;
    fs::write(root.join("generation.json"), format!("{metadata}\n"))
        .map_err(|error| format!("write Python generation metadata: {error}"))
}

fn replace_output(source: &Path, destination: &Path) -> Result<(), String> {
    if destination.exists() {
        if !destination.is_dir() {
            return Err(format!(
                "Python output is not a directory: {}",
                destination.display()
            ));
        }
        fs::remove_dir_all(destination)
            .map_err(|error| format!("remove stale Python output: {error}"))?;
    }
    copy_tree(source, destination).map_err(|error| format!("copy generated Python output: {error}"))
}

fn copy_tree(source: &Path, destination: &Path) -> io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_tree(&source_path, &destination_path)?;
        } else {
            fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}

fn same_tree(left: &Path, right: &Path) -> Result<bool, String> {
    if !right.is_dir() {
        return Ok(false);
    }
    let left_files = relative_files(left)?;
    let right_files = relative_files(right)?;
    if left_files != right_files {
        return Ok(false);
    }
    for relative in left_files {
        if fs::read(left.join(&relative))
            .map_err(|error| format!("read generated output: {error}"))?
            != fs::read(right.join(&relative))
                .map_err(|error| format!("read existing output: {error}"))?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn relative_files(root: &Path) -> Result<BTreeSet<PathBuf>, String> {
    let mut files = BTreeSet::new();
    collect_relative_files(root, root, &mut files)
        .map_err(|error| format!("walk generated output: {error}"))?;
    Ok(files)
}

fn collect_relative_files(
    root: &Path,
    current: &Path,
    files: &mut BTreeSet<PathBuf>,
) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_relative_files(root, &path, files)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .map_err(io::Error::other)?
                .to_owned();
            files.insert(relative);
        }
    }
    Ok(())
}

fn collect_directories(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut directories = vec![root.to_owned()];
    for entry in fs::read_dir(root).map_err(|error| format!("walk generated output: {error}"))? {
        let entry = entry.map_err(|error| format!("walk generated output: {error}"))?;
        if entry.path().is_dir() {
            directories.extend(collect_directories(&entry.path())?);
        }
    }
    Ok(directories)
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    for entry in fs::read_dir(root).map_err(|error| format!("walk generated output: {error}"))? {
        let entry = entry.map_err(|error| format!("walk generated output: {error}"))?;
        if entry.path().is_dir() {
            files.extend(collect_files(&entry.path())?);
        } else {
            files.push(entry.path());
        }
    }
    Ok(files)
}

fn temporary_directory(prefix: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    env::temp_dir().join(format!("{prefix}-{}-{timestamp}", std::process::id()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incompatible_generator_versions_fail_closed() {
        assert!(ensure_versions("1.82.0", EXPECTED_PROTOBUF).is_err());
        assert!(ensure_versions(EXPECTED_GRPCIO_TOOLS, "7.35.0").is_err());
        assert!(ensure_versions(EXPECTED_GRPCIO_TOOLS, EXPECTED_PROTOBUF).is_ok());
    }

    #[test]
    fn schema_root_requires_explicit_rust_authority_marker() {
        let root = temporary_directory("sdk-python-test-authority");
        fs::create_dir_all(root.join("actors/v1")).expect("create actors fixture");
        fs::create_dir_all(root.join("stream/v2")).expect("create stream fixture");
        fs::create_dir_all(root.join("workers/v1")).expect("create workers fixture");
        fs::write(root.join("actors/v1/actors.proto"), b"syntax = \"proto3\";").expect("actors");
        fs::write(root.join("stream/v2/stream.proto"), b"syntax = \"proto3\";").expect("stream");
        fs::write(
            root.join("workers/v1/workers.proto"),
            b"syntax = \"proto3\";",
        )
        .expect("workers");
        fs::write(root.join("actors/v1/actors.fds.bin"), b"descriptor").expect("actors descriptor");
        fs::write(root.join("stream/v2/stream.fds.bin"), b"descriptor").expect("stream descriptor");
        fs::write(root.join("workers/v1/workers.fds.bin"), b"descriptor")
            .expect("workers descriptor");
        assert!(validate_schema_root(&root).is_err());
        let source_hash = format!("{:x}", Sha256::digest(b"syntax = \"proto3\";"));
        let descriptor_hash = format!("{:x}", Sha256::digest(b"descriptor"));
        let marker = format!(
            "{{\"schema\":\"acyclic.sdk.rust-authority.v1\",\"authority\":\"rust\",\"families\":[{{\"source\":\"actors/v1/actors.proto\",\"source_sha256\":\"{source_hash}\",\"descriptor\":\"actors/v1/actors.fds.bin\",\"descriptor_sha256\":\"{descriptor_hash}\"}},{{\"source\":\"stream/v2/stream.proto\",\"source_sha256\":\"{source_hash}\",\"descriptor\":\"stream/v2/stream.fds.bin\",\"descriptor_sha256\":\"{descriptor_hash}\"}},{{\"source\":\"workers/v1/workers.proto\",\"source_sha256\":\"{source_hash}\",\"descriptor\":\"workers/v1/workers.fds.bin\",\"descriptor_sha256\":\"{descriptor_hash}\"}}]}}"
        );
        fs::write(root.join(AUTHORITY_MARKER), marker).expect("marker");
        assert!(validate_schema_root(&root).is_ok());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn schema_root_rejects_content_hash_drift() {
        let root = temporary_directory("sdk-python-test-authority-drift");
        fs::create_dir_all(root.join("actors/v1")).expect("create actors fixture");
        fs::create_dir_all(root.join("stream/v2")).expect("create stream fixture");
        for relative in ["actors/v1/actors.proto", "stream/v2/stream.proto"] {
            fs::write(root.join(relative), b"syntax = \"proto3\";").expect("schema");
        }
        for relative in ["actors/v1/actors.fds.bin", "stream/v2/stream.fds.bin"] {
            fs::write(root.join(relative), b"descriptor").expect("descriptor");
        }
        let marker = r#"{"schema":"acyclic.sdk.rust-authority.v1","authority":"rust","families":[{"source":"actors/v1/actors.proto","source_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","descriptor":"actors/v1/actors.fds.bin","descriptor_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"source":"stream/v2/stream.proto","source_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","descriptor":"stream/v2/stream.fds.bin","descriptor_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}"#;
        fs::write(root.join(AUTHORITY_MARKER), marker).expect("marker");
        assert!(validate_schema_root(&root).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn drift_check_detects_stale_output_without_writing_it() {
        let left = temporary_directory("sdk-python-test-left");
        let right = temporary_directory("sdk-python-test-right");
        fs::create_dir_all(&left).expect("left");
        fs::create_dir_all(&right).expect("right");
        fs::write(left.join("generated.py"), b"one").expect("left file");
        fs::write(right.join("generated.py"), b"two").expect("right file");
        assert!(!same_tree(&left, &right).expect("compare trees"));
        assert_eq!(
            fs::read(right.join("generated.py")).expect("read right"),
            b"two"
        );
        let _ = fs::remove_dir_all(left);
        let _ = fs::remove_dir_all(right);
    }

    #[test]
    fn generated_subtree_replacement_preserves_rust_facade_files() {
        let source = temporary_directory("sdk-python-test-generated-source");
        let package = temporary_directory("sdk-python-test-package");
        fs::create_dir_all(&source).expect("create generated source");
        fs::create_dir_all(package.join("src/acyclic_sdk/generated"))
            .expect("create generated package");
        fs::write(package.join("src/acyclic_sdk/remote.py"), b"rust facade\n")
            .expect("write facade");
        fs::write(package.join("src/acyclic_sdk/py.typed"), b"typed\n")
            .expect("write package marker");
        fs::write(
            package.join("src/acyclic_sdk/generated/stale_pb2.py"),
            b"stale\n",
        )
        .expect("write stale generated file");
        fs::write(source.join("fresh_pb2.py"), b"fresh\n").expect("write fresh generated file");

        replace_output(&source, &package.join("src/acyclic_sdk/generated"))
            .expect("replace generated subtree");

        assert_eq!(
            fs::read(package.join("src/acyclic_sdk/remote.py")).expect("read facade"),
            b"rust facade\n"
        );
        assert_eq!(
            fs::read(package.join("src/acyclic_sdk/py.typed")).expect("read package marker"),
            b"typed\n"
        );
        assert!(
            !package
                .join("src/acyclic_sdk/generated/stale_pb2.py")
                .exists()
        );
        assert_eq!(
            fs::read(package.join("src/acyclic_sdk/generated/fresh_pb2.py"))
                .expect("read fresh generated file"),
            b"fresh\n"
        );

        let _ = fs::remove_dir_all(source);
        let _ = fs::remove_dir_all(package);
    }
}
