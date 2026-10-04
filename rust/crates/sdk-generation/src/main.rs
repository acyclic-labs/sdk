//! Rust-owned SDK generation orchestration.
//!
//! This binary intentionally has a small, explicit interface. It does not
//! know how to generate a language client itself: it writes a request envelope
//! and invokes the family-specific tool, retaining the tool result and exact
//! artifact hashes in a manifest. Missing tools and missing qualification
//! evidence remain "pending"; they are never represented as successful output.

use acyclic_sdk_contract_wire::{FAMILY_VIEWS, explicit_http_family_views};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io::{self, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use tar::Archive;

const GENERATION_SCHEMA: &str = "acyclic.sdk.generation.manifest.v1";
const REQUEST_SCHEMA: &str = "acyclic.sdk.generation.request.v1";
const EVIDENCE_SCHEMA: &str = "acyclic.sdk.qualification.evidence.v1";
const INVENTORY_SCHEMA: &str = "acyclic.sdk.language-inventory.v1";
const OPENAPI_STAGE_RECEIPT_SCHEMA: &str = "acyclic.sdk.openapi.stage-receipt.v1";
const REQUIRED_TOOL_IDS: &[&str] = &[
    "sdk-product-artifacts",
    "sdk-contract-wire",
    "sdk-contract-validation",
    "sdk-openapi-prototype",
    "sdk-examples",
    "sdk-docs-rustdoc",
    "sdk-docs",
    "sdk-language-producers",
    "sdk-python",
    "sdk-typescript",
    "sdk-typescript-rpc-contracts",
];
const OPTIONAL_TOOL_IDS: &[&str] = &[];

// These values are qualification metadata for the remote HTTP projection. The
// orchestration stage does not pretend to be the OpenAPI Generator CLI: the
// Rust projection remains authoritative, while this pin records the OAG
// release used to reproduce the target package in the research lane.
const OPENAPI_GENERATOR_RELEASE: &str = "7.25.0";
const OPENAPI_GENERATOR_COMMIT: &str = "ef964b04480889ef86b56cfae84ade8ad4c91c41";
const OPENAPI_GENERATOR_JAR_SHA256: &str =
    "41ce4f6b07f196676439d710759fa1ced7a08066d06ff1bf314681470289efae";
const OPENAPI_LICENSE_NAME: &str = "Apache-2.0";
const OPENAPI_LICENSE_URL: &str = "https://www.apache.org/licenses/LICENSE-2.0";
const OPENAPI_QUALIFICATION_RECEIPT: &str =
    "research/additional-languages/openapi-targets/receipt.json";
const OPENAPI_ANCHOR_MARKERS: &[&str] = &[
    "generated anchor",
    "$requestPattern =",
    "$bodyAnchor =",
    "$shaAnchor =",
    "$apiAnchor =",
    "request anchor changed",
    "response anchors changed",
    "API client anchor changed",
];

fn openapi_family_names() -> Vec<&'static str> {
    explicit_http_family_views()
        .map(|family| family.name)
        .collect()
}

// These are archived compatibility identities, not generated output hashes.
// A source checkout must not be able to change a baseline and then bless the
// changed bytes as a new compatibility oracle. Keep these values in this
// runner until the archive has a separately versioned manifest.
const IMMUTABLE_BASELINES: &[(&str, &str)] = &[
    (
        "actors",
        "0515dc7e3f38a5648f85ee52f5bda7e639cc31cf83179a208bb5abbd398a04bf",
    ),
    (
        "objects",
        "1968e12e89d38f7076b9aee559c815748858c156401a56ba53e615def49fe86f",
    ),
    (
        "filesystem",
        "105e153060d229569836982527c91bd56a69891691007215fbc599115eca2093",
    ),
    (
        "harness",
        "b1d721a657f40f652a560769ff440b7a1ca44739765fd3114269b727de5474e4",
    ),
    (
        "machines",
        "68feb507148fbf798a3e05236a4d93d36d216c260db0a6a339db5919c630e758",
    ),
    (
        "inference",
        "21c35707beb7d3aa8c87f63ceb129083ad092010a64d9b9e82924a0f5661bf15",
    ),
    (
        "protocol",
        "ce697a9dede342fa869397ca984dd148d6c6375d33a0a633483615a506398660",
    ),
    (
        "stream",
        "1d311dd12a56de4f04923e4144071c507c6b59b1789955fd8629d09c990abd0c",
    ),
    (
        "workers",
        "851b6cd37b8cb4baa6d3a111efdad655b89936b2e1057ecb74e62825715bd7d8",
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Generate,
    Check,
    Drift,
    Qualify,
    QualifyEmbedded,
    Inventory,
}

#[derive(Debug)]
struct Args {
    operation: Operation,
    source_root: PathBuf,
    output: PathBuf,
    receipt: Option<PathBuf>,
    evidence: Option<PathBuf>,
    package_root: Option<PathBuf>,
    platform_receipt: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceIdentity {
    revision: String,
    digest: String,
    dirty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ToolResult {
    id: String,
    status: String,
    required: bool,
    command: Vec<String>,
    request: String,
    stdout_sha256: Option<String>,
    stderr_sha256: Option<String>,
    exit_code: Option<i32>,
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Artifact {
    path: String,
    sha256: String,
    bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LanguageStatus {
    id: String,
    classification: String,
    excluded: bool,
    registry: String,
    package: Option<String>,
    remote: String,
    embedded: String,
    docs: String,
    snippets: String,
    install: String,
    evidence: Vec<String>,
    outstanding: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    schema: String,
    operation: String,
    status: String,
    source: SourceIdentity,
    #[serde(default)]
    authoritative_source: Option<SourceIdentity>,
    generator: GeneratorIdentity,
    tools: Vec<ToolResult>,
    artifacts: Vec<Artifact>,
    /// Digest of the canonical path+content digest list above. Keeping this
    /// in the manifest lets consumers bind evidence to the exact artifact
    /// set instead of recomputing an unadvertised list.
    #[serde(default)]
    artifact_digest: Option<String>,
    languages: Vec<LanguageStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeneratorIdentity {
    name: String,
    version: String,
}

#[derive(Debug, Clone)]
struct ToolSpec {
    id: &'static str,
    required: bool,
    manifest: Option<PathBuf>,
    script: Option<PathBuf>,
}

/// A producer recipe is an explicit, source-bound command supplied by the
/// language owner.  The Rust entrypoint only expands the small placeholder
/// vocabulary below; it never evaluates a shell string.  Recipes are
/// optional while a target is being brought up, but a target with a recipe
/// must produce a staged artifact or the stage fails closed.
#[derive(Debug, Clone)]
struct ProducerRecipe {
    program: String,
    args: Vec<String>,
    output: String,
}

#[derive(Debug, Clone, Serialize)]
struct RequestEnvelope {
    schema: &'static str,
    operation: String,
    tool: String,
    source_root: String,
    output: String,
    source: SourceIdentity,
    contract_scope: &'static str,
    contract_inputs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generated_package_roots: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct Evidence {
    schema: String,
    language: String,
    source_revision: String,
    contract_digest: String,
    artifact_digest: String,
    remote: CapabilityEvidence,
    embedded: CapabilityEvidence,
    docs: CapabilityEvidence,
    snippets: CapabilityEvidence,
    install: CapabilityEvidence,
}

#[derive(Debug, Clone)]
struct EvidenceExpectations {
    source_revision: String,
    contract_digest: String,
    artifact_digest: String,
    artifacts: Vec<Artifact>,
}

#[derive(Debug, Clone, Deserialize)]
struct CapabilityEvidence {
    status: String,
    tests: Vec<String>,
    #[serde(default)]
    scope: Option<String>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("sdk-generation: {error}");
        std::process::exit(error.exit_code());
    }
}

#[derive(Debug)]
struct CliError {
    message: String,
    code: i32,
}

impl CliError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: 1,
        }
    }
    fn pending(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: 2,
        }
    }
    fn exit_code(&self) -> i32 {
        self.code
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl From<io::Error> for CliError {
    fn from(error: io::Error) -> Self {
        Self::new(error.to_string())
    }
}
impl From<serde_json::Error> for CliError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(error.to_string())
    }
}

fn run() -> Result<(), CliError> {
    let args = parse_args()?;
    let source_root = canonical_existing_directory(&args.source_root, "source root")?;
    let output = absolute_path(&args.output)?;
    validate_output_path(&output)?;
    fs::create_dir_all(&output)?;
    match args.operation {
        Operation::Generate => generate(&source_root, &output),
        Operation::Check => check(&source_root, &output),
        Operation::Drift => drift(&source_root, &output),
        Operation::Qualify => qualify(&source_root, &output),
        Operation::QualifyEmbedded => qualify_embedded(
            &source_root,
            &output,
            args.receipt
                .as_deref()
                .ok_or_else(|| CliError::new("qualify-embedded requires --receipt PATH"))?,
            args.evidence
                .as_deref()
                .ok_or_else(|| CliError::new("qualify-embedded requires --evidence PATH"))?,
            args.package_root.as_deref().ok_or_else(|| {
                CliError::new("qualify-embedded requires --package-root PATH")
            })?,
            args.platform_receipt.as_deref(),
        ),
        Operation::Inventory => inventory_command(&source_root, &output),
    }
}

fn parse_args() -> Result<Args, CliError> {
    let mut values = env::args().skip(1);
    let operation = match values.next().as_deref() {
        Some("generate") => Operation::Generate,
        Some("check") => Operation::Check,
        Some("drift") => Operation::Drift,
        Some("qualify") => Operation::Qualify,
        Some("qualify-embedded") => Operation::QualifyEmbedded,
        Some("inventory") => Operation::Inventory,
        Some(other) => {
            return Err(CliError::new(format!(
                "unknown operation {other}; expected generate, check, drift, qualify, qualify-embedded, or inventory"
            )));
        }
        None => {
            return Err(CliError::new(
                "usage: sdk-generation <generate|check|drift|qualify|qualify-embedded|inventory> --source-root PATH --output PATH [--receipt PATH --evidence PATH --package-root PATH --platform-receipt PATH]",
            ));
        }
    };
    let mut source_root = None;
    let mut output = None;
    let mut receipt = None;
    let mut evidence = None;
    let mut package_root = None;
    let mut platform_receipt = None;
    while let Some(flag) = values.next() {
        match flag.as_str() {
            "--source-root" => {
                source_root =
                    Some(PathBuf::from(values.next().ok_or_else(|| {
                        CliError::new("--source-root requires a path")
                    })?))
            }
            "--output" => {
                output = Some(PathBuf::from(
                    values
                        .next()
                        .ok_or_else(|| CliError::new("--output requires a path"))?,
                ))
            }
            "--receipt" => {
                receipt = Some(PathBuf::from(
                    values
                        .next()
                        .ok_or_else(|| CliError::new("--receipt requires a path"))?,
                ))
            }
            "--evidence" => {
                evidence = Some(PathBuf::from(
                    values
                        .next()
                        .ok_or_else(|| CliError::new("--evidence requires a path"))?,
                ))
            }
            "--package-root" => {
                package_root = Some(PathBuf::from(
                    values
                        .next()
                        .ok_or_else(|| CliError::new("--package-root requires a path"))?,
                ))
            }
            "--platform-receipt" => {
                platform_receipt = Some(PathBuf::from(
                    values
                        .next()
                        .ok_or_else(|| CliError::new("--platform-receipt requires a path"))?,
                ))
            }
            "--help" | "-h" => {
                return Err(CliError::new(
                    "usage: sdk-generation <generate|check|drift|qualify|qualify-embedded|inventory> --source-root PATH --output PATH [--receipt PATH --evidence PATH --package-root PATH --platform-receipt PATH]",
                ));
            }
            other => return Err(CliError::new(format!("unknown argument {other}"))),
        }
    }
    Ok(Args {
        operation,
        source_root: source_root.ok_or_else(|| CliError::new("--source-root is required"))?,
        output: output.ok_or_else(|| CliError::new("--output is required"))?,
        receipt,
        evidence,
        package_root,
        platform_receipt,
    })
}

fn generate(source_root: &Path, output: &Path) -> Result<(), CliError> {
    let source = source_identity(source_root)?;
    let authoritative_source = authoritative_source_identity(source_root)?;
    let tools = run_tools(source_root, output, &source, Operation::Generate)?;
    let source_after_tools = source_identity(source_root)?;
    ensure_source_identity_unchanged(&source, &source_after_tools, "generation")?;
    let authoritative_after_tools = authoritative_source_identity(source_root)?;
    if authoritative_after_tools.digest != authoritative_source.digest
        || authoritative_after_tools.revision != authoritative_source.revision
    {
        return Err(CliError::new(
            "authoritative source changed during generation; discard mixed artifacts and rerun from a frozen checkout",
        ));
    }
    verify_revision_artifacts(source_root, output, &source.revision)?;
    let languages = language_inventory(source_root, output, &source.revision, None)?;
    let artifacts = collect_artifacts(output)?;
    let artifact_digest = artifact_digest(&artifacts);
    let failed = tools.iter().any(|tool| tool.status == "failed");
    let pending = tools.iter().any(|tool| tool.status == "pending");
    let manifest = Manifest {
        schema: GENERATION_SCHEMA.into(),
        operation: "generate".into(),
        status: if failed {
            "failed".into()
        } else if pending {
            "pending".into()
        } else {
            "generated".into()
        },
        source,
        authoritative_source: Some(authoritative_source),
        generator: GeneratorIdentity {
            name: "acyclic-sdk-generation".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
        tools,
        artifacts,
        artifact_digest: Some(artifact_digest),
        languages,
    };
    write_json(&output.join("sdk-generation-manifest.json"), &manifest)?;
    print_json(&manifest)?;
    let failed = manifest
        .tools
        .iter()
        .filter(|tool| tool.status == "failed")
        .map(|tool| tool.id.as_str())
        .collect::<Vec<_>>();
    if !failed.is_empty() {
        return Err(CliError::new(format!(
            "generation failed in tool stages: {}",
            failed.join(", ")
        )));
    }
    let pending = manifest
        .tools
        .iter()
        .filter(|tool| tool.status == "pending")
        .map(|tool| tool.id.as_str())
        .collect::<Vec<_>>();
    if !pending.is_empty() {
        return Err(CliError::pending(format!(
            "generation remains pending in tool stages: {}",
            pending.join(", ")
        )));
    }
    Ok(())
}

fn check(source_root: &Path, output: &Path) -> Result<(), CliError> {
    let manifest_path = output.join("sdk-generation-manifest.json");
    let manifest: Manifest = read_json(&manifest_path)
        .map_err(|error| CliError::new(format!("cannot read generation manifest: {error}")))?;
    if manifest.schema != GENERATION_SCHEMA {
        return Err(CliError::new("unsupported generation manifest schema"));
    }
    verify_generator_identity(&manifest.generator)?;
    if manifest.status != "generated" {
        return Err(CliError::pending(format!(
            "generation manifest is {}, so drift checking is unavailable",
            manifest.status
        )));
    }
    verify_required_tools(&manifest.tools, source_root, output)?;
    let expected_artifact_digest = artifact_digest(&manifest.artifacts);
    if manifest.artifact_digest.as_deref() != Some(expected_artifact_digest.as_str()) {
        return Err(CliError::new(
            "generation manifest artifact digest does not match its artifact set",
        ));
    }
    let source = source_identity(source_root)?;
    let authoritative_source = authoritative_source_identity(source_root)?;
    if source.revision != manifest.source.revision
        || source.digest != manifest.source.digest
        || source.dirty != manifest.source.dirty
    {
        return Err(CliError::new(
            "source identity differs from the generation manifest",
        ));
    }
    let expected_authoritative = manifest
        .authoritative_source
        .as_ref()
        .unwrap_or(&manifest.source);
    if authoritative_source.revision != expected_authoritative.revision
        || authoritative_source.digest != expected_authoritative.digest
        || authoritative_source.dirty != expected_authoritative.dirty
    {
        return Err(CliError::new(
            "authoritative source revision or digest differs from the generation manifest",
        ));
    }
    verify_artifacts(output, &manifest.artifacts)?;
    verify_authority_manifest(source_root, output)?;
    verify_revision_artifacts(source_root, output, &source.revision)?;
    let check_root = output.join(".check");
    if check_root.exists() {
        reject_symlink(&check_root)?;
        fs::remove_dir_all(&check_root)?;
    }
    fs::create_dir_all(&check_root)?;
    let fresh_tools = run_tools(source_root, &check_root, &source, Operation::Check)?;
    let source_after_tools = source_identity(source_root)?;
    ensure_source_identity_unchanged(&source, &source_after_tools, "drift check")?;
    let authoritative_after_tools = authoritative_source_identity(source_root)?;
    if authoritative_after_tools.digest != authoritative_source.digest
        || authoritative_after_tools.revision != authoritative_source.revision
    {
        let _ = fs::remove_dir_all(&check_root);
        return Err(CliError::new(
            "authoritative source changed during drift check; retry from a frozen checkout",
        ));
    }
    verify_required_tools(&fresh_tools, source_root, &check_root)?;
    for result in &fresh_tools {
        if result.status == "failed" {
            return Err(CliError::new(format!(
                "tool {} failed during drift check",
                result.id
            )));
        }
    }
    compare_fresh_artifacts(output, &check_root, &manifest.artifacts)?;
    let pending_tools = fresh_tools
        .iter()
        .filter(|tool| tool.status == "pending")
        .map(|tool| tool.id.clone())
        .collect::<Vec<_>>();
    let _ = fs::remove_dir_all(&check_root);
    let report = json!({
        "schema": "acyclic.sdk.generation.check.v1",
        "source": source,
        "manifest": manifest_path.to_string_lossy(),
        "status": if pending_tools.is_empty() { "passed" } else { "pending" },
        "pending_tools": pending_tools,
    });
    print_json(&report)?;
    if !report["pending_tools"]
        .as_array()
        .is_some_and(|items| items.is_empty())
    {
        return Err(CliError::pending(
            "generation check remains pending; required generators are not implemented in this checkout",
        ));
    }
    Ok(())
}

/// Verify the already generated manifest and its on-disk artifacts without
/// invoking downstream generators. This is the cheap PR gate; `check` remains
/// the deterministic regeneration gate and `qualify` remains the evidence
/// gate.
fn drift(source_root: &Path, output: &Path) -> Result<(), CliError> {
    let manifest_path = output.join("sdk-generation-manifest.json");
    let manifest: Manifest = read_json(&manifest_path)
        .map_err(|error| CliError::new(format!("cannot read generation manifest: {error}")))?;
    if manifest.schema != GENERATION_SCHEMA {
        return Err(CliError::new("unsupported generation manifest schema"));
    }
    verify_generator_identity(&manifest.generator)?;
    if manifest.status != "generated" {
        return Err(CliError::pending(format!(
            "generation manifest is {}, so drift checking is unavailable",
            manifest.status
        )));
    }
    verify_required_tools(&manifest.tools, source_root, output)?;

    let expected_artifact_digest = artifact_digest(&manifest.artifacts);
    if manifest.artifact_digest.as_deref() != Some(expected_artifact_digest.as_str()) {
        return Err(CliError::new(
            "generation manifest artifact digest does not match its artifact set",
        ));
    }

    let source = source_identity(source_root)?;
    if source.revision != manifest.source.revision
        || source.digest != manifest.source.digest
        || source.dirty != manifest.source.dirty
    {
        return Err(CliError::new(
            "source identity differs from the generation manifest",
        ));
    }
    let authoritative_source = authoritative_source_identity(source_root)?;
    let expected_authoritative = manifest
        .authoritative_source
        .as_ref()
        .unwrap_or(&manifest.source);
    if authoritative_source.revision != expected_authoritative.revision
        || authoritative_source.digest != expected_authoritative.digest
        || authoritative_source.dirty != expected_authoritative.dirty
    {
        return Err(CliError::new(
            "authoritative source identity differs from the generation manifest",
        ));
    }
    verify_artifacts(output, &manifest.artifacts)?;
    verify_authority_manifest(source_root, output)?;
    verify_revision_artifacts(source_root, output, &source.revision)?;
    let report = json!({
        "schema": "acyclic.sdk.generation.drift.v1",
        "source": source,
        "manifest": manifest_path.to_string_lossy(),
        "status": "passed",
        "checked_artifacts": manifest.artifacts.len(),
        "downstream_generators": "not_run",
    });
    write_json_value(&output.join("sdk-generation-drift.json"), &report)?;
    print_json(&report)
}

fn qualify(source_root: &Path, output: &Path) -> Result<(), CliError> {
    check(source_root, output)?;
    let source = source_identity(source_root)?;
    let expectations = evidence_expectations(source_root, output)?;
    let statuses =
        language_inventory(source_root, output, &source.revision, expectations.as_ref())?;
    let qualified = statuses
        .iter()
        .filter(|item| {
            !item.excluded
                && item.remote == "qualified"
                && matches!(item.embedded.as_str(), "qualified" | "excluded")
                && item.docs == "qualified"
                && item.snippets == "qualified"
                && item.install == "qualified"
        })
        .count();
    let pending = statuses
        .iter()
        .filter(|item| !item.excluded)
        .filter(|item| {
            item.outstanding
                .iter()
                .any(|task| task.starts_with("pending:"))
        })
        .count();
    let report = json!({
        "schema": "acyclic.sdk.qualification.v1",
        "source": source,
        "languages": statuses,
        "qualified_languages": qualified,
        "pending_languages": pending,
        "active_languages": statuses.iter().filter(|item| !item.excluded).count(),
        "status": if pending == 0 && qualified == statuses.iter().filter(|item| !item.excluded).count() { "qualified" } else { "pending" },
    });
    write_json_value(&output.join("qualification.json"), &report)?;
    print_json(&report)?;
    if pending != 0 || qualified != statuses.iter().filter(|item| !item.excluded).count() {
        return Err(CliError::pending(
            "qualification remains pending; inspect qualification.json for outstanding evidence",
        ));
    }
    Ok(())
}

/// Qualify the Rust-owned embedded boundary without routing it through the
/// remote RPC inventory. Embedded consumers have a different contract: the
/// C ABI layout, ownership/lifetime rules, cancellation wakeup and package
/// loading are the capabilities under test. The receipt and unified evidence
/// still use the same Rust source and canonical artifact digest bindings as
/// every other generation lane.
fn qualify_embedded(
    source_root: &Path,
    output: &Path,
    receipt_path: &Path,
    evidence_path: &Path,
    package_root: &Path,
    platform_receipt_path: Option<&Path>,
) -> Result<(), CliError> {
    let source = canonical_existing_directory(source_root, "source root")?;
    let output = absolute_path(output)?;
    let package = canonical_existing_directory(package_root, "embedded package root")?;
    if package.starts_with(&source) || output.starts_with(&source) {
        return Err(CliError::new(
            "embedded qualification output and package must be outside the Rust source root",
        ));
    }
    let receipt: Value = read_json(receipt_path)
        .map_err(|error| CliError::new(format!("cannot read embedded ABI receipt: {error}")))?;
    let evidence: Value = read_json(evidence_path).map_err(|error| {
        CliError::new(format!("cannot read unified embedded evidence: {error}"))
    })?;
    let receipt_schema = receipt
        .get("schema")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("embedded receipt is missing schema"))?;
    let receipt_status = receipt
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("embedded receipt is missing status"))?;
    if receipt_schema != "acyclic.sdk.embedded.release-abi.v1"
        || receipt_status != "qualified-local-release-package"
    {
        return Err(CliError::new("embedded ABI receipt schema or status is invalid"));
    }
    let source_record = receipt
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| CliError::new("embedded ABI receipt source binding is missing"))?;
    let source_revision = source_record
        .get("revision")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("embedded ABI receipt source revision is missing"))?;
    if !is_commit_revision(source_revision) {
        return Err(CliError::new("embedded ABI source revision is not immutable"));
    }
    let source_digest = source_record
        .get("source_digest")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("embedded ABI source digest is missing"))?;
    if source_digest.len() != 64 || !source_digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CliError::new("embedded ABI source digest is invalid"));
    }
    let git_root = command_stdout(&source, "git", &["rev-parse", "--show-toplevel"])
        .and_then(|root| canonical_existing_directory(Path::new(&root), "Git source root"))?;
    if git_root != source {
        return Err(CliError::new(format!(
            "Git source root does not match requested embedded source root: {} != {}",
            git_root.display(),
            source.display()
        )));
    }
    let current_revision = command_stdout(&source, "git", &["rev-parse", "HEAD"])?;
    if current_revision != source_revision {
        return Err(CliError::new(format!(
            "embedded source revision is stale: receipt {source_revision}, checkout {current_revision}"
        )));
    }
    let dirty = command_stdout(&source, "git", &["status", "--porcelain"])?;
    if !dirty.is_empty() {
        return Err(CliError::new(
            "embedded source checkout is dirty; qualification requires a clean Rust source root",
        ));
    }
    let source_inputs = source_record
        .get("source_inputs")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::new("embedded ABI source inputs are missing"))?;
    let mut source_digest_lines = Vec::with_capacity(source_inputs.len());
    for input in source_inputs {
        let relative = input
            .as_str()
            .ok_or_else(|| CliError::new("embedded ABI source input is not a string"))?;
        if !is_portable_relative(relative) {
            return Err(CliError::new(format!(
                "embedded ABI source input is not portable: {relative}"
            )));
        }
        let path = source.join(relative);
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            CliError::new(format!("embedded ABI source input is missing: {relative}: {error}"))
        })?;
        if !metadata.file_type().is_file() {
            return Err(CliError::new(format!(
                "embedded ABI source input is not a file: {relative}"
            )));
        }
        let digest = hash_bytes(&fs::read(&path)?).trim_start_matches("sha256:").to_owned();
        source_digest_lines.push(format!("{relative} {digest}"));
    }
    let observed_source_digest = hash_bytes(source_digest_lines.join("\n").as_bytes())
        .trim_start_matches("sha256:")
        .to_owned();
    if observed_source_digest != source_digest.to_ascii_lowercase() {
        return Err(CliError::new(format!(
            "embedded source digest mismatch: receipt {source_digest}, checkout {observed_source_digest}"
        )));
    }
    let consumers = receipt
        .get("foreign_consumers")
        .and_then(Value::as_object)
        .ok_or_else(|| CliError::new("embedded consumer receipts are missing"))?;
    for name in ["c_consumer", "python_ctypes_consumer", "cpp_cross_thread_consumer"] {
        if consumers
            .get(name)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str)
            != Some("passed")
        {
            return Err(CliError::new(format!("embedded consumer receipt did not pass: {name}")));
        }
    }
    let actual_consumers = receipt
        .get("consumer_receipts")
        .and_then(Value::as_object)
        .ok_or_else(|| CliError::new("actual embedded consumer receipts are missing"))?;
    for name in ["c", "python", "cpp"] {
        let consumer = actual_consumers
            .get(name)
            .and_then(Value::as_object)
            .ok_or_else(|| CliError::new(format!("actual embedded consumer receipt is missing: {name}")))?;
        validate_embedded_consumer_receipt(
            &source,
            &package,
            source_revision,
            name,
            consumer,
            "bin/acyclic_sdk_embedded_prototype.dll",
        )?;
    }
    if receipt
        .get("reproducibility")
        .and_then(|value| value.get("artifact_hashes_equal"))
        .and_then(Value::as_bool)
        != Some(true)
    {
        return Err(CliError::new("embedded ABI reproducibility evidence is missing"));
    }
    let artifacts = receipt
        .get("artifacts")
        .and_then(Value::as_object)
        .ok_or_else(|| CliError::new("embedded ABI artifact hashes are missing"))?;
    let mut canonical = String::new();
    let mut artifact_count = 0usize;
    for (path, expected) in artifacts {
        if !is_portable_relative(path) {
            return Err(CliError::new(format!("embedded artifact path is not portable: {path}")));
        }
        let expected = expected
            .as_str()
            .ok_or_else(|| CliError::new(format!("embedded artifact hash is invalid: {path}")))?;
        if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(CliError::new(format!("embedded artifact hash is invalid: {path}")));
        }
        let artifact_path = package.join(path);
        let metadata = fs::symlink_metadata(&artifact_path).map_err(|error| {
            CliError::new(format!("embedded artifact is missing: {path}: {error}"))
        })?;
        if !metadata.file_type().is_file() {
            return Err(CliError::new(format!("embedded artifact is not a file: {path}")));
        }
        let bytes = fs::read(&artifact_path)?;
        let actual = hash_bytes(&bytes);
        if actual != format!("sha256:{}", expected.to_ascii_lowercase()) {
            return Err(CliError::new(format!("embedded artifact hash mismatch: {path}")));
        }
        canonical.push_str(path);
        canonical.push('\0');
        canonical.push_str(&actual);
        canonical.push('\0');
        artifact_count += 1;
    }
    // JSON object iteration is not an ordering contract. Rebuild the digest
    // from sorted path/hash pairs exactly as the generation manifest does.
    let mut pairs = artifacts
        .iter()
        .map(|(path, hash)| {
            Ok((
                path.clone(),
                hash
                    .as_str()
                    .ok_or_else(|| CliError::new("embedded artifact hash is not a string"))?
                    .to_ascii_lowercase(),
            ))
        })
        .collect::<Result<Vec<_>, CliError>>()?;
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    let mut sorted_canonical = String::new();
    for (path, hash) in pairs {
        sorted_canonical.push_str(&path);
        sorted_canonical.push('\0');
        sorted_canonical.push_str("sha256:");
        sorted_canonical.push_str(&hash);
        sorted_canonical.push('\0');
    }
    let expected_artifact_digest = hash_bytes(sorted_canonical.as_bytes());
    let evidence_contract = format!("sha256:{source_digest}");
    let evidence_ok = evidence.get("schema").and_then(Value::as_str) == Some(EVIDENCE_SCHEMA)
        && evidence.get("language").and_then(Value::as_str) == Some("cpp")
        && evidence.get("source_revision").and_then(Value::as_str) == Some(source_revision)
        && evidence.get("contract_digest").and_then(Value::as_str) == Some(evidence_contract.as_str())
        && evidence.get("artifact_digest").and_then(Value::as_str) == Some(expected_artifact_digest.as_str())
        && evidence
            .get("embedded")
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str)
            == Some("qualified");
    if !evidence_ok {
        return Err(CliError::new(
            format!(
                "unified embedded evidence is not bound to the Rust source and artifact set (schema={}, language={}, revision={}, contract={}, artifact={}, expected_artifact={})",
                evidence.get("schema").and_then(Value::as_str).unwrap_or("missing"),
                evidence.get("language").and_then(Value::as_str).unwrap_or("missing"),
                evidence.get("source_revision").and_then(Value::as_str).unwrap_or("missing"),
                evidence.get("contract_digest").and_then(Value::as_str).unwrap_or("missing"),
                evidence.get("artifact_digest").and_then(Value::as_str).unwrap_or("missing"),
                expected_artifact_digest,
            ),
        ));
    }
    let platform_runtime_artifact = if let Some(platform_receipt_path) = platform_receipt_path {
        Some(validate_embedded_platform_receipt(
            &source,
            &package,
            source_revision,
            source_digest,
            source_inputs,
            platform_receipt_path,
        )?)
    } else {
        None
    };
    if platform_runtime_artifact.is_none() {
        let runtime_path = package.join("bin/acyclic_sdk_embedded_prototype.dll");
        let runtime = fs::read(&runtime_path).map_err(|error| {
            CliError::new(format!("embedded runtime artifact is missing: {error}"))
        })?;
        if runtime.len() < 64
            || &runtime[0..2] != b"MZ"
            || runtime
                .get(0x3c..0x40)
                .map(|bytes| u32::from_le_bytes(bytes.try_into().expect("PE offset")) as usize)
                .and_then(|offset| runtime.get(offset..offset + 4))
                != Some(b"PE\0\0")
            || !runtime
                .windows(b"acyclic_embedded_abi_version".len())
                .any(|window| window == b"acyclic_embedded_abi_version")
        {
            return Err(CliError::new(
                "embedded runtime does not have a valid PE signature and ABI export",
            ));
        }
    }
    let report = json!({
        "schema": "acyclic.sdk.embedded.qualification.v1",
        "status": "passed",
        "source_revision": source_revision,
        "contract_digest": format!("sha256:{source_digest}"),
        "artifact_digest": expected_artifact_digest,
        "artifact_count": artifact_count,
        "runtime_artifact": platform_runtime_artifact
            .unwrap_or_else(|| "bin/acyclic_sdk_embedded_prototype.dll".to_owned()),
        "consumers": ["c", "python-ctypes", "cpp"],
        "embedded_scope": ["layout", "ownership", "lifetime", "cancellation", "cross-thread wakeup", "package loading"],
        "pe": { "mz": true, "pe": true, "export": "acyclic_embedded_abi_version" },
    });
    write_json_value(&output.join("embedded-qualification.json"), &report)?;
    print_json(&report)
}

fn validate_embedded_platform_receipt(
    source_root: &Path,
    package_root: &Path,
    source_revision: &str,
    source_digest: &str,
    expected_source_inputs: &[Value],
    receipt_path: &Path,
) -> Result<String, CliError> {
    let receipt: Value = read_json(receipt_path).map_err(|error| {
        CliError::new(format!("cannot read embedded platform receipt: {error}"))
    })?;
    if receipt.get("schema").and_then(Value::as_str)
        != Some("acyclic.sdk.embedded.platform-package.v1")
        || receipt.get("status").and_then(Value::as_str) != Some("passed")
    {
        return Err(CliError::new("embedded platform receipt schema or status is invalid"));
    }
    if receipt.get("source_revision").and_then(Value::as_str) != Some(source_revision) {
        return Err(CliError::new("embedded platform receipt source revision is stale"));
    }
    let platform_digest = receipt
        .get("source_digest")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("embedded platform receipt source digest is missing"))?;
    if platform_digest != format!("sha256:{source_digest}") {
        return Err(CliError::new("embedded platform receipt source digest is stale"));
    }
    let platform_inputs = receipt
        .get("source_inputs")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::new("embedded platform receipt source inputs are missing"))?;
    if platform_inputs != expected_source_inputs {
        return Err(CliError::new(
            "embedded platform receipt source inputs differ from the release receipt",
        ));
    }
    let runtime_artifact = receipt
        .get("runtime_artifact")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("embedded platform receipt runtime artifact is missing"))?;
    if !is_portable_relative(runtime_artifact)
        || !(runtime_artifact.ends_with(".dll")
            || runtime_artifact.ends_with(".so")
            || runtime_artifact.ends_with(".dylib"))
    {
        return Err(CliError::new(
            "embedded platform runtime artifact path is invalid",
        ));
    }
    let artifacts = receipt
        .get("artifacts")
        .and_then(Value::as_object)
        .filter(|artifacts| !artifacts.is_empty())
        .ok_or_else(|| CliError::new("embedded platform receipt artifact hashes are missing"))?;
    let mut package_files = Vec::new();
    collect_output_files(package_root, package_root, &mut package_files)?;
    let actual_files = package_files.into_iter().collect::<BTreeSet<_>>();
    let expected_files = artifacts.keys().cloned().collect::<BTreeSet<_>>();
    if actual_files != expected_files {
        return Err(CliError::new(
            "embedded platform receipt artifact set differs from installed prefix",
        ));
    }
    for (relative, expected) in artifacts {
        if !is_portable_relative(relative) {
            return Err(CliError::new(format!(
                "embedded platform artifact path is not portable: {relative}"
            )));
        }
        let expected = expected
            .as_str()
            .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or_else(|| CliError::new(format!("embedded platform artifact hash is invalid: {relative}")))?;
        let artifact = package_root.join(relative);
        let metadata = fs::symlink_metadata(&artifact).map_err(|error| {
            CliError::new(format!("embedded platform artifact is missing: {relative}: {error}"))
        })?;
        if !metadata.file_type().is_file()
            || hash_bytes(&fs::read(&artifact)?) != format!("sha256:{}", expected.to_ascii_lowercase())
        {
            return Err(CliError::new(format!(
                "embedded platform artifact hash mismatch: {relative}"
            )));
        }
    }
    let runtime = package_root.join(runtime_artifact);
    let runtime_bytes = fs::read(&runtime).map_err(|error| {
        CliError::new(format!("embedded platform runtime artifact is missing: {error}"))
    })?;
    let valid_format = if runtime_artifact.ends_with(".dll") {
        runtime_bytes.len() >= 64
            && runtime_bytes.starts_with(b"MZ")
            && runtime_bytes
                .get(0x3c..0x40)
                .and_then(|bytes| bytes.try_into().ok())
                .map(|bytes: [u8; 4]| u32::from_le_bytes(bytes) as usize)
                .and_then(|offset| runtime_bytes.get(offset..offset + 4))
                == Some(b"PE\0\0")
    } else if runtime_artifact.ends_with(".so") {
        runtime_bytes.starts_with(b"\x7fELF")
    } else {
        matches!(
            runtime_bytes.get(0..4),
            Some(b"\xfe\xed\xfa\xce")
                | Some(b"\xce\xfa\xed\xfe")
                | Some(b"\xfe\xed\xfa\xcf")
                | Some(b"\xcf\xfa\xed\xfe")
                | Some(b"\xca\xfe\xba\xbe")
                | Some(b"\xbe\xba\xfe\xca")
        )
    };
    if !valid_format
        || !runtime_bytes
            .windows(b"acyclic_embedded_abi_version".len())
            .any(|window| window == b"acyclic_embedded_abi_version")
    {
        return Err(CliError::new(
            "embedded platform runtime has an invalid binary format or ABI export",
        ));
    }
    let consumers = receipt
        .get("consumers")
        .and_then(Value::as_object)
        .ok_or_else(|| CliError::new("embedded platform consumer receipts are missing"))?;
    for name in ["c", "python", "cpp"] {
        let consumer = consumers
            .get(name)
            .and_then(Value::as_object)
            .ok_or_else(|| CliError::new(format!("embedded platform consumer receipt is missing: {name}")))?;
        validate_embedded_consumer_receipt(
            source_root,
            package_root,
            source_revision,
            name,
            consumer,
            runtime_artifact,
        )?;
    }
    for field in ["ctest", "clean_prefix"] {
        if receipt.get(field).and_then(Value::as_str) != Some("passed") {
            return Err(CliError::new(format!(
                "embedded platform receipt did not pass {field}"
            )));
        }
    }
    Ok(runtime_artifact.to_owned())
}

/// Validate the producer's actual ABI consumer invocation against the bytes
/// used for qualification. The descriptive `checks` list cannot certify a
/// consumer by itself: the source program, immutable revision, invoked exit
/// status, and loaded ABI artifact must all be bound and hashed.
fn validate_embedded_consumer_receipt(
    source_root: &Path,
    package_root: &Path,
    source_revision: &str,
    name: &str,
    consumer: &serde_json::Map<String, Value>,
    runtime_artifact: &str,
) -> Result<(), CliError> {
    if consumer.get("status").and_then(Value::as_str) != Some("passed")
        || consumer.get("scope").and_then(Value::as_str) != Some("embedded-native-abi")
        || consumer.get("invoked").and_then(Value::as_bool) != Some(true)
        || consumer.get("exit_code").and_then(Value::as_i64) != Some(0)
    {
        return Err(CliError::new(format!(
            "actual embedded consumer receipt did not record an invoked exit-0 ABI scenario: {name}"
        )));
    }
    if consumer.get("source_revision").and_then(Value::as_str) != Some(source_revision) {
        return Err(CliError::new(format!(
            "embedded consumer source revision is stale: {name}"
        )));
    }
    let source_path = consumer
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("embedded consumer source is missing: {name}")))?;
    if !is_portable_relative(source_path) {
        return Err(CliError::new(format!(
            "embedded consumer source path is not portable: {source_path}"
        )));
    }
    let source_sha256 = consumer
        .get("source_sha256")
        .and_then(Value::as_str)
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| CliError::new(format!("embedded consumer source hash is invalid: {name}")))?;
    let source_file = source_root.join(source_path);
    let source_metadata = fs::symlink_metadata(&source_file).map_err(|error| {
        CliError::new(format!("embedded consumer source is missing: {source_path}: {error}"))
    })?;
    if !source_metadata.file_type().is_file() {
        return Err(CliError::new(format!(
            "embedded consumer source is not a regular file: {source_path}"
        )));
    }
    let source_root_canonical = fs::canonicalize(source_root)?;
    let source_file_canonical = fs::canonicalize(&source_file)?;
    if !source_file_canonical.starts_with(&source_root_canonical) {
        return Err(CliError::new(format!(
            "embedded consumer source escapes the Rust checkout: {source_path}"
        )));
    }
    let actual_source_sha256 = hash_bytes(&fs::read(&source_file)?);
    if actual_source_sha256 != format!("sha256:{}", source_sha256.to_ascii_lowercase()) {
        return Err(CliError::new(format!(
            "embedded consumer source hash mismatch: {name}"
        )));
    }

    let artifact_path = consumer
        .get("package_artifact")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("embedded consumer package artifact is missing: {name}")))?;
    if !is_portable_relative(artifact_path) {
        return Err(CliError::new(format!(
            "embedded consumer package artifact path is not portable: {artifact_path}"
        )));
    }
    if artifact_path != runtime_artifact {
        return Err(CliError::new(format!(
            "embedded consumer is not bound to the runtime artifact {runtime_artifact}: {name}"
        )));
    }
    let artifact_sha256 = consumer
        .get("package_artifact_sha256")
        .and_then(Value::as_str)
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| CliError::new(format!("embedded consumer artifact hash is invalid: {name}")))?;
    let artifact = package_root.join(artifact_path);
    let artifact_metadata = fs::symlink_metadata(&artifact).map_err(|error| {
        CliError::new(format!("embedded consumer artifact is missing: {artifact_path}: {error}"))
    })?;
    if !artifact_metadata.file_type().is_file() {
        return Err(CliError::new(format!(
            "embedded consumer artifact is not a regular file: {artifact_path}"
        )));
    }
    let package_root_canonical = fs::canonicalize(package_root)?;
    let artifact_canonical = fs::canonicalize(&artifact)?;
    if !artifact_canonical.starts_with(&package_root_canonical) {
        return Err(CliError::new(format!(
            "embedded consumer artifact escapes the ABI package: {artifact_path}"
        )));
    }
    let actual_artifact_sha256 = hash_bytes(&fs::read(&artifact)?);
    if actual_artifact_sha256 != format!("sha256:{}", artifact_sha256.to_ascii_lowercase()) {
        return Err(CliError::new(format!(
            "embedded consumer artifact hash mismatch: {name}"
        )));
    }
    let checks = consumer
        .get("checks")
        .and_then(Value::as_array)
        .filter(|checks| !checks.is_empty())
        .ok_or_else(|| CliError::new(format!("embedded consumer checks are missing: {name}")))?;
    let mut seen_checks = BTreeSet::new();
    for check in checks {
        let check = check
            .as_str()
            .filter(|check| !check.trim().is_empty())
            .ok_or_else(|| CliError::new(format!("embedded consumer check is invalid: {name}")))?;
        if !seen_checks.insert(check) {
            return Err(CliError::new(format!(
                "embedded consumer checks contain a duplicate: {name}"
            )));
        }
    }
    let required_checks: &[&str] = match name {
        "c" => &["layout", "append", "read", "release", "stale_handles"],
        "python" => &["append", "follow", "owned_buffers", "cancel", "stale_handles"],
        "cpp" => &["blocked_pull_wakeup", "cross_thread_cancel", "clean_prefix_install"],
        _ => &[],
    };
    if !required_checks.iter().all(|required| seen_checks.contains(required)) {
        return Err(CliError::new(format!(
            "embedded consumer behavior checks are incomplete: {name}"
        )));
    }
    Ok(())
}

fn inventory_command(source_root: &Path, output: &Path) -> Result<(), CliError> {
    let source = source_identity(source_root)?;
    let expectations = evidence_expectations(source_root, output)?;
    validate_current_source(&source, expectations.as_ref())?;
    let languages =
        language_inventory(source_root, output, &source.revision, expectations.as_ref())?;
    let value = json!({ "schema": INVENTORY_SCHEMA, "source": source, "languages": languages });
    write_json_value(&output.join("language-inventory.json"), &value)?;
    print_json(&value)
}

fn validate_current_source(
    source: &SourceIdentity,
    expectations: Option<&EvidenceExpectations>,
) -> Result<(), CliError> {
    let Some(expected) = expectations else {
        return Ok(());
    };
    if source.dirty
        || source.revision != expected.source_revision
        || source.digest != expected.contract_digest
    {
        return Err(CliError::new(
            "current source identity differs from the generated qualification manifest",
        ));
    }
    Ok(())
}

fn ensure_source_identity_unchanged(
    before: &SourceIdentity,
    after: &SourceIdentity,
    phase: &str,
) -> Result<(), CliError> {
    if before.revision != after.revision
        || before.digest != after.digest
        || before.dirty != after.dirty
    {
        return Err(CliError::new(format!(
            "source checkout changed during {phase}; generators must write only to the output tree"
        )));
    }
    Ok(())
}

fn source_identity(root: &Path) -> Result<SourceIdentity, CliError> {
    source_identity_with_filter(root, |_| true)
}

fn verify_generator_identity(generator: &GeneratorIdentity) -> Result<(), CliError> {
    if generator.name != "acyclic-sdk-generation" || generator.version != env!("CARGO_PKG_VERSION")
    {
        return Err(CliError::new(
            "generation manifest was produced by an unexpected generator identity",
        ));
    }
    Ok(())
}

fn authoritative_source_identity(root: &Path) -> Result<SourceIdentity, CliError> {
    source_identity_with_filter(root, |path| !is_generated_output_path(path))
}

fn source_identity_with_filter<F>(root: &Path, include: F) -> Result<SourceIdentity, CliError>
where
    F: Fn(&str) -> bool,
{
    let expected_root = canonical_existing_directory(root, "source root")?;
    let git_root_text = command_stdout(root, "git", &["rev-parse", "--show-toplevel"])
        .map_err(|error| CliError::new(format!("cannot resolve Git source root: {error}")))?;
    let git_root = canonical_existing_directory(Path::new(&git_root_text), "Git source root")?;
    if git_root != expected_root {
        return Err(CliError::new(format!(
            "Git source root {} does not match requested source root {}",
            git_root.display(),
            expected_root.display()
        )));
    }
    let revision = command_stdout(root, "git", &["rev-parse", "HEAD"])
        .map_err(|error| CliError::new(format!("cannot resolve Git source revision: {error}")))?;
    let status = command_stdout(root, "git", &["status", "--porcelain"])
        .map_err(|error| CliError::new(format!("cannot read Git source status: {error}")))?;
    let mut files = tracked_files(root)?;
    files.sort();
    if let Some(path) = files.iter().find(|path| is_compiler_cache_path(path)) {
        return Err(CliError::new(format!(
            "source closure contains compiler cache material: {path}"
        )));
    }
    let mut hash = Sha256::new();
    for relative in files {
        if !include(&relative) {
            continue;
        }
        let path = root.join(&relative);
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(&path)?;
        validate_authored_text_bytes(&relative, &bytes)?;
        hash.update(relative.replace('\\', "/").as_bytes());
        hash.update([0]);
        hash.update(&bytes);
        hash.update([0]);
    }
    Ok(SourceIdentity {
        revision,
        digest: format!("sha256:{:x}", hash.finalize()),
        dirty: !status.trim().is_empty(),
    })
}

fn validate_authored_text_bytes(path: &str, bytes: &[u8]) -> Result<(), CliError> {
    if !is_authored_text_path(path) || is_serialized_evaluation_artifact(path) {
        return Ok(());
    }
    if bytes.contains(&0) {
        return Err(CliError::new(format!(
            "source closure contains a NUL byte in authored text: {path}"
        )));
    }
    std::str::from_utf8(bytes).map_err(|_| {
        CliError::new(format!(
            "source closure contains invalid UTF-8 in authored text: {path}"
        ))
    })?;
    Ok(())
}

fn is_serialized_evaluation_artifact(path: &str) -> bool {
    let path = path.replace('\\', "/");
    path.starts_with("arena/evals/results/")
        || path.starts_with("arena/evals/selfrace/")
        || path.starts_with("arena/evals/labelbias/")
        || path.starts_with("arena/evals/judge/")
}

fn is_authored_text_path(path: &str) -> bool {
    let path = path.replace('\\', "/");
    let Some(extension) = Path::new(&path).extension().and_then(OsStr::to_str) else {
        return false;
    };
    matches!(
        extension.to_ascii_lowercase().as_str(),
        "c" | "cc"
            | "cpp"
            | "cs"
            | "css"
            | "d"
            | "dart"
            | "dts"
            | "go"
            | "h"
            | "hpp"
            | "html"
            | "java"
            | "js"
            | "json"
            | "jsonl"
            | "jsx"
            | "kt"
            | "lock"
            | "mjs"
            | "md"
            | "php"
            | "proto"
            | "ps1"
            | "py"
            | "rb"
            | "rs"
            | "sh"
            | "swift"
            | "toml"
            | "ts"
            | "tsx"
            | "txt"
            | "xml"
            | "yaml"
            | "yml"
    )
}

fn is_generated_output_path(path: &str) -> bool {
    let path = path.replace('\\', "/");
    path == "generated"
        || path.starts_with("generated/")
        || path.starts_with("rust/crates/sdk-contract-wire/generated/")
        || path.starts_with("python/src/acyclic_sdk/generated/")
        || path.starts_with("typescript/packages/actors/generated/")
        || path.starts_with("typescript/packages/workers/generated/")
        || path.starts_with("typescript/packages/stream/generated/")
        || path.starts_with("typescript/packages/objects/generated/")
        || path.starts_with("typescript/packages/filesystem/generated/")
        || path.starts_with("typescript/packages/harness/generated/")
        || path.starts_with("typescript/packages/machines/generated/")
        || path.starts_with("typescript/packages/inference/generated/")
        || (path.starts_with("typescript/packages/")
            && path.ends_with("/src/generated-client.ts"))
        || matches!(
            path.as_str(),
            "rust/crates/actors/src/generated/acyclic.actors.v1.rs"
                | "rust/crates/actors/src/generated/acyclic.actors.v1.tonic.rs"
                | "rust/crates/workers/src/generated/acyclic.workers.v1.rs"
                | "rust/crates/workers/src/generated/acyclic.workers.v1.tonic.rs"
                | "rust/crates/objects/src/generated/acyclic.objects.v2.rs"
                | "rust/crates/objects/src/generated/acyclic.objects.v2.tonic.rs"
                | "rust/crates/filesystem/src/generated/rust-model-filesystem-v2.bin"
                | "rust/crates/harness/src/generated/rust-model-harness-v2.bin"
                | "rust/crates/harness/src/generated/harness-archived-v2.bin"
                | "rust/crates/machines/src/generated/acyclic-machines-v1.model.bin"
                | "rust/crates/machines/src/generated/acyclic-machines-v1.bin"
                | "rust/crates/inference/inference_model_descriptor.bin"
                | "rust/crates/inference/inference_descriptor.bin"
                | "rust/crates/inference-contract/inference_model_descriptor.bin"
                | "rust/crates/inference-contract/inference_model_descriptor_docs.bin"
                | "rust/crates/inference-contract/inference_descriptor.bin"
                | "rust/crates/inference/inference_model_descriptor_docs.bin"
                | "rust/crates/inference-wasm/inference_reflection_descriptor.bin"
                | "ruby/lib/acyclic_sdk/generated_remote_policy.rb"
                | "php/src/Acyclic/Runtime/GeneratedRemotePolicy.php"
                | "dart/lib/src/generated_remote_policy.dart"
                | "dart/lib/src/generated.dart"
                | "jvm/src/main/java/dev/acyclic/transport/GeneratedRemotePolicy.java"
                | "dotnet/GeneratedRemotePolicy.cs"
                | "typescript/packages/actors/src/generated-client.ts"
                | "typescript/packages/workers/src/generated-client.ts"
                | "typescript/packages/stream/src/generated-client.ts"
                | "typescript/packages/objects/src/generated-client.ts"
                | "typescript/packages/filesystem/src/generated-client.ts"
                | "typescript/packages/harness/src/generated-client.ts"
                | "typescript/packages/machines/src/generated-client.ts"
                | "typescript/packages/inference/src/generated-client.ts"
        )
}

fn is_compiler_cache_path(path: &str) -> bool {
    let components = Path::new(path).components().collect::<Vec<_>>();
    components.iter().enumerate().any(|(index, component)| {
        let std::path::Component::Normal(component) = component else {
            return false;
        };
        let name = component.to_string_lossy().to_ascii_lowercase();
        if name == ".cargo" {
            // `.cargo/config.toml` and `.cargo/config` are source inputs. Cargo's
            // registry, Git checkout, target and package-cache trees are build
            // material and must stay outside the source closure.
            return components
                .get(index + 1)
                .and_then(|next| match next {
                    std::path::Component::Normal(next) => {
                        Some(next.to_string_lossy().to_ascii_lowercase())
                    }
                    _ => None,
                })
                .is_some_and(|next| {
                    matches!(
                        next.as_str(),
                        "registry" | "git" | "target" | ".package-cache"
                    )
                });
        }
        matches!(
            name.as_str(),
            "target"
                | "node_modules"
                | ".rustc_info"
                | "__pycache__"
                | ".pytest_cache"
                | ".gradle"
                | ".mypy_cache"
        )
    })
}

fn verify_revision_artifacts(
    source_root: &Path,
    output: &Path,
    source_revision: &str,
) -> Result<(), CliError> {
    let checks = [
        ("docs.json", &["source_revision"] as &[&str]),
        (
            "sdk-examples-manifest.json",
            &["source", "revision"] as &[&str],
        ),
    ];
    for (relative, fields) in checks {
        let path = output.join(relative);
        if !path.is_file() {
            continue;
        }
        let value: Value = read_json(&path).map_err(|error| {
            CliError::new(format!(
                "cannot read generated revision artifact {relative}: {error}"
            ))
        })?;
        let mut current = &value;
        for field in fields {
            current = current.get(*field).ok_or_else(|| {
                CliError::new(format!(
                    "generated revision artifact {relative} is missing {field}"
                ))
            })?;
        }
        let Some(revision) = current.as_str() else {
            return Err(CliError::new(format!(
                "generated revision artifact {relative} has a non-string source revision"
            )));
        };
        if revision != source_revision {
            return Err(CliError::new(format!(
                "generated revision artifact {relative} is bound to {revision}, expected {source_revision}"
            )));
        }
        if relative == "sdk-examples-manifest.json" {
            verify_rust_snippet_receipts(source_root, output, &path, &value, source_revision)?;
        }
    }
    Ok(())
}

fn verify_rust_snippet_receipts(
    source_root: &Path,
    output: &Path,
    manifest_path: &Path,
    manifest: &Value,
    source_revision: &str,
) -> Result<(), CliError> {
    let Some(snippets) = manifest.get("snippets").and_then(Value::as_array) else {
        return Ok(());
    };
    for (index, snippet) in snippets.iter().enumerate() {
        if snippet.get("language").and_then(Value::as_str) != Some("rust") {
            continue;
        }
        let label = format!("{} snippet {index}", manifest_path.display());
        let validation = snippet.get("validation").ok_or_else(|| {
            CliError::new(format!("{label} is missing executable validation evidence"))
        })?;
        if validation.get("declared_level").and_then(Value::as_str) != Some("executed")
            || validation.get("declared_status").and_then(Value::as_str) != Some("passed")
        {
            return Err(CliError::new(format!(
                "{label} is not backed by an executed passing validation"
            )));
        }
        let receipt = validation
            .get("receipt")
            .ok_or_else(|| CliError::new(format!("{label} is missing exact snippet receipt")))?;
        if receipt.get("status").and_then(Value::as_str) != Some("qualified")
            || receipt.get("executed").and_then(Value::as_bool) != Some(true)
            || receipt.get("exit_code").and_then(Value::as_i64) != Some(0)
            || receipt
                .get("command")
                .and_then(Value::as_str)
                .is_none_or(|command| !command.contains("cargo test"))
            || receipt.get("source_revision").and_then(Value::as_str) != Some(source_revision)
        {
            return Err(CliError::new(format!(
                "{label} has no source-bound executed compile receipt"
            )));
        }

        let Some(snippet_path) = snippet.get("path").and_then(Value::as_str) else {
            return Err(CliError::new(format!(
                "{label} is missing its rendered path"
            )));
        };
        if !is_portable_relative(snippet_path) || !snippet_path.starts_with("snippets/") {
            return Err(CliError::new(format!(
                "{label} has a non-portable rendered path"
            )));
        }
        let rendered = output.join(snippet_path);
        let rendered_bytes = fs::read(&rendered).map_err(|error| {
            CliError::new(format!("{label} rendered snippet is missing: {error}"))
        })?;
        let Some(code_digest) = snippet.get("code_sha256").and_then(Value::as_str) else {
            return Err(CliError::new(format!(
                "{label} is missing rendered source hash"
            )));
        };
        let source_path = snippet
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new(format!("{label} is missing source snapshot path")))?;
        let source_digest = snippet
            .get("source_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new(format!("{label} is missing source snapshot hash")))?;
        if !is_portable_relative(source_path)
            || !is_sha256(source_digest)
            || receipt.get("source_path").and_then(Value::as_str) != Some(source_path)
            || receipt.get("source_sha256").and_then(Value::as_str) != Some(source_digest)
        {
            return Err(CliError::new(format!(
                "{label} receipt is not bound to its source snapshot"
            )));
        }
        let source_bytes = fs::read(source_root.join(source_path)).map_err(|error| {
            CliError::new(format!("{label} source snapshot is missing: {error}"))
        })?;
        if hash_bytes(&source_bytes) != source_digest {
            return Err(CliError::new(format!(
                "{label} source snapshot hash differs from authored bytes"
            )));
        }
        let Some(receipt_code_digest) = receipt.get("snippet_sha256").and_then(Value::as_str)
        else {
            return Err(CliError::new(format!(
                "{label} receipt is missing snippet hash"
            )));
        };
        if !is_sha256(code_digest)
            || receipt_code_digest != code_digest
            || hash_bytes(&rendered_bytes) != code_digest
            || receipt.get("snippet_path").and_then(Value::as_str) != Some(snippet_path)
        {
            return Err(CliError::new(format!(
                "{label} receipt is not bound to rendered snippet bytes"
            )));
        }
        verify_receipt_file_binding(output, &label, receipt, "compile_artifact")?;
        verify_receipt_file_binding(output, &label, receipt, "runtime_artifact")?;
        verify_receipt_file_binding(output, &label, receipt, "package_artifact")?;
        let package_resolution = receipt.get("package_resolution").ok_or_else(|| {
            CliError::new(format!(
                "{label} receipt is missing package resolution evidence"
            ))
        })?;
        let compile_digest = receipt
            .get("compile_artifact_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new(format!("{label} receipt is missing compile hash")))?;
        let package_digest = receipt
            .get("package_artifact_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new(format!("{label} receipt is missing package hash")))?;
        if package_resolution.get("resolved").and_then(Value::as_bool) != Some(true)
            || package_resolution.get("status").and_then(Value::as_str) != Some("qualified")
            || package_resolution
                .get("command")
                .and_then(Value::as_str)
                .is_none_or(|command| {
                    !command.contains("cargo test")
                        || !command.contains("--locked")
                        || !command.contains("--manifest-path")
                })
            || package_resolution
                .get("package_manager")
                .and_then(Value::as_str)
                != Some("cargo")
            || package_resolution
                .get("source_revision")
                .and_then(Value::as_str)
                != Some(source_revision)
            || package_resolution
                .get("compiled_snippet_sha256")
                .and_then(Value::as_str)
                != Some(code_digest)
            || package_resolution
                .get("compile_artifact_sha256")
                .and_then(Value::as_str)
                != Some(compile_digest)
            || package_resolution
                .get("package_artifact_sha256")
                .and_then(Value::as_str)
                != Some(package_digest)
            || package_resolution
                .get("package_artifact_path")
                .and_then(Value::as_str)
                != receipt.get("package_artifact_path").and_then(Value::as_str)
        {
            return Err(CliError::new(format!(
                "{label} package resolution is not tied to the compiled snippet"
            )));
        }
        let consumer_command = package_resolution
            .get("consumer_command")
            .and_then(Value::as_str)
            .filter(|command| {
                command.contains("cargo test")
                    && command.contains("--manifest-path")
                    && command.contains("--locked")
                    && command.contains("--offline")
            })
            .ok_or_else(|| {
                CliError::new(format!(
                    "{label} package resolution is missing a source-bound consumer command"
                ))
            })?;
        if consumer_command.trim().is_empty()
            || package_resolution
                .get("consumer_exit_code")
                .and_then(Value::as_i64)
                != Some(0)
            || package_resolution
                .get("artifact_consumed")
                .and_then(Value::as_bool)
                != Some(true)
            || package_resolution
                .get("artifact_consumption_command")
                .and_then(Value::as_str)
                .is_none_or(|command| command.trim().is_empty())
        {
            return Err(CliError::new(format!(
                "{label} package resolution has no successful retained consumer execution"
            )));
        }
        for stream in ["consumer_stdout", "consumer_stderr"] {
            verify_resolution_file_binding(output, &label, package_resolution, stream)?;
        }
        let package_root = package_resolution
            .get("package_root_path")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                CliError::new(format!("{label} package resolution has no package root"))
            })?;
        if !is_portable_relative(package_root) {
            return Err(CliError::new(format!(
                "{label} package root path is not portable"
            )));
        }
        let package_root_path = output.join(package_root);
        let package_root_metadata = fs::symlink_metadata(&package_root_path)
            .map_err(|error| CliError::new(format!("{label} package root is missing: {error}")))?;
        if package_root_metadata.file_type().is_symlink() {
            return Err(CliError::new(format!(
                "{label} package root must not be a symlink"
            )));
        }
        verify_resolution_file_binding(output, &label, package_resolution, "consumer_manifest")?;
        verify_resolution_file_binding(output, &label, package_resolution, "consumer_lock")?;
        verify_package_resolution(output, &label, package_resolution)?;
        for field in ["stdout_sha256", "stderr_sha256"] {
            if !receipt
                .get(field)
                .and_then(Value::as_str)
                .is_some_and(is_sha256)
            {
                return Err(CliError::new(format!(
                    "{label} receipt is missing {field} hash"
                )));
            }
        }
    }
    Ok(())
}

fn verify_receipt_file_binding(
    output: &Path,
    label: &str,
    receipt: &Value,
    prefix: &str,
) -> Result<(), CliError> {
    let path_field = format!("{prefix}_path");
    let digest_field = format!("{prefix}_sha256");
    let path_text = receipt
        .get(&path_field)
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("{label} receipt is missing {path_field}")))?;
    let digest = receipt
        .get(&digest_field)
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("{label} receipt is missing {digest_field}")))?;
    let expected_prefix = if prefix == "package_artifact" {
        "qualification/packages/"
    } else {
        "qualification/consumers/"
    };
    if !is_portable_relative(path_text)
        || !path_text.starts_with(expected_prefix)
        || !is_sha256(digest)
    {
        return Err(CliError::new(format!(
            "{label} receipt has an invalid {prefix} binding"
        )));
    }
    let path = output.join(path_text);
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| CliError::new(format!("{label} receipt artifact is missing: {error}")))?;
    if !metadata.file_type().is_file() {
        return Err(CliError::new(format!(
            "{label} receipt artifact is not a regular file"
        )));
    }
    let bytes = fs::read(&path)?;
    if hash_bytes(&bytes) != digest {
        return Err(CliError::new(format!(
            "{label} receipt {prefix} hash differs from runtime bytes"
        )));
    }
    Ok(())
}

fn verify_resolution_file_binding(
    output: &Path,
    label: &str,
    resolution: &Value,
    prefix: &str,
) -> Result<(), CliError> {
    let path_field = format!("{prefix}_path");
    let digest_field = format!("{prefix}_sha256");
    let path_text = resolution
        .get(&path_field)
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("{label} is missing {path_field}")))?;
    let digest = resolution
        .get(&digest_field)
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("{label} is missing {digest_field}")))?;
    let expected_prefix = match prefix {
        "package_manifest" | "package_artifact" => "qualification/packages/",
        _ => "qualification/consumers/",
    };
    if !is_portable_relative(path_text)
        || !path_text.starts_with(expected_prefix)
        || !is_sha256(digest)
    {
        return Err(CliError::new(format!(
            "{label} has an invalid {prefix} binding"
        )));
    }
    let path = output.join(path_text);
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| CliError::new(format!("{label} {prefix} is missing: {error}")))?;
    if !metadata.file_type().is_file() {
        return Err(CliError::new(format!(
            "{label} {prefix} is not a regular file"
        )));
    }
    let bytes = fs::read(&path)?;
    if hash_bytes(&bytes) != digest {
        return Err(CliError::new(format!(
            "{label} {prefix} hash differs from runtime bytes"
        )));
    }
    Ok(())
}

fn verify_package_resolution(
    output: &Path,
    label: &str,
    resolution: &Value,
) -> Result<(), CliError> {
    let package_name = resolution
        .get("package_name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| CliError::new(format!("{label} is missing package name")))?;
    if package_name == "sdk-example-consumer" {
        return Err(CliError::new(format!(
            "{label} package identity is a generic test consumer, not an SDK package"
        )));
    }
    let package_version = resolution
        .get("package_version")
        .and_then(Value::as_str)
        .filter(|version| !version.is_empty())
        .ok_or_else(|| CliError::new(format!("{label} is missing package version")))?;
    let package_root = resolution
        .get("package_root_path")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("{label} is missing package extraction path")))?;
    let package_manifest = resolution
        .get("package_manifest_path")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("{label} is missing package manifest path")))?;
    let package_tree_digest = resolution
        .get("package_tree_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("{label} is missing package tree hash")))?;
    if !is_portable_relative(package_root)
        || !package_root.starts_with("qualification/packages/")
        || !is_sha256(package_tree_digest)
    {
        return Err(CliError::new(format!(
            "{label} has an invalid package extraction binding"
        )));
    }
    let package_root_path = output.join(package_root);
    if !package_root_path.is_dir() {
        return Err(CliError::new(format!(
            "{label} package extraction path is missing"
        )));
    }
    if !package_manifest.starts_with(&format!("{package_root}/")) {
        return Err(CliError::new(format!(
            "{label} package manifest is outside extracted package"
        )));
    }
    verify_resolution_file_binding(output, label, resolution, "package_manifest")?;
    let manifest_bytes = fs::read(output.join(package_manifest))?;
    let manifest_text = String::from_utf8_lossy(&manifest_bytes);
    if !manifest_text.contains(&format!("name = \"{package_name}\""))
        || !manifest_text.contains(&format!("version = \"{package_version}\""))
    {
        return Err(CliError::new(format!(
            "{label} package manifest identity differs from resolved package"
        )));
    }
    let consumer_manifest = resolution
        .get("consumer_manifest_path")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("{label} is missing consumer manifest path")))?;
    let consumer_manifest_bytes = fs::read(output.join(consumer_manifest))?;
    let consumer_manifest_text = String::from_utf8_lossy(&consumer_manifest_bytes);
    let package_dir = Path::new(package_root)
        .file_name()
        .ok_or_else(|| CliError::new(format!("{label} has an invalid package root")))?
        .to_string_lossy();
    let extracted_relative = format!("path = \"../packages/{package_dir}\"");
    if !consumer_manifest_text.contains(&extracted_relative) {
        return Err(CliError::new(format!(
            "{label} consumer does not resolve the extracted package path"
        )));
    }
    let consumer_lock = resolution
        .get("consumer_lock_path")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("{label} is missing consumer lock path")))?;
    let consumer_lock_bytes = fs::read(output.join(consumer_lock))?;
    let lock_text = String::from_utf8_lossy(&consumer_lock_bytes);
    if !lock_resolves_local_package(&lock_text, package_name, package_version) {
        return Err(CliError::new(format!(
            "{label} Cargo.lock does not resolve the extracted package"
        )));
    }
    if directory_digest(&package_root_path)? != package_tree_digest {
        return Err(CliError::new(format!(
            "{label} extracted package bytes differ from receipt"
        )));
    }
    let package_artifact = resolution
        .get("package_artifact_path")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new(format!("{label} is missing package artifact path")))?;
    verify_resolution_file_binding(output, label, resolution, "package_artifact")?;
    verify_package_archive(
        output,
        label,
        package_artifact,
        package_root,
        &package_root_path,
        package_tree_digest,
    )?;
    Ok(())
}

fn verify_package_archive(
    output: &Path,
    label: &str,
    package_artifact: &str,
    package_root: &str,
    package_root_path: &Path,
    package_tree_digest: &str,
) -> Result<(), CliError> {
    if !is_portable_relative(package_artifact)
        || !package_artifact.starts_with("qualification/packages/")
        || !is_portable_relative(package_root)
    {
        return Err(CliError::new(format!(
            "{label} package archive binding is not portable"
        )));
    }
    if directory_digest(package_root_path)? != package_tree_digest {
        return Err(CliError::new(format!(
            "{label} extracted package bytes differ from receipt"
        )));
    }
    let expected_files = {
        let mut files = Vec::new();
        collect_output_files(package_root_path, package_root_path, &mut files)?;
        files.sort();
        files
    };
    let package_dir = Path::new(package_root)
        .file_name()
        .ok_or_else(|| CliError::new(format!("{label} has an invalid package root")))?
        .to_string_lossy()
        .into_owned();
    let archive_bytes = fs::read(output.join(package_artifact))?;
    let mut decoder = GzDecoder::new(BytewiseCursor::new(&archive_bytes));
    let mut tar_bytes = Vec::new();
    decoder.read_to_end(&mut tar_bytes).map_err(|error| {
        CliError::new(format!(
            "{label} package archive is not valid gzip: {error}"
        ))
    })?;
    let compressed = decoder.into_inner();
    if compressed.position != archive_bytes.len() {
        return Err(CliError::new(format!(
            "{label} package archive has trailing compressed bytes"
        )));
    }

    let mut archive = Archive::new(Cursor::new(tar_bytes));
    let mut seen = BTreeSet::new();
    let entries = archive
        .entries()
        .map_err(|error| CliError::new(format!("{label} package tar cannot be read: {error}")))?;
    for entry in entries {
        let mut entry = entry.map_err(|error| {
            CliError::new(format!("{label} package tar entry is invalid: {error}"))
        })?;
        let entry_type = entry.header().entry_type();
        let entry_path = entry
            .path()
            .map_err(|error| {
                CliError::new(format!("{label} package tar path is invalid: {error}"))
            })?
            .to_string_lossy()
            .replace('\\', "/");
        let prefix = format!("{package_dir}/");
        if entry_path == package_dir {
            if !entry_type.is_dir() {
                return Err(CliError::new(format!(
                    "{label} package root member is not a directory"
                )));
            }
            continue;
        }
        let Some(relative) = entry_path.strip_prefix(&prefix) else {
            return Err(CliError::new(format!(
                "{label} package member escapes extracted package: {entry_path}"
            )));
        };
        if !is_portable_relative(relative) {
            return Err(CliError::new(format!(
                "{label} package member is not portable: {entry_path}"
            )));
        }
        if entry_type.is_dir() {
            continue;
        }
        if !entry_type.is_file() {
            return Err(CliError::new(format!(
                "{label} package member is not a regular file: {entry_path}"
            )));
        }
        if !seen.insert(relative.to_owned()) {
            return Err(CliError::new(format!(
                "{label} package archive contains duplicate member: {entry_path}"
            )));
        }
        let extracted = package_root_path.join(relative);
        let metadata = fs::symlink_metadata(&extracted)?;
        if !metadata.file_type().is_file() {
            return Err(CliError::new(format!(
                "{label} extracted package member is not a regular file: {relative}"
            )));
        }
        let mut archived_bytes = Vec::new();
        entry.read_to_end(&mut archived_bytes).map_err(|error| {
            CliError::new(format!(
                "{label} package member cannot be read: {entry_path}: {error}"
            ))
        })?;
        if archived_bytes != fs::read(&extracted)? {
            return Err(CliError::new(format!(
                "{label} package member differs from extracted bytes: {relative}"
            )));
        }
    }
    let expected = expected_files.into_iter().collect::<BTreeSet<_>>();
    if seen != expected {
        return Err(CliError::new(format!(
            "{label} package archive member set differs from extracted package"
        )));
    }
    Ok(())
}

struct BytewiseCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> BytewiseCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
}

impl Read for BytewiseCursor<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() || self.position == self.bytes.len() {
            return Ok(0);
        }
        buffer[0] = self.bytes[self.position];
        self.position += 1;
        Ok(1)
    }
}

fn lock_resolves_local_package(lock_text: &str, package_name: &str, package_version: &str) -> bool {
    lock_text.split("[[package]]").map(str::trim).any(|entry| {
        entry.contains(&format!("name = \"{package_name}\""))
            && entry.contains(&format!("version = \"{package_version}\""))
            && !entry
                .lines()
                .any(|line| line.trim_start().starts_with("source = "))
    })
}

fn directory_digest(root: &Path) -> Result<String, CliError> {
    let mut files = Vec::new();
    collect_output_files(root, root, &mut files)?;
    files.sort();
    let mut hash = Sha256::new();
    for relative in files {
        hash.update(relative.as_bytes());
        hash.update([0]);
        hash.update(fs::read(root.join(&relative))?);
        hash.update([0]);
    }
    Ok(format!("sha256:{:x}", hash.finalize()))
}

fn tracked_files(root: &Path) -> Result<Vec<String>, CliError> {
    let result = repository_command()
        .args(["ls-files", "-co", "--exclude-standard", "-z"])
        .current_dir(root)
        .output()
        .map_err(|error| CliError::new(format!("cannot enumerate Git source files: {error}")))?;
    if !result.status.success() {
        return Err(CliError::new(format!(
            "git ls-files failed: {}",
            String::from_utf8_lossy(&result.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&result.stdout)
        .split('\0')
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect())
}

fn tool_specs(root: &Path) -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            id: "sdk-product-artifacts",
            required: true,
            manifest: some_file(root, "rust/crates/sdk-contract-wire/Cargo.toml"),
            script: None,
        },
        ToolSpec {
            id: "sdk-contract-wire",
            required: true,
            manifest: some_file(root, "rust/crates/sdk-contract-wire/Cargo.toml"),
            script: None,
        },
        ToolSpec {
            id: "sdk-contract-validation",
            required: true,
            manifest: some_file(root, "rust/crates/sdk-contract-validation/Cargo.toml"),
            script: None,
        },
        ToolSpec {
            id: "sdk-openapi-prototype",
            required: true,
            manifest: some_file(root, "rust/crates/sdk-openapi-prototype/Cargo.toml"),
            script: None,
        },
        ToolSpec {
            id: "sdk-language-producers",
            required: true,
            manifest: some_file(root, "languages/generation-targets.json"),
            script: None,
        },
        ToolSpec {
            id: "sdk-python",
            required: true,
            manifest: some_file(root, "rust/crates/sdk-python/Cargo.toml"),
            script: first_file(
                root,
                &[
                    "languages/python/generate.py",
                    "languages/python/generator.py",
                    "scripts/generate-python-sdk.py",
                ],
            ),
        },
        ToolSpec {
            id: "sdk-typescript",
            required: true,
            manifest: some_file(root, "rust/crates/sdk-typescript/Cargo.toml"),
            script: None,
        },
        ToolSpec {
            id: "sdk-typescript-rpc-contracts",
            required: true,
            manifest: some_file(root, "rust/crates/sdk-typescript/Cargo.toml"),
            script: None,
        },
        // Generated packages must exist before examples and docs consume
        // their exact source-bound artifacts.
        ToolSpec {
            id: "sdk-examples",
            required: true,
            manifest: some_file(root, "rust/crates/sdk-examples/Cargo.toml"),
            script: first_file(root, &["docs/sdk-examples.py", "scripts/sdk-examples.py"]),
        },
        ToolSpec {
            id: "sdk-docs",
            required: true,
            manifest: some_file(root, "rust/crates/sdk-docs/Cargo.toml"),
            script: first_file(root, &["docs/sdk-docs.py", "scripts/sdk-docs.py"]),
        },
    ]
}

fn some_file(root: &Path, relative: &str) -> Option<PathBuf> {
    let path = root.join(relative);
    path.is_file().then_some(path)
}

fn generated_package_roots(output: &Path) -> BTreeMap<String, String> {
    [
        ("language-producers", output.join("language-producers")),
        ("python", output.join("python")),
        ("typescript", output.join("typescript")),
    ]
    .into_iter()
    .filter(|(_, path)| path.is_dir())
    .map(|(name, path)| (name.to_owned(), path.to_string_lossy().into_owned()))
    .collect()
}

fn first_file(root: &Path, paths: &[&str]) -> Option<PathBuf> {
    paths
        .iter()
        .map(|path| root.join(path))
        .find(|path| path.is_file())
}

fn cargo_program() -> OsString {
    env::var_os("SDK_CARGO")
        .or_else(|| env::var_os("CARGO"))
        .unwrap_or_else(|| OsString::from("cargo"))
}

fn rust_toolchain(root: &Path) -> String {
    env::var("SDK_RUST_TOOLCHAIN")
        .ok()
        .or_else(|| {
            fs::read_to_string(root.join("rust-toolchain.toml"))
                .ok()
                .and_then(|contents| {
                    contents.lines().find_map(|line| {
                        let (key, value) = line.split_once('=')?;
                        (key.trim() == "channel").then(|| value.trim().trim_matches('"').to_owned())
                    })
                })
        })
        .unwrap_or_else(|| "1.98.1".into())
}

fn docs_rustdoc_command(
    root: &Path,
    output: &Path,
    profile_manifest: &Path,
) -> Option<Vec<OsString>> {
    let manifest = root.join("rust/crates/sdk-docs/Cargo.toml");
    if !manifest.is_file() || !profile_manifest.is_file() {
        return None;
    }
    Some(vec![
        cargo_program(),
        OsString::from("run"),
        OsString::from("--manifest-path"),
        manifest.as_os_str().to_os_string(),
        OsString::from("--locked"),
        OsString::from("--"),
        OsString::from("generate-rustdoc"),
        OsString::from("--repo-root"),
        root.as_os_str().to_os_string(),
        OsString::from("--profile-manifest"),
        profile_manifest.as_os_str().to_os_string(),
        OsString::from("--output-dir"),
        output.join("rustdoc-json").as_os_str().to_os_string(),
        OsString::from("--toolchain"),
        OsString::from(rust_toolchain(root)),
    ])
}

/// Run each docs profile separately so its Cargo compiler cache can be
/// reclaimed after the JSON graph and source receipt have been copied. The
/// generated graphs remain in one output tree; only disposable build products
/// are removed between profiles.
fn run_docs_rustdoc(root: &Path, output: &Path, request: String) -> Result<ToolResult, CliError> {
    let source_profile = root.join("docs/rustdoc-profiles.json");
    let source_value: Value = read_json(&source_profile)
        .map_err(|error| CliError::new(format!("cannot read docs profile manifest: {error}")))?;
    let profiles = source_value
        .get("profiles")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::new("docs profile manifest has no profiles array"))?;
    let profile_root = output.join(".docs-profiles");
    if profile_root.exists() {
        reject_symlink(&profile_root)?;
        fs::remove_dir_all(&profile_root)?;
    }
    fs::create_dir_all(&profile_root)?;
    let rustdoc_output = output.join("rustdoc-json");
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut first_command = Vec::new();
    let mut failure = None;
    let mut generated_artifacts = Vec::new();
    for profile in profiles {
        let name = profile
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| CliError::new("docs profile is missing a name"))?;
        let profile_manifest = profile_root.join(format!("{name}.json"));
        let mut profile_value = source_value.clone();
        profile_value["profiles"] = Value::Array(vec![profile.clone()]);
        normalize_profile_feature_order(&mut profile_value);
        write_json_value(&profile_manifest, &profile_value)?;
        let Some(command) = docs_rustdoc_command(root, output, &profile_manifest) else {
            failure = Some("docs rustdoc graph generator is not present in this checkout".into());
            break;
        };
        if first_command.is_empty() {
            first_command = command.clone();
        }
        match Command::new(&command[0])
            .args(&command[1..])
            .current_dir(root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
        {
            Ok(process) => {
                stdout.extend_from_slice(format!("[{name}]\n").as_bytes());
                stdout.extend_from_slice(&process.stdout);
                stderr.extend_from_slice(format!("[{name}]\n").as_bytes());
                stderr.extend_from_slice(&process.stderr);
                if !process.status.success() {
                    failure = Some(format!(
                        "docs rustdoc profile {name} failed with exit code {:?}",
                        process.status.code()
                    ));
                    break;
                }
            }
            Err(error) => {
                failure = Some(format!(
                    "could not start docs rustdoc profile {name}: {error}"
                ));
                break;
            }
        }
        let receipt_path = rustdoc_output.join("generation-receipt.json");
        if receipt_path.is_file() {
            let receipt: Value = read_json(&receipt_path).map_err(|error| {
                CliError::new(format!(
                    "cannot read docs rustdoc generation receipt: {error}"
                ))
            })?;
            if let Some(artifacts) = receipt.get("artifacts").and_then(Value::as_array) {
                generated_artifacts.extend(artifacts.iter().cloned());
            }
        }
        let target_dir = rustdoc_output.join(".cargo-target").join(name);
        if target_dir.exists() {
            reject_symlink(&target_dir)?;
            fs::remove_dir_all(&target_dir)?;
        }
    }
    let _ = fs::remove_dir_all(&profile_root);
    let cargo_target_root = rustdoc_output.join(".cargo-target");
    if cargo_target_root.is_dir() {
        let _ = fs::remove_dir_all(&cargo_target_root);
    }
    if failure.is_none() {
        generated_artifacts.sort_by(|left, right| {
            left.get("profile")
                .and_then(Value::as_str)
                .cmp(&right.get("profile").and_then(Value::as_str))
                .then_with(|| {
                    left.get("package_name")
                        .and_then(Value::as_str)
                        .cmp(&right.get("package_name").and_then(Value::as_str))
                })
        });
        write_json_value(
            &rustdoc_output.join("generation-receipt.json"),
            &json!({
                "schema_version": 1,
                "source_revision": source_identity(root)?.revision,
                "toolchain": rust_toolchain(root),
                "profile_manifest_blake3": hash_bytes(&fs::read(&source_profile)?),
                "artifacts": generated_artifacts,
            }),
        )?;
    }
    let logs = output.join("logs");
    fs::create_dir_all(&logs)?;
    fs::write(logs.join("sdk-docs-rustdoc.stdout"), &stdout)?;
    fs::write(logs.join("sdk-docs-rustdoc.stderr"), &stderr)?;
    Ok(ToolResult {
        id: "sdk-docs-rustdoc".into(),
        status: if failure.is_some() {
            "failed"
        } else {
            "passed"
        }
        .into(),
        required: true,
        command: first_command
            .iter()
            .map(|part| part.to_string_lossy().into_owned())
            .collect(),
        request,
        stdout_sha256: Some(hash_bytes(&stdout)),
        stderr_sha256: Some(hash_bytes(&stderr)),
        exit_code: Some(if failure.is_some() { 1 } else { 0 }),
        message: failure,
    })
}

fn normalize_profile_feature_order(profile_manifest: &mut Value) {
    let Some(profiles) = profile_manifest
        .get_mut("profiles")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for profile in profiles {
        let Some(packages) = profile.get_mut("packages").and_then(Value::as_array_mut) else {
            continue;
        };
        for package in packages {
            let Some(features) = package.get_mut("features").and_then(Value::as_array_mut) else {
                continue;
            };
            features.sort_by(|left, right| {
                left.as_str()
                    .unwrap_or_default()
                    .cmp(right.as_str().unwrap_or_default())
            });
        }
    }
}

fn replace_sdk_examples_operation(command: &mut [OsString], operation: &str) {
    if let Some(argument) = command.iter_mut().find(|argument| *argument == "generate") {
        *argument = OsString::from(operation);
    }
}

fn run_tools(
    root: &Path,
    output: &Path,
    source: &SourceIdentity,
    operation: Operation,
) -> Result<Vec<ToolResult>, CliError> {
    let request_directory = output.join("requests");
    fs::create_dir_all(&request_directory)?;
    let mut results = Vec::new();
    for spec in tool_specs(root) {
        let request = RequestEnvelope {
            schema: REQUEST_SCHEMA,
            operation: operation_name(operation).into(),
            tool: spec.id.into(),
            source_root: root.to_string_lossy().into_owned(),
            output: output.to_string_lossy().into_owned(),
            source: source.clone(),
            contract_scope: "explicit",
            contract_inputs: contract_inputs(root, spec.id),
            generated_package_roots: (spec.id == "sdk-examples")
                .then(|| generated_package_roots(output))
                .filter(|roots| !roots.is_empty()),
        };
        let request_path = request_directory.join(format!("{}.json", spec.id));
        write_json(&request_path, &request)?;
        let relative_request = relative_or_absolute(&request_path, output);
        if spec.id == "sdk-product-artifacts" {
            results.push(run_product_artifacts(
                root,
                output,
                &spec,
                relative_request,
                operation,
            )?);
            continue;
        }
        if spec.id == "sdk-language-producers" {
            results.push(run_language_producers(
                root,
                output,
                &spec,
                relative_request,
                source,
                operation,
            )?);
            continue;
        }
        if spec.id == "sdk-openapi-prototype" {
            results.push(run_openapi_projections(
                root,
                output,
                &spec,
                relative_request,
                operation,
            )?);
            continue;
        }
        if spec.id == "sdk-docs" {
            results.push(run_docs_rustdoc(root, output, relative_request.clone())?);
            if results
                .last()
                .is_some_and(|result| result.status == "failed")
            {
                continue;
            }
        }
        if spec.id == "sdk-contract-validation" {
            results.push(run_contract_validation(
                root,
                output,
                &spec,
                relative_request,
            )?);
            continue;
        }
        if spec.id == "sdk-docs" && !output.join("source-authority.json").is_file() {
            results.push(ToolResult {
                id: spec.id.into(),
                status: "failed".into(),
                required: spec.required,
                command: Vec::new(),
                request: relative_request,
                stdout_sha256: None,
                stderr_sha256: None,
                exit_code: Some(1),
                message: Some(
                    "sdk-examples source authority was not emitted before the docs stage".into(),
                ),
            });
            continue;
        }
        let Some(command) = tool_command(root, &spec, operation, &request_path, output, source)
        else {
            results.push(ToolResult {
                id: spec.id.into(),
                status: "pending".into(),
                required: spec.required,
                command: Vec::new(),
                request: relative_request,
                stdout_sha256: None,
                stderr_sha256: None,
                exit_code: None,
                message: Some("tool is not present in this checkout".into()),
            });
            continue;
        };
        let command_text = command
            .iter()
            .map(|part| part.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        if let Err(error) = ensure_generation_destination(
            root,
            output,
            spec.id,
            operation,
            &command,
        ) {
            results.push(ToolResult {
                id: spec.id.into(),
                status: "failed".into(),
                required: spec.required,
                command: command_text,
                request: relative_request,
                stdout_sha256: None,
                stderr_sha256: None,
                exit_code: Some(1),
                message: Some(error.to_string()),
            });
            continue;
        }
        let process = Command::new(&command[0])
            .args(&command[1..])
            .current_dir(root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output();
        match process {
            Ok(process) => {
                let result = tool_result(
                    spec.id,
                    spec.required,
                    relative_request.clone(),
                    command_text,
                    process,
                    output,
                )?;
                if spec.id == "sdk-examples"
                    && result.status == "passed"
                    && operation == Operation::Generate
                {
                    // The examples producer has three distinct source-bound
                    // outputs. Keep them in one generation transaction so a
                    // successful snippet run cannot leave fixtures or the
                    // qualification receipt stale or absent.
                    let mut fixtures_command = command.clone();
                    replace_sdk_examples_operation(&mut fixtures_command, "fixtures");
                    fixtures_command.insert(
                        fixtures_command
                            .iter()
                            .position(|argument| argument == "fixtures")
                            .map(|position| position + 1)
                            .unwrap_or(0),
                        OsString::from("generate"),
                    );
                    let fixtures_command_text = fixtures_command
                        .iter()
                        .map(|part| part.to_string_lossy().into_owned())
                        .collect::<Vec<_>>();
                    let fixtures_result = match Command::new(&fixtures_command[0])
                        .args(&fixtures_command[1..])
                        .current_dir(root)
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .output()
                    {
                        Ok(process) => tool_result(
                            "sdk-examples-fixtures",
                            true,
                            relative_request.clone(),
                            fixtures_command_text,
                            process,
                            output,
                        )?,
                        Err(error) => ToolResult {
                            id: "sdk-examples-fixtures".into(),
                            status: "failed".into(),
                            required: true,
                            command: fixtures_command_text,
                            request: relative_request.clone(),
                            stdout_sha256: None,
                            stderr_sha256: None,
                            exit_code: None,
                            message: Some(format!(
                                "could not start sdk-examples fixtures in {}: {error}",
                                root.display()
                            )),
                        },
                    };
                    let fixtures_passed = fixtures_result.status == "passed";
                    results.push(fixtures_result);

                    if fixtures_passed {
                        let mut receipts_command = command.clone();
                        replace_sdk_examples_operation(&mut receipts_command, "receipts");
                        let receipts_command_text = receipts_command
                            .iter()
                            .map(|part| part.to_string_lossy().into_owned())
                            .collect::<Vec<_>>();
                        let receipts_result = match Command::new(&receipts_command[0])
                            .args(&receipts_command[1..])
                            .current_dir(root)
                            .stdout(Stdio::piped())
                            .stderr(Stdio::piped())
                            .output()
                        {
                            Ok(process) => {
                                let mut receipt = tool_result(
                                    "sdk-examples-receipts",
                                    true,
                                    relative_request.clone(),
                                    receipts_command_text,
                                    process,
                                    output,
                                )?;
                                if receipt.status == "passed" {
                                    let qualification =
                                        output.join("sdk-qualification-receipt.json");
                                    match read_json::<Value>(&qualification) {
                                        Ok(value)
                                            if value.get("status").and_then(Value::as_str)
                                                == Some("passed") => {}
                                        Ok(value) => {
                                            receipt.status = "failed".into();
                                            receipt.exit_code = Some(1);
                                            receipt.message = Some(format!(
                                                "qualification receipt is not complete: {}",
                                                value
                                                    .get("status")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or("missing status")
                                            ));
                                        }
                                        Err(error) => {
                                            receipt.status = "failed".into();
                                            receipt.exit_code = Some(1);
                                            receipt.message = Some(format!(
                                                "read qualification receipt: {error}"
                                            ));
                                        }
                                    }
                                }
                                receipt
                            }
                            Err(error) => ToolResult {
                                id: "sdk-examples-receipts".into(),
                                status: "failed".into(),
                                required: true,
                                command: receipts_command_text,
                                request: relative_request.clone(),
                                stdout_sha256: None,
                                stderr_sha256: None,
                                exit_code: None,
                                message: Some(format!(
                                    "could not start sdk-examples receipts in {}: {error}",
                                    root.display()
                                )),
                            },
                        };
                        let receipts_passed = receipts_result.status == "passed";
                        results.push(receipts_result);
                        if receipts_passed {
                            if let Err(error) =
                                write_source_authority_manifest(output, &source.revision)
                            {
                                results.push(ToolResult {
                                    id: "sdk-examples-source-authority".into(),
                                    status: "failed".into(),
                                    required: true,
                                    command: Vec::new(),
                                    request: "sdk-examples-manifest.json".into(),
                                    stdout_sha256: None,
                                    stderr_sha256: None,
                                    exit_code: Some(1),
                                    message: Some(error.to_string()),
                                });
                            }
                        }
                    }
                }
                results.push(result);
            }
            Err(error) => results.push(ToolResult {
                id: spec.id.into(),
                status: "failed".into(),
                required: spec.required,
                command: command_text,
                request: relative_request,
                stdout_sha256: None,
                stderr_sha256: None,
                exit_code: None,
                message: Some(format!(
                    "could not start tool in {}: {error}",
                    root.display()
                )),
            }),
        }
    }
    Ok(results)
}

fn ensure_generation_destination(
    root: &Path,
    output: &Path,
    tool: &str,
    operation: Operation,
    command: &[OsString],
) -> Result<(), CliError> {
    if operation != Operation::Generate {
        return Ok(());
    }
    // The current migration hazard is the unified TypeScript package writer,
    // whose legacy command accepted the repository root as its destination.
    // Other tools legitimately receive the source root as a read-only input
    // (for example rustdoc's --repo-root), so do not reject those arguments.
    if tool != "sdk-typescript" {
        return Ok(());
    }
    let root = canonical_existing_directory(root, "source root")?;
    let output = canonical_existing_directory(output, "generation output")?;
    if output == root {
        return Err(CliError::new(format!(
            "{tool} generation output must be outside the frozen source checkout"
        )));
    }
    // The package writer receives the positional contract
    // `packages-write <source-root> <output-root> <wire-root> <revision>`
    // after Cargo's `--` separator. The source root is a read-only input;
    // the output root and wire root must remain in the isolated tree. Do not
    // infer the destination from the final argument, which is the Git OID.
    let separator = command
        .iter()
        .position(|argument| argument == "--")
        .ok_or_else(|| {
            CliError::new(format!(
                "{tool} generation command is missing its Cargo argument separator"
            ))
        })?;
    let package_args = command.get(separator + 1..).unwrap_or_default();
    if package_args.len() != 5 || package_args[0] != "packages-write" {
        return Err(CliError::new(format!(
            "{tool} generation command must use packages-write <source-root> <output-root> <wire-root> <revision>"
        )));
    }
    let source_argument = canonical_existing_directory(
        Path::new(&package_args[1]),
        "generation source root",
    )?;
    if source_argument != root {
        return Err(CliError::new(format!(
            "{tool} generation source root must resolve to the frozen source checkout"
        )));
    }
    let destination = canonical_existing_directory(
        Path::new(&package_args[2]),
        "generation destination",
    )?;
    if destination != output {
        return Err(CliError::new(format!(
            "{tool} generation command must target the isolated output tree"
        )));
    }
    let wire = canonical_existing_directory(Path::new(&package_args[3]), "wire output")?;
    if wire == output || !wire.starts_with(&output) {
        return Err(CliError::new(format!(
            "{tool} generation wire output must be nested under the isolated output tree"
        )));
    }
    Ok(())
}

fn operation_name(operation: Operation) -> &'static str {
    match operation {
        Operation::Generate => "generate",
        Operation::Check => "check",
        Operation::Drift => "drift",
        Operation::Qualify => "qualify",
        Operation::QualifyEmbedded => "qualify-embedded",
        Operation::Inventory => "inventory",
    }
}

fn run_contract_validation(
    root: &Path,
    output: &Path,
    spec: &ToolSpec,
    request: String,
) -> Result<ToolResult, CliError> {
    let Some(manifest) = spec.manifest.as_ref() else {
        return Ok(ToolResult {
            id: spec.id.into(),
            status: "pending".into(),
            required: spec.required,
            command: Vec::new(),
            request,
            stdout_sha256: None,
            stderr_sha256: None,
            exit_code: None,
            message: Some("semantic validator is not present in this checkout".into()),
        });
    };
    let fixtures = root.join("rust/crates/sdk-contract-wire/tests/fixtures");
    let mut candidate_paths = Vec::new();
    collect_output_files(output, output, &mut candidate_paths)?;
    let mut families = candidate_paths
        .into_iter()
        .filter(|path| path.starts_with("wire/") && path.ends_with(".fds.bin"))
        .map(|relative| {
            let candidate = output.join(&relative);
            let stem = Path::new(&relative)
                .file_name()
                .and_then(OsStr::to_str)
                .and_then(|name| name.strip_suffix(".fds.bin"))
                .unwrap_or_default()
                .to_owned();
            let baseline = fixtures.join(format!("{stem}.descriptor.bin"));
            let baseline = if baseline.is_file() {
                baseline
            } else {
                baseline_for_family(&fixtures, &stem)
                    .unwrap_or_else(|| fixtures.join(format!("{stem}.descriptor.bin")))
            };
            (stem.clone(), baseline, candidate)
        })
        .collect::<Vec<_>>();
    families.sort_by(|left, right| left.0.cmp(&right.0));
    if families.is_empty() {
        return Ok(ToolResult {
            id: spec.id.into(),
            status: "failed".into(),
            required: spec.required,
            command: Vec::new(),
            request,
            stdout_sha256: None,
            stderr_sha256: None,
            exit_code: Some(1),
            message: Some("Rust wire exporter emitted no descriptor families".into()),
        });
    }
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut first_command = Vec::new();
    let mut failed = None;
    for (family, baseline, candidate) in families {
        if !baseline.is_file() {
            failed = Some(format!(
                "{family} deployed compatibility descriptor is missing: {}",
                baseline.display()
            ));
            break;
        }
        if let Err(error) = verify_immutable_baseline(&family, &baseline) {
            failed = Some(error);
            break;
        }
        if !candidate.is_file() {
            failed = Some(format!(
                "{family} Rust authority descriptor is missing: {}",
                candidate.display()
            ));
            break;
        }
        let command = vec![
            cargo_program(),
            OsString::from("run"),
            OsString::from("--manifest-path"),
            manifest.as_os_str().to_os_string(),
            OsString::from("--locked"),
            OsString::from("--"),
            OsString::from("--baseline"),
            baseline.as_os_str().to_os_string(),
            OsString::from("--candidate"),
            candidate.as_os_str().to_os_string(),
        ];
        if first_command.is_empty() {
            first_command = command.clone();
        }
        match Command::new(&command[0])
            .args(&command[1..])
            .current_dir(root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
        {
            Ok(process) => {
                stdout.extend_from_slice(format!("[{family}]\n").as_bytes());
                stdout.extend_from_slice(&process.stdout);
                stderr.extend_from_slice(format!("[{family}]\n").as_bytes());
                stderr.extend_from_slice(&process.stderr);
                if !process.status.success() {
                    failed = Some(format!(
                        "{family} semantic compatibility failed with exit code {:?}",
                        process.status.code()
                    ));
                    break;
                }
            }
            Err(error) => {
                failed = Some(format!(
                    "{family} semantic validator could not start: {error}"
                ));
                break;
            }
        }
    }
    let logs = output.join("logs");
    fs::create_dir_all(&logs)?;
    fs::write(logs.join("sdk-contract-validation.stdout"), &stdout)?;
    fs::write(logs.join("sdk-contract-validation.stderr"), &stderr)?;
    Ok(ToolResult {
        id: spec.id.into(),
        status: if failed.is_some() {
            "failed".into()
        } else {
            "passed".into()
        },
        required: spec.required,
        command: first_command
            .iter()
            .map(|part| part.to_string_lossy().into_owned())
            .collect(),
        request,
        stdout_sha256: Some(hash_bytes(&stdout)),
        stderr_sha256: Some(hash_bytes(&stderr)),
        exit_code: failed.as_ref().map_or(Some(0), |_| Some(1)),
        message: failed,
    })
}

fn run_product_artifacts(
    root: &Path,
    output: &Path,
    spec: &ToolSpec,
    request: String,
    operation: Operation,
) -> Result<ToolResult, CliError> {
    let Some(manifest) = spec.manifest.as_ref() else {
        return Ok(ToolResult {
            id: spec.id.into(),
            status: "failed".into(),
            required: spec.required,
            command: Vec::new(),
            request,
            stdout_sha256: None,
            stderr_sha256: None,
            exit_code: None,
            message: Some("product artifact command is not present in this checkout".into()),
        });
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut first_command = Vec::new();
    let mut failure = None;
    let operations = if matches!(operation, Operation::Generate | Operation::Check) {
        ["generate-products", "check-products"].as_slice()
    } else {
        ["check-products"].as_slice()
    };
    // Generation and its clean drift check must stage product artifacts under
    // the isolated output tree. The source checkout is a frozen authority;
    // only the standalone command may target it by omitting --out.
    let destination = if matches!(operation, Operation::Generate | Operation::Check) {
        output
    } else {
        root
    };
    for operation in operations {
        let command = vec![
            cargo_program(),
            OsString::from("run"),
            OsString::from("--manifest-path"),
            manifest.as_os_str().to_os_string(),
            OsString::from("--locked"),
            OsString::from("--bin"),
            OsString::from("sdk-contract-wire"),
            OsString::from("--"),
            OsString::from(operation),
            OsString::from("--root"),
            root.as_os_str().to_os_string(),
            OsString::from("--out"),
            destination.as_os_str().to_os_string(),
        ];
        if first_command.is_empty() {
            first_command = command.clone();
        }
        match Command::new(&command[0])
            .args(&command[1..])
            .current_dir(root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
        {
            Ok(process) => {
                stdout.extend_from_slice(format!("[{operation}]\n").as_bytes());
                stdout.extend_from_slice(&process.stdout);
                stderr.extend_from_slice(format!("[{operation}]\n").as_bytes());
                stderr.extend_from_slice(&process.stderr);
                if !process.status.success() {
                    failure = Some(format!(
                        "{operation} failed with exit code {:?}",
                        process.status.code()
                    ));
                    break;
                }
            }
            Err(error) => {
                failure = Some(format!("{operation} could not start: {error}"));
                break;
            }
        }
    }
    let logs = output.join("logs");
    fs::create_dir_all(&logs)?;
    fs::write(logs.join("sdk-product-artifacts.stdout"), &stdout)?;
    fs::write(logs.join("sdk-product-artifacts.stderr"), &stderr)?;
    Ok(ToolResult {
        id: spec.id.into(),
        status: if failure.is_some() {
            "failed"
        } else {
            "passed"
        }
        .into(),
        required: spec.required,
        command: first_command
            .iter()
            .map(|part| part.to_string_lossy().into_owned())
            .collect(),
        request,
        stdout_sha256: Some(hash_bytes(&stdout)),
        stderr_sha256: Some(hash_bytes(&stderr)),
        exit_code: Some(if failure.is_some() { 1 } else { 0 }),
        message: failure,
    })
}

/// Emit the Rust-owned language producer plan and, when a target supplies an
/// explicit staged-output recipe, execute that recipe against the current Rust
/// authority. The plan never claims package installation or qualification;
/// those consumer checks remain evidence for the individual language.
fn run_language_producers(
    root: &Path,
    output: &Path,
    spec: &ToolSpec,
    request: String,
    source: &SourceIdentity,
    operation: Operation,
) -> Result<ToolResult, CliError> {
    let Some(targets_path) = spec.manifest.as_ref() else {
        return Ok(ToolResult {
            id: spec.id.into(),
            status: "failed".into(),
            required: spec.required,
            command: Vec::new(),
            request,
            stdout_sha256: None,
            stderr_sha256: None,
            exit_code: Some(1),
            message: Some("language generation target catalog is missing".into()),
        });
    };
    let catalog: Value = read_json(targets_path).map_err(|error| {
        CliError::new(format!(
            "cannot read language generation target catalog {}: {error}",
            targets_path.display()
        ))
    })?;
    let targets = catalog
        .get("targets")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::new("language generation target catalog has no targets array"))?;
    if targets.is_empty() {
        return Err(CliError::new(
            "language generation target catalog must contain at least one target",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut plans = Vec::with_capacity(targets.len());
    let mut stage_failure = None;
    let mut stage_pending = false;
    for target in targets {
        let id = target
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new("language generation target has no string id"))?;
        if !is_language_target_id(id) || !seen.insert(id.to_owned()) {
            return Err(CliError::new(format!(
                "language generation target id is unsafe or duplicated: {id}"
            )));
        }
        let family = target
            .get("language_family")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new(format!("language target {id} has no language_family")))?;
        let status = target
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new(format!("language target {id} has no status")))?;
        let generator = target
            .get("generator")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                CliError::new(format!("language target {id} has no generator recipe"))
            })?;
        for field in ["name", "version", "source", "license", "pin"] {
            if generator
                .get(field)
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(CliError::new(format!(
                    "language target {id} generator recipe is missing {field}"
                )));
            }
        }
        let package = target
            .get("package")
            .and_then(Value::as_object)
            .ok_or_else(|| CliError::new(format!("language target {id} has no package recipe")))?;
        for field in ["ecosystem", "artifact"] {
            if package
                .get(field)
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(CliError::new(format!(
                    "language target {id} package recipe is missing {field}"
                )));
            }
        }
        let recipe = parse_producer_recipe(target, id)?;
        let mut execution = json!({
            "status": "pending",
            "reason": if recipe.is_some() {
                "recipe is present but has not run"
            } else {
                "language owner has not supplied an executable recipe"
            }
        });
        if let Some(recipe) = recipe {
            if operation == Operation::Drift {
                execution = json!({
                    "status": "pending",
                    "reason": "drift never invokes downstream producers",
                });
                stage_pending = true;
            } else {
                let target_output = output
                    .join("language-producers")
                    .join(id)
                    .join(&recipe.output);
                let target_request = output
                    .join("language-producers/requests")
                    .join(format!("{id}.json"));
                fs::create_dir_all(&target_output)?;
                let target_request_value = json!({
                    "schema": REQUEST_SCHEMA,
                    "operation": operation_name(operation),
                    "tool": "sdk-language-producer",
                    "target": id,
                    "source_root": root,
                    "output": target_output,
                    "source": source,
                    "contract_scope": "rust-authority",
                    "contract_inputs": [
                        "rust/crates/sdk-contract-wire",
                        "languages/generation-targets.json"
                    ]
                });
                write_json_value(&target_request, &target_request_value)?;
                let command = expand_producer_command(
                    &recipe,
                    root,
                    output,
                    &target_output,
                    &target_request,
                    id,
                    operation,
                )?;
                let command_text = command
                    .iter()
                    .map(|part| part.to_string_lossy().into_owned())
                    .collect::<Vec<_>>();
                let process = Command::new(&command[0])
                    .args(&command[1..])
                    .current_dir(root)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .output();
                let logs = output.join("logs");
                fs::create_dir_all(&logs)?;
                match process {
                    Ok(process) => {
                        let stdout_path = logs.join(format!("sdk-language-producer-{id}.stdout"));
                        let stderr_path = logs.join(format!("sdk-language-producer-{id}.stderr"));
                        fs::write(&stdout_path, &process.stdout)?;
                        fs::write(&stderr_path, &process.stderr)?;
                        let stdout_sha256 = hash_bytes(&process.stdout);
                        let stderr_sha256 = hash_bytes(&process.stderr);
                        if !process.status.success() {
                            stage_failure = Some(format!(
                                "language producer {id} failed with exit code {:?}",
                                process.status.code()
                            ));
                            execution = json!({
                                "status": "failed",
                                "command": command_text,
                                "request": relative_or_absolute(&target_request, output),
                                "stdout": relative_or_absolute(&stdout_path, output),
                                "stdout_sha256": stdout_sha256,
                                "stderr": relative_or_absolute(&stderr_path, output),
                                "stderr_sha256": stderr_sha256,
                                "exit_code": process.status.code(),
                            });
                        } else if !target_output.is_dir()
                            || collect_output_files(&target_output, &target_output, &mut Vec::new())
                                .is_err()
                        {
                            stage_failure = Some(format!(
                                "language producer {id} exited successfully without a valid staged output"
                            ));
                            execution = json!({
                                "status": "failed",
                                "command": command_text,
                                "request": relative_or_absolute(&target_request, output),
                                "stdout": relative_or_absolute(&stdout_path, output),
                                "stdout_sha256": stdout_sha256,
                                "stderr": relative_or_absolute(&stderr_path, output),
                                "stderr_sha256": stderr_sha256,
                                "exit_code": process.status.code(),
                            });
                        } else {
                            let artifact_digest = directory_digest(&target_output)?;
                            execution = json!({
                                "status": "passed",
                                "command": command_text,
                                "request": relative_or_absolute(&target_request, output),
                                "stdout": relative_or_absolute(&stdout_path, output),
                                "stdout_sha256": stdout_sha256,
                                "stderr": relative_or_absolute(&stderr_path, output),
                                "stderr_sha256": stderr_sha256,
                                "exit_code": process.status.code(),
                                "output": relative_or_absolute(&target_output, output),
                                "artifact_digest": artifact_digest,
                            });
                        }
                    }
                    Err(error) => {
                        stage_pending = true;
                        execution = json!({
                            "status": "pending",
                            "command": command_text,
                            "request": relative_or_absolute(&target_request, output),
                            "reason": format!("producer executable is unavailable: {error}"),
                        });
                    }
                }
            }
        } else {
            stage_pending = true;
        }
        plans.push(json!({
            "id": id,
            "language_family": family,
            "status": status,
            "generator": generator,
            "package": package,
            "execution": execution,
            "request": {
                "schema": REQUEST_SCHEMA,
                "operation": operation_name(operation),
                "source_revision": source.revision,
                "source_digest": source.digest,
                "contract_scope": "rust-authority",
                "contract_inputs": [
                    "rust/crates/sdk-contract-wire",
                    "languages/generation-targets.json"
                ]
            }
        }));
    }
    let plan = json!({
        "schema": "acyclic.sdk.language-producer-plan.v1",
        "operation": operation_name(operation),
        "source": source,
        "authority": "rust",
        "qualification": "pending-until-language-consumer-receipt",
        "targets": plans,
    });
    let plan_path = output.join("language-producers/plan.json");
    write_json_value(&plan_path, &plan)?;
    let stdout = serde_json::to_vec_pretty(&json!({
        "stage": spec.id,
        "operation": operation_name(operation),
        "targets": seen.len(),
        "plan": relative_or_absolute(&plan_path, output),
    }))?;
    let stderr = Vec::new();
    let logs = output.join("logs");
    fs::create_dir_all(&logs)?;
    fs::write(logs.join("sdk-language-producers.stdout"), &stdout)?;
    fs::write(logs.join("sdk-language-producers.stderr"), &stderr)?;
    let stage_status = if stage_failure.is_some() {
        "failed"
    } else if stage_pending {
        "pending"
    } else {
        "passed"
    };
    let stage_message = if let Some(failure) = stage_failure.clone() {
        Some(failure)
    } else if stage_pending {
        Some(
            "producer recipes or toolchains remain pending; no package qualification is claimed"
                .into(),
        )
    } else {
        Some(
            "all declared language producer recipes executed against staged Rust authority output"
                .into(),
        )
    };
    Ok(ToolResult {
        id: spec.id.into(),
        status: stage_status.into(),
        required: spec.required,
        command: vec![
            "sdk-generation".into(),
            "language-producers".into(),
            "--source-root".into(),
            root.to_string_lossy().into_owned(),
            "--output".into(),
            output.to_string_lossy().into_owned(),
        ],
        request,
        stdout_sha256: Some(hash_bytes(&stdout)),
        stderr_sha256: Some(hash_bytes(&stderr)),
        exit_code: if stage_failure.is_some() {
            Some(1)
        } else if stage_pending {
            None
        } else {
            Some(0)
        },
        message: stage_message,
    })
}

fn parse_producer_recipe(target: &Value, id: &str) -> Result<Option<ProducerRecipe>, CliError> {
    let Some(value) = target.get("producer") else {
        return Ok(None);
    };
    let object = value
        .as_object()
        .ok_or_else(|| CliError::new(format!("language target {id} producer must be an object")))?;
    let program = object
        .get("program")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            CliError::new(format!("language target {id} producer is missing program"))
        })?;
    let args = object
        .get("args")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::new(format!("language target {id} producer is missing args")))?
        .iter()
        .map(|arg| {
            arg.as_str().map(str::to_owned).ok_or_else(|| {
                CliError::new(format!(
                    "language target {id} producer args must be strings"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if args.is_empty() {
        return Err(CliError::new(format!(
            "language target {id} producer args must not be empty"
        )));
    }
    let output = object
        .get("output")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| CliError::new(format!("language target {id} producer is missing output")))?;
    if !is_safe_relative_path(output) {
        return Err(CliError::new(format!(
            "language target {id} producer output must be a safe relative path"
        )));
    }
    for required in ["{source_root}", "{target_output}", "{request}"] {
        if !args.iter().any(|arg| arg.contains(required)) {
            return Err(CliError::new(format!(
                "language target {id} producer args must bind {required}"
            )));
        }
    }
    for arg in &args {
        let mut rest = arg.as_str();
        while let Some(start) = rest.find('{') {
            let end = rest[start..]
                .find('}')
                .map(|offset| start + offset)
                .ok_or_else(|| {
                    CliError::new(format!(
                        "language target {id} producer has an unterminated placeholder"
                    ))
                })?;
            let placeholder = &rest[start..=end];
            if !matches!(
                placeholder,
                "{source_root}"
                    | "{output_root}"
                    | "{target_output}"
                    | "{wire_root}"
                    | "{authority_manifest}"
                    | "{request}"
                    | "{target_id}"
                    | "{operation}"
            ) {
                return Err(CliError::new(format!(
                    "language target {id} producer has unknown placeholder {placeholder}"
                )));
            }
            rest = &rest[end + 1..];
        }
    }
    Ok(Some(ProducerRecipe {
        program: program.to_owned(),
        args,
        output: output.to_owned(),
    }))
}

fn expand_producer_command(
    recipe: &ProducerRecipe,
    root: &Path,
    output: &Path,
    target_output: &Path,
    request: &Path,
    target_id: &str,
    operation: Operation,
) -> Result<Vec<OsString>, CliError> {
    let authority_manifest = output.join("source-authority.json");
    let replacements = [
        ("{source_root}", root.to_string_lossy().into_owned()),
        ("{output_root}", output.to_string_lossy().into_owned()),
        (
            "{target_output}",
            target_output.to_string_lossy().into_owned(),
        ),
        (
            "{wire_root}",
            output.join("wire").to_string_lossy().into_owned(),
        ),
        (
            "{authority_manifest}",
            authority_manifest.to_string_lossy().into_owned(),
        ),
        ("{request}", request.to_string_lossy().into_owned()),
        ("{target_id}", target_id.to_owned()),
        ("{operation}", operation_name(operation).to_owned()),
    ];
    let expand = |value: &str| {
        replacements
            .iter()
            .fold(value.to_owned(), |value, (placeholder, replacement)| {
                value.replace(placeholder, replacement)
            })
    };
    let mut command = vec![OsString::from(expand(&recipe.program))];
    command.extend(recipe.args.iter().map(|arg| OsString::from(expand(arg))));
    Ok(command)
}

fn is_safe_relative_path(value: &str) -> bool {
    let path = Path::new(value);
    !path.is_absolute()
        && !value.is_empty()
        && path
            .components()
            .all(|component| !matches!(component, std::path::Component::ParentDir))
}

fn is_language_target_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn run_openapi_projections(
    root: &Path,
    output: &Path,
    spec: &ToolSpec,
    request: String,
    operation: Operation,
) -> Result<ToolResult, CliError> {
    let Some(manifest) = spec.manifest.as_ref() else {
        return Ok(ToolResult {
            id: spec.id.into(),
            status: "failed".into(),
            required: spec.required,
            command: Vec::new(),
            request,
            stdout_sha256: None,
            stderr_sha256: None,
            exit_code: None,
            message: Some("OpenAPI projection command is not present in this checkout".into()),
        });
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut first_command = Vec::new();
    let mut failure = None;
    prepare_openapi_stage_output(output, operation)?;
    for family in openapi_family_names() {
        let path = output.join(format!("openapi/{family}.json"));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let command = vec![
            cargo_program(),
            OsString::from("run"),
            OsString::from("--manifest-path"),
            manifest.as_os_str().to_os_string(),
            OsString::from("--locked"),
            OsString::from("--bin"),
            OsString::from("sdk-openapi"),
            OsString::from("--"),
            OsString::from("--contract"),
            OsString::from(family),
            path.as_os_str().to_os_string(),
        ];
        if first_command.is_empty() {
            first_command = command.clone();
        }
        match Command::new(&command[0])
            .args(&command[1..])
            .current_dir(root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
        {
            Ok(process) => {
                stdout.extend_from_slice(format!("[{family}]\n").as_bytes());
                stdout.extend_from_slice(&process.stdout);
                stderr.extend_from_slice(format!("[{family}]\n").as_bytes());
                stderr.extend_from_slice(&process.stderr);
                if !process.status.success() {
                    failure = Some(format!(
                        "OpenAPI projection for {family} failed with exit code {:?}",
                        process.status.code()
                    ));
                    break;
                }
            }
            Err(error) => {
                failure = Some(format!(
                    "OpenAPI projection for {family} could not start: {error}"
                ));
                break;
            }
        }
    }
    if failure.is_none() {
        // OAG supplies the PowerShell package scaffolding, while Workers
        // protobuf byte/uint64 behavior is emitted by the Rust authority.
        // Keep this adaptation in the OpenAPI stage so its bytes are hashed
        // with the projections and checked for drift on every run.
        let path = output.join("openapi/workers-powershell-adaptation.ps1");
        let command = vec![
            cargo_program(),
            OsString::from("run"),
            OsString::from("--manifest-path"),
            manifest.as_os_str().to_os_string(),
            OsString::from("--locked"),
            OsString::from("--bin"),
            OsString::from("sdk-openapi"),
            OsString::from("--"),
            OsString::from("--powershell-adaptation"),
            path.as_os_str().to_os_string(),
        ];
        match Command::new(&command[0])
            .args(&command[1..])
            .current_dir(root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
        {
            Ok(process) => {
                stdout.extend_from_slice(b"[powershell-adaptation]\n");
                stdout.extend_from_slice(&process.stdout);
                stderr.extend_from_slice(b"[powershell-adaptation]\n");
                stderr.extend_from_slice(&process.stderr);
                if !process.status.success() {
                    failure = Some(format!(
                        "Rust-owned PowerShell adaptation failed with exit code {:?}",
                        process.status.code()
                    ));
                }
            }
            Err(error) => {
                failure = Some(format!(
                    "Rust-owned PowerShell adaptation could not start: {error}"
                ));
            }
        }
    }
    if failure.is_none() {
        if let Err(error) = write_openapi_stage_receipt(output) {
            failure = Some(format!("OpenAPI stage receipt failed: {error}"));
        }
    }
    let logs = output.join("logs");
    fs::create_dir_all(&logs)?;
    fs::write(logs.join("sdk-openapi-prototype.stdout"), &stdout)?;
    fs::write(logs.join("sdk-openapi-prototype.stderr"), &stderr)?;
    Ok(ToolResult {
        id: spec.id.into(),
        status: if failure.is_some() {
            "failed"
        } else {
            "passed"
        }
        .into(),
        required: spec.required,
        command: first_command
            .iter()
            .map(|part| part.to_string_lossy().into_owned())
            .collect(),
        request,
        stdout_sha256: Some(hash_bytes(&stdout)),
        stderr_sha256: Some(hash_bytes(&stderr)),
        exit_code: Some(if failure.is_some() { 1 } else { 0 }),
        message: failure,
    })
}

fn prepare_openapi_stage_output(output: &Path, operation: Operation) -> Result<(), CliError> {
    if operation != Operation::Generate {
        return Ok(());
    }
    let stage_receipt = output.join("openapi/stage-receipt.json");
    if stage_receipt.exists() {
        reject_symlink(&stage_receipt)?;
        fs::remove_file(&stage_receipt)?;
    }
    Ok(())
}

fn write_openapi_stage_receipt(output: &Path) -> Result<(), CliError> {
    let families = openapi_family_names();
    let projections = families
        .iter()
        .map(|family| {
            let relative = format!("openapi/{family}.json");
            let path = output.join(&relative);
            let bytes = fs::read(&path).map_err(|error| {
                CliError::new(format!(
                    "cannot read OpenAPI projection {relative}: {error}"
                ))
            })?;
            Ok(json!({
                "family": family,
                "path": relative,
                "sha256": hash_bytes(&bytes),
                "bytes": bytes.len(),
            }))
        })
        .collect::<Result<Vec<_>, CliError>>()?;

    let adaptation_relative = "openapi/workers-powershell-adaptation.ps1";
    let adaptation_path = output.join(adaptation_relative);
    let adaptation = fs::read(&adaptation_path).map_err(|error| {
        CliError::new(format!(
            "cannot read Rust-owned PowerShell adaptation {adaptation_relative}: {error}"
        ))
    })?;
    let anchor_markers = OPENAPI_ANCHOR_MARKERS
        .iter()
        .filter(|marker| String::from_utf8_lossy(&adaptation).contains(**marker))
        .map(|marker| *marker)
        .collect::<Vec<_>>();
    if anchor_markers.len() != OPENAPI_ANCHOR_MARKERS.len() {
        let missing = OPENAPI_ANCHOR_MARKERS
            .iter()
            .filter(|marker| !anchor_markers.contains(marker))
            .copied()
            .collect::<Vec<_>>();
        return Err(CliError::new(format!(
            "Rust-owned PowerShell adaptation is missing anchor markers: {}",
            missing.join(", ")
        )));
    }

    write_json_value(
        &output.join("openapi/stage-receipt.json"),
        &json!({
            "schema": OPENAPI_STAGE_RECEIPT_SCHEMA,
            "stage": "sdk-openapi-prototype",
            "transport": "HTTP/JSON projection",
            "authority": {
                "contract": "rust/crates/sdk-contract-wire",
                "symbol": "acyclic_sdk_contract_wire::WORKERS",
                "projection": "rust/crates/sdk-openapi-prototype",
            },
            "openapi_generator_qualification": {
                "name": "OpenAPI Generator",
                "release": OPENAPI_GENERATOR_RELEASE,
                "commit": OPENAPI_GENERATOR_COMMIT,
                "jar_sha256": OPENAPI_GENERATOR_JAR_SHA256,
                "receipt": OPENAPI_QUALIFICATION_RECEIPT,
                "role": "pinned package qualification metadata; Rust remains the projection authority",
            },
            "license": {
                "name": OPENAPI_LICENSE_NAME,
                "url": OPENAPI_LICENSE_URL,
                "scope": "metadata overlay only; routes, schemas, and Rust-owned extensions remain sourced from the Workers projection",
                "receipt": OPENAPI_QUALIFICATION_RECEIPT,
                "global_claim": false,
                "target_scopes": {
                    "rust": "generated Cargo.toml license field from the Workers Apache metadata overlay",
                    "powershell": "generated module license URI and local package metadata",
                    "java": "generated POM license metadata",
                    "perl": null
                },
            },
            "adaptation": {
                "path": adaptation_relative,
                "sha256": hash_bytes(&adaptation),
                "bytes": adaptation.len(),
                "authority": "rust/crates/sdk-openapi-prototype/src/lib.rs:powershell_workers_adaptation_script",
                "anchor_check": {
                    "status": "passed",
                    "markers": anchor_markers,
                },
                "shared_behavior": "none; generated package scaffolding and shared validation stay outside this Rust-owned adapter",
            },
            "projections": projections,
        }),
    )
}

fn verify_immutable_baseline(family: &str, path: &Path) -> Result<(), String> {
    let expected = IMMUTABLE_BASELINES
        .iter()
        .find_map(|(name, digest)| (*name == family).then_some(*digest))
        .ok_or_else(|| format!("{family} has no pinned compatibility identity"))?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("{family} baseline metadata failed: {error}"))?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "{family} compatibility baseline is a symlink: {}",
            path.display()
        ));
    }
    let bytes = fs::read(path)
        .map_err(|error| format!("{family} compatibility baseline read failed: {error}"))?;
    let actual = hash_bytes(&bytes);
    let actual = actual
        .strip_prefix("sha256:")
        .expect("hash_bytes always returns a sha256 digest");
    if actual != expected {
        return Err(format!(
            "{family} compatibility baseline changed: expected sha256:{expected}, got sha256:{actual}"
        ));
    }
    Ok(())
}

fn baseline_for_family(fixtures: &Path, family: &str) -> Option<PathBuf> {
    let prefix = format!("{family}-");
    let mut matches = fs::read_dir(fixtures)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".descriptor.bin"))
        })
        .collect::<Vec<_>>();
    matches.sort();
    (matches.len() == 1).then(|| matches.remove(0))
}

fn contract_inputs(root: &Path, tool: &str) -> Vec<String> {
    let mut inputs = Vec::new();
    match tool {
        "sdk-contract-wire" | "sdk-openapi-prototype" => {
            for path in [
                "rust/crates/sdk-contract-wire",
                "rust/crates/sdk-contract-validation",
                "rust/crates/sdk-contract-wire/tests/fixtures",
                "compatibility/manifest.json",
                "languages/generation-targets.json",
            ] {
                if root.join(path).exists() {
                    inputs.push(path.into());
                }
            }
        }
        "sdk-product-artifacts" => {
            for path in [
                "rust/crates/sdk-contract-wire",
                "rust/crates/sdk-contract-options",
                "rust/crates/sdk-contract-validation",
            ] {
                if root.join(path).exists() {
                    inputs.push(path.into());
                }
            }
        }
        "sdk-language-producers" => {
            for path in [
                "languages/generation-targets.json",
                "languages/package-names.json",
                "compatibility/manifest.json",
            ] {
                if root.join(path).exists() {
                    inputs.push(path.into());
                }
            }
        }
        "sdk-docs" => {
            for path in ["docs", "rust"] {
                if root.join(path).exists() {
                    inputs.push(path.into());
                }
            }
        }
        "sdk-examples" => {
            for path in ["examples", "rust"] {
                if root.join(path).exists() {
                    inputs.push(path.into());
                }
            }
        }
        "sdk-python" => {
            for path in [
                "rust/crates/sdk-contract-wire",
                "rust/crates/sdk-python",
                "languages/generation-targets.json",
            ] {
                if root.join(path).exists() {
                    inputs.push(path.into());
                }
            }
        }
        "sdk-typescript" | "sdk-typescript-rpc-contracts" => {
            for path in [
                "rust/crates/sdk-typescript",
                "rust/crates/sdk-contract-wire",
                "compatibility/objects/v1/objects_descriptor.bin",
                "typescript/packages",
            ] {
                if root.join(path).exists() {
                    inputs.push(path.into());
                }
            }
        }
        _ => {}
    }
    inputs
}

fn tool_command(
    root: &Path,
    spec: &ToolSpec,
    operation: Operation,
    request: &Path,
    output: &Path,
    source: &SourceIdentity,
) -> Option<Vec<std::ffi::OsString>> {
    let output_path = output.to_path_buf();
    if spec.id == "sdk-contract-wire" {
        let manifest = spec.manifest.as_ref()?;
        let wire_operation = if operation == Operation::Check {
            "generate"
        } else {
            operation_name(operation)
        };
        return Some(vec![
            cargo_program(),
            OsString::from("run"),
            OsString::from("--manifest-path"),
            manifest.as_os_str().to_os_string(),
            OsString::from("--locked"),
            OsString::from("--bin"),
            OsString::from("sdk-contract-wire"),
            OsString::from("--"),
            OsString::from(wire_operation),
            OsString::from("--out"),
            output_path.join("wire").as_os_str().to_os_string(),
        ]);
    }
    if spec.id == "sdk-docs" {
        let manifest = spec.manifest.as_ref()?;
        let mut command = vec![
            cargo_program(),
            OsString::from("run"),
            OsString::from("--manifest-path"),
            manifest.as_os_str().to_os_string(),
            OsString::from("--locked"),
            OsString::from("--"),
            OsString::from("--repo-root"),
            root.as_os_str().to_os_string(),
            OsString::from("--output"),
            output_path.join("docs.json").as_os_str().to_os_string(),
            OsString::from("--website-output"),
            output_path.join("website.json").as_os_str().to_os_string(),
            OsString::from("--source-revision"),
            OsString::from(source.revision.as_str()),
        ];
        command.extend([
            OsString::from("--rustdoc-json"),
            output.join("rustdoc-json").as_os_str().to_os_string(),
            OsString::from("--strict-rustdoc-json"),
        ]);
        let profile = root.join("docs/rustdoc-profiles.json");
        if profile.is_file() {
            command.extend([
                OsString::from("--profile-manifest"),
                profile.as_os_str().to_os_string(),
            ]);
        }
        let authority = output.join("source-authority.json");
        if authority.is_file() {
            let authority_sha256 = hash_bytes(&fs::read(&authority).ok()?);
            command.extend([
                OsString::from("--examples-bundle"),
                output.as_os_str().to_os_string(),
                OsString::from("--source-authority"),
                authority.as_os_str().to_os_string(),
                OsString::from("--source-authority-sha256"),
                OsString::from(authority_sha256),
            ]);
        }
        // Registry metadata is Rust-owned input to the docs bundle. Keep the
        // path explicit in the generated command so a release or preview can
        // be reproduced from the exact checkout without relying on ambient
        // defaults.
        let registry_manifest = root.join("release/cargo-registry-metadata.json");
        if registry_manifest.is_file() {
            command.extend([
                OsString::from("--registry-manifest"),
                registry_manifest.as_os_str().to_os_string(),
            ]);
        }
        return Some(command);
    }
    if spec.id == "sdk-python" {
        let manifest = spec.manifest.as_ref()?;
        let mut command = vec![
            cargo_program(),
            OsString::from("run"),
            OsString::from("--manifest-path"),
            manifest.as_os_str().to_os_string(),
            OsString::from("--locked"),
            OsString::from("--"),
            OsString::from(if operation == Operation::Check {
                "generate"
            } else {
                operation_name(operation)
            }),
            OsString::from("--schema-root"),
            // The Python generator validates the Rust authority manifest and
            // exported schemas emitted by the preceding wire stage.
            output.join("wire").as_os_str().to_os_string(),
            OsString::from("--output"),
            output.join("python").as_os_str().to_os_string(),
        ];
        if let Some(script) = &spec.script {
            command.extend([
                OsString::from("--python-script"),
                script.as_os_str().to_os_string(),
            ]);
        }
        return Some(command);
    }
    if spec.id == "sdk-typescript" {
        let manifest = spec.manifest.as_ref()?;
        let mode = match operation {
            // `check` runs against a fresh `.check` output tree. Generate
            // into that tree, then compare the complete artifact set in the
            // orchestrator; a destination-only check would always fail before
            // drift comparison because the tree starts empty.
            Operation::Generate | Operation::Check => "packages-write",
            Operation::Drift | Operation::Qualify | Operation::QualifyEmbedded | Operation::Inventory => "packages-check",
        };
        return Some(vec![
            cargo_program(),
            OsString::from("run"),
            OsString::from("--manifest-path"),
            manifest.as_os_str().to_os_string(),
            OsString::from("--locked"),
            OsString::from("--bin"),
            OsString::from("sdk-typescript"),
            OsString::from("--"),
            OsString::from(mode),
            // Keep source identity and generated artifacts separate. The
            // package generator reads the pinned Rust model and emits the
            // installable package tree under the generation output root.
            root.as_os_str().to_os_string(),
            output.as_os_str().to_os_string(),
            output.join("wire").as_os_str().to_os_string(),
            OsString::from(source.revision.as_str()),
        ]);
    }
    if spec.id == "sdk-typescript-rpc-contracts" {
        let manifest = spec.manifest.as_ref()?;
        return Some(vec![
            cargo_program(),
            OsString::from("run"),
            OsString::from("--manifest-path"),
            manifest.as_os_str().to_os_string(),
            OsString::from("--locked"),
            OsString::from("--bin"),
            OsString::from("sdk-rpc-contracts"),
            OsString::from("--"),
            OsString::from("--source-root"),
            root.as_os_str().to_os_string(),
            OsString::from("--output"),
            output.join("typescript/rpc-contracts.json").as_os_str().to_os_string(),
        ]);
    }
    if let Some(script) = &spec.script {
        let mut command = if script.extension() == Some(OsStr::new("py")) {
            vec![OsString::from("python"), script.as_os_str().to_os_string()]
        } else {
            vec![script.as_os_str().to_os_string()]
        };
        command.extend([
            OsString::from("--request"),
            request.as_os_str().to_os_string(),
        ]);
        return Some(command);
    }
    let manifest = spec.manifest.as_ref()?;
    let mut command = vec![
        cargo_program(),
        OsString::from("run"),
        OsString::from("--manifest-path"),
        manifest.as_os_str().to_os_string(),
        OsString::from("--locked"),
    ];
    if spec.id == "sdk-contract-wire" {
        command[1] = OsString::from("test");
        return Some(command);
    }
    if spec.id == "sdk-examples" {
        command.extend([OsString::from("--bin"), OsString::from("sdk-examples")]);
    }
    command.push(OsString::from("--"));
    if spec.id == "sdk-openapi-prototype" {
        let _ = fs::create_dir_all(output.join("openapi"));
        command.push(
            output
                .join("openapi/actors.json")
                .as_os_str()
                .to_os_string(),
        );
    } else {
        let tool_operation = if spec.id == "sdk-examples" && operation == Operation::Check {
            "generate"
        } else {
            operation_name(operation)
        };
        command.extend([
            OsString::from(tool_operation),
            OsString::from("--request"),
            request.as_os_str().to_os_string(),
        ]);
    }
    Some(command)
}

fn tool_result(
    id: &str,
    required: bool,
    request: String,
    command: Vec<String>,
    process: Output,
    output: &Path,
) -> Result<ToolResult, CliError> {
    let logs = output.join("logs");
    fs::create_dir_all(&logs)?;
    fs::write(logs.join(format!("{id}.stdout")), &process.stdout)?;
    fs::write(logs.join(format!("{id}.stderr")), &process.stderr)?;
    Ok(ToolResult {
        id: id.into(),
        status: if process.status.success() {
            "passed".into()
        } else {
            "failed".into()
        },
        required,
        command,
        request,
        stdout_sha256: Some(hash_bytes(&process.stdout)),
        stderr_sha256: Some(hash_bytes(&process.stderr)),
        exit_code: process.status.code(),
        message: (!process.status.success())
            .then(|| String::from_utf8_lossy(&process.stderr).trim().to_owned()),
    })
}

/// Materialize the examples producer's source closure as an independent
/// authority input for sdk-docs. The docs importer verifies every file byte,
/// the closure digest, and this manifest's SHA-256 before it accepts the
/// scenario bundle.
fn write_source_authority_manifest(output: &Path, expected_revision: &str) -> Result<(), CliError> {
    let examples_manifest_path = output.join("sdk-examples-manifest.json");
    let examples: Value = read_json(&examples_manifest_path).map_err(|error| {
        CliError::new(format!(
            "cannot read sdk-examples manifest for source authority: {error}"
        ))
    })?;
    let source = examples
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| CliError::new("sdk-examples manifest has no source object"))?;
    let revision = source
        .get("revision")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("sdk-examples source authority has no revision"))?;
    if revision != expected_revision {
        return Err(CliError::new(format!(
            "sdk-examples source authority revision differs from generation source: expected {expected_revision}, got {revision}"
        )));
    }
    let source_path = source
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("sdk-examples source authority has no source path"))?;
    let source_sha256 = source
        .get("sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("sdk-examples source authority has no source digest"))?;
    let source_files = source
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::new("sdk-examples source authority has no source file list"))?;
    if source_files.is_empty() {
        return Err(CliError::new(
            "sdk-examples source authority has an empty source file list",
        ));
    }
    let request: Value = read_json(&output.join("requests/sdk-examples.json"))?;
    let source_root = request
        .get("source_root")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("sdk-examples request has no source root"))?;
    let mut source_file_hashes = serde_json::Map::new();
    for item in source_files {
        let relative = item
            .as_str()
            .ok_or_else(|| CliError::new("sdk-examples source authority file is not a string"))?;
        let source_path = Path::new(source_root).join(relative);
        let bytes = fs::read(&source_path).map_err(|error| {
            CliError::new(format!(
                "cannot read sdk-examples authority file {relative}: {error}"
            ))
        })?;
        source_file_hashes.insert(relative.to_owned(), Value::String(hash_bytes(&bytes)));
    }
    let authority = json!({
        "schema": "acyclic.sdk.examples.source-authority.v1",
        "source_revision": revision,
        "source_path": source_path,
        "source_sha256": source_sha256,
        "source_files": source_files,
        "source_file_hashes": source_file_hashes,
    });
    write_json_value(&output.join("source-authority.json"), &authority)
}

fn canonicalize_language_families(
    families: serde_json::Map<String, Value>,
    aliases: &BTreeMap<String, String>,
) -> serde_json::Map<String, Value> {
    let mut normalized = BTreeMap::new();
    for (id, family) in families {
        let canonical = aliases.get(&id).cloned().unwrap_or(id.clone());
        // Prefer an explicitly canonical entry when both a legacy alias and
        // the canonical key are present in a package manifest.
        if id == canonical || !normalized.contains_key(&canonical) {
            normalized.insert(canonical, family);
        }
    }
    normalized.into_iter().collect()
}

fn language_inventory(
    root: &Path,
    output: &Path,
    revision: &str,
    expectations: Option<&EvidenceExpectations>,
) -> Result<Vec<LanguageStatus>, CliError> {
    let package_names = root.join("languages/package-names.json");
    let value: Value = if package_names.is_file() {
        read_json(&package_names)?
    } else {
        json!({ "families": {} })
    };
    let mut families = value
        .get("families")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    // Package manifests may retain ecosystem spellings such as `jvm` and
    // `dotnet`, but the Rust-owned target catalog has one canonical identity
    // per language.  Normalize aliases before producing qualification rows so
    // aliases cannot become phantom independent SDK targets.
    let mut aliases = BTreeMap::new();
    for (id, family) in &families {
        if let Some(items) = family.get("aliases").and_then(Value::as_array) {
            for alias in items.iter().filter_map(Value::as_str) {
                aliases.insert(alias.to_owned(), id.clone());
            }
        }
    }
    families = canonicalize_language_families(families, &aliases);
    let targets_path = root.join("languages/generation-targets.json");
    if targets_path.is_file() {
        let targets: Value = read_json(&targets_path)?;
        if let Some(targets) = targets.get("targets").and_then(Value::as_array) {
            for target in targets {
                let Some(id) = target.get("id").and_then(Value::as_str) else {
                    continue;
                };
                if let Some(items) = target.get("aliases").and_then(Value::as_array) {
                    for alias in items.iter().filter_map(Value::as_str) {
                        aliases.insert(alias.to_owned(), id.to_owned());
                    }
                }
            }
            families = canonicalize_language_families(families, &aliases);
            for target in targets {
                let Some(raw_id) = target.get("id").and_then(Value::as_str) else {
                    continue;
                };
                let id = aliases.get(raw_id).map(String::as_str).unwrap_or(raw_id);
                if let Some(existing) = families.get_mut(id) {
                    if existing.get("status").is_none() {
                        if let Some(object) = existing.as_object_mut() {
                            object.insert(
                                "status".into(),
                                target
                                    .get("status")
                                    .cloned()
                                    .unwrap_or_else(|| Value::String("candidate".into())),
                            );
                        }
                    }
                    continue;
                }
                let registry = target
                    .get("package")
                    .and_then(|package| package.get("ecosystem"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let package = target
                    .get("package")
                    .and_then(|package| package.get("artifact"))
                    .and_then(Value::as_str);
                let classification = target
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("candidate");
                families.insert(
                    id.to_owned(),
                    json!({
                        "registry": registry,
                        "umbrella": package,
                        "status": classification,
                    }),
                );
            }
        }
    }
    if families.is_empty() {
        return Err(CliError::new(
            "language inventory is empty; provide languages/package-names.json or languages/generation-targets.json",
        ));
    }
    let mut result = Vec::new();
    for (id, family) in families {
        let evidence_path = output.join("qualification").join(format!("{id}.json"));
        let evidence_relative = relative_or_absolute(&evidence_path, output);
        let evidence = evidence_path
            .is_file()
            .then(|| read_json::<Evidence>(&evidence_path).ok())
            .flatten();
        let binding_valid = expectations.is_some()
            && evidence.as_ref().is_some_and(|evidence| {
                let expected = expectations.as_ref().expect("expectations checked");
                evidence.schema == EVIDENCE_SCHEMA
                    && evidence.language == id
                    && evidence.source_revision == revision
                    && is_sha256(&evidence.contract_digest)
                    && is_sha256(&evidence.artifact_digest)
                    && evidence.source_revision == expected.source_revision
                    && evidence.contract_digest == expected.contract_digest
                    && evidence.artifact_digest == expected.artifact_digest
            });
        let classification = family
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("candidate")
            .to_owned();
        let excluded = matches!(
            classification.as_str(),
            "not-qualifiable" | "adapter-only" | "excluded"
        );
        let package = family
            .get("umbrella")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let registry = family
            .get("registry")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let (remote, embedded, docs, snippets, install, outstanding) = if excluded {
            (
                "excluded".into(),
                "excluded".into(),
                "excluded".into(),
                "excluded".into(),
                "excluded".into(),
                vec![format!("excluded:{classification}")],
            )
        } else if binding_valid {
            let evidence = evidence.as_ref().expect("validated evidence");
            let expected = expectations.as_ref().expect("validated expectations");
            let mut missing = Vec::new();
            for (name, capability) in [
                ("remote", &evidence.remote),
                ("embedded", &evidence.embedded),
                ("docs", &evidence.docs),
                ("snippets", &evidence.snippets),
                ("install", &evidence.install),
            ] {
                if capability_excluded(capability) {
                    continue;
                }
                match capability_qualification(output, &id, name, capability, expected) {
                    CapabilityQualification::Qualified => {}
                    CapabilityQualification::Partial => {
                        let scope = capability
                            .scope
                            .as_deref()
                            .filter(|scope| !scope.trim().is_empty());
                        missing.push(match scope {
                            Some(scope) => format!("partial:{name}:{scope}"),
                            None => format!("pending:{name}:partial-scope-required"),
                        });
                    }
                    CapabilityQualification::Invalid => missing.push(format!("pending:{name}")),
                }
            }
            let capability_status = |name: &str, capability: &CapabilityEvidence| {
                if capability_excluded(capability) {
                    "excluded".to_owned()
                } else {
                    match capability_qualification(output, &id, name, capability, expected) {
                        CapabilityQualification::Qualified => "qualified".to_owned(),
                        CapabilityQualification::Partial
                            if capability
                                .scope
                                .as_deref()
                                .is_some_and(|scope| !scope.trim().is_empty()) =>
                        {
                            "partial".to_owned()
                        }
                        CapabilityQualification::Partial | CapabilityQualification::Invalid => {
                            "pending".to_owned()
                        }
                    }
                }
            };
            (
                capability_status("remote", &evidence.remote),
                capability_status("embedded", &evidence.embedded),
                capability_status("docs", &evidence.docs),
                capability_status("snippets", &evidence.snippets),
                capability_status("install", &evidence.install),
                missing,
            )
        } else {
            (
                "pending".into(),
                "pending".into(),
                "pending".into(),
                "pending".into(),
                "pending".into(),
                vec!["pending:qualification evidence".into()],
            )
        };
        result.push(LanguageStatus {
            id,
            classification,
            excluded,
            registry,
            package,
            remote,
            embedded,
            docs,
            snippets,
            install,
            evidence: if evidence_path.is_file() {
                vec![evidence_relative]
            } else {
                Vec::new()
            },
            outstanding,
        });
    }
    result.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(result)
}

fn evidence_expectations(
    source_root: &Path,
    output: &Path,
) -> Result<Option<EvidenceExpectations>, CliError> {
    let path = output.join("sdk-generation-manifest.json");
    if !path.is_file() {
        return Ok(None);
    }
    let manifest: Manifest = read_json(&path)?;
    if manifest.schema != GENERATION_SCHEMA {
        return Err(CliError::new(
            "qualification manifest has an unsupported generation schema",
        ));
    }
    if manifest.status != "generated" {
        return Err(CliError::pending(format!(
            "qualification requires a generated manifest, found {}",
            manifest.status
        )));
    }
    verify_required_tools(&manifest.tools, source_root, output)?;
    if manifest.source.dirty || manifest.source.revision == "uncommitted" {
        return Err(CliError::new(
            "qualification requires a clean, committed generation source identity",
        ));
    }
    if !is_sha256(&manifest.source.digest) {
        return Err(CliError::new(
            "generation manifest has an invalid source digest",
        ));
    }
    if !is_commit_revision(&manifest.source.revision) {
        return Err(CliError::new(
            "generation manifest has an invalid source commit revision",
        ));
    }
    verify_authority_manifest(source_root, output)?;
    // Qualification must bind to bytes that are still present on disk. A
    // caller cannot manufacture a matching artifact-list digest after the
    // generated output has been removed or altered.
    verify_artifacts(output, &manifest.artifacts)?;
    let expected_artifact_digest = artifact_digest(&manifest.artifacts);
    if manifest.artifact_digest.as_deref() != Some(expected_artifact_digest.as_str()) {
        return Err(CliError::new(
            "generation manifest artifact digest does not match its artifact set",
        ));
    }
    Ok(Some(EvidenceExpectations {
        source_revision: manifest.source.revision,
        contract_digest: manifest.source.digest,
        artifact_digest: expected_artifact_digest,
        artifacts: manifest.artifacts,
    }))
}

fn verify_required_tools(
    tools: &[ToolResult],
    source_root: &Path,
    output: &Path,
) -> Result<(), CliError> {
    let source_binding = source_root.to_string_lossy().replace('\\', "/");
    let output_binding = output.to_string_lossy().replace('\\', "/");
    let expected = REQUIRED_TOOL_IDS.iter().copied().collect::<BTreeSet<_>>();
    let optional = OPTIONAL_TOOL_IDS.iter().copied().collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut invalid = BTreeSet::new();
    for tool in tools {
        if !seen.insert(tool.id.as_str())
            || (!expected.contains(tool.id.as_str()) && !optional.contains(tool.id.as_str()))
        {
            invalid.insert(tool.id.as_str());
            continue;
        }
        if optional.contains(tool.id.as_str()) {
            if tool.required {
                invalid.insert(tool.id.as_str());
            }
            continue;
        }
        if !tool.required {
            invalid.insert(tool.id.as_str());
            continue;
        }
        let hashes_valid = tool.stdout_sha256.as_deref().is_some_and(is_sha256)
            && tool.stderr_sha256.as_deref().is_some_and(is_sha256);
        let command_text = tool
            .command
            .iter()
            .map(|part| part.replace('\\', "/"))
            .collect::<Vec<_>>();
        let source_bound = command_text.iter().any(|part| part == &source_binding);
        let output_bound = command_text.iter().any(|part| part == &output_binding);
        let retained_logs_bound = [
            ("stdout", tool.stdout_sha256.as_deref()),
            ("stderr", tool.stderr_sha256.as_deref()),
        ]
        .into_iter()
        .all(|(stream, expected)| {
            let Some(expected) = expected else {
                return false;
            };
            let path = output.join("logs").join(format!("{}.{}", tool.id, stream));
            let Ok(metadata) = fs::symlink_metadata(&path) else {
                return false;
            };
            metadata.file_type().is_file()
                && fs::read(path)
                    .ok()
                    .is_some_and(|bytes| hash_bytes(&bytes) == expected)
        });
        if tool.status != "passed"
            || tool.exit_code != Some(0)
            || !hashes_valid
            || !source_bound
            || !output_bound
            || !retained_logs_bound
        {
            invalid.insert(tool.id.as_str());
        }
    }
    for id in REQUIRED_TOOL_IDS {
        if !seen.contains(id) {
            invalid.insert(id);
        }
    }
    if invalid.is_empty() {
        return Ok(());
    }
    Err(CliError::new(format!(
        "generated manifest has required stages without valid passed, hashed, source-bound results: {}",
        invalid.into_iter().collect::<Vec<_>>().join(", ")
    )))
}

fn is_commit_revision(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_portable_relative(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\\')
        && !path.contains("//")
        && Path::new(path).is_relative()
        && !Path::new(path).components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
}

fn authority_path(root: &Path, relative: &str) -> Result<PathBuf, CliError> {
    if !is_portable_relative(relative) {
        return Err(CliError::new(format!(
            "Rust authority manifest contains a non-portable path: {relative}"
        )));
    }
    let path = root.join(relative);
    let canonical_root = fs::canonicalize(root)
        .map_err(|error| CliError::new(format!("authority root cannot be resolved: {error}")))?;
    let canonical_path = fs::canonicalize(&path).map_err(|error| {
        CliError::new(format!(
            "Rust authority manifest path is missing: {relative}: {error}"
        ))
    })?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(CliError::new(format!(
            "Rust authority manifest path escapes its root: {relative}"
        )));
    }
    Ok(canonical_path)
}

enum DescriptorField<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    Fixed,
}

fn descriptor_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, CliError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| CliError::new("truncated descriptor varint"))?;
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(CliError::new("descriptor varint exceeds 64 bits"))
}

fn descriptor_fields(bytes: &[u8]) -> Result<Vec<(u32, DescriptorField<'_>)>, CliError> {
    let mut fields = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let key = descriptor_varint(bytes, &mut cursor)?;
        let number = u32::try_from(key >> 3)
            .map_err(|_| CliError::new("descriptor field number exceeds 32 bits"))?;
        if number == 0 {
            return Err(CliError::new("descriptor contains field number zero"));
        }
        let wire_type = key & 7;
        let value = match wire_type {
            0 => DescriptorField::Varint(descriptor_varint(bytes, &mut cursor)?),
            1 => {
                let end = cursor
                    .checked_add(8)
                    .ok_or_else(|| CliError::new("descriptor fixed field overflows"))?;
                if end > bytes.len() {
                    return Err(CliError::new("truncated descriptor fixed field"));
                }
                cursor = end;
                DescriptorField::Fixed
            }
            2 => {
                let length = usize::try_from(descriptor_varint(bytes, &mut cursor)?)
                    .map_err(|_| CliError::new("descriptor length exceeds platform size"))?;
                let end = cursor
                    .checked_add(length)
                    .ok_or_else(|| CliError::new("descriptor length overflows"))?;
                if end > bytes.len() {
                    return Err(CliError::new("truncated descriptor bytes field"));
                }
                let value = DescriptorField::Bytes(&bytes[cursor..end]);
                cursor = end;
                value
            }
            5 => {
                let end = cursor
                    .checked_add(4)
                    .ok_or_else(|| CliError::new("descriptor fixed field overflows"))?;
                if end > bytes.len() {
                    return Err(CliError::new("truncated descriptor fixed field"));
                }
                cursor = end;
                DescriptorField::Fixed
            }
            _ => return Err(CliError::new("descriptor contains unsupported wire type")),
        };
        fields.push((number, value));
    }
    Ok(fields)
}

fn descriptor_rpc_shapes(bytes: &[u8], source: &str) -> Result<BTreeSet<String>, CliError> {
    let mut matching_files = 0;
    let mut shapes = BTreeSet::new();
    for (number, value) in descriptor_fields(bytes)? {
        let DescriptorField::Bytes(file_bytes) = value else {
            continue;
        };
        if number != 1 {
            continue;
        }
        let file_fields = descriptor_fields(file_bytes)?;
        let file_name = file_fields
            .iter()
            .find_map(|(field, value)| {
                (*field == 1).then(|| match value {
                    DescriptorField::Bytes(bytes) => std::str::from_utf8(bytes).ok(),
                    _ => None,
                })
            })
            .flatten();
        if file_name != Some(source) {
            continue;
        }
        matching_files += 1;
        for (field, value) in file_fields {
            if field != 6 {
                continue;
            }
            let DescriptorField::Bytes(service_bytes) = value else {
                return Err(CliError::new("descriptor service field is not bytes"));
            };
            for (method_field, method_value) in descriptor_fields(service_bytes)? {
                if method_field != 2 {
                    continue;
                }
                let DescriptorField::Bytes(method_bytes) = method_value else {
                    return Err(CliError::new("descriptor method field is not bytes"));
                };
                let mut client = false;
                let mut server = false;
                for (stream_field, stream_value) in descriptor_fields(method_bytes)? {
                    match (stream_field, stream_value) {
                        (5, DescriptorField::Varint(value)) => client = value != 0,
                        (6, DescriptorField::Varint(value)) => server = value != 0,
                        _ => {}
                    }
                }
                shapes.insert(
                    match (client, server) {
                        (false, false) => "unary",
                        (true, false) => "client",
                        (false, true) => "server",
                        (true, true) => "bidi",
                    }
                    .to_owned(),
                );
            }
        }
    }
    if matching_files != 1 {
        return Err(CliError::new(format!(
            "family descriptor has {matching_files} source file entries: {source}"
        )));
    }
    Ok(shapes)
}

/// Return the canonical RPC identities and their descriptor-derived shapes for
/// one authority file.  The identity intentionally comes from the descriptor
/// package/service/method names rather than a receipt's feature labels:
/// `package.Service/Method`.
fn descriptor_rpc_methods(
    bytes: &[u8],
    source: &str,
) -> Result<BTreeMap<String, String>, CliError> {
    let mut matching_files = 0;
    let mut methods = BTreeMap::new();
    for (number, value) in descriptor_fields(bytes)? {
        let DescriptorField::Bytes(file_bytes) = value else {
            continue;
        };
        if number != 1 {
            continue;
        }
        let file_fields = descriptor_fields(file_bytes)?;
        let file_name = file_fields
            .iter()
            .find_map(|(field, value)| {
                (*field == 1).then(|| match value {
                    DescriptorField::Bytes(bytes) => std::str::from_utf8(bytes).ok(),
                    _ => None,
                })
            })
            .flatten();
        if file_name != Some(source) {
            continue;
        }
        matching_files += 1;
        let package = file_fields
            .iter()
            .find_map(|(field, value)| {
                (*field == 2).then(|| match value {
                    DescriptorField::Bytes(bytes) => std::str::from_utf8(bytes).ok(),
                    _ => None,
                })
            })
            .flatten()
            .unwrap_or_default();
        for (field, value) in file_fields {
            if field != 6 {
                continue;
            }
            let DescriptorField::Bytes(service_bytes) = value else {
                return Err(CliError::new("descriptor service field is not bytes"));
            };
            let service_fields = descriptor_fields(service_bytes)?;
            let service = service_fields
                .iter()
                .find_map(|(field, value)| {
                    (*field == 1).then(|| match value {
                        DescriptorField::Bytes(bytes) => std::str::from_utf8(bytes).ok(),
                        _ => None,
                    })
                })
                .flatten()
                .ok_or_else(|| CliError::new("descriptor service has no valid name"))?;
            for (method_field, method_value) in service_fields {
                if method_field != 2 {
                    continue;
                }
                let DescriptorField::Bytes(method_bytes) = method_value else {
                    return Err(CliError::new("descriptor method field is not bytes"));
                };
                let method_fields = descriptor_fields(method_bytes)?;
                let method = method_fields
                    .iter()
                    .find_map(|(field, value)| {
                        (*field == 1).then(|| match value {
                            DescriptorField::Bytes(bytes) => std::str::from_utf8(bytes).ok(),
                            _ => None,
                        })
                    })
                    .flatten()
                    .ok_or_else(|| CliError::new("descriptor method has no valid name"))?;
                let mut client = false;
                let mut server = false;
                for (stream_field, stream_value) in method_fields {
                    match (stream_field, stream_value) {
                        (5, DescriptorField::Varint(value)) => client = value != 0,
                        (6, DescriptorField::Varint(value)) => server = value != 0,
                        _ => {}
                    }
                }
                let shape = match (client, server) {
                    (false, false) => "unary",
                    (true, false) => "client",
                    (false, true) => "server",
                    (true, true) => "bidi",
                };
                let identity = if package.is_empty() {
                    format!("{service}/{method}")
                } else {
                    format!("{package}.{service}/{method}")
                };
                if methods.insert(identity, shape.to_owned()).is_some() {
                    return Err(CliError::new("descriptor contains duplicate RPC identity"));
                }
            }
        }
    }
    if matching_files != 1 {
        return Err(CliError::new(format!(
            "family descriptor has {matching_files} source file entries: {source}"
        )));
    }
    Ok(methods)
}

fn verify_authority_manifest(source_root: &Path, output: &Path) -> Result<(), CliError> {
    let path = output.join("wire/rust-authority.json");
    if !path.is_file() {
        return Err(CliError::new(
            "Rust authority manifest is missing from generated wire output",
        ));
    }
    authority_path(&output.join("wire"), "validation/v1/options.proto")?;
    let value: Value = read_json(&path)?;
    if value.get("schema").and_then(Value::as_str) != Some("acyclic.sdk.rust-authority.v1")
        || value.get("authority").and_then(Value::as_str) != Some("rust")
    {
        return Err(CliError::new("invalid Rust authority manifest identity"));
    }
    let model_revision = value
        .get("source_revision")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::new("Rust authority manifest has no model source revision"))?;
    if model_revision.len() != 64 || !model_revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CliError::new(
            "Rust authority manifest has an invalid model source revision",
        ));
    }
    let source_files = value
        .get("source_files")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::new("Rust authority manifest has no source file list"))?;
    let source_hashes = value
        .get("source_file_hashes")
        .and_then(Value::as_object)
        .ok_or_else(|| CliError::new("Rust authority manifest has no source file hashes"))?;
    if source_files.is_empty() || source_files.len() != source_hashes.len() {
        return Err(CliError::new(
            "Rust authority source file list and hash map differ",
        ));
    }
    let mut canonical = Vec::new();
    let mut seen_sources = BTreeSet::new();
    for item in source_files {
        let relative = item
            .as_str()
            .ok_or_else(|| CliError::new("Rust authority source file is not a string"))?;
        if !seen_sources.insert(relative.to_owned()) {
            return Err(CliError::new(format!(
                "duplicate Rust authority source file: {relative}"
            )));
        }
        let source_path = authority_path(source_root, relative)?;
        let bytes = fs::read(&source_path)?;
        let expected = source_hashes
            .get(relative)
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new(format!("missing source hash: {relative}")))?;
        if expected.len() != 64
            || !expected.bytes().all(|byte| byte.is_ascii_hexdigit())
            || hash_bytes(&bytes) != format!("sha256:{expected}")
        {
            return Err(CliError::new(format!(
                "Rust authority source hash mismatch: {relative}"
            )));
        }
        canonical.extend_from_slice(relative.as_bytes());
        canonical.push(0);
        canonical.extend_from_slice(&bytes);
        canonical.push(0);
    }
    if hash_bytes(&canonical) != format!("sha256:{model_revision}") {
        return Err(CliError::new(
            "Rust authority model source revision does not match source files",
        ));
    }

    let families = value
        .get("families")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::new("Rust authority manifest has no families"))?;
    if families.is_empty() {
        return Err(CliError::new(
            "Rust authority manifest has no family entries",
        ));
    }
    let required_families = FAMILY_VIEWS
        .iter()
        .map(|family| family.name)
        .collect::<BTreeSet<_>>();
    let mut seen_families = BTreeSet::new();
    let mut seen_descriptors = BTreeSet::new();
    for family in families {
        let source = family
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new("Rust authority family has no source path"))?;
        let family_name = source.split('/').next().unwrap_or_default();
        if required_families.contains(family_name) && !seen_families.insert(family_name) {
            return Err(CliError::new(format!(
                "duplicate Rust authority family: {family_name}"
            )));
        }
        let descriptor = family
            .get("descriptor")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new("Rust authority family has no descriptor path"))?;
        let source_path = authority_path(&output.join("wire"), source)?;
        let descriptor_path = authority_path(&output.join("wire"), descriptor)?;
        let source_hash = family
            .get("source_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new(format!("missing family source hash: {source}")))?;
        let descriptor_hash = family
            .get("descriptor_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                CliError::new(format!("missing family descriptor hash: {descriptor}"))
            })?;
        if source_hash.len() != 64
            || !source_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            || hash_bytes(&fs::read(source_path)?) != format!("sha256:{source_hash}")
        {
            return Err(CliError::new(format!(
                "family source hash mismatch: {source}"
            )));
        }
        if descriptor_hash.len() != 64
            || !descriptor_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            || hash_bytes(&fs::read(&descriptor_path)?) != format!("sha256:{descriptor_hash}")
        {
            return Err(CliError::new(format!(
                "family descriptor hash mismatch: {descriptor}"
            )));
        }
        let declared_rpc_shapes = family
            .get("rpc_shapes")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                CliError::new(format!("family has no RPC shape declaration: {descriptor}"))
            })?
            .iter()
            .map(|shape| {
                shape.as_str().ok_or_else(|| {
                    CliError::new(format!("family has a non-string RPC shape: {descriptor}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<String>>();
        if declared_rpc_shapes
            .iter()
            .any(|shape| !matches!(shape.as_str(), "unary" | "client" | "server" | "bidi"))
        {
            return Err(CliError::new(format!(
                "family has an invalid RPC shape declaration: {descriptor}"
            )));
        }
        let actual_rpc_shapes = descriptor_rpc_shapes(&fs::read(&descriptor_path)?, source)?;
        if declared_rpc_shapes != actual_rpc_shapes {
            return Err(CliError::new(format!(
                "family RPC shapes do not match descriptor services: {descriptor}"
            )));
        }
        if family.get("descriptor_role").and_then(Value::as_str) != Some("canonical_schema") {
            return Err(CliError::new(format!(
                "family descriptor is not marked canonical: {descriptor}"
            )));
        }
        if !seen_descriptors.insert(descriptor.to_owned()) {
            return Err(CliError::new(format!(
                "duplicate Rust authority descriptor: {descriptor}"
            )));
        }
    }
    let missing_families = required_families
        .difference(&seen_families)
        .copied()
        .collect::<Vec<_>>();
    if !missing_families.is_empty() {
        return Err(CliError::new(format!(
            "Rust authority manifest is missing registry families: {}",
            missing_families.join(", ")
        )));
    }
    Ok(())
}

fn artifact_digest(artifacts: &[Artifact]) -> String {
    let mut canonical = String::new();
    for artifact in artifacts {
        canonical.push_str(&artifact.path);
        canonical.push('\0');
        canonical.push_str(&artifact.sha256);
        canonical.push('\0');
    }
    hash_bytes(canonical.as_bytes())
}

fn is_sha256(value: &str) -> bool {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return false;
    };
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
fn is_sha256_digest_in(value: &str) -> bool {
    value.split_whitespace().any(|part| is_sha256(part))
}

/// Evidence test entries are receipts, not labels. The entry may contain a
/// human-readable case name, but one token must name an output-relative file
/// and another must be its exact SHA-256 digest, for example:
/// `stream-unary qualification/receipts/stream.json sha256:<64 hex>`. This
/// keeps qualification tied to bytes produced by the current generation run.
fn capability_excluded(capability: &CapabilityEvidence) -> bool {
    capability.status == "excluded"
        && capability
            .scope
            .as_deref()
            .is_some_and(|scope| !scope.trim().is_empty())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapabilityQualification {
    Invalid,
    Partial,
    Qualified,
}

fn capability_qualification(
    output: &Path,
    language: &str,
    capability: &str,
    evidence: &CapabilityEvidence,
    expected: &EvidenceExpectations,
) -> CapabilityQualification {
    if evidence.status != "qualified" || evidence.tests.is_empty() {
        return CapabilityQualification::Invalid;
    }
    let mut partial = false;
    for test in &evidence.tests {
        if !evidence_test_receipt(output, language, capability, expected, test) {
            return CapabilityQualification::Invalid;
        }
        match receipt_has_partial_scope(output, capability, test) {
            Some(true) => partial = true,
            Some(false) => {}
            None => return CapabilityQualification::Invalid,
        }
    }
    if partial {
        CapabilityQualification::Partial
    } else {
        CapabilityQualification::Qualified
    }
}

fn receipt_has_partial_scope(output: &Path, capability: &str, value: &str) -> Option<bool> {
    let digest = value
        .split_whitespace()
        .find(|part| is_sha256(part))
        .and_then(|part| part.strip_prefix("sha256:"))?;
    let token = value.split_whitespace().find_map(|token| {
        let token = token.strip_prefix("receipt=").unwrap_or(token);
        if token.starts_with("sha256:")
            || token.contains(':')
            || token.is_empty()
            || !is_portable_relative(token)
            || !token.starts_with("qualification/receipts/")
        {
            return None;
        }
        Some(token)
    })?;
    let path = output.join(token);
    let bytes = fs::read(&path).ok()?;
    if hash_bytes(&bytes) != format!("sha256:{digest}") {
        return None;
    }
    let receipt: Value = serde_json::from_slice(&bytes).ok()?;
    let authority_manifest = output.join("wire/rust-authority.json");
    if !authority_manifest.is_file() {
        return Some(false);
    }
    let authority = authority_rpc_inventory(output).ok()?;
    let families = receipt.get("families")?.as_array()?;
    let mut partial = false;
    for entry in families {
        let family = entry.get("family")?.as_str()?;
        let methods = string_set_field(entry, "methods")?;
        let shapes = string_set_field(entry, "rpc_shapes")?;
        let authority_methods = authority.get(family)?;
        if !receipt_family_matches_authority(entry, capability, Some(authority_methods)) {
            return None;
        }
        partial |= receipt_family_is_partial(&methods, &shapes, authority_methods)?;
    }
    Some(partial)
}

fn receipt_family_is_partial(
    methods: &BTreeSet<String>,
    shapes: &BTreeSet<String>,
    authority_methods: &BTreeMap<String, String>,
) -> Option<bool> {
    if authority_methods.is_empty() {
        return Some(false);
    }
    let expected_methods = authority_methods.keys().cloned().collect::<BTreeSet<_>>();
    let expected_shapes = authority_methods.values().cloned().collect::<BTreeSet<_>>();
    if methods.is_empty() || shapes.is_empty() {
        return None;
    }
    Some(*methods != expected_methods || *shapes != expected_shapes)
}

#[derive(Debug, Clone)]
struct ConsumerScenario {
    shape: String,
    checks: BTreeSet<String>,
}

fn consumer_scenario_inventory(
    output: &Path,
    receipt: &Value,
    expected: &EvidenceExpectations,
) -> Option<BTreeMap<(String, String), ConsumerScenario>> {
    let Some(consumer) = receipt.get("consumer") else {
        return None;
    };
    if consumer.get("executed").and_then(Value::as_bool) != Some(true)
        || !consumer
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| !name.trim().is_empty())
        || !consumer
            .get("version")
            .and_then(Value::as_str)
            .is_some_and(|version| !version.trim().is_empty())
        || consumer.get("source_revision").and_then(Value::as_str)
            != Some(expected.source_revision.as_str())
    {
        return None;
    }
    let Some(path_text) = consumer.get("artifact_path").and_then(Value::as_str) else {
        return None;
    };
    if !is_portable_relative(path_text) || !path_text.starts_with("qualification/consumers/") {
        return None;
    }
    let Some(declared_digest) = consumer.get("artifact_sha256").and_then(Value::as_str) else {
        return None;
    };
    if !is_sha256(declared_digest) {
        return None;
    }
    let path = output.join(path_text);
    let Ok(metadata) = fs::symlink_metadata(&path) else {
        return None;
    };
    if !metadata.file_type().is_file() {
        return None;
    }
    let Some(manifest_artifact) = expected
        .artifacts
        .iter()
        .find(|artifact| artifact.path == path_text)
    else {
        return None;
    };
    if manifest_artifact.sha256 != declared_digest {
        return None;
    }
    if !fs::read(&path).ok().is_some_and(|bytes| {
        bytes.len() as u64 == manifest_artifact.bytes && hash_bytes(&bytes) == declared_digest
    }) {
        return None;
    }
    let scenarios = consumer.get("scenarios")?.as_array()?;
    let mut inventory = BTreeMap::new();
    for scenario in scenarios {
        let family = scenario.get("family")?.as_str()?.trim();
        let rpc = scenario.get("rpc")?.as_str()?.trim();
        let shape = scenario.get("shape")?.as_str()?.trim();
        if family.is_empty()
            || rpc.is_empty()
            || !matches!(shape, "unary" | "client" | "server" | "bidi")
            || scenario.get("status").and_then(Value::as_str) != Some("passed")
        {
            return None;
        }
        let output_path = scenario.get("output_path").and_then(Value::as_str)?;
        let output_digest = scenario.get("output_sha256").and_then(Value::as_str)?;
        if !is_portable_relative(output_path)
            || !output_path.starts_with("qualification/consumers/")
            || !is_sha256(output_digest)
        {
            return None;
        }
        let output_file = output.join(output_path);
        let metadata = fs::symlink_metadata(&output_file).ok()?;
        if !metadata.file_type().is_file() {
            return None;
        }
        let artifact = expected
            .artifacts
            .iter()
            .find(|artifact| artifact.path == output_path && artifact.sha256 == output_digest)?;
        let bytes = fs::read(&output_file).ok()?;
        if bytes.len() as u64 != artifact.bytes || hash_bytes(&bytes) != output_digest {
            return None;
        }
        let result: Value = serde_json::from_slice(&bytes).ok()?;
        if result.get("schema").and_then(Value::as_str)
            != Some("acyclic.sdk.rpc-scenario-result.v1")
            || result.get("source_revision").and_then(Value::as_str)
                != Some(expected.source_revision.as_str())
            || result.get("status").and_then(Value::as_str) != Some("passed")
            || result.get("invoked").and_then(Value::as_bool) != Some(true)
            || result.get("exit_code").and_then(Value::as_i64) != Some(0)
            || result.get("family").and_then(Value::as_str) != Some(family)
            || result.get("rpc").and_then(Value::as_str) != Some(rpc)
            || result.get("shape").and_then(Value::as_str) != Some(shape)
        {
            return None;
        }
        let transport = result.get("transport").and_then(Value::as_str)?.trim();
        if !matches!(transport, "grpc" | "http" | "http-json" | "grpc-web") {
            return None;
        }
        let checks = result.get("checks").and_then(Value::as_array)?;
        let mut check_set = BTreeSet::new();
        for check in checks {
            let check = check.as_str()?.trim();
            if !matches!(
                check,
                "invocation" | "transport" | "serialization" | "cancellation" | "recovery"
            ) || !check_set.insert(check.to_owned())
            {
                return None;
            }
        }
        if !check_set.contains("invocation") || !check_set.contains("transport") {
            return None;
        }
        if inventory
            .insert(
                (family.to_owned(), rpc.to_owned()),
                ConsumerScenario {
                    shape: shape.to_owned(),
                    checks: check_set,
                },
            )
            .is_some()
        {
            return None;
        }
    }
    Some(inventory)
}

fn consumer_receipt_valid(output: &Path, receipt: &Value, expected: &EvidenceExpectations) -> bool {
    consumer_scenario_inventory(output, receipt, expected).is_some()
}

fn authority_rpc_inventory(
    output: &Path,
) -> Result<BTreeMap<String, BTreeMap<String, String>>, CliError> {
    let manifest_path = output.join("wire/rust-authority.json");
    let value: Value = read_json(&manifest_path)?;
    let families = value
        .get("families")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::new("Rust authority manifest has no families"))?;
    let mut inventory = BTreeMap::new();
    for family in families {
        let source = family
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new("Rust authority family has no source path"))?;
        if !is_portable_relative(source) {
            return Err(CliError::new(format!(
                "Rust authority family has a non-portable source path: {source}"
            )));
        }
        let descriptor = family
            .get("descriptor")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new("Rust authority family has no descriptor path"))?;
        let descriptor_path = authority_path(&output.join("wire"), descriptor)?;
        let descriptor_bytes = fs::read(&descriptor_path)?;
        let descriptor_hash = family
            .get("descriptor_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new("Rust authority family has no descriptor hash"))?;
        if descriptor_hash.len() != 64
            || !descriptor_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            || hash_bytes(&descriptor_bytes) != format!("sha256:{descriptor_hash}")
        {
            return Err(CliError::new(format!(
                "Rust authority descriptor hash mismatch: {descriptor}"
            )));
        }
        let methods = descriptor_rpc_methods(&descriptor_bytes, source)?;
        if let Some(declared) = family.get("rpc_methods") {
            let declared = declared
                .as_array()
                .ok_or_else(|| CliError::new("Rust authority RPC methods are not an array"))?;
            let mut declared_methods = BTreeMap::new();
            for item in declared {
                let rpc = item
                    .get("rpc")
                    .and_then(Value::as_str)
                    .filter(|rpc| !rpc.trim().is_empty())
                    .ok_or_else(|| CliError::new("Rust authority RPC method has no identity"))?;
                let shape = item
                    .get("shape")
                    .and_then(Value::as_str)
                    .filter(|shape| matches!(*shape, "unary" | "client" | "server" | "bidi"))
                    .ok_or_else(|| CliError::new("Rust authority RPC method has invalid shape"))?;
                if declared_methods
                    .insert(rpc.to_owned(), shape.to_owned())
                    .is_some()
                {
                    return Err(CliError::new(format!(
                        "duplicate Rust authority RPC method: {rpc}"
                    )));
                }
            }
            if declared_methods != methods {
                return Err(CliError::new(format!(
                    "Rust authority RPC methods do not match descriptor: {descriptor}"
                )));
            }
        }
        if let Some(declared) = family.get("rpc_shapes") {
            let declared = declared
                .as_array()
                .ok_or_else(|| CliError::new("Rust authority RPC shapes are not an array"))?;
            let mut declared_shapes = BTreeSet::new();
            for shape in declared {
                let shape = shape
                    .as_str()
                    .filter(|shape| matches!(*shape, "unary" | "client" | "server" | "bidi"))
                    .ok_or_else(|| CliError::new("Rust authority RPC shape is invalid"))?;
                if !declared_shapes.insert(shape.to_owned()) {
                    return Err(CliError::new(format!(
                        "duplicate Rust authority RPC shape: {shape}"
                    )));
                }
            }
            let actual_shapes = methods.values().cloned().collect::<BTreeSet<_>>();
            if declared_shapes != actual_shapes {
                return Err(CliError::new(format!(
                    "Rust authority RPC shapes do not match descriptor: {descriptor}"
                )));
            }
        }
        let family_name = source
            .split('/')
            .next()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| CliError::new("Rust authority family source has no family name"))?;
        if inventory.insert(family_name.to_owned(), methods).is_some() {
            return Err(CliError::new(format!(
                "duplicate Rust authority RPC family: {family_name}"
            )));
        }
    }
    Ok(inventory)
}

fn string_set_field(entry: &Value, field: &str) -> Option<BTreeSet<String>> {
    let values = entry.get(field)?.as_array()?;
    let mut result = BTreeSet::new();
    for value in values {
        let value = value.as_str()?.trim();
        if value.is_empty() || !result.insert(value.to_owned()) {
            return None;
        }
    }
    Some(result)
}

fn receipt_family_matches_authority(
    entry: &Value,
    capability: &str,
    authority_methods: Option<&BTreeMap<String, String>>,
) -> bool {
    if !matches!(
        capability,
        "remote" | "embedded" | "docs" | "snippets" | "install"
    ) {
        return false;
    }
    let Some(family) = entry.get("family").and_then(Value::as_str) else {
        return false;
    };
    let Some(features) = string_set_field(entry, "features") else {
        return false;
    };
    if features.is_empty() {
        return false;
    }
    let Some(methods) = string_set_field(entry, "methods") else {
        return false;
    };
    let Some(shapes) = string_set_field(entry, "rpc_shapes") else {
        return false;
    };
    if shapes
        .iter()
        .any(|shape| !matches!(shape.as_str(), "unary" | "client" | "server" | "bidi"))
    {
        return false;
    }
    // HTTP is a projection of only the families explicitly marked as HTTP
    // capable by the Rust registry. Other transport claims remain descriptor
    // neutral, so this does not invent RPCs for service-free families.
    if features.contains("http") && !explicit_http_family_views().any(|view| view.name == family) {
        return false;
    }
    let Some(authority_methods) = authority_methods else {
        return !methods.is_empty() && !shapes.is_empty();
    };
    // A gRPC claim needs a service-backed authority family. Protocol-only
    // descriptors are valid receipt entries, but they cannot qualify a
    // transport that exposes RPCs.
    if features.contains("grpc") && authority_methods.is_empty() {
        return false;
    }
    let expected_methods = authority_methods.keys().cloned().collect::<BTreeSet<_>>();
    let expected_shapes = authority_methods.values().cloned().collect::<BTreeSet<_>>();
    // Every service-backed family must name at least one real method and one
    // descriptor-derived shape for every capability. The protocol family is
    // the intentional exception: it has no RPC service and therefore carries
    // empty method and shape sets.
    if !authority_methods.is_empty() && (methods.is_empty() || shapes.is_empty()) {
        return false;
    }
    if capability == "remote" {
        // Remote evidence must cover every descriptor method and every shape;
        // a unary-only receipt therefore cannot qualify a streaming family.
        methods == expected_methods && shapes == expected_shapes
    } else {
        // Embedded/docs/snippet/install evidence may exercise a subset, but it
        // can never introduce a method or shape absent from Rust authority.
        methods.is_subset(&expected_methods) && shapes.is_subset(&expected_shapes)
    }
}

fn evidence_test_receipt(
    output: &Path,
    language: &str,
    capability: &str,
    expected: &EvidenceExpectations,
    value: &str,
) -> bool {
    let digest = value
        .split_whitespace()
        .find(|part| is_sha256(part))
        .and_then(|part| part.strip_prefix("sha256:"));
    let Some(digest) = digest else {
        return false;
    };
    value.split_whitespace().any(|token| {
        let token = token.strip_prefix("receipt=").unwrap_or(token);
        if token.starts_with("sha256:") || token.contains(':') || token.is_empty() {
            return false;
        }
        let path = Path::new(token);
        if !is_portable_relative(token) {
            return false;
        }
        let path = output.join(path);
        if !path.is_file() || !token.starts_with("qualification/receipts/") {
            return false;
        }
        let bytes = fs::read(&path).ok();
        let Some(bytes) = bytes else { return false };
        if hash_bytes(&bytes) != format!("sha256:{digest}") {
            return false;
        }
        let receipt: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => return false,
        };
        let authority_manifest_present = output.join("wire/rust-authority.json").is_file();
        let authority_inventory = if authority_manifest_present {
            authority_rpc_inventory(output).ok()
        } else {
            None
        };
        let families_valid = receipt
            .get("families")
            .and_then(Value::as_array)
            .is_some_and(|families| {
                if authority_manifest_present && authority_inventory.is_none() {
                    return false;
                }
                let Some(scenarios) = consumer_scenario_inventory(output, &receipt, expected)
                else {
                    return false;
                };
                let mut covered = BTreeSet::new();
                let valid_entries = families.iter().all(|entry| {
                    let Some(family) = entry.get("family").and_then(Value::as_str) else {
                        return false;
                    };
                    let authority_methods = authority_inventory
                        .as_ref()
                        .and_then(|inventory| inventory.get(family));
                    if authority_inventory.is_some() && authority_methods.is_none() {
                        return false;
                    }
                    if !receipt_family_matches_authority(entry, capability, authority_methods) {
                        false
                    } else {
                        let Some(methods) = string_set_field(entry, "methods") else {
                            return false;
                        };
                        let scenario_methods = scenarios
                            .iter()
                            .filter_map(|((scenario_family, rpc), shape)| {
                                (scenario_family == family)
                                    .then(|| (rpc.clone(), shape.shape.clone()))
                            })
                            .collect::<BTreeMap<_, _>>();
                        if scenario_methods.keys().cloned().collect::<BTreeSet<_>>() != methods {
                            return false;
                        }
                        if let Some(authority_methods) = authority_methods {
                            if scenario_methods
                                .iter()
                                .any(|(rpc, shape)| authority_methods.get(rpc) != Some(shape))
                            {
                                return false;
                            }
                        }
                        let Some(features) = string_set_field(entry, "features") else {
                            return false;
                        };
                        for scenario in
                            scenarios
                                .iter()
                                .filter_map(|((scenario_family, _), scenario)| {
                                    (scenario_family == family).then_some(scenario)
                                })
                        {
                            if features.contains("transport")
                                && !scenario.checks.contains("transport")
                            {
                                return false;
                            }
                            if features.contains("serialization")
                                && !scenario.checks.contains("serialization")
                            {
                                return false;
                            }
                            if features.contains("cancellation")
                                && !scenario.checks.contains("cancellation")
                            {
                                return false;
                            }
                            if features.contains("recovery")
                                && !scenario.checks.contains("recovery")
                            {
                                return false;
                            }
                        }
                        covered.insert(family);
                        true
                    }
                });
                valid_entries
                    && !covered.is_empty()
                    && scenarios
                        .keys()
                        .all(|(family, _)| covered.contains(family.as_str()))
                    && emitted_contract_families(output)
                        .iter()
                        .all(|family| covered.contains(family.as_str()))
            });
        receipt.get("schema").and_then(Value::as_str)
            == Some("acyclic.sdk.qualification.receipt.v1")
            && receipt
                .get("tool")
                .and_then(Value::as_str)
                .is_some_and(|tool| !tool.trim().is_empty())
            && receipt.get("language").and_then(Value::as_str) == Some(language)
            && receipt.get("capability").and_then(Value::as_str) == Some(capability)
            && receipt.get("source_revision").and_then(Value::as_str)
                == Some(expected.source_revision.as_str())
            && receipt.get("contract_digest").and_then(Value::as_str)
                == Some(expected.contract_digest.as_str())
            && receipt.get("artifact_digest").and_then(Value::as_str)
                == Some(expected.artifact_digest.as_str())
            && receipt.get("status").and_then(Value::as_str) == Some("passed")
            && receipt.get("exit_code").and_then(Value::as_i64) == Some(0)
            && receipt
                .get("suite")
                .and_then(Value::as_str)
                .is_some_and(|suite| !suite.trim().is_empty())
            && receipt
                .get("assertions")
                .and_then(Value::as_u64)
                .is_some_and(|assertions| assertions > 0)
            && consumer_receipt_valid(output, &receipt, expected)
            && families_valid
    })
}

fn emitted_contract_families(output: &Path) -> BTreeSet<String> {
    let wire = output.join("wire");
    if !wire.is_dir() {
        return BTreeSet::new();
    }
    let mut paths = Vec::new();
    if collect_output_files(output, &wire, &mut paths).is_err() {
        return BTreeSet::new();
    }
    paths
        .into_iter()
        .filter_map(|path| {
            let name = Path::new(&path).file_name()?.to_string_lossy();
            name.strip_suffix(".fds.bin").map(str::to_owned)
        })
        .collect()
}

fn collect_artifacts(output: &Path) -> Result<Vec<Artifact>, CliError> {
    let mut paths = Vec::new();
    collect_output_files(output, output, &mut paths)?;
    paths.sort();
    paths.retain(|path| {
        !path.starts_with("logs/")
            && !path.starts_with("requests/")
            && (!path.starts_with("qualification/")
                || path.starts_with("qualification/consumers/")
                || path.starts_with("qualification/packages/"))
            && !path.starts_with(".check/")
            && path != "sdk-generation-manifest.json"
            && path != "sdk-generation-drift.json"
            && path != "qualification.json"
            && path != "language-inventory.json"
    });
    paths
        .into_iter()
        .map(|relative| {
            let bytes = fs::read(output.join(&relative))?;
            Ok(Artifact {
                path: relative,
                sha256: hash_bytes(&bytes),
                bytes: bytes.len() as u64,
            })
        })
        .collect()
}

fn collect_output_files(
    root: &Path,
    directory: &Path,
    paths: &mut Vec<String>,
) -> Result<(), CliError> {
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(CliError::new(format!(
                "output contains a symlink entry: {}",
                path.display()
            )));
        }
        if metadata.is_dir() {
            collect_output_files(root, &path, paths)?;
        } else if metadata.is_file() {
            paths.push(
                path.strip_prefix(root)
                    .map_err(|_| CliError::new("output path escaped root"))?
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
    Ok(())
}

fn verify_artifacts(output: &Path, artifacts: &[Artifact]) -> Result<(), CliError> {
    for artifact in artifacts {
        if !is_portable_relative(&artifact.path) {
            return Err(CliError::new(format!(
                "generated artifact path is not portable: {}",
                artifact.path
            )));
        }
        let path = output.join(&artifact.path);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_file() {
            return Err(CliError::new(format!(
                "generated artifact is missing: {}",
                artifact.path
            )));
        }
        let bytes = fs::read(path)?;
        if bytes.len() as u64 != artifact.bytes || hash_bytes(&bytes) != artifact.sha256 {
            return Err(CliError::new(format!(
                "generated artifact differs: {}",
                artifact.path
            )));
        }
    }
    let actual = collect_artifacts(output)?;
    if actual != artifacts {
        return Err(CliError::new(
            "generated artifact set differs from the qualification manifest",
        ));
    }
    Ok(())
}

fn compare_fresh_artifacts(
    output: &Path,
    fresh_output: &Path,
    artifacts: &[Artifact],
) -> Result<(), CliError> {
    let expected = artifacts
        .iter()
        .map(|artifact| artifact.path.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut actual = Vec::new();
    collect_output_files(fresh_output, fresh_output, &mut actual)?;
    actual.retain(|path| {
        !path.starts_with("logs/")
            && !path.starts_with("requests/")
            && (!path.starts_with("qualification/")
                || path.starts_with("qualification/consumers/")
                || path.starts_with("qualification/packages/"))
            && !path.starts_with(".check/")
    });
    actual.sort();
    let actual = actual
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    if expected != actual {
        return Err(CliError::new(format!(
            "regenerated artifact set differs (expected {:?}, got {:?})",
            expected, actual
        )));
    }
    for artifact in artifacts {
        let expected = output.join(&artifact.path);
        let fresh = fresh_output.join(&artifact.path);
        if !fresh.is_file() {
            return Err(CliError::new(format!(
                "regenerated artifact is missing: {}",
                artifact.path
            )));
        }
        let expected_bytes = fs::read(&expected)?;
        let fresh_bytes = fs::read(&fresh)?;
        if expected_bytes != fresh_bytes {
            return Err(CliError::new(format!(
                "regenerated artifact differs: {}",
                artifact.path
            )));
        }
    }
    Ok(())
}

fn hash_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}
fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), CliError> {
    write_json_value(path, &serde_json::to_value(value)?)
}
fn write_json_value(path: &Path, value: &Value) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, CliError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn print_json<T: Serialize>(value: &T) -> Result<(), CliError> {
    serde_json::to_writer_pretty(io::stdout(), value)?;
    println!();
    Ok(())
}
fn absolute_path(path: &Path) -> Result<PathBuf, CliError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn validate_output_path(path: &Path) -> Result<(), CliError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if current.exists() {
            reject_symlink(&current)?;
        }
    }
    Ok(())
}

fn reject_symlink(path: &Path) -> Result<(), CliError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(CliError::new(format!(
            "refusing to operate on symlink path: {}",
            path.display()
        )));
    }
    Ok(())
}
fn canonical_existing_directory(path: &Path, label: &str) -> Result<PathBuf, CliError> {
    let path = absolute_path(path)?;
    if !path.is_dir() {
        return Err(CliError::new(format!(
            "{label} does not exist: {}",
            path.display()
        )));
    }
    let canonical = fs::canonicalize(path)?;
    // Cargo rejects Windows extended-length paths when they are interpolated
    // into temporary path dependencies (for example, `//?/Q:` in a generated
    // manifest). Keep the resolved checkout identity while handing child
    // tools a normal drive-qualified path.
    let canonical_text = canonical.to_string_lossy();
    let normalized = canonical_text
        .strip_prefix("\\\\?\\")
        .map(PathBuf::from)
        .unwrap_or(canonical);
    Ok(normalized)
}
fn relative_or_absolute(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .map(|value| value.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| path.to_string_lossy().into_owned())
}
fn command_stdout(root: &Path, command: &str, args: &[&str]) -> Result<String, CliError> {
    let mut process = Command::new(command);
    if command == "git" {
        clear_repository_selection_environment(&mut process);
    }
    let output = process.args(args).current_dir(root).output()?;
    if !output.status.success() {
        return Err(CliError::new(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn repository_command() -> Command {
    let mut command = Command::new("git");
    clear_repository_selection_environment(&mut command);
    command
}

fn clear_repository_selection_environment(command: &mut Command) {
    // Keep authentication, signing, and global configuration intact. These
    // variables alone let a parent process redirect Git's repository reads to
    // a different checkout, defeating the source-root identity guard.
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    ] {
        command.env_remove(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::GzEncoder};
    use tar::Builder;

    fn test_directory(name: &str) -> PathBuf {
        let path = env::temp_dir().join(format!(
            "acyclic-sdk-generation-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create test directory");
        path
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }

    fn initialize_git_source(path: &Path) {
        for args in [
            vec!["init"],
            vec![
                "config",
                "user.email",
                "sdk-generation-tests@example.invalid",
            ],
            vec!["config", "user.name", "sdk-generation-tests"],
            vec!["config", "commit.gpgSign", "false"],
            vec!["add", "existing.rs"],
            vec!["commit", "-m", "initial"],
        ] {
            let result = Command::new("git")
                .args(args)
                .current_dir(path)
                .output()
                .expect("git test setup");
            assert!(
                result.status.success(),
                "git setup failed: {}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
    }

    #[test]
    fn hashes_are_prefixed_and_stable() {
        assert_eq!(
            hash_bytes(b"abc"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn artifact_digest_binds_the_canonical_artifact_set() {
        let original = vec![Artifact {
            path: "wire/actors.bin".into(),
            sha256: hash_bytes(b"actors-v1"),
            bytes: 9,
        }];
        let mut changed = original.clone();
        changed[0].path = "wire/workers.bin".into();
        assert_ne!(artifact_digest(&original), artifact_digest(&changed));
    }

    #[test]
    fn product_artifact_inputs_are_rust_model_owned() {
        let root = test_directory("product-artifact-inputs");
        for path in [
            "rust/crates/sdk-contract-wire",
            "rust/crates/sdk-contract-options",
            "rust/crates/sdk-contract-validation",
            "generated/rust",
            "rust/crates/machines/src/generated",
        ] {
            fs::create_dir_all(root.join(path)).expect("create product input fixture");
        }

        let inputs = contract_inputs(&root, "sdk-product-artifacts");
        assert_eq!(
            inputs,
            vec![
                "rust/crates/sdk-contract-wire",
                "rust/crates/sdk-contract-options",
                "rust/crates/sdk-contract-validation",
            ]
        );
        assert!(
            inputs.iter().all(|path| !path.contains("generated")),
            "product packaging must not declare checked-in generated copies as authority"
        );
        cleanup(&root);
    }

    #[test]
    fn drift_report_is_control_metadata_not_an_artifact() {
        let root = test_directory("drift-report-artifact");
        fs::write(root.join("artifact.json"), b"artifact").expect("write artifact");
        for control in [
            "sdk-generation-manifest.json",
            "sdk-generation-drift.json",
            "language-inventory.json",
            "qualification.json",
        ] {
            fs::write(root.join(control), b"control").expect("write control metadata");
        }
        let artifacts = collect_artifacts(&root).expect("collect artifacts");
        assert_eq!(
            artifacts
                .iter()
                .map(|artifact| artifact.path.as_str())
                .collect::<Vec<_>>(),
            vec!["artifact.json"]
        );
        cleanup(&root);
    }

    #[test]
    fn generated_manifest_cannot_relabel_a_failed_required_stage() {
        let tools = vec![ToolResult {
            id: "sdk-docs".into(),
            status: "failed".into(),
            required: true,
            command: vec!["Q:/source".into(), "Q:/output".into()],
            request: "requests/sdk-docs.json".into(),
            stdout_sha256: Some(hash_bytes(b"")),
            stderr_sha256: Some(hash_bytes(b"")),
            exit_code: Some(1),
            message: Some("closure mismatch".into()),
        }];
        let error = verify_required_tools(&tools, Path::new("Q:/source"), Path::new("Q:/output"))
            .expect_err("a required failed stage must not be relabeled generated");
        assert!(error.message.contains("sdk-docs"));
    }

    #[test]
    fn generated_manifest_requires_hashed_source_bound_tool_results() {
        let tools = vec![ToolResult {
            id: "sdk-docs".into(),
            status: "passed".into(),
            required: true,
            command: vec!["Q:/source".into(), "Q:/output".into()],
            request: "requests/sdk-docs.json".into(),
            stdout_sha256: None,
            stderr_sha256: None,
            exit_code: Some(0),
            message: None,
        }];
        assert!(
            verify_required_tools(&tools, Path::new("Q:/source"), Path::new("Q:/output")).is_err()
        );
    }

    #[test]
    fn generated_manifest_tool_hashes_bind_retained_log_bytes() {
        let root = test_directory("tool-log-binding");
        fs::create_dir_all(root.join("logs")).expect("create tool logs");
        fs::write(root.join("logs/sdk-docs.stdout"), b"actual stdout").expect("write stdout log");
        fs::write(root.join("logs/sdk-docs.stderr"), b"actual stderr").expect("write stderr log");
        let tools = vec![ToolResult {
            id: "sdk-docs".into(),
            status: "passed".into(),
            required: true,
            command: vec![
                root.to_string_lossy().into_owned(),
                root.to_string_lossy().into_owned(),
            ],
            request: "requests/sdk-docs.json".into(),
            stdout_sha256: Some(hash_bytes(b"forged stdout")),
            stderr_sha256: Some(hash_bytes(b"forged stderr")),
            exit_code: Some(0),
            message: None,
        }];
        assert!(verify_required_tools(&tools, &root, &root).is_err());
        cleanup(&root);
    }

    #[test]
    fn drift_is_a_distinct_non_generating_operation() {
        assert_eq!(operation_name(Operation::Drift), "drift");
        assert_ne!(
            operation_name(Operation::Drift),
            operation_name(Operation::Check)
        );
    }

    #[test]
    fn openapi_check_keeps_existing_stage_receipt() {
        let root = test_directory("openapi-check-receipt-preserved");
        fs::create_dir_all(root.join("openapi")).expect("create OpenAPI output");
        let receipt = root.join("openapi/stage-receipt.json");
        fs::write(&receipt, b"stale receipt").expect("write stale receipt");

        prepare_openapi_stage_output(&root, Operation::Check).expect("check preparation");
        assert_eq!(
            fs::read(&receipt).expect("read preserved receipt"),
            b"stale receipt"
        );

        prepare_openapi_stage_output(&root, Operation::Generate).expect("generate preparation");
        assert!(
            !receipt.exists(),
            "generate may clear only its staged receipt"
        );
        cleanup(&root);
    }

    #[test]
    fn openapi_stage_receipt_binds_pin_license_adaptation_and_anchors() {
        let root = test_directory("openapi-stage-receipt");
        for family in openapi_family_names() {
            fs::create_dir_all(root.join("openapi")).expect("create OpenAPI output");
            fs::write(
                root.join(format!("openapi/{family}.json")),
                format!("{{\"family\":\"{family}\"}}"),
            )
            .expect("write projection");
        }
        let adaptation = OPENAPI_ANCHOR_MARKERS.join("\n");
        fs::write(
            root.join("openapi/workers-powershell-adaptation.ps1"),
            adaptation.as_bytes(),
        )
        .expect("write adaptation");

        write_openapi_stage_receipt(&root).expect("write stage receipt");
        let receipt: Value =
            read_json(&root.join("openapi/stage-receipt.json")).expect("read stage receipt");
        assert_eq!(
            receipt.get("schema").and_then(Value::as_str),
            Some(OPENAPI_STAGE_RECEIPT_SCHEMA)
        );
        assert_eq!(
            receipt
                .get("openapi_generator_qualification")
                .and_then(|value| value.get("release"))
                .and_then(Value::as_str),
            Some(OPENAPI_GENERATOR_RELEASE)
        );
        assert_eq!(
            receipt
                .get("license")
                .and_then(|value| value.get("name"))
                .and_then(Value::as_str),
            Some(OPENAPI_LICENSE_NAME)
        );
        assert_eq!(
            receipt
                .get("adaptation")
                .and_then(|value| value.get("anchor_check"))
                .and_then(|value| value.get("status"))
                .and_then(Value::as_str),
            Some("passed")
        );
        assert_eq!(
            receipt
                .get("projections")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(openapi_family_names().len())
        );
        cleanup(&root);
    }

    #[test]
    fn openapi_stage_receipt_rejects_missing_anchor_marker() {
        let root = test_directory("openapi-stage-receipt-missing-anchor");
        fs::create_dir_all(root.join("openapi")).expect("create OpenAPI output");
        for family in openapi_family_names() {
            fs::write(root.join(format!("openapi/{family}.json")), b"{}")
                .expect("write projection");
        }
        fs::write(
            root.join("openapi/workers-powershell-adaptation.ps1"),
            OPENAPI_ANCHOR_MARKERS[..OPENAPI_ANCHOR_MARKERS.len() - 1].join("\n"),
        )
        .expect("write incomplete adaptation");
        let error = write_openapi_stage_receipt(&root).expect_err("missing marker rejected");
        assert!(error.to_string().contains("anchor markers"));
        cleanup(&root);
    }

    #[test]
    fn relative_paths_are_portable() {
        assert_eq!(
            relative_or_absolute(Path::new("C:/out/a.json"), Path::new("C:/out")),
            "a.json"
        );
    }
    #[test]
    fn pending_is_not_qualified() {
        let evidence = CapabilityEvidence {
            status: "pending".into(),
            tests: Vec::new(),
            scope: None,
        };
        assert_ne!(evidence.status, "qualified");
    }

    #[test]
    fn qualification_manifest_must_match_current_source_identity() {
        let expected = EvidenceExpectations {
            source_revision: "head".into(),
            contract_digest: "sha256:current".into(),
            artifact_digest: "sha256:artifacts".into(),
            artifacts: Vec::new(),
        };
        let clean = SourceIdentity {
            revision: "head".into(),
            dirty: false,
            digest: "sha256:current".into(),
        };
        assert!(validate_current_source(&clean, Some(&expected)).is_ok());
        let changed = SourceIdentity {
            digest: "sha256:changed".into(),
            ..clean.clone()
        };
        assert!(validate_current_source(&changed, Some(&expected)).is_err());
        let dirty = SourceIdentity {
            dirty: true,
            ..clean
        };
        assert!(validate_current_source(&dirty, Some(&expected)).is_err());
    }

    #[test]
    fn generation_rejects_any_source_checkout_mutation() {
        let clean = SourceIdentity {
            revision: "head".into(),
            dirty: false,
            digest: "sha256:current".into(),
        };
        assert!(ensure_source_identity_unchanged(&clean, &clean, "generation").is_ok());
        for changed in [
            SourceIdentity {
                digest: "sha256:changed".into(),
                ..clean.clone()
            },
            SourceIdentity {
                dirty: true,
                ..clean.clone()
            },
            SourceIdentity {
                revision: "other".into(),
                ..clean.clone()
            },
        ] {
            let error = ensure_source_identity_unchanged(&clean, &changed, "generation")
                .expect_err("source mutation must fail closed");
            assert!(error.to_string().contains("output tree"));
        }
    }

    #[test]
    fn docs_and_examples_are_required_generation_stages() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let specs = tool_specs(&root);
        for id in ["sdk-docs", "sdk-examples"] {
            let spec = specs.iter().find(|spec| spec.id == id).unwrap();
            assert!(spec.required, "{id} must be a required source-bound stage");
        }
    }

    #[test]
    fn package_producers_precede_examples_and_docs() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let ids = tool_specs(&root)
            .into_iter()
            .map(|spec| spec.id)
            .collect::<Vec<_>>();
        let index = |id: &str| ids.iter().position(|candidate| *candidate == id).unwrap();
        assert!(index("sdk-contract-wire") < index("sdk-language-producers"));
        assert!(index("sdk-openapi-prototype") < index("sdk-language-producers"));
        assert!(index("sdk-language-producers") < index("sdk-python"));
        assert!(index("sdk-python") < index("sdk-typescript"));
        assert!(index("sdk-typescript") < index("sdk-typescript-rpc-contracts"));
        assert!(index("sdk-typescript-rpc-contracts") < index("sdk-examples"));
        assert!(index("sdk-typescript") < index("sdk-examples"));
        assert!(index("sdk-examples") < index("sdk-docs"));
    }

    #[test]
    fn language_producer_plan_covers_primary_and_http_targets() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let output = test_directory("language-producer-plan");
        let spec = tool_specs(&root)
            .into_iter()
            .find(|spec| spec.id == "sdk-language-producers")
            .expect("language producer stage is registered");
        let source = SourceIdentity {
            revision: "test-revision".into(),
            digest: "sha256:test-source".into(),
            dirty: false,
        };
        let result = run_language_producers(
            &root,
            &output,
            &spec,
            "requests/sdk-language-producers.json".into(),
            &source,
            Operation::Generate,
        )
        .expect("target catalog is valid");
        assert!(matches!(result.status.as_str(), "pending" | "failed"));
        let plan: Value = read_json(&output.join("language-producers/plan.json"))
            .expect("language producer plan");
        assert_eq!(
            plan["qualification"],
            "pending-until-language-consumer-receipt"
        );
        let ids = plan["targets"]
            .as_array()
            .expect("target plan array")
            .iter()
            .filter_map(|target| target["id"].as_str())
            .collect::<BTreeSet<_>>();
        for id in [
            "go", "java", "csharp", "swift", "cpp", "ruby", "php", "dart",
        ] {
            assert!(ids.contains(id), "missing planned target {id}");
        }
        cleanup(&output);
    }

    #[test]
    fn producer_recipe_requires_source_request_and_staged_output_bindings() {
        let target = json!({
            "id": "python",
            "producer": {
                "program": "python",
                "args": ["--source-root", "{source_root}", "--request", "{request}"],
                "output": "generated"
            }
        });
        let error = parse_producer_recipe(&target, "python")
            .expect_err("an unbound staged output must be rejected");
        assert!(error.message.contains("{target_output}"));

        let target = json!({
            "id": "python",
            "producer": {
                "program": "python",
                "args": [
                    "--source-root", "{source_root}",
                    "--request", "{request}",
                    "--output", "{target_output}",
                    "--unknown", "{ambient_shell}"
                ],
                "output": "generated"
            }
        });
        let error = parse_producer_recipe(&target, "python")
            .expect_err("unknown placeholders must be rejected");
        assert!(error.message.contains("unknown placeholder"));
    }

    #[test]
    fn language_producer_plan_rejects_missing_generator_pin() {
        let root = test_directory("language-producer-invalid");
        fs::create_dir_all(root.join("languages")).expect("create language catalog");
        write_json_value(
            &root.join("languages/generation-targets.json"),
            &json!({
                "targets": [{
                    "id": "python",
                    "language_family": "Python",
                    "status": "candidate",
                    "generator": {
                        "name": "grpcio-tools",
                        "version": "1",
                        "source": "https://example.invalid/python",
                        "license": "Apache-2.0"
                    },
                    "package": {
                        "ecosystem": "PyPI",
                        "artifact": "acyclic-sdk"
                    }
                }]
            }),
        )
        .expect("write target catalog");
        let spec = ToolSpec {
            id: "sdk-language-producers",
            required: true,
            manifest: Some(root.join("languages/generation-targets.json")),
            script: None,
        };
        let source = SourceIdentity {
            revision: "test-revision".into(),
            digest: "sha256:test-source".into(),
            dirty: false,
        };
        let error = run_language_producers(
            &root,
            &root.join("output"),
            &spec,
            "requests/sdk-language-producers.json".into(),
            &source,
            Operation::Generate,
        )
        .expect_err("missing generator pin must fail closed");
        assert!(error.message.contains("missing pin"));
        cleanup(&root);
    }

    #[test]
    fn docs_command_requires_output_bound_rustdoc_graph() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let spec = ToolSpec {
            id: "sdk-docs",
            required: true,
            manifest: some_file(&root, "rust/crates/sdk-docs/Cargo.toml"),
            script: None,
        };
        let output = root.join("target/sdk-generation-test");
        let source = SourceIdentity {
            revision: "test".into(),
            dirty: false,
            digest: "sha256:test".into(),
        };
        let command = tool_command(
            &root,
            &spec,
            Operation::Generate,
            &output.join("request.json"),
            &output,
            &source,
        )
        .expect("docs manifest is present");
        let args = command
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let rustdoc_index = args
            .iter()
            .position(|argument| argument == "--rustdoc-json")
            .unwrap();
        assert_eq!(
            args[rustdoc_index + 1],
            output.join("rustdoc-json").to_string_lossy()
        );
        assert!(
            args.iter()
                .any(|argument| argument == "--strict-rustdoc-json")
        );
        let registry_index = args
            .iter()
            .position(|argument| argument == "--registry-manifest")
            .expect("Rust-owned registry metadata must be passed to docs");
        assert_eq!(
            args[registry_index + 1],
            root.join("release/cargo-registry-metadata.json")
                .to_string_lossy()
        );
    }

    #[test]
    fn docs_profile_features_are_staged_in_canonical_order() {
        let mut profile = json!({
            "profiles": [{
                "packages": [{
                    "features": ["distributed", "s3-http", "acyclic-objects/grpc", "default"]
                }]
            }]
        });
        normalize_profile_feature_order(&mut profile);
        assert_eq!(
            profile["profiles"][0]["packages"][0]["features"],
            json!(["acyclic-objects/grpc", "default", "distributed", "s3-http"])
        );
    }

    #[test]
    fn rpc_inventory_command_is_source_and_output_bound() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let output = root.join("target/sdk-generation-rpc-inventory-test");
        let spec = tool_specs(&root)
            .into_iter()
            .find(|spec| spec.id == "sdk-typescript-rpc-contracts")
            .expect("RPC inventory stage is registered");
        let source = SourceIdentity {
            revision: "0123456789012345678901234567890123456789".into(),
            dirty: false,
            digest: "sha256:test".into(),
        };
        let command = tool_command(
            &root,
            &spec,
            Operation::Generate,
            &output.join("request.json"),
            &output,
            &source,
        )
        .expect("RPC inventory binary is registered");
        let args = command
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(args.iter().any(|argument| argument == "sdk-rpc-contracts"));
        assert!(args.iter().any(|argument| argument == &root.to_string_lossy()));
        assert!(args.iter().any(|argument| {
            argument == &output.join("typescript/rpc-contracts.json").to_string_lossy()
        }));
    }

    #[test]
    fn examples_command_selects_binary_before_cargo_separator() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let spec = ToolSpec {
            id: "sdk-examples",
            required: true,
            manifest: some_file(&root, "rust/crates/sdk-examples/Cargo.toml"),
            script: None,
        };
        let request = root.join("target/sdk-generation-test/request.json");
        let output = root.join("target/sdk-generation-test");
        let source = SourceIdentity {
            revision: "test".into(),
            dirty: false,
            digest: "sha256:test".into(),
        };
        let command = tool_command(
            &root,
            &spec,
            Operation::Generate,
            &request,
            &output,
            &source,
        )
        .expect("examples manifest is present");
        let separator = command
            .iter()
            .position(|argument| argument == "--")
            .expect("cargo separator");
        assert_eq!(command[separator - 2], "--bin");
        assert_eq!(command[separator - 1], "sdk-examples");
        assert_eq!(command[separator + 1], "generate");
        assert_eq!(command[separator + 2], "--request");
    }

    #[test]
    fn typescript_check_generates_into_fresh_check_output() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let spec = ToolSpec {
            id: "sdk-typescript",
            required: true,
            manifest: some_file(&root, "rust/crates/sdk-typescript/Cargo.toml"),
            script: None,
        };
        let output = root.join("target/sdk-generation-typescript-check");
        let source = SourceIdentity {
            revision: "test".into(),
            dirty: false,
            digest: "sha256:test".into(),
        };
        let command = tool_command(
            &root,
            &spec,
            Operation::Check,
            &output.join("request.json"),
            &output,
            &source,
        )
        .expect("TypeScript manifest is present");
        let args = command
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let separator = args
            .iter()
            .position(|argument| argument == "--")
            .expect("cargo separator");
        assert_eq!(args[separator + 1], "packages-write");
        assert_eq!(args[separator + 2], root.to_string_lossy());
        assert_eq!(args[separator + 3], output.to_string_lossy());
        assert_eq!(args[separator + 4], output.join("wire").to_string_lossy());
        assert_eq!(args[separator + 5], "test");
    }

    #[test]
    fn generation_commands_cannot_target_the_frozen_source_checkout() {
        let root = test_directory("generation-destination");
        let output = root.join("output");
        fs::create_dir_all(&output).expect("create output directory");
        fs::create_dir_all(output.join("wire")).expect("create wire output directory");
        fs::create_dir_all(root.join("rust/crates/sdk-typescript"))
            .expect("create TypeScript manifest directory");
        fs::write(
            root.join("rust/crates/sdk-typescript/Cargo.toml"),
            b"[package]\nname = \"fixture\"\nversion = \"0.0.0\"\n",
        )
        .expect("write TypeScript manifest fixture");
        let spec = ToolSpec {
            id: "sdk-typescript",
            required: true,
            manifest: some_file(&root, "rust/crates/sdk-typescript/Cargo.toml"),
            script: None,
        };
        let source = SourceIdentity {
            revision: "test".into(),
            dirty: false,
            digest: "sha256:test".into(),
        };
        let command = tool_command(
            &root,
            &spec,
            Operation::Generate,
            &output.join("request.json"),
            &output,
            &source,
        )
        .expect("TypeScript generation command is present");
        let separator = command
            .iter()
            .position(|argument| argument == "--")
            .expect("Cargo separator");
        assert_eq!(command[separator + 1], "packages-write");
        assert_eq!(command[separator + 2], root.as_os_str());
        assert_eq!(command[separator + 3], output.as_os_str());
        assert_eq!(command[separator + 4], output.join("wire").as_os_str());
        assert_eq!(command[separator + 5], "test");

        let mut source_command = command.clone();
        source_command[separator + 3] = root.as_os_str().to_os_string();
        let output_command = command.clone();
        assert!(ensure_generation_destination(
            &root,
            &output,
            "sdk-typescript",
            Operation::Generate,
            &source_command,
        )
        .is_err());
        assert!(ensure_generation_destination(
            &root,
            &output,
            "sdk-typescript",
            Operation::Generate,
            &output_command,
        )
        .is_ok());
        let mut source_input_command = command.clone();
        source_input_command[separator + 2] = output.as_os_str().to_os_string();
        assert!(ensure_generation_destination(
            &root,
            &output,
            "sdk-typescript",
            Operation::Generate,
            &source_input_command,
        )
        .is_err());
        assert!(ensure_generation_destination(
            &root,
            &root,
            "sdk-typescript",
            Operation::Generate,
            &output_command,
        )
        .is_err());
        assert!(ensure_generation_destination(
            &root,
            &root,
            "sdk-typescript",
            Operation::Check,
            &source_command,
        )
        .is_ok());
        assert!(ensure_generation_destination(
            &root,
            &output,
            "sdk-docs",
            Operation::Generate,
            &source_command,
        )
        .is_ok());
        cleanup(&root);
    }

    #[test]
    fn receipt_evidence_requires_full_sha256_content_digests() {
        assert!(!is_sha256("sha256:deadbeef"));
        assert!(!is_sha256_digest_in("executed case sha256:deadbeef"));
        let digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert!(is_sha256(digest));
        assert!(is_sha256_digest_in(&format!("case-1 {digest}")));
        assert!(!is_sha256_digest_in("case-1 passed"));
    }

    #[test]
    fn receipt_evidence_must_bind_to_output_bytes() {
        let root = test_directory("receipt-binding");
        let receipt = root.join("qualification/receipts/remote.json");
        fs::create_dir_all(receipt.parent().expect("receipt parent")).expect("receipt directory");
        fs::create_dir_all(root.join("wire")).expect("wire directory");
        fs::write(root.join("wire/actors.fds.bin"), b"descriptor").expect("write descriptor");
        let consumer = root.join("qualification/consumers/remote.bin");
        fs::create_dir_all(consumer.parent().expect("consumer parent"))
            .expect("consumer directory");
        fs::write(&consumer, b"compiled-consumer-v1").expect("write consumer");
        let consumer_digest = hash_bytes(b"compiled-consumer-v1");
        let scenario = root.join("qualification/consumers/remote-scenario.json");
        let scenario_bytes = br#"{"schema":"acyclic.sdk.rpc-scenario-result.v1","source_revision":"revision","status":"passed","invoked":true,"exit_code":0,"family":"actors","rpc":"list","shape":"unary","transport":"grpc","checks":["invocation","transport","serialization"]}"#;
        fs::write(&scenario, scenario_bytes).expect("write scenario result");
        let scenario_digest = hash_bytes(scenario_bytes);
        let expected = EvidenceExpectations {
            source_revision: "revision".into(),
            contract_digest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            artifact_digest:
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            artifacts: vec![
                Artifact {
                    path: "qualification/consumers/remote.bin".into(),
                    sha256: consumer_digest.clone(),
                    bytes: b"compiled-consumer-v1".len() as u64,
                },
                Artifact {
                    path: "qualification/consumers/remote-scenario.json".into(),
                    sha256: scenario_digest.clone(),
                    bytes: scenario_bytes.len() as u64,
                },
            ],
        };
        let receipt_bytes = format!(
            "{{\"schema\":\"acyclic.sdk.qualification.receipt.v1\",\"tool\":\"sdk-generation\",\"language\":\"rust\",\"capability\":\"remote\",\"source_revision\":\"revision\",\"contract_digest\":\"{}\",\"artifact_digest\":\"{}\",\"status\":\"passed\",\"exit_code\":0,\"suite\":\"smoke\",\"assertions\":1,\"consumer\":{{\"executed\":true,\"name\":\"fixture-consumer\",\"version\":\"1\",\"source_revision\":\"revision\",\"artifact_path\":\"qualification/consumers/remote.bin\",\"artifact_sha256\":\"{}\",\"scenarios\":[{{\"family\":\"actors\",\"rpc\":\"list\",\"shape\":\"unary\",\"status\":\"passed\",\"output_path\":\"qualification/consumers/remote-scenario.json\",\"output_sha256\":\"{}\"}}]}},\"families\":[{{\"family\":\"actors\",\"methods\":[\"list\"],\"features\":[\"serialization\",\"transport\"],\"rpc_shapes\":[\"unary\"]}}]}}",
            expected.contract_digest, expected.artifact_digest, consumer_digest, scenario_digest
        );
        fs::write(&receipt, receipt_bytes.as_bytes()).expect("write receipt");
        let digest = hash_bytes(receipt_bytes.as_bytes());
        assert!(evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!("case qualification/receipts/remote.json {digest}")
        ));
        let relabeled_scenario = receipt_bytes.replace(&scenario_digest, &consumer_digest);
        fs::write(&receipt, relabeled_scenario.as_bytes())
            .expect("write relabeled scenario receipt");
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!(
                "case qualification/receipts/remote.json {}",
                hash_bytes(relabeled_scenario.as_bytes())
            )
        ));
        fs::write(&receipt, receipt_bytes.as_bytes()).expect("restore receipt after hash check");
        let uninvoked_scenario = std::str::from_utf8(scenario_bytes)
            .expect("scenario fixture is UTF-8")
            .replace("\"invoked\":true", "\"invoked\":false")
            .into_bytes();
        fs::write(&scenario, &uninvoked_scenario).expect("write uninvoked scenario result");
        let uninvoked_digest = hash_bytes(&uninvoked_scenario);
        let uninvoked_receipt = receipt_bytes.replace(&scenario_digest, &uninvoked_digest);
        fs::write(&receipt, uninvoked_receipt.as_bytes())
            .expect("write uninvoked scenario receipt");
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!(
                "case qualification/receipts/remote.json {}",
                hash_bytes(uninvoked_receipt.as_bytes())
            )
        ));
        fs::write(&scenario, scenario_bytes).expect("restore scenario result");
        fs::write(&receipt, receipt_bytes.as_bytes()).expect("restore receipt after scenario check");
        let mut missing_scenarios: Value =
            serde_json::from_str(&receipt_bytes).expect("parse receipt for scenario mutation");
        missing_scenarios["consumer"]
            .as_object_mut()
            .expect("consumer object")
            .remove("scenarios");
        let missing_scenarios =
            serde_json::to_string(&missing_scenarios).expect("encode missing scenarios receipt");
        fs::write(&receipt, missing_scenarios.as_bytes()).expect("write missing scenarios receipt");
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!(
                "case qualification/receipts/remote.json {}",
                hash_bytes(missing_scenarios.as_bytes())
            )
        ));
        fs::write(&receipt, receipt_bytes.as_bytes())
            .expect("restore receipt after scenario check");
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            "case qualification/receipts/remote.json sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!("case {digest}")
        ));
        let stale = receipt_bytes.replace("\"revision\"", "\"old-revision\"");
        fs::write(&receipt, stale.as_bytes()).expect("write stale receipt");
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!(
                "case qualification/receipts/remote.json {}",
                hash_bytes(stale.as_bytes())
            )
        ));
        fs::write(&receipt, receipt_bytes.as_bytes()).expect("restore receipt");
        let missing_rpc_shapes = receipt_bytes.replace(",\"rpc_shapes\":[\"unary\"]", "");
        fs::write(&receipt, missing_rpc_shapes.as_bytes()).expect("write incomplete receipt");
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!(
                "case qualification/receipts/remote.json {}",
                hash_bytes(missing_rpc_shapes.as_bytes())
            )
        ));
        fs::write(&receipt, receipt_bytes.as_bytes()).expect("restore receipt after shape check");
        fs::write(&consumer, b"compiled-consumer-v0").expect("write relabeled consumer");
        let stale_consumer =
            receipt_bytes.replace(&consumer_digest, &hash_bytes(b"compiled-consumer-v0"));
        fs::write(&receipt, stale_consumer.as_bytes()).expect("write stale consumer receipt");
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!(
                "case qualification/receipts/remote.json {}",
                hash_bytes(stale_consumer.as_bytes())
            )
        ));
        fs::write(&consumer, b"compiled-consumer-v1").expect("restore consumer");
        let not_executed = receipt_bytes.replace("\"executed\":true", "\"executed\":false");
        fs::write(&receipt, not_executed.as_bytes()).expect("write non-executed receipt");
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!(
                "case qualification/receipts/remote.json {}",
                hash_bytes(not_executed.as_bytes())
            )
        ));
        fs::write(&receipt, receipt_bytes.as_bytes())
            .expect("restore receipt after consumer check");
        fs::write(root.join("wire/objects.fds.bin"), b"second descriptor")
            .expect("write second descriptor");
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!("case qualification/receipts/remote.json {digest}")
        ));
        fs::remove_file(root.join("wire/objects.fds.bin")).expect("remove second descriptor");
        let forged = b"passed; exit_code=0; assertions=1";
        let forged_path = root.join("qualification/receipts/forged.json");
        fs::write(&forged_path, forged).expect("write forged receipt");
        assert!(!evidence_test_receipt(
            &root,
            "rust",
            "remote",
            &expected,
            &format!(
                "case qualification/receipts/forged.json {}",
                hash_bytes(forged)
            )
        ));
        fs::remove_file(forged_path).expect("remove forged receipt");
        cleanup(&root);
    }

    #[test]
    fn source_closure_rejects_compiler_cache_paths() {
        for path in [
            "rust/crates/sdk-generation/src/main.rs",
            "docs/targeted-contract.md",
        ] {
            assert!(
                !is_compiler_cache_path(path),
                "source path was rejected: {path}"
            );
        }
        for path in [
            "target/debug/sdk-generation.exe",
            "rust/crates/sdk-generation/target/debug/deps/libsdk.rlib",
            "node_modules/protobuf/index.js",
            ".cargo/registry/src/index.crates.io/cache",
            "python/__pycache__/module.cpython-313.pyc",
            "build/.gradle/configuration-cache.bin",
        ] {
            assert!(
                is_compiler_cache_path(path),
                "cache path was accepted: {path}"
            );
        }
    }

    #[test]
    fn source_closure_rejects_nul_or_invalid_utf8_in_authored_text() {
        let nul = validate_authored_text_bytes("rust/crates/example/src/lib.rs", b"ok\0bad")
            .expect_err("NUL in authored Rust must fail closed");
        assert!(nul.to_string().contains("NUL byte"));
        let invalid = validate_authored_text_bytes("docs/example.md", &[0xff])
            .expect_err("invalid UTF-8 in authored documentation must fail closed");
        assert!(invalid.to_string().contains("invalid UTF-8"));
        validate_authored_text_bytes("fixtures/model.bin", b"binary\0bytes")
            .expect("binary compatibility fixtures are not authored text");
        validate_authored_text_bytes(
            "arena/evals/results/first/raced-fixedjudge-states/race-003.txt",
            b"serialized\0state",
        )
        .expect("serialized evaluation state is hashed as an artifact");
    }

    #[test]
    fn authoritative_identity_excludes_generated_copies() {
        let root = test_directory("authoritative-identity");
        fs::write(root.join("existing.rs"), "pub fn authority() {}\n").expect("write source");
        initialize_git_source(&root);
        let before = authoritative_source_identity(&root).expect("hash authoritative source");
        fs::create_dir_all(root.join("generated/rust/acyclic/actors/v1"))
            .expect("create generated tree");
        fs::write(
            root.join("generated/rust/acyclic/actors/v1/actors.rs"),
            "generated-v1",
        )
        .expect("write generated copy");
        fs::create_dir_all(root.join("rust/crates/actors/src/generated"))
            .expect("create package generated tree");
        fs::write(
            root.join("rust/crates/actors/src/generated/acyclic.actors.v1.rs"),
            "generated-package-v1",
        )
        .expect("write package generated copy");
        for path in [
            "ruby/lib/acyclic_sdk/generated_remote_policy.rb",
            "php/src/Acyclic/Runtime/GeneratedRemotePolicy.php",
            "dart/lib/src/generated_remote_policy.dart",
            "jvm/src/main/java/dev/acyclic/transport/GeneratedRemotePolicy.java",
            "dotnet/GeneratedRemotePolicy.cs",
            "typescript/packages/actors/src/generated-client.ts",
            "python/src/acyclic_sdk/generated/actors/v1/actors_pb2.py",
        ] {
            let path = root.join(path);
            fs::create_dir_all(path.parent().expect("facade parent"))
                .expect("create generated facade parent");
            fs::write(path, "generated facade").expect("write generated facade");
        }
        let after = authoritative_source_identity(&root).expect("rehash authoritative source");
        let complete_after = source_identity(&root).expect("hash complete source");
        assert_eq!(before.revision, after.revision);
        assert_eq!(before.digest, after.digest);
        assert_ne!(before.digest, complete_after.digest);
        cleanup(&root);
    }

    #[test]
    fn docs_and_examples_revisions_are_bound_to_generation_source() {
        let root = test_directory("revision-artifacts");
        let docs = root.join("docs.json");
        let examples = root.join("sdk-examples-manifest.json");
        let revision = "0123456789abcdef0123456789abcdef01234567";
        fs::write(&docs, format!(r#"{{"source_revision":"{revision}"}}"#))
            .expect("write docs revision");
        fs::write(
            &examples,
            format!(r#"{{"source":{{"revision":"{revision}"}}}}"#),
        )
        .expect("write examples revision");
        assert!(verify_revision_artifacts(&root, &root, revision).is_ok());

        fs::write(
            &docs,
            r#"{"source_revision":"fedcba9876543210fedcba9876543210fedcba98"}"#,
        )
        .expect("write stale docs revision");
        assert!(verify_revision_artifacts(&root, &root, revision).is_err());
        fs::write(&docs, format!(r#"{{"source_revision":"{revision}"}}"#))
            .expect("restore docs revision");
        fs::write(
            &examples,
            r#"{"source":{"revision":"fedcba9876543210fedcba9876543210fedcba98"}}"#,
        )
        .expect("write stale examples revision");
        assert!(verify_revision_artifacts(&root, &root, revision).is_err());
        cleanup(&root);
    }

    #[test]
    fn rust_snippet_receipts_require_render_compile_and_runtime_bindings() {
        let root = test_directory("rust-snippet-receipt");
        let snippet_path = root.join("snippets/actors.rs");
        let compile_path = root.join("qualification/consumers/actors-compile.bin");
        let runtime_path = root.join("qualification/consumers/actors-runtime.bin");
        let consumer_stdout_path = root.join("qualification/consumers/actors-consumer.stdout");
        let consumer_stderr_path = root.join("qualification/consumers/actors-consumer.stderr");
        let consumer_manifest_path = root.join("qualification/consumers/Cargo.toml");
        let consumer_lock_path = root.join("qualification/consumers/Cargo.lock");
        let package_path = root.join("qualification/packages/actors-sdk.tgz");
        let package_root = root.join("qualification/packages/actors-sdk");
        let package_manifest_path = package_root.join("Cargo.toml");
        fs::create_dir_all(snippet_path.parent().expect("snippet parent"))
            .expect("snippet directory");
        fs::create_dir_all(compile_path.parent().expect("consumer parent"))
            .expect("consumer directory");
        fs::create_dir_all(package_path.parent().expect("package parent"))
            .expect("package directory");
        fs::create_dir_all(&package_root).expect("extracted package directory");
        let snippet_bytes = b"fn main() { println!(\"actors\"); }\n";
        let compile_bytes = b"compiled snippet test artifact";
        let runtime_bytes = b"rust toolchain runtime";
        fs::write(&snippet_path, snippet_bytes).expect("write snippet");
        fs::write(&compile_path, compile_bytes).expect("write compile artifact");
        fs::write(&runtime_path, runtime_bytes).expect("write runtime artifact");
        let consumer_stdout_bytes = b"consumer passed\n";
        let consumer_stderr_bytes = b"";
        fs::write(&consumer_stdout_path, consumer_stdout_bytes).expect("write consumer stdout");
        fs::write(&consumer_stderr_path, consumer_stderr_bytes).expect("write consumer stderr");
        let consumer_manifest_bytes =
            b"[package]\nname = \"rendered-actors\"\nversion = \"0.0.0\"\n\n[dependencies]\nactors-sdk = { path = \"../packages/actors-sdk\" }\n";
        let consumer_lock_bytes = b"# disposable locked consumer\nversion = 4\n\n[[package]]\nname = \"actors-sdk\"\nversion = \"0.1.0\"\n";
        let package_manifest_bytes = b"[package]\nname = \"actors-sdk\"\nversion = \"0.1.0\"\n";
        fs::write(&package_manifest_path, package_manifest_bytes)
            .expect("write extracted package manifest");
        fs::write(package_root.join("src.rs"), b"pub fn actors() {}\n")
            .expect("write extracted package source");
        fs::write(&consumer_manifest_path, consumer_manifest_bytes)
            .expect("write consumer manifest");
        fs::write(&consumer_lock_path, consumer_lock_bytes).expect("write consumer lock");
        let package_tree_digest = directory_digest(&package_root).expect("package tree hash");
        let package_bytes = {
            let encoder = GzEncoder::new(Vec::new(), Compression::default());
            let mut builder = Builder::new(encoder);
            builder
                .append_dir("actors-sdk", &package_root)
                .expect("append package root");
            builder
                .append_path_with_name(&package_manifest_path, "actors-sdk/Cargo.toml")
                .expect("append package manifest");
            builder
                .append_path_with_name(&package_root.join("src.rs"), "actors-sdk/src.rs")
                .expect("append package source");
            builder
                .into_inner()
                .expect("finish tar builder")
                .finish()
                .expect("finish gzip archive")
        };
        fs::write(&package_path, &package_bytes).expect("write package artifact");
        let source_path = "rust/crates/sdk-examples/src/lib.rs";
        let source_bytes = b"pub fn actors_example() {}\n";
        let source_digest = hash_bytes(source_bytes);
        let revision = "0123456789abcdef0123456789abcdef01234567";
        let snippet_digest = hash_bytes(snippet_bytes);
        let authored_source = root.join(source_path);
        fs::create_dir_all(authored_source.parent().expect("source parent"))
            .expect("source directory");
        fs::write(&authored_source, source_bytes).expect("write source snapshot");
        let package_resolution = json!({
            "resolved": true,
            "status": "qualified",
            "command": "cargo test --manifest-path qualification/consumers/Cargo.toml --locked --offline",
            "consumer_command": "cargo test --manifest-path qualification/consumers/Cargo.toml --locked --offline",
            "consumer_exit_code": 0,
            "consumer_stdout_path": "qualification/consumers/actors-consumer.stdout",
            "consumer_stdout_sha256": hash_bytes(consumer_stdout_bytes),
            "consumer_stderr_path": "qualification/consumers/actors-consumer.stderr",
            "consumer_stderr_sha256": hash_bytes(consumer_stderr_bytes),
            "artifact_consumed": true,
            "artifact_consumption_command": "cargo test --manifest-path qualification/consumers/Cargo.toml --locked --offline; installed consumer",
            "package_manager": "cargo",
            "source_revision": revision,
            "compiled_snippet_sha256": snippet_digest,
            "compile_artifact_sha256": hash_bytes(compile_bytes),
            "package_artifact_path": "qualification/packages/actors-sdk.tgz",
            "package_artifact_sha256": hash_bytes(&package_bytes),
            "package_name": "actors-sdk",
            "package_version": "0.1.0",
            "package_root_path": "qualification/packages/actors-sdk",
            "package_manifest_path": "qualification/packages/actors-sdk/Cargo.toml",
            "package_manifest_sha256": hash_bytes(package_manifest_bytes),
            "package_tree_sha256": package_tree_digest,
            "consumer_manifest_path": "qualification/consumers/Cargo.toml",
            "consumer_manifest_sha256": hash_bytes(consumer_manifest_bytes),
            "consumer_lock_path": "qualification/consumers/Cargo.lock",
            "consumer_lock_sha256": hash_bytes(consumer_lock_bytes)
        });
        let manifest = json!({
            "schema": "acyclic.sdk.examples.bundle.v1",
            "source": { "revision": revision },
            "snippets": [{
                "language": "rust",
                "path": "snippets/actors.rs",
                "code_sha256": snippet_digest,
                "source": source_path,
                "source_sha256": source_digest,
                "validation": {
                    "declared_level": "executed",
                    "declared_status": "passed",
                    "receipt": {
                        "command": "cargo test --test rendered-actors-snippet",
                        "status": "qualified",
                        "executed": true,
                        "exit_code": 0,
                        "source_revision": revision,
                        "source_path": source_path,
                        "source_sha256": source_digest,
                        "snippet_path": "snippets/actors.rs",
                        "snippet_sha256": snippet_digest,
                        "compile_artifact_path": "qualification/consumers/actors-compile.bin",
                        "compile_artifact_sha256": hash_bytes(compile_bytes),
                        "runtime_artifact_path": "qualification/consumers/actors-runtime.bin",
                        "runtime_artifact_sha256": hash_bytes(runtime_bytes),
                        "package_artifact_path": "qualification/packages/actors-sdk.tgz",
                        "package_artifact_sha256": hash_bytes(&package_bytes),
                        "package_resolution": package_resolution,
                        "stdout_sha256": hash_bytes(b"snippet stdout"),
                        "stderr_sha256": hash_bytes(b"")
                    }
                }
            }]
        });
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &manifest,
                revision
            )
            .is_ok()
        );
        let mut missing_consumer_log = manifest.clone();
        missing_consumer_log["snippets"][0]["validation"]["receipt"]["package_resolution"]
            .as_object_mut()
            .expect("package resolution object")
            .remove("consumer_stdout_path");
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &missing_consumer_log,
                revision
            )
            .is_err()
        );
        fs::write(&consumer_stdout_path, b"tampered consumer output\n")
            .expect("tamper consumer stdout");
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &manifest,
                revision
            )
            .is_err()
        );
        fs::write(&consumer_stdout_path, consumer_stdout_bytes).expect("restore consumer stdout");
        let mut generic_identity = manifest.clone();
        generic_identity["snippets"][0]["validation"]["receipt"]["package_resolution"]["package_name"] =
            Value::String("sdk-example-consumer".to_owned());
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &generic_identity,
                revision
            )
            .is_err()
        );
        let mut unbound_package = manifest.clone();
        unbound_package["snippets"][0]["validation"]["receipt"]["package_resolution"]["compiled_snippet_sha256"] =
            Value::String(hash_bytes(b"different snippet"));
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &unbound_package,
                revision
            )
            .is_err()
        );

        // A package with the same Cargo name/version is not interchangeable:
        // the consumer path, extracted tree, and archive metadata must all
        // identify the exact bytes that were exercised.
        let unrelated_package_root = root.join("qualification/packages/unrelated-actors-sdk");
        fs::create_dir_all(&unrelated_package_root).expect("create unrelated package");
        fs::write(
            unrelated_package_root.join("Cargo.toml"),
            package_manifest_bytes,
        )
        .expect("write unrelated package manifest");
        fs::write(
            unrelated_package_root.join("src.rs"),
            b"pub fn unrelated_actors() {}\n",
        )
        .expect("write unrelated package source");
        let unrelated_tree_digest =
            directory_digest(&unrelated_package_root).expect("hash unrelated package tree");
        let unrelated_consumer_manifest =
            b"[package]\nname = \"rendered-actors\"\nversion = \"0.0.0\"\n\n[dependencies]\nactors-sdk = { path = \"../packages/unrelated-actors-sdk\" }\n";
        fs::write(&consumer_manifest_path, unrelated_consumer_manifest)
            .expect("point consumer at unrelated package");
        let mut coincident_package = manifest.clone();
        let coincident_resolution =
            &mut coincident_package["snippets"][0]["validation"]["receipt"]["package_resolution"];
        coincident_resolution["package_root_path"] =
            Value::String("qualification/packages/unrelated-actors-sdk".to_owned());
        coincident_resolution["package_manifest_path"] =
            Value::String("qualification/packages/unrelated-actors-sdk/Cargo.toml".to_owned());
        coincident_resolution["package_manifest_sha256"] =
            Value::String(hash_bytes(package_manifest_bytes));
        coincident_resolution["package_tree_sha256"] = Value::String(unrelated_tree_digest);
        coincident_resolution["consumer_manifest_sha256"] =
            Value::String(hash_bytes(unrelated_consumer_manifest));
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &coincident_package,
                revision
            )
            .is_err()
        );
        fs::write(&consumer_manifest_path, consumer_manifest_bytes)
            .expect("restore consumer manifest");

        fs::write(package_root.join("src.rs"), b"pub fn changed_actors() {}\n")
            .expect("alter extracted package source");
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &manifest,
                revision
            )
            .is_err()
        );
        fs::write(package_root.join("src.rs"), b"pub fn actors() {}\n")
            .expect("restore extracted package source");

        let mut mismatched_package_bytes = package_bytes.clone();
        let midpoint = mismatched_package_bytes.len() / 2;
        mismatched_package_bytes[midpoint] ^= 0x01;
        fs::write(&package_path, &mismatched_package_bytes).expect("alter package archive");
        let mut mismatched_archive = manifest.clone();
        mismatched_archive["snippets"][0]["validation"]["receipt"]["package_artifact_sha256"] =
            Value::String(hash_bytes(&mismatched_package_bytes));
        mismatched_archive["snippets"][0]["validation"]["receipt"]["package_resolution"]["package_artifact_sha256"] =
            Value::String(hash_bytes(&mismatched_package_bytes));
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &mismatched_archive,
                revision
            )
            .is_err()
        );
        fs::write(&package_path, &package_bytes).expect("restore package archive");

        let mut trailing_archive = package_bytes.clone();
        trailing_archive.extend_from_slice(b"trailing-junk");
        fs::write(&package_path, &trailing_archive).expect("append archive trailing junk");
        let mut trailing_receipt = manifest.clone();
        trailing_receipt["snippets"][0]["validation"]["receipt"]["package_artifact_sha256"] =
            Value::String(hash_bytes(&trailing_archive));
        trailing_receipt["snippets"][0]["validation"]["receipt"]["package_resolution"]["package_artifact_sha256"] =
            Value::String(hash_bytes(&trailing_archive));
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &trailing_receipt,
                revision
            )
            .is_err()
        );
        fs::write(&package_path, &package_bytes).expect("restore package archive after junk test");

        let changed_lock =
            b"# disposable locked consumer\nversion = 4\n\n[[package]]\nname = \"actors-sdk\"\nversion = \"0.1.0\"\nsource = \"registry+https://example.invalid/index\"\n";
        fs::write(&consumer_lock_path, changed_lock).expect("alter lock source");
        let mut changed_lock_receipt = manifest.clone();
        changed_lock_receipt["snippets"][0]["validation"]["receipt"]["package_resolution"]["consumer_lock_sha256"] =
            Value::String(hash_bytes(changed_lock));
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &changed_lock_receipt,
                revision
            )
            .is_err()
        );
        fs::write(&consumer_lock_path, consumer_lock_bytes).expect("restore consumer lock");

        fs::write(&snippet_path, b"fn main() { println!(\"changed\"); }\n")
            .expect("change rendered snippet");
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &manifest,
                revision
            )
            .is_err()
        );
        fs::write(&snippet_path, snippet_bytes).expect("restore rendered snippet");
        let generic = json!({
            "source": { "revision": revision },
            "snippets": [{
                "language": "rust",
                "path": "snippets/actors.rs",
                "code_sha256": snippet_digest,
                "source": source_path,
                "source_sha256": source_digest,
                "validation": {
                    "declared_level": "executed",
                    "declared_status": "passed",
                    "receipt": {
                        "command": "cargo test",
                        "status": "qualified"
                    }
                }
            }]
        });
        assert!(
            verify_rust_snippet_receipts(
                &root,
                &root,
                &root.join("sdk-examples-manifest.json"),
                &generic,
                revision
            )
            .is_err()
        );
        cleanup(&root);
    }

    #[test]
    fn authority_manifest_binds_source_and_descriptor_bytes() {
        let root = test_directory("authority-binding");
        let source_root = root.join("source");
        let output = root.join("output");
        let source_path = source_root.join("model.rs");
        let descriptor_path = output.join("wire/actors/v1/actors.fds.bin");
        fs::create_dir_all(source_path.parent().expect("source parent")).expect("source directory");
        fs::write(&source_path, b"model").expect("write source");
        fs::create_dir_all(output.join("wire/validation/v1")).expect("options directory");
        fs::write(
            output.join("wire/validation/v1/options.proto"),
            b"syntax = \"proto3\";",
        )
        .expect("write options proto");
        let length_delimited = |field: u8, value: &[u8]| {
            let mut encoded = vec![(field << 3) | 2, value.len() as u8];
            encoded.extend_from_slice(value);
            encoded
        };
        let source_hash = hash_bytes(b"model")
            .strip_prefix("sha256:")
            .unwrap()
            .to_owned();
        let mut canonical = Vec::new();
        canonical.extend_from_slice(b"model.rs");
        canonical.push(0);
        canonical.extend_from_slice(b"model");
        canonical.push(0);
        let mut family_entries = Vec::new();
        for family in FAMILY_VIEWS {
            let source = format!("{}/v1/{}.proto", family.name, family.name);
            let descriptor = format!("{}/v1/{}.fds.bin", family.name, family.name);
            let proto = output.join("wire").join(&source);
            let descriptor_path = output.join("wire").join(&descriptor);
            fs::create_dir_all(proto.parent().expect("family proto parent"))
                .expect("create family wire directory");
            fs::write(&proto, b"syntax = \"proto3\";").expect("write family proto");
            let file = [
                length_delimited(1, source.as_bytes()),
                length_delimited(6, &length_delimited(2, &[])),
            ]
            .concat();
            let descriptor_bytes = length_delimited(1, &file);
            fs::write(&descriptor_path, &descriptor_bytes).expect("write family descriptor");
            family_entries.push(json!({
                "source": source,
                "source_sha256": hash_bytes(b"syntax = \"proto3\";").strip_prefix("sha256:").unwrap(),
                "descriptor": descriptor,
                "descriptor_sha256": hash_bytes(&descriptor_bytes).strip_prefix("sha256:").unwrap(),
                "rpc_shapes": ["unary"],
                "descriptor_role": "canonical_schema"
            }));
        }
        let manifest = json!({
            "schema": "acyclic.sdk.rust-authority.v1",
            "authority": "rust",
            "source_revision": hash_bytes(&canonical).strip_prefix("sha256:").unwrap(),
            "source_files": ["model.rs"],
            "source_file_hashes": {"model.rs": source_hash},
            "families": family_entries
        });
        fs::write(
            output.join("wire/rust-authority.json"),
            serde_json::to_vec(&manifest).expect("encode authority manifest"),
        )
        .expect("write authority manifest");
        assert!(verify_authority_manifest(&source_root, &output).is_ok());
        let mut missing_family = manifest.clone();
        missing_family["families"]
            .as_array_mut()
            .expect("family array")
            .pop();
        fs::write(
            output.join("wire/rust-authority.json"),
            serde_json::to_vec(&missing_family).expect("encode missing-family manifest"),
        )
        .expect("write missing-family manifest");
        assert!(verify_authority_manifest(&source_root, &output).is_err());
        fs::write(
            output.join("wire/rust-authority.json"),
            serde_json::to_vec(&manifest).expect("restore authority manifest"),
        )
        .expect("restore authority manifest");
        let mut missing_shapes = manifest.clone();
        missing_shapes["families"][0]
            .as_object_mut()
            .expect("family object")
            .remove("rpc_shapes");
        fs::write(
            output.join("wire/rust-authority.json"),
            serde_json::to_vec(&missing_shapes).expect("encode missing-shapes manifest"),
        )
        .expect("write missing-shapes manifest");
        assert!(verify_authority_manifest(&source_root, &output).is_err());
        fs::write(
            output.join("wire/rust-authority.json"),
            serde_json::to_vec(&manifest).expect("restore authority manifest"),
        )
        .expect("restore authority manifest");
        fs::write(&source_path, b"changed").expect("mutate source");
        assert!(verify_authority_manifest(&source_root, &output).is_err());
        fs::write(&source_path, b"model").expect("restore source");
        fs::write(&descriptor_path, b"changed descriptor").expect("mutate descriptor");
        assert!(verify_authority_manifest(&source_root, &output).is_err());
        cleanup(&root);
    }

    #[test]
    fn rpc_shapes_are_derived_from_descriptor_methods() {
        let length_delimited = |field: u8, value: &[u8]| {
            let mut encoded = vec![(field << 3) | 2, value.len() as u8];
            encoded.extend_from_slice(value);
            encoded
        };
        let method = |client: bool, server: bool| {
            let mut encoded = Vec::new();
            if client {
                encoded.extend_from_slice(&[0x28, 1]);
            }
            if server {
                encoded.extend_from_slice(&[0x30, 1]);
            }
            encoded
        };
        let service = [
            length_delimited(2, &method(false, false)),
            length_delimited(2, &method(true, false)),
            length_delimited(2, &method(false, true)),
            length_delimited(2, &method(true, true)),
        ]
        .concat();
        let file = [
            length_delimited(1, b"actors/v1/actors.proto"),
            length_delimited(6, &service),
        ]
        .concat();
        let descriptor = length_delimited(1, &file);
        let actual = descriptor_rpc_shapes(&descriptor, "actors/v1/actors.proto")
            .expect("derive descriptor RPC shapes");
        let expected = ["bidi", "client", "server", "unary"]
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
        assert!(descriptor_rpc_shapes(&descriptor, "stream/v2/stream.proto").is_err());
        let archived_actors = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../sdk-contract-wire/tests/fixtures/actors-v1.descriptor.bin"
        ));
        assert_eq!(
            descriptor_rpc_shapes(archived_actors, "actors/v1/actors.proto")
                .expect("derive archived Actors RPC shapes"),
            ["unary"].into_iter().map(str::to_owned).collect()
        );
    }

    #[test]
    fn rpc_method_identities_and_stream_shapes_are_derived_from_descriptor_authority() {
        let length_delimited = |field: u8, value: &[u8]| {
            let mut encoded = vec![(field << 3) | 2, value.len() as u8];
            encoded.extend_from_slice(value);
            encoded
        };
        let method = |name: &str, client: bool, server: bool| {
            let mut encoded = length_delimited(1, name.as_bytes());
            if client {
                encoded.extend_from_slice(&[0x28, 1]);
            }
            if server {
                encoded.extend_from_slice(&[0x30, 1]);
            }
            encoded
        };
        let service = [
            length_delimited(1, b"StreamService"),
            length_delimited(2, &method("Read", false, false)),
            length_delimited(2, &method("Watch", false, true)),
        ]
        .concat();
        let file = [
            length_delimited(1, b"stream/v2/stream.proto"),
            length_delimited(2, b"acyclic.stream.v2"),
            length_delimited(6, &service),
        ]
        .concat();
        let descriptor = length_delimited(1, &file);
        let actual = descriptor_rpc_methods(&descriptor, "stream/v2/stream.proto")
            .expect("derive descriptor RPC identities");
        assert_eq!(
            actual,
            BTreeMap::from([
                (
                    "acyclic.stream.v2.StreamService/Read".to_owned(),
                    "unary".to_owned()
                ),
                (
                    "acyclic.stream.v2.StreamService/Watch".to_owned(),
                    "server".to_owned()
                ),
            ])
        );
    }

    #[test]
    fn authority_inventory_rejects_tampered_descriptor_family_metadata() {
        let root = test_directory("authority-rpc-metadata");
        let wire = root.join("wire/actors/v1");
        fs::create_dir_all(&wire).expect("create authority wire directory");
        let descriptor = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../sdk-contract-wire/tests/fixtures/actors-v1.descriptor.bin"
        ));
        fs::write(wire.join("actors.fds.bin"), descriptor).expect("write Rust descriptor");
        let descriptor_hash = hash_bytes(descriptor)
            .strip_prefix("sha256:")
            .expect("descriptor hash prefix")
            .to_owned();
        let actual_methods = descriptor_rpc_methods(descriptor, "actors/v1/actors.proto")
            .expect("derive methods from Rust descriptor");
        let mut manifest = json!({
            "schema": "acyclic.sdk.rust-authority.v1",
            "authority": "rust",
            "families": [{
                "source": "actors/v1/actors.proto",
                "descriptor": "actors/v1/actors.fds.bin",
                "descriptor_sha256": descriptor_hash,
                "rpc_shapes": ["unary"],
                "rpc_methods": actual_methods.iter().map(|(rpc, shape)| json!({
                    "rpc": rpc,
                    "shape": shape,
                })).collect::<Vec<_>>(),
            }],
        });
        fs::create_dir_all(root.join("wire")).expect("create wire root");
        fs::write(
            root.join("wire/rust-authority.json"),
            serde_json::to_vec(&manifest).expect("encode authority manifest"),
        )
        .expect("write authority manifest");
        assert!(authority_rpc_inventory(&root).is_ok());

        manifest["families"][0]["rpc_methods"][0]["shape"] = json!("server");
        fs::write(
            root.join("wire/rust-authority.json"),
            serde_json::to_vec(&manifest).expect("encode tampered authority manifest"),
        )
        .expect("write tampered authority manifest");
        let error = authority_rpc_inventory(&root).expect_err("tampered method metadata rejected");
        assert!(
            error
                .to_string()
                .contains("RPC methods do not match descriptor")
        );

        let actual_methods = descriptor_rpc_methods(descriptor, "actors/v1/actors.proto")
            .expect("derive methods from Rust descriptor");
        manifest["families"][0]["rpc_methods"] = json!(
            actual_methods
                .iter()
                .map(|(rpc, shape)| json!({ "rpc": rpc, "shape": shape }))
                .collect::<Vec<_>>()
        );
        manifest["families"][0]["source"] = json!("stream/v2/stream.proto");
        fs::write(
            root.join("wire/rust-authority.json"),
            serde_json::to_vec(&manifest).expect("encode wrong-source authority manifest"),
        )
        .expect("write wrong-source authority manifest");
        assert!(
            authority_rpc_inventory(&root).is_err(),
            "descriptor bytes must stay bound to their Rust source file"
        );
        cleanup(&root);
    }

    #[test]
    fn remote_receipts_require_authority_methods_and_stream_shapes() {
        let authority = BTreeMap::from([
            (
                "acyclic.stream.v2.StreamService/Read".to_owned(),
                "unary".to_owned(),
            ),
            (
                "acyclic.stream.v2.StreamService/Watch".to_owned(),
                "server".to_owned(),
            ),
        ]);
        let entry = |methods: &[&str], shapes: &[&str]| {
            json!({
                "family": "stream",
                "methods": methods,
                "features": ["serialization", "transport"],
                "rpc_shapes": shapes,
            })
        };
        let complete = entry(
            &[
                "acyclic.stream.v2.StreamService/Read",
                "acyclic.stream.v2.StreamService/Watch",
            ],
            &["unary", "server"],
        );
        assert!(receipt_family_matches_authority(
            &complete,
            "remote",
            Some(&authority)
        ));
        let unary_only = entry(
            &[
                "acyclic.stream.v2.StreamService/Read",
                "acyclic.stream.v2.StreamService/Watch",
            ],
            &["unary"],
        );
        assert!(!receipt_family_matches_authority(
            &unary_only,
            "remote",
            Some(&authority)
        ));
        let invented = entry(
            &[
                "acyclic.stream.v2.StreamService/Read",
                "acyclic.stream.v2.StreamService/Watch",
                "acyclic.stream.v2.StreamService/Fabricated",
            ],
            &["unary", "server"],
        );
        assert!(!receipt_family_matches_authority(
            &invented,
            "remote",
            Some(&authority)
        ));
    }

    #[test]
    fn service_free_authority_family_is_not_forced_to_claim_rpc_methods() {
        let entry = json!({
            "family": "filesystem",
            "methods": [],
            "features": ["serialization", "embedded"],
            "rpc_shapes": [],
        });
        let authority = BTreeMap::new();
        assert!(receipt_family_matches_authority(
            &entry,
            "embedded",
            Some(&authority)
        ));
    }

    #[test]
    fn capability_validator_scopes_transports_and_service_coverage() {
        let service_authority = BTreeMap::from([
            (
                "acyclic.stream.v2.StreamService/Read".to_owned(),
                "server".to_owned(),
            ),
            (
                "acyclic.stream.v2.StreamService/Append".to_owned(),
                "unary".to_owned(),
            ),
        ]);
        let service_entry = |features: &[&str], methods: &[&str], shapes: &[&str]| {
            json!({
                "family": "stream",
                "methods": methods,
                "features": features,
                "rpc_shapes": shapes,
            })
        };

        let empty_docs = service_entry(&["docs"], &[], &[]);
        assert!(!receipt_family_matches_authority(
            &empty_docs,
            "docs",
            Some(&service_authority)
        ));

        let http_stream = service_entry(
            &["http", "transport"],
            &["acyclic.stream.v2.StreamService/Read"],
            &["server"],
        );
        assert!(receipt_family_matches_authority(
            &http_stream,
            "docs",
            Some(&service_authority)
        ));
        let partial_methods = string_set_field(&http_stream, "methods").expect("partial methods");
        let partial_shapes = string_set_field(&http_stream, "rpc_shapes").expect("partial shapes");
        assert_eq!(
            receipt_family_is_partial(&partial_methods, &partial_shapes, &service_authority),
            Some(true)
        );

        let grpc_protocol = json!({
            "family": "protocol",
            "methods": [],
            "features": ["grpc", "transport"],
            "rpc_shapes": [],
        });
        assert!(!receipt_family_matches_authority(
            &grpc_protocol,
            "install",
            Some(&BTreeMap::new())
        ));

        let http_protocol = json!({
            "family": "protocol",
            "methods": [],
            "features": ["http", "transport"],
            "rpc_shapes": [],
        });
        assert!(!receipt_family_matches_authority(
            &http_protocol,
            "docs",
            Some(&BTreeMap::new())
        ));

        assert!(!receipt_family_matches_authority(
            &service_entry(
                &["serialization"],
                &["acyclic.stream.v2.StreamService/Read"],
                &["server"],
            ),
            "unknown-capability",
            Some(&service_authority)
        ));
    }

    #[test]
    fn authority_manifest_rejects_nonportable_family_paths() {
        let root = test_directory("authority-path");
        let source_root = root.join("source");
        let output = root.join("output");
        fs::create_dir_all(&source_root).expect("source directory");
        fs::create_dir_all(output.join("wire")).expect("wire directory");
        fs::create_dir_all(output.join("wire/validation/v1")).expect("options directory");
        fs::write(
            output.join("wire/validation/v1/options.proto"),
            b"syntax = \"proto3\";",
        )
        .expect("write options proto");
        fs::write(source_root.join("model.rs"), b"model").expect("write source");
        let mut canonical = Vec::new();
        canonical.extend_from_slice(b"model.rs");
        canonical.push(0);
        canonical.extend_from_slice(b"model");
        canonical.push(0);
        let model_revision = hash_bytes(&canonical)
            .strip_prefix("sha256:")
            .unwrap()
            .to_owned();
        let source_hash = hash_bytes(b"model")
            .strip_prefix("sha256:")
            .unwrap()
            .to_owned();
        let manifest = json!({
            "schema": "acyclic.sdk.rust-authority.v1",
            "authority": "rust",
            "source_revision": model_revision,
            "source_files": ["model.rs"],
            "source_file_hashes": {"model.rs": source_hash},
            "families": [{
                "source": "../escape.proto",
                "source_sha256": "0",
                "descriptor": "actors.fds.bin",
                "descriptor_sha256": "0",
                "rpc_shapes": ["unary"],
                "descriptor_role": "canonical_schema"
            }]
        });
        fs::write(
            output.join("wire/rust-authority.json"),
            serde_json::to_vec(&manifest).expect("encode authority manifest"),
        )
        .expect("write authority manifest");
        let error = verify_authority_manifest(&source_root, &output)
            .expect_err("parent path must fail closed");
        assert!(error.to_string().contains("non-portable path"));
        cleanup(&root);
    }

    #[test]
    fn regenerated_artifacts_reject_missing_and_extra_files() {
        let root = test_directory("artifact-set");
        let fresh = root.join("fresh");
        fs::create_dir_all(&fresh).expect("create fresh output");
        fs::write(root.join("artifact.json"), b"stable").expect("write expected");
        let artifact = Artifact {
            path: "artifact.json".into(),
            sha256: hash_bytes(b"stable"),
            bytes: 6,
        };
        assert!(verify_artifacts(&root, std::slice::from_ref(&artifact)).is_ok());
        fs::write(root.join("unlisted.json"), b"extra").expect("write unlisted");
        assert!(verify_artifacts(&root, std::slice::from_ref(&artifact)).is_err());
        fs::remove_file(root.join("unlisted.json")).expect("remove unlisted");
        assert!(compare_fresh_artifacts(&root, &fresh, std::slice::from_ref(&artifact)).is_err());
        fs::write(fresh.join("artifact.json"), b"stable").expect("write fresh");
        assert!(compare_fresh_artifacts(&root, &fresh, std::slice::from_ref(&artifact)).is_ok());
        fs::write(fresh.join("extra.json"), b"extra").expect("write extra");
        assert!(compare_fresh_artifacts(&root, &fresh, std::slice::from_ref(&artifact)).is_err());
        cleanup(&root);
    }

    #[test]
    fn source_digest_changes_for_untracked_author_input() {
        let root = test_directory("untracked-source");
        fs::write(root.join("existing.rs"), b"one").expect("write existing source");
        initialize_git_source(&root);
        let first = source_identity(&root).expect("hash source");
        fs::write(root.join("new-author-input.rs"), b"two").expect("write new source");
        let second = source_identity(&root).expect("rehash source");
        assert_ne!(first.digest, second.digest);
        cleanup(&root);
    }

    #[test]
    fn source_identity_rejects_nested_path_with_parent_git_root() {
        let root = test_directory("nested-git-root");
        fs::write(root.join("existing.rs"), b"one").expect("write source");
        initialize_git_source(&root);
        let nested = root.join("nested");
        fs::create_dir_all(&nested).expect("create nested source path");
        let error =
            source_identity(&nested).expect_err("nested path must not inherit parent Git root");
        assert!(
            error
                .to_string()
                .contains("does not match requested source root")
        );
        cleanup(&root);
    }

    #[test]
    fn source_identity_ignores_ambient_git_repository_selection_environment() {
        const CHILD_MARKER: &str = "ACYCLIC_SDK_GENERATION_AMBIENT_GIT_TEST";
        const ROOT_PATH: &str = "ACYCLIC_SDK_GENERATION_AMBIENT_GIT_ROOT";
        const EXPECTED_REVISION: &str = "ACYCLIC_SDK_GENERATION_AMBIENT_GIT_REVISION";

        if env::var_os(CHILD_MARKER).is_some() {
            let root = PathBuf::from(env::var_os(ROOT_PATH).expect("child source root"));
            let expected = env::var(EXPECTED_REVISION).expect("child source revision");
            let identity = source_identity(&root).expect("ambient Git variables must be ignored");
            assert_eq!(identity.revision, expected);
            assert!(!identity.dirty);
            return;
        }

        let root = test_directory("ambient-git-root");
        fs::write(root.join("existing.rs"), b"source").expect("write source");
        initialize_git_source(&root);
        let expected_revision =
            command_stdout(&root, "git", &["rev-parse", "HEAD"]).expect("read source revision");

        let unrelated = test_directory("ambient-git-unrelated");
        fs::write(unrelated.join("existing.rs"), b"unrelated").expect("write unrelated source");
        initialize_git_source(&unrelated);
        let unrelated_git = unrelated.join(".git");
        let child = Command::new(env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "tests::source_identity_ignores_ambient_git_repository_selection_environment",
                "--nocapture",
            ])
            .env(CHILD_MARKER, "1")
            .env(ROOT_PATH, &root)
            .env(EXPECTED_REVISION, &expected_revision)
            .env("GIT_DIR", &unrelated_git)
            .env("GIT_WORK_TREE", &unrelated)
            .env("GIT_COMMON_DIR", &unrelated_git)
            .env("GIT_INDEX_FILE", unrelated_git.join("index"))
            .env("GIT_OBJECT_DIRECTORY", unrelated_git.join("objects"))
            .env(
                "GIT_ALTERNATE_OBJECT_DIRECTORIES",
                unrelated_git.join("objects"),
            )
            .output()
            .expect("run ambient environment child");
        assert!(
            child.status.success(),
            "child identity check failed: {}{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );
        cleanup(&unrelated);
        cleanup(&root);
    }

    #[test]
    fn archived_baseline_hashes_are_pinned() {
        let root =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../sdk-contract-wire/tests/fixtures");
        for (family, expected) in IMMUTABLE_BASELINES {
            let filename = match *family {
                "actors" => "actors-v1.descriptor.bin",
                "objects" => "objects-v2.descriptor.bin",
                "stream" => "stream-v2.descriptor.bin",
                "workers" => "workers-v1.descriptor.bin",
                "filesystem" => "filesystem-v2.descriptor.bin",
                "harness" => "harness-v2.descriptor.bin",
                "machines" => "machines-v1.descriptor.bin",
                "inference" => "inference-v1.descriptor.bin",
                "protocol" => "protocol-v1.descriptor.bin",
                _ => unreachable!("pinned test family has no fixture"),
            };
            let actual = hash_bytes(&fs::read(root.join(filename)).expect("baseline fixture"));
            assert_eq!(actual, format!("sha256:{expected}"), "{family} baseline");
        }
    }

    #[test]
    fn changed_archived_baseline_is_rejected() {
        let root = test_directory("immutable-baseline");
        let baseline = root.join("baseline.bin");
        fs::write(&baseline, b"changed").expect("write baseline");
        let error = verify_immutable_baseline("actors", &baseline)
            .expect_err("unrecognized baseline bytes must fail closed");
        assert!(error.contains("compatibility baseline changed"));
        cleanup(&root);
    }

    #[test]
    fn empty_language_inventory_fails_closed() {
        let root = test_directory("empty-inventory");
        fs::create_dir_all(root.join("languages")).expect("create language directory");
        fs::write(
            root.join("languages/package-names.json"),
            b"{\"families\":{}}",
        )
        .expect("write empty inventory");
        let output = root.join("output");
        fs::create_dir_all(&output).expect("create output");
        assert!(language_inventory(&root, &output, "uncommitted", None).is_err());
        cleanup(&root);
    }

    #[test]
    fn direct_inventory_cannot_qualify_unbound_receipts() {
        let root = test_directory("direct-inventory-evidence");
        fs::create_dir_all(root.join("languages")).expect("create language directory");
        fs::write(
            root.join("languages/package-names.json"),
            b"{\"families\":{\"rust\":{\"registry\":\"crates.io\"}}}",
        )
        .expect("write language inventory");
        let output = root.join("output");
        fs::create_dir_all(output.join("qualification")).expect("create qualification directory");
        let digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let evidence = format!(
            "{{\"schema\":\"{EVIDENCE_SCHEMA}\",\"language\":\"rust\",\"source_revision\":\"uncommitted\",\"contract_digest\":\"{digest}\",\"artifact_digest\":\"{digest}\",\"remote\":{{\"status\":\"qualified\",\"tests\":[\"case {digest}\"]}},\"embedded\":{{\"status\":\"qualified\",\"tests\":[\"case {digest}\"]}},\"docs\":{{\"status\":\"qualified\",\"tests\":[\"case {digest}\"]}},\"snippets\":{{\"status\":\"qualified\",\"tests\":[\"case {digest}\"]}},\"install\":{{\"status\":\"qualified\",\"tests\":[\"case {digest}\"]}}}}"
        );
        fs::write(output.join("qualification/rust.json"), evidence).expect("write evidence");
        let inventory = language_inventory(&root, &output, "uncommitted", None)
            .expect("inventory remains inspectable");
        assert_eq!(inventory[0].remote, "pending");
        assert_eq!(
            inventory[0].outstanding,
            vec!["pending:qualification evidence"]
        );
        cleanup(&root);
    }

    #[test]
    fn language_inventory_canonicalizes_package_and_target_aliases() {
        let root = test_directory("canonical-language-aliases");
        fs::create_dir_all(root.join("languages")).expect("create language directory");
        fs::write(
            root.join("languages/package-names.json"),
            br#"{"families":{"jvm":{"registry":"Maven Central","umbrella":"dev.acyclic:sdk-java","aliases":["jvm"]}}}"#,
        )
        .expect("write package inventory");
        fs::write(
            root.join("languages/generation-targets.json"),
            br#"{"targets":[{"id":"java","aliases":["jvm"],"status":"candidate","package":{"ecosystem":"Maven Central","artifact":"dev.acyclic:sdk-java"}}]}"#,
        )
        .expect("write generation targets");
        let output = root.join("output");
        fs::create_dir_all(&output).expect("create output");
        let inventory = language_inventory(&root, &output, "uncommitted", None)
            .expect("aliases remain inspectable");
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].id, "java");
        cleanup(&root);
    }
}
