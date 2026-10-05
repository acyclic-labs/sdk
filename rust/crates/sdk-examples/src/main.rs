//! Deterministic snippet bundle writer used by `sdk-generation`.
//!
//! The bundle is intentionally small: Rust owns the scenario registry, this
//! binary writes source files and a manifest, and language validation is an
//! explicit command result. A snippet is marked `qualified` only after its
//! language runtime actually executes it.
#![recursion_limit = "256"]

use acyclic_sdk_examples::{
    filesystem_scenarios, guide_projections, harness_scenarios, inference_scenarios, machines_scenarios,
    objects_scenarios, workers_scenarios, GUIDE_SCENARIOS, Language, RenderedSnippet,
    TransportFixture, execute_actors_roundtrip, execute_stream_append_read, render_all, scenarios,
    transport_fixtures,
};
use acyclic_sdk_examples::fixtures::{
    filesystem_harness_scenarios, qualification_scenarios, scenario_expectation,
};
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

mod source_closure;

const MANIFEST: &str = "sdk-examples-manifest.json";
const FIXTURE_MANIFEST: &str = "sdk-transport-fixtures-manifest.json";
const RECEIPT: &str = "sdk-qualification-receipt.json";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Generate,
    Check,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliCommand {
    Snippets(Mode),
    Fixtures(Mode),
    Receipts,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("sdk-examples: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let (command, request) = parse_args()?;
    let request: Value = serde_json::from_slice(
        &fs::read(&request).map_err(|error| format!("read request: {error}"))?,
    )
    .map_err(|error| format!("decode request: {error}"))?;
    let source_root = request
        .get("source_root")
        .and_then(Value::as_str)
        .ok_or("request.source_root is required")?;
    let output = request
        .get("output")
        .and_then(Value::as_str)
        .ok_or("request.output is required")?;
    let source_root = PathBuf::from(source_root);
    let output = PathBuf::from(output);
    let requested_model_source_digest = request
        .get("model_source_digest")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| env::var("SDK_MODEL_SOURCE_DIGEST").ok());
    if !source_root.is_dir() {
        return Err(format!(
            "source root is not a directory: {}",
            source_root.display()
        ));
    }
    let source_sha256 = scenario_source_sha256(&source_root)?;
    ensure_compiled_source_matches(&source_sha256)?;
    let computed_model_source_digest =
        source_closure::model_digest(&source_root, option_env!("SDK_EXAMPLES_BUILD_TARGET"))?;
    if let Some(requested) = requested_model_source_digest {
        if requested != computed_model_source_digest {
            return Err(format!(
                "model source digest does not match the resolved local dependency closure: requested {requested}, observed {computed_model_source_digest}"
            ));
        }
    }
    let model_source_digest = computed_model_source_digest;
    if matches!(
        command,
        CliCommand::Snippets(Mode::Generate)
            | CliCommand::Fixtures(Mode::Generate)
            | CliCommand::Receipts
    ) {
        fs::create_dir_all(&output).map_err(|error| format!("create output: {error}"))?;
    }
    match command {
        CliCommand::Snippets(mode) => {
            let bundle = build_bundle(&source_root, &output)?;
            match mode {
                Mode::Generate => write_bundle(&output, &bundle),
                Mode::Check => check_bundle(&output, &bundle),
            }
        }
        CliCommand::Fixtures(mode) => {
            let bundle = build_fixture_bundle(&source_root, &model_source_digest)?;
            match mode {
                Mode::Generate => write_fixture_bundle(&output, &bundle),
                Mode::Check => check_fixture_bundle(&output, &bundle),
            }
        }
        CliCommand::Receipts => {
            write_qualification_receipt(&source_root, &output, &model_source_digest)
        }
    }
}

fn parse_args() -> Result<(CliCommand, PathBuf), String> {
    let mut args = env::args().skip(1);
    let first = args.next();
    let command = match first.as_deref() {
        Some("generate") => CliCommand::Snippets(Mode::Generate),
        Some("check") => CliCommand::Snippets(Mode::Check),
        Some("fixtures") => {
            let mode = match args.next().as_deref() {
                Some("generate") => Mode::Generate,
                Some("check") => Mode::Check,
                Some(other) => {
                    return Err(format!(
                        "unknown fixtures mode {other}; expected generate or check"
                    ));
                }
                None => return Err("fixtures requires generate or check".to_owned()),
            };
            CliCommand::Fixtures(mode)
        }
        Some("receipts") => CliCommand::Receipts,
        Some("--help") | Some("-h") => {
            println!("sdk-examples <generate|check> --request PATH");
            println!("sdk-examples fixtures <generate|check> --request PATH");
            println!("sdk-examples receipts --request PATH");
            std::process::exit(0);
        }
        Some(other) => {
            return Err(format!(
                "unknown command {other}; expected generate, check, fixtures, or receipts"
            ));
        }
        None => return Err("usage: sdk-examples <generate|check> --request PATH".to_owned()),
    };
    if args.next().as_deref() != Some("--request") {
        return Err("--request PATH is required".to_owned());
    }
    let request = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "--request PATH is required".to_owned())?;
    if args.next().is_some() {
        return Err("unexpected argument after --request PATH".to_owned());
    }
    Ok((command, request))
}

fn write_qualification_receipt(
    source_root: &Path,
    output: &Path,
    model_source_digest: &str,
) -> Result<(), String> {
    let source_sha256 = scenario_source_sha256(&source_root)?;
    let build_target = option_env!("SDK_EXAMPLES_BUILD_TARGET");
    let build_recipe_sha256 = source_closure::recipe_digest(source_root, build_target)?;
    let source_files = source_closure::closure_files(source_root)?;
    let snippets = build_bundle(source_root, output)?;
    let fixtures = build_fixture_bundle(source_root, model_source_digest)?;
    let guides = build_guide_receipts(source_root)?;
    ensure_source_unchanged(source_root, &source_sha256)?;
    let snippets_bytes = serde_json::to_vec(&snippets)
        .map_err(|error| format!("encode snippets evidence: {error}"))?;
    let fixtures_bytes = serde_json::to_vec(&fixtures)
        .map_err(|error| format!("encode fixtures evidence: {error}"))?;
    let rpc_scenarios = fixtures
        .get("qualification")
        .and_then(|qualification| qualification.get("rpc_scenarios"))
        .cloned()
        .ok_or("fixture manifest is missing Rust-owned RPC scenarios")?;
    let rpc_scenario_count = rpc_scenarios
        .get("count")
        .and_then(Value::as_u64)
        .ok_or("Rust-owned RPC scenario set is missing count")?;
    if rpc_scenario_count != 35 {
        return Err(format!("Rust-owned RPC scenario set has {rpc_scenario_count} entries; expected 35"));
    }
    let rpc_scenarios_bytes = serde_json::to_vec(&rpc_scenarios)
        .map_err(|error| format!("encode RPC scenario evidence: {error}"))?;
    let seed_graph = fixtures
        .get("qualification")
        .and_then(|qualification| qualification.get("seed_graph"))
        .cloned()
        .ok_or("fixture manifest is missing Rust-owned semantic seed graph")?;
    let seed_graph_count = seed_graph
        .get("count")
        .and_then(Value::as_u64)
        .ok_or("Rust-owned semantic seed graph is missing count")?;
    if seed_graph_count != rpc_scenario_count {
        return Err("Rust-owned semantic seed graph count differs from RPC scenarios".to_owned());
    }
    let seed_graph_bytes = serde_json::to_vec(&seed_graph)
        .map_err(|error| format!("encode semantic seed graph evidence: {error}"))?;
    let status = if [(&snippets, &snippets_bytes), (&fixtures, &fixtures_bytes)]
        .into_iter()
        .all(|(manifest, _)| manifest_status(manifest) == "passed")
        && guides
            .iter()
            .all(|guide| matches!(guide["status"].as_str(), Some("passed" | "qualified")))
    {
        "passed"
    } else {
        "partial"
    };
    let receipt = json!({
        "schema": "acyclic.sdk.qualification-receipt.v1",
        "status": status,
        "source": {
            "path": "rust/crates/sdk-examples",
            "files": source_files,
            "sha256": source_sha256,
            "build_target": build_target,
            "build_recipe_sha256": build_recipe_sha256,
            "revision": git_revision(source_root),
            "revision_kind": "git-revision",
            "model_source_digest": model_source_digest,
            "model_source_digest_kind": "rust-model-sha256",
        },
        "suite": {
            "id": "sdk-examples",
            "status": status,
            "command": "sdk-examples receipts --request PATH",
            "scopes": ["typed-rust", "loopback-local", "source-bound"],
        },
        "scenarios": guides,
        "artifacts": [
            {
                "id": "snippets-manifest",
                "kind": "manifest",
                "status": manifest_status(&snippets),
                "schema": "acyclic.sdk.examples.bundle.v1",
                "sha256": hash(&snippets_bytes),
            },
            {
                "id": "transport-fixtures-manifest",
                "kind": "manifest",
                "status": manifest_status(&fixtures),
                "schema": "acyclic.sdk.transport-fixtures.v1",
                "sha256": hash(&fixtures_bytes),
            },
            {
                "id": "rust-rpc-scenarios",
                "kind": "semantic-scenario-set",
                "status": "passed",
                "schema": "acyclic.sdk.rust-rpc-scenarios.v1",
                "count": rpc_scenario_count,
                "sha256": hash(&rpc_scenarios_bytes),
                "source": "rust/crates/sdk-examples/src/fixtures/filesystem_harness.rs",
            },
            {
                "id": "rust-semantic-seed-graph",
                "kind": "semantic-seed-graph",
                "status": "passed",
                "schema": "acyclic.sdk.rust-semantic-seed-graph.v1",
                "count": seed_graph_count,
                "sha256": hash(&seed_graph_bytes),
                "source": "rust/crates/sdk-examples/src/fixtures/filesystem_harness.rs",
            },
            {
                "id": "rust-canonical-vectors",
                "kind": "golden-vector-bundle",
                "status": "passed",
                "schema": "acyclic.sdk.rust-canonical-vectors.v1",
                "path": "rust-canonical-vectors.json",
                "sha256": fixtures["golden_vectors"]["sha256"],
                "count": fixtures["golden_vectors"]["count"],
                "model_source_digest": model_source_digest,
                "model_source_digest_kind": "rust-model-sha256",
                "git_revision": git_revision(source_root),
                "git_revision_kind": "git-revision",
            },
        ],
    });
    let bytes =
        serde_json::to_vec_pretty(&receipt).map_err(|error| format!("encode receipt: {error}"))?;
    fs::write(output.join(RECEIPT), [bytes.as_slice(), b"\n"].concat())
        .map_err(|error| format!("write qualification receipt: {error}"))
}

fn build_guide_receipts(source_root: &Path) -> Result<Vec<Value>, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("build guide scenario runtime: {error}"))?;
    GUIDE_SCENARIOS
        .iter()
        .map(|spec| {
            let source_sha256 = hash(
                &fs::read(source_root.join(spec.source))
                    .map_err(|error| format!("read guide source {}: {error}", spec.source))?,
            );
            let result = match spec.id {
                "actors-create-roundtrip" => acyclic_sdk_examples::execute_actors_roundtrip()
                    .map_err(|error| error.to_string())
                    .map(|()| json!({
                        "status": "passed",
                        "scope": "rust-wire-validation",
                        "evidence": {
                            "roundtrip": true,
                            "validator": "acyclic_actors::validate_create",
                        },
                    })),
                "stream-append-read" => runtime
                    .block_on(acyclic_sdk_examples::execute_stream_append_read())
                    .map_err(|error| error.to_string())
                    .map(|records| json!({
                        "status": if records.len() == 2 { "passed" } else { "failed" },
                        "scope": "rust-memory-provider",
                        "evidence": {
                            "record_count": records.len(),
                            "records": records.iter().map(|record| String::from_utf8_lossy(record).into_owned()).collect::<Vec<_>>(),
                        },
                    })),
                acyclic_sdk_examples::filesystem_scenarios::SCENARIO_ID => runtime
                    .block_on(acyclic_sdk_examples::filesystem_scenarios::execute_filesystem_scenario())
                    .map_err(|error| error.to_string())
                    .map(|receipt| {
                        json!({
                            "status": if receipt.checkpointed && receipt.mounted_bindings == 2 { "passed" } else { "failed" },
                            "scope": "rust-memory-provider",
                            "evidence": {
                                "durable_volume_created": receipt.durable_volume_created,
                                "ephemeral_volume_created": receipt.ephemeral_volume_created,
                                "mounted_bindings": receipt.mounted_bindings,
                                "checkpointed": receipt.checkpointed,
                                "written_path": receipt.written_path,
                            },
                        })
                    }),
                acyclic_sdk_examples::harness_scenarios::SCENARIO_ID => {
                    let receipt = runtime
                        .block_on(acyclic_sdk_examples::harness_scenarios::execute_harness_scenario());
                    Ok(json!({
                            "status": if receipt.admitted_and_completed && receipt.cancellation_rejected_admission && receipt.recovered_with_fresh_group && receipt.journal_boundary_enforced { "passed" } else { "failed" },
                            "scope": "rust-memory-provider",
                            "evidence": {
                                "admitted_and_completed": receipt.admitted_and_completed,
                                "cancellation_rejected_admission": receipt.cancellation_rejected_admission,
                                "recovered_with_fresh_group": receipt.recovered_with_fresh_group,
                                "journal_boundary_enforced": receipt.journal_boundary_enforced,
                            },
                    }))
                }
                acyclic_sdk_examples::inference_scenarios::SCENARIO_ID => acyclic_sdk_examples::inference_scenarios::execute()
                    .map_err(|error| error.to_string())
                    .map(|receipt| json!({"status": receipt.status, "scope": receipt.scope, "evidence": {"event_count": receipt.event_count, "terminal": receipt.terminal}})),
                acyclic_sdk_examples::machines_scenarios::SCENARIO_ID => runtime
                    .block_on(acyclic_sdk_examples::machines_scenarios::execute())
                    .map_err(|error| error.to_string())
                    .map(|receipt| json!({"status": receipt.status, "scope": receipt.scope, "evidence": {"event_count": receipt.event_count, "checkpoint_children": receipt.checkpoint_children, "machine_state": format!("{:?}", receipt.machine_state)}})),
                acyclic_sdk_examples::objects_scenarios::SCENARIO_ID => runtime
                    .block_on(acyclic_sdk_examples::objects_scenarios::execute())
                    .map_err(|error| error.to_string())
                    .map(|receipt| json!({"status": receipt.status, "scope": receipt.scope, "evidence": {"body_size": receipt.body_size}})),
                acyclic_sdk_examples::workers_scenarios::SCENARIO_ID => acyclic_sdk_examples::workers_scenarios::execute()
                    .map_err(|error| error.to_string())
                    .map(|receipt| json!({"status": receipt.status, "scope": receipt.scope, "evidence": {"module_sha256": hash(&receipt.module_sha256)}})),
                other => return Err(format!("unregistered guide scenario {other}")),
            }
            .map_err(|error| format!("guide scenario {} failed: {error}", spec.id))?;
            Ok(json!({
                "id": spec.id,
                "family": spec.family,
                "source": spec.source,
                "source_sha256": source_sha256,
                "status": result["status"],
                "scope": result["scope"],
                "evidence": result["evidence"],
            }))
        })
        .collect()
}

fn manifest_status(manifest: &Value) -> &'static str {
    let receipts = manifest
        .get("snippets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            entry
                .pointer("/validation/receipt/status")
                .and_then(Value::as_str)
        })
        .collect::<Vec<_>>();
    if receipts.iter().any(|status| *status == "failed") {
        "partial"
    } else if receipts.iter().any(|status| *status == "pending") {
        "partial"
    } else {
        "passed"
    }
}

fn scenario_source_sha256(source_root: &Path) -> Result<String, String> {
    source_closure::source_digest(source_root, option_env!("SDK_EXAMPLES_BUILD_TARGET"))
}

fn ensure_compiled_source_matches(source_sha256: &str) -> Result<(), String> {
    let compiled = env!("SDK_EXAMPLES_SOURCE_SHA256");
    if source_sha256 != compiled {
        return Err(format!(
            "live example source closure does not match compiled producer: compiled {compiled}, observed {source_sha256}; rebuild from the matching source snapshot"
        ));
    }
    Ok(())
}
fn ensure_source_unchanged(source_root: &Path, expected: &str) -> Result<(), String> {
    let actual = scenario_source_sha256(source_root)?;
    if actual != expected {
        return Err(format!(
            "source-owned example inputs changed during generation: expected {expected}, observed {actual}"
        ));
    }
    Ok(())
}

fn build_bundle(source_root: &Path, output: &Path) -> Result<Value, String> {
    let source_sha256 = scenario_source_sha256(source_root)?;
    let build_target = option_env!("SDK_EXAMPLES_BUILD_TARGET");
    let build_recipe_sha256 = source_closure::recipe_digest(source_root, build_target)?;
    let source_files = source_closure::closure_files(source_root)?;
    ensure_compiled_source_matches(&source_sha256)?;
    let source_revision = git_revision(source_root);
    let snippets = all_rendered_snippets();
    let mut entries = Vec::new();
    let mut files = BTreeMap::new();
    for snippet in snippets {
        let snippet_source_sha256 = hash(
            &fs::read(source_root.join(snippet.metadata.source)).map_err(|error| {
                format!(
                    "read snippet source {}: {error}",
                    source_root.join(snippet.metadata.source).display()
                )
            })?,
        );
        let validation = validate_snippet(
            source_root,
            &snippet,
            &snippet_source_sha256,
            &output.join(".validation"),
        )?;
        let relative = format!(
            "snippets/{}/{}.{}",
            snippet.metadata.id,
            snippet.metadata.language.as_str(),
            extension(snippet.metadata.language)
        );
        files.insert(relative.clone(), snippet.code.clone());
        entries.push(json!({
            "id": snippet.metadata.id,
            "family": snippet.metadata.family,
            "title": snippet.metadata.title,
            "language": snippet.metadata.language.as_str(),
            "language_name": snippet.metadata.language.language_name(),
            "source": snippet.metadata.source,
            "source_sha256": snippet_source_sha256,
            "source_closure_sha256": source_sha256,
            "snippet_source_sha256": snippet_source_sha256,
            "capability": snippet.capability.as_str(),
            "validation": {
                "declared_level": format!("{:?}", snippet.metadata.validation.level).to_ascii_lowercase(),
                "declared_status": format!("{:?}", snippet.metadata.validation.status).to_ascii_lowercase(),
                "evidence": snippet.metadata.validation.evidence,
                "receipt": validation,
            },
            "path": relative,
            "code_sha256": hash(snippet.code.as_bytes()),
        }));
    }
    let rpc_scenarios = rust_rpc_scenarios();
    ensure_source_unchanged(source_root, &source_sha256)?;
    Ok(json!({
        "schema": "acyclic.sdk.examples.bundle.v1",
        "generator": "acyclic-sdk-examples@0.2.0",
        "source": {
            "revision": source_revision,
            "path": "rust/crates/sdk-examples",
            "files": source_files,
            "sha256": source_sha256,
            "build_target": build_target,
            "build_recipe_sha256": build_recipe_sha256,
        },
        "snippets": entries,
        "files": files.keys().collect::<Vec<_>>(),
    }))
}


fn rust_rpc_scenarios() -> Vec<Value> {
    qualification_scenarios()
        .map(|scenario| {
            let semantic = scenario_expectation(scenario);
            json!({
                "family": scenario.family,
                "operation": scenario.operation,
                "input": scenario.input,
                "expected": scenario.expected,
                "order": scenario.order,
                "seed": scenario.seed,
                "depends_on": scenario.depends_on,
                "known_output": scenario.known_output,
                "semantic": {
                    "operation_id": semantic.operation_id,
                    "authority": semantic.authority,
                    "cursor": semantic.cursor,
                    "status": semantic.status,
                    "output": semantic.output,
                },
                "source": "rust/crates/sdk-examples/src/fixtures/filesystem_harness.rs",
            })
        })
        .collect()
}

fn build_fixture_bundle(source_root: &Path, model_source_digest: &str) -> Result<Value, String> {
    let source_sha256 = scenario_source_sha256(source_root)?;
    let build_target = option_env!("SDK_EXAMPLES_BUILD_TARGET");
    let build_recipe_sha256 = source_closure::recipe_digest(source_root, build_target)?;
    let source_files = source_closure::closure_files(source_root)?;
    ensure_compiled_source_matches(&source_sha256)?;
    let mut entries = Vec::new();
    for fixture in transport_fixtures() {
        let validation = validate_fixture(&fixture, &source_sha256)?;
        let requests = fixture
            .requests
            .iter()
            .map(|request| {
                let path = format!("fixtures/{}/{}.bin", fixture.id, request.name);
                json!({
                    "name": request.name,
                    "message": request.message,
                    "path": path,
                    "size": request.bytes.len(),
                    "sha256": hash(&request.bytes),
                })
            })
            .collect::<Vec<_>>();
        let expected_bytes = expected_bytes(&fixture);
        let expected_path = format!("fixtures/{}/expected.json", fixture.id);
        entries.push(json!({
            "id": fixture.id,
            "operation_id": fixture.operation_id,
            "scenario_id": fixture.scenario_id,
            "family": fixture.family,
            "route": fixture.route,
            "source": "rust/crates/sdk-examples",
            "source_files": &source_files,
            "source_sha256": source_sha256,
            "requests": requests,
            "expected": fixture.expected,
            "expected_path": expected_path,
            "expected_sha256": hash(&expected_bytes),
            "validation": validation,
        }));
    }
    let source_revision = git_revision(source_root);
    let vectors = canonical_wire_vectors(
        &source_revision,
        model_source_digest,
        &source_sha256,
        &source_files,
        build_target,
        &build_recipe_sha256,
    );
    let vector_bytes = json_bytes(&vectors);
    let rpc_scenarios = rust_rpc_scenarios();
    let typed_wire_evidence = typed_wire_evidence()?;
    let seed_graph = json!({
        "schema": "acyclic.sdk.rust-semantic-seed-graph.v1",
        "count": rpc_scenarios.len(),
        "steps": rpc_scenarios.iter().map(|scenario| json!({
            "order": scenario["order"],
            "family": scenario["family"],
            "operation": scenario["operation"],
            "seed": scenario["seed"],
            "depends_on": scenario["depends_on"],
            "known_output": scenario["known_output"],
            "semantic": scenario["semantic"],
        })).collect::<Vec<_>>(),
    });
    ensure_source_unchanged(source_root, &source_sha256)?;
    Ok(json!({
        "schema": "acyclic.sdk.transport-fixtures.v1",
        "generator": "acyclic-sdk-examples@0.2.0",
        "source": {
            "revision": git_revision(source_root),
            "revision_kind": "git-revision",
            "model_source_digest": model_source_digest,
            "model_source_digest_kind": "rust-model-sha256",
            "path": "rust/crates/sdk-examples",
            "files": source_files,
            "sha256": source_sha256,
            "build_target": build_target,
            "build_recipe_sha256": build_recipe_sha256,
        },
        "golden_vectors": {
            "path": "rust-canonical-vectors.json",
            "schema": "acyclic.sdk.rust-canonical-vectors.v1",
            "sha256": hash(&vector_bytes),
            "count": vectors["fixtures"].as_array().map_or(0, Vec::len),
        },
        "qualification": {
            "scope": "loopback-local",
            "service_availability": "not_claimed",
            "rpc_scenarios": {
                "schema": "acyclic.sdk.rust-rpc-scenarios.v1",
                "count": rpc_scenarios.len(),
                "scenarios": rpc_scenarios,
            },
            "seed_graph": seed_graph,
            "typed_wire_evidence": typed_wire_evidence,
            "fixture_server": {
                "command": "cargo run --manifest-path rust/crates/sdk-examples/Cargo.toml --bin fixture-server -- --port 0",
                "bind": "127.0.0.1",
                "grpc_bind": "127.0.0.1",
                "grpc_address_env": "FIXTURE_GRPC_ADDRESS",
                "grpc_services": ["acyclic.actors.v1.ActorsService", "acyclic.stream.v2.StreamService"],
                "max_requests": 32,
                "source_sha256": source_sha256,
            },
        },
        "fixtures": entries,
    }))
}

fn typed_wire_evidence() -> Result<Vec<Value>, String> {
    let evidence = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("build typed fixture runtime: {error}"))?
        .block_on(filesystem_harness_scenarios::export())?;
    if evidence.len() != 35 {
        return Err(format!(
            "typed Filesystem/Harness exporter returned {}; expected 35",
            evidence.len()
        ));
    }
    Ok(evidence)
}

fn json_bytes(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).expect("JSON evidence is serializable");
    bytes.push(b'\n');
    bytes
}

fn canonical_vector(family: &str, message: &str, bytes: Vec<u8>) -> Value {
    json!({
        "family": family,
        "message": message,
        "bytes_hex": bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        "sha256": hash(&bytes),
    })
}

fn canonical_wire_vectors(
    source_revision: &str,
    model_source_digest: &str,
    source_sha256: &str,
    source_files: &[String],
    build_target: Option<&str>,
    build_recipe_sha256: &str,
) -> Value {
    let transport = transport_fixtures();
    let objects = acyclic_sdk_examples::objects_scenarios::fixture();
    let inference = acyclic_sdk_examples::inference_scenarios::fixture();
    let machines = acyclic_sdk_examples::machines_scenarios::fixture();
    let workers = acyclic_sdk_examples::workers_scenarios::fixture();
    let metadata = acyclic_objects::wire::ObjectMetadata {
        content_type: "text/plain".into(),
        user: BTreeMap::from([
            (String::from("alpha"), String::from("one")),
            (String::from("beta"), String::from("two")),
        ]),
        expires_unix_seconds: Some(-123),
        ..Default::default()
    };
    let fixtures = vec![
        canonical_vector(
            "actors",
            "CreateActorRequest",
            transport[0].requests[0].bytes.clone(),
        ),
        canonical_vector(
            "stream",
            "AppendRequest",
            transport[1].requests[0].bytes.clone(),
        ),
        canonical_vector(
            "stream",
            "ReadRequest",
            transport[1].requests[1].bytes.clone(),
        ),
        canonical_vector("workers", "PublishVersionRequest", workers.request),
        canonical_vector(
            "objects",
            "PutObjectHeader",
            objects.requests[0].bytes.clone(),
        ),
        canonical_vector(
            "objects",
            "GetObjectRequest",
            objects.requests[1].bytes.clone(),
        ),
        canonical_vector("objects", "ObjectMetadata", metadata.encode_to_vec()),
        canonical_vector(
            "objects",
            "ErrorDetail",
            acyclic_objects::wire::ErrorDetail {
                code: acyclic_objects::wire::ErrorCode::InvalidArgument as i32,
                request_id: "req-rust-1".into(),
            }
            .encode_to_vec(),
        ),
        canonical_vector(
            "actors",
            "Error",
            acyclic_actors::wire::Error {
                code: acyclic_actors::wire::ErrorCode::InvalidArgument as i32,
                message: "invalid fixture".into(),
            }
            .encode_to_vec(),
        ),
        canonical_vector(
            "workers",
            "Error",
            acyclic_workers::wire::Error {
                code: acyclic_workers::wire::ErrorCode::InvalidArgument as i32,
                message: "invalid fixture".into(),
            }
            .encode_to_vec(),
        ),
        canonical_vector("machines", "CreateMachineRequest", machines.request),
        canonical_vector("inference", "RunView", inference.view),
        canonical_vector("inference", "RunEvent[0]", inference.events[0].clone()),
        canonical_vector("inference", "RunEvent[1]", inference.events[1].clone()),
        canonical_vector(
            "filesystem",
            "HandshakeRequest",
            acyclic_fs::wire::filesystem::v2::HandshakeRequest::default().encode_to_vec(),
        ),
        canonical_vector(
            "harness",
            "CommandEnvelope",
            acyclic_harness::wire::CommandEnvelope::default().encode_to_vec(),
        ),
        canonical_vector(
            "protocol",
            "HandshakeRequest",
            acyclic_fs::wire::protocol::v1::HandshakeRequest::default().encode_to_vec(),
        ),
    ];
    json!({
        "schema": "acyclic.sdk.rust-canonical-vectors.v1",
        "source": {
            "path": "rust/crates/sdk-examples",
            "files": source_files,
            "sha256": source_sha256,
            "build_target": build_target,
            "build_recipe_sha256": build_recipe_sha256,
            "git_revision": source_revision,
            "git_revision_kind": "git-revision",
            "model_source_digest": model_source_digest,
            "model_source_digest_kind": "rust-model-sha256",
        },
        "fixtures": fixtures,
    })
}

fn validate_fixture(fixture: &TransportFixture, source_sha256: &str) -> Result<Value, String> {
    match fixture.id {
        "actors-create-unary-v1" => execute_actors_roundtrip()
            .map(|()| {
                json!({
                    "status": "qualified",
                    "scope": "rust-wire-validation",
                    "transport": "not-run",
                    "evidence": fixture.validation,
                    "source_sha256": source_sha256,
                })
            })
            .map_err(|error| format!("Actors fixture validation failed: {error}")),
        "stream-append-read-v2" => run_stream()
            .map(|()| {
                json!({
                    "status": "qualified",
                    "scope": "rust-memory-provider",
                    "transport": "in-process",
                    "evidence": fixture.validation,
                    "source_sha256": source_sha256,
                })
            })
            .map_err(|error| format!("Stream fixture validation failed: {error}")),
        other => Err(format!("unknown transport fixture {other}")),
    }
}

fn expected_bytes(fixture: &TransportFixture) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(&fixture.expected)
        .expect("fixture expected result is JSON serializable");
    bytes.push(b'\n');
    bytes
}

fn fixture_files() -> Vec<(String, Vec<u8>)> {
    transport_fixtures()
        .into_iter()
        .flat_map(|fixture| {
            let expected = expected_bytes(&fixture);
            let expected_path = format!("fixtures/{}/expected.json", fixture.id);
            let request_files = fixture.requests.into_iter().map(move |request| {
                (
                    format!("fixtures/{}/{}.bin", fixture.id, request.name),
                    request.bytes,
                )
            });
            request_files
                .chain(std::iter::once((expected_path, expected)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn write_fixture_bundle(output: &Path, manifest: &Value) -> Result<(), String> {
    let root = output.join("fixtures");
    if root.exists() {
        fs::remove_dir_all(&root).map_err(|error| format!("remove stale fixtures: {error}"))?;
    }
    for (relative, bytes) in fixture_files() {
        let path = output.join(&relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("create fixture directory: {error}"))?;
        }
        fs::write(path, bytes).map_err(|error| format!("write fixture {relative}: {error}"))?;
    }
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| format!("encode fixture manifest: {error}"))?;
    fs::write(
        output.join(FIXTURE_MANIFEST),
        [bytes.as_slice(), b"\n"].concat(),
    )
    .map_err(|error| format!("write fixture manifest: {error}"))?;
    let source = manifest
        .get("source")
        .ok_or("fixture manifest source is missing")?;
    let model_source_digest = source
        .get("model_source_digest")
        .and_then(Value::as_str)
        .ok_or("fixture manifest model_source_digest is missing")?;
    let source_sha256 = source
        .get("sha256")
        .and_then(Value::as_str)
        .ok_or("fixture manifest source sha256 is missing")?;
    let build_target = source.get("build_target").and_then(Value::as_str);
    let build_recipe_sha256 = source
        .get("build_recipe_sha256")
        .and_then(Value::as_str)
        .ok_or("fixture manifest source build recipe sha256 is missing")?;
    let source_revision = source
        .get("revision")
        .and_then(Value::as_str)
        .ok_or("fixture manifest source revision is missing")?;
    let source_files = source
        .get("files")
        .and_then(Value::as_array)
        .ok_or("fixture manifest source files are missing")?
        .iter()
        .map(|file| {
            file.as_str()
                .map(ToOwned::to_owned)
                .ok_or("fixture manifest source file is not a string")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let vectors = canonical_wire_vectors(
        source_revision,
        model_source_digest,
        source_sha256,
        &source_files,
        build_target,
        build_recipe_sha256,
    );
    fs::write(
        output.join("rust-canonical-vectors.json"),
        json_bytes(&vectors),
    )
    .map_err(|error| format!("write Rust canonical vectors: {error}"))
}

fn check_fixture_bundle(output: &Path, manifest: &Value) -> Result<(), String> {
    let path = output.join(FIXTURE_MANIFEST);
    let actual = serde_json::from_slice::<Value>(
        &fs::read(&path).map_err(|error| format!("read fixture manifest: {error}"))?,
    )
    .map_err(|error| format!("decode fixture manifest: {error}"))?;
    if actual != *manifest {
        return Err("fixture manifest differs; run sdk-examples fixtures generate".to_owned());
    }
    for (relative, expected) in fixture_files() {
        let bytes = fs::read(output.join(&relative))
            .map_err(|error| format!("read fixture {relative}: {error}"))?;
        if bytes != expected {
            return Err(format!("fixture drift detected: {relative}"));
        }
    }
    let source = manifest
        .get("source")
        .ok_or("fixture manifest source is missing")?;
    let model_source_digest = source
        .get("model_source_digest")
        .and_then(Value::as_str)
        .ok_or("fixture manifest model_source_digest is missing")?;
    let source_sha256 = source
        .get("sha256")
        .and_then(Value::as_str)
        .ok_or("fixture manifest source sha256 is missing")?;
    let build_target = source.get("build_target").and_then(Value::as_str);
    let build_recipe_sha256 = source
        .get("build_recipe_sha256")
        .and_then(Value::as_str)
        .ok_or("fixture manifest source build recipe sha256 is missing")?;
    let source_revision = source
        .get("revision")
        .and_then(Value::as_str)
        .ok_or("fixture manifest source revision is missing")?;
    let source_files = source
        .get("files")
        .and_then(Value::as_array)
        .ok_or("fixture manifest source files are missing")?
        .iter()
        .map(|file| {
            file.as_str()
                .map(ToOwned::to_owned)
                .ok_or("fixture manifest source file is not a string")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected_vectors = json_bytes(&canonical_wire_vectors(
        source_revision,
        model_source_digest,
        source_sha256,
        &source_files,
        build_target,
        build_recipe_sha256,
    ));
    let actual_vectors = fs::read(output.join("rust-canonical-vectors.json"))
        .map_err(|error| format!("read Rust canonical vectors: {error}"))?;
    if actual_vectors != expected_vectors {
        return Err("Rust canonical vectors drift detected".to_owned());
    }
    Ok(())
}

fn extension(language: Language) -> &'static str {
    match language {
        Language::Rust => "rs",
        Language::Python => "py",
        Language::TypeScript => "ts",
        Language::Go => "go",
        Language::Java => "java",
        Language::CSharp => "cs",
        Language::Ruby => "rb",
        Language::Dart => "dart",
        Language::Php => "php",
    }
}

fn validate_snippet(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
    staging_root: &Path,
) -> Result<Value, String> {
    match snippet.metadata.language {
        Language::Rust => run_rust(source_root, snippet, source_sha256, staging_root),
        Language::Python => run_python(source_root, snippet, source_sha256, staging_root),
        Language::TypeScript => run_typescript(source_root, snippet, source_sha256, staging_root),
        Language::Go => run_go(source_root, snippet, source_sha256, staging_root),
        Language::Java => run_java(source_root, snippet, source_sha256),
        Language::CSharp => run_dotnet(source_root, snippet, source_sha256),
        Language::Ruby => run_ruby(source_root, snippet, source_sha256, staging_root),
        Language::Dart => run_dart(source_root, snippet, source_sha256, staging_root),
        Language::Php => run_php(source_root, snippet, source_sha256, staging_root),
    }
}

fn run_stream() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let values = runtime.block_on(execute_stream_append_read())?;
    if values != vec![b"hello".to_vec(), b"world".to_vec()] {
        return Err("Stream scenario returned unexpected values".into());
    }
    Ok(())
}

fn run_rust(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
    staging_root: &Path,
) -> Result<Value, String> {
    let staging = validation_staging(staging_root, "rust", snippet.metadata.id);
    if staging.exists() {
        fs::remove_dir_all(&staging).map_err(|error| format!("remove Rust staging: {error}"))?;
    }
    fs::create_dir_all(staging.join("src"))
        .map_err(|error| format!("create Rust staging: {error}"))?;
    let actors = cargo_path(&source_root.join("rust/crates/actors"));
    let stream = cargo_path(&source_root.join("rust/crates/stream"));
    let filesystem = cargo_path(&source_root.join("rust/crates/filesystem"));
    let harness = cargo_path(&source_root.join("rust/crates/harness"));
    let inference = cargo_path(&source_root.join("rust/crates/inference"));
    let machines = cargo_path(&source_root.join("rust/crates/machines"));
    let objects = cargo_path(&source_root.join("rust/crates/objects"));
    let workers = cargo_path(&source_root.join("rust/crates/workers"));
    let manifest = format!(
        "[package]\nname = \"sdk-example-consumer\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n\n[dependencies]\nacyclic-actors = {{ path = \"{actors}\" }}\nacyclic-fs = {{ path = \"{filesystem}\" }}\nacyclic-harness = {{ path = \"{harness}\", features = [\"grpc\"] }}\nacyclic-inference = {{ path = \"{inference}\" }}\nacyclic-machines = {{ path = \"{machines}\" }}\nacyclic-objects = {{ path = \"{objects}\" }}\nacyclic-stream = {{ path = \"{stream}\", features = [\"grpc\"] }}\nacyclic-workers = {{ path = \"{workers}\" }}\nbytes = \"1.10.1\"\nfutures = \"0.3.31\"\nprost = \"0.14.4\"\ntokio = {{ version = \"1.48.0\", features = [\"macros\", \"rt-multi-thread\"] }}\n"
    );
    fs::write(staging.join("Cargo.toml"), manifest)
        .map_err(|error| format!("write Rust consumer manifest: {error}"))?;
    let bundle_snippet = snippet.code.clone();
    let main = format!(
        "#![allow(unused_imports)]\nuse std::error::Error;\n\n#[tokio::main]\nasync fn main() -> Result<(), Box<dyn Error>> {{\n{}\nOk(())\n}}\n",
        bundle_snippet
    );
    fs::write(staging.join("src/main.rs"), main)
        .map_err(|error| format!("write Rust consumer snippet: {error}"))?;
    let qualification = staging_root
        .parent()
        .ok_or("Rust validation staging has no output parent")?
        .join("qualification");
    let consumers = qualification.join("consumers");
    let packages = qualification.join("packages");
    fs::create_dir_all(&consumers).map_err(|error| format!("create Rust receipts: {error}"))?;
    fs::create_dir_all(&packages)
        .map_err(|error| format!("create Rust package receipts: {error}"))?;
    let package_dir_name = format!("{}-sdk-package", snippet.metadata.id);
    let package_root = packages.join(&package_dir_name);
    let package_manifest = package_root.join("Cargo.toml");

    let package_path = packages.join(format!("{}-sdk-package.tgz", snippet.metadata.id));
    let package_name = "acyclic-sdk-bundle";
    fs::create_dir_all(package_root.join("src"))
        .map_err(|error| format!("create SDK package root: {error}"))?;
    // The extracted archive is a standalone Cargo workspace.  Without an
    // explicit workspace root Cargo walks up into the checkout that happened
    // to produce the bundle, so an archive consumer can accidentally resolve
    // source paths outside the artifact (or inherit the producer's members,
    // lints, and workspace dependency table).  Preserve the Rust workspace
    // policy and dependency pins while relocating its members below `crates`.
    let workspace_manifest = fs::read_to_string(source_root.join("Cargo.toml"))
        .map_err(|error| format!("read Rust workspace manifest: {error}"))?;
    let workspace_tail = workspace_manifest
        .find("[workspace.package]")
        .map(|index| &workspace_manifest[index..])
        .ok_or("Rust workspace manifest is missing [workspace.package]")?;
    let package_manifest_contents = format!(
        "[package]\nname = \"{package_name}\"\nversion = \"0.0.0\"\nedition = \"2024\"\nlicense = \"Apache-2.0\"\npublish = false\n\n[lib]\npath = \"src/lib.rs\"\n\n[dependencies]\nacyclic-actors = {{ path = \"crates/actors\" }}\nacyclic-fs = {{ path = \"crates/filesystem\" }}\nacyclic-harness = {{ path = \"crates/harness\" }}\nacyclic-inference = {{ path = \"crates/inference\" }}\nacyclic-machines = {{ path = \"crates/machines\" }}\nacyclic-objects = {{ path = \"crates/objects\" }}\nacyclic-stream = {{ path = \"crates/stream\", features = [\"grpc\"] }}\nacyclic-workers = {{ path = \"crates/workers\" }}\n\n[workspace]\nmembers = [\"crates/*\"]\nresolver = \"2\"\n\n{workspace_tail}",
        workspace_tail = workspace_tail.trim_start(),
    );
    fs::write(&package_manifest, package_manifest_contents)
        .map_err(|error| format!("write SDK package manifest: {error}"))?;
    fs::write(
        package_root.join("src/lib.rs"),
        b"//! Bundled generated Rust SDK facade.\npub use acyclic_actors::{validate_create, wire};\npub use acyclic_fs;\npub use acyclic_harness;\npub use acyclic_inference;\npub use acyclic_machines;\npub use acyclic_objects;\npub use acyclic_stream::{AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath, StreamProvider};\npub use acyclic_workers;\n",
    )
    .map_err(|error| format!("write SDK package library: {error}"))?;
    let crates_root = source_root.join("rust/crates");
    let bundled_crates = package_root.join("crates");
    for entry in
        fs::read_dir(&crates_root).map_err(|error| format!("read SDK crate directory: {error}"))?
    {
        let entry = entry.map_err(|error| format!("read SDK crate entry: {error}"))?;
        let crate_source = entry.path();
        if !crate_source.join("Cargo.toml").is_file() || entry.file_name() == "sdk-examples" {
            continue;
        }
        copy_dir_recursive(&crate_source, &bundled_crates.join(entry.file_name()))?;
    }
    let package_tree_digest = tree_digest(&package_root)?;
    let archive = Command::new("tar")
        .args(["-czf"])
        .arg(&package_path)
        .args(["--format", "ustar", "--mtime", "1970-01-01"])
        .args(["-C"])
        .arg(&packages)
        .arg(&package_dir_name)
        .output()
        .map_err(|error| format!("start SDK package archive: {error}"))?;
    if !archive.status.success() {
        return Err(format!(
            "SDK package archive failed: {}",
            String::from_utf8_lossy(&archive.stderr).trim()
        ));
    }
    normalize_gzip_header(&package_path)?;
    let repeat_package_path = packages.join(format!("{}-repeat.tgz", snippet.metadata.id));
    let repeat_archive = Command::new("tar")
        .args(["-czf"])
        .arg(&repeat_package_path)
        .args(["--format", "ustar", "--mtime", "1970-01-01"])
        .args(["-C"])
        .arg(&packages)
        .arg(&package_dir_name)
        .output()
        .map_err(|error| format!("start repeated SDK package archive: {error}"))?;
    if !repeat_archive.status.success() {
        return Err(format!(
            "repeated SDK package archive failed: {}",
            String::from_utf8_lossy(&repeat_archive.stderr).trim()
        ));
    }
    normalize_gzip_header(&repeat_package_path)?;
    let deterministic_archive_bytes = fs::read(&package_path)
        .map_err(|error| format!("read deterministic SDK package archive: {error}"))?;
    let repeat_package_bytes = fs::read(&repeat_package_path)
        .map_err(|error| format!("read repeated SDK package archive: {error}"))?;
    if deterministic_archive_bytes != repeat_package_bytes {
        return Err("SDK package archive is not deterministic across repeated writes".to_owned());
    }
    fs::remove_file(&repeat_package_path)
        .map_err(|error| format!("remove repeated SDK package archive: {error}"))?;
    fs::remove_dir_all(&package_root)
        .map_err(|error| format!("remove package staging tree: {error}"))?;
    let extract = Command::new("tar")
        .args(["-xzf"])
        .arg(&package_path)
        .args(["-C"])
        .arg(&packages)
        .output()
        .map_err(|error| format!("start SDK package extraction: {error}"))?;
    if !extract.status.success() {
        return Err(format!(
            "SDK package extraction failed: {}",
            String::from_utf8_lossy(&extract.stderr).trim()
        ));
    }
    let package_manifest = package_root.join("Cargo.toml");
    if snippet.metadata.id == "actors-create-roundtrip" {
        run_guide_rust_consumers(
            source_root,
            source_sha256,
            &package_root,
            &package_path,
            &qualification,
        )?;
    }

    let consumer_manifest = consumers.join(format!("{}-Cargo.toml", snippet.metadata.id));
    let consumer_lock = consumers.join(format!("{}-Cargo.lock", snippet.metadata.id));
    let consumer_metadata = consumers.join(format!("{}-cargo-metadata.json", snippet.metadata.id));
    let portable_consumer_manifest_bytes = format!(
        "[package]\nname = \"rendered-{}\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n\n[dependencies]\nacyclic-sdk-bundle = {{ path = \"../../qualification/packages/{package_dir_name}\" }}\nbytes = \"1.10.1\"\nfutures = \"0.3.31\"\nprost = \"0.14.4\"\ntokio = {{ version = \"1.48.0\", features = [\"macros\", \"rt-multi-thread\"] }}\n",
        snippet.metadata.id
    );
    let consumed_package_root = staging
        .parent()
        .unwrap_or(staging.as_path())
        .join(format!(".sdk-examples-{}-consumed-sdk-package", snippet.metadata.id));
    if consumed_package_root.exists() {
        fs::remove_dir_all(&consumed_package_root)
            .map_err(|error| format!("remove stale consumed SDK package: {error}"))?;
    }
    copy_dir_recursive(&package_root, &consumed_package_root)?;
    let consumed_package_root_string = consumed_package_root.to_string_lossy().replace('\\', "/");
    let compile_consumer_manifest_bytes = format!(
        "[package]\nname = \"rendered-{}\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n\n[dependencies]\nacyclic-sdk-bundle = {{ path = \"{consumed_package_root_string}\" }}\nbytes = \"1.10.1\"\nfutures = \"0.3.31\"\nprost = \"0.14.4\"\ntokio = {{ version = \"1.48.0\", features = [\"macros\", \"rt-multi-thread\"] }}\n",
        snippet.metadata.id
    );
    fs::write(staging.join("Cargo.toml"), &compile_consumer_manifest_bytes)
        .map_err(|error| format!("write archive consumer manifest: {error}"))?;
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let deterministic_rustflags = deterministic_rustflags();
    let lock = Command::new(&cargo)
        .args(["generate-lockfile", "--offline"])
        .env("CARGO_NET_OFFLINE", "true")
        .envs(env::var_os("CARGO_TARGET_DIR").map(|value| ("CARGO_TARGET_DIR", value)))
        .current_dir(&staging)
        .output()
        .map_err(|error| format!("start Rust consumer lockfile generation: {error}"))?;
    if !lock.status.success() {
        let mut receipt = command_receipt("cargo generate-lockfile --offline", lock, source_sha256);
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        let _ = fs::remove_dir_all(&staging);
        return Ok(receipt);
    }
    let metadata = Command::new(&cargo)
        .args([
            "metadata",
            "--manifest-path",
            "Cargo.toml",
            "--locked",
            "--offline",
            "--format-version",
            "1",
        ])
        .env("CARGO_NET_OFFLINE", "true")
        .envs(env::var_os("CARGO_TARGET_DIR").map(|value| ("CARGO_TARGET_DIR", value)))
        .current_dir(&staging)
        .output()
        .map_err(|error| format!("start Rust consumer metadata resolution: {error}"))?;
    if !metadata.status.success() {
        let mut receipt = command_receipt(
            "cargo metadata --manifest-path Cargo.toml --locked --offline --format-version 1",
            metadata,
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        let _ = fs::remove_dir_all(&staging);
        return Ok(receipt);
    }
    let metadata_json: Value = serde_json::from_slice(&metadata.stdout)
        .map_err(|error| format!("decode Rust consumer metadata: {error}"))?;
    let resolved_package = metadata_json
        .get("packages")
        .and_then(Value::as_array)
        .and_then(|packages| {
            packages.iter().find(|package| {
                package.get("name").and_then(Value::as_str) == Some(package_name)
                    && package.get("version").and_then(Value::as_str) == Some("0.0.0")
            })
        })
        .ok_or("Rust consumer metadata did not resolve the extracted SDK package")?;
    let resolved_manifest = resolved_package
        .get("manifest_path")
        .and_then(Value::as_str)
        .ok_or("Rust consumer metadata package has no manifest path")?;
    let expected_manifest = consumed_package_root
        .join("Cargo.toml")
        .canonicalize()
        .map_err(|error| format!("canonicalize extracted SDK manifest: {error}"))?;
    let resolved_manifest_path = PathBuf::from(resolved_manifest)
        .canonicalize()
        .map_err(|error| format!("canonicalize resolved SDK manifest: {error}"))?;
    if resolved_manifest_path != expected_manifest {
        return Err(format!(
            "Rust consumer resolved a different SDK manifest: expected {}, observed {}",
            expected_manifest.display(),
            resolved_manifest_path.display()
        ));
    }
    fs::write(&consumer_metadata, &metadata.stdout)
        .map_err(|error| format!("write Rust consumer metadata: {error}"))?;
    let test = Command::new(&cargo)
        .args([
            "test",
            "--manifest-path",
            "Cargo.toml",
            "--locked",
            "--offline",
            "--quiet",
        ])
        .env("CARGO_NET_OFFLINE", "true")
        .env("RUSTFLAGS", &deterministic_rustflags)
        .envs(env::var_os("CARGO_TARGET_DIR").map(|value| ("CARGO_TARGET_DIR", value)))
        .current_dir(&staging)
        .output();
    let test = match test {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut receipt = pending_receipt(
                "cargo run --offline",
                &format!(
                    "runtime executable is unavailable: {}",
                    PathBuf::from(&cargo).display()
                ),
                source_sha256,
            );
            receipt
                .as_object_mut()
                .ok_or("Rust receipt is not an object")?
                .insert(
                    "snippet_code_sha256".to_owned(),
                    json!(hash(snippet.code.as_bytes())),
                );
            let _ = fs::remove_dir_all(&staging);
            return Ok(receipt);
        }
        Err(error) => return Err(format!("start Rust consumer tests: {error}")),
    };
    if !test.status.success() {
        let mut receipt = command_receipt(
            "cargo test --manifest-path Cargo.toml --locked --offline --quiet",
            test,
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        let _ = fs::remove_dir_all(&staging);
        return Ok(receipt);
    }

    let install_root = staging.join("install-root");
    let install = Command::new(&cargo)
        .args([
            "install",
            "--offline",
            "--path",
            ".",
            "--root",
            "install-root",
            "--force",
        ])
        .env("CARGO_NET_OFFLINE", "true")
        .env("RUSTFLAGS", &deterministic_rustflags)
        .envs(env::var_os("CARGO_TARGET_DIR").map(|value| ("CARGO_TARGET_DIR", value)))
        .current_dir(&staging)
        .output()
        .map_err(|error| format!("start Rust consumer install: {error}"))?;
    if !install.status.success() {
        let mut receipt = command_receipt(
            "cargo test --manifest-path Cargo.toml --locked --offline --quiet; cargo install --offline --path .",
            install,
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        let _ = fs::remove_dir_all(&staging);
        return Ok(receipt);
    }

    let executable_name = if cfg!(windows) {
        format!("rendered-{}.exe", snippet.metadata.id)
    } else {
        format!("rendered-{}", snippet.metadata.id)
    };
    let installed = install_root.join("bin").join(executable_name);
    if !installed.is_file() {
        return Err(format!(
            "Rust install did not produce expected executable: {}",
            installed.display()
        ));
    }
    let output = Command::new(&installed)
        .env(
            "FIXTURE_GRPC_ADDRESS",
            env::var("FIXTURE_GRPC_ADDRESS").unwrap_or_default(),
        )
        .current_dir(&staging)
        .output()
        .map_err(|error| format!("run installed Rust consumer: {error}"))?;

    let stdout_path = consumers.join(format!("{}-stdout.log", snippet.metadata.id));
    let stderr_path = consumers.join(format!("{}-stderr.log", snippet.metadata.id));
    fs::write(&stdout_path, &output.stdout)
        .map_err(|error| format!("write Rust consumer stdout: {error}"))?;
    fs::write(&stderr_path, &output.stderr)
        .map_err(|error| format!("write Rust consumer stderr: {error}"))?;

    let compile_path = consumers.join(format!("{}-compile.bin", snippet.metadata.id));
    let runtime_path = consumers.join(format!("{}-runtime.bin", snippet.metadata.id));
    // Windows PE linkers stamp the executable header with the invocation time.
    // Keep the exact installed executable for the invocation, but publish
    // normalized compile/runtime evidence so regeneration compares the
    // authored/package bytes rather than that volatile header field.
    let installed_bytes =
        fs::read(&installed).map_err(|error| format!("read installed Rust consumer: {error}"))?;
    let (normalized_bytes, artifact_normalization) =
        normalize_invocation_artifact(&installed_bytes);
    let invocation_path = consumers.join(format!("{}-invocation.bin", snippet.metadata.id));
    fs::write(&invocation_path, &installed_bytes)
        .map_err(|error| format!("write Rust invocation artifact: {error}"))?;
    fs::write(&compile_path, &normalized_bytes)
        .map_err(|error| format!("write Rust compile artifact: {error}"))?;
    fs::write(&runtime_path, &normalized_bytes)
        .map_err(|error| format!("write Rust runtime artifact: {error}"))?;
    fs::write(&consumer_manifest, &portable_consumer_manifest_bytes)
        .map_err(|error| format!("write Rust consumer manifest: {error}"))?;
    fs::copy(staging.join("Cargo.lock"), &consumer_lock)
        .map_err(|error| format!("copy Rust consumer lock: {error}"))?;
    let compile_digest = hash(&normalized_bytes);
    let runtime_digest = hash(&normalized_bytes);
    let invocation_digest = hash(&installed_bytes);
    let package_bytes = fs::read(&package_path).map_err(|error| error.to_string())?;
    let package_digest = hash(&package_bytes);
    let package_size = package_bytes.len();
    let metadata_digest = hash(&metadata.stdout);
    let snippet_digest = hash(snippet.code.as_bytes());
    let source_revision = git_revision(source_root);
    let source_closure_sha256 = scenario_source_sha256(source_root)?;
    let status = if output.status.success() {
        "qualified"
    } else {
        "failed"
    };
    let mut receipt = json!({
        "status": status,
        "command": "cargo test --manifest-path Cargo.toml --locked --offline --quiet; cargo install --offline --path .; installed consumer",
        "executed": true,
        "exit_code": output.status.code(),
        "source_revision": source_revision,
        "source_path": snippet.metadata.source,
        "source_sha256": source_sha256,
        "snippet_path": format!("snippets/{}/rust.rs", snippet.metadata.id),
        "snippet_sha256": snippet_digest,
        "snippet_code_sha256": snippet_digest,
        "compile_artifact_path": portable_output_path(&compile_path, &qualification),
        "compile_artifact_sha256": compile_digest,
        "runtime_artifact_path": portable_output_path(&runtime_path, &qualification),
        "runtime_artifact_sha256": runtime_digest,
        "invocation_artifact": {
            "path": portable_output_path(&invocation_path, &qualification),
            "bytes": installed_bytes.len(),
            "sha256": invocation_digest,
            "normalized_sha256": hash(&normalized_bytes),
            "normalization": artifact_normalization,
        },
        "package_artifact_path": portable_output_path(&package_path, &qualification),
        "package_artifact_sha256": package_digest,
        "package_artifact_size": package_size,
        "package_archive_format": "gzip+ustar",
        "package_archive_deterministic": true,
        "source_closure_sha256": source_closure_sha256,
        "package_resolution": {
            "resolved": true,
            "status": "qualified",
            "command": "cargo test --manifest-path Cargo.toml --locked --offline --quiet",
            "artifact_consumed": true,
            "artifact_consumption_command": "cargo test --manifest-path Cargo.toml --locked --offline --quiet; cargo install --offline --path .; installed consumer",
            "package_manager": "cargo",
            "source_revision": source_revision,
            "source_closure_sha256": source_closure_sha256,
            "compiled_snippet_sha256": snippet_digest,
            "compile_artifact_path": portable_output_path(&compile_path, &qualification),
            "compile_artifact_sha256": compile_digest,
            "invocation_artifact": {
                "path": portable_output_path(&invocation_path, &qualification),
                "bytes": installed_bytes.len(),
                "sha256": invocation_digest,
                "normalized_sha256": hash(&normalized_bytes),
                "normalization": artifact_normalization,
            },
            "package_artifact_path": portable_output_path(&package_path, &qualification),
            "package_artifact_sha256": package_digest,
            "package_artifact_size": package_size,
            "package_archive_format": "gzip+ustar",
            "package_archive_deterministic": true,
            "package_name": package_name,
            "package_version": "0.0.0",
            "package_root_path": portable_output_path(&package_root, &qualification),
            "package_manifest_path": portable_output_path(&package_manifest, &qualification),
            "package_manifest_sha256": hash(&fs::read(&package_manifest).map_err(|error| error.to_string())?),
            "package_tree_sha256": package_tree_digest,
            "consumer_manifest_path": portable_output_path(&consumer_manifest, &qualification),
            "consumer_manifest_sha256": hash(&fs::read(&consumer_manifest).map_err(|error| error.to_string())?),
            "consumer_lock_path": portable_output_path(&consumer_lock, &qualification),
            "consumer_lock_sha256": hash(&fs::read(&consumer_lock).map_err(|error| error.to_string())?),
            "consumer_metadata_path": portable_output_path(&consumer_metadata, &qualification),
            "consumer_metadata_sha256": metadata_digest,
            "consumer_metadata_command": "cargo metadata --manifest-path Cargo.toml --locked --offline --format-version 1",
            "resolved_package_name": package_name,
            "resolved_package_version": "0.0.0",
            "resolved_package_manifest_path": portable_output_path(&package_manifest, &qualification),
            "consumer_exit_code": output.status.code(),
            "consumer_stdout_path": portable_output_path(&stdout_path, &qualification),
            "consumer_stdout_sha256": hash(&output.stdout),
            "consumer_stderr_path": portable_output_path(&stderr_path, &qualification),
            "consumer_stderr_sha256": hash(&output.stderr),
        },
        "stdout_sha256": hash(&output.stdout),
        "stderr_sha256": hash(&output.stderr),
        "consumer_exit_code": output.status.code(),
        "consumer_stdout_path": portable_output_path(&stdout_path, &qualification),
        "consumer_stdout_sha256": hash(&output.stdout),
        "consumer_stderr_path": portable_output_path(&stderr_path, &qualification),
        "consumer_stderr_sha256": hash(&output.stderr),
    });
    if snippet.metadata.id == "actors-create-roundtrip" {
        let guide_receipts = guide_rust_cases()
            .into_iter()
            .map(|(scenario_id, _, _)| {
                let receipt_path = qualification
                    .join("guide-consumers")
                    .join(format!("{scenario_id}.receipt.json"));
                let receipt = fs::read(&receipt_path)
                    .map_err(|error| format!("read guide receipt {scenario_id}: {error}"))?;
                serde_json::from_slice::<Value>(&receipt)
                    .map_err(|error| format!("decode guide receipt {scenario_id}: {error}"))
            })
            .collect::<Result<Vec<_>, String>>()?;
        receipt["guide_consumers"] = json!(guide_receipts);
    }
    if !output.status.success() {
        receipt["message"] = json!(String::from_utf8_lossy(&output.stderr).trim());
    }
    let _ = fs::remove_dir_all(&staging);
    Ok(receipt)
}

fn guide_rust_cases() -> Vec<(&'static str, &'static str, String)> {
    let filesystem = format!(
        "let root = std::env::temp_dir().join(format!(\"acyclic-sdk-guide-fs-{{}}\", std::process::id()));
{}",
        filesystem_scenarios::QUICKSTART_SNIPPET
    );
    let harness = r#"use acyclic_harness::{Admission, Outcome, TaskGroup};

let group = TaskGroup::new(1);
let first = group.try_spawn(async { 7_u8 }).await;
if let Admission::Accepted(handle) = first {
    assert!(matches!(handle.result().await, Outcome::Succeeded(7)));
} else {
    panic!("initial task was not admitted");
}
group.cancel();
assert!(matches!(
    group.try_spawn(async { 9_u8 }).await,
    Admission::Rejected { .. }
));
let recovered = TaskGroup::new(1).try_spawn(async { 11_u8 }).await;
assert!(matches!(recovered, Admission::Accepted(_)));
"#.to_owned();
    vec![
        (
            "actors-create-roundtrip",
            "rust/crates/sdk-examples/src/lib.rs",
            scenarios()[0].render(Language::Rust).code,
        ),
        (
            "stream-append-read",
            "rust/crates/sdk-examples/src/lib.rs",
            scenarios()[1].render(Language::Rust).code,
        ),
        (
            filesystem_scenarios::SCENARIO_ID,
            filesystem_scenarios::SOURCE,
            filesystem,
        ),
        (
            harness_scenarios::SCENARIO_ID,
            harness_scenarios::SOURCE,
            harness,
        ),
        (
            inference_scenarios::SCENARIO_ID,
            inference_scenarios::SOURCE,
            inference_scenarios::rust_snippet().to_owned(),
        ),
        (
            machines_scenarios::SCENARIO_ID,
            machines_scenarios::SOURCE,
            machines_scenarios::rust_snippet().to_owned(),
        ),
        (
            objects_scenarios::SCENARIO_ID,
            objects_scenarios::SOURCE,
            objects_scenarios::rust_snippet().to_owned(),
        ),
        (
            workers_scenarios::SCENARIO_ID,
            workers_scenarios::SOURCE,
            workers_scenarios::rust_snippet(),
        ),
    ]
}

fn rewrite_guide_imports(mut code: String) -> String {
    for crate_name in [
        "acyclic_fs",
        "acyclic_harness",
        "acyclic_inference",
        "acyclic_machines",
        "acyclic_objects",
        "acyclic_workers",
    ] {
        code = code.replace(
            &format!("use {crate_name}::"),
            &format!("use acyclic_sdk_bundle::{crate_name}::"),
        );
    }
    code
}

fn run_guide_rust_consumers(
    source_root: &Path,
    source_sha256: &str,
    package_root: &Path,
    package_path: &Path,
    qualification: &Path,
) -> Result<(), String> {
    let package_bytes =
        fs::read(package_path).map_err(|error| format!("read guide package archive: {error}"))?;
    let package_sha256 = hash(&package_bytes);
    let package_root = package_root
        .canonicalize()
        .map_err(|error| format!("canonicalize guide package root: {error}"))?;
    let guide_root = qualification.join("guide-consumers");
    fs::create_dir_all(&guide_root)
        .map_err(|error| format!("create guide consumer root: {error}"))?;
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let rustflags = deterministic_rustflags();

    for (scenario_id, source, source_code) in guide_rust_cases() {
        let staging = guide_root.join(scenario_id);
        if staging.exists() {
            fs::remove_dir_all(&staging)
                .map_err(|error| format!("remove guide staging: {error}"))?;
        }
        fs::create_dir_all(staging.join("src"))
            .map_err(|error| format!("create guide staging: {error}"))?;
        let package_path_text = package_root.to_string_lossy().replace('\\', "/");
        let manifest = format!(
            r#"[package]
name = "guide-{scenario_id}"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

[dependencies]
acyclic-sdk-bundle = {{ path = "{package_path_text}" }}
bytes = "1.10.1"
futures = "0.3.31"
prost = "0.14.4"
serde_json = "1.0.145"
sha2 = "0.10.9"
tokio = {{ version = "1.48.0", features = ["macros", "rt-multi-thread"] }}
"#,
            scenario_id = scenario_id,
            package_path_text = package_path_text,
        );
        fs::write(staging.join("Cargo.toml"), manifest)
            .map_err(|error| format!("write guide consumer manifest: {error}"))?;
        let snippet_sha256 = hash(source_code.as_bytes());
        let code = rewrite_guide_imports(source_code);
        let main = format!(
            r#"#![allow(unused_imports)]
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {{
{code}
Ok(())
}}
"#,
            code = code,
        );
        fs::write(staging.join("src/main.rs"), main)
            .map_err(|error| format!("write guide consumer source: {error}"))?;

        let lock = Command::new(&cargo)
            .args(["generate-lockfile", "--offline"])
            .env("CARGO_NET_OFFLINE", "true")
            .envs(env::var_os("CARGO_TARGET_DIR").map(|value| ("CARGO_TARGET_DIR", value)))
            .current_dir(&staging)
            .output()
            .map_err(|error| format!("start guide lockfile generation: {error}"))?;
        if !lock.status.success() {
            return Err(format!(
                "guide {scenario_id} lockfile failed: {}",
                String::from_utf8_lossy(&lock.stderr).trim()
            ));
        }
        let test = Command::new(&cargo)
            .args([
                "test",
                "--manifest-path",
                "Cargo.toml",
                "--locked",
                "--offline",
                "--quiet",
            ])
            .env("CARGO_NET_OFFLINE", "true")
            .env("RUSTFLAGS", &rustflags)
            .envs(env::var_os("CARGO_TARGET_DIR").map(|value| ("CARGO_TARGET_DIR", value)))
            .current_dir(&staging)
            .output()
            .map_err(|error| format!("start guide consumer test: {error}"))?;
        if !test.status.success() {
            let stderr = String::from_utf8_lossy(&test.stderr);
            let _ = fs::write(guide_root.join(format!("{scenario_id}.stderr.log")), &test.stderr);
            return Err(format!("guide {scenario_id} test failed: {}", stderr.trim()));
        }
        let install = Command::new(&cargo)
            .args([
                "install",
                "--offline",
                "--path",
                ".",
                "--root",
                "install-root",
                "--force",
            ])
            .env("CARGO_NET_OFFLINE", "true")
            .env("RUSTFLAGS", &rustflags)
            .envs(env::var_os("CARGO_TARGET_DIR").map(|value| ("CARGO_TARGET_DIR", value)))
            .current_dir(&staging)
            .output()
            .map_err(|error| format!("start guide consumer install: {error}"))?;
        if !install.status.success() {
            return Err(format!(
                "guide {scenario_id} install failed: {}",
                String::from_utf8_lossy(&install.stderr).trim()
            ));
        }
        let executable_name = if cfg!(windows) {
            format!("guide-{scenario_id}.exe")
        } else {
            format!("guide-{scenario_id}")
        };
        let executable = staging.join("install-root").join("bin").join(executable_name);
        if !executable.is_file() {
            return Err(format!("guide {scenario_id} install produced no executable"));
        }
        let output = Command::new(&executable)
            .current_dir(&staging)
            .output()
            .map_err(|error| format!("run guide {scenario_id} consumer: {error}"))?;
        let stdout_path = guide_root.join(format!("{scenario_id}.stdout.log"));
        let stderr_path = guide_root.join(format!("{scenario_id}.stderr.log"));
        fs::write(&stdout_path, &output.stdout)
            .map_err(|error| format!("write guide stdout: {error}"))?;
        fs::write(&stderr_path, &output.stderr)
            .map_err(|error| format!("write guide stderr: {error}"))?;
        let receipt = json!({
            "schema": "acyclic.sdk.guide-consumer-receipt.v1",
            "scenario_id": scenario_id,
            "source_path": source,
            "source_revision": git_revision(source_root),
            "source_sha256": source_sha256,
            "snippet_sha256": snippet_sha256,
            "package_artifact_path": portable_output_path(package_path, qualification),
            "package_artifact_sha256": package_sha256,
            "package_root_path": portable_output_path(&package_root, qualification),
            "command": "cargo test --manifest-path Cargo.toml --locked --offline --quiet; cargo install --offline --path .; installed consumer",
            "executed": true,
            "status": if output.status.success() { "qualified" } else { "failed" },
            "exit_code": output.status.code(),
            "stdout_path": portable_output_path(&stdout_path, qualification),
            "stdout_sha256": hash(&output.stdout),
            "stderr_path": portable_output_path(&stderr_path, qualification),
            "stderr_sha256": hash(&output.stderr),
        });
        let receipt_path = guide_root.join(format!("{scenario_id}.receipt.json"));
        let receipt_bytes = serde_json::to_vec_pretty(&receipt)
            .map_err(|error| format!("encode guide receipt: {error}"))?;
        fs::write(receipt_path, receipt_bytes)
            .map_err(|error| format!("write guide receipt: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "guide {scenario_id} consumer failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        fs::remove_dir_all(&staging)
            .map_err(|error| format!("remove guide staging: {error}"))?;
    }
    Ok(())
}
fn add_snippet_binding(
    receipt: &mut Value,
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
) {
    if let Some(object) = receipt.as_object_mut() {
        object.insert("executed".to_owned(), json!(false));
        object.insert(
            "source_revision".to_owned(),
            json!(git_revision(source_root)),
        );
        object.insert("source_path".to_owned(), json!(snippet.metadata.source));
        object.insert("source_sha256".to_owned(), json!(source_sha256));
        object.insert(
            "snippet_path".to_owned(),
            json!(format!(
                "snippets/{}/{}.{}",
                snippet.metadata.id,
                snippet.metadata.language.as_str(),
                extension(snippet.metadata.language)
            )),
        );
        object.insert(
            "snippet_sha256".to_owned(),
            json!(hash(snippet.code.as_bytes())),
        );
    }
}

fn cargo_path(path: &Path) -> String {
    let text = path.to_string_lossy().replace('\\', "/");
    if let Some(rest) = text.strip_prefix("//?/UNC/") {
        format!("//{rest}")
    } else {
        text.strip_prefix("//?/")
            .or_else(|| text.strip_prefix("//./"))
            .unwrap_or(&text)
            .to_owned()
    }
}
fn portable_output_path(path: &Path, qualification: &Path) -> String {
    let relative = path
        .strip_prefix(qualification.parent().unwrap_or(qualification))
        .unwrap_or(path);
    relative.to_string_lossy().replace('\\', "/")
}

fn run_python(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
    staging_root: &Path,
) -> Result<Value, String> {
    let Some(wheel) = env::var_os("PYTHON_WHEEL").map(PathBuf::from) else {
        let mut receipt = pending_receipt(
            "python wheel install",
            "PYTHON_WHEEL must identify the source-bound generated wheel",
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        return Ok(receipt);
    };
    if !wheel.is_file() {
        let mut receipt = pending_receipt(
            "python wheel install",
            &format!("source-bound Python wheel is absent: {}", wheel.display()),
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        return Ok(receipt);
    }
    let staging = validation_staging(staging_root, "python", snippet.metadata.id);
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .map_err(|error| format!("remove stale Python staging: {error}"))?;
    }
    fs::create_dir_all(&staging).map_err(|error| format!("create Python staging: {error}"))?;
    let python = env::var_os("PYTHON").unwrap_or_else(|| "python".into());
    let install = Command::new(&python)
        .args([
            "-c",
            "import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])",
        ])
        .arg(&wheel)
        .arg(&staging)
        .current_dir(source_root)
        .output();
    let install = match install {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut receipt = pending_receipt(
                "python wheel install",
                &format!(
                    "runtime executable is unavailable: {}",
                    PathBuf::from(&python).display()
                ),
                source_sha256,
            );
            attach_artifact(&mut receipt, "python-wheel", &wheel)?;
            add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
            let _ = fs::remove_dir_all(&staging);
            return Ok(receipt);
        }
        Err(error) => return Err(format!("start Python wheel installation: {error}")),
    };
    if !install.status.success() {
        let mut receipt = command_receipt("python wheel install", install, source_sha256);
        attach_artifact(&mut receipt, "python-wheel", &wheel)?;
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        let _ = fs::remove_dir_all(&staging);
        return Ok(receipt);
    }
    let output = Command::new(&python)
        .arg("-c")
        .arg(&snippet.code)
        .env("PYTHONPATH", &staging)
        .current_dir(source_root)
        .output()
        .map_err(|error| format!("start Python validation: {error}"))?;
    let mut receipt = command_receipt("python wheel install + python", output, source_sha256);
    attach_artifact(&mut receipt, "python-wheel", &wheel)?;
    add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
    receipt["executed"] = json!(true);
    let _ = fs::remove_dir_all(&staging);
    Ok(receipt)
}

fn run_typescript(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
    staging_root: &Path,
) -> Result<Value, String> {
    let package = match snippet.metadata.family {
        "actors" => source_root.join("typescript/packages/actors"),
        "stream" => source_root.join("typescript/packages/stream"),
        family => return Err(format!("unknown TypeScript family {family}")),
    };
    let artifact = package.join("dist");
    if !artifact.is_dir() {
        return Ok(json!({
            "status": "pending",
            "command": "bun",
            "message": format!("generated package artifact is absent: {}", artifact.display()),
            "source_sha256": source_sha256,
        }));
    }
    // The checked-out worktree can be read by the validator but is not always
    // writable on Windows. Stage the installed package and its runtime
    // dependencies into a disposable consumer workspace so this still runs
    // the same package entrypoints a user quickstart imports.
    let package_name = package
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("TypeScript package has no directory name")?;
    let staging = validation_staging(staging_root, "typescript", snippet.metadata.id);
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .map_err(|error| format!("remove stale TypeScript staging: {error}"))?;
    }
    let staged_package = staging
        .join("node_modules")
        .join("@acyclic-labs")
        .join(package_name);
    fs::create_dir_all(&staged_package)
        .map_err(|error| format!("create TypeScript staging: {error}"))?;
    for directory in ["dist", "generated"] {
        let source = package.join(directory);
        if source.is_dir() {
            copy_tree(&source, &staged_package.join(directory))?;
        }
    }
    fs::copy(
        package.join("package.json"),
        staged_package.join("package.json"),
    )
    .map_err(|error| format!("stage TypeScript package metadata: {error}"))?;
    let dependencies = package.join("node_modules");
    if dependencies.is_dir() {
        for entry in fs::read_dir(&dependencies)
            .map_err(|error| format!("read TypeScript dependencies: {error}"))?
        {
            let entry = entry.map_err(|error| format!("read TypeScript dependency: {error}"))?;
            let name = entry.file_name();
            if name == ".bin" {
                continue;
            }
            copy_tree(
                &entry.path(),
                &staging
                    .join("node_modules")
                    .join(name.to_string_lossy().as_ref()),
            )?;
        }
    }
    let path = staging.join("snippet.ts");
    fs::write(&path, &snippet.code)
        .map_err(|error| format!("write TypeScript validation: {error}"))?;
    let output = Command::new(env::var_os("BUN").unwrap_or_else(|| "bun".into()))
        .args(["run", "snippet.ts"])
        .current_dir(&staging)
        .output();
    let output = match output {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let receipt = pending_receipt("bun", "Bun executable is unavailable", source_sha256);
            let _ = fs::remove_dir_all(&staging);
            return Ok(receipt);
        }
        Err(error) => return Err(format!("start Bun validation: {error}")),
    };
    let receipt = command_receipt("bun", output, source_sha256);
    let _ = fs::remove_dir_all(&staging);
    Ok(receipt)
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), String> {
    if source.is_dir() {
        fs::create_dir_all(destination).map_err(|error| {
            format!("create staged directory {}: {error}", destination.display())
        })?;
        for entry in fs::read_dir(source)
            .map_err(|error| format!("read staged directory {}: {error}", source.display()))?
        {
            let entry = entry.map_err(|error| format!("read staged entry: {error}"))?;
            copy_tree(&entry.path(), &destination.join(entry.file_name()))?;
        }
    } else {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("create staged parent {}: {error}", parent.display()))?;
        }
        fs::copy(source, destination)
            .map_err(|error| format!("copy staged file {}: {error}", source.display()))?;
    }
    Ok(())
}

fn validation_staging(root: &Path, language: &str, scenario: &str) -> PathBuf {
    let stage_root = env::var_os("SDK_EXAMPLES_STAGING_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.to_owned());
    stage_root.join(format!(".sdk-examples-{language}-{scenario}"))
}

fn receipt(result: Result<&'static str, Box<dyn std::error::Error>>, source_sha256: &str) -> Value {
    match result {
        Ok(evidence) => {
            json!({ "status": "qualified", "command": "cargo test", "evidence": evidence, "source_sha256": source_sha256 })
        }
        Err(error) => {
            json!({ "status": "failed", "command": "cargo test", "message": error.to_string(), "source_sha256": source_sha256 })
        }
    }
}

fn command_receipt(command: &str, output: std::process::Output, source_sha256: &str) -> Value {
    let status = if output.status.success() {
        "qualified"
    } else {
        "failed"
    };
    json!({
        "status": status,
        "command": command,
        "exit_code": output.status.code(),
        "stdout_sha256": hash(&output.stdout),
        "stderr_sha256": hash(&output.stderr),
        "source_sha256": source_sha256,
        "message": (!output.status.success()).then(|| String::from_utf8_lossy(&output.stderr).trim().to_owned()),
    })
}

fn pending_receipt(command: &str, message: &str, source_sha256: &str) -> Value {
    json!({
        "status": "pending",
        "command": command,
        "message": message,
        "source_sha256": source_sha256,
    })
}

fn tree_digest(path: &Path) -> Result<String, String> {
    let mut files = Vec::new();
    collect_files(path, &mut files)?;
    files.sort();
    let mut digest = Sha256::new();
    for file in files {
        let relative = file
            .strip_prefix(path)
            .map_err(|error| format!("strip artifact prefix: {error}"))?
            .to_string_lossy()
            .replace('\\', "/");
        digest.update(relative.as_bytes());
        digest.update([0]);
        digest.update(
            fs::read(&file)
                .map_err(|error| format!("read artifact {}: {error}", file.display()))?,
        );
        digest.update([0]);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<(), String> {
    fs::create_dir_all(destination).map_err(|error| {
        format!(
            "create package directory {}: {error}",
            destination.display()
        )
    })?;
    let mut entries = fs::read_dir(source)
        .map_err(|error| format!("read package directory {}: {error}", source.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read package entries: {error}"))?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let kind = fs::symlink_metadata(&source_path)
            .map_err(|error| format!("inspect package entry {}: {error}", source_path.display()))?;
        if kind.file_type().is_symlink() {
            return Err(format!(
                "package source contains unsupported symlink: {}",
                source_path.display()
            ));
        }
        if kind.is_dir() {
            if entry.file_name() == "target" || entry.file_name() == ".git" {
                continue;
            }
            copy_dir_recursive(&source_path, &destination_path)?;
        } else if kind.is_file() {
            fs::copy(&source_path, &destination_path).map_err(|error| {
                format!(
                    "copy package file {} to {}: {error}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
        }
    }
    Ok(())
}

fn normalize_gzip_header(path: &Path) -> Result<(), String> {
    let mut bytes = fs::read(path).map_err(|error| format!("read gzip archive: {error}"))?;
    if bytes.len() < 10 || bytes[0] != 0x1f || bytes[1] != 0x8b || bytes[2] != 8 {
        return Err("tar output is not a gzip stream".to_owned());
    }
    if bytes[3] != 0 {
        return Err("tar gzip header has unexpected optional fields".to_owned());
    }
    bytes[4..8].fill(0);
    bytes[8] = 0;
    bytes[9] = 3;
    fs::write(path, bytes).map_err(|error| format!("write gzip metadata: {error}"))
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    if path.is_file() {
        files.push(path.to_owned());
        return Ok(());
    }
    if !path.is_dir() {
        return Err(format!("artifact path does not exist: {}", path.display()));
    }
    for entry in fs::read_dir(path)
        .map_err(|error| format!("read artifact tree {}: {error}", path.display()))?
    {
        let entry = entry.map_err(|error| format!("read artifact entry: {error}"))?;
        collect_files(&entry.path(), files)?;
    }
    Ok(())
}

fn attach_artifact(receipt: &mut Value, kind: &str, path: &Path) -> Result<(), String> {
    let digest = if path.is_dir() {
        tree_digest(path)?
    } else {
        hash(
            &fs::read(path)
                .map_err(|error| format!("read artifact {}: {error}", path.display()))?,
        )
    };
    let artifact = json!({
        "kind": kind,
        "path": path.to_string_lossy(),
        "sha256": digest,
    });
    receipt
        .as_object_mut()
        .ok_or("command receipt is not an object")?
        .insert("artifact".to_owned(), artifact);
    Ok(())
}

fn attach_runtime_identity(receipt: &mut Value, runtime: &Path) -> Result<(), String> {
    let bytes = fs::read(runtime)
        .map_err(|error| format!("read runtime {}: {error}", runtime.display()))?;
    receipt
        .as_object_mut()
        .ok_or("runtime receipt is not an object")?
        .insert(
            "runtime".to_owned(),
            json!({
                "path": runtime.to_string_lossy(),
                "sha256": hash(&bytes),
            }),
        );
    Ok(())
}
fn fixture_endpoint_for_client() -> Option<String> {
    env::var("FIXTURE_GRPC_ADDRESS").ok().map(|address| {
        address
            .strip_prefix("http://")
            .or_else(|| address.strip_prefix("https://"))
            .unwrap_or(&address)
            .to_owned()
    })
}
fn fixture_address_required(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
) -> Option<Value> {
    if env::var("FIXTURE_GRPC_ADDRESS")
        .ok()
        .is_none_or(|address| address.trim().is_empty())
    {
        let mut receipt = pending_receipt(
            "loopback consumer",
            "FIXTURE_GRPC_ADDRESS is required for installed gRPC consumer qualification",
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        Some(receipt)
    } else {
        None
    }
}

fn run_go(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
    staging_root: &Path,
) -> Result<Value, String> {
    if let Some(receipt) = fixture_address_required(source_root, snippet, source_sha256) {
        return Ok(receipt);
    }
    let staging = validation_staging(staging_root, "go", snippet.metadata.id);
    if staging.exists() {
        fs::remove_dir_all(&staging).map_err(|error| format!("remove Go staging: {error}"))?;
    }
    fs::create_dir_all(&staging).map_err(|error| format!("create Go staging: {error}"))?;
    let go_package = env::var_os("GO_PACKAGE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| source_root.join("go"));
    let module_path = go_package.to_string_lossy().replace('\\', "/");
    fs::write(
        staging.join("go.mod"),
        format!(
            "module acyclic.example/consumer\n\ngo 1.27\n\nrequire github.com/acyclic-labs/sdk/go v0.0.0\n\nreplace github.com/acyclic-labs/sdk/go => {module_path}\n"
        ),
    )
    .map_err(|error| format!("write Go consumer module: {error}"))?;
    fs::write(staging.join("main.go"), &snippet.code)
        .map_err(|error| format!("write Go snippet: {error}"))?;
    let go = env::var_os("GO").unwrap_or_else(|| "go".into());
    let endpoint = env::var("FIXTURE_GRPC_ADDRESS").ok().map(|address| {
        address
            .strip_prefix("http://")
            .or_else(|| address.strip_prefix("https://"))
            .unwrap_or(&address)
            .to_owned()
    });
    let mut command = Command::new(&go);
    command.args(["run", "."]).current_dir(&staging);
    if let Some(endpoint) = endpoint {
        command.env("FIXTURE_GRPC_ADDRESS", endpoint);
    }
    let output = command.output();
    let output = match output {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut receipt = pending_receipt(
                "go run .",
                &format!(
                    "runtime executable is unavailable: {}",
                    PathBuf::from(&go).display()
                ),
                source_sha256,
            );
            attach_artifact(&mut receipt, "go-generated-tree", &go_package.join("gen"))?;
            receipt
                .as_object_mut()
                .ok_or("Go receipt is not an object")?
                .insert(
                    "snippet_code_sha256".to_owned(),
                    json!(hash(snippet.code.as_bytes())),
                );
            add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
            let _ = fs::remove_dir_all(staging);
            return Ok(receipt);
        }
        Err(error) => return Err(format!("start Go consumer: {error}")),
    };
    let mut receipt = command_receipt("go run .", output, source_sha256);
    attach_artifact(&mut receipt, "go-generated-tree", &go_package.join("gen"))?;
    add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
    receipt["executed"] = json!(true);
    let _ = fs::remove_dir_all(staging);
    Ok(receipt)
}

fn run_java(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
) -> Result<Value, String> {
    let Some(artifact) = env::var_os("JVM_ARTIFACT").map(PathBuf::from) else {
        let mut receipt = pending_receipt(
            "mvn -f jvm/consumer/pom.xml test",
            "JVM_ARTIFACT must identify the source-bound generated JAR",
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        return Ok(receipt);
    };
    if !artifact.is_file() {
        let mut receipt = pending_receipt(
            "mvn -f jvm/consumer/pom.xml test",
            &format!(
                "source-bound JVM artifact is absent: {}",
                artifact.display()
            ),
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        return Ok(receipt);
    }
    run_installed_consumer(
        source_root,
        snippet,
        source_sha256,
        "mvn -f jvm/consumer/pom.xml test",
        "mvn",
        &["-f", "jvm/consumer/pom.xml", "test"],
        &artifact,
        "jvm-jar",
    )
}

fn run_dotnet(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
) -> Result<Value, String> {
    let Some(artifact) = env::var_os("DOTNET_ARTIFACT").map(PathBuf::from) else {
        let mut receipt = pending_receipt(
            "dotnet run --project dotnet/consumer/Acyclic.Sdk.Transport.Consumer.csproj",
            "DOTNET_ARTIFACT must identify the source-bound generated package",
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        return Ok(receipt);
    };
    if !artifact.is_file() {
        let mut receipt = pending_receipt(
            "dotnet run --project dotnet/consumer/Acyclic.Sdk.Transport.Consumer.csproj",
            &format!(
                "source-bound .NET artifact is absent: {}",
                artifact.display()
            ),
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        return Ok(receipt);
    }
    run_installed_consumer(
        source_root,
        snippet,
        source_sha256,
        "dotnet run --project dotnet/consumer/Acyclic.Sdk.Transport.Consumer.csproj",
        "dotnet",
        &[
            "run",
            "--project",
            "dotnet/consumer/Acyclic.Sdk.Transport.Consumer.csproj",
        ],
        &artifact,
        "dotnet-nupkg",
    )
}

fn run_ruby(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
    staging_root: &Path,
) -> Result<Value, String> {
    if let Some(receipt) = fixture_address_required(source_root, snippet, source_sha256) {
        return Ok(receipt);
    }
    let artifact = env::var_os("RUBY_ARTIFACT").map(PathBuf::from);
    let Some(artifact) = artifact else {
        let mut receipt = pending_receipt(
            "ruby gem install + rendered snippet",
            "RUBY_ARTIFACT must identify the source-bound generated gem",
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        return Ok(receipt);
    };
    if !artifact.is_file() {
        let mut receipt = pending_receipt(
            "ruby gem install + rendered snippet",
            &format!(
                "source-bound Ruby artifact is absent: {}",
                artifact.display()
            ),
            source_sha256,
        );
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        return Ok(receipt);
    };
    let staging = validation_staging(staging_root, "ruby", snippet.metadata.id);
    if staging.exists() {
        fs::remove_dir_all(&staging).map_err(|error| format!("remove Ruby staging: {error}"))?;
    }
    fs::create_dir_all(&staging).map_err(|error| format!("create Ruby staging: {error}"))?;
    let gem_home = staging.join("gems");
    let ruby = runtime_command(source_root, "ruby");
    let install = Command::new(&ruby)
        .args(["-S", "gem", "install", "--local", "--no-document"])
        .arg(&artifact)
        .args(["--install-dir"])
        .arg(&gem_home)
        .output();
    let install = match install {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut receipt = pending_receipt(
                "ruby gem install + rendered snippet",
                &format!("Ruby runtime is unavailable: {}", ruby.display()),
                source_sha256,
            );
            attach_artifact(&mut receipt, "ruby-gem", &artifact)?;
            let _ = fs::remove_dir_all(&staging);
            return Ok(receipt);
        }
        Err(error) => return Err(format!("start Ruby gem installation: {error}")),
    };
    if !install.status.success() {
        let mut receipt = command_receipt("ruby gem install", install, source_sha256);
        attach_artifact(&mut receipt, "ruby-gem", &artifact)?;
        add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
        let _ = fs::remove_dir_all(&staging);
        return Ok(receipt);
    }
    let gem_root = gem_home.join("gems");
    let gem_name = artifact
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or("Ruby artifact has no filename")?;
    let installed = gem_root.join(gem_name);
    if !installed.is_dir() {
        return Err(format!(
            "Ruby installation did not produce the source-bound gem directory: {}",
            installed.display()
        ));
    }
    let snippet_path = staging.join("snippet.rb");
    fs::write(&snippet_path, &snippet.code)
        .map_err(|error| format!("write Ruby snippet: {error}"))?;
    let output = Command::new(&ruby)
        .args(["-I"])
        .arg(installed.join("lib"))
        .arg(&snippet_path)
        .env(
            "FIXTURE_GRPC_ADDRESS",
            fixture_endpoint_for_client().unwrap_or_default(),
        )
        .env("GEM_HOME", &gem_home)
        .output()
        .map_err(|error| format!("start Ruby rendered snippet: {error}"))?;
    let mut receipt = command_receipt("ruby gem install + rendered snippet", output, source_sha256);
    attach_artifact(&mut receipt, "ruby-gem", &artifact)?;
    add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
    attach_runtime_identity(&mut receipt, &ruby)?;
    receipt["executed"] = json!(true);
    let _ = fs::remove_dir_all(&staging);
    Ok(receipt)
}

fn run_php(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
    staging_root: &Path,
) -> Result<Value, String> {
    if let Some(receipt) = fixture_address_required(source_root, snippet, source_sha256) {
        return Ok(receipt);
    }
    let Some(package) = env::var_os("PHP_PACKAGE_ROOT").map(PathBuf::from) else {
        return Ok(pending_receipt(
            "php native package + rendered snippet",
            "PHP_PACKAGE_ROOT must identify the source-bound generated package",
            source_sha256,
        ));
    };
    if !package.is_dir() {
        return Ok(pending_receipt(
            "php native package + rendered snippet",
            &format!("PHP package artifact is absent: {}", package.display()),
            source_sha256,
        ));
    }
    let staging = validation_staging(staging_root, "php", snippet.metadata.id);
    if staging.exists() {
        fs::remove_dir_all(&staging).map_err(|error| format!("remove PHP staging: {error}"))?;
    }
    copy_tree(&package, &staging)?;
    let bin = staging.join("bin");
    fs::create_dir_all(&bin).map_err(|error| format!("create PHP bin: {error}"))?;
    let snippet_path = bin.join("snippet.php");
    fs::write(&snippet_path, &snippet.code)
        .map_err(|error| format!("write PHP snippet: {error}"))?;
    let php = runtime_command(source_root, "php");
    let Some(protobuf) = env::var_os("PHP_PROTOBUF_EXTENSION").map(PathBuf::from) else {
        return Ok(pending_receipt(
            "php native package + rendered snippet",
            "PHP_PROTOBUF_EXTENSION must identify the source-bound protobuf extension",
            source_sha256,
        ));
    };
    let Some(grpc) = env::var_os("PHP_GRPC_EXTENSION").map(PathBuf::from) else {
        return Ok(pending_receipt(
            "php native package + rendered snippet",
            "PHP_GRPC_EXTENSION must identify the source-bound gRPC extension",
            source_sha256,
        ));
    };
    let output = Command::new(&php)
        .args(["-d"])
        .arg(format!("extension={}", protobuf.display()))
        .args(["-d"])
        .arg(format!("extension={}", grpc.display()))
        .arg(&snippet_path)
        .env(
            "FIXTURE_GRPC_ADDRESS",
            fixture_endpoint_for_client().unwrap_or_default(),
        )
        .current_dir(&staging)
        .output();
    let output = match output {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut receipt = pending_receipt(
                "php native package + rendered snippet",
                &format!("PHP runtime is unavailable: {}", php.display()),
                source_sha256,
            );
            attach_artifact(&mut receipt, "php-package-tree", &package)?;
            let _ = fs::remove_dir_all(&staging);
            return Ok(receipt);
        }
        Err(error) => return Err(format!("start PHP rendered snippet: {error}")),
    };
    let mut receipt = command_receipt(
        "php native package + rendered snippet",
        output,
        source_sha256,
    );
    if matches!(
        receipt["exit_code"].as_i64(),
        Some(-1_073_741_819 | -1_073_740_791)
    ) && receipt["stderr_sha256"] == hash(b"")
    {
        receipt["native_shutdown_exit_code"] = receipt["exit_code"].clone();
        receipt["message"] =
            json!("native gRPC extension emitted output, but the PHP process did not exit cleanly");
    }
    attach_artifact(&mut receipt, "php-package-tree", &package)?;
    add_snippet_binding(&mut receipt, source_root, snippet, source_sha256);
    attach_runtime_identity(&mut receipt, &php)?;
    receipt
        .as_object_mut()
        .ok_or("PHP receipt is not an object")?
        .insert(
            "native_extensions".to_owned(),
            json!({"protobuf": protobuf.to_string_lossy(), "grpc": grpc.to_string_lossy()}),
        );
    receipt["executed"] = json!(true);
    let _ = fs::remove_dir_all(&staging);
    Ok(receipt)
}
fn run_dart(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
    staging_root: &Path,
) -> Result<Value, String> {
    if let Some(receipt) = fixture_address_required(source_root, snippet, source_sha256) {
        return Ok(receipt);
    }
    let package = env::var_os("DART_PACKAGE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| source_root.join("dart"));
    let generated = package.join("lib/src/generated");
    if !generated.is_dir() {
        return Ok(pending_receipt(
            "dart pub get + dart run",
            "dart generated artifact is absent",
            source_sha256,
        ));
    }
    let staging = validation_staging(staging_root, "dart", snippet.metadata.id);
    if staging.exists() {
        fs::remove_dir_all(&staging).map_err(|error| format!("remove Dart staging: {error}"))?;
    }
    fs::create_dir_all(staging.join("lib"))
        .map_err(|error| format!("create Dart staging: {error}"))?;
    copy_tree(&package.join("lib"), &staging.join("lib"))?;
    let package_config = package.join(".dart_tool");
    if package_config.is_dir() {
        copy_tree(&package_config, &staging.join(".dart_tool"))?;
    }
    for name in ["pubspec.yaml", "pubspec.lock", "analysis_options.yaml"] {
        fs::copy(package.join(name), staging.join(name))
            .map_err(|error| format!("stage Dart {name}: {error}"))?;
    }
    let bin = staging.join("bin");
    fs::create_dir_all(&bin).map_err(|error| format!("create Dart bin: {error}"))?;
    fs::write(bin.join("snippet.dart"), &snippet.code)
        .map_err(|error| format!("write Dart snippet: {error}"))?;
    let dart = runtime_command(source_root, "dart");
    let pub_cache = if package.join(".pub-cache").is_dir() {
        package.join(".pub-cache")
    } else if source_root.join("dart/.pub-cache").is_dir() {
        source_root.join("dart/.pub-cache")
    } else {
        package.join(".pub-cache")
    };
    let resolve = Command::new(&dart)
        .args(["pub", "get", "--offline"])
        .env("PUB_CACHE", &pub_cache)
        .env(
            "FIXTURE_GRPC_ADDRESS",
            fixture_endpoint_for_client().unwrap_or_default(),
        )
        .current_dir(&staging)
        .output();
    let resolve = match resolve {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut receipt = pending_receipt(
                "dart pub get + dart run",
                &format!("runtime executable is unavailable: {}", dart.display()),
                source_sha256,
            );
            attach_artifact(&mut receipt, "dart-generated-tree", &generated)?;
            let _ = fs::remove_dir_all(&staging);
            return Ok(receipt);
        }
        Err(error) => return Err(format!("start Dart package resolution: {error}")),
    };
    if !resolve.status.success() {
        let mut receipt = command_receipt("dart pub get", resolve, source_sha256);
        attach_artifact(&mut receipt, "dart-generated-tree", &generated)?;
        let _ = fs::remove_dir_all(&staging);
        return Ok(receipt);
    }
    let output = Command::new(&dart)
        .args(["run", "--no-pub", "bin/snippet.dart"])
        .env("PUB_CACHE", &pub_cache)
        .current_dir(&staging)
        .output()
        .map_err(|error| format!("start Dart snippet: {error}"))?;
    let mut receipt = command_receipt("dart pub get + dart run", output, source_sha256);
    attach_artifact(&mut receipt, "dart-package-tree", &package)?;
    attach_artifact(&mut receipt, "dart-generated-tree", &generated)?;
    attach_runtime_identity(&mut receipt, &dart)?;
    receipt
        .as_object_mut()
        .ok_or("Dart receipt is not an object")?
        .insert(
            "snippet_code_sha256".to_owned(),
            json!(hash(snippet.code.as_bytes())),
        );
    let _ = fs::remove_dir_all(&staging);
    Ok(receipt)
}

fn run_installed_consumer(
    source_root: &Path,
    snippet: &RenderedSnippet,
    source_sha256: &str,
    command_name: &str,
    executable: &str,
    args: &[&str],
    artifact: &Path,
    artifact_kind: &str,
) -> Result<Value, String> {
    if let Some(receipt) = fixture_address_required(source_root, snippet, source_sha256) {
        return Ok(receipt);
    }
    let runtime = runtime_command(source_root, executable);
    let mut command = Command::new(&runtime);
    if executable == "dotnet" {
        if let Ok(address) = env::var("FIXTURE_GRPC_ADDRESS") {
            command.env("ACYCLIC_FIXTURE_ENDPOINT", address);
        }
    }
    let working_dir = if executable == "bundle" {
        source_root.join("ruby")
    } else {
        source_root.to_owned()
    };
    let output = command.args(args).current_dir(working_dir).output();
    let output = match output {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut receipt = pending_receipt(
                command_name,
                &format!("runtime executable is unavailable: {}", runtime.display()),
                source_sha256,
            );
            if artifact.exists() {
                attach_artifact(&mut receipt, artifact_kind, artifact)?;
            }
            receipt
                .as_object_mut()
                .ok_or("consumer receipt is not an object")?
                .insert(
                    "snippet_code_sha256".to_owned(),
                    json!(hash(snippet.code.as_bytes())),
                );
            return Ok(receipt);
        }
        Err(error) => return Err(format!("start {command_name}: {error}")),
    };
    let mut receipt = command_receipt(command_name, output, source_sha256);
    if receipt_status_is_toolchain_pending(command_name, &receipt) {
        receipt
            .as_object_mut()
            .ok_or("consumer receipt is not an object")?
            .insert("status".to_owned(), json!("pending"));
    }
    if artifact.exists() {
        attach_artifact(&mut receipt, artifact_kind, artifact)?;
    }
    receipt
        .as_object_mut()
        .ok_or("consumer receipt is not an object")?
        .insert(
            "snippet_code_sha256".to_owned(),
            json!(hash(snippet.code.as_bytes())),
        );
    Ok(receipt)
}

fn receipt_status_is_toolchain_pending(command_name: &str, receipt: &Value) -> bool {
    if !command_name.starts_with("dotnet ") || receipt["status"] != "failed" {
        return false;
    }
    receipt["message"]
        .as_str()
        .is_some_and(|message| message.contains("No .NET SDKs were found"))
}

fn runtime_command(source_root: &Path, executable: &str) -> PathBuf {
    if let Some(value) = env::var_os(executable.to_ascii_uppercase()) {
        return PathBuf::from(value);
    }
    if executable == "dart" {
        let package_dart = source_root.join("dart/.toolchain/dart-sdk/bin/dart.exe");
        if package_dart.is_file() {
            return package_dart;
        }
    }
    if cfg!(windows) {
        return PathBuf::from(match executable {
            "mvn" => "mvn.cmd".to_owned(),
            "bundle" => "bundle.bat".to_owned(),
            _ => executable.to_owned(),
        });
    }
    PathBuf::from(executable)
}

fn write_bundle(output: &Path, manifest: &Value) -> Result<(), String> {
    let snippets = manifest
        .get("snippets")
        .and_then(Value::as_array)
        .ok_or("bundle snippets are missing")?;
    let files = manifest
        .get("files")
        .and_then(Value::as_array)
        .ok_or("bundle files are missing")?;
    let snippets_root = output.join("snippets");
    if snippets_root.exists() {
        fs::remove_dir_all(&snippets_root)
            .map_err(|error| format!("remove stale snippets: {error}"))?;
    }
    for item in snippets {
        let path = output.join(
            item.get("path")
                .and_then(Value::as_str)
                .ok_or("snippet path missing")?,
        );
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("create snippet directory: {error}"))?;
        }
        let language = item
            .get("language")
            .and_then(Value::as_str)
            .ok_or("snippet language missing")?;
        let id = item
            .get("id")
            .and_then(Value::as_str)
            .ok_or("snippet ID missing")?;
        let code = all_rendered_snippets()
            .into_iter()
            .find(|snippet| {
                snippet.metadata.id == id && snippet.metadata.language.as_str() == language
            })
            .ok_or("snippet source disappeared during write")?;
        fs::write(path, code.code).map_err(|error| format!("write snippet: {error}"))?;
    }
    let expected = files.len();
    if expected != snippets.len() {
        return Err("bundle file list does not match snippets".to_owned());
    }
    let bytes =
        serde_json::to_vec_pretty(manifest).map_err(|error| format!("encode manifest: {error}"))?;
    fs::write(output.join(MANIFEST), [bytes.as_slice(), b"\n"].concat())
        .map_err(|error| format!("write manifest: {error}"))
}

/// Returns every Rust-owned scenario projection in the stable bundle order.
///
/// The manifest and the files must be produced from the same registry.  Keep
/// the guide projections here rather than reconstructing the lookup from the
/// legacy two-scenario renderer: otherwise the six public family projections
/// are present in the manifest but disappear when the bundle is written.
fn all_rendered_snippets() -> Vec<RenderedSnippet> {
    render_all()
        .into_iter()
        .chain(
            guide_projections::all()
                .into_iter()
                .map(guide_projections::rendered),
        )
        .collect()
}

fn check_bundle(output: &Path, manifest: &Value) -> Result<(), String> {
    let path = output.join(MANIFEST);
    let actual = serde_json::from_slice::<Value>(
        &fs::read(&path).map_err(|error| format!("read manifest: {error}"))?,
    )
    .map_err(|error| format!("decode manifest: {error}"))?;
    if actual != *manifest {
        return Err("generated snippet manifest differs; run sdk-examples generate".to_owned());
    }
    for item in manifest
        .get("snippets")
        .and_then(Value::as_array)
        .ok_or("bundle snippets are missing")?
    {
        let relative = item
            .get("path")
            .and_then(Value::as_str)
            .ok_or("snippet path missing")?;
        let code = fs::read(output.join(relative))
            .map_err(|error| format!("read snippet {relative}: {error}"))?;
        let expected = item
            .get("code_sha256")
            .and_then(Value::as_str)
            .ok_or("snippet hash missing")?;
        if hash(&code) != expected {
            return Err(format!("snippet drift detected: {relative}"));
        }
    }
    Ok(())
}

fn git_revision(root: &Path) -> String {
    let root = match fs::canonicalize(root) {
        Ok(root) => root,
        Err(_) => return "working-tree".to_owned(),
    };
    let repository = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(&root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| fs::canonicalize(String::from_utf8_lossy(&output.stdout).trim()).ok());
    if repository.as_deref() != Some(root.as_path()) {
        return "working-tree".to_owned();
    }
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|revision| !revision.is_empty())
        .unwrap_or_else(|| "working-tree".to_owned())
}

fn normalize_invocation_artifact(bytes: &[u8]) -> (Vec<u8>, &'static str) {
    // PE/COFF stores a linker timestamp in the COFF header. It changes on
    // every cargo install even when the source, lockfile, and package bytes
    // are identical. The timestamp is not part of the executable behavior,
    // so zero only that field and retain every other byte for the evidence
    // artifact. Non-PE targets remain byte-for-byte unchanged.
    if bytes.len() < 0x40 || &bytes[..2] != b"MZ" {
        return (bytes.to_vec(), "identity-v1");
    }
    let pe_offset =
        u32::from_le_bytes([bytes[0x3c], bytes[0x3d], bytes[0x3e], bytes[0x3f]]) as usize;
    let Some(timestamp) = pe_offset.checked_add(8) else {
        return (bytes.to_vec(), "identity-v1");
    };
    let Some(pe_header_end) = pe_offset.checked_add(12) else {
        return (bytes.to_vec(), "identity-v1");
    };
    let Some(signature_end) = pe_offset.checked_add(4) else {
        return (bytes.to_vec(), "identity-v1");
    };
    let Some(timestamp_end) = timestamp.checked_add(4) else {
        return (bytes.to_vec(), "identity-v1");
    };
    if pe_header_end > bytes.len()
        || signature_end > bytes.len()
        || &bytes[pe_offset..signature_end] != b"PE\0\0"
        || timestamp_end > bytes.len()
    {
        return (bytes.to_vec(), "identity-v1");
    }
    let mut normalized = bytes.to_vec();
    normalized[timestamp..timestamp + 4].fill(0);
    (normalized, "pe-coff-timestamp-zero-v1")
}

fn deterministic_rustflags() -> String {
    let existing = env::var("RUSTFLAGS").unwrap_or_default();
    if cfg!(windows) {
        if existing.trim().is_empty() {
            "-C link-arg=/Brepro".to_owned()
        } else if existing
            .split_whitespace()
            .any(|flag| flag == "link-arg=/Brepro")
        {
            existing
        } else {
            format!("{existing} -C link-arg=/Brepro")
        }
    } else {
        existing
    }
}

fn hash(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_extensions_are_stable() {
        assert_eq!(extension(Language::Rust), "rs");
        assert_eq!(extension(Language::Python), "py");
        assert_eq!(extension(Language::TypeScript), "ts");
        assert_eq!(extension(Language::Go), "go");
        assert_eq!(extension(Language::Java), "java");
        assert_eq!(extension(Language::CSharp), "cs");
        assert_eq!(extension(Language::Ruby), "rb");
        assert_eq!(extension(Language::Dart), "dart");
        assert_eq!(extension(Language::Php), "php");
        assert_eq!(extension(Language::Php), "php");
    }

    #[test]
    fn bundle_renderer_includes_every_rust_owned_guide_projection() {
        let snippets = all_rendered_snippets();
        let guide = snippets
            .iter()
            .filter(|snippet| snippet.metadata.family != "actors" && snippet.metadata.family != "stream")
            .collect::<Vec<_>>();
        assert_eq!(guide.len(), 54, "six guide families across nine languages");
        assert!(guide.iter().all(|snippet| !snippet.code.is_empty()));
        assert!(guide.iter().any(|snippet| {
            snippet.metadata.id == "objects-memory-put-get"
                && snippet.metadata.language == Language::TypeScript
        }));
    }

    #[test]
    fn compiled_source_gate_accepts_only_the_compiled_closure() {
        let compiled = env!("SDK_EXAMPLES_SOURCE_SHA256");
        ensure_compiled_source_matches(compiled).expect("matching compiled source accepted");
        let changed = hash(b"source changed after the producer was compiled");
        assert_ne!(compiled, changed);
        let error = ensure_compiled_source_matches(&changed)
            .expect_err("a stale compiled producer must reject changed source");
        assert!(error.contains("does not match compiled producer"));
        assert!(error.contains(compiled));
        assert!(error.contains(&changed));
    }

    #[test]
    fn pe_invocation_normalization_only_clears_coff_timestamp() {
        let mut executable = vec![0u8; 0x90];
        executable[..2].copy_from_slice(b"MZ");
        executable[0x3c..0x40].copy_from_slice(&(0x60u32).to_le_bytes());
        executable[0x60..0x64].copy_from_slice(b"PE\0\0");
        executable[0x68..0x6c].copy_from_slice(&0x12345678u32.to_le_bytes());
        executable[0x80] = 0xa5;
        let (normalized, method) = normalize_invocation_artifact(&executable);
        assert_eq!(method, "pe-coff-timestamp-zero-v1");
        assert_eq!(&normalized[0x68..0x6c], &[0, 0, 0, 0]);
        assert_eq!(normalized[0x80], 0xa5);
    }

    #[test]
    fn non_pe_invocation_normalization_preserves_bytes() {
        let bytes = b"not an executable";
        let (normalized, method) = normalize_invocation_artifact(bytes);
        assert_eq!(method, "identity-v1");
        assert_eq!(normalized, bytes);
    }

    #[test]
    fn malformed_pe_offset_is_rejected_without_panicking() {
        let mut bytes = vec![0u8; 0x40];
        bytes[..2].copy_from_slice(b"MZ");
        bytes[0x3c..0x40].copy_from_slice(&u32::MAX.to_le_bytes());
        let (normalized, method) = normalize_invocation_artifact(&bytes);
        assert_eq!(method, "identity-v1");
        assert_eq!(normalized, bytes);
    }

    #[test]
    fn bundle_source_hash_is_content_based() {
        assert_ne!(hash(b"before"), hash(b"after"));
    }

    #[test]
    fn live_source_closure_detects_mutation_before_final_receipt() {
        let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let source_files = source_closure::closure_files(&source_root).expect("source closure");
        let root = env::temp_dir().join(format!(
            "acyclic-sdk-examples-source-closure-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after epoch")
                .as_nanos()
        ));
        for relative in &source_files {
            let path = root.join(relative);
            fs::create_dir_all(path.parent().expect("source parent")).expect("source directory");
            fs::copy(source_root.join(relative), &path).expect("source input");
        }

        let before = scenario_source_sha256(&root).expect("initial source hash");
        ensure_source_unchanged(&root, &before).expect("unchanged source accepted");
        let relocated = root.with_file_name(format!(
            "{}-relocated",
            root.file_name()
                .expect("source fixture name")
                .to_string_lossy()
        ));
        for relative in &source_files {
            let path = relocated.join(relative);
            fs::create_dir_all(path.parent().expect("relocated source parent"))
                .expect("relocated source directory");
            fs::copy(root.join(relative), &path).expect("relocated source input");
        }
        let relocated_digest = scenario_source_sha256(&relocated).expect("relocated source hash");
        assert_eq!(
            before, relocated_digest,
            "source identity must be relocation stable"
        );
        let mutated = root.join("rust/crates/objects/src/lib.rs");
        fs::write(&mutated, b"mutated during generation").expect("mutate source input");
        let after = scenario_source_sha256(&root).expect("mutated source hash");
        assert_ne!(before, after, "live source hash must observe runtime edits");
        assert!(
            ensure_source_unchanged(&root, &before).is_err(),
            "final receipt gate must reject source edits"
        );
        fs::remove_dir_all(root).expect("clean source closure fixture");
        fs::remove_dir_all(relocated).expect("clean relocated source closure fixture");
    }
}



