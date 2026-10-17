use super::*;
/// Render the TypeScript consumer from the JSON emitted by the Rust example.
/// The wrapper is deliberately small; every request value comes from the
/// executable output and the installed package remains the runtime authority.
pub fn render_typescript(
    executions: &[ScenarioExecution],
) -> Result<Vec<TypeScriptSnippet>, Error> {
    executions
        .iter()
        .filter(|execution| execution.scenario.kind.has_typescript_projection())
        .map(|execution| match execution.scenario.kind {
            ScenarioKind::MachinesTypescriptConsumer => render_machines_typescript(execution),
            ScenarioKind::ActorsTypescriptConsumer => render_actors_typescript(execution),
            ScenarioKind::StreamTypescriptConsumer => render_stream_typescript(execution),
            ScenarioKind::ObjectsTypescriptConsumer => render_objects_typescript(execution),
            ScenarioKind::InferenceContractDefaults => render_inference_typescript(execution),
            _ => unreachable!("filtered scenario kind must have a TypeScript renderer"),
        })
        .collect()
}

fn render_stream_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    let request = value
        .get("request")
        .ok_or_else(|| Error::Invalid("Stream scenario output has no request".into()))?;
    let path = string_field(request, "path")?;
    let values = required_field(request, "values")?;
    let value_arrays = values
        .as_array()
        .ok_or_else(|| Error::Invalid("Stream request values are not an array".into()))?;
    if value_arrays.is_empty() {
        return Err(Error::Invalid("Stream request values are empty".into()));
    }
    let values_literal = value_arrays
        .iter()
        .map(|value| {
            let _ = bytes_hex(value)?;
            serde_json::to_string(value).map_err(|error| Error::Invalid(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    let idempotency_key = required_field(request, "idempotency_key")?;
    let _ = bytes_hex(idempotency_key)?;
    let idempotency_literal = serde_json::to_string(idempotency_key)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let read_limit = number_field(request, "read_limit")?;
    let append = value
        .get("append")
        .ok_or_else(|| Error::Invalid("Stream scenario output has no append receipt".into()))?;
    let append_tail = number_field(append, "tail")?;
    let tail = number_field(&value, "tail")?;
    let records = value
        .get("records")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::Invalid("Stream scenario output has no records".into()))?;
    if records.len() != value_arrays.len() {
        return Err(Error::Invalid(
            "Stream read record count differs from request".into(),
        ));
    }
    let record_values = records
        .iter()
        .map(|record| {
            let bytes = required_field(record, "value")?;
            let _ = bytes_hex(bytes)?;
            serde_json::to_string(bytes).map_err(|error| Error::Invalid(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ StreamClient, idempotencyKey }} from \"@acyclic-labs/stream\";\n\nconst path = {path:?};\nconst values = [{values_literal}].map(value => Uint8Array.from(value));\nconst retry = idempotencyKey(Uint8Array.from({idempotency_literal}));\nconst stream = StreamClient.memory().bytes(path);\nconst append = await stream.appendBatch(values, {{ idempotencyKey: retry }});\nif (!append.ok || append.tail !== BigInt(\"{append_tail}\")) throw new Error(\"Rust append receipt parity failed\");\nconst tail = await stream.tail();\nif (tail !== BigInt(\"{tail}\")) throw new Error(\"Rust tail parity failed\");\nconst records = [];\nfor await (const record of stream.read({{ from: 0n, limit: {read_limit} }})) records.push(record.value);\nconst expected = [{record_values}].map(value => Uint8Array.from(value));\nconst recordsDiffer = records.some((record, index) => {{\n  const expectedRecord = expected[index];\n  return expectedRecord === undefined || record.length !== expectedRecord.length || record.some((byte, offset) => byte !== expectedRecord[offset]);\n}});\nif (records.length !== expected.length || recordsDiffer) throw new Error(\"Rust read parity failed\");\nconsole.log(JSON.stringify({{ tail: tail.toString(), records: records.length }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/stream",
        path: "generated/scenarios/stream/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn render_actors_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    let request = value
        .get("request")
        .ok_or_else(|| Error::Invalid("Actors scenario output has no request".into()))?;
    let code_sha256 = required_field(request, "code_sha256")?;
    let _code_sha256_hex = bytes_hex(code_sha256)?;
    let home_region = string_field(request, "home_region")?;
    let idempotency_key = string_field(request, "idempotency_key")?;
    let limits = request
        .get("limits")
        .ok_or_else(|| Error::Invalid("Actors request has no limits".into()))?;
    let handler_timeout_millis = number_field(limits, "handler_timeout_millis")?;
    let memory_bytes = number_field(limits, "memory_bytes")?;
    let checkpoint_bytes = number_field(limits, "checkpoint_bytes")?;
    let bindings = required_field(request, "bindings")?;
    let subscriptions = required_field(request, "subscriptions")?;
    if value.get("validated") != Some(&serde_json::Value::Bool(true)) {
        return Err(Error::Invalid(
            "Actors scenario did not report canonical validation".into(),
        ));
    }
    let code_literal =
        serde_json::to_string(code_sha256).map_err(|error| Error::Invalid(error.to_string()))?;
    let region_literal =
        serde_json::to_string(&home_region).map_err(|error| Error::Invalid(error.to_string()))?;
    let idempotency_literal = serde_json::to_string(&idempotency_key)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let bindings_literal = bindings
        .as_array()
        .ok_or_else(|| Error::Invalid("Actors bindings are not an array".into()))?
        .iter()
        .map(|binding| {
            let name = string_field(binding, "name")?;
            let capability = string_field(binding, "capability")?;
            let resource = string_field(binding, "resource")?;
            Ok(format!(
                "create(BindingSchema, {{ name: {name:?}, capability: {capability:?}, resource: {resource:?} }})"
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?
        .join(", ");
    let subscriptions_literal = subscriptions
        .as_array()
        .ok_or_else(|| Error::Invalid("Actors subscriptions are not an array".into()))?
        .iter()
        .map(|subscription| {
            let subscription_id = string_field(subscription, "subscription_id")?;
            let stream_path = string_field(subscription, "stream_path")?;
            let placement_anchor = subscription
                .get("placement_anchor")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| Error::Invalid("Actor subscription anchor is not a bool".into()))?;
            let start = subscription
                .get("start")
                .and_then(|start| start.get("cursor"))
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| Error::Invalid("Actor subscription cursor is absent".into()))?;
            Ok(format!(
                "create(SubscriptionSpecSchema, {{ subscriptionId: {subscription_id:?}, streamPath: {stream_path:?}, start: create(SubscriptionStartSchema, {{ start: {{ case: \"cursor\", value: BigInt(\"{start}\") }} }}), placementAnchor: {placement_anchor} }})"
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?
        .join(", ");
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ ActorLimitsSchema, BindingSchema, CreateActorRequestSchema, SubscriptionSpecSchema, SubscriptionStartSchema }} from \"@acyclic-labs/actors/proto\";\nimport {{ create, toBinary }} from \"@bufbuild/protobuf\";\n\nconst request = create(CreateActorRequestSchema, {{\n  codeSha256: Uint8Array.from({code_literal}),\n  homeRegion: {region_literal},\n  bindings: [{bindings_literal}],\n  limits: create(ActorLimitsSchema, {{\n    handlerTimeoutMillis: BigInt(\"{handler_timeout_millis}\"),\n    memoryBytes: BigInt(\"{memory_bytes}\"),\n    checkpointBytes: BigInt(\"{checkpoint_bytes}\"),\n  }}),\n  subscriptions: [{subscriptions_literal}],\n  idempotencyKey: {idempotency_literal},\n}});\nconst encoded = toBinary(CreateActorRequestSchema, request);\nif (encoded.length === 0) throw new Error(\"Rust Actors request encoded to an empty payload\");\nconsole.log(JSON.stringify({{ homeRegion: request.homeRegion, encodedBytes: encoded.length }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/actors",
        path: "generated/scenarios/actors/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn render_machines_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    let request = value
        .get("request")
        .ok_or_else(|| Error::Invalid("Machines scenario output has no request".into()))?;
    let idempotency_key = string_field(request, "idempotency_key")?;
    let compatibility = policy_kind(request, "compatibility", "BestEffort", "best-effort")?;
    let expiration = policy_kind(request, "expiration", "Never", "never")?;
    let image = request
        .get("image")
        .and_then(|image| image.get("ManagedOci"))
        .ok_or_else(|| Error::Invalid("Machines request has no ManagedOci image".into()))?;
    let image_hex = bytes_hex(image)?;
    let network_hex =
        bytes_hex(request.get("network_policy_digest").ok_or_else(|| {
            Error::Invalid("Machines request has no network policy digest".into())
        })?)?;
    let suspension_policy = request
        .get("suspension")
        .ok_or_else(|| Error::Invalid("Machines request has no suspension policy".into()))?;
    let suspension_kind = nested_policy_kind(suspension_policy, "AfterIdle", "after-idle")?;
    let suspension = suspension_policy
        .get("AfterIdle")
        .ok_or_else(|| Error::Invalid("Machines request has no AfterIdle policy".into()))?;
    let seconds = number_field(suspension, "secs")?;
    let nanos = number_field(suspension, "nanos")?;
    let milliseconds = seconds * 1_000 + nanos / 1_000_000;
    let budgets = request
        .get("budgets")
        .ok_or_else(|| Error::Invalid("Machines request has no budgets".into()))?;
    let spend_micros = number_field(budgets, "spend_micros")?;
    let concurrency = number_field(budgets, "concurrency")?;
    if value
        .get("outcome")
        .and_then(|outcome| outcome.get("Created"))
        .is_none()
    {
        return Err(Error::Invalid(
            "Machines scenario did not create a Created outcome".into(),
        ));
    }
    let page_machines = value
        .get("page")
        .and_then(|page| page.get("machines"))
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::Invalid("Machines scenario has no page machines".into()))?;
    let page_limit = value
        .get("page_size")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::Invalid("Machines scenario has no page size".into()))?;
    let page_size = page_machines.len();
    let page_suspension = page_machines
        .first()
        .and_then(|machine| machine.get("contract"))
        .and_then(|contract| contract.get("suspension"))
        .and_then(|suspension| suspension.get("AfterIdle"))
        .map(|_| "after-idle")
        .ok_or_else(|| Error::Invalid("Machines page has no AfterIdle policy".into()))?;
    let idempotency_literal = serde_json::to_string(&idempotency_key)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let image_literal =
        serde_json::to_string(&format!("registry.example/generated@sha256:{image_hex}"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ SimulatedMachines, idempotencyKey, managedOci, type CreateMachine }} from \"@acyclic-labs/machines\";\n\nconst request = {{\n  idempotencyKey: idempotencyKey({idempotency_literal}),\n  image: managedOci({image_literal}),\n  compatibility: {{ kind: \"{compatibility}\" }},\n  suspension: {{ kind: \"{suspension_kind}\", milliseconds: {milliseconds} }},\n  expiration: {{ kind: \"{expiration}\" }},\n  networkPolicyDigestHex: \"{network_hex}\",\n  budgets: {{ spendMicros: BigInt(\"{spend_micros}\"), concurrency: {concurrency} }},\n}} satisfies CreateMachine;\n\nconst provider = new SimulatedMachines();\nconst outcome = await provider.create(request);\nif (outcome.kind !== \"created\") throw new Error(`expected created outcome, received ${{outcome.kind}}`);\nconst page = await provider.listMachines(null, {page_limit});\nconst firstMachine = page.machines[0];\nif (page.machines.length !== {page_size} || firstMachine === undefined || firstMachine.contract.suspension.kind !== \"{page_suspension}\") throw new Error(\"Rust scenario parity failed\");\nconsole.log(JSON.stringify({{ kind: outcome.kind, pageSize: page.machines.length, suspension: firstMachine.contract.suspension.kind }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/machines",
        path: "generated/scenarios/machines/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn render_objects_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    let bucket = string_field(&value, "bucket")?;
    let key = string_field(&value, "key")?;
    let body = string_field(&value, "body")?;
    let size = number_field(&value, "size")?;
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ create }} from \"@bufbuild/protobuf\";\nimport {{ BucketRefSchema, CreateBucketRequestSchema, GetObjectRequestSchema, MemoryObjectsV1, PutObjectHeaderSchema }} from \"@acyclic-labs/objects/v1\";\n\nconst bucket = {bucket:?};\nconst key = {key:?};\nconst body = new TextEncoder().encode({body:?});\nconst provider = await MemoryObjectsV1.create();\nawait provider.createBucket(create(CreateBucketRequestSchema, {{ name: bucket }}));\nconst written = await provider.put(create(PutObjectHeaderSchema, {{ bucket: create(BucketRefSchema, {{ name: bucket }}), objectKey: key }}), body);\nif (written.size !== BigInt(\"{size}\")) throw new Error(\"Rust Objects size parity failed\");\nconst selected = await provider.get(create(GetObjectRequestSchema, {{ bucket: create(BucketRefSchema, {{ name: bucket }}), objectKey: key }}), 1024n);\nif (new TextDecoder().decode(selected.body) !== {body:?}) throw new Error(\"Rust Objects body parity failed\");\nconsole.log(JSON.stringify({{ bucket, key, size: Number(written.size) }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/objects",
        path: "generated/scenarios/objects/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn render_inference_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    let message = number_field(&value, "maximum_message_bytes")?;
    let http = number_field(&value, "maximum_http_json_bytes")?;
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ create }} from \"@bufbuild/protobuf\";\nimport {{ RequestIdentitySchema }} from \"@acyclic-labs/inference/proto\";\n\nconst identity = create(RequestIdentitySchema);\nif (identity.requestId.byteLength !== 0) throw new Error(\"generated identity default changed\");\nconst maximumMessageBytes = {message};\nconst maximumHttpJsonBytes = {http};\nconsole.log(JSON.stringify({{ maximumMessageBytes, maximumHttpJsonBytes }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/inference",
        path: "generated/scenarios/inference/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn nested_policy_kind(
    value: &serde_json::Value,
    rust_variant: &str,
    typescript_kind: &'static str,
) -> Result<&'static str, Error> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::Invalid("Machines policy is not a Rust enum object".into()))?;
    match object.len() {
        1 if object.contains_key(rust_variant) => Ok(typescript_kind),
        _ => Err(Error::Invalid(format!(
            "unsupported Machines policy variant; expected {rust_variant}"
        ))),
    }
}

fn required_field<'a>(
    value: &'a serde_json::Value,
    name: &str,
) -> Result<&'a serde_json::Value, Error> {
    value
        .get(name)
        .ok_or_else(|| Error::Invalid(format!("scenario output has no {name} field")))
}

fn policy_kind(
    value: &serde_json::Value,
    field: &str,
    rust_variant: &str,
    typescript_kind: &'static str,
) -> Result<&'static str, Error> {
    match value.get(field).and_then(serde_json::Value::as_str) {
        Some(value) if value == rust_variant => Ok(typescript_kind),
        Some(value) => Err(Error::Invalid(format!(
            "unsupported Machines {field} policy variant {value}"
        ))),
        None => Err(Error::Invalid(format!(
            "Machines request field {field} is not a Rust enum variant"
        ))),
    }
}

fn string_field(value: &serde_json::Value, name: &str) -> Result<String, Error> {
    value
        .get(name)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Error::Invalid(format!("scenario request field {name} is not a string")))
}

fn number_field(value: &serde_json::Value, name: &str) -> Result<u64, Error> {
    value
        .get(name)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::Invalid(format!("scenario request field {name} is not a u64")))
}

fn bytes_hex(value: &serde_json::Value) -> Result<String, Error> {
    let bytes = value
        .as_array()
        .ok_or_else(|| Error::Invalid("scenario byte field is not an array".into()))?;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let byte = byte
            .as_u64()
            .filter(|byte| *byte <= u8::MAX as u64)
            .ok_or_else(|| {
                Error::Invalid("scenario byte field contains an invalid value".into())
            })?;
        result.push_str(&format!("{byte:02x}"));
    }
    Ok(result)
}

pub(super) fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}
