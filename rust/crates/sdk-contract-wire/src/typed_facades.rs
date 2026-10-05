//! Rust-owned nominal type projections for JVM-family SDKs.
//!
//! These sources are emitted from `type_policy.rs`.  The generated wrappers
//! deliberately sit at the language boundary: protobuf messages remain the
//! wire representation, while consumers receive validated nominal values and
//! an open union that preserves values introduced by newer servers.

use std::collections::{BTreeMap, BTreeSet};

use crate::type_policy::{
    PUBLIC_FIELD_BINDINGS, PublicFieldBinding, PublicFieldDirection, SEMANTIC_TYPES, SemanticRule,
    SemanticType, WireValueKind, semantic_type,
};

pub const JAVA_PATH: &str = "jvm/src/main/java/dev/acyclic/transport/RustSemanticTypes.java";
pub const KOTLIN_PATH: &str = "jvm/src/main/kotlin/dev/acyclic/transport/RustSemanticTypes.kt";
pub const SCALA_PATH: &str = "jvm/src/main/scala/dev/acyclic/transport/RustSemanticTypes.scala";
pub const JAVA_REQUESTS_PATH: &str =
    "jvm/src/main/java/dev/acyclic/transport/RustTypedRequests.java";
pub const KOTLIN_REQUESTS_PATH: &str =
    "jvm/src/main/kotlin/dev/acyclic/transport/RustTypedRequests.kt";
pub const SCALA_REQUESTS_PATH: &str =
    "jvm/src/main/scala/dev/acyclic/transport/RustTypedRequests.scala";
pub const JAVA_CLIENTS_PATH: &str =
    "jvm/src/main/java/dev/acyclic/transport/RustTypedClients.java";
pub const KOTLIN_CLIENTS_PATH: &str =
    "jvm/src/main/kotlin/dev/acyclic/transport/RustTypedClients.kt";
pub const SCALA_CLIENTS_PATH: &str =
    "jvm/src/main/scala/dev/acyclic/transport/RustTypedClients.scala";
pub const JAVA_RESPONSES_PATH: &str =
    "jvm/src/main/java/dev/acyclic/transport/RustTypedResponses.java";
pub const KOTLIN_RESPONSES_PATH: &str =
    "jvm/src/main/kotlin/dev/acyclic/transport/RustTypedResponses.kt";
pub const SCALA_RESPONSES_PATH: &str =
    "jvm/src/main/scala/dev/acyclic/transport/RustTypedResponses.scala";

pub fn generate_jvm_semantic_types() -> Vec<(&'static str, String)> {
    vec![
        (JAVA_PATH, render_java()),
        (KOTLIN_PATH, render_kotlin()),
        (SCALA_PATH, render_scala()),
    ]
}

/// Emit public request factories from the Rust-owned field bindings.  The
/// factories are the actual SDK boundary: consumers pass nominal values and
/// the generated adapter performs the wire conversion before invoking the
/// protobuf builder.
pub fn generate_jvm_typed_requests() -> Vec<(&'static str, String)> {
    vec![
        (JAVA_REQUESTS_PATH, render_java_requests()),
        (KOTLIN_REQUESTS_PATH, render_kotlin_requests()),
        (SCALA_REQUESTS_PATH, render_scala_requests()),
    ]
}

/// Emit public client calls from the same Rust-owned request bindings.  The
/// generated methods accept nominal values, construct the protobuf request,
/// and immediately invoke the generated blocking stub.  This keeps callers
/// from opting out of the semantic policy by reaching for a raw request.
pub fn generate_jvm_typed_clients() -> Vec<(&'static str, String)> {
    vec![
        (JAVA_CLIENTS_PATH, render_java_clients()),
        (KOTLIN_CLIENTS_PATH, render_kotlin_clients()),
        (SCALA_CLIENTS_PATH, render_scala_clients()),
    ]
}

/// Emit response wrappers from the Rust-owned response and nested-field
/// bindings. A public client never exposes a raw protobuf response: the
/// wrapper keeps the lossless wire value while projecting constrained fields
/// into the same nominal types used by requests.
pub fn generate_jvm_typed_responses() -> Vec<(&'static str, String)> {
    vec![
        (JAVA_RESPONSES_PATH, render_java_responses()),
        (KOTLIN_RESPONSES_PATH, render_kotlin_responses()),
        (SCALA_RESPONSES_PATH, render_scala_responses()),
    ]
}

fn request_groups() -> Vec<(&'static str, &'static str, Vec<&'static PublicFieldBinding>)> {
    let mut groups: BTreeMap<(&'static str, &'static str), Vec<&'static PublicFieldBinding>> =
        BTreeMap::new();
    for binding in PUBLIC_FIELD_BINDINGS {
        if binding.direction == PublicFieldDirection::Request {
            groups
                .entry((binding.module, binding.message))
                .or_default()
                .push(binding);
        }
    }
    groups
        .into_iter()
        .map(|((module, message), fields)| (module, message, fields))
        .collect()
}

fn java_proto_container(module: &str) -> &'static str {
    match module {
        "actors" => "acyclic.actors.v1.Actors",
        "workers" => "acyclic.workers.v1.Workers",
        "stream" => "acyclic.stream.v2.Stream",
        "objects" => "acyclic.objects.v2.Objects",
        "inference" => "inference.customer.v1.Inference",
        "machines" => "acyclic.machines.v1.Machines",
        "filesystem" => "acyclic.filesystem.v2.Filesystem",
        "harness" => "acyclic.harness.v2.Harness",
        _ => panic!("missing JVM protobuf container for Rust module {module}"),
    }
}

fn upper_camel(value: &str) -> String {
    value
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn request_method_name(module: &str, message: &str) -> String {
    let base = message.strip_suffix("Request").unwrap_or(message);
    format!("{}{}", module.replace('.', ""), base)
}

fn rpc_method_name(rpc: &str) -> String {
    let mut chars = rpc.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn client_service(module: &str, message: &str) -> &'static str {
    match (module, message) {
        ("actors", _) => "acyclic.actors.v1.ActorsServiceGrpc.ActorsServiceBlockingStub",
        ("workers", _) => "acyclic.workers.v1.WorkersServiceGrpc.WorkersServiceBlockingStub",
        ("stream", _) => "acyclic.stream.v2.StreamServiceGrpc.StreamServiceBlockingStub",
        ("objects", "ListPartsRequest") => {
            "acyclic.objects.v2.MultipartServiceGrpc.MultipartServiceBlockingStub"
        }
        ("objects", _) => "acyclic.objects.v2.ObjectsServiceGrpc.ObjectsServiceBlockingStub",
        ("inference", "InspectRunRequest") => {
            "inference.customer.v1.RunsServiceGrpc.RunsServiceBlockingStub"
        }
        ("inference", "InspectContextRequest") => {
            "inference.customer.v1.ContextsServiceGrpc.ContextsServiceBlockingStub"
        }
        ("inference", "InspectWarmRequest") => {
            "inference.customer.v1.WarmContextsServiceGrpc.WarmContextsServiceBlockingStub"
        }
        ("inference", "InspectEvaluationRequest") => {
            "inference.customer.v1.EvaluationsServiceGrpc.EvaluationsServiceBlockingStub"
        }
        ("machines", _) => "acyclic.machines.v1.MachinesServiceGrpc.MachinesServiceBlockingStub",
        ("filesystem", _) => {
            "acyclic.filesystem.v2.FilesystemServiceGrpc.FilesystemServiceBlockingStub"
        }
        _ => panic!("missing JVM gRPC service for {module}.{message}"),
    }
}

fn client_response(module: &str, message: &str) -> (&'static str, bool) {
    match (module, message) {
        ("actors", "InvokeActorRequest") => ("acyclic.actors.v1.Actors.InvokeActorResponse", false),
        ("workers", "SelectDeploymentRequest") => (
            "acyclic.workers.v1.Workers.SelectDeploymentResponse",
            false,
        ),
        ("workers", "InspectJobRequest") => ("acyclic.workers.v1.Workers.InspectJobResponse", false),
        ("workers", "InvokeVersionRequest") => ("acyclic.workers.v1.Workers.InvokeResponse", false),
        ("stream", "AppendRequest") => ("acyclic.stream.v2.Stream.AppendResponse", false),
        ("stream", "ForkRequest") => ("acyclic.stream.v2.Stream.ForkReceipt", false),
        ("stream", "ReadRequest") => ("acyclic.stream.v2.Stream.ReadResponse", true),
        ("stream", "ReadCommitRequest") => ("acyclic.stream.v2.Stream.CommittedEnvelope", false),
        ("objects", "GetObjectRequest") => ("acyclic.objects.v2.Objects.GetObjectResponse", true),
        ("objects", "ListObjectsRequest") => ("acyclic.objects.v2.Objects.ListObjectsResponse", false),
        ("objects", "ListPartsRequest") => ("acyclic.objects.v2.Objects.ListPartsResponse", false),
        ("inference", "InspectRunRequest") => ("inference.customer.v1.Inference.RunView", false),
        ("inference", "InspectContextRequest") => ("inference.customer.v1.Inference.ContextView", false),
        ("inference", "InspectWarmRequest") => ("inference.customer.v1.Inference.WarmView", false),
        ("inference", "InspectEvaluationRequest") => {
            ("inference.customer.v1.Inference.EvaluationView", false)
        }
        ("machines", "CreateMachineRequest") => ("acyclic.machines.v1.Machines.MachineAdmission", false),
        ("machines", "InspectMachineRequest") => ("acyclic.machines.v1.Machines.MachineState", false),
        ("machines", "InspectCheckpointRequest") => ("acyclic.machines.v1.Machines.CheckpointState", false),
        ("machines", "OperationRequest") => ("acyclic.machines.v1.Machines.OperationState", false),
        ("machines", "ListMachinesRequest") => ("acyclic.machines.v1.Machines.MachinePage", false),
        ("filesystem", "ReadRequest") => ("acyclic.filesystem.v2.Filesystem.ReadResponse", false),
        _ => panic!("missing JVM response mapping for {module}.{message}"),
    }
}

fn render_java_responses() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Response projections and nested semantic decoding originate in Rust type_policy.rs.\npackage dev.acyclic.transport;\n\nimport java.util.Iterator;\nimport java.util.NoSuchElementException;\nimport java.util.function.Function;\n\npublic final class RustTypedResponses {\n  private RustTypedResponses() {}\n  public static <W,T> Iterator<T> mapIterator(Iterator<W> wire, Function<W,T> mapper) {\n    return new Iterator<>() { public boolean hasNext() { return wire.hasNext(); } public T next() { if (!hasNext()) throw new NoSuchElementException(); return mapper.apply(wire.next()); } };\n  }\n\n",
    );
    for (module, message, _fields) in request_groups() {
        let (wire, _streaming) = client_response(module, message);
        let name = response_wrapper_name(module, message);
        out.push_str("  public record ");
        out.push_str(&name);
        out.push_str("(");
        out.push_str(wire);
        out.push_str(" value) { public static ");
        out.push_str(&name);
        out.push_str(" fromWire(");
        out.push_str(wire);
        out.push_str(" value) { return new ");
        out.push_str(&name);
        out.push_str("(value); } public ");
        out.push_str(wire);
        out.push_str(" toWire() { return value; } }\n\n");
    }
    out.push_str("  public static RustSemanticTypes.IdempotencyKeyText mutationIdentityIdempotencyKey(acyclic.objects.v2.Objects.MutationIdentity value) { return RustSemanticTypes.IdempotencyKeyText.of(value.getIdempotencyKey()); }\n");
    out.push_str("  public static RustSemanticTypes.Sha256Digest evaluationSpecDigest(inference.customer.v1.Inference.EvaluationSpec value) { return RustSemanticTypes.Sha256Digest.of(value.getSpecDigest()); }\n");
    out.push_str("  public static RustSemanticTypes.ResourcePath fileRefPath(acyclic.harness.v2.Harness.FileRef value) { return RustSemanticTypes.ResourcePath.of(value.getNormalizedPath()); }\n");
    out.push_str("  public static RustSemanticTypes.WireChoice preserveKnown(String tag, com.google.protobuf.ByteString payload) { return new RustSemanticTypes.Known(tag, payload); }\n");
    out.push_str("  public static RustSemanticTypes.WireChoice preserveUnknown(int tag, com.google.protobuf.ByteString payload) { return new RustSemanticTypes.Unknown(tag, payload); }\n");
    out.push_str("  public static RustSemanticTypes.WireChoice preserveOneof(int tag, String knownTag, com.google.protobuf.ByteString payload) { return tag == 0 ? preserveKnown(knownTag, payload) : preserveUnknown(tag, payload); }\n\n");
    out.push_str("}\n");
    out
}

fn render_kotlin_responses() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Response projections and nested semantic decoding originate in Rust type_policy.rs.\npackage dev.acyclic.transport\n\n/** Rust-owned response wrappers. */\nobject RustTypedResponsesKotlin {\n  fun <W,T> mapIterator(wire: kotlin.collections.Iterator<W>, mapper: (W) -> T): kotlin.collections.Iterator<T> = object : kotlin.collections.Iterator<T> { override fun hasNext() = wire.hasNext(); override fun next(): T = mapper(wire.next()) }\n\n",
    );
    for (module, message, _fields) in request_groups() {
        let (wire, _streaming) = client_response(module, message);
        let name = response_wrapper_name(module, message);
        out.push_str("  data class ");
        out.push_str(&name);
        out.push_str("(val value: ");
        out.push_str(wire);
        out.push_str(") { companion object { fun fromWire(value: ");
        out.push_str(wire);
        out.push_str("): ");
        out.push_str(&name);
        out.push_str(" = ");
        out.push_str(&name);
        out.push_str("(value) } fun toWire(): ");
        out.push_str(wire);
        out.push_str(" = value }\n\n");
    }
    out.push_str("  fun mutationIdentityIdempotencyKey(value: acyclic.objects.v2.Objects.MutationIdentity): RustSemanticTypesKotlin.IdempotencyKeyText = RustSemanticTypesKotlin.IdempotencyKeyText.of(value.idempotencyKey)\n");
    out.push_str("  fun evaluationSpecDigest(value: inference.customer.v1.Inference.EvaluationSpec): RustSemanticTypesKotlin.Sha256Digest = RustSemanticTypesKotlin.Sha256Digest.of(value.specDigest)\n");
    out.push_str("  fun fileRefPath(value: acyclic.harness.v2.Harness.FileRef): RustSemanticTypesKotlin.ResourcePath = RustSemanticTypesKotlin.ResourcePath.of(value.normalizedPath)\n");
    out.push_str("  fun preserveKnown(tag: String, payload: com.google.protobuf.ByteString): RustSemanticTypesKotlin.WireChoice = RustSemanticTypesKotlin.Known(tag, payload)\n");
    out.push_str("  fun preserveUnknown(tag: Int, payload: com.google.protobuf.ByteString): RustSemanticTypesKotlin.WireChoice = RustSemanticTypesKotlin.Unknown(tag, payload)\n");
    out.push_str("  fun preserveOneof(tag: Int, knownTag: String, payload: com.google.protobuf.ByteString): RustSemanticTypesKotlin.WireChoice = if (tag == 0) preserveKnown(knownTag, payload) else preserveUnknown(tag, payload)\n\n");
    out.push_str("}\n");
    out
}

fn render_scala_responses() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Response projections and nested semantic decoding originate in Rust type_policy.rs.\npackage dev.acyclic.transport\n\n/** Rust-owned response wrappers. */\nobject RustTypedResponsesScala {\n  def mapIterator[W,T](wire: java.util.Iterator[W], mapper: W => T): java.util.Iterator[T] = new java.util.Iterator[T] { def hasNext: Boolean = wire.hasNext; def next(): T = mapper(wire.next()) }\n\n",
    );
    for (module, message, _fields) in request_groups() {
        let (wire, _streaming) = client_response(module, message);
        let name = response_wrapper_name(module, message);
        out.push_str("  final case class ");
        out.push_str(&name);
        out.push_str("(value: ");
        out.push_str(wire);
        out.push_str(") { def toWire: ");
        out.push_str(wire);
        out.push_str(" = value }\n");
        out.push_str("  object ");
        out.push_str(&name);
        out.push_str(" { def fromWire(value: ");
        out.push_str(wire);
        out.push_str("): ");
        out.push_str(&name);
        out.push_str(" = ");
        out.push_str(&name);
        out.push_str("(value) }\n\n");
    }
    out.push_str("  def mutationIdentityIdempotencyKey(value: acyclic.objects.v2.Objects.MutationIdentity): RustSemanticTypesScala.IdempotencyKeyText = RustSemanticTypesScala.IdempotencyKeyText.from(value.getIdempotencyKey).toOption.get\n");
    out.push_str("  def evaluationSpecDigest(value: inference.customer.v1.Inference.EvaluationSpec): RustSemanticTypesScala.Sha256Digest = RustSemanticTypesScala.Sha256Digest.from(value.getSpecDigest).toOption.get\n");
    out.push_str("  def fileRefPath(value: acyclic.harness.v2.Harness.FileRef): RustSemanticTypesScala.ResourcePath = RustSemanticTypesScala.ResourcePath.from(value.getNormalizedPath).toOption.get\n");
    out.push_str("  def preserveKnown(tag: String, payload: Array[Byte]): RustSemanticTypesScala.WireChoice = RustSemanticTypesScala.Known(tag, payload)\n");
    out.push_str("  def preserveUnknown(tag: Int, payload: Array[Byte]): RustSemanticTypesScala.WireChoice = RustSemanticTypesScala.Unknown(tag, payload)\n");
    out.push_str("  def preserveOneof(tag: Int, knownTag: String, payload: Array[Byte]): RustSemanticTypesScala.WireChoice = if (tag == 0) preserveKnown(knownTag, payload) else preserveUnknown(tag, payload)\n\n");
    out.push_str("}\n");
    out
}

fn response_wrapper_name(module: &str, message: &str) -> String {
    format!(
        "{}{}Response",
        upper_camel(module),
        upper_camel(message.strip_suffix("Request").unwrap_or(message))
    )
}

fn response_wrapper_type(module: &str, message: &str) -> String {
    format!("RustTypedResponses.{}", response_wrapper_name(module, message))
}

fn render_java_clients() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Client signatures and request construction originate in Rust type_policy.rs.\npackage dev.acyclic.transport;\n\n/** Typed blocking client calls generated from Rust-owned request bindings. */\npublic final class RustTypedClients {\n  private RustTypedClients() {}\n\n",
    );
    for (module, message, fields) in request_groups() {
        let method = request_method_name(module, message);
        let rpc = fields[0].rpc().expect("every request binding has an RPC");
        let rpc_method = rpc_method_name(rpc);
        let service = client_service(module, message);
        let (_response, streaming) = client_response(module, message);
        let wrapper = response_wrapper_type(module, message);
        let result = if streaming {
            format!("java.util.Iterator<{wrapper}>")
        } else {
            wrapper.clone()
        };
        out.push_str("  public static ");
        out.push_str(&result);
        out.push(' ');
        out.push_str(&method);
        out.push('(');
        out.push_str(service);
        out.push_str(" stub");
        for binding in &fields {
            let ty = semantic_for(binding);
            out.push_str(", RustSemanticTypes.");
            out.push_str(ty.rust_name);
            out.push(' ');
            out.push_str(binding.field);
        }
        out.push_str(") {\n    var wire = stub.");
        out.push_str(&rpc_method);
        out.push_str("(RustTypedRequests.");
        out.push_str(&method);
        out.push('(');
        for (index, binding) in fields.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            out.push_str(binding.field);
        }
        out.push_str("));\n");
        if streaming {
            out.push_str("    return RustTypedResponses.mapIterator(wire, ");
            out.push_str(&wrapper);
            out.push_str("::fromWire);\n  }\n\n");
        } else {
            out.push_str("    return ");
            out.push_str(&wrapper);
            out.push_str(".fromWire(wire);\n  }\n\n");
        }
    }
    out.push_str("}\n");
    out
}

fn render_kotlin_clients() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Client signatures and request construction originate in Rust type_policy.rs.\npackage dev.acyclic.transport\n\n/** Typed blocking client calls generated from Rust-owned request bindings. */\nobject RustTypedClientsKotlin {\n\n",
    );
    for (module, message, fields) in request_groups() {
        let method = request_method_name(module, message);
        let rpc = fields[0].rpc().expect("every request binding has an RPC");
        let rpc_method = rpc_method_name(rpc);
        let service = client_service(module, message);
        let (_response, streaming) = client_response(module, message);
        let wrapper_name = response_wrapper_name(module, message);
        let result = if streaming {
            format!("kotlin.collections.Iterator<RustTypedResponsesKotlin.{wrapper_name}>")
        } else {
            format!("RustTypedResponsesKotlin.{wrapper_name}")
        };
        out.push_str("  fun ");
        out.push_str(&method);
        out.push_str("(stub: ");
        out.push_str(service);

        for binding in &fields {
            let ty = semantic_for(binding);
            out.push_str(", ");
            out.push_str(binding.field);
            out.push_str(": RustSemanticTypesKotlin.");
            out.push_str(ty.rust_name);
        }
        out.push_str("): ");
        out.push_str(&result);
        out.push_str(" = ");
        if streaming {
            out.push_str("RustTypedResponsesKotlin.mapIterator(stub.");
        } else {
            out.push_str("RustTypedResponsesKotlin.");
            out.push_str(&wrapper_name);
            out.push_str(".fromWire(stub.");
        }
        out.push_str(&rpc_method);
        out.push_str("(RustTypedRequestsKotlin.");
        out.push_str(&method);
        out.push('(');
        for (index, binding) in fields.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            out.push_str(binding.field);
        }
        if streaming {
            out.push_str("))) { RustTypedResponsesKotlin.");
            out.push_str(&wrapper_name);
            out.push_str(".fromWire(it) }\n\n");
        } else {
            out.push_str(")))\n\n");
        }
    }
    out.push_str("}\n");
    out
}

fn render_scala_clients() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Client signatures and request construction originate in Rust type_policy.rs.\npackage dev.acyclic.transport\n\n/** Typed blocking client calls generated from Rust-owned request bindings. */\nobject RustTypedClientsScala {\n\n",
    );
    for (module, message, fields) in request_groups() {
        let method = request_method_name(module, message);
        let rpc = fields[0].rpc().expect("every request binding has an RPC");
        let rpc_method = rpc_method_name(rpc);
        let service = client_service(module, message);
        let (_response, streaming) = client_response(module, message);
        let wrapper_name = response_wrapper_name(module, message);
        let result = if streaming {
            format!("java.util.Iterator[RustTypedResponsesScala.{wrapper_name}]")
        } else {
            format!("RustTypedResponsesScala.{wrapper_name}")
        };
        out.push_str("  def ");
        out.push_str(&method);
        out.push_str("(stub: ");
        out.push_str(service);

        for binding in &fields {
            let ty = semantic_for(binding);
            out.push_str(", ");
            out.push_str(binding.field);
            out.push_str(": RustSemanticTypesScala.");
            out.push_str(ty.rust_name);
        }
        out.push_str("): ");
        out.push_str(&result);
        out.push_str(" = ");
        if streaming {
            out.push_str("RustTypedResponsesScala.mapIterator(stub.");
        } else {
            out.push_str("RustTypedResponsesScala.");
            out.push_str(&wrapper_name);
            out.push_str(".fromWire(stub.");
        }
        out.push_str(&rpc_method);
        out.push_str("(RustTypedRequestsScala.");
        out.push_str(&method);
        out.push('(');
        for (index, binding) in fields.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            out.push_str(binding.field);
        }
        if streaming {
            out.push_str(")))(RustTypedResponsesScala.");
            out.push_str(&wrapper_name);
            out.push_str(".fromWire)\n\n");
        } else {
            out.push_str("))) }\n\n");
        }
    }
    out.push_str("}\n");
    out
}

fn semantic_for(binding: &PublicFieldBinding) -> &'static SemanticType {
    semantic_type(binding.semantic_type)
        .expect("every public binding resolves to a Rust semantic type")
}

fn java_value_expression(binding: &PublicFieldBinding, parameter: &str) -> String {
    let ty = semantic_for(binding);
    if binding.module == "machines"
        && matches!(binding.wire_field, "machine" | "checkpoint" | "operation")
    {
        let message_type = match binding.wire_field {
            "machine" => "MachineId",
            "checkpoint" => "CheckpointId",
            "operation" => "OperationId",
            _ => unreachable!(),
        };
        return format!(
            "acyclic.machines.v1.Machines.{message_type}.newBuilder().setValue(com.google.protobuf.ByteString.copyFrom({parameter}.toWire(), java.nio.charset.StandardCharsets.UTF_8)).build()"
        );
    }
    match ty.wire_kind {
        WireValueKind::Message => {
            let container = java_proto_container(binding.module);
            let message_type = match ty.rust_name {
                "Image" => format!("{container}.Image"),
                "IdempotencyKey" => format!("{container}.IdempotencyKey"),
                _ => panic!("missing message wire projection for {}", ty.rust_name),
            };
            format!("({message_type}) {parameter}.toWire()")
        }
        WireValueKind::UnsignedInteger if ty.rust_name == "PageLimit" => {
            format!("Math.toIntExact({parameter}.toWire())")
        }
        _ => format!("{parameter}.toWire()"),
    }
}

fn kotlin_value_expression(binding: &PublicFieldBinding, parameter: &str) -> String {
    let ty = semantic_for(binding);
    if binding.module == "machines"
        && matches!(binding.wire_field, "machine" | "checkpoint" | "operation")
    {
        let message_type = match binding.wire_field {
            "machine" => "MachineId",
            "checkpoint" => "CheckpointId",
            "operation" => "OperationId",
            _ => unreachable!(),
        };
        return format!(
            "acyclic.machines.v1.Machines.{message_type}.newBuilder().setValue(com.google.protobuf.ByteString.copyFrom({parameter}.toWire(), kotlin.text.Charsets.UTF_8)).build()"
        );
    }
    match ty.wire_kind {
        WireValueKind::Message => {
            let container = java_proto_container(binding.module);
            let message_type = match ty.rust_name {
                "Image" => format!("{container}.Image"),
                "IdempotencyKey" => format!("{container}.IdempotencyKey"),
                _ => panic!("missing message wire projection for {}", ty.rust_name),
            };
            format!("({parameter}.toWire() as {message_type})")
        }
        WireValueKind::UnsignedInteger if ty.rust_name == "PageLimit" => {
            format!("{parameter}.toWire().toInt()")
        }
        _ => format!("{parameter}.toWire()"),
    }
}

fn scala_value_expression(binding: &PublicFieldBinding, parameter: &str) -> String {
    let ty = semantic_for(binding);
    if binding.module == "machines"
        && matches!(binding.wire_field, "machine" | "checkpoint" | "operation")
    {
        let message_type = match binding.wire_field {
            "machine" => "MachineId",
            "checkpoint" => "CheckpointId",
            "operation" => "OperationId",
            _ => unreachable!(),
        };
        return format!(
            "acyclic.machines.v1.Machines.{message_type}.newBuilder().setValue(com.google.protobuf.ByteString.copyFrom({parameter}.toWire.getBytes(java.nio.charset.StandardCharsets.UTF_8))).build()"
        );
    }
    match ty.wire_kind {
        WireValueKind::Message => {
            let container = java_proto_container(binding.module);
            let message_type = match ty.rust_name {
                "Image" => format!("{container}.Image"),
                "IdempotencyKey" => format!("{container}.IdempotencyKey"),
                _ => panic!("missing message wire projection for {}", ty.rust_name),
            };
            format!("{parameter}.toWire.asInstanceOf[{message_type}]")
        }
        WireValueKind::UnsignedInteger if ty.rust_name == "PageLimit" => {
            format!("{parameter}.toWire.toInt")
        }
        _ => format!("{parameter}.toWire"),
    }
}

fn unique_types() -> impl Iterator<Item = &'static SemanticType> {
    let mut seen = BTreeSet::new();
    SEMANTIC_TYPES
        .iter()
        .filter(move |ty| seen.insert(ty.rust_name))
}

fn java_wire_type(ty: &SemanticType) -> &'static str {
    match ty.wire_kind {
        WireValueKind::String => "String",
        WireValueKind::Bytes => "com.google.protobuf.ByteString",
        WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => "long",
        WireValueKind::Boolean => "boolean",
        WireValueKind::Timestamp => "java.time.Instant",
        WireValueKind::Enum => "int",
        WireValueKind::Message => "com.google.protobuf.Message",
        WireValueKind::Oneof => "WireChoice",
    }
}

fn kotlin_wire_type(ty: &SemanticType) -> &'static str {
    match ty.wire_kind {
        WireValueKind::String => "String",
        WireValueKind::Bytes => "com.google.protobuf.ByteString",
        WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => "Long",
        WireValueKind::Boolean => "Boolean",
        WireValueKind::Timestamp => "java.time.Instant",
        WireValueKind::Enum => "Int",
        WireValueKind::Message => "com.google.protobuf.Message",
        WireValueKind::Oneof => "WireChoice",
    }
}

fn scala_wire_type(ty: &SemanticType) -> &'static str {
    match ty.wire_kind {
        WireValueKind::String => "String",
        WireValueKind::Bytes => "Array[Byte]",
        WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => "Long",
        WireValueKind::Boolean => "Boolean",
        WireValueKind::Timestamp => "java.time.Instant",
        WireValueKind::Enum => "Int",
        WireValueKind::Message => "Array[Byte]",
        WireValueKind::Oneof => "WireChoice",
    }
}

fn java_validation(ty: &SemanticType, value: &str) -> String {
    let mut lines = Vec::new();
    for rule in ty.rules {
        match rule {
            SemanticRule::NonEmpty | SemanticRule::Utf8 if ty.wire_kind == WireValueKind::String => {
                lines.push(format!("if ({value} == null || {value}.isEmpty()) throw new IllegalArgumentException(\"{} must be non-empty\");", ty.rust_name));
            }
            SemanticRule::NonEmpty if ty.wire_kind == WireValueKind::Bytes => {
                lines.push(format!("if ({value} == null || {value}.isEmpty()) throw new IllegalArgumentException(\"{} must be non-empty\");", ty.rust_name));
            }
            SemanticRule::FixedLength(n) if ty.wire_kind == WireValueKind::Bytes => {
                lines.push(format!("if ({value} == null || {value}.size() != {n}) throw new IllegalArgumentException(\"{} must contain exactly {n} bytes\");", ty.rust_name));
            }
            SemanticRule::NonNegative => lines.push(format!("if ({value} < 0) throw new IllegalArgumentException(\"{} must be non-negative\");", ty.rust_name)),
            SemanticRule::StrictlyPositive => lines.push(format!("if ({value} <= 0) throw new IllegalArgumentException(\"{} must be positive\");", ty.rust_name)),
            SemanticRule::MaxItems(n) => lines.push(format!("if ({value} > {n}) throw new IllegalArgumentException(\"{} exceeds its maximum\");", ty.rust_name)),
            _ => {}
        }
    }
    lines.join("; ")
}

fn kotlin_validation(ty: &SemanticType, value: &str) -> String {
    let mut lines = Vec::new();
    for rule in ty.rules {
        match rule {
            SemanticRule::NonEmpty | SemanticRule::Utf8
                if ty.wire_kind == WireValueKind::String =>
            {
                lines.push(format!(
                    "require({value}.isNotEmpty()) {{ \"{} must be non-empty\" }}",
                    ty.rust_name
                ));
            }
            SemanticRule::NonEmpty if ty.wire_kind == WireValueKind::Bytes => {
                lines.push(format!(
                    "require({value}.size() > 0) {{ \"{} must be non-empty\" }}",
                    ty.rust_name
                ));
            }
            SemanticRule::FixedLength(n) if ty.wire_kind == WireValueKind::Bytes => {
                lines.push(format!(
                    "require({value}.size() == {n}) {{ \"{} must contain exactly {n} bytes\" }}",
                    ty.rust_name
                ));
            }
            SemanticRule::NonNegative => lines.push(format!(
                "require({value} >= 0) {{ \"{} must be non-negative\" }}",
                ty.rust_name
            )),
            SemanticRule::StrictlyPositive => lines.push(format!(
                "require({value} > 0) {{ \"{} must be positive\" }}",
                ty.rust_name
            )),
            SemanticRule::MaxItems(n) => lines.push(format!(
                "require({value} <= {n}) {{ \"{} exceeds its maximum\" }}",
                ty.rust_name
            )),
            _ => {}
        }
    }
    lines.join("; ")
}

fn scala_validation(ty: &SemanticType, value: &str) -> String {
    let mut lines = Vec::new();
    for rule in ty.rules {
        match rule {
            SemanticRule::NonEmpty | SemanticRule::Utf8
                if ty.wire_kind == WireValueKind::String =>
            {
                lines.push(format!(
                    "require({value}.nonEmpty, \"{} must be non-empty\")",
                    ty.rust_name
                ));
            }
            SemanticRule::NonEmpty if ty.wire_kind == WireValueKind::Bytes => {
                lines.push(format!(
                    "require({value}.nonEmpty, \"{} must be non-empty\")",
                    ty.rust_name
                ));
            }
            SemanticRule::FixedLength(n) if ty.wire_kind == WireValueKind::Bytes => {
                lines.push(format!(
                    "require({value}.length == {n}, \"{} must contain exactly {n} bytes\")",
                    ty.rust_name
                ));
            }
            SemanticRule::NonNegative => lines.push(format!(
                "require({value} >= 0, \"{} must be non-negative\")",
                ty.rust_name
            )),
            SemanticRule::StrictlyPositive => lines.push(format!(
                "require({value} > 0, \"{} must be positive\")",
                ty.rust_name
            )),
            SemanticRule::MaxItems(n) => lines.push(format!(
                "require({value} <= {n}, \"{} exceeds its maximum\")",
                ty.rust_name
            )),
            _ => {}
        }
    }
    lines.join("; ")
}

fn render_java_requests() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Request signatures and wire conversions originate in Rust type_policy.rs.\npackage dev.acyclic.transport;\n\n/** Public typed request factories generated from Rust-owned field bindings. */\npublic final class RustTypedRequests {\n  private RustTypedRequests() {}\n\n",
    );
    for (module, message, fields) in request_groups() {
        let container = java_proto_container(module);
        let method = request_method_name(module, message);
        out.push_str("  public static ");
        out.push_str(container);
        out.push('.');
        out.push_str(message);
        out.push(' ');
        out.push_str(&method);
        out.push('(');
        for (index, binding) in fields.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            let ty = semantic_for(binding);
            out.push_str("RustSemanticTypes.");
            out.push_str(ty.rust_name);
            out.push(' ');
            out.push_str(binding.field);
        }
        out.push_str(") {\n    var builder = ");
        out.push_str(container);
        out.push('.');
        out.push_str(message);
        out.push_str(".newBuilder();\n");
        for binding in fields {
            out.push_str("    builder.set");
            out.push_str(&upper_camel(binding.wire_field));
            out.push('(');
            out.push_str(&java_value_expression(binding, binding.field));
            out.push_str(");\n");
        }
        out.push_str("    return builder.build();\n  }\n\n");
    }
    out.push_str("}\n");
    out
}

fn render_kotlin_requests() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Request signatures and wire conversions originate in Rust type_policy.rs.\npackage dev.acyclic.transport\n\n/** Public typed request factories generated from Rust-owned field bindings. */\nobject RustTypedRequestsKotlin {\n\n",
    );
    for (module, message, fields) in request_groups() {
        let container = java_proto_container(module);
        let method = request_method_name(module, message);
        out.push_str("  fun ");
        out.push_str(&method);
        out.push('(');
        for (index, binding) in fields.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            let ty = semantic_for(binding);
            out.push_str(binding.field);
            out.push_str(": RustSemanticTypesKotlin.");
            out.push_str(ty.rust_name);
        }
        out.push_str("): ");
        out.push_str(container);
        out.push('.');
        out.push_str(message);
        out.push_str(" = ");
        out.push_str(container);
        out.push('.');
        out.push_str(message);
        out.push_str(".newBuilder().apply {\n");
        for binding in fields {
            out.push_str("    set");
            out.push_str(&upper_camel(binding.wire_field));
            out.push('(');
            out.push_str(&kotlin_value_expression(binding, binding.field));
            out.push_str(")\n");
        }
        out.push_str("  }.build()\n\n");
    }
    out.push_str("}\n");
    out
}

fn render_scala_requests() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Request signatures and wire conversions originate in Rust type_policy.rs.\npackage dev.acyclic.transport\n\n/** Public typed request factories generated from Rust-owned field bindings. */\nobject RustTypedRequestsScala {\n\n",
    );
    for (module, message, fields) in request_groups() {
        let container = java_proto_container(module);
        let method = request_method_name(module, message);
        out.push_str("  def ");
        out.push_str(&method);
        out.push('(');
        for (index, binding) in fields.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            let ty = semantic_for(binding);
            out.push_str(binding.field);
            out.push_str(": RustSemanticTypesScala.");
            out.push_str(ty.rust_name);
        }
        out.push_str("): ");
        out.push_str(container);
        out.push('.');
        out.push_str(message);
        out.push_str(" = { val builder = ");
        out.push_str(container);
        out.push('.');
        out.push_str(message);
        out.push_str(".newBuilder()");
        for binding in fields {
            out.push_str("; builder.set");
            out.push_str(&upper_camel(binding.wire_field));
            out.push('(');
            out.push_str(&scala_value_expression(binding, binding.field));
            out.push(')');
        }
        out.push_str("; builder.build() }\n\n");
    }
    out.push_str("}\n");
    out
}

fn render_java() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Semantic intent and validation rules originate in rust/crates/sdk-contract-wire/src/type_policy.rs.\npackage dev.acyclic.transport;\n\nimport java.util.Optional;\n\n/** Rust-owned nominal values. Protobuf classes remain the wire boundary. */\npublic final class RustSemanticTypes {\n  private RustSemanticTypes() {}\n\n  public sealed interface WireChoice permits Known, Unknown {}\n  public record Known(String tag, com.google.protobuf.ByteString payload) implements WireChoice {}\n  public record Unknown(int tag, com.google.protobuf.ByteString payload) implements WireChoice {}\n  public static <T> Optional<T> present(T value, boolean isPresent) { return isPresent ? Optional.ofNullable(value) : Optional.empty(); }\n\n",
    );
    for ty in unique_types() {
        let wire = java_wire_type(ty);
        let validation = java_validation(ty, "value");
        if ty.wire_kind == WireValueKind::Oneof {
            // `WireChoice` is the shared open union itself.  It is emitted
            // above as a sealed interface; do not wrap it in a same-named
            // record when the Rust policy names the union type.
            if ty.rust_name == "WireChoice" {
                continue;
            }
            out.push_str("  public record ");
            out.push_str(ty.rust_name);
            out.push_str("(WireChoice value) { public static ");
            out.push_str(ty.rust_name);
            out.push_str(" of(WireChoice value) { return new ");
            out.push_str(ty.rust_name);
            out.push_str("(java.util.Objects.requireNonNull(value)); } }\n\n");
            continue;
        }
        out.push_str("  public record ");
        out.push_str(ty.rust_name);
        out.push('(');
        out.push_str(wire);
        out.push_str(" value) {\n    public ");
        out.push_str(ty.rust_name);
        out.push('(');
        out.push_str(wire);
        out.push_str(" value) { ");
        if wire == "String"
            || wire == "com.google.protobuf.ByteString"
            || wire == "com.google.protobuf.Message"
        {
            out.push_str("java.util.Objects.requireNonNull(value); ");
        }
        out.push_str(&validation);
        out.push_str(" this.value = value; }\n    public static ");
        out.push_str(ty.rust_name);
        out.push_str(" of(");
        out.push_str(wire);
        out.push_str(" value) { return new ");
        out.push_str(ty.rust_name);
        out.push_str("(value); }\n    public ");
        out.push_str(wire);
        out.push_str(" toWire() { return value; }\n  }\n\n");
    }
    out.push_str("}\n");
    out
}

fn render_kotlin() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Semantic intent and validation rules originate in Rust type_policy.rs.\npackage dev.acyclic.transport\n\nimport com.google.protobuf.ByteString\nimport java.util.Optional\n\n/** Rust-owned nominal values. Protobuf classes remain the wire boundary. */\nobject RustSemanticTypesKotlin {\n  sealed interface WireChoice\n  data class Known(val tag: String, val payload: ByteString) : WireChoice\n  data class Unknown(val tag: Int, val payload: ByteString) : WireChoice\n  fun <T: Any> present(value: T?, isPresent: Boolean): Optional<T> = if (isPresent && value != null) Optional.of(value) else Optional.empty()\n\n",
    );
    for ty in unique_types() {
        let wire = kotlin_wire_type(ty);
        let validation = kotlin_validation(ty, "value");
        if ty.wire_kind == WireValueKind::Oneof {
            if ty.rust_name == "WireChoice" {
                continue;
            }
            out.push_str("  @JvmInline value class ");
            out.push_str(ty.rust_name);
            out.push_str(" private constructor(val value: WireChoice) { companion object { fun of(value: WireChoice) = ");
            out.push_str(ty.rust_name);
            out.push_str("(value) } }\n\n");
            continue;
        }
        out.push_str("  @JvmInline value class ");
        out.push_str(ty.rust_name);
        out.push_str(" private constructor(val value: ");
        out.push_str(wire);
        out.push_str(") {\n    fun toWire(): ");
        out.push_str(wire);
        out.push_str(" = value\n    companion object { fun of(value: ");
        out.push_str(wire);
        out.push_str("): ");
        out.push_str(ty.rust_name);
        out.push_str(" { ");
        if !validation.is_empty() {
            out.push_str(&validation);
            out.push(';');
        }
        out.push_str(" return ");
        out.push_str(ty.rust_name);
        out.push_str("(value) } }\n  }\n\n");
    }
    out.push_str("}\n");
    out
}

fn render_scala() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Semantic intent and validation rules originate in Rust type_policy.rs.\npackage dev.acyclic.transport\n\n/** Rust-owned nominal values. Protobuf classes remain the wire boundary. */\nobject RustSemanticTypesScala {\n  sealed trait WireChoice\n  final case class Known(tag: String, payload: Array[Byte]) extends WireChoice\n  final case class Unknown(tag: Int, payload: Array[Byte]) extends WireChoice\n  def present[T](value: T, isPresent: Boolean): Option[T] = if (isPresent) Option(value) else None\n\n",
    );
    for ty in unique_types() {
        let wire = scala_wire_type(ty);
        let validation = scala_validation(ty, "value");
        if ty.wire_kind == WireValueKind::Oneof {
            if ty.rust_name == "WireChoice" {
                continue;
            }
            out.push_str("  final case class ");
            out.push_str(ty.rust_name);
            out.push_str(" private (value: WireChoice)\n  object ");
            out.push_str(ty.rust_name);
            out.push_str(" { def of(value: WireChoice): ");
            out.push_str(ty.rust_name);
            out.push_str(" = new ");
            out.push_str(ty.rust_name);
            out.push_str("(value) }\n\n");
            continue;
        }
        out.push_str("  final case class ");
        out.push_str(ty.rust_name);
        out.push_str(" private (value: ");
        out.push_str(wire);
        out.push_str(") { def toWire: ");
        out.push_str(wire);
        out.push_str(" = value }\n  object ");
        out.push_str(ty.rust_name);
        out.push_str(" { def from(value: ");
        out.push_str(wire);
        out.push_str("): Either[String, ");
        out.push_str(ty.rust_name);
        out.push_str("] = try { ");
        if !validation.is_empty() {
            out.push_str(&validation);
            out.push_str("; ");
        }
        out.push_str("Right(new ");
        out.push_str(ty.rust_name);
        out.push_str(
            "(value)) } catch { case e: IllegalArgumentException => Left(e.getMessage) } }\n\n",
        );
    }
    out.push_str("}\n");
    out
}
