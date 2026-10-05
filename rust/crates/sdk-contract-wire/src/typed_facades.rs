//! Rust-owned nominal type projections for JVM-family SDKs.
//!
//! These sources are emitted from `type_policy.rs`.  The generated wrappers
//! deliberately sit at the language boundary: protobuf messages remain the
//! wire representation, while consumers receive validated nominal values and
//! an open union that preserves values introduced by newer servers.

use std::collections::{BTreeMap, BTreeSet};

use crate::type_policy::{
    resolved_request_fields, resolved_response_fields, resolved_rpc_methods, PUBLIC_FIELD_BINDINGS,
    PublicFieldBinding, PublicFieldDirection, ResolvedRequestField, SEMANTIC_TYPES, SemanticRule,
    SemanticType, WireValueKind, semantic_type,
};
use prost_types::field_descriptor_proto::{Label as FieldLabel, Type as FieldType};

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
        "protocol" => "acyclic.protocol.v1.Protocol",
        "transport" => "acyclic.transport.v1.Transport",
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

fn descriptor_message_type(family: &str, message: &str) -> String {
    let outer = if message.starts_with("acyclic.protocol.v1.") {
        java_proto_container("protocol")
    } else if message.starts_with("acyclic.transport.v1.") {
        java_proto_container("transport")
    } else {
        java_proto_container(family)
    };
    let leaf = message.rsplit('.').next().unwrap_or(message);
    format!("{outer}.{leaf}")
}

fn descriptor_request_name(method: &crate::type_policy::ResolvedRpcMethod) -> String {
    format!(
        "{}{}{}Request",
        upper_camel(&method.family),
        upper_camel(method.service.trim_end_matches("Service")),
        upper_camel(&method.method)
    )
}

fn descriptor_response_name(method: &crate::type_policy::ResolvedRpcMethod) -> String {
    format!(
        "{}{}{}Response",
        upper_camel(&method.family),
        upper_camel(method.service.trim_end_matches("Service")),
        upper_camel(&method.method)
    )
}

fn descriptor_client_name(method: &crate::type_policy::ResolvedRpcMethod) -> String {
    format!(
        "{}{}{}",
        method.family,
        method.service.trim_end_matches("Service"),
        method.method
    )
}

fn java_grpc_service(family: &str, service: &str, async_stub: bool) -> String {
    let class = match family {
        "actors" => "acyclic.actors.v1.ActorsServiceGrpc",
        "workers" => "acyclic.workers.v1.WorkersServiceGrpc",
        "stream" => "acyclic.stream.v2.StreamServiceGrpc",
        "objects" => match service {
            "BucketsService" => "acyclic.objects.v2.BucketsServiceGrpc",
            "MultipartService" => "acyclic.objects.v2.MultipartServiceGrpc",
            _ => "acyclic.objects.v2.ObjectsServiceGrpc",
        },
        "inference" => match service {
            "ContextsService" => "inference.customer.v1.ContextsServiceGrpc",
            "EvaluationsService" => "inference.customer.v1.EvaluationsServiceGrpc",
            "ModelsService" => "inference.customer.v1.ModelsServiceGrpc",
            "RunsService" => "inference.customer.v1.RunsServiceGrpc",
            _ => "inference.customer.v1.WarmContextsServiceGrpc",
        },
        "machines" => "acyclic.machines.v1.MachinesServiceGrpc",
        "filesystem" => "acyclic.filesystem.v2.FilesystemServiceGrpc",
        "harness" => "acyclic.harness.v2.HarnessServiceGrpc",
        _ => panic!("missing JVM gRPC service for {family}.{service}"),
    };
    if async_stub {
        format!("{class}.{service}Stub")
    } else {
        format!("{class}.{service}BlockingStub")
    }
}

fn descriptor_method_descriptor(method: &crate::type_policy::ResolvedRpcMethod) -> String {
    let grpc = java_grpc_service(&method.family, &method.service, true);
    let class = grpc.rsplit_once('.').map(|(class, _)| class).unwrap_or(&grpc);
    format!("{class}.get{}Method()", method.method)
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
        ("machines", "EventsRequest") => ("acyclic.machines.v1.Machines.EventPage", false),
        ("machines", "QualifyImageRequest") => (
            "acyclic.machines.v1.Machines.ImageQualification",
            false,
        ),
        ("filesystem", "ReadRequest") => ("acyclic.filesystem.v2.Filesystem.ReadResponse", false),
        _ => panic!("missing JVM response mapping for {module}.{message}"),
    }
}

fn render_java_responses() -> String {
    let mut out = String::from(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// Response projections and nested semantic decoding originate in Rust type_policy.rs.\npackage dev.acyclic.transport;\n\nimport java.util.Iterator;\nimport java.util.NoSuchElementException;\nimport java.util.Optional;\nimport java.util.function.Function;\n\npublic final class RustTypedResponses {\n  private RustTypedResponses() {}\n  public static <W,T> Iterator<T> mapIterator(Iterator<W> wire, Function<W,T> mapper) {\n    return new Iterator<>() { public boolean hasNext() { return wire.hasNext(); } public T next() { if (!hasNext()) throw new NoSuchElementException(); return mapper.apply(wire.next()); } };\n  }\n\n",
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
        out.push_str(" value) { ");
        if module == "inference" && message == "InspectEvaluationRequest" {
            out.push_str("RustSemanticTypes.Sha256Digest.of(value.getSpec().getSpecDigest()); ");
        } else if module == "objects" && message == "GetObjectRequest" {
            out.push_str("if (value.getFrameCase() == acyclic.objects.v2.Objects.GetObjectResponse.FrameCase.HEADER && !value.getHeader().getObject().getEtag().isEmpty()) RustSemanticTypes.OpaqueText.of(value.getHeader().getObject().getEtag()); ");
        }
        out.push_str("return new ");
        out.push_str(&name);
        out.push_str("(value); } public ");
        out.push_str(wire);
        out.push_str(" toWire() { return value; }");
        if module == "objects" && message == "GetObjectRequest" {
            out.push_str(" public Optional<RustSemanticTypes.OpaqueText> etag() { if (value.getFrameCase() != acyclic.objects.v2.Objects.GetObjectResponse.FrameCase.HEADER) return Optional.empty(); String etag = value.getHeader().getObject().getEtag(); return etag.isEmpty() ? Optional.empty() : Optional.of(RustSemanticTypes.OpaqueText.of(etag)); }");
            out.push_str(" public RustSemanticTypes.WireChoice frameChoice() { return RustTypedResponses.frameChoice(value); }");
        }
        if module == "inference" && message == "InspectEvaluationRequest" {
            out.push_str(" public RustSemanticTypes.Sha256Digest specDigest() { return RustSemanticTypes.Sha256Digest.of(value.getSpec().getSpecDigest()); }");
        }
        out.push_str(" }\n\n");
    }
    render_java_descriptor_responses(&mut out);
    out.push_str("  public static RustSemanticTypes.IdempotencyKeyText mutationIdentityIdempotencyKey(acyclic.objects.v2.Objects.MutationIdentity value) { return RustSemanticTypes.IdempotencyKeyText.of(value.getIdempotencyKey()); }\n");
    out.push_str("  public static RustSemanticTypes.Sha256Digest evaluationSpecDigest(inference.customer.v1.Inference.EvaluationSpec value) { return RustSemanticTypes.Sha256Digest.of(value.getSpecDigest()); }\n");
    out.push_str("  public static RustSemanticTypes.ResourcePath fileRefPath(acyclic.harness.v2.Harness.FileRef value) { return RustSemanticTypes.ResourcePath.of(value.getNormalizedPath()); }\n");
    out.push_str("  public static RustSemanticTypes.WireChoice preserveKnown(String tag, com.google.protobuf.ByteString payload) { return new RustSemanticTypes.Known(tag, payload); }\n");
    out.push_str("  public static RustSemanticTypes.WireChoice preserveUnknown(int tag, com.google.protobuf.ByteString payload) { return new RustSemanticTypes.Unknown(tag, payload); }\n");
    out.push_str("  public static RustSemanticTypes.WireChoice preserveOneof(int tag, String knownTag, com.google.protobuf.ByteString payload) { return tag == 0 ? preserveKnown(knownTag, payload) : preserveUnknown(tag, payload); }\n");
    out.push_str("  public static RustSemanticTypes.WireChoice frameChoice(acyclic.objects.v2.Objects.GetObjectResponse value) { switch (value.getFrameCase()) { case HEADER: return preserveKnown(\"header\", value.getHeader().toByteString()); case BODY: return preserveKnown(\"body\", value.getBody()); case ERROR: return preserveKnown(\"error\", value.getError().toByteString()); default: for (var entry : value.getUnknownFields().asMap().entrySet()) { var fields = entry.getValue(); if (!fields.getLengthDelimitedList().isEmpty()) return preserveUnknown(entry.getKey(), fields.getLengthDelimitedList().get(0)); if (!fields.getVarintList().isEmpty()) return preserveUnknown(entry.getKey(), com.google.protobuf.ByteString.copyFromUtf8(Long.toString(fields.getVarintList().get(0)))); } return preserveUnknown(0, com.google.protobuf.ByteString.EMPTY); } }\n\n");
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
        if module == "inference" && message == "InspectEvaluationRequest" {
            out.push_str("run { RustSemanticTypesKotlin.Sha256Digest.of(value.spec.specDigest); ");
        } else if module == "objects" && message == "GetObjectRequest" {
            out.push_str("run { if (value.frameCase == acyclic.objects.v2.Objects.GetObjectResponse.FrameCase.HEADER && value.header.getObject().etag.isNotEmpty()) RustSemanticTypesKotlin.OpaqueText.of(value.header.getObject().etag); ");
        }
        out.push_str(&name);
        out.push_str("(value)");
        if (module == "inference" && message == "InspectEvaluationRequest") || (module == "objects" && message == "GetObjectRequest") {
            out.push_str(" }");
        }
        out.push_str(" } fun toWire(): ");
        out.push_str(wire);
        out.push_str(" = value; ");
        if module == "objects" && message == "GetObjectRequest" {
            out.push_str("fun etag(): RustSemanticTypesKotlin.OpaqueText? { if (value.frameCase != acyclic.objects.v2.Objects.GetObjectResponse.FrameCase.HEADER) return null; val etag = value.header.getObject().etag; return if (etag.isEmpty()) null else RustSemanticTypesKotlin.OpaqueText.of(etag) } fun frameChoice(): RustSemanticTypesKotlin.WireChoice = RustTypedResponsesKotlin.frameChoice(value)");
        }
        if module == "inference" && message == "InspectEvaluationRequest" {
            out.push_str("fun specDigest(): RustSemanticTypesKotlin.Sha256Digest = RustSemanticTypesKotlin.Sha256Digest.of(value.spec.specDigest)");
        }
        out.push_str("}\n\n");
    }
    render_kotlin_descriptor_responses(&mut out);
    out.push_str("  fun mutationIdentityIdempotencyKey(value: acyclic.objects.v2.Objects.MutationIdentity): RustSemanticTypesKotlin.IdempotencyKeyText = RustSemanticTypesKotlin.IdempotencyKeyText.of(value.idempotencyKey)\n");
    out.push_str("  fun evaluationSpecDigest(value: inference.customer.v1.Inference.EvaluationSpec): RustSemanticTypesKotlin.Sha256Digest = RustSemanticTypesKotlin.Sha256Digest.of(value.specDigest)\n");
    out.push_str("  fun fileRefPath(value: acyclic.harness.v2.Harness.FileRef): RustSemanticTypesKotlin.ResourcePath = RustSemanticTypesKotlin.ResourcePath.of(value.normalizedPath)\n");
    out.push_str("  fun preserveKnown(tag: String, payload: com.google.protobuf.ByteString): RustSemanticTypesKotlin.WireChoice = RustSemanticTypesKotlin.Known(tag, payload)\n");
    out.push_str("  fun preserveUnknown(tag: Int, payload: com.google.protobuf.ByteString): RustSemanticTypesKotlin.WireChoice = RustSemanticTypesKotlin.Unknown(tag, payload)\n");
    out.push_str("  fun preserveOneof(tag: Int, knownTag: String, payload: com.google.protobuf.ByteString): RustSemanticTypesKotlin.WireChoice = if (tag == 0) preserveKnown(knownTag, payload) else preserveUnknown(tag, payload)\n\n");    out.push_str("  fun frameChoice(value: acyclic.objects.v2.Objects.GetObjectResponse): RustSemanticTypesKotlin.WireChoice = when (value.frameCase) { acyclic.objects.v2.Objects.GetObjectResponse.FrameCase.HEADER -> preserveKnown(\"header\", value.header.toByteString()); acyclic.objects.v2.Objects.GetObjectResponse.FrameCase.BODY -> preserveKnown(\"body\", value.body); acyclic.objects.v2.Objects.GetObjectResponse.FrameCase.ERROR -> preserveKnown(\"error\", value.error.toByteString()); else -> { val entry = value.getUnknownFields().asMap().entries.firstOrNull(); if (entry != null && entry.value.getLengthDelimitedList().isNotEmpty()) preserveUnknown(entry.key, entry.value.getLengthDelimitedList().first()) else if (entry != null && entry.value.getVarintList().isNotEmpty()) preserveUnknown(entry.key, com.google.protobuf.ByteString.copyFromUtf8(entry.value.getVarintList().first().toString())) else preserveUnknown(0, com.google.protobuf.ByteString.EMPTY) } }\n\n");
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
        out.push_str(" = value; ");
        if module == "objects" && message == "GetObjectRequest" {
            out.push_str("def etag: Option[RustSemanticTypesScala.OpaqueText] = if (value.getFrameCase != acyclic.objects.v2.Objects.GetObjectResponse.FrameCase.HEADER) None else Option(RustSemanticTypesScala.OpaqueText.from(value.getHeader.getObject.getEtag).toOption).flatten; def frameChoice: RustSemanticTypesScala.WireChoice = RustTypedResponsesScala.frameChoice(value)");
        }
        if module == "inference" && message == "InspectEvaluationRequest" {
            out.push_str("def specDigest: RustSemanticTypesScala.Sha256Digest = RustSemanticTypesScala.Sha256Digest.from(value.getSpec.getSpecDigest.toByteArray).toOption.get");
        }
        out.push_str(" }\n");
        out.push_str("  object ");
        out.push_str(&name);
        out.push_str(" { def fromWire(value: ");
        out.push_str(wire);
        out.push_str("): ");
        out.push_str(&name);
        out.push_str(" = ");
        if module == "inference" && message == "InspectEvaluationRequest" {
            out.push_str("{ RustSemanticTypesScala.Sha256Digest.from(value.getSpec.getSpecDigest.toByteArray).toOption.get; ");
        } else if module == "objects" && message == "GetObjectRequest" {
            out.push_str("{ if (value.getFrameCase == acyclic.objects.v2.Objects.GetObjectResponse.FrameCase.HEADER && !value.getHeader.getObject.getEtag.isEmpty) RustSemanticTypesScala.OpaqueText.from(value.getHeader.getObject.getEtag).toOption.get; ");
        }
        out.push_str(&name);
        out.push_str("(value)");
        if (module == "inference" && message == "InspectEvaluationRequest") || (module == "objects" && message == "GetObjectRequest") {
            out.push_str(" }");
        }
        out.push_str(" }");
        out.push_str("\n\n");
    }
    render_scala_descriptor_responses(&mut out);
    out.push_str("  def mutationIdentityIdempotencyKey(value: acyclic.objects.v2.Objects.MutationIdentity): RustSemanticTypesScala.IdempotencyKeyText = RustSemanticTypesScala.IdempotencyKeyText.from(value.getIdempotencyKey).toOption.get\n");
    out.push_str("  def evaluationSpecDigest(value: inference.customer.v1.Inference.EvaluationSpec): RustSemanticTypesScala.Sha256Digest = RustSemanticTypesScala.Sha256Digest.from(value.getSpecDigest.toByteArray).toOption.get\n");
    out.push_str("  def fileRefPath(value: acyclic.harness.v2.Harness.FileRef): RustSemanticTypesScala.ResourcePath = RustSemanticTypesScala.ResourcePath.from(value.getNormalizedPath).toOption.get\n");
    out.push_str("  def preserveKnown(tag: String, payload: Array[Byte]): RustSemanticTypesScala.WireChoice = RustSemanticTypesScala.Known(tag, payload)\n");
    out.push_str("  def preserveUnknown(tag: Int, payload: Array[Byte]): RustSemanticTypesScala.WireChoice = RustSemanticTypesScala.Unknown(tag, payload)\n");
    out.push_str("  def preserveOneof(tag: Int, knownTag: String, payload: Array[Byte]): RustSemanticTypesScala.WireChoice = if (tag == 0) preserveKnown(knownTag, payload) else preserveUnknown(tag, payload)\n\n");    out.push_str("  def frameChoice(value: acyclic.objects.v2.Objects.GetObjectResponse): RustSemanticTypesScala.WireChoice = RustTypedResponses.frameChoice(value) match { case known: RustSemanticTypes.Known => RustSemanticTypesScala.Known(known.tag(), known.payload().toByteArray); case unknown: RustSemanticTypes.Unknown => RustSemanticTypesScala.Unknown(unknown.tag(), unknown.payload().toByteArray) }\n\n");
    out.push_str("}\n");
    out
}

/// Emit a lossless request/response DTO for every descriptor-resolved RPC.
/// Semantic refinements above remain the ergonomic overloads; these DTOs make
/// the complete Rust operation inventory available without a handwritten RPC
/// table or a raw protobuf return type.
fn descriptor_fields(method: &crate::type_policy::ResolvedRpcMethod, request: bool) -> Vec<ResolvedRequestField> {
    let fields = if request {
        resolved_request_fields().expect("Rust request fields must resolve before JVM generation")
    } else {
        resolved_response_fields().expect("Rust response fields must resolve before JVM generation")
    };
    fields
        .into_iter()
        .filter(|field| field.family == method.family && field.rpc == method.rpc)
        .collect()
}

fn descriptor_field_chain(fields: &[ResolvedRequestField], target: &ResolvedRequestField) -> Option<Vec<ResolvedRequestField>> {
    let mut current = target.message_path.clone();
    let mut chain = Vec::new();
    while current != target.root_message {
        let parent = fields.iter().find(|field| {
            field.message_path != current
                && field.type_name.as_deref().map(|name| name.trim_start_matches('.')) == Some(current.as_str())
        })?;
        if parent.label == Some(FieldLabel::Repeated as i32) {
            return None;
        }
        chain.push(parent.clone());
        current = parent.message_path.clone();
    }
    chain.reverse();
    Some(chain)
}

fn descriptor_projected_fields(method: &crate::type_policy::ResolvedRpcMethod, request: bool) -> Vec<(ResolvedRequestField, Vec<ResolvedRequestField>)> {
    let fields = descriptor_fields(method, request);
    fields
        .iter()
        .filter_map(|field| {
            if field.message_path == field.root_message || field.semantic_type.is_some() {
                descriptor_field_chain(&fields, field).map(|chain| (field.clone(), chain))
            } else {
                None
            }
        })
        .collect()
}

fn descriptor_field_suffix(field: &ResolvedRequestField, chain: &[ResolvedRequestField]) -> String {
    let mut suffix = String::new();
    for parent in chain {
        suffix.push_str(&upper_camel(&parent.field));
    }
    suffix.push_str(&upper_camel(&field.field));
    suffix
}

fn descriptor_semantic_type(field: &ResolvedRequestField) -> Option<&'static SemanticType> {
    let semantic = field.semantic_type.as_deref().and_then(semantic_type)?;
    let wire = field.wire_type.and_then(|kind| FieldType::try_from(kind).ok());
    let applies = match semantic.wire_kind {
        WireValueKind::Message => matches!(wire, Some(FieldType::Message | FieldType::Group)),
        WireValueKind::String => matches!(wire, Some(FieldType::String)),
        WireValueKind::Bytes => matches!(wire, Some(FieldType::Bytes))
            || (matches!(wire, Some(FieldType::Message | FieldType::Group))
                && matches!(semantic.rust_name, "MachineId" | "CheckpointId" | "OperationId")),
        WireValueKind::Boolean => matches!(wire, Some(FieldType::Bool)),
        WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => matches!(
            wire,
            Some(
                FieldType::Int32
                    | FieldType::Sint32
                    | FieldType::Sfixed32
                    | FieldType::Uint32
                    | FieldType::Fixed32
                    | FieldType::Int64
                    | FieldType::Sint64
                    | FieldType::Sfixed64
                    | FieldType::Uint64
                    | FieldType::Fixed64
            )
        ),
        WireValueKind::Enum => matches!(wire, Some(FieldType::Enum)),
        WireValueKind::Timestamp => matches!(wire, Some(FieldType::Message | FieldType::Group)),
        WireValueKind::Oneof => field.oneof_index.is_some(),
    };
    applies.then_some(semantic)
}

fn descriptor_field_type(family: &str, field: &ResolvedRequestField, language: &str) -> String {
    let semantic = descriptor_semantic_type(field);
    let repeated = field.label == Some(FieldLabel::Repeated as i32);
    let base = if let Some(semantic) = semantic {
        match language {
            "java" => format!("RustSemanticTypes.{}", semantic.rust_name),
            "kotlin" => format!("RustSemanticTypesKotlin.{}", semantic.rust_name),
            "scala" => format!("RustSemanticTypesScala.{}", semantic.rust_name),
            _ => unreachable!(),
        }
    } else {
        match field.wire_type.and_then(|kind| FieldType::try_from(kind).ok()) {
            Some(FieldType::String) => "String".to_owned(),
            Some(FieldType::Bytes) => "com.google.protobuf.ByteString".to_owned(),
            Some(FieldType::Bool) => match language { "scala" => "Boolean", _ => "boolean" }.to_owned(),
            Some(FieldType::Double) => "double".to_owned(),
            Some(FieldType::Float) => "float".to_owned(),
            Some(FieldType::Int32 | FieldType::Sint32 | FieldType::Sfixed32 | FieldType::Uint32 | FieldType::Fixed32 | FieldType::Enum) => match language { "scala" => "Int", _ => "int" }.to_owned(),
            Some(FieldType::Int64 | FieldType::Sint64 | FieldType::Sfixed64 | FieldType::Uint64 | FieldType::Fixed64) => match language { "scala" => "Long", _ => "long" }.to_owned(),
            Some(FieldType::Message | FieldType::Group) => {
                let type_name = field.type_name.as_deref().unwrap_or("com.google.protobuf.Message").trim_start_matches('.');
                if type_name.starts_with("google.protobuf.") {
                    format!("com.google.protobuf.{}", type_name.rsplit('.').next().unwrap_or("Message"))
                } else {
                    descriptor_message_type(family, type_name)
                }
            }
            _ => "com.google.protobuf.Message".to_owned(),
        }
    };
    if repeated {
        let boxed = if language == "java" {
            match base.as_str() {
                "int" => "Integer".to_owned(),
                "long" => "Long".to_owned(),
                "boolean" => "Boolean".to_owned(),
                "float" => "Float".to_owned(),
                "double" => "Double".to_owned(),
                other => other.to_owned(),
            }
        } else {
            base.clone()
        };
        match language {
            "java" => format!("java.util.List<{boxed}>") ,
            "kotlin" => format!("kotlin.collections.List<{boxed}>") ,
            "scala" => format!("java.util.List[{boxed}]") ,
            _ => unreachable!(),
        }
    } else {
        base
    }
}

fn descriptor_field_expression(field: &ResolvedRequestField, chain: &[ResolvedRequestField], receiver: &str) -> String {
    let mut expression = receiver.to_owned();
    for parent in chain {
        expression.push_str(".get");
        expression.push_str(&upper_camel(&parent.field));
        expression.push_str("()");
    }
    expression.push_str(".get");
    expression.push_str(&upper_camel(&field.field));
    if field.label == Some(FieldLabel::Repeated as i32) {
        expression.push_str("List");
    } else if field.wire_type == Some(FieldType::Enum as i32) {
        expression.push_str("Value");
    }
    expression.push_str("()");
    if field.semantic_type.is_some()
        && field.wire_type == Some(FieldType::Message as i32)
        && field.label != Some(FieldLabel::Repeated as i32)
    {
        expression.push_str(".getValue()");
    }
    expression
}

fn descriptor_field_has_expression(field: &ResolvedRequestField, receiver: &str) -> Option<String> {
    if field.label == Some(FieldLabel::Repeated as i32) {
        return None;
    }
    if field.oneof_index.is_some() || field.proto3_optional || matches!(field.wire_type, Some(x) if x == FieldType::Message as i32 || x == FieldType::Group as i32) {
        Some(format!("{receiver}.has{}()", upper_camel(&field.field)))
    } else {
        None
    }
}

fn descriptor_java_value(field: &ResolvedRequestField, chain: &[ResolvedRequestField]) -> String {
    let expression = descriptor_field_expression(field, chain, "value");
    if field.label == Some(FieldLabel::Repeated as i32) || descriptor_semantic_type(field).is_none() {
        expression
    } else {
        format!("RustSemanticTypes.{}.of({expression})", descriptor_semantic_type(field).unwrap().rust_name)
    }
}

fn descriptor_kotlin_value(field: &ResolvedRequestField, chain: &[ResolvedRequestField]) -> String {
    let expression = descriptor_field_expression(field, chain, "value");
    if field.label == Some(FieldLabel::Repeated as i32) || descriptor_semantic_type(field).is_none() {
        expression
    } else {
        format!("RustSemanticTypesKotlin.{}.of({expression})", descriptor_semantic_type(field).unwrap().rust_name)
    }
}

fn descriptor_scala_value(field: &ResolvedRequestField, chain: &[ResolvedRequestField]) -> String {
    let expression = descriptor_field_expression(field, chain, "value");
    if field.label == Some(FieldLabel::Repeated as i32) || descriptor_semantic_type(field).is_none() {
        expression
    } else {
        format!("RustSemanticTypesScala.{}.from({expression}).toOption.get", descriptor_semantic_type(field).unwrap().rust_name)
    }
}

fn render_java_descriptor_projection(out: &mut String, method: &crate::type_policy::ResolvedRpcMethod, request: bool) {
    for (field, chain) in descriptor_projected_fields(method, request) {
        let suffix = descriptor_field_suffix(&field, &chain);
        let method_name = suffix[..1].to_lowercase() + &suffix[1..];
        let ty = descriptor_field_type(&method.family, &field, "java");
        out.push_str(" public ");
        out.push_str(&ty);
        out.push(' ');
        out.push_str(&method_name);
        out.push_str("() { return ");
        out.push_str(&descriptor_java_value(&field, &chain));
        out.push_str("; }");
        if chain.is_empty() {
            if let Some(has) = descriptor_field_has_expression(&field, "value") {
                out.push_str(" public boolean has");
                out.push_str(&suffix);
                out.push_str("() { return ");
                out.push_str(&has);
                out.push_str("; }");
            }
        }
    }
}

fn render_kotlin_descriptor_projection(out: &mut String, method: &crate::type_policy::ResolvedRpcMethod, request: bool) {
    for (field, chain) in descriptor_projected_fields(method, request) {
        let suffix = descriptor_field_suffix(&field, &chain);
        let method_name = suffix[..1].to_lowercase() + &suffix[1..];
        out.push_str(" fun ");
        out.push_str(&method_name);
        out.push_str("(): ");
        out.push_str(&descriptor_field_type(&method.family, &field, "kotlin"));
        out.push_str(" = ");
        out.push_str(&descriptor_kotlin_value(&field, &chain));
        out.push(';');
        if chain.is_empty() {
            if let Some(has) = descriptor_field_has_expression(&field, "value") {
                out.push_str(" fun has");
                out.push_str(&suffix);
                out.push_str("(): Boolean = ");
                out.push_str(&has);
                out.push(';');
            }
        }
    }
}

fn render_scala_descriptor_projection(out: &mut String, method: &crate::type_policy::ResolvedRpcMethod, request: bool) {
    for (field, chain) in descriptor_projected_fields(method, request) {
        let suffix = descriptor_field_suffix(&field, &chain);
        let method_name = suffix[..1].to_lowercase() + &suffix[1..];
        out.push_str(" def ");
        out.push_str(&method_name);
        out.push_str(": ");
        out.push_str(&descriptor_field_type(&method.family, &field, "scala"));
        out.push_str(" = ");
        out.push_str(&descriptor_scala_value(&field, &chain));
        out.push(';');
        if chain.is_empty() {
            if let Some(has) = descriptor_field_has_expression(&field, "value") {
                out.push_str(" def has");
                out.push_str(&suffix);
                out.push_str(": Boolean = ");
                out.push_str(&has);
                out.push(';');
            }
        }
    }
}

fn render_java_descriptor_validation(out: &mut String, method: &crate::type_policy::ResolvedRpcMethod, request: bool) {
    for (field, chain) in descriptor_projected_fields(method, request) {
        if !chain.is_empty() || descriptor_semantic_type(&field).is_none() || field.label == Some(FieldLabel::Repeated as i32) {
            continue;
        }
        let value = descriptor_java_value(&field, &chain);
        if let Some(has) = descriptor_field_has_expression(&field, "value") {
            out.push_str("if (");
            out.push_str(&has);
            out.push_str(") ");
        }
        out.push_str(" ");
        out.push_str(&value);
        out.push_str(";");
    }
}

fn render_kotlin_descriptor_validation(out: &mut String, method: &crate::type_policy::ResolvedRpcMethod, request: bool) {
    for (field, chain) in descriptor_projected_fields(method, request) {
        if !chain.is_empty() || descriptor_semantic_type(&field).is_none() || field.label == Some(FieldLabel::Repeated as i32) {
            continue;
        }
        let value = descriptor_kotlin_value(&field, &chain);
        if let Some(has) = descriptor_field_has_expression(&field, "value") {
            out.push_str("if (");
            out.push_str(&has);
            out.push_str(") ");
        }
        out.push_str(value.as_str());
        out.push(';');
    }
}

fn render_scala_descriptor_validation(out: &mut String, method: &crate::type_policy::ResolvedRpcMethod, request: bool) {
    for (field, chain) in descriptor_projected_fields(method, request) {
        if !chain.is_empty() || descriptor_semantic_type(&field).is_none() || field.label == Some(FieldLabel::Repeated as i32) {
            continue;
        }
        let value = descriptor_scala_value(&field, &chain);
        if let Some(has) = descriptor_field_has_expression(&field, "value") {
            out.push_str("if (");
            out.push_str(&has);
            out.push_str(") ");
        }
        out.push_str(value.as_str());
        out.push(';');
    }
}
fn render_java_descriptor_responses(out: &mut String) {
    let methods = resolved_rpc_methods().expect("Rust RPC identities must resolve before JVM generation");
    for method in methods {
        let name = descriptor_response_name(&method);
        let wire = descriptor_message_type(&method.family, &method.output_message);
        out.push_str("  public record ");
        out.push_str(&name);
        out.push('(');
        out.push_str(&wire);
        out.push_str(" value) { public static ");
        out.push_str(&name);
        out.push_str(" fromWire(");
        out.push_str(&wire);
        out.push_str(" value) { ");
        render_java_descriptor_validation(out, &method, false);
        out.push_str(" return new ");
        out.push_str(&name);
        out.push_str("(java.util.Objects.requireNonNull(value)); } public ");
        out.push_str(&wire);
        out.push_str(" toWire() { return value; }");
        render_java_descriptor_projection(out, &method, false);
        out.push_str(" }\n\n");
    }
}

fn render_kotlin_descriptor_responses(out: &mut String) {
    let methods = resolved_rpc_methods().expect("Rust RPC identities must resolve before JVM generation");
    for method in methods {
        let name = descriptor_response_name(&method);
        let wire = descriptor_message_type(&method.family, &method.output_message);
        out.push_str("  data class ");
        out.push_str(&name);
        out.push_str("(val value: ");
        out.push_str(&wire);
        out.push_str(") { fun toWire(): ");
        out.push_str(&wire);
        out.push_str(" = value;");
        render_kotlin_descriptor_projection(out, &method, false);
        out.push_str(" companion object { fun fromWire(value: ");
        out.push_str(&wire);
        out.push_str("): ");
        out.push_str(&name);
        out.push_str(" = run { ");
        render_kotlin_descriptor_validation(out, &method, false);
        out.push_str(&name);
        out.push_str("(value) } } }\n\n");
    }
}

fn render_scala_descriptor_responses(out: &mut String) {
    let methods = resolved_rpc_methods().expect("Rust RPC identities must resolve before JVM generation");
    for method in methods {
        let name = descriptor_response_name(&method);
        let wire = descriptor_message_type(&method.family, &method.output_message);
        out.push_str("  final case class ");
        out.push_str(&name);
        out.push_str("(value: ");
        out.push_str(&wire);
        out.push_str(") { def toWire: ");
        out.push_str(&wire);
        out.push_str(" = value;");
        render_scala_descriptor_projection(out, &method, false);
        out.push_str(" }\n");
        out.push_str("  object ");
        out.push_str(&name);
        out.push_str(" { def fromWire(value: ");
        out.push_str(&wire);
        out.push_str("): ");
        out.push_str(&name);
        out.push_str(" = { ");
        render_scala_descriptor_validation(out, &method, false);
        out.push_str(&name);
        out.push_str("(value) } }\n\n");
    }
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

fn render_java_descriptor_clients(out: &mut String) {
    let methods = resolved_rpc_methods().expect("Rust RPC identities must resolve before JVM generation");
    for method in methods {
        let request_name = descriptor_request_name(&method);
        let response_name = descriptor_response_name(&method);
        let request_wire = descriptor_message_type(&method.family, &method.input_message);
        let response_wire = descriptor_message_type(&method.family, &method.output_message);
        let client_name = rpc_method_name(&descriptor_client_name(&method));
        let rpc_method = rpc_method_name(&method.method);
        if method.client_streaming {
            let descriptor = descriptor_method_descriptor(&method);
            let response_stream = format!("RustTypedResponses.{response_name}");
            out.push_str("  public static io.grpc.stub.StreamObserver<RustTypedRequests.");
            out.push_str(&request_name);
            out.push_str("> ");
            out.push_str(&client_name);
            out.push_str("(io.grpc.Channel channel, io.grpc.stub.StreamObserver<");
            out.push_str(&response_stream);
            out.push_str("> observer) {\n");
            out.push_str("    var wireObserver = new io.grpc.stub.StreamObserver<");
            out.push_str(&response_wire);
            out.push_str(">() { public void onNext(");
            out.push_str(&response_wire);
            out.push_str(" value) { observer.onNext(");
            out.push_str(&response_stream);
            out.push_str(".fromWire(value)); } public void onError(Throwable error) { observer.onError(error); } public void onCompleted() { observer.onCompleted(); } };\n");
            let call = if method.client_streaming && method.server_streaming {
                format!("io.grpc.stub.ClientCalls.asyncBidiStreamingCall(channel.newCall({descriptor}, io.grpc.CallOptions.DEFAULT), wireObserver)")
            } else {
                format!("io.grpc.stub.ClientCalls.asyncClientStreamingCall(channel.newCall({descriptor}, io.grpc.CallOptions.DEFAULT), wireObserver)")
            };
            out.push_str("    var wireRequest = ");
            out.push_str(&call);
            out.push_str(";\n    return new io.grpc.stub.StreamObserver<RustTypedRequests.");
            out.push_str(&request_name);
            out.push_str(">() { public void onNext(RustTypedRequests.");
            out.push_str(&request_name);
            out.push_str(" value) { wireRequest.onNext(value.toWire()); } public void onError(Throwable error) { wireRequest.onError(error); } public void onCompleted() { wireRequest.onCompleted(); } };\n  }\n\n");
            continue;
        }
        let stub = java_grpc_service(&method.family, &method.service, false);
        let response = format!("RustTypedResponses.{response_name}");
        let result = if method.server_streaming {
            format!("java.util.Iterator<{response}>")
        } else {
            response.clone()
        };
        out.push_str("  public static ");
        out.push_str(&result);
        out.push(' ');
        out.push_str(&client_name);
        out.push('(');
        out.push_str(&stub);
        out.push_str(" stub, RustTypedRequests.");
        out.push_str(&request_name);
        out.push_str(" request) { var wire = stub.");
        out.push_str(&rpc_method);
        out.push_str("(request.toWire());\n");
        if method.server_streaming {
            out.push_str("    return RustTypedResponses.mapIterator(wire, ");
            out.push_str(&response);
            out.push_str("::fromWire);\n  }\n\n");
        } else {
            out.push_str("    return ");
            out.push_str(&response);
            out.push_str(".fromWire(wire);\n  }\n\n");
        }
        let _ = request_wire;
    }
}

fn render_kotlin_descriptor_clients(out: &mut String) {
    let methods = resolved_rpc_methods().expect("Rust RPC identities must resolve before JVM generation");
    for method in methods {
        if method.client_streaming {
            continue;
        }
        let request_name = descriptor_request_name(&method);
        let response_name = descriptor_response_name(&method);
        let stub = java_grpc_service(&method.family, &method.service, false);
        let client_name = rpc_method_name(&descriptor_client_name(&method));
        let rpc_method = rpc_method_name(&method.method);
        let result = if method.server_streaming {
            format!("kotlin.collections.Iterator<RustTypedResponsesKotlin.{response_name}>")
        } else {
            format!("RustTypedResponsesKotlin.{response_name}")
        };
        out.push_str("  fun ");
        out.push_str(&client_name);
        out.push_str("(stub: ");
        out.push_str(&stub);
        out.push_str(", request: RustTypedRequestsKotlin.");
        out.push_str(&request_name);
        out.push_str("): ");
        out.push_str(&result);
        out.push_str(" { val wire = stub.");
        out.push_str(&rpc_method);
        out.push_str("(request.toWire()); return ");
        if method.server_streaming {
            out.push_str("RustTypedResponsesKotlin.mapIterator(wire) { RustTypedResponsesKotlin.");
            out.push_str(&response_name);
            out.push_str(".fromWire(it) }");
        } else {
            out.push_str("RustTypedResponsesKotlin.");
            out.push_str(&response_name);
            out.push_str(".fromWire(wire)");
        }
        out.push_str(" }\n\n");
    }
}

fn render_scala_descriptor_clients(out: &mut String) {
    let methods = resolved_rpc_methods().expect("Rust RPC identities must resolve before JVM generation");
    for method in methods {
        if method.client_streaming {
            continue;
        }
        let request_name = descriptor_request_name(&method);
        let response_name = descriptor_response_name(&method);
        let stub = java_grpc_service(&method.family, &method.service, false);
        let client_name = rpc_method_name(&descriptor_client_name(&method));
        let rpc_method = rpc_method_name(&method.method);
        let result = if method.server_streaming {
            format!("java.util.Iterator[RustTypedResponsesScala.{response_name}]")
        } else {
            format!("RustTypedResponsesScala.{response_name}")
        };
        out.push_str("  def ");
        out.push_str(&client_name);
        out.push_str("(stub: ");
        out.push_str(&stub);
        out.push_str(", request: RustTypedRequestsScala.");
        out.push_str(&request_name);
        out.push_str("): ");
        out.push_str(&result);
        out.push_str(" = { val wire = stub.");
        out.push_str(&rpc_method);
        out.push_str("(request.toWire); ");
        if method.server_streaming {
            out.push_str("RustTypedResponsesScala.mapIterator(wire, RustTypedResponsesScala.");
            out.push_str(&response_name);
            out.push_str(".fromWire)");
        } else {
            out.push_str("RustTypedResponsesScala.");
            out.push_str(&response_name);
            out.push_str(".fromWire(wire)");
        }
        out.push_str(" }\n\n");
    }
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
    render_java_descriptor_clients(&mut out);
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
    render_kotlin_descriptor_clients(&mut out);
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
            out.push_str(")), RustTypedResponsesScala.");
            out.push_str(&wrapper_name);
            out.push_str(".fromWire _)\n\n");
        } else {
            out.push_str(")))\n\n");
        }
    }
    render_scala_descriptor_clients(&mut out);
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
            "acyclic.machines.v1.Machines.{message_type}.newBuilder().setValue({parameter}.toWire()).build()"
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
        WireValueKind::UnsignedInteger
            if matches!(
                ty.rust_name,
                "PageLimit" | "StreamPageLimit" | "MachinePageLimit" | "MachineEventPageLimit"
            ) =>
        {
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
            "acyclic.machines.v1.Machines.{message_type}.newBuilder().setValue({parameter}.toWire()).build()"
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
        WireValueKind::UnsignedInteger
            if matches!(
                ty.rust_name,
                "PageLimit" | "StreamPageLimit" | "MachinePageLimit" | "MachineEventPageLimit"
            ) =>
        {
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
            "acyclic.machines.v1.Machines.{message_type}.newBuilder().setValue(com.google.protobuf.ByteString.copyFrom({parameter}.toWire)).build()"
        );
    }
    match ty.wire_kind {
        WireValueKind::Bytes => format!("com.google.protobuf.ByteString.copyFrom({parameter}.toWire)"),
        WireValueKind::Message => {
            let container = java_proto_container(binding.module);
            let message_type = match ty.rust_name {
                "Image" => format!("{container}.Image"),
                "IdempotencyKey" => format!("{container}.IdempotencyKey"),
                _ => panic!("missing message wire projection for {}", ty.rust_name),
            };
            format!("{parameter}.toWire.asInstanceOf[{message_type}]")
        }
        WireValueKind::UnsignedInteger
            if matches!(
                ty.rust_name,
                "PageLimit" | "StreamPageLimit" | "MachinePageLimit" | "MachineEventPageLimit"
            ) =>
        {
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

fn render_java_descriptor_requests(out: &mut String) {
    let methods = resolved_rpc_methods().expect("Rust RPC identities must resolve before JVM generation");
    for method in methods {
        let name = descriptor_request_name(&method);
        let wire = descriptor_message_type(&method.family, &method.input_message);
        out.push_str("  public record ");
        out.push_str(&name);
        out.push('(');
        out.push_str(&wire);
        out.push_str(" value) { public static ");
        out.push_str(&name);
        out.push_str(" fromWire(");
        out.push_str(&wire);
        out.push_str(" value) { ");
        render_java_descriptor_validation(out, &method, true);
        out.push_str(" return new ");
        out.push_str(&name);
        out.push_str("(java.util.Objects.requireNonNull(value)); } public ");
        out.push_str(&wire);
        out.push_str(" toWire() { return value; }");
        render_java_descriptor_projection(out, &method, true);
        out.push_str(" }\n\n");
    }
}

fn render_kotlin_descriptor_requests(out: &mut String) {
    let methods = resolved_rpc_methods().expect("Rust RPC identities must resolve before JVM generation");
    for method in methods {
        let name = descriptor_request_name(&method);
        let wire = descriptor_message_type(&method.family, &method.input_message);
        out.push_str("  data class ");
        out.push_str(&name);
        out.push_str("(val value: ");
        out.push_str(&wire);
        out.push_str(") { fun toWire(): ");
        out.push_str(&wire);
        out.push_str(" = value;");
        render_kotlin_descriptor_projection(out, &method, true);
        out.push_str(" companion object { fun fromWire(value: ");
        out.push_str(&wire);
        out.push_str("): ");
        out.push_str(&name);
        out.push_str(" = run { ");
        render_kotlin_descriptor_validation(out, &method, true);
        out.push_str(&name);
        out.push_str("(value) } } }\n\n");
    }
}

fn render_scala_descriptor_requests(out: &mut String) {
    let methods = resolved_rpc_methods().expect("Rust RPC identities must resolve before JVM generation");
    for method in methods {
        let name = descriptor_request_name(&method);
        let wire = descriptor_message_type(&method.family, &method.input_message);
        out.push_str("  final case class ");
        out.push_str(&name);
        out.push_str("(value: ");
        out.push_str(&wire);
        out.push_str(") { def toWire: ");
        out.push_str(&wire);
        out.push_str(" = value;");
        render_scala_descriptor_projection(out, &method, true);
        out.push_str(" }\n");
        out.push_str("  object ");
        out.push_str(&name);
        out.push_str(" { def fromWire(value: ");
        out.push_str(&wire);
        out.push_str("): ");
        out.push_str(&name);
        out.push_str(" = { ");
        render_scala_descriptor_validation(out, &method, true);
        out.push_str(&name);
        out.push_str("(value) } }\n\n");
    }
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
    render_java_descriptor_requests(&mut out);
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
    render_kotlin_descriptor_requests(&mut out);
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
    render_scala_descriptor_requests(&mut out);
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
