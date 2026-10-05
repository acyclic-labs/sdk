//! Rust-owned nominal type projections for JVM-family SDKs.
//!
//! These sources are emitted from `type_policy.rs`.  The generated wrappers
//! deliberately sit at the language boundary: protobuf messages remain the
//! wire representation, while consumers receive validated nominal values and
//! an open union that preserves values introduced by newer servers.

use std::collections::BTreeSet;

use crate::type_policy::{SemanticRule, SemanticType, WireValueKind, SEMANTIC_TYPES};

pub const JAVA_PATH: &str = "jvm/src/main/java/dev/acyclic/transport/RustSemanticTypes.java";
pub const KOTLIN_PATH: &str = "jvm/src/main/kotlin/dev/acyclic/transport/RustSemanticTypes.kt";
pub const SCALA_PATH: &str = "jvm/src/main/scala/dev/acyclic/transport/RustSemanticTypes.scala";

pub fn generate_jvm_semantic_types() -> Vec<(&'static str, String)> {
    vec![
        (JAVA_PATH, render_java()),
        (KOTLIN_PATH, render_kotlin()),
        (SCALA_PATH, render_scala()),
    ]
}

fn unique_types() -> impl Iterator<Item = &'static SemanticType> {
    let mut seen = BTreeSet::new();
    SEMANTIC_TYPES.iter().filter(move |ty| seen.insert(ty.rust_name))
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
            SemanticRule::NonEmpty | SemanticRule::Utf8 if ty.wire_kind == WireValueKind::String => {
                lines.push(format!("require({value}.isNotEmpty()) {{ \"{} must be non-empty\" }}", ty.rust_name));
            }
            SemanticRule::NonEmpty if ty.wire_kind == WireValueKind::Bytes => {
                lines.push(format!("require({value}.size() > 0) {{ \"{} must be non-empty\" }}", ty.rust_name));
            }
            SemanticRule::FixedLength(n) if ty.wire_kind == WireValueKind::Bytes => {
                lines.push(format!("require({value}.size() == {n}) {{ \"{} must contain exactly {n} bytes\" }}", ty.rust_name));
            }
            SemanticRule::NonNegative => lines.push(format!("require({value} >= 0) {{ \"{} must be non-negative\" }}", ty.rust_name)),
            SemanticRule::StrictlyPositive => lines.push(format!("require({value} > 0) {{ \"{} must be positive\" }}", ty.rust_name)),
            SemanticRule::MaxItems(n) => lines.push(format!("require({value} <= {n}) {{ \"{} exceeds its maximum\" }}", ty.rust_name)),
            _ => {}
        }
    }
    lines.join("; ")
}

fn scala_validation(ty: &SemanticType, value: &str) -> String {
    let mut lines = Vec::new();
    for rule in ty.rules {
        match rule {
            SemanticRule::NonEmpty | SemanticRule::Utf8 if ty.wire_kind == WireValueKind::String => {
                lines.push(format!("require({value}.nonEmpty, \"{} must be non-empty\")", ty.rust_name));
            }
            SemanticRule::NonEmpty if ty.wire_kind == WireValueKind::Bytes => {
                lines.push(format!("require({value}.nonEmpty, \"{} must be non-empty\")", ty.rust_name));
            }
            SemanticRule::FixedLength(n) if ty.wire_kind == WireValueKind::Bytes => {
                lines.push(format!("require({value}.length == {n}, \"{} must contain exactly {n} bytes\")", ty.rust_name));
            }
            SemanticRule::NonNegative => lines.push(format!("require({value} >= 0, \"{} must be non-negative\")", ty.rust_name)),
            SemanticRule::StrictlyPositive => lines.push(format!("require({value} > 0, \"{} must be positive\")", ty.rust_name)),
            SemanticRule::MaxItems(n) => lines.push(format!("require({value} <= {n}, \"{} exceeds its maximum\")", ty.rust_name)),
            _ => {}
        }
    }
    lines.join("; ")
}

fn render_java() -> String {
    let mut out = String::from("// Generated by acyclic-sdk-contract-wire; do not edit.\n// Semantic intent and validation rules originate in rust/crates/sdk-contract-wire/src/type_policy.rs.\npackage dev.acyclic.transport;\n\nimport java.util.Optional;\n\n/** Rust-owned nominal values. Protobuf classes remain the wire boundary. */\npublic final class RustSemanticTypes {\n  private RustSemanticTypes() {}\n\n  public sealed interface WireChoice permits Known, Unknown {}\n  public record Known(String tag, com.google.protobuf.ByteString payload) implements WireChoice {}\n  public record Unknown(int tag, com.google.protobuf.ByteString payload) implements WireChoice {}\n  public static <T> Optional<T> present(T value, boolean isPresent) { return isPresent ? Optional.ofNullable(value) : Optional.empty(); }\n\n");
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
        if wire == "String" || wire == "com.google.protobuf.ByteString" || wire == "com.google.protobuf.Message" {
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
    let mut out = String::from("// Generated by acyclic-sdk-contract-wire; do not edit.\n// Semantic intent and validation rules originate in Rust type_policy.rs.\npackage dev.acyclic.transport\n\nimport com.google.protobuf.ByteString\nimport java.util.Optional\n\n/** Rust-owned nominal values. Protobuf classes remain the wire boundary. */\nobject RustSemanticTypesKotlin {\n  sealed interface WireChoice\n  data class Known(val tag: String, val payload: ByteString) : WireChoice\n  data class Unknown(val tag: Int, val payload: ByteString) : WireChoice\n  fun <T: Any> present(value: T?, isPresent: Boolean): Optional<T> = if (isPresent && value != null) Optional.of(value) else Optional.empty()\n\n");
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
    let mut out = String::from("// Generated by acyclic-sdk-contract-wire; do not edit.\n// Semantic intent and validation rules originate in Rust type_policy.rs.\npackage dev.acyclic.transport\n\n/** Rust-owned nominal values. Protobuf classes remain the wire boundary. */\nobject RustSemanticTypesKotlin {\n  sealed trait WireChoice\n  final case class Known(tag: String, payload: Array[Byte]) extends WireChoice\n  final case class Unknown(tag: Int, payload: Array[Byte]) extends WireChoice\n  def present[T](value: T, isPresent: Boolean): Option[T] = if (isPresent) Option(value) else None\n\n");
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
        if !validation.is_empty() { out.push_str(&validation); out.push_str("; "); }
        out.push_str("Right(new ");
        out.push_str(ty.rust_name);
        out.push_str("(value)) } catch { case e: IllegalArgumentException => Left(e.getMessage) } }\n\n");
    }
    out.push_str("}\n");
    out
}
