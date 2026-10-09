//! Shared descriptor/semantic-name projection for public Rust client methods.
use prost_reflect::DescriptorPool;
use std::collections::{HashMap, HashSet};

pub fn render(arguments: &[String]) -> Result<String, Box<dyn std::error::Error>> {
    let [
        descriptor,
        names,
        service_name,
        family,
        proto_import,
        semantic_import,
        options_import,
    ] = arguments
    else {
        return Err(
            "service mode expects descriptor, semantic names, service, family and three imports"
                .into(),
        );
    };
    let descriptor = DescriptorPool::decode(std::fs::read(descriptor)?.as_slice())?;
    let (names, pairs): (Vec<String>, Vec<(String, String)>) =
        serde_json::from_slice(&std::fs::read(names)?)?;
    let unique: HashSet<_> = names.iter().cloned().collect();
    if unique.len() != names.len() {
        return Err("duplicate Rust semantic export name".into());
    }
    let mut semantic = HashMap::new();
    for (wire, name) in pairs {
        if descriptor.get_message_by_name(&wire).is_none() || !unique.contains(&name) {
            return Err("typed Rust message identity is absent from descriptor or exports".into());
        }
        if semantic.insert(wire, name).is_some() {
            return Err("duplicate typed Rust wire message identity".into());
        }
    }
    let service = descriptor
        .get_service_by_name(service_name)
        .ok_or("canonical service missing")?;
    for method in service.methods() {
        if method.is_client_streaming() || method.is_server_streaming() {
            return Err("unary semantic client projection required".into());
        }
        let package = service.parent_file().package_name().to_owned();
        if method.input().full_name() != format!("{package}.{}", method.input().name())
            || method.output().full_name() != format!("{package}.{}", method.output().name())
        {
            return Err(
                "RPC semantic types must have the canonical service package identity".into(),
            );
        }
        for message in [method.input(), method.output()] {
            if !semantic.contains_key(message.full_name()) {
                return Err("RPC message lacks a typed Rust wire/TS identity".into());
            }
        }
    }
    if service.methods().count() == 0 {
        return Err("canonical service has no RPCs".into());
    }
    let mut associations = semantic.iter().collect::<Vec<_>>();
    associations.sort_by(|left, right| left.0.cmp(right.0));
    let associations = associations
        .iter()
        .map(|(wire, name)| format!("  {wire:?}: Semantic.{name};"))
        .collect::<Vec<_>>()
        .join("\n");
    let service_ident = service.name();
    let upper = family.to_ascii_uppercase();
    Ok(format!(
        r#"// Generated from the canonical {service_ident} descriptor. Do not edit.
import {{ {service_ident} }} from {proto_import:?};
import type {{ DescMessage, MessageShape }} from "@bufbuild/protobuf";
import type {{ {family}CallOptions }} from {options_import:?};
import type {{ CanonicalCompatible, ReadonlyInputSemantic, ReadonlySemantic }} from "./readonly.js";
import type * as Semantic from {semantic_import:?};

export {{ canonicalSemantic, requireBigIntCodec }} from "./readonly.js";

export type {family}Method = (typeof {service_ident}.methods)[number];
export type {family}Operation = keyof typeof {service_ident}.method;
export const {upper}_OPERATION_NAMES = Object.freeze({service_ident}.methods.map(method => method.localName)) as readonly {family}Operation[];

type SemanticMessages = {{
{associations}
}};
type Methods = typeof {service_ident}.method;
type SemanticShape<S extends DescMessage> = SemanticMessages[Extract<MessageShape<S>["$typeName"], keyof SemanticMessages>];

type AssertCanonical<T extends true> = T;
/** Every RPC's actual Rust root must recursively match the maintained wire shape. */
export type {family}CanonicalContract = AssertCanonical<{{
  [K in keyof Methods]: CanonicalCompatible<SemanticShape<Methods[K]["input"]>, MessageShape<Methods[K]["input"]>> extends true
    ? CanonicalCompatible<SemanticShape<Methods[K]["output"]>, MessageShape<Methods[K]["output"]>> : false;
}}[keyof Methods]>;

export type {family}ClientMethods = {{
  readonly [K in keyof Methods]: (request: ReadonlyInputSemantic<SemanticShape<Methods[K]["input"]>>, options?: {family}CallOptions) => Promise<ReadonlySemantic<SemanticShape<Methods[K]["output"]>>>;
}};

export type {family}MethodCall = (method: {family}Method, request: unknown, options?: {family}CallOptions) => Promise<unknown>;

/** Installs the typed public methods from the maintained service descriptor. */
export function install{family}Methods(target: object, call: {family}MethodCall): void {{
  for (const method of {service_ident}.methods) {{
    Object.defineProperty(target, method.localName, {{
      configurable: true,
      enumerable: true,
      value: (request: unknown, options?: {family}CallOptions) => call(method, request, options),
    }});
  }}
}}
"#
    ))
}
