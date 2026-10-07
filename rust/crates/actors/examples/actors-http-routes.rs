//! Emits Rust-owned Actors service metadata for TypeScript generation.
use prost_reflect::DescriptorPool;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args().nth(1);
    let pool = DescriptorPool::decode(acyclic_actors::FILE_DESCRIPTOR_SET)?;
    let service = pool
        .get_service_by_name("acyclic.actors.v1.ActorsService")
        .ok_or("ActorsService is missing from the canonical descriptor")?;
    let methods = service
        .methods()
        .map(|method| {
            let operation = lower_camel(method.name());
            let input = method.input().name().to_owned();
            let output = method.output().name().to_owned();
            (operation, input, output)
        })
        .collect::<Vec<_>>();
    if let Some(output) = output {
        let source = render_typescript(&methods);
        if let Some(parent) = std::path::Path::new(&output).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(output, source)?;
    } else {
        // Keep the original no-argument contract used by
        // `scripts/generate-runtime-routes.mjs`.  The package generation
        // path passes an output file when it needs the descriptor-derived
        // TypeScript facade, while existing route consumers continue to
        // receive the canonical JSON table.
        println!("{}", serde_json::to_string(acyclic_actors::HTTP_ROUTES)?);
    }
    Ok(())
}

fn lower_camel(value: &str) -> String {
    let mut characters = value.chars();
    let first = characters
        .next()
        .expect("descriptor method names must not be empty")
        .to_ascii_lowercase();
    format!("{first}{}", characters.collect::<String>())
}

fn render_typescript(methods: &[(String, String, String)]) -> String {
    let operations = methods
        .iter()
        .map(|(operation, _, _)| format!("\"{operation}\""))
        .collect::<Vec<_>>()
        .join(" | ");
    let signatures = methods
        .iter()
        .map(|(operation, input, output)| {
            format!(
                "  readonly {operation}: (request: ReadonlySemantic<Semantic.{input}>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.{output}>>;"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"// Generated from the canonical ActorsService descriptor. Do not edit.
import {{ ActorsService }} from "../../generated/proto/actors/v1/actors_pb.js";
import type {{ ActorsCallOptions, ReadonlySemantic }} from "../client.js";
import type * as Semantic from "./semantic/actors/index.js";

export type ActorsMethod = (typeof ActorsService.methods)[number];
export type ActorsOperation = {operations};
export const ACTORS_OPERATION_NAMES = Object.freeze(ActorsService.methods.map(method => method.localName)) as readonly ActorsOperation[];

export interface ActorsClientMethods {{
{signatures}
}}

export type ActorsMethodCall = (method: ActorsMethod, request: unknown, options?: ActorsCallOptions) => Promise<unknown>;

/** Installs the typed public methods from the maintained service descriptor. */
export function installActorsMethods(target: object, call: ActorsMethodCall): void {{
  for (const method of ActorsService.methods) {{
    Object.defineProperty(target, method.localName, {{
      configurable: true,
      enumerable: true,
      value: (request: unknown, options?: ActorsCallOptions) => call(method, request, options),
    }});
  }}
}}
"#
    )
}
