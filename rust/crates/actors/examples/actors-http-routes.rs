//! Emits Rust-owned Actors service metadata for TypeScript generation.
use std::{env, fs};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let output = arguments.next();
    let mut proto_output = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--proto-out" => {
                proto_output = Some(arguments.next().ok_or("--proto-out requires a directory")?);
            }
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    if let Some(proto_output) = proto_output {
        acyclic_actors::contract::render_proto_files(proto_output)?;
    }
    if let Some(output) = output {
        if let Some(parent) = std::path::Path::new(&output).parent() {
            std::fs::create_dir_all(parent)?;
            std::fs::write(parent.join("nominal.ts"), render_nominal())?;
            <acyclic_actors::client::ErrorMetadata as ts_rs::TS>::export_all(
                &ts_rs::Config::default().with_out_dir(parent),
            )?;
            let semantic_root = parent.join("semantic");
            if semantic_root.exists() {
                fs::remove_dir_all(&semantic_root)?;
            }
            let public_semantic = semantic_root.join("actors");
            fs::create_dir_all(&public_semantic)?;
            acyclic_actors::domain::export_typescript(&semantic_root)?;
            fs::write(
                public_semantic.join("index.ts"),
                render_semantic_index(&ts_rs::Config::default()),
            )?;
            std::fs::write(
                public_semantic.join("readonly.ts"),
                render_readonly_semantic(&ts_rs::Config::default()),
            )?;
        }
        let parent = std::path::Path::new(&output)
            .parent()
            .ok_or("service output requires a parent")?;
        fs::write(
            parent.join("Actors.service.bin"),
            acyclic_actors::FILE_DESCRIPTOR_SET,
        )?;
        fs::write(
            parent.join("Actors.semantic-names.json"),
            serde_json::to_vec(&acyclic_actors::domain::typescript_export_metadata(
                &ts_rs::Config::default(),
            ))?,
        )?;
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

fn render_nominal() -> String {
    r#"// Generated from Rust nominal semantic declarations. Do not edit.
import type * as Semantic from "./semantic/actors/index.js";
import type { ReadonlyBytes } from "./readonly.js";

export interface ActorsNominalBinding {
  readonly ActorId: (value: string) => Semantic.ActorId;
  readonly CodeSha256: (value: ReadonlyBytes | Uint8Array) => Semantic.CodeSha256;
  readonly PositiveU64: (value: bigint) => Semantic.PositiveU64;
  readonly CurrentHeadMarker: (value: true) => Semantic.CurrentHeadMarker;
}

let resolveBinding: (() => Promise<ActorsNominalBinding>) | undefined;

/** Installs the platform resolver supplied by the package's Rust bridge. */
export function configureActorsNominalBinding(resolve: () => Promise<ActorsNominalBinding>): void {
  resolveBinding = resolve;
}

async function binding(): Promise<ActorsNominalBinding> {
  if (resolveBinding === undefined) throw new Error("Actors nominal binding is not configured");
  return resolveBinding();
}

export async function ActorId(value: string): Promise<Semantic.ActorId> {
  return (await binding()).ActorId(value);
}

export async function CodeSha256(value: ReadonlyBytes | Uint8Array): Promise<Semantic.CodeSha256> {
  return (await binding()).CodeSha256(value);
}

export async function PositiveU64(value: bigint): Promise<Semantic.PositiveU64> {
  return (await binding()).PositiveU64(value);
}

export type CurrentHeadMarker = Semantic.CurrentHeadMarker;

export async function CurrentHeadMarker(value: true): Promise<CurrentHeadMarker> {
  return (await binding()).CurrentHeadMarker(value);
}
"#
    .to_owned()
}

fn render_readonly_semantic(config: &ts_rs::Config) -> String {
    let aliases = acyclic_actors::domain::typescript_export_names(config)
        .iter()
        .map(|name| format!("export type {name} = ReadonlySemantic<Semantic.{name}>;"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"// Generated from the Rust semantic metadata entrypoint. Do not edit.
// Raw ts-rs declarations remain an internal generator boundary; package users
// receive this recursive readonly projection from the package root.
import type * as Semantic from "./index.js";
import type {{ ReadonlySemantic }} from "../../readonly.js";

{aliases}
"#
    )
}

fn render_semantic_index(config: &ts_rs::Config) -> String {
    let exports = acyclic_actors::domain::typescript_export_names(config)
        .iter()
        .map(|name| format!("export * from \"./{name}.js\";"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "// Generated from the canonical Rust Actors domain declarations. Do not edit.\n{exports}\n"
    )
}
