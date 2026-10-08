//! Emits Rust-owned Actors service metadata for TypeScript generation.
use prost_reflect::DescriptorPool;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let output = arguments.next();
    if output.as_deref() == Some("--generate") {
        let mode = arguments
            .next()
            .ok_or("--generate requires write, check, or rust")?;
        let root = arguments
            .next()
            .ok_or("--generate requires the repository root")?;
        if arguments.next().is_some() {
            return Err("unexpected generation argument".into());
        }
        return generate(&mode, Path::new(&root));
    }
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
    emit(output.as_deref().map(Path::new))
}

fn emit(output: Option<&Path>) -> Result<(), Box<dyn std::error::Error>> {
    let pool = DescriptorPool::decode(acyclic_actors::FILE_DESCRIPTOR_SET)?;
    let service = pool
        .get_service_by_name("acyclic.actors.v1.ActorsService")
        .ok_or("ActorsService is missing from the canonical descriptor")?;
    let methods = service
        .methods()
        .map(|method| {
            let operation = lower_camel(method.name())?;
            let input = method.input().name().to_owned();
            let output = method.output().name().to_owned();
            Ok((operation, input, output))
        })
        .collect::<Result<Vec<_>, &'static str>>()?;
    if let Some(output) = output {
        let source = render_typescript(&methods);
        if let Some(parent) = std::path::Path::new(&output).parent() {
            std::fs::create_dir_all(parent)?;
            std::fs::write(parent.join("readonly.ts"), render_readonly())?;
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

struct Temporary(PathBuf);
impl Temporary {
    fn new(root: &Path) -> std::io::Result<Self> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(std::io::Error::other)?
            .as_nanos();
        let path = root.join(format!(
            ".tmp-actors-generation-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    fn visit(root: &Path, current: &Path, output: &mut Vec<PathBuf>) -> std::io::Result<()> {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                visit(root, &entry.path(), output)?;
            } else if entry.file_type()?.is_file() {
                output.push(
                    entry
                        .path()
                        .strip_prefix(root)
                        .map_err(std::io::Error::other)?
                        .to_path_buf(),
                );
            }
        }
        Ok(())
    }
    let mut output = Vec::new();
    visit(root, root, &mut output)?;
    output.sort();
    Ok(output)
}

fn assert_tree_equal(
    expected: &Path,
    actual: &Path,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let inventory = files(expected)?;
    if inventory != files(actual)? {
        return Err(format!("{label} file set drift").into());
    }
    for file in inventory {
        if fs::read(expected.join(&file))? != fs::read(actual.join(&file))? {
            return Err(format!("{label} drift: {}", file.display()).into());
        }
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> std::io::Result<()> {
    for file in files(source)? {
        let to = destination.join(&file);
        fs::create_dir_all(
            to.parent()
                .ok_or_else(|| std::io::Error::other("missing output parent"))?,
        )?;
        fs::copy(source.join(file), to)?;
    }
    Ok(())
}

fn generate(mode: &str, root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if !matches!(mode, "write" | "check" | "rust") {
        return Err("expected write, check, or rust".into());
    }
    let temporary = Temporary::new(root)?;
    let generated = temporary.0.join("generated");
    let proto_root = temporary.0.join("proto");
    acyclic_actors::contract::render_proto_files(&proto_root)?;
    let package_generated = root.join("typescript/packages/actors/src/generated");
    emit(Some(
        &if mode == "check" {
            generated.clone()
        } else {
            package_generated.clone()
        }
        .join("actors-service.ts"),
    ))?;
    let proto_file = proto_root.join("actors/v1/actors.proto");
    let checked_proto = root.join("proto/actors/v1/actors.proto");
    if mode == "check" {
        assert_tree_equal(
            &package_generated,
            &generated,
            "Actors Rust TypeScript generation",
        )?;
        if fs::read(&proto_file)? != fs::read(&checked_proto)? {
            return Err("Actors Rust-rendered Proto drift".into());
        }
    } else {
        fs::create_dir_all(checked_proto.parent().ok_or("missing Proto parent")?)?;
        fs::copy(&proto_file, checked_proto)?;
    }
    if mode == "rust" {
        return Ok(());
    }
    fs::write(
        temporary.0.join("buf.yaml"),
        "version: v2\nmodules:\n  - path: proto\n",
    )?;
    let plugin = env::var("ACYCLIC_PROTOC_GEN_ES").map_or_else(
        |_| vec!["bun".to_owned(), "x".to_owned(), "protoc-gen-es".to_owned()],
        |path| vec![path.replace('\\', "/")],
    );
    let config = format!(
        "version: v2\nplugins:\n  - local: {}\n    out: generated/typescript\n    strategy: all\n    opt:\n      - target=js+dts\n      - import_extension=js\n",
        serde_json::to_string(&plugin)?
    );
    fs::write(temporary.0.join("buf.gen.yaml"), config)?;
    let buf = env::var_os("ACYCLIC_BUF_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            root.join(if cfg!(windows) {
                "node_modules/.bin/buf.exe"
            } else {
                "node_modules/.bin/buf"
            })
        });
    let status = Command::new(&buf)
        .arg("generate")
        .current_dir(&temporary.0)
        .status()?;
    if !status.success() {
        return Err(format!("{} generate exited with {status}", buf.display()).into());
    }
    let fresh_proto = temporary.0.join("generated/typescript/actors/v1");
    for file in ["actors_pb.js", "actors_pb.d.ts"] {
        let path = fresh_proto.join(file);
        fs::write(
            &path,
            format!("{}\n", fs::read_to_string(&path)?.trim_end_matches('\n')),
        )?;
    }
    let package_proto = root.join("typescript/packages/actors/generated/proto/actors/v1");
    if mode == "check" {
        assert_tree_equal(
            &package_proto,
            &fresh_proto,
            "Actors Buf TypeScript generation",
        )?;
    } else {
        if package_proto.exists() {
            fs::remove_dir_all(&package_proto)?;
        }
        copy_tree(&fresh_proto, &package_proto)?;
    }
    Ok(())
}

fn lower_camel(value: &str) -> Result<String, &'static str> {
    let mut characters = value.chars();
    let first = characters
        .next()
        .ok_or("descriptor method names must not be empty")?
        .to_ascii_lowercase();
    Ok(format!("{first}{}", characters.collect::<String>()))
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
                "  readonly {operation}: (request: ReadonlyInputSemantic<Semantic.{input}>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.{output}>>;"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"// Generated from the canonical ActorsService descriptor. Do not edit.
import {{ ActorsService }} from "../../generated/proto/actors/v1/actors_pb.js";
import type {{ ActorsCallOptions }} from "../client.js";
import type {{ ReadonlyInputSemantic, ReadonlySemantic }} from "./readonly.js";
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

fn render_readonly() -> String {
    r#"// Generated from the Rust semantic metadata entrypoint. Do not edit.

/**
 * The Rust-owned semantic projection of byte fields. Its TypeScript surface
 * omits mutating methods and mutable aliases, but an ordinary Uint8Array is
 * still mutable at runtime; this type does not freeze or proxy that value.
 * The Rust bridge snapshots inputs before encoding and returns detached byte
 * values, so later caller mutation cannot alter an admitted Rust request.
 */
export interface ReadonlyByteSurface {
  readonly [index: number]: number;
  readonly length: number;
  readonly byteLength: number;
  at(index: number): number | undefined;
  slice(start?: number, end?: number): ReadonlyBytes;
  subarray(start?: number, end?: number): ReadonlyBytes;
  every(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): boolean;
  filter(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): ReadonlyBytes;
  find(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): number | undefined;
  findIndex(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): number;
  findLast?(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): number | undefined;
  findLastIndex?(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): number;
  forEach(callbackfn: (value: number, index: number, array: ReadonlyBytes) => void, thisArg?: unknown): void;
  includes(searchElement: number, fromIndex?: number): boolean;
  indexOf(searchElement: number, fromIndex?: number): number;
  join(separator?: string): string;
  lastIndexOf(searchElement: number, fromIndex?: number): number;
  map(callbackfn: (value: number, index: number, array: ReadonlyBytes) => number, thisArg?: unknown): ReadonlyBytes;
  reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: ReadonlyBytes) => number, initialValue?: number): number;
  reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: ReadonlyBytes) => U, initialValue: U): U;
  reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: ReadonlyBytes) => number, initialValue?: number): number;
  reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: ReadonlyBytes) => U, initialValue: U): U;
  entries(): IterableIterator<[number, number]>;
  keys(): IterableIterator<number>;
  values(): IterableIterator<number>;
  [Symbol.iterator](): IterableIterator<number>;
  toLocaleString(): string;
  toString(): string;
}

/** Preserve Rust nominal brands while projecting byte operations readonly. */
export type ReadonlyBytes<T extends Uint8Array = Uint8Array> = ReadonlyByteSurface & {
  readonly [K in Exclude<keyof T, keyof Uint8Array>]: T[K]
};

/** Recursively project Rust semantic records and collections to readonly data. */
export type ReadonlySemantic<T> = T extends Uint8Array
  ? ReadonlyBytes<T>
  : T extends (...args: never[]) => unknown
    ? T
    : T extends readonly (infer Item)[]
      ? readonly ReadonlySemantic<Item>[]
      : T extends object
        ? { readonly [K in keyof T]: ReadonlySemantic<T[K]> }
        : T;

/** Inputs may be supplied by existing Uint8Array callers; the output view remains readonly. */
export type ReadonlyInputSemantic<T> = T extends Uint8Array
  ? ReadonlyBytes<T> | Uint8Array
  : T extends (...args: never[]) => unknown
    ? T
    : T extends readonly (infer Item)[]
      ? readonly ReadonlyInputSemantic<Item>[]
      : T extends object
        ? { readonly [K in keyof T]: ReadonlyInputSemantic<T[K]> }
        : T;
"#
    .to_owned()
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

#[cfg(test)]
mod generation_tests {
    use super::{Temporary, assert_tree_equal, files};
    use std::fs;

    #[test]
    fn rejects_missing_extra_and_mutated_outputs() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = Temporary::new(&std::env::temp_dir())?;
        let expected = temporary.0.join("expected");
        let actual = temporary.0.join("actual");
        for root in [&expected, &actual] {
            fs::create_dir_all(root.join("semantic/actors"))?;
            fs::write(
                root.join("semantic/actors/CurrentHeadMarker.ts"),
                "export type CurrentHeadMarker = true;\n",
            )?;
        }
        assert_tree_equal(&expected, &actual, "Actors generation")?;
        let file = actual.join("semantic/actors/CurrentHeadMarker.ts");
        fs::remove_file(&file)?;
        assert!(
            assert_tree_equal(&expected, &actual, "Actors generation")
                .expect_err("missing output")
                .to_string()
                .contains("file set drift")
        );
        fs::write(&file, "stale declaration")?;
        assert!(
            assert_tree_equal(&expected, &actual, "Actors generation")
                .expect_err("stale output")
                .to_string()
                .contains("drift:")
        );
        fs::write(
            &file,
            fs::read(expected.join("semantic/actors/CurrentHeadMarker.ts"))?,
        )?;
        fs::write(actual.join("extra.ts"), "extra declaration")?;
        assert!(
            assert_tree_equal(&expected, &actual, "Actors generation")
                .expect_err("extra output")
                .to_string()
                .contains("file set drift")
        );
        assert_eq!(files(&expected)?.len(), 1);
        Ok(())
    }
}
