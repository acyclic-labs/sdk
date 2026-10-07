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
            std::fs::write(parent.join("readonly.ts"), render_readonly())?;
            std::fs::write(parent.join("nominal.ts"), render_nominal())?;
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
