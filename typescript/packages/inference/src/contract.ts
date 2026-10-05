import { toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { RunEventSchema, RunTerminal, type RunEvent } from "../generated/proto/inference/v1/inference_pb.js";
import { RUN_TERMINAL_METADATA } from "../generated/terminal-metadata.js";
import type { RunTerminalKind, RunTerminalMetadata } from "../generated/terminal-metadata.js";

type InferenceWasm = typeof import("../generated/wasm/acyclic_inference_wasm.js");
type WatchRunState = ReturnType<InferenceWasm["watch_run_start_state_wire"]>;

export class InferenceProtocolError extends Error {}

export type { RunTerminalKind, RunTerminalMetadata };

let binding: Promise<InferenceWasm> | undefined;
const empty = new Uint8Array();

export async function loadInferenceWasm(): Promise<InferenceWasm> {
  binding ??= (async () => {
    // This path is emitted by the inference WASM build and shipped beside dist.
    const module = await import("../generated/wasm/acyclic_inference_wasm.js");
    const nodeVersion = (globalThis as { process?: { versions?: { node?: string } } }).process?.versions?.node;
    if (typeof nodeVersion !== "string") {
      await module.default();
    } else {
      const fsModule: string = "node:fs/promises";
      const { readFile } = await import(fsModule) as { readFile(url: URL): Promise<Uint8Array> };
      await module.default({ module_or_path: await readFile(new URL("../generated/wasm/acyclic_inference_wasm_bg.wasm", import.meta.url)) });
    }
    return module;
  })();
  return binding;
}

/** Rust-owned browser/native WASM client constructor exposed to thin adapters. */
export type RustInferenceClient = Awaited<ReturnType<InferenceWasm["BrowserInferenceClient"]["connect"]>>;

/** Validate a bearer credential through the Rust/WASM boundary. */
export async function validateInferenceCredential(token: string): Promise<void> {
  const module = await loadInferenceWasm();
  try {
    module.validate_remote_web_credential(token);
  } catch (error) {
    throw new InferenceProtocolError(String(error));
  }
}

/** Validate generated protobuf bytes without JSON or safe-integer conversion. */
export async function validateContract<Schema extends DescMessage>(
  kind: string,
  schema: Schema,
  value: MessageShape<Schema>,
  expected: Uint8Array = empty,
  related: Uint8Array = empty,
): Promise<void> {
  const module = await loadInferenceWasm();
  try {
    module.validate_customer_wire(kind, toBinary(schema, value), expected, related);
  } catch (error) {
    throw new InferenceProtocolError(String(error));
  }
}

/** Validate a protobuf-shaped JavaScript request before JSON conversion. */
export async function validateRuntimeShape<Schema extends DescMessage>(
  schema: Schema,
  value: MessageShape<Schema>,
): Promise<void> {
  const module = await loadInferenceWasm();
  try {
    const error = module.runtime_shape_error(schema.typeName, value);
    if (error !== undefined) throw new InferenceProtocolError(error);
  } catch (error) {
    if (error instanceof InferenceProtocolError) throw error;
    throw new InferenceProtocolError(String(error));
  }
}

let terminalMetadataBinding: Promise<readonly RunTerminalMetadata[]> | undefined;

/** Read terminal names and partial outcome policy from the Rust descriptor. */
export function runTerminalMetadata(): Promise<readonly RunTerminalMetadata[]> {
  terminalMetadataBinding ??= loadTerminalMetadata();
  return terminalMetadataBinding;
}

async function loadTerminalMetadata(): Promise<readonly RunTerminalMetadata[]> {
  const module = await loadInferenceWasm();
  try {
    return validateRunTerminalMetadata(module.run_terminal_metadata());
  } catch (error) {
    if (error instanceof InferenceProtocolError) throw error;
    throw new InferenceProtocolError(String(error));
  }
}

/** Validate Rust terminal metadata against the generated protobuf enum contract. */
export function validateRunTerminalMetadata(raw: string): readonly RunTerminalMetadata[] {
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) throw new InferenceProtocolError("run terminal metadata is not a list");
    const metadata: RunTerminalMetadata[] = [];
    for (const item of parsed) {
      if (item === null || typeof item !== "object" ||
          typeof item.number !== "number" || !Number.isSafeInteger(item.number) || item.number <= 0 ||
          typeof item.kind !== "string" || item.kind.length === 0 ||
          typeof item.partial !== "boolean") {
        throw new InferenceProtocolError("run terminal metadata has an invalid entry");
      }
      metadata.push({ number: item.number, kind: item.kind, partial: item.partial });
    }
    if (metadata.length === 0) throw new InferenceProtocolError("run terminal metadata is empty");
    const expectedEnum = Object.entries(RunTerminal)
      .filter((entry): entry is [string, number] => typeof entry[1] === "number" && entry[1] > 0)
      .map(([, number]) => number);
    if (metadata.length !== expectedEnum.length || RUN_TERMINAL_METADATA.length !== expectedEnum.length ||
        new Set(metadata.map(item => item.number)).size !== metadata.length ||
        new Set(metadata.map(item => item.kind)).size !== metadata.length ||
        expectedEnum.some(number => !RUN_TERMINAL_METADATA.some(item => item.number === number))) {
      throw new InferenceProtocolError("run terminal metadata does not cover the generated enum");
    }
    const validated: RunTerminalMetadata[] = [];
    for (const item of metadata) {
      const expected = RUN_TERMINAL_METADATA.find(candidate => candidate.number === item.number);
      const actual = metadata.find(candidate => candidate.number === item.number);
      if (expected === undefined || actual === undefined || actual.kind !== expected.kind || actual.partial !== expected.partial) {
        throw new InferenceProtocolError("run terminal metadata does not cover the generated enum");
      }
      validated.push({ number: actual.number, kind: expected.kind, partial: actual.partial });
    }
    return validated;
  } catch (error) {
    if (error instanceof InferenceProtocolError) throw error;
    throw new InferenceProtocolError(String(error));
  }
}

/** Start the Rust-owned ordered Run watch state from a validated protobuf view. */
export async function watchRunStart(
  viewBytes: Uint8Array,
  runId: Uint8Array,
  fromSequence: bigint,
): Promise<WatchRunState> {
  const module = await loadInferenceWasm();
  try {
    return module.watch_run_start_state_wire(viewBytes, runId, fromSequence.toString());
  } catch (error) {
    throw new InferenceProtocolError(String(error));
  }
}

/** Advance Rust-owned ordered Run watch state with one protobuf event. */
export function watchRunAdvance(state: WatchRunState, event: RunEvent): void {
  try {
    state.advance(toBinary(RunEventSchema, event));
  } catch (error) {
    throw new InferenceProtocolError(String(error));
  }
}

/** Confirm that a Rust-owned ordered Run watch ended after terminal evidence. */
export function watchRunFinish(state: WatchRunState): void {
  try {
    state.finish();
  } catch (error) {
    throw new InferenceProtocolError(String(error));
  }
}
