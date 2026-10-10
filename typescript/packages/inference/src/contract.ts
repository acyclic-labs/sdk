import { toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { RunEventSchema, type RunEvent } from "../generated/proto/inference/v1/inference_pb.js";
import { RUN_TERMINAL_METADATA } from "../generated/terminal-metadata.js";
import type { RunTerminalKind, RunTerminalMetadata } from "../generated/terminal-metadata.js";
import type * as InferenceWasmModule from "../generated/wasm/acyclic_inference_wasm.js";
import type { WatchRunState } from "../generated/wasm/acyclic_inference_wasm.js";

type InferenceWasm = typeof InferenceWasmModule;

/** `invalid` for a contract violation; `unavailable` when the Rust descriptor cannot load. */
export type InferenceProtocolErrorCode = "invalid" | "unavailable";

export class InferenceProtocolError extends Error {
  constructor(message: string, readonly code: InferenceProtocolErrorCode = "invalid") {
    super(message);
  }
}

/** Keep the Rust message and stable `code` of a WASM error. */
function protocolError(error: unknown): InferenceProtocolError {
  if (error instanceof InferenceProtocolError) return error;
  const code = (error as { code?: unknown } | null)?.code;
  return new InferenceProtocolError(error instanceof Error ? error.message : String(error),
    code === "unavailable" ? code : "invalid");
}

export type { RunTerminalKind, RunTerminalMetadata };

const empty = new Uint8Array();

/** Share one in-flight load, but forget a rejected one so the next call retries. */
export function retryableOnce<T>(load: () => Promise<T>): () => Promise<T> {
  let pending: Promise<T> | undefined;
  return () => pending ??= load().catch((error: unknown) => {
    pending = undefined;
    throw error;
  });
}

let compiledModule: WebAssembly.Module | undefined;

/** Initialize the canonical Rust descriptor from the deployment's static module. */
export async function initializeInferenceWasm(module: WebAssembly.Module): Promise<void> {
  compiledModule = module;
  await loadBinding();
}

const loadBinding = retryableOnce(async (): Promise<InferenceWasm> => {
  // This path is emitted by the inference WASM build and shipped beside dist.
  const module = await import("../generated/wasm/acyclic_inference_wasm.js");
  const nodeVersion = (globalThis as { process?: { versions?: { node?: string } } }).process?.versions?.node;
  if (compiledModule !== undefined) {
    await module.default({ module_or_path: compiledModule });
  } else if (typeof nodeVersion !== "string") {
    await module.default();
  } else {
    const fsModule: string = "node:fs/promises";
    const { readFile } = await import(fsModule) as { readFile(url: URL): Promise<Uint8Array> };
    await module.default({ module_or_path: await readFile(new URL("../generated/wasm/acyclic_inference_wasm_bg.wasm", import.meta.url)) });
  }
  return module;
});

/** Validate generated protobuf bytes without JSON or safe-integer conversion. */
export async function validateContract<Schema extends DescMessage>(
  kind: string,
  schema: Schema,
  value: MessageShape<Schema>,
  expected: Uint8Array = empty,
  related: Uint8Array = empty,
): Promise<void> {
  const module = await loadBinding();
  try {
    module.validate_customer_wire(kind, toBinary(schema, value), expected, related);
  } catch (error) {
    throw protocolError(error);
  }
}

/** Validate a protobuf-shaped JavaScript request before JSON conversion. */
export async function validateRuntimeShape<Schema extends DescMessage>(
  schema: Schema,
  value: MessageShape<Schema>,
): Promise<void> {
  const module = await loadBinding();
  try {
    module.validate_runtime_shape(schema.typeName, value);
  } catch (error) {
    throw protocolError(error);
  }
}

/** Read the frozen terminal contract generated from the current Rust descriptor. */
export async function runTerminalMetadata(): Promise<readonly RunTerminalMetadata[]> {
  return RUN_TERMINAL_METADATA;
}

/** Start the Rust-owned ordered Run watch state from a validated protobuf view. */
export async function watchRunStart(
  viewBytes: Uint8Array,
  runId: Uint8Array,
  fromSequence: bigint,
): Promise<WatchRunState> {
  const module = await loadBinding();
  try {
    return module.watch_run_start_state_wire(viewBytes, runId, fromSequence.toString());
  } catch (error) {
    throw protocolError(error);
  }
}

/** Advance Rust-owned ordered Run watch state with one protobuf event. */
export function watchRunAdvance(state: WatchRunState, event: RunEvent): void {
  try {
    state.advance(toBinary(RunEventSchema, event));
  } catch (error) {
    throw protocolError(error);
  }
}

/** Confirm that a Rust-owned ordered Run watch ended after terminal evidence. */
export function watchRunFinish(state: WatchRunState): void {
  try {
    state.finish();
  } catch (error) {
    throw protocolError(error);
  }
}
