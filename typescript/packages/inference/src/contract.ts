import { toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { RunEventSchema, RunTerminal, type RunEvent } from "../generated/proto/inference/v1/inference_pb.js";

type InferenceWasm = typeof import("../generated/wasm/acyclic_inference_wasm.js");
type WatchRunState = ReturnType<InferenceWasm["watch_run_start_state_wire"]>;

export class InferenceProtocolError extends Error {}

export interface RunTerminalMetadata {
  readonly number: number;
  readonly kind: string;
  readonly partial: boolean;
}

let binding: Promise<InferenceWasm> | undefined;
const empty = new Uint8Array();

async function loadBinding(): Promise<InferenceWasm> {
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
    throw new InferenceProtocolError(String(error));
  }
}

/** Validate a protobuf-shaped JavaScript request before JSON conversion. */
export async function validateRuntimeShape<Schema extends DescMessage>(
  schema: Schema,
  value: MessageShape<Schema>,
): Promise<void> {
  const module = await loadBinding();
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
  const module = await loadBinding();
  try {
    const raw = module.run_terminal_metadata();
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
    const expected = Object.entries(RunTerminal)
      .filter((entry): entry is [string, number] => typeof entry[1] === "number" && entry[1] > 0)
      .map(([name, number]) => ({ number, kind: name.toLowerCase().replaceAll("_", "-") }));
    if (metadata.length !== expected.length || new Set(metadata.map(item => item.number)).size !== metadata.length) {
      throw new InferenceProtocolError("run terminal metadata does not cover the generated enum");
    }
    for (const item of expected) {
      const actual = metadata.find(candidate => candidate.number === item.number);
      if (actual === undefined || actual.kind !== item.kind) {
        throw new InferenceProtocolError("run terminal metadata does not cover the generated enum");
      }
    }
    return metadata;
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
  const module = await loadBinding();
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
