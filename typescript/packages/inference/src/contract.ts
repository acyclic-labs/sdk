import { toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";

type InferenceWasm = typeof import("../generated/wasm/acyclic_inference_wasm.js");

export class InferenceProtocolError extends Error {}

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
