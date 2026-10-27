/** bun examples/client-conformance.ts; same host-created fixture as Rust runner. */
import { open } from "node:fs/promises";
import { decodeClientJson, initializeClientQueries, WasmStream } from "../src/client-query.js";
import { verifyClientControls, verifyClientToolOutput, type ClientControlFixture,
  type ClientToolOutputFixture } from "../src/client-conformance.js";

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`CLIENT_INPUT ${name} is required`);
  return value;
}
const endpoint = required("ACYCLIC_CONFORMANCE_ENDPOINT");
const token = required("ACYCLIC_CONFORMANCE_TOKEN");
const capability = required("ACYCLIC_CONFORMANCE_SESSION_CREDENTIAL");
if (capability.length > 131_072 || capability.length % 2 !== 0 || !/^[0-9a-f]+$/i.test(capability)) {
  throw new Error("CLIENT_INPUT invalid bounded capability hex");
}
const toolPath = process.env.ACYCLIC_CONFORMANCE_TOOL_FIXTURE;
const controlPath = process.env.ACYCLIC_CONFORMANCE_CONTROL_FIXTURE;
if (toolPath && controlPath) throw new Error("CLIENT_INPUT choose one tool or control fixture");
const handle = await open(toolPath ?? required("ACYCLIC_CONFORMANCE_CONTROL_FIXTURE"), "r");
let encoded: Uint8Array;
try {
  const maximum = 32 * 1024 * 1024;
  const input = new Uint8Array(maximum + 1);
  let length = 0;
  while (length < input.length) {
    const result = await handle.read(input, length, input.length - length);
    if (result.bytesRead === 0) break;
    length += result.bytesRead;
  }
  if (length > maximum) throw new Error("CLIENT_INPUT fixture byte capacity exceeded");
  encoded = input.slice(0, length);
} finally { await handle.close(); }
await initializeClientQueries();
const stream = WasmStream.connect(endpoint, () => token);
try {
  stream.registerCredential(0, Uint8Array.from(Buffer.from(capability, "hex")));
  const report = toolPath
    ? await verifyClientToolOutput(stream, 0, decodeClientJson(encoded) as ClientToolOutputFixture)
    : await verifyClientControls(stream, 0, JSON.parse(new TextDecoder("utf-8", {fatal: true}).decode(encoded)) as ClientControlFixture);
  console.log(JSON.stringify(report));
} finally { stream.free(); }
