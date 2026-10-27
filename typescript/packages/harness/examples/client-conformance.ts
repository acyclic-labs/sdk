/** bun examples/client-conformance.ts; same host-created fixture as Rust runner. */
import { open } from "node:fs/promises";
import { decodeClientJson, initializeClientQueries, WasmStream } from "../src/client-query.js";
import { verifyClientControls, verifyClientToolOutput, verifyClientObservation, type ClientControlFixture,
  type ClientObservationFixture, type ClientToolOutputFixture } from "../src/client-conformance.js";

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
const agentPath = process.env.ACYCLIC_CONFORMANCE_AGENT_FIXTURE;
if ([toolPath, controlPath, agentPath].filter(value => value !== undefined).length !== 1) {
  throw new Error("CLIENT_INPUT choose exactly one tool, control or agent fixture");
}
const handle = await open(toolPath ?? controlPath ?? required("ACYCLIC_CONFORMANCE_AGENT_FIXTURE"), "r");
let encoded: Uint8Array;
try {
  const maximum = toolPath ? 8 * 1024 * 1024 : controlPath ? 32 * 1024 * 1024 : 65_536;
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
const observation = agentPath ? decodeClientJson(encoded) as ClientObservationFixture : undefined;
if (agentPath) {
  if (observation === null || typeof observation !== "object") throw new Error("CLIENT_INPUT invalid observation fixture");
  const namespace = required("ACYCLIC_CONFORMANCE_NAMESPACE");
  if (!/^[0-9a-f]{32}$/.test(namespace)) throw new Error("CLIENT_INPUT invalid namespace identity");
  const root = `harness/agents/${namespace}`;
  if (typeof observation.agent !== "string" || (observation.agent !== root && !observation.agent.startsWith(`${root}/children/`))) {
    throw new Error("CLIENT_INPUT fixture is outside the authorized namespace");
  }
}
const stream = WasmStream.connect(endpoint, () => token);
try {
  stream.registerCredential(0, Uint8Array.from(Buffer.from(capability, "hex")));
  const report = toolPath
    ? await verifyClientToolOutput(stream, 0, decodeClientJson(encoded) as ClientToolOutputFixture)
    : controlPath
      ? await verifyClientControls(stream, 0, JSON.parse(new TextDecoder("utf-8", {fatal: true}).decode(encoded)) as ClientControlFixture)
      : await verifyClientObservation(stream, 0, observation!);
  console.log(JSON.stringify(report));
} finally { stream.free(); }
