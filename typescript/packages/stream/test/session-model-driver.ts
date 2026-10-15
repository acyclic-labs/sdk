import { createInterface } from "node:readline";
import { create, equals, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { SessionNode, checkpoint } from "./session-node.js";

// Test-only fault injection changes the actual module loaded by Bun. Each
// negative control must fail its stated invariant, not merely start a process.
const fault = process.env.STREAM_SESSION_FAULT;
if (fault) {
  const { plugin } = await import("bun");
  plugin({ name: "session-negative-control", setup(build) {
    build.onLoad({ filter: /[\\/]src[\\/]session\.ts$/ }, async args => {
      let source = await Bun.file(args.path).text();
      const old = fault === "resume" ? "route.placement = target.placementEpoch;"
        : fault === "cancel" ? "if (route !== undefined) this.#remove(route); };"
        : "queue.discard(delivery => delivery.release());";
      if (!source.includes(old)) throw new Error(`negative control no longer matches: ${fault}`);
      const replacement = fault === "resume" ? "route.applied = body.value.resume?.next ?? route.applied; route.placement = target.placementEpoch;"
        : fault === "cancel" ? "};" : "queue.discard(() => {});";
      source = source.replaceAll(old, replacement);
      return { contents: source, loader: "ts" };
    });
  } });
}
const { StreamSessions } = await import("../src/session.js");
const scenario = process.env.STREAM_SESSION_SCENARIO ?? "move";
const peer = new SessionNode();
const parked: (() => void)[] = [];
let frozen = true;
const sessions = new StreamSessions(async (node, signal) => {
  const transport = await peer.connect(node, signal);
  return { ...transport, async send(input) {
    if (input.channelId === 0n && input.body.case === "window" && frozen) await new Promise<void>(resolve => parked.push(resolve));
    await transport.send(input);
  } };
});
const controller = new AbortController();
if (scenario === "drop") peer.append("events", 2);
const records = sessions.follow("events", { from: 0n, signal: controller.signal });
const seen: number[] = [];
let settled = false;
let rejected: unknown;
let appliedAtMove = 0;
let seenAtAbort: number[] = [];
function receive(): void {
  void records.next().then(result => { settled = true; if (!result.done) seen.push(Number(result.value.sequence)); }, error => { settled = true; rejected = error; });
}
if (scenario !== "decode") receive();
async function command(action: string, hex?: string): Promise<unknown> {
  switch (action) {
    case "ready": break;
    case "append": peer.append("events"); break;
    case "consume": receive(); break;
    case "drop": await records.return(undefined); break;
    case "move": appliedAtMove = seen.length; peer.move("events"); break;
    case "release": frozen = false; for (const resolve of parked.splice(0)) resolve(); break;
    case "abort": seenAtAbort = [...seen]; controller.abort(); break;
    case "decode": {
      let decoded: wire.SessionFrame;
      try { decoded = fromBinary(wire.SessionFrameSchema, Buffer.from(hex ?? "", "hex")); }
      catch { return { valid: false, outcome: "invalid" }; }
      const encoded = toBinary(wire.SessionFrameSchema, decoded);
      if (!equals(wire.SessionFrameSchema, decoded, fromBinary(wire.SessionFrameSchema, encoded))) throw new Error("session frame round trip changed fields");
      const validatorPeer = new SessionNode();
      const validator = new StreamSessions(validatorPeer.connect);
      const cancel = new AbortController();
      const iterator = validator.follow("probe", { from: 0n, signal: cancel.signal });
      let outcome = "pending";
      let sequence: number | undefined;
      const pending = iterator.next().then(result => {
        if (!result.done) { outcome = "record"; sequence = Number(result.value.sequence); }
      }, error => { outcome = error?.code ?? "unavailable"; });
      await checkpoint();
      validatorPeer.connections[0]!.output.push(decoded);
      await checkpoint();
      cancel.abort();
      await pending;
      await iterator.return(undefined);
      for (const connection of validatorPeer.connections) validatorPeer.disconnect(connection.node);
      await checkpoint();
      if (decoded.channelId === 1n && decoded.generation === 1n && decoded.body.case === "closed") {
        const code = decoded.body.value.code;
        const canonical = new Map([["not_found", "stream_not_found"], ["already_exists", "destination_exists"], ["capacity", "capacity_exhausted"]]);
        const retained = new Set(["invalid_path", "invalid_argument", "limit_exceeded", "prefix_not_retained", "out_of_range", "idempotency_mismatch", "access_denied", "unavailable", "hierarchy_changed", "deadline_elapsed", "unsupported"]);
        if (outcome !== (canonical.get(code) ?? (retained.has(code) ? code : "unavailable"))) throw new Error("channel close changed the public error code");
      }
      return { valid: true, outcome, sequence };
    }
    default: throw new Error(`unknown action ${action}`);
  }
  await checkpoint();
  if (rejected !== undefined) throw rejected;
  return { settled, seen, seenAtAbort, appliedAtMove, connections: peer.connections.length,
    resumes: peer.resumes.filter(resume => resume.node === "owner").map(resume => Number(resume.next)),
    active: peer.active, outstanding: peer.outstandingFrames, returned: peer.returned };
}
for await (const line of createInterface({ input: process.stdin })) {
  try { const input = JSON.parse(line); process.stdout.write(`${JSON.stringify(await command(input.action, input.hex))}\n`); }
  catch (error) { process.stdout.write(`${JSON.stringify({ error: String(error) })}\n`); }
}
controller.abort();
