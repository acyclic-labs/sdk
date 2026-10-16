import { createInterface } from "node:readline";
import { create, equals, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { SessionNode, checkpoint } from "./session-node.js";
import { StreamError } from "../src/types.js";

// Test-only fault injection changes the actual module loaded by Bun. Each
// negative control must fail its stated invariant, not merely start a process.
const fault = process.env.STREAM_SESSION_FAULT;
const scenario = process.env.STREAM_SESSION_SCENARIO ?? "move";
const occupancy = { active: 0, minimum: 0 };
(globalThis as typeof globalThis & { sessionModelOccupancy: typeof occupancy }).sessionModelOccupancy = occupancy;
if (fault || scenario === "reservation") {
  const { plugin } = await import("bun");
  plugin({ name: "session-negative-control", setup(build) {
    build.onLoad({ filter: /[\\/]src[\\/]session\.ts$/ }, async args => {
      let source = await Bun.file(args.path).text();
      if (scenario === "reservation") {
        // Observe the actual private counter without adding a production API.
        for (const operator of ["+=", "-="]) {
          const statement = `this.#active ${operator} 1;`;
          if (!source.includes(statement)) throw new Error("reservation probe no longer matches");
          source = source.replaceAll(statement, `${statement} globalThis.sessionModelOccupancy.active = this.#active; globalThis.sessionModelOccupancy.minimum = Math.min(globalThis.sessionModelOccupancy.minimum, this.#active);`);
        }
      }
      if (!fault) return { contents: source, loader: "ts" };
      const old = fault === "resume" ? "route.placement = target.placementEpoch;"
        : fault === "cancel" ? "if (route !== undefined) this.#remove(route); };"
        : fault === "control" ? "try { await untilAborted(link.transport.send(input, signal), signal, link.controller.signal); }\n    catch (error) { if (!signal?.aborted) this.#lost(link, error); }"
        : fault === "resubscribe" ? "catch (error) { if (route.link === next && route.generation === generation) this.#fail(route, error); }"
        : fault === "reservation" ? "const abort = (): void => { release();"
        : "queue.discard(delivery => delivery.release());";
      if (!source.includes(old)) throw new Error(`negative control no longer matches: ${fault}`);
      const replacement = fault === "resume" ? "route.applied = body.value.resume?.next ?? route.applied; route.placement = target.placementEpoch;"
        : fault === "control" ? "await link.transport.send(input, signal);"
        : fault === "resubscribe" ? "catch (error) { if (route.link === link) this.#fail(route, error); }"
        : fault === "reservation" ? "const abort = (): void => {"
        : fault === "cancel" ? "};" : "queue.discard(() => {});";
      source = source.replaceAll(old, replacement);
      if (fault === "reservation") source = source.replaceAll("route.release();", "");
      return { contents: source, loader: "ts" };
    });
  } });
}
const { StreamSessions } = await import("../src/session.js");
const peer = new SessionNode();
const parked: (() => void)[] = [];
const acknowledgements: (() => void)[] = [];
let frozen = true;
let ackFrozen = true;
let ackStarted = false;
const sessions = new StreamSessions(async (node, signal) => {
  const transport = await peer.connect(node, signal);
  const connection = peer.connections.length;
  return { ...transport, async send(input) {
    if (input.channelId === 0n && input.body.case === "window" && frozen) await new Promise<void>(resolve => parked.push(resolve));
    if (scenario === "ack" && input.body.case === "ack" && ackFrozen) {
      ackStarted = true;
      await new Promise<void>(resolve => acknowledgements.push(resolve));
    }
    if (scenario === "reject" && connection > 1 && input.body.case === "interest" && input.body.value.upsert.length) {
      throw new StreamError("unavailable", "resubscription write rejected");
    }
    await transport.send(input);
  } };
});
const controller = new AbortController();
if (scenario === "drop") peer.append("events", 2);
if (["immediate", "ack", "reject", "reservation"].includes(scenario)) peer.append("events");
const records = sessions.follow("events", { from: 0n, signal: controller.signal });
const seen: number[] = [];
let settled = false;
let rejected: unknown;
let appliedAtMove = 0;
let seenAtAbort: number[] = [];
function receive(): void {
  settled = false;
  void records.next().then(result => { settled = true; if (!result.done) seen.push(Number(result.value.sequence)); }, error => { settled = true; rejected = error; });
}
if (scenario !== "decode") receive();
async function command(action: string, hex?: string): Promise<unknown> {
  switch (action) {
    case "ready": break;
    case "append": peer.append("events"); break;
    case "consume": receive(); break;
    case "disconnectNext": peer.disconnect(); receive(); break;
    case "disconnect": peer.disconnect(); break;
    case "releaseAck": ackFrozen = false; for (const resolve of acknowledgements.splice(0)) resolve(); break;
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
  if (scenario === "ack" && action === "consume" && !ackStarted) throw new Error("model did not park the actual acknowledgement write");
  if (rejected !== undefined && !["immediate", "ack", "reject"].includes(scenario)) throw rejected;
  return { settled, seen, seenAtAbort, appliedAtMove, connections: peer.connections.length,
    activeReservations: occupancy.active, minimumReservations: occupancy.minimum,
    rejected: rejected === undefined ? null : String(rejected), rejectedCode: rejected instanceof StreamError ? rejected.code : null, ackStarted,
    resumes: peer.resumes.filter(resume => resume.node === "owner").map(resume => Number(resume.next)),
    active: peer.active, outstanding: peer.outstandingFrames, returned: peer.returned };
}
for await (const line of createInterface({ input: process.stdin })) {
  try { const input = JSON.parse(line); process.stdout.write(`${JSON.stringify(await command(input.action, input.hex))}\n`); }
  catch (error) { process.stdout.write(`${JSON.stringify({ error: String(error) })}\n`); }
}
controller.abort();
