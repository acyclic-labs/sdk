import { create, equals, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { acknowledge, serve, spend } from "./model-driver.js";
import { SessionNode, checkpoint } from "./session-node.js";

// Test-only fault injection changes the actual module loaded by Bun. Each
// negative control must fail its stated invariant, not merely start a process.
const fault = process.env.STREAM_SESSION_FAULT;
const scenario = process.env.STREAM_SESSION_SCENARIO ?? "move";
if (fault === "followSelect") {
  const { plugin } = await import("bun");
  plugin({ name: "selection-negative-control", setup(build) {
    build.onLoad({ filter: /[\\/]src[\\/]default\.ts$/ }, async args => {
      const source = await Bun.file(args.path).text();
      const old = "this.acquireProvider(options.signal)";
      if (!source.includes(old)) throw new Error("follow selection negative control no longer matches");
      return { contents: source.replace(old, "this.acquireProvider(undefined)"), loader: "ts" };
    });
  } });
}
const { StreamSessions } = await import("../src/session.js");
const peer = new SessionNode(spend);
let rejectWrite = false;
let observedRejection = false;
const sessions = new StreamSessions(async (node, signal) => {
  const transport = await peer.connect(node, signal);
  return { ...transport, async send(frame, writeSignal) {
    if (rejectWrite && frame.body.case === "interest" && frame.body.value.upsert.some(item => item.kind.case === "follow" && item.kind.value.resource === "streams:/events")) {
      observedRejection = true;
      for (const notify of peer.changed) notify();
      if (fault === "rejection") await new Promise<void>(() => {});
      throw new Error("subscription rejected");
    }
    return transport.send(frame, writeSignal);
  } };
});
const controller = new AbortController();
const { DefaultStreamProvider } = await import("../src/default.js");
const facade = new DefaultStreamProvider({ endpoint: "https://home", token: "model" });
let releaseSelection: (() => void) | undefined;
Reflect.set(facade, "select", async () => {
  if (scenario === "followSelect") await new Promise<void>(resolve => { releaseSelection = resolve; });
  return { follow: (path: string, options: Parameters<StreamSessions["follow"]>[1]) => sessions.follow(path, options) };
});
const records = scenario === "rejection" ? sessions.follow("events", { from: 0n, signal: controller.signal })
  : facade.follow("agents", { from: 0n, signal: controller.signal });
if (scenario === "rejection") {
  peer.append("events", 2);
  await records.next();
}
let settled = false;
let rejected: unknown;
if (scenario === "followSelect") void records.next().then(() => { settled = true; }, error => { settled = true; rejected = error; });
async function command(action: string, hex?: string, path?: string,
  work?: { warm: number; samples: number; reverse: boolean; shared: boolean }, count?: number): Promise<unknown> {
  switch (action) {
    case "latency": {
      if (work === undefined || path === undefined) throw new Error("latency work missing");
      const { measure } = await import("./session-latency-measure.mjs");
      const { MemoryStreamProvider } = await import("../src/memory.js");
      const memory = new MemoryStreamProvider();
      const accepted = new Map<string, number>();
      const provider = {
        async append(path: string, values: readonly Uint8Array[], options: import("../src/types.js").AppendOptions) {
          const result = await memory.append(path, values, options);
          if (result.ok) accepted.set(path, (accepted.get(path) ?? 0) + values.length);
          return result;
        },
        follow: memory.follow.bind(memory),
      };
      let rejection: string | null = null;
      try {
        for (const runtime of work.reverse ? ["bun", "node"] : ["node", "bun"]) {
          await measure(provider, path, undefined, { warm: work.warm, samples: work.samples, runtime: work.shared ? "shared" : runtime });
        }
      } catch (error) { rejection = String(error); }
      const tails = [];
      for (const path of accepted.keys()) tails.push(Number(await memory.tail(path)));
      return { isolated: rejection === null, rejection, accepted: [...accepted.values()].reduce((sum, count) => sum + count, 0), tails };
    }
    case "ready":
      if (scenario === "rejection") {
        rejectWrite = true;
        peer.disconnect();
        await peer.until(() => observedRejection);
        void records.next().then(() => { settled = true; }, error => { settled = true; rejected = error; });
      }
      break;
    // Lifetime controls: a command that never settles while client work stays scheduled, a client that keeps
    // reconnecting, and an exact number of connections. None may outlive the owner or the process-wide budget.
    case "park":
      acknowledge("parked");
      await new Promise<void>(() => {});
      break;
    case "storm":
      for (;;) { try { await peer.connect(undefined, controller.signal); } catch { /* each attempt spends a step */ } }
    case "connect": {
      const before = peer.connections.length;
      for (let index = 0; index < (count ?? 0); index += 1) (await peer.connect(undefined, controller.signal)).close();
      return { connections: peer.connections.length - before };
    }
    case "releaseSelection": releaseSelection?.(); break;
    case "drop": await records.return(undefined); break;
    case "abort": controller.abort(); break;
    case "decode": {
      let decoded: wire.SessionFrame;
      try { decoded = fromBinary(wire.SessionFrameSchema, Buffer.from(hex ?? "", "hex")); }
      catch { return { valid: false, outcome: "invalid" }; }
      const encoded = toBinary(wire.SessionFrameSchema, decoded);
      if (!equals(wire.SessionFrameSchema, decoded, fromBinary(wire.SessionFrameSchema, encoded))) throw new Error("session frame round trip changed fields");
      const validatorPeer = new SessionNode(spend);
      const validator = new StreamSessions(validatorPeer.connect);
      const cancel = new AbortController();
      const iterator = validator.follow("probe", { from: 0n, signal: cancel.signal });
      let outcome = "pending";
      let sequence: number | undefined;
      const pending = iterator.next().then(result => {
        if (!result.done) { outcome = "record"; sequence = Number(result.value.sequence); }
      }, error => { outcome = error?.code ?? "unavailable"; });
      await validatorPeer.until(() => validatorPeer.active === 1);
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
  if (rejected !== undefined && scenario !== "rejection") throw rejected;
  return { settled, code: (rejected as { code?: string } | undefined)?.code,
    active: peer.active, outstanding: peer.outstandingFrames };
}
await serve((input: { action: string; hex?: string; path?: string; work?: Parameters<typeof command>[3]; count?: number }) =>
  command(input.action, input.hex, input.path, input.work, input.count));
