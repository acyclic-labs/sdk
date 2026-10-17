import { plugin } from "bun";
import { Code, ConnectError } from "@connectrpc/connect";
import { MODEL_STEPS, serve } from "./model-driver.js";
import { SessionNode, checkpoint } from "./session-node.js";

// Only the transport factory is substituted: the public raw-follow wrapper,
// StreamSessions, and Connect's deadline/link helpers run unchanged. The clock
// fires the real helper callback, without wall time or a second implementation.
const peer = new SessionNode(MODEL_STEPS);
const fault = process.env.STREAM_SESSION_FAULT;
const requestedScenario = process.env.STREAM_SESSION_SCENARIO ?? "deadline";
const children = requestedScenario.endsWith("-children");
const scenario = requestedScenario.replace(/-children$/, "");
plugin({ name: "raw-follow-deadline-model", setup(build) {
  build.onLoad({ filter: /[\\/]src[\\/]session\.ts$/ }, async args => {
    let source = await Bun.file(args.path).text();
    if (fault === "write") {
      const cancellation = "const abort = (): void => cursor?.close();";
      if (!source.includes(cancellation)) throw new Error("blocked-write negative control no longer matches");
      source = source.replace(cancellation, "const abort = (): void => {}; ");
    }
    if (fault === "writeSignal") {
      const forwarding = "transport.send(fromBinary(wire.SessionFrameSchema, bytes), writeSignal)";
      if (!source.includes(forwarding)) throw new Error("write signal negative control no longer matches");
      source = source.replace(forwarding, "transport.send(fromBinary(wire.SessionFrameSchema, bytes))");
    }
    return { contents: source, loader: "ts" };
  });
  build.onLoad({ filter: /[\\/]src[\\/]grpc\.ts$/ }, async args => {
    let source = await Bun.file(args.path).text();
    const factory = "const { client, sessions } = streamGrpc(options);";
    if (!source.includes(factory)) throw new Error("raw-follow factory probe no longer matches");
    source = source.replace(factory, "const { client, sessions } = { client: {}, sessions: globalThis.sessionDeadlineSessions };");
    if (fault === "deadline") {
      const forwarding = "const { signal, timeoutMs } = callOptions ?? {};";
      if (fault !== "deadline" || !source.includes(forwarding)) throw new Error("deadline negative control no longer matches");
      source = source.replaceAll(forwarding, "const { signal } = callOptions ?? {}; const timeoutMs = undefined;");
    }
    return { contents: source, loader: "ts" };
  });
} });
const { StreamSessions } = await import("../src/session.js");
let releaseWrite: (() => void) | undefined;
let blocked = false;
let writesReleased = false;
let readData = 0;
let resumeConsumer: (() => void) | undefined;
let consumerPaused = false;
const sessions = new StreamSessions(async (node, signal) => {
  const transport = await peer.connect(node, signal);
  return { ...transport, incoming: { async *[Symbol.asyncIterator]() {
    for await (const frame of transport.incoming) {
      if (frame.body.case === "data") readData += 1;
      for (const notify of peer.changed) notify();
      yield frame;
    }
  } }, async send(input, writeSignal) {
    const subscription = input.body.case === "interest" && input.body.value.upsert.some(value => value.kind.case === "follow" && value.kind.value.path === "events" || value.kind.case === "children" && value.kind.value.parent === "events");
    const acknowledgement = input.body.case === "ack";
    if (!writesReleased && ((scenario === "deadline-subscription" && subscription) || (scenario === "deadline-ack" && acknowledgement))) {
      blocked = true;
      for (const notify of peer.changed) notify();
      await new Promise<void>(resolve => { releaseWrite = resolve; });
    }
    writeSignal?.throwIfAborted();
    await transport.send(input, writeSignal);
  } };
});
Object.assign(globalThis, { sessionDeadlineSessions: sessions });
const { createStreamGrpcClient } = await import("../src/grpc.js");
const nativeSetTimeout = globalThis.setTimeout;
const nativeClearTimeout = globalThis.clearTimeout;
const duration = 421337;
const handle = {};
let callback: (() => void) | undefined;
let timerActive = false;
let timerCreated = 0;
let timerCleaned = 0;
Object.assign(globalThis, {
  setTimeout(fn: () => void, delay?: number, ...args: unknown[]) {
    if (delay !== duration) return nativeSetTimeout(fn, delay, ...args);
    timerCreated += 1;
    timerActive = true;
    callback = fn;
    return handle;
  },
  clearTimeout(value: ReturnType<typeof setTimeout>) {
    if (value !== handle) return nativeClearTimeout(value);
    timerActive = false;
    timerCleaned += 1;
  },
});
const client = createStreamGrpcClient({ endpoint: "https://home", token: "model" });
const cancel = new AbortController();
const siblingCancel = new AbortController();
let settled = false;
let code: number | null = null;
let cleanup = false;
let expired = false;
let seen = 0;
let stoppedAt: number | undefined;
let siblingSeen = 0;
let siblingError: unknown;
const sibling = (async () => {
  try {
    for await (const _ of client.follow({ path: "healthy", from: 0n }, { timeoutMs: 0, signal: siblingCancel.signal })) siblingSeen += 1;
  } catch (error) { if (!siblingCancel.signal.aborted) siblingError = error; }
})();
// Establish the healthy channel before parking the other channel's write.
await peer.until(() => peer.active === 1);
const records = (children ? client.children({ parent: "events", limit: 1 }, { timeoutMs: duration, signal: cancel.signal })
  : client.follow({ path: "events", from: 0n }, { timeoutMs: duration, signal: cancel.signal }))[Symbol.asyncIterator]();
const follow = (async () => {
  try {
    for await (const _ of { [Symbol.asyncIterator]: () => records }) {
      seen += 1;
      if (scenario === "deadline-ack" && seen === 1) {
        consumerPaused = true;
        for (const notify of peer.changed) notify();
        await new Promise<void>(resolve => { resumeConsumer = resolve; });
      }
    }
  } catch (error) { code = error instanceof ConnectError ? error.code : -1; }
  finally { settled = true; }
})();
let returned = false;
let releaseReconnect: (() => void) | undefined;
let connecting = false;
async function command(action: string): Promise<unknown> {
  switch (action) {
    case "ready":
      if (scenario === "deadline-ack") {
        if (children) { peer.append("events/child"); peer.append("events/other"); peer.append("events/third"); }
        else peer.append("events", 3);
        // The first yield releases one frame; fill both remaining slots while
        // its JS consumer is paused, then consume to force the real Ack path.
        await peer.until(() => readData === 3 && consumerPaused);
        await checkpoint();
        resumeConsumer!();
      }
      if (scenario.startsWith("deadline-")) {
        await peer.until(() => blocked);
        if (!blocked) throw new Error("model did not park the actual channel write");
      } else await peer.until(() => peer.active === 2);
      break;
    case "releaseWrite": writesReleased = true; releaseWrite?.(); break;
    case "return": void records.return?.().then(() => { returned = true; }); break;
    case "deliver": peer.append(children ? "events/child" : "events"); break;
    case "expire":
      expired = true;
      stoppedAt ??= seen;
      if (timerActive) { timerActive = false; callback!(); }
      break;
    case "reconnect":
      peer.beforeConnect = async () => {
        connecting = true;
        await new Promise<void>(resolve => { releaseReconnect = resolve; });
      };
      peer.disconnect();
      break;
    case "releaseReconnect": peer.beforeConnect = undefined; releaseReconnect?.(); break;
    case "cleanup": cleanup = true; stoppedAt ??= seen; cancel.abort(); break;
    case "finish":
      peer.beforeConnect = undefined;
      releaseReconnect?.();
      await checkpoint();
      peer.append("healthy");
      await checkpoint();
      break;
    case "stop":
      cancel.abort(); siblingCancel.abort();
      await Promise.all([follow, sibling]);
      break;
    default: throw new Error(`unknown deadline action ${action}`);
  }
  await checkpoint();
  return { settled, returned, blocked, siblingError: siblingError === undefined ? null : String(siblingError), code, cleanup, expired, seen, stoppedAt, siblingSeen,
    active: peer.active, outstanding: peer.outstandingFrames, connections: peer.connections.length, connecting,
    timerActive, timerCreated, timerCleaned, deadlineCode: Code.DeadlineExceeded };
}
await serve((input: { action: string }) => command(input.action));
