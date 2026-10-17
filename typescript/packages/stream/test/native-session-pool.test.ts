import { test, expect } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { credentialTlsFixture } from "../../../test/credential-tls.js";
import { NativeStreamProvider } from "../src/native.js";
import { StreamError } from "../src/types.js";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { SessionNode, checkpoint } from "./session-node.js";

async function fixture(node: SessionNode, name: string, movedEndpoint: () => string,
  welcome?: { arrived: () => void; released: Promise<void> }) {
  let sessions = 0;
  let live = 0;
  const changed = new Set<() => void>();
  const host = await credentialTlsFixture(router => {
    router.service(wire.StreamSessionService, { async *session(input, context) {
      expect(context.requestHeader.get("authorization")).toBe("Bearer fixture");
      sessions++;
      live++;
      const transport = await node.connect(name === "home" ? undefined : create(wire.SessionNodeSchema, { nodeId: name }), context.signal);
      const close = () => transport.close();
      context.signal.addEventListener("abort", close, { once: true });
      const pump = async () => { try { for await (const frame of input) await transport.send(frame); } finally { close(); } };
      void pump().catch(close);
      try {
        for await (const frame of transport.incoming) {
          if (welcome && frame.body.case === "welcome" && sessions === 1) {
            welcome.arrived();
            await Promise.race([welcome.released, new Promise<void>(resolve => {
              if (context.signal.aborted) resolve();
              else context.signal.addEventListener("abort", () => resolve(), { once: true });
            })]);
            if (context.signal.aborted) return;
          }
          if (frame.body.case === "moved" && frame.body.value.node) frame.body.value.node.endpoint = movedEndpoint();
          yield frame;
        }
      } catch (error) {
        if (error instanceof StreamError && error.code === "unavailable") throw new ConnectError("fixture disconnected", Code.Unavailable);
        throw error;
      } finally {
        context.signal.removeEventListener("abort", close); close();
        live--; for (const notify of changed) notify();
      }
    } });
  });
  return { host, get sessions() { return sessions; }, get live() { return live; },
    closed: () => new Promise<void>(resolve => {
      const check = () => { if (live === 0) { changed.delete(check); resolve(); } };
      changed.add(check); check();
    }),
  };
}

test("native concurrent discovery and follows share one Session per node with local cancellation", async () => {
  const home = new SessionNode();
  const owner = new SessionNode();
  home.append("agents/a"); owner.append("agents/a");
  const target = await fixture(owner, "owner", () => { throw new Error("unexpected second move"); });
  const edge = await fixture(home, "home", () => target.host.endpoint);
  const controllers = Array.from({ length: 32 }, () => new AbortController());
  const healthy = new AbortController();
  let credentials = 0;
  try {
    const provider = await NativeStreamProvider.connect(edge.host.endpoint, undefined, Buffer.from(edge.host.certificate), undefined,
      signal => { expect(signal?.aborted).toBe(false); credentials++; return "fixture"; });
    const children = controllers.map(controller => provider.children({ parent: "agents", limit: 1, signal: controller.signal })[Symbol.asyncIterator]());
    const records = provider.follow("healthy", { from: 0n, signal: healthy.signal })[Symbol.asyncIterator]();
    try {
      const healthyRecord = records.next();
      const first = await Promise.all(children.map(child => child.next()));
      expect(first.every(result => result.value?.path === "agents/a")).toBe(true);
      await home.until(() => home.active === 33);
      expect(edge.sessions).toBe(1); expect(edge.host.connections).toBe(1);
      expect(credentials).toBe(1);
      const pending = children.map(child => child.next());
      home.move("agents");
      await owner.until(() => owner.active === 32);
      owner.append("agents/b");
      expect((await Promise.all(pending)).every(result => result.value?.path === "agents/b")).toBe(true);
      expect(target.sessions).toBe(1); expect(target.host.connections).toBe(1);
      expect(edge.sessions).toBe(1); expect(edge.host.connections).toBe(1);
      expect(credentials).toBe(2);
      controllers.slice(0, 16).forEach(controller => controller.abort());
      await owner.until(() => owner.active === 16);
      const remaining = children.slice(16).map(child => child.next());
      owner.append("agents/c"); home.append("healthy");
      expect((await Promise.all(remaining)).every(result => result.value?.path === "agents/c")).toBe(true);
      expect((await healthyRecord).value?.sequence).toBe(0n);
      expect(home.actorChannels + owner.actorChannels).toBe(0);
      expect(edge.sessions + target.sessions).toBe(2);
      expect(edge.host.connections + target.host.connections).toBe(2);
    } finally {
      controllers.forEach(controller => controller.abort()); healthy.abort();
      await Promise.all(children.map(child => child.return?.()));
      await records.return?.();
      await Promise.all([edge.closed(), target.closed()]);
      expect(edge.live + target.live).toBe(0);
    }
  } finally {
    await Promise.all([edge.host.close(), target.host.close()]);
  }
});

test("cancelling a discovery during shared initialization preserves its sibling", async () => {
  const node = new SessionNode(); node.append("agents/a");
  const peer = await fixture(node, "home", () => { throw new Error("unexpected move"); });
  const cancelled = new AbortController(), healthy = new AbortController();
  let started!: () => void, finish!: () => void;
  const initializing = new Promise<void>(resolve => { started = resolve; });
  const credential = new Promise<void>(resolve => { finish = resolve; });
  try {
    const provider = await NativeStreamProvider.connect(peer.host.endpoint, undefined, Buffer.from(peer.host.certificate), undefined,
      async () => { started(); await credential; return "fixture"; });
    const first = provider.children({ parent: "agents", limit: 1, signal: cancelled.signal })[Symbol.asyncIterator]();
    const second = provider.children({ parent: "agents", limit: 1, signal: healthy.signal })[Symbol.asyncIterator]();
    try {
      const abandoned = first.next(), surviving = second.next();
      await initializing;
      cancelled.abort();
      expect((await abandoned).done).toBe(true);
      finish();
      expect((await surviving).value?.path).toBe("agents/a");
      expect(peer.sessions).toBe(1); expect(peer.host.connections).toBe(1);
      expect(node.actorChannels).toBe(0);
    } finally {
      finish(); cancelled.abort(); healthy.abort();
      await Promise.all([first.return?.(), second.return?.()]);
      await peer.closed(); expect(peer.live).toBe(0);
    }
  } finally { await peer.host.close(); }
});

test("a cancelled waiter cannot cancel a shared Session paused before Welcome", async () => {
  const node = new SessionNode(); node.append("agents/a");
  let arrived!: () => void, finish!: () => void;
  const paused = new Promise<void>(resolve => { arrived = resolve; });
  const released = new Promise<void>(resolve => { finish = resolve; });
  const peer = await fixture(node, "home", () => { throw new Error("unexpected move"); }, { arrived, released });
  const cancelled = new AbortController(), healthy = new AbortController();
  try {
    const provider = await NativeStreamProvider.connect(peer.host.endpoint, undefined, Buffer.from(peer.host.certificate), undefined, () => "fixture");
    const first = provider.children({ parent: "agents", limit: 1, signal: cancelled.signal })[Symbol.asyncIterator]();
    const second = provider.children({ parent: "agents", limit: 1, signal: healthy.signal })[Symbol.asyncIterator]();
    try {
      const abandoned = first.next();
      await paused;
      const surviving = second.next();
      await checkpoint();
      cancelled.abort();
      expect((await abandoned).done).toBe(true);
      expect(peer.live).toBe(1); expect(peer.sessions).toBe(1);
      finish();
      expect((await surviving).value?.path).toBe("agents/a");
      expect(peer.sessions).toBe(1); expect(peer.host.connections).toBe(1);
    } finally {
      finish(); cancelled.abort(); healthy.abort();
      await Promise.all([first.return?.(), second.return?.()]);
      await peer.closed(); expect(peer.live).toBe(0);
    }
  } finally { await peer.host.close(); }
});

test("new discovery abandons a cancelled home reconnect before its credential settles", async () => {
  const node = new SessionNode(); node.append("agents/a");
  const peer = await fixture(node, "home", () => { throw new Error("unexpected move"); });
  const cancelled = new AbortController(), healthy = new AbortController();
  let started!: () => void, aborted!: () => void, finish!: () => void;
  const reconnecting = new Promise<void>(resolve => { started = resolve; });
  const cancellation = new Promise<void>(resolve => { aborted = resolve; });
  let calls = 0, settled = false;
  try {
    const provider = await NativeStreamProvider.connect(peer.host.endpoint, undefined, Buffer.from(peer.host.certificate), undefined,
      signal => {
        if (++calls !== 2) return "fixture";
        started();
        return new Promise<string>((_resolve, reject) => {
          signal!.addEventListener("abort", aborted, { once: true });
          finish = () => { settled = true; reject(signal!.reason); };
        });
      });
    const first = provider.children({ parent: "agents", limit: 1, signal: cancelled.signal })[Symbol.asyncIterator]();
    const second = provider.children({ parent: "agents", limit: 1, signal: healthy.signal })[Symbol.asyncIterator]();
    try {
      expect((await first.next()).value?.path).toBe("agents/a");
      const abandoned = first.next().then(result => ({ result }), error => ({ error }));
      node.disconnect();
      await reconnecting;
      cancelled.abort();
      const replacement = second.next().catch(error => {
        throw new Error(`replacement failed: credentials=${calls}, sessions=${peer.sessions}, live=${peer.live}, settled=${settled}`, { cause: error });
      });
      await cancellation;
      expect(await abandoned).toEqual({ result: { done: true, value: undefined } });
      expect((await replacement).value?.path).toBe("agents/a");
      expect(settled).toBe(false);
      expect(calls).toBe(3);
      expect(peer.sessions).toBe(2);
      expect(peer.host.connections).toBe(1);
      expect(node.actorChannels).toBe(0);
      finish();
      const next = second.next(); node.append("agents/b");
      expect((await next).value?.path).toBe("agents/b");
    } finally {
      finish?.(); cancelled.abort(); healthy.abort();
      await Promise.all([first.return?.(), second.return?.()]);
      await peer.closed(); expect(peer.live).toBe(0);
    }
  } finally { await peer.host.close(); }
});
