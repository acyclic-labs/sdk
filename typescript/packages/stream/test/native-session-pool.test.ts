import { test, expect } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { credentialTlsFixture } from "../../../test/credential-tls.js";
import { NativeStreamProvider } from "../src/native.js";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { SessionNode } from "./session-node.js";

async function fixture(node: SessionNode, name: string, movedEndpoint: () => string) {
  let sessions = 0;
  const host = await credentialTlsFixture(router => {
    router.service(wire.StreamSessionService, { async *session(input, context) {
      expect(context.requestHeader.get("authorization")).toBe("Bearer fixture");
      sessions++;
      const transport = await node.connect(name === "home" ? undefined : create(wire.SessionNodeSchema, { nodeId: name }), context.signal);
      const close = () => transport.close();
      context.signal.addEventListener("abort", close, { once: true });
      const pump = async () => { try { for await (const frame of input) await transport.send(frame); } finally { close(); } };
      void pump().catch(close);
      try {
        for await (const frame of transport.incoming) {
          if (frame.body.case === "moved" && frame.body.value.node) frame.body.value.node.endpoint = movedEndpoint();
          yield frame;
        }
      } finally { context.signal.removeEventListener("abort", close); close(); }
    } });
  });
  return { host, get sessions() { return sessions; } };
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
    }
  } finally { await peer.host.close(); }
});
