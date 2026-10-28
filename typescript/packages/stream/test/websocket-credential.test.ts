import { expect, test } from "bun:test";
import { CredentialCache, CredentialSnapshot } from "@acyclic-labs/auth/credential";
import { create, toBinary } from "@bufbuild/protobuf";
import { SessionFrameSchema } from "../generated/proto/stream/v1/session_pb.js";
import { webSocketSession } from "../src/session-websocket.js";

// Synthetic frames use the SDK's canonical encoder over a real WebSocket.
test("WebSocket lease custody covers queued records, wire decode, writes and producer loss", async () => {
  const frame = create(SessionFrameSchema, { channelId: 1n, generation: 1n });
  const encoded = toBinary(SessionFrameSchema, frame);
  for (const operation of ["queued", "message", "send", "pending", "disconnect"] as const) {
    const changes = operation === "pending" ? ["invalidate"] as const
      : operation === "disconnect" ? ["unchanged"] as const
      : ["unchanged", "account", "unavailable", "expiry", "invalidate", "cancel"] as const;
    for (const change of changes) {
      let revision: string | undefined = "account/audience/1", now = 1, refreshes = 0;
      const cache = new CredentialCache(async () => {
        refreshes++;
        return new CredentialSnapshot("synthetic-websocket", revision!, 100);
      }, () => ({ nowUnixSeconds: now, revision }));
      let peer: Bun.ServerWebSocket<undefined> | undefined;
      let delivered!: (event: "message" | "close") => void;
      const received = new Promise<"message" | "close">(resolve => { delivered = resolve; });
      let sent!: () => void;
      const written = new Promise<void>(resolve => { sent = resolve; });
      const server = Bun.serve<undefined>({ hostname: "127.0.0.1", port: 0,
        fetch(request, server) {
          if (server.upgrade(request, { headers: { "Sec-WebSocket-Protocol": "acyclic.session.v1" }, data: undefined })) return;
          return new Response("upgrade failed", { status: 400 });
        },
        websocket: { open(socket) { peer = socket; }, message() { sent(); } },
      });
      const controller = new AbortController();
      const connect = webSocketSession("https://example.test", cache.provider, (_url, protocols) => {
        const socket = new WebSocket(server.url.href.replace("http:", "ws:"), protocols);
        socket.addEventListener("message", () => delivered("message"), { once: true });
        socket.addEventListener("close", () => delivered("close"), { once: true });
        return socket;
      });
      try {
        const transport = await connect(undefined, controller.signal);
        const incoming = transport.incoming[Symbol.asyncIterator]();
        expect(peer).toBeDefined();
        if (operation === "queued") {
          peer!.send(encoded);
          expect(await received).toBe("message");
        }
        // Install rejection custody before a source event can complete a pending read.
        const pending = operation === "pending" || operation === "disconnect" ? incoming.next().then(
          value => ({ value }), error => ({ error }),
        ) : undefined;
        switch (change) {
          case "account": revision = "other-account/audience/2"; break;
          case "unavailable": revision = undefined; break;
          case "expiry": now = 100; break;
          case "invalidate": cache.invalidate(); break;
          case "cancel": controller.abort(); break;
        }
        if (operation === "disconnect") peer!.close();
        // Invalid bytes prove freshness refusal happens before protobuf decoding.
        if (operation === "message") peer!.send(change === "unchanged" ? encoded : new Uint8Array([0xff]));
        if (operation === "send") {
          if (change === "unchanged") { await transport.send(frame); await written; }
          else await expect(transport.send(frame)).rejects.toHaveProperty("code", change === "cancel" ? "unavailable" : "access_denied");
        } else {
          const outcome = await (pending ?? incoming.next().then(value => ({ value }), error => ({ error })));
          if (change === "unchanged" && operation !== "disconnect") expect(outcome).toHaveProperty("value.value.channelId", 1n);
          else expect(outcome).toHaveProperty("error.code", change === "cancel" || operation === "disconnect" ? "unavailable" : "access_denied");
        }
        transport.close();
        revision = "account/audience/1"; now = 1;
        await cache.resolve();
        expect(refreshes).toBe(change === "invalidate" ? 2 : 1);
      } finally { controller.abort(); await server.stop(true); }
    }
  }
});
