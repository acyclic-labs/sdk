import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { SessionFrameSchema, type SessionFrame } from "../generated/proto/stream/v1/session_pb.js";
import type { SessionWebSocketFactory } from "../src/session-websocket.js";
import { SessionNode } from "./session-node.js";

/** A binary-message peer for deterministic browser cancellation/parser tests. */
export class SessionSocket {
  readonly node = new SessionNode();
  readonly sockets: WebSocket[] = [];
  transform: (frame: SessionFrame) => SessionFrame | string | Uint8Array = frame => frame;
  readonly factory: SessionWebSocketFactory = (_url, protocols) => {
    if (!protocols.includes("acyclic.session.v1")) throw new Error("missing session protocol");
    const transport = this.node.connect(undefined, new AbortController().signal);
    let closed = false;
    const socket = {
      protocol: "acyclic.session.v1", binaryType: "arraybuffer", bufferedAmount: 0, readyState: 0,
      onopen: null, onclose: null, onerror: null, onmessage: null,
      send(input: Uint8Array) {
        void transport.then(peer => peer.send(fromBinary(SessionFrameSchema, input))).catch(() => socket.onerror?.(new Event("error")));
      },
      close() {
        if (closed) return;
        closed = true; socket.readyState = 3;
        void transport.then(peer => peer.close());
        queueMicrotask(() => socket.onclose?.(new CloseEvent("close")));
      },
    } as unknown as WebSocket;
    this.sockets.push(socket);
    queueMicrotask(() => {
      if (closed) return;
      Object.defineProperty(socket, "readyState", { value: 1, writable: true });
      socket.onopen?.(new Event("open"));
    });
    void (async () => {
      try {
        for await (const output of (await transport).incoming) {
          const transformed = this.transform(output);
          const data = typeof transformed === "string" ? transformed : (transformed instanceof Uint8Array ? transformed : toBinary(SessionFrameSchema, transformed)).slice().buffer;
          socket.onmessage?.(new MessageEvent("message", { data }));
        }
      } catch { socket.onerror?.(new Event("error")); }
    })();
    return socket;
  };
  close(): void { for (const socket of this.sockets) socket.close(); }
}
