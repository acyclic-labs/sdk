import { credentialProvider, type CredentialProvider } from "@acyclic-labs/actors/credential";
import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { SessionFrameSchema } from "../generated/proto/stream/v1/session_pb.js";
import { StreamLimit } from "../generated/proto/stream/v1/stream_pb.js";
import { SessionQueue } from "./session-queue.js";
import type { SessionConnect } from "./session.js";
import { StreamError } from "./types.js";

export type SessionWebSocketFactory = (url: string, protocols: string[]) => WebSocket;

/** Browser transport for the canonical binary Streams session, never one socket per follow. */
export function webSocketSession(endpoint: string, token: string | CredentialProvider, factory: SessionWebSocketFactory = (url, protocols) => new WebSocket(url, protocols)): SessionConnect {
  const provider = credentialProvider(typeof token === "string" ? { token } : { credential: token });
  return async (node, signal) => {
    const value = await provider(signal);
    const credential = btoa(String.fromCharCode(...new TextEncoder().encode(value))).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
    const base = new URL(node?.endpoint ?? endpoint);
    if (base.protocol !== "https:" || base.username || base.password || base.search || base.hash) throw new StreamError("configuration", "session node must be an authenticated HTTPS endpoint");
    if (!base.pathname.endsWith("/")) base.pathname += "/";
    const url = new URL("v1/stream/session", base);
    url.protocol = "wss:";
    const socket = factory(url.href, ["acyclic.session.v1", `acyclic.bearer.${credential}`]);
    socket.binaryType = "arraybuffer";
    const incoming = new SessionQueue<import("../generated/proto/stream/v1/session_pb.js").SessionFrame>(StreamLimit.MAX_ITEMS);
    let opened = false;
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const ready = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
    const abort = (): void => { socket.close(); incoming.end(); reject(new StreamError("unavailable", "session cancelled")); };
    const fail = (error: unknown): void => { reject(error); incoming.end(error); socket.close(); };
    signal.addEventListener("abort", abort, { once: true });
    socket.onopen = () => {
      if (socket.protocol !== "acyclic.session.v1") { fail(new StreamError("invalid_response", "Streams session subprotocol was not selected")); return; }
      opened = true;
      resolve();
    };
    socket.onmessage = event => {
      try {
        if (!(event.data instanceof ArrayBuffer) || event.data.byteLength > StreamLimit.MAX_COMMAND_BYTES) throw new StreamError("invalid_response", "invalid session message framing");
        incoming.push(fromBinary(SessionFrameSchema, new Uint8Array(event.data)));
      } catch (error) { fail(error instanceof StreamError ? error : new StreamError("invalid_response", "invalid session protobuf frame")); }
    };
    socket.onerror = () => fail(new StreamError("transport", "Streams session transport failed"));
    socket.onclose = () => {
      signal.removeEventListener("abort", abort);
      const error = new StreamError("unavailable", "Streams session transport closed");
      if (!opened) reject(error);
      incoming.end(error);
    };
    if (signal.aborted) abort();
    await ready;
    return {
      incoming,
      async send(frame) {
        if (signal.aborted || socket.readyState !== WebSocket.OPEN) throw new StreamError("unavailable", "Streams session is closed");
        const bytes = toBinary(SessionFrameSchema, frame);
        if (bytes.byteLength > StreamLimit.MAX_COMMAND_BYTES || socket.bufferedAmount + bytes.byteLength > StreamLimit.MAX_COMMAND_BYTES) throw new StreamError("capacity", "Streams session output capacity exceeded");
        socket.send(bytes);
      },
      close() { signal.removeEventListener("abort", abort); incoming.end(); socket.close(); },
    };
  };
}
