import { credentialProvider, credentialStream, resolveCredential, type CredentialProvider } from "@acyclic-labs/auth/credential";
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
    const watch = provider.watch?.();
    const sourceSignal = watch ? AbortSignal.any([signal, watch.changed]) : signal;
    let watching = true;
    const unwatch = (): void => { if (watching) { watching = false; watch?.close(); } };
    try {
      const lease = await resolveCredential(provider, sourceSignal);
      const credential = btoa(String.fromCharCode(...new TextEncoder().encode(lease.token()))).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
      const base = new URL(node?.endpoint ?? endpoint);
      if (base.protocol !== "https:" || base.username || base.password || base.search || base.hash) throw new StreamError("configuration", "session node must be an authenticated HTTPS endpoint");
      if (!base.pathname.endsWith("/")) base.pathname += "/";
      const url = new URL("v1/stream/session", base);
      url.protocol = "wss:";
      lease.check();
      sourceSignal.throwIfAborted();
      const socket = factory(url.href, ["acyclic.session.v1", `acyclic.bearer.${credential}`]);
      socket.binaryType = "arraybuffer";
      const incoming = new SessionQueue<import("../generated/proto/stream/v1/session_pb.js").SessionFrame>(StreamLimit.MAX_ITEMS);
      let closed = false;
      let resolve!: () => void;
      let reject!: (error: unknown) => void;
      const ready = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
      const cleanup = (): void => {
        signal.removeEventListener("abort", abort);
        watch?.changed.removeEventListener("abort", changed);
        unwatch();
      };
      const close = (error?: unknown): void => {
        if (closed) return;
        closed = true;
        cleanup();
        reject(error ?? new StreamError("unavailable", "session cancelled"));
        incoming.end(error);
        socket.close();
      };
      const abort = (): void => close(new StreamError("unavailable", "session cancelled"));
      const changed = (): void => close(new StreamError("access_denied", "session credential source changed or unavailable"));
      const check = (): void => {
        if (signal.aborted) {
          abort();
          throw new StreamError("unavailable", "session cancelled");
        }
        try { lease.check(); }
        catch {
          const error = new StreamError("access_denied", "session credential source changed or expired");
          close(error);
          throw error;
        }
      };
      signal.addEventListener("abort", abort, { once: true });
      watch?.changed.addEventListener("abort", changed, { once: true });
      socket.onopen = () => {
        try {
          check();
          if (closed) return;
          if (socket.protocol !== "acyclic.session.v1") throw new StreamError("invalid_response", "Streams session subprotocol was not selected");
          resolve();
        } catch (error) { close(error); }
      };
      socket.onmessage = event => {
        if (closed) return;
        try {
          check();
          if (!(event.data instanceof ArrayBuffer) || event.data.byteLength > StreamLimit.MAX_COMMAND_BYTES) throw new StreamError("invalid_response", "invalid session message framing");
          incoming.push(fromBinary(SessionFrameSchema, new Uint8Array(event.data)));
        } catch (error) { close(error instanceof StreamError ? error : new StreamError("invalid_response", "invalid session protobuf frame")); }
      };
      socket.onerror = () => close(new StreamError("transport", "Streams session transport failed"));
      socket.onclose = () => close(new StreamError("unavailable", "Streams session transport closed"));
      if (signal.aborted) abort();
      else if (watch?.changed.aborted) changed();
      await ready;
      check();
      return {
        incoming: credentialStream(incoming, {
          token: () => lease.token(), check, reject: () => false,
        }, () => false),
        async send(frame, writeSignal) {
          writeSignal?.throwIfAborted();
          check();
          if (closed || socket.readyState !== WebSocket.OPEN) throw new StreamError("unavailable", "Streams session is closed");
          const bytes = toBinary(SessionFrameSchema, frame);
          if (bytes.byteLength > StreamLimit.MAX_COMMAND_BYTES || socket.bufferedAmount + bytes.byteLength > StreamLimit.MAX_COMMAND_BYTES) throw new StreamError("capacity", "Streams session output capacity exceeded");
          socket.send(bytes);
        },
        close() { close(); },
      };
    } catch (error) {
      unwatch();
      if (!signal.aborted && watch?.changed.aborted) throw new StreamError("access_denied", "session credential source changed or unavailable");
      throw error;
    }
  };
}
