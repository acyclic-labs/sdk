/** Delivery-scoped socket effects over the SDK's canonical binary host codec. */
import { loadWasmModule } from "./client.js";
import { snapshotBytes } from "@acyclic-labs/auth/owned-bytes";
import type { GuestContext, SocketCommand, WebSocketAutoResponse } from "./generated/guest.js";

type Port = NonNullable<GuestContext<unknown>["socketHost"]>;

/** The host retains connections and configuration across guest hibernation.
 * Identity comes from the admitted delivery, never a caller-supplied actor. */
export class SocketHostClient {
  readonly #port: Port;
  constructor(port: Port) { this.#port = port; }

  async #call(command: SocketCommand, body: Uint8Array = new Uint8Array(), signal?: AbortSignal) {
    // Capture before resolving WASM: mutation by the caller cannot change an
    // operation already accepted by this async owner.
    const retained = snapshotBytes(body);
    const wasm = await loadWasmModule();
    signal?.throwIfAborted();
    const response = await this.#port(wasm.encodeSocketRequest(command, retained), signal);
    signal?.throwIfAborted();
    return wasm.decodeSocketReply(response);
  }

  async #done(command: SocketCommand, body?: Uint8Array, signal?: AbortSignal): Promise<void> {
    const { reply } = await this.#call(command, body, signal);
    if (reply.type !== "done") throw new Error("invalid socket mutation result");
  }

  send(connection: string, payload: string | Uint8Array, signal?: AbortSignal): Promise<void> {
    return this.#done({ type: "send", connection, binary: typeof payload !== "string" },
      typeof payload === "string" ? new TextEncoder().encode(payload) : payload, signal);
  }

  close(connection: string, code: number, reason: string, signal?: AbortSignal): Promise<void> {
    return this.#done({ type: "close", connection, code, reason }, undefined, signal);
  }

  async list(tag: string | null = null, signal?: AbortSignal): Promise<readonly string[]> {
    const { reply } = await this.#call({ type: "list", tag }, undefined, signal);
    if (reply.type !== "connections") throw new Error("invalid socket list result");
    return reply.connections;
  }

  setAttachment(connection: string, body: Uint8Array, signal?: AbortSignal): Promise<void> {
    return this.#done({ type: "setAttachment", connection }, body, signal);
  }

  async attachment(connection: string, signal?: AbortSignal): Promise<Uint8Array | null> {
    const { reply, body } = await this.#call({ type: "getAttachment", connection }, undefined, signal);
    if (reply.type !== "attachment") throw new Error("invalid socket attachment result");
    return reply.present ? body : null;
  }

  setAutoResponse(value: WebSocketAutoResponse | null, signal?: AbortSignal): Promise<void> {
    const pair = value === null ? null : { request: value.request, response: value.response };
    return this.#done({ type: "setAutoResponse", value: pair }, undefined, signal);
  }

  async autoResponse(signal?: AbortSignal): Promise<WebSocketAutoResponse | null> {
    const { reply } = await this.#call({ type: "getAutoResponse" }, undefined, signal);
    if (reply.type !== "autoResponse") throw new Error("invalid socket auto-response result");
    return reply.value;
  }

  async autoResponseTimestamp(connection: string, signal?: AbortSignal): Promise<bigint | null> {
    const { reply } = await this.#call({ type: "autoResponseTimestamp", connection }, undefined, signal);
    if (reply.type !== "timestamp") throw new Error("invalid socket timestamp result");
    return reply.unixMs === null ? null : BigInt(reply.unixMs);
  }

  async tags(connection: string, signal?: AbortSignal): Promise<readonly string[]> {
    const { reply } = await this.#call({ type: "tags", connection }, undefined, signal);
    if (reply.type !== "tags") throw new Error("invalid socket tags result");
    return reply.tags;
  }
}

/** Bind the original host port this delivery received. */
export function socketHost(context: Pick<GuestContext<unknown>, "socketHost">): SocketHostClient {
  if (context.socketHost === undefined) throw new Error("socket host unavailable");
  return new SocketHostClient(context.socketHost);
}
