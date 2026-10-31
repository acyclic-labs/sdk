import { ActorActivation } from "./activation.js";
export { ActorActivation } from "./activation.js";
import { ActorReference } from "@acyclic-labs/auth/addressing";
import type { WebSocketAutoResponse } from "./generated/guest.js";

/** Host adapter admitted for one actor. Serialized addresses grant no authority. */
export interface DurableHost {
  deleteAll(actor: Uint8Array): Promise<void>;
  acceptWebSocket(actor: Uint8Array, socket: WebSocket, tags: readonly string[]): void;
  getWebSockets(actor: Uint8Array, tag?: string): WebSocket[];
  serializeAttachment(actor: Uint8Array, socket: WebSocket, bytes: Uint8Array): void;
  deserializeAttachment(actor: Uint8Array, socket: WebSocket): Uint8Array | null;
  setWebSocketAutoResponse(actor: Uint8Array, pair: WebSocketAutoResponse | null): void;
  getWebSocketAutoResponse(actor: Uint8Array): WebSocketAutoResponse | null;
  getWebSocketAutoResponseTimestamp(actor: Uint8Array, socket: WebSocket): bigint | null;
  getTags(actor: Uint8Array, socket: WebSocket): string[];
}

/** One activation's scoped state. The host persists socket attachments; alarms commit with the checkpoint (acyclic_actors::local). */
export class DurableObjectState {
  readonly #reference: ActorReference;
  readonly #host: DurableHost;
  constructor(reference: ActorReference, host: DurableHost) {
    this.#reference = ActorReference.decode(reference.encode());
    this.#host = host;
  }
  get id(): ActorReference { return ActorReference.decode(this.#reference.encode()); }
  #address(): Uint8Array { return this.#reference.encode(); }
  deleteAll(): Promise<void> { return this.#host.deleteAll(this.#address()); }
  acceptWebSocket(socket: WebSocket, tags: readonly string[] = []): void {
    tags = [...tags];
    this.#host.acceptWebSocket(this.#address(), socket, tags);
  }
  getWebSockets(tag?: string): WebSocket[] {
    return this.#host.getWebSockets(this.#address(), tag);
  }
  serializeAttachment(socket: WebSocket, bytes: Uint8Array): void {
    this.#host.serializeAttachment(this.#address(), socket, new Uint8Array(bytes));
  }
  deserializeAttachment(socket: WebSocket): Uint8Array | null {
    const bytes = this.#host.deserializeAttachment(this.#address(), socket);
    return bytes === null ? null : new Uint8Array(bytes);
  }
  setWebSocketAutoResponse(pair: WebSocketAutoResponse | null): void {
    this.#host.setWebSocketAutoResponse(this.#address(), pair === null ? null : { ...pair });
  }
  getWebSocketAutoResponse(): WebSocketAutoResponse | null { return this.#host.getWebSocketAutoResponse(this.#address()); }
  getWebSocketAutoResponseTimestamp(socket: WebSocket): bigint | null { return this.#host.getWebSocketAutoResponseTimestamp(this.#address(), socket); }
  getTags(socket: WebSocket): string[] { return this.#host.getTags(this.#address(), socket); }
}

/** Namespace class handlers. The constructor runs before any delivered call. */
export interface DurableObjectHandlers<State> {
  initialize(state: DurableObjectState): State | Promise<State>;
  fetch?(state: State, request: Request): Response | Promise<Response>;
  rpc?(state: State, method: string, payload: Uint8Array): Uint8Array | Promise<Uint8Array>;
  alarm?(state: State): void | Promise<void>;
  webSocketMessage?(state: State, socket: WebSocket, message: string | ArrayBuffer): void | Promise<void>;
  webSocketClose?(state: State, socket: WebSocket, code: number, reason: string, wasClean: boolean): void | Promise<void>;
  webSocketError?(state: State, socket: WebSocket, error: unknown): void | Promise<void>;
}

/** The host's sole dispatch path for fetch, RPC, alarm and hibernating socket events. */
export class DurableObjectActivation<State> {
  readonly #activation: ActorActivation<State>;
  readonly #handlers: DurableObjectHandlers<State>;
  constructor(state: DurableObjectState, handlers: DurableObjectHandlers<State>) {
    this.#handlers = handlers;
    this.#activation = new ActorActivation(() => handlers.initialize(state));
  }
  blockConcurrencyWhile<Result>(callback: (state: State) => Result | Promise<Result>): Promise<Result> {
    return this.#activation.blockConcurrencyWhile(callback);
  }
  fence(): void { this.#activation.fence(); }
  fetch(request: Request): Promise<Response> {
    return this.#activation.deliver(state => {
      if (!this.#handlers.fetch) throw new Error("namespace class has no fetch handler");
      return this.#handlers.fetch(state, request);
    });
  }
  rpc(method: string, payload: Uint8Array): Promise<Uint8Array> {
    const snapshot = payload.slice();
    return this.#activation.deliver(state => {
      if (!this.#handlers.rpc) throw new Error("namespace class has no RPC handler");
      return this.#handlers.rpc(state, method, snapshot);
    });
  }
  alarm(): Promise<void> {
    return this.#activation.deliver(state => this.#handlers.alarm?.(state));
  }
  webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const snapshot = typeof message === "string" ? message : message.slice(0);
    return this.#activation.deliver(state => this.#handlers.webSocketMessage?.(state, socket, snapshot));
  }
  webSocketClose(socket: WebSocket, code: number, reason: string, wasClean: boolean): Promise<void> {
    return this.#activation.deliver(state => this.#handlers.webSocketClose?.(state, socket, code, reason, wasClean));
  }
  webSocketError(socket: WebSocket, error: unknown): Promise<void> {
    return this.#activation.deliver(state => this.#handlers.webSocketError?.(state, socket, error));
  }
}
