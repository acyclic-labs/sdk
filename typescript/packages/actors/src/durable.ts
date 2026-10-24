import { ActorReference } from "@acyclic-labs/auth/addressing";
import { WEB_SOCKET_TAG_COUNT, WEB_SOCKET_TAG_CHARACTERS, WEB_SOCKET_ATTACHMENT_BYTES, WEB_SOCKET_AUTO_RESPONSE_CHARACTERS } from "./generated/guest.js";
import type { AlarmInfo, WebSocketAutoResponse } from "./generated/guest.js";

/** Host adapter admitted for one actor. Serialized addresses grant no authority. */
export interface DurableHost {
  getAlarm(actor: Uint8Array): Promise<bigint | null>;
  setAlarm(actor: Uint8Array, timestamp: bigint): Promise<void>;
  deleteAlarm(actor: Uint8Array): Promise<void>;
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

/** One activation's scoped state. The host persists alarms and socket attachments. */
export class DurableObjectState {
  readonly #reference: ActorReference;
  readonly #host: DurableHost;
  constructor(reference: ActorReference, host: DurableHost) {
    this.#reference = ActorReference.decode(reference.encode());
    this.#host = host;
  }
  get id(): ActorReference { return ActorReference.decode(this.#reference.encode()); }
  #address(): Uint8Array { return this.#reference.encode(); }
  getAlarm(): Promise<bigint | null> { return this.#host.getAlarm(this.#address()); }
  setAlarm(timestamp: bigint | Date): Promise<void> {
    const at = timestamp instanceof Date ? BigInt(timestamp.getTime()) : timestamp;
    if (at < 0n || at > 0xffff_ffff_ffff_ffffn) throw new RangeError("alarm timestamp must fit u64 milliseconds");
    return this.#host.setAlarm(this.#address(), at);
  }
  deleteAlarm(): Promise<void> { return this.#host.deleteAlarm(this.#address()); }
  deleteAll(): Promise<void> { return this.#host.deleteAll(this.#address()); }
  acceptWebSocket(socket: WebSocket, tags: readonly string[] = []): void {
    tags = [...tags];
    validateTags(tags);
    this.#host.acceptWebSocket(this.#address(), socket, tags);
  }
  getWebSockets(tag?: string): WebSocket[] {
    if (tag !== undefined) validateTags([tag]);
    return this.#host.getWebSockets(this.#address(), tag);
  }
  serializeAttachment(socket: WebSocket, bytes: Uint8Array): void {
    if (bytes.byteLength > WEB_SOCKET_ATTACHMENT_BYTES) throw new RangeError("WebSocket attachment exceeds 2048 bytes");
    this.#host.serializeAttachment(this.#address(), socket, new Uint8Array(bytes));
  }
  deserializeAttachment(socket: WebSocket): Uint8Array | null {
    const bytes = this.#host.deserializeAttachment(this.#address(), socket);
    return bytes === null ? null : new Uint8Array(bytes);
  }
  setWebSocketAutoResponse(pair: WebSocketAutoResponse | null): void {
    if (pair !== null && ([...pair.request].length > WEB_SOCKET_AUTO_RESPONSE_CHARACTERS || [...pair.response].length > WEB_SOCKET_AUTO_RESPONSE_CHARACTERS)) {
      throw new RangeError("WebSocket auto-response exceeds 2048 characters");
    }
    this.#host.setWebSocketAutoResponse(this.#address(), pair === null ? null : { ...pair });
  }
  getWebSocketAutoResponse(): WebSocketAutoResponse | null { return this.#host.getWebSocketAutoResponse(this.#address()); }
  getWebSocketAutoResponseTimestamp(socket: WebSocket): bigint | null { return this.#host.getWebSocketAutoResponseTimestamp(this.#address(), socket); }
  getTags(socket: WebSocket): string[] { return this.#host.getTags(this.#address(), socket); }
}

function validateTags(tags: readonly string[]): void {
  if (tags.length > WEB_SOCKET_TAG_COUNT || tags.some(tag => [...tag].length > WEB_SOCKET_TAG_CHARACTERS)) throw new RangeError("WebSocket tags exceed 10 tags or 256 characters");
}

/** Validate host metadata before delivering an alarm to application code. */
export function alarmInfo(retryCount: number, isRetry = retryCount !== 0): Readonly<AlarmInfo> {
  if (!Number.isInteger(retryCount) || retryCount < 0 || retryCount > 0xffff_ffff || isRetry !== (retryCount !== 0)) {
    throw new TypeError("invalid alarm retry metadata");
  }
  return Object.freeze({ retryCount, isRetry });
}

/** Constructor and exclusive sections gate every new delivery. Failed heaps stay fenced. */
export class ActorActivation<State> {
  #initialize: (() => State | Promise<State>) | undefined;
  #value: State | undefined;
  #initialized = false;
  #failed = false;
  #fenced = false;
  #gate: Promise<void> = Promise.resolve();
  constructor(initialize: () => State | Promise<State>) { this.#initialize = initialize; }
  blockConcurrencyWhile<Result>(callback: (state: State) => Result | Promise<Result>): Promise<Result> {
    const operation = this.#gate.then(async () => {
      if (this.#failed || this.#fenced) throw new Error("activation failed; host must discard heap");
      // Failure remains terminal even when the host cancels or fences this activation.
      this.#failed = true;
      if (!this.#initialized) {
        const initialize = this.#initialize!;
        this.#initialize = undefined;
        this.#value = await initialize();
        this.#initialized = true;
      }
      const result = await callback(this.#value as State);
      if (this.#fenced) throw new Error("activation fenced; host must discard heap");
      this.#failed = false;
      return result;
    });
    this.#gate = operation.then(() => undefined, () => undefined);
    return operation;
  }
  async deliver<Result>(handler: (state: State) => Result | Promise<Result>): Promise<Result> {
    const state = await this.blockConcurrencyWhile(state => state);
    // Ownership may be revoked after admission resolves but before this continuation.
    if (this.#fenced) throw new Error("activation fenced; host must discard heap");
    return handler(state);
  }
  /** Host cancellation/ownership loss permanently invalidates this heap. */
  fence(): void { this.#fenced = true; }
}

/** Namespace class handlers. The constructor runs before any delivered call. */
export interface DurableObjectHandlers<State> {
  initialize(state: DurableObjectState): State | Promise<State>;
  fetch?(state: State, request: Request): Response | Promise<Response>;
  rpc?(state: State, method: string, payload: Uint8Array): Uint8Array | Promise<Uint8Array>;
  alarm?(state: State, info: Readonly<AlarmInfo>): void | Promise<void>;
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
  alarm(info: AlarmInfo): Promise<void> {
    const checked = alarmInfo(info.retryCount, info.isRetry);
    return this.#activation.deliver(state => this.#handlers.alarm?.(state, checked));
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
