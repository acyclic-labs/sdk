import { ActorReference } from "@acyclic-labs/auth/addressing";
import { WEB_SOCKET_TAG_COUNT, WEB_SOCKET_TAG_CHARACTERS, WEB_SOCKET_ATTACHMENT_BYTES, WEB_SOCKET_AUTO_RESPONSE_CHARACTERS } from "./generated/guest.js";
import type { AlarmInfo, WebSocketAutoResponse } from "./generated/guest.js";

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

interface ActivationObserver {
  previous: ActivationObserver | undefined;
  next: ActivationObserver | undefined;
  reject(reason: unknown): void;
}

/** Constructor and exclusive sections gate every new delivery. Failed heaps stay fenced. */
export class ActorActivation<State> {
  #initialize: (() => State | Promise<State>) | undefined;
  #value: State | undefined;
  #initialized = false;
  #failure: (Error & { code: "unavailable" }) | undefined;
  #observers: ActivationObserver | undefined;
  #gate: Promise<void> = Promise.resolve();
  #admission: Promise<void> | undefined;
  #batch: { active: number; drained?: () => void } = { active: 0 };
  constructor(initialize: () => State | Promise<State>) { this.#initialize = initialize; }
  // Remove settled observers: a terminal promise's reactions would retain history
  // for the entire activation. Only currently pending operations observe stop.
  #observe<Result>(operation: (fail: (error: unknown) => void) => Promise<Result>): Promise<Result> {
    return new Promise<Result>((resolve, reject) => {
      const observer: ActivationObserver = { previous: undefined, next: undefined, reject };
      if (this.#failure) { reject(this.#failure); return; }
      observer.next = this.#observers;
      if (observer.next) observer.next.previous = observer;
      this.#observers = observer;
      const fail = (error: unknown) => {
        this.#unlink(observer);
        reject(error);
        this.fence(error);
      };
      operation(fail).then(value => {
        this.#unlink(observer);
        if (this.#failure) reject(this.#failure); else resolve(value);
      }, fail);
    });
  }
  #unlink(observer: ActivationObserver): void {
    if (observer.previous) observer.previous.next = observer.next;
    else if (this.#observers === observer) this.#observers = observer.next;
    else return;
    if (observer.next) observer.next.previous = observer.previous;
    observer.previous = observer.next = undefined;
  }
  #available(): void { if (this.#failure) throw this.#failure; }
  blockConcurrencyWhile<Result>(callback: (state: State) => Result | Promise<Result>): Promise<Result> {
    if (this.#failure) return Promise.reject(this.#failure);
    return this.#observe(fail => this.#exclusive(callback, fail));
  }
  #exclusive<Result>(callback: (state: State) => Result | Promise<Result>, fail: (error: unknown) => void): Promise<Result> {
    const operation = this.#gate.then(async () => {
      this.#available();
      if (!this.#initialized) {
        const initialize = this.#initialize!;
        this.#initialize = undefined;
        this.#value = await initialize();
        this.#initialized = true;
      }
      this.#available();
      const result = await callback(this.#value as State);
      this.#available();
      return result;
    });
    // Settle this section's original public owner before fencing its peers or
    // opening the queue. An internal section is not another public observer.
    this.#gate = operation.then(() => undefined, fail);
    return operation;
  }
  deliver<Result>(handler: (state: State) => Result | Promise<Result>): Promise<Result> {
    if (this.#failure) return Promise.reject(this.#failure);
    const batch = this.#batch, admission = this.#admission;
    // Registration precedes every await, including initialization and admission.
    batch.active++;
    return this.#observe(fail => (async () => {
      try {
        if (admission) await admission;
        const state = await this.#exclusive(state => state, fail);
        // Ownership may be revoked after admission resolves but before this continuation.
        this.#available();
        const result = await handler(state);
        this.#available();
        return result;
      } finally { if (--batch.active === 0) batch.drained?.(); }
    })());
  }
  /** Host capture waits for admitted handlers and gates new deliveries until complete.
   * Call outside a handler: a handler cannot wait for its own completion.
   */
  quiesce<Result>(capture: (state: State) => Result | Promise<Result>): Promise<Result> {
    if (this.#failure) return Promise.reject(this.#failure);
    const batch = this.#batch;
    const drained = batch.active === 0 ? Promise.resolve() : new Promise<void>(resolve => { batch.drained = resolve; });
    // Close admission first. Already admitted handlers retain exclusive access
    // while draining; capture acquires it only after their completion event.
    return this.#observe(fail => {
      const operation = (this.#admission?.then(() => drained) ?? drained).then(() => this.#exclusive(capture, fail));
      const released = () => { if (this.#admission === admission) this.#admission = undefined; };
      const admission = operation.then(released, released);
      this.#admission = admission;
      this.#batch = { active: 0 };
      return operation;
    });
  }
  /** Host cancellation/ownership loss permanently invalidates this heap. */
  fence(cause?: unknown): void {
    if (this.#failure) return;
    this.#failure = Object.assign(
      new Error("activation fenced; host must discard heap", { cause }),
      { code: "unavailable" as const },
    );
    // Only terminal notification visits all currently armed observers.
    while (this.#observers) {
      const observer = this.#observers;
      this.#unlink(observer);
      observer.reject(this.#failure);
    }
  }
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
