/** Host adapter for the public ActorDefinition; admission and publication stay with the host. */
import { ActorReference } from "@acyclic-labs/auth/addressing";
import { ActorActivation } from "./activation.js";
import { GUEST_ABI_VERSION } from "./generated/guest.js";
import { loadWasmModule } from "./client.js";
import { openGuestResponse } from "./fetch-response.js";
import type { ActorDefinition, ActorRuntime, Checkpoint, CodecIdentity, GuestContext, GuestFetchResponse, GuestEvent, GuestEventResult, GuestState } from "./generated/guest.js";

const encoder = new TextEncoder();
let mutating = false;

// All states in this JavaScript execution context share the no-nesting guard,
// including states owned by distinct activations. A mutation never owns an await.
function stateHandle<State>(value: State): GuestState<State> {
  return Object.freeze({ with(mutate: (state: State) => undefined): void {
    if (mutating) throw new Error("nested guest state mutation");
    mutating = true;
    try {
      if (mutate(value) !== undefined) throw new TypeError("guest state mutation must finish synchronously");
    } finally { mutating = false; }
  } });
}

function reference(actor: ActorRuntime): ActorReference | null {
  if (actor.id === null && actor.account === undefined && actor.namespace === undefined) return null;
  return new ActorReference(Uint8Array.from(actor.account ?? []),
    encoder.encode(actor.namespace ?? "__default"), encoder.encode(actor.id ?? ""));
}

function admitCheckpoint(value: Checkpoint | null, identity: CodecIdentity): void {
  if (identity.name.length === 0 || identity.version.length === 0 ||
      value !== null && (value.codec.name !== identity.name || value.codec.version !== identity.version)) {
    throw Object.assign(new Error("incompatible checkpoint codec"), { type: "incompatibleCodec" as const });
  }
}

function sameCheckpoint(left: Checkpoint | null, right: Checkpoint | null): boolean {
  return left === null || right === null ? left === right
    : left.codec.name === right.codec.name && left.codec.version === right.codec.version &&
      left.bytes.length === right.bytes.length && left.bytes.every((byte, index) => byte === right.bytes[index]);
}

/** Rust `guest::retry_key`: length-prefixed, so distinct deliveries never share a key. */
function retryKey(deliveryId: string, effect: string): string {
  if (effect.length === 0) throw Object.assign(new Error("missing delivery or effect identity"), { type: "missingIdentity" as const });
  return `guest-v1:${encoder.encode(deliveryId).length}:${deliveryId}:${effect}`;
}

function checkpointCopy(value: Checkpoint): Checkpoint {
  if (!(value.bytes instanceof Uint8Array)) throw new TypeError("checkpoint bytes must be Uint8Array");
  return { codec: { ...value.codec }, bytes: new Uint8Array(value.bytes) };
}

type GuestWasm = Awaited<ReturnType<typeof loadWasmModule>>;
type GuestDelivery = ReturnType<GuestWasm["decodeDelivery"]>;
type GuestInvokeRequest = NonNullable<ReturnType<GuestWasm["decodeInvokeRequest"]>>;

/** Read one complete fetch response into its canonical InvokeActorResponse. A
 * reply is one committed value, so a response with trailers is refused rather
 * than silently losing them. */
async function invokeResponse(wasm: GuestWasm, response: GuestFetchResponse, signal?: AbortSignal): Promise<Uint8Array> {
  const opened = openGuestResponse(response, signal);
  try {
    const chunks: Uint8Array[] = [];
    for await (const frame of opened.frames) {
      if ("data" in frame) chunks.push(frame.data);
      else if ([...frame.trailers].length !== 0) throw new Error("an invocation reply cannot carry trailers");
    }
    const body = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.length, 0));
    chunks.reduce((offset, chunk) => { body.set(chunk, offset); return offset + chunk.length; }, 0);
    const headers = [...opened.headers];
    return wasm.encodeInvokeResponse(opened.status, headers.map(([name]) => name), headers.map(([, value]) => value), body);
  } catch (error) {
    opened.close();
    throw error;
  }
}

/** One lazy activation of the SDK's typed guest hooks, on local or hosted JavaScript.
 * GuestContext is supplied by an admitted host. This adapter cannot authenticate
 * a request or publish a checkpoint, and adds no guest context properties.
 */
export class GuestActivation<State, Bindings> {
  readonly #activation: ActorActivation<GuestState<State>>;
  readonly #definition: ActorDefinition<State, Bindings>;
  readonly #context: GuestContext<Bindings>;
  readonly #reference: ActorReference | null;
  /** The committed checkpoint this heap extends; undefined once a delivery's outcome is unknown. */
  #basis: Checkpoint | null | undefined;
  constructor(definition: ActorDefinition<State, Bindings>, checkpoint: Checkpoint | null, context: GuestContext<Bindings>) {
    if (definition.abiVersion !== GUEST_ABI_VERSION) throw new Error("unsupported guest ABI");
    admitCheckpoint(checkpoint, definition.codec.identity);
    this.#definition = definition;
    this.#context = context;
    this.#reference = reference(context.actor);
    this.#admit(context);
    const retained = checkpoint === null ? null : checkpointCopy(checkpoint);
    this.#basis = checkpoint === null ? null : checkpointCopy(checkpoint);
    this.#activation = new ActorActivation(async () => stateHandle(await definition.restore(retained, context)));
  }
  #admit(context: GuestContext<Bindings>): void {
    const actual = reference(context.actor);
    if (this.#reference === null ? actual !== null : actual === null || !this.#reference.equals(actual))
      throw new Error("delivery actor differs from activation");
    const caller = context.admittedIdentity;
    if (caller !== null) {
      const audience = new ActorReference(Uint8Array.from(caller.accountId), encoder.encode(caller.namespace), encoder.encode(caller.actorId));
      if (this.#reference === null || !this.#reference.equals(audience))
        throw new Error("delivery audience differs from activation");
    }
  }
  event(event: GuestEvent, context: GuestContext<Bindings>): Promise<GuestEventResult | null> {
    this.#admit(context);
    return this.#activation.deliver(state => this.#definition.event(state, event, context));
  }
  fetch(request: Request, context: GuestContext<Bindings>): Promise<GuestFetchResponse> {
    this.#admit(context);
    return this.#activation.deliver(state => this.#definition.fetch(state, request, context));
  }
  /** Capture is host work; it cannot be invoked from one of this activation's handlers. */
  checkpoint(): Promise<Checkpoint> {
    return this.#activation.quiesce(async state => {
      const checkpoint = await this.#definition.checkpoint(state, this.#context);
      admitCheckpoint(checkpoint, this.#definition.codec.identity);
      return checkpointCopy(checkpoint);
    });
  }
  /** Run one Actors-core delivery through the Rust `guest::wire` codec and return its encoded
   * Transition: the event's reply, the quiesced checkpoint, the alarm kept, no effects.
   * The delivery must name this actor and carry the checkpoint this heap extends, so a
   * host retry after an unpublished transition needs a fresh activation. A failure or
   * an abort of `signal` fences the heap.
   */
  async onRecord(bytes: Uint8Array, signal?: AbortSignal): Promise<Uint8Array> {
    const wasm = await loadWasmModule();
    return this.deliver(wasm, wasm.decodeDelivery(bytes), null, signal);
  }
  /** {@link onRecord} over a delivery the caller already decoded with `wasm`. A
   * non-null `invocation` is its front-door HTTP request, served by `fetch`. */
  async deliver(wasm: GuestWasm, delivery: GuestDelivery, invocation: GuestInvokeRequest | null,
    signal?: AbortSignal): Promise<Uint8Array> {
    const { actor, trigger, nowUnixMs, checkpoint } = delivery;
    if (this.#reference === null || !this.#reference.equals(ActorReference.decode(actor)))
      throw new Error("delivery actor differs from activation");
    if (this.#basis === undefined || !sameCheckpoint(this.#basis, checkpoint))
      throw new Error("delivery checkpoint differs from activation");
    signal?.throwIfAborted();
    this.#basis = undefined;
    const abort = () => this.#activation.fence(signal?.reason);
    signal?.addEventListener("abort", abort, { once: true });
    try {
      const deliveryId = trigger.type === "record" ? `record:${trigger.subscription}:${trigger.sequence}` : `alarm:${trigger.generation}`;
      const event: GuestEvent = trigger.type === "alarm" ? { type: "alarm" } : {
        type: "stream", subscription: trigger.subscription, stream: trigger.record.origin,
        sequence: String(trigger.record.sequence), committedAtMicros: String(trigger.record.committedAtMicros),
        value: btoa(Array.from(trigger.record.value, byte => String.fromCharCode(byte)).join("")),
        commitId: Array.from(trigger.record.commitId, byte => byte.toString(16).padStart(2, "0")).join(""),
      };
      const context: GuestContext<Bindings> = {
        ...this.#context, deliveryId, admittedIdentity: null, clock: () => nowUnixMs,
        retryKey: effect => retryKey(deliveryId, effect),
      };
      let reply: Uint8Array | null;
      if (invocation !== null) {
        // An InvokeActor input is the definition's fetch; its reply is the
        // canonical InvokeActorResponse the front door returns to the caller.
        const { method, url, headers, body } = invocation;
        const request = new Request(url, { method, headers: headers.map(([name, value]) => [name, value]),
          body: method === "GET" || method === "HEAD" ? undefined : body, signal });
        reply = await invokeResponse(wasm, await this.fetch(request, context), signal);
      } else {
        const result = await this.event(event, context);
        if (result !== null && result.type !== "reply") throw new Error("socket result outside a socket delivery");
        reply = result?.payload ?? null;
      }
      const next = await this.checkpoint();
      const encoded = wasm.encodeTransition(next.codec.name, next.codec.version, next.bytes, reply);
      signal?.throwIfAborted();
      this.#basis = next;
      return encoded;
    } catch (error) {
      this.#activation.fence(error);
      throw error;
    } finally {
      signal?.removeEventListener("abort", abort);
    }
  }
  /** Original host authority/cancellation permanently discards this heap. */
  fence(): void { this.#activation.fence(); }
}

/** Deployment facts a namespace package's manifest states. The host admitted
 * them with the deployment; the definition supplies only its codec. */
export interface DefinitionDeployment {
  /** Admitted code SHA-256 of this artifact. */
  readonly code: Uint8Array;
  /** Declared subscription ids; each actor binds its own path when created. */
  readonly subscriptions: readonly string[];
  /** "fetch" when every invocation is a front-door `InvokeActorRequest` served
   * by the definition's fetch; otherwise invocations are application-defined
   * bytes delivered to its event hook. */
  readonly invocations?: "fetch" | "event";
  readonly limits?: { readonly handlerTimeoutMillis?: bigint; readonly memoryBytes?: bigint; readonly checkpointBytes?: bigint };
}

/** The SDK Handler ABI a host core runs, for a whole namespace of one public
 * ActorDefinition. Each delivery restores its actor from the checkpoint the
 * delivery carries, so no actor's state or activation outlives its delivery
 * and a host retry needs no heap-side reconciliation. */
export function definitionHandler<State, Bindings>(definition: ActorDefinition<State, Bindings>,
  deployment: DefinitionDeployment, env: Bindings): {
  manifest(): Promise<Uint8Array>;
  onRecord(bytes: Uint8Array, signal?: AbortSignal): Promise<Uint8Array>;
} {
  if (definition.abiVersion !== GUEST_ABI_VERSION) throw new Error("unsupported guest ABI");
  const decoder = new TextDecoder("utf-8", { fatal: true });
  return {
    async manifest() {
      const wasm = await loadWasmModule();
      const { identity } = definition.codec;
      const { limits } = deployment;
      return wasm.encodeManifest(Uint8Array.from(deployment.code), identity.name, identity.version,
        [...deployment.subscriptions], limits?.handlerTimeoutMillis, limits?.memoryBytes, limits?.checkpointBytes);
    },
    async onRecord(bytes, signal) {
      const wasm = await loadWasmModule();
      const delivery = wasm.decodeDelivery(bytes);
      const actor = ActorReference.decode(delivery.actor);
      const context: GuestContext<Bindings> = {
        env, clock: () => delivery.nowUnixMs, deliveryId: null, admittedIdentity: null,
        actor: { id: decoder.decode(actor.id), account: Array.from(actor.account),
          namespace: decoder.decode(actor.namespace), class: null },
        retryKey() { throw Object.assign(new Error("missing delivery identity"), { type: "missingIdentity" as const }); },
      };
      const invocation = deployment.invocations === "fetch" ? wasm.decodeInvokeRequest(bytes) : null;
      return new GuestActivation(definition, delivery.checkpoint, context).deliver(wasm, delivery, invocation, signal);
    },
  };
}
