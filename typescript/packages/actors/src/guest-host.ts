/** Host adapter for the public ActorDefinition; admission and publication stay with the host. */
import { ActorReference } from "@acyclic-labs/auth/addressing";
import { ActorActivation } from "./activation.js";
import { GUEST_ABI_VERSION } from "./generated/guest.js";
import { loadWasmModule } from "./client.js";
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
    const { actor, trigger, nowUnixMs, checkpoint } = wasm.decodeDelivery(bytes);
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
      const result = await this.event(event, {
        ...this.#context, deliveryId, admittedIdentity: null, clock: () => nowUnixMs,
        retryKey: effect => retryKey(deliveryId, effect),
      });
      if (result !== null && result.type !== "reply") throw new Error("socket result outside a socket delivery");
      const next = await this.checkpoint();
      const encoded = wasm.encodeTransition(next.codec.name, next.codec.version, next.bytes, result?.payload ?? null);
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
