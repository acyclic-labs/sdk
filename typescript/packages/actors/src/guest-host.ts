/** Host adapter for the public ActorDefinition; admission and publication stay with the host. */
import { ActorReference } from "@acyclic-labs/auth/addressing";
import { ActorActivation } from "./activation.js";
import { GUEST_ABI_VERSION } from "./generated/guest.js";
import type { ActorDefinition, ActorRuntime, Checkpoint, GuestContext, GuestFetchResponse, GuestEvent, GuestEventResult, GuestState } from "./generated/guest.js";

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
  constructor(definition: ActorDefinition<State, Bindings>, checkpoint: Checkpoint | null, context: GuestContext<Bindings>) {
    if (definition.abiVersion !== GUEST_ABI_VERSION) throw new Error("unsupported guest ABI");
    this.#definition = definition;
    this.#context = context;
    this.#reference = reference(context.actor);
    this.#admit(context);
    const retained = checkpoint === null ? null : checkpointCopy(checkpoint);
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
    return this.#activation.quiesce(async state => checkpointCopy(await this.#definition.checkpoint(state, this.#context)));
  }
  /** Original host authority/cancellation permanently discards this heap. */
  fence(): void { this.#activation.fence(); }
}
