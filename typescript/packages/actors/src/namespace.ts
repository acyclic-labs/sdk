import { ActorReference, actorLocationHint } from "@acyclic-labs/auth/addressing";
import type { ActorsClient, ActorsCallOptions } from "./client.js";
import type * as Semantic from "./generated/semantic/actors/readonly.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
export type LocationHint = "wnam" | "enam" | "sam" | "weur" | "eeur" | "apac" | "oc" | "afr" | "me";
export interface ActorGetOptions { readonly locationHint?: LocationHint; }
export type ActorInvocationTransport = Pick<ActorsClient, "invokeActor">;

/** Plain ID scoped to its namespace. A saved reference carries no transport. */
export class NamespaceActorId {
  readonly #reference: ActorReference;
  readonly #name: string | undefined;
  constructor(reference: ActorReference, name?: string) {
    this.#reference = ActorReference.decode(reference.encode());
    if (name !== undefined && name !== decoder.decode(reference.id)) throw new TypeError("name differs from actor id");
    this.#name = name;
  }
  get name(): string | undefined { return this.#name; }
  toString(): string { return decoder.decode(this.#reference.id); }
  equals(other: NamespaceActorId): boolean { return this.#reference.equals(other.#reference); }
  get reference(): ActorReference { return ActorReference.decode(this.#reference.encode()); }
}
/** Host-authorized namespace binding. get performs no network access or creation. */
export class ActorNamespaceBinding {
  readonly #scope: ActorReference;
  readonly #transport: ActorInvocationTransport;
  readonly #binding: string;
  constructor(account: Uint8Array, namespace: string, transport: ActorInvocationTransport, bindingName: string) {
    if (typeof namespace !== "string") throw new TypeError("namespace must be text");
    this.#scope = new ActorReference(account, encoder.encode(namespace), encoder.encode("_"));
    if (typeof bindingName !== "string" || bindingName.length === 0) throw new TypeError("binding name is required");
    this.#transport = transport; this.#binding = bindingName;
  }
  #id(id: string, named: boolean): NamespaceActorId {
    if (typeof id !== "string") throw new TypeError("actor id must be text");
    return new NamespaceActorId(new ActorReference(this.#scope.account, this.#scope.namespace, encoder.encode(id)), named ? id : undefined);
  }
  idFromName(name: string): NamespaceActorId { return this.#id(name, true); }
  idFromString(id: string): NamespaceActorId { return this.#id(id, false); }
  newUniqueId(): NamespaceActorId {
    const bytes = globalThis.crypto.getRandomValues(new Uint8Array(16));
    return this.#id(Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join(""), false);
  }
  idFromReference(reference: ActorReference): NamespaceActorId {
    const expected = new ActorReference(this.#scope.account, this.#scope.namespace, reference.id);
    if (!reference.equals(expected)) throw new TypeError("actor reference is outside namespace binding");
    return new NamespaceActorId(reference);
  }
  get(id: NamespaceActorId, options: ActorGetOptions = {}): ActorStub {
    this.idFromReference(id.reference);
    if (options.locationHint !== undefined) actorLocationHint(options.locationHint);
    return new ActorStub(id, this.#transport, this.#binding, options.locationHint);
  }
}
/** Ordered calls on one stub; a failed call permanently breaks that stub. */
export class ActorStub {
  readonly #id: NamespaceActorId;
  readonly #transport: ActorInvocationTransport;
  readonly #binding: string;
  readonly #hint: LocationHint | undefined;
  #tail: Promise<void> = Promise.resolve();
  #broken = false;
  constructor(id: NamespaceActorId, transport: ActorInvocationTransport, binding: string, hint?: LocationHint) {
    if (typeof binding !== "string" || binding.length === 0) throw new TypeError("binding name is required");
    if (hint !== undefined) actorLocationHint(hint);
    this.#id = id; this.#transport = transport; this.#binding = binding; this.#hint = hint;
  }
  get id(): NamespaceActorId { return this.#id; }
  get reference(): ActorReference { return this.#id.reference; }
  #call(request: () => Promise<Semantic.InvokeActorRequest>, options?: ActorsCallOptions): Promise<Semantic.InvokeActorResponse> {
    const call = this.#tail.then(async () => {
      if (this.#broken) throw new Error("actor stub is broken; obtain a new stub");
      this.#broken = true;
      const result = await this.#transport.invokeActor(await request(), options);
      this.#broken = false;
      return result;
    });
    this.#tail = call.then(() => undefined, () => undefined);
    return call;
  }
  #request(method: string, url: string, body: Uint8Array, headers: readonly Semantic.Header[], rpcMethod?: string): Semantic.InvokeActorRequest {
    const reference = this.reference;
    return { actorId: this.#id.toString() as Semantic.ActorId, namespace: decoder.decode(reference.namespace) as Semantic.ActorNamespace,
      accountId: reference.account, method, url, body, headers, locationHint: this.#hint, bindingName: this.#binding, rpcMethod };
  }
  async fetch(input: RequestInfo | URL, init?: RequestInit, options?: ActorsCallOptions): Promise<Response> {
    const request = new Request(input, init);
    const result = await this.#call(async () => this.#request(request.method, request.url, new Uint8Array(await request.arrayBuffer()), Array.from(request.headers, ([name, value]) => ({ name, value }))), options);
    return new Response(Uint8Array.from(result.body), { status: result.status, headers: result.headers.map(h => [h.name, h.value]) });
  }
  /** Named RPC payload bytes use the same namespace binding, never a public HTTP credential. */
  async rpc(method: string, payload: Uint8Array, options?: ActorsCallOptions): Promise<Uint8Array> {
    if (typeof method !== "string" || method.length === 0) throw new TypeError("RPC method is required");
    const snapshot = payload.slice();
    const result = await this.#call(async () => this.#request("RPC", "https://actor.internal/", snapshot, [], method), options);
    return Uint8Array.from(result.body);
  }
}
