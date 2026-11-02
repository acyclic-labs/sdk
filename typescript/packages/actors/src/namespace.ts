import { bindingRequest, bindingResponse, bindingSignal, readBindingRequestBody } from "./binding-fetch.js";
import { snapshotBytes } from "@acyclic-labs/auth";
import { ActorReference, actorLocationHint } from "@acyclic-labs/auth/addressing";
import { awaitSignal } from "@acyclic-labs/auth/credential";
import type { ActorsClient, ActorsCallOptions } from "./client.js";
import type * as Semantic from "./generated/semantic/actors/readonly.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const randomId = () => Array.from(globalThis.crypto.getRandomValues(new Uint8Array(16)), byte => byte.toString(16).padStart(2, "0")).join("");
export type LocationHint = "wnam" | "enam" | "sam" | "weur" | "eeur" | "apac" | "oc" | "afr" | "me";
export interface ActorGetOptions { readonly locationHint?: LocationHint; }
/** A client transport also forks; a host binding that only invokes cannot. */
export type ActorInvocationTransport = Pick<ActorsClient, "invokeActor"> & Partial<Pick<ActorsClient, "forkActor">>;
export interface ActorForkOptions extends ActorsCallOptions {
  /** Retry identity; a retry of an unchanged fork commits once. Defaults to a fresh key. */
  readonly idempotencyKey?: string;
}

// Internal admission borrows the canonical owner; public references remain copies.
let namespaceReference: (id: NamespaceActorId) => ActorReference;

/** Plain ID scoped to its namespace. A saved reference carries no transport. */
export class NamespaceActorId {
  readonly #reference: ActorReference;
  readonly #text: string;
  readonly #name: string | undefined;
  static { namespaceReference = id => id.#reference; }
  constructor(reference: ActorReference, name?: string) {
    this.#reference = ActorReference.decode(reference.encode());
    this.#text = decoder.decode(this.#reference.id);
    if (name !== undefined && name !== this.#text) throw new TypeError("name differs from actor id");
    this.#name = name;
  }
  get name(): string | undefined { return this.#name; }
  toString(): string { return this.#text; }
  equals(other: NamespaceActorId): boolean { return this.#reference.equals(other.#reference); }
  get reference(): ActorReference { return ActorReference.decode(this.#reference.encode()); }
}
/** Host-authorized namespace binding. get performs no network access or creation. */
export class ActorNamespaceBinding {
  readonly #account: Uint8Array;
  readonly #namespace: Uint8Array;
  readonly #transport: ActorInvocationTransport;
  readonly #binding: string;
  constructor(account: Uint8Array, namespace: string, transport: ActorInvocationTransport, bindingName: string) {
    if (typeof namespace !== "string") throw new TypeError("namespace must be text");
    const scope = new ActorReference(account, encoder.encode(namespace), encoder.encode("_"));
    this.#account = scope.account;
    this.#namespace = scope.namespace;
    if (typeof bindingName !== "string" || bindingName.length === 0) throw new TypeError("binding name is required");
    this.#transport = transport; this.#binding = bindingName;
  }
  #id(id: string, named: boolean): NamespaceActorId {
    if (typeof id !== "string") throw new TypeError("actor id must be text");
    return new NamespaceActorId(new ActorReference(this.#account, this.#namespace, encoder.encode(id)), named ? id : undefined);
  }
  idFromName(name: string): NamespaceActorId { return this.#id(name, true); }
  idFromString(id: string): NamespaceActorId { return this.#id(id, false); }
  newUniqueId(): NamespaceActorId {
    return this.#id(randomId(), false);
  }
  #assertScope(reference: ActorReference): void {
    const expected = new ActorReference(this.#account, this.#namespace, reference.id);
    if (!reference.equals(expected)) throw new TypeError("actor reference is outside namespace binding");
  }
  idFromReference(reference: ActorReference): NamespaceActorId {
    const id = new NamespaceActorId(reference);
    this.#assertScope(namespaceReference(id));
    return id;
  }
  get(id: NamespaceActorId, options: ActorGetOptions = {}): ActorStub {
    this.#assertScope(namespaceReference(id));
    return new ActorStub(id, this.#transport, this.#binding, options.locationHint);
  }
  getByName(name: string, options: ActorGetOptions = {}): ActorStub { return this.get(this.idFromName(name), options); }
}
type PendingCall = {
  previous: PendingCall | undefined;
  next: PendingCall | undefined;
  release: (() => void) | undefined;
  linked: boolean;
};
/** Ordered calls retain the original invocation; a failed call permanently breaks that stub. */
export class ActorStub {
  readonly #id: NamespaceActorId;
  readonly #actor: Semantic.ActorId;
  readonly #account: Uint8Array;
  readonly #namespace: Semantic.ActorNamespace;
  readonly #transport: ActorInvocationTransport;
  readonly #invoke: ActorsClient["invokeActor"];
  readonly #binding: string;
  readonly #hint: LocationHint | undefined;
  #tail: Promise<void> = Promise.resolve();
  #pendingHead: PendingCall | undefined;
  #pendingTail: PendingCall | undefined;
  #broken = false;
  readonly #session = randomId();
  #sequence = 0n;
  readonly #lifetime = new AbortController();
  readonly #failure: Promise<never>;
  #reject!: (reason: unknown) => void;
  constructor(id: NamespaceActorId, transport: ActorInvocationTransport, binding: string, hint?: LocationHint) {
    if (typeof binding !== "string" || binding.length === 0) throw new TypeError("binding name is required");
    if (hint !== undefined) actorLocationHint(hint);
    this.#id = id; this.#transport = transport; this.#binding = binding; this.#hint = hint;
    this.#invoke = transport.invokeActor.bind(transport);
    const reference = namespaceReference(id);
    this.#actor = decoder.decode(reference.id) as Semantic.ActorId;
    this.#account = reference.account;
    this.#namespace = decoder.decode(reference.namespace) as Semantic.ActorNamespace;
    this.#failure = new Promise<never>((_resolve, reject) => { this.#reject = reject; });
    void this.#failure.catch(() => undefined);
  }
  get id(): NamespaceActorId { return this.#id; }
  get name(): string | undefined { return this.#id.name; }
  get reference(): ActorReference { return new ActorReference(this.#account, encoder.encode(this.#namespace), encoder.encode(this.#actor)); }
  #appendPending(release?: () => void): PendingCall {
    const node: PendingCall = { previous: this.#pendingTail, next: undefined, release, linked: true };
    if (this.#pendingTail) this.#pendingTail.next = node;
    else this.#pendingHead = node;
    this.#pendingTail = node;
    return node;
  }
  #unlinkPending(node: PendingCall): void {
    if (!node.linked) return;
    if (node.previous) node.previous.next = node.next;
    else this.#pendingHead = node.next;
    if (node.next) node.next.previous = node.previous;
    else this.#pendingTail = node.previous;
    node.previous = undefined; node.next = undefined; node.linked = false;
  }
  #releasePending(): void {
    // A marker releases after all its predecessors, independently of later calls.
    // Only actual waiters are visited; completed call nodes never accumulate.
    while (this.#pendingHead?.release) {
      const release = this.#pendingHead.release;
      const marker = this.#pendingHead;
      this.#unlinkPending(marker);
      release();
    }
  }
  #finishPending(node: PendingCall): void {
    this.#unlinkPending(node);
    this.#releasePending();
  }
  #waitPending(signal: AbortSignal): Promise<void> {
    let marker!: PendingCall;
    const settled = new Promise<void>(resolve => { marker = this.#appendPending(resolve); });
    this.#releasePending();
    return awaitSignal(settled, signal, () => this.#finishPending(marker));
  }
  #call(request: (signal: AbortSignal) => Promise<Semantic.InvokeActorRequest>, options?: ActorsCallOptions): Promise<Semantic.InvokeActorResponse> {
    if (this.#broken) return Promise.reject(new Error("actor stub is broken; obtain a new stub"));
    const signal = options?.signal ? AbortSignal.any([options.signal, this.#lifetime.signal]) : this.#lifetime.signal;
    const cancelled = () => { this.#break(signal.reason); };
    signal.addEventListener("abort",cancelled,{once:true});
    if (signal.aborted) cancelled();
    const pending = this.#appendPending();
    const admitted = this.#tail.then(async () => {
      if (this.#broken) throw new Error("actor stub is broken; obtain a new stub");
      const input = await request(signal);
      if (this.#broken) throw new Error("actor stub is broken; obtain a new stub");
      if (this.#sequence > 0xffff_ffff_ffff_ffffn) throw new RangeError("stub sequence exhausted");
      signal.throwIfAborted();
      // Queue body preparation and sequence assignment, then release admission before the response.
      const response = this.#invoke({...input,invocationOrder:{session:this.#session,sequence:this.#sequence++}}, {signal});
      return {response};
    });
    this.#tail = admitted.then(() => undefined, error => { this.#break(error); });
    const result = Promise.race([admitted.then(({response}) => response),this.#failure]).then(response => {
      if (this.#broken) throw new Error("actor stub is broken; obtain a new stub");
      return response;
    }, error => { this.#break(error); throw error; }).finally(() => {
      signal.removeEventListener("abort",cancelled);
      this.#finishPending(pending);
    });
    return result;
  }
  #break(error: unknown): void {
    if (this.#broken) return;
    this.#broken = true; this.#lifetime.abort(error); this.#reject(error);
  }
  #request(method: string, url: string, body: Uint8Array, headers: readonly Semantic.Header[], rpcMethod?: string): Semantic.InvokeActorRequest {
    return { actorId: this.#actor, namespace: this.#namespace,
      accountId: this.#account.slice(), method, url, body, headers, bindingName: this.#binding, deploymentEnvelope: new Uint8Array(0),
      ...(this.#hint === undefined ? {} : {locationHint:this.#hint}),
      ...(rpcMethod === undefined ? {} : {rpcMethod}) };
  }
  async fetch(input: RequestInfo | URL, init?: RequestInit, options?: ActorsCallOptions): Promise<Response> {
    if (this.#broken) throw new Error("actor stub is broken; obtain a new stub");
    const request = bindingRequest(input, init);
    const signal = AbortSignal.any([bindingSignal(request,options?.signal),this.#lifetime.signal]);
    // Own each body before queueing admission, so a cancelled queued call cannot
    // strand its stream. Only admission/sequence order is serialized.
    const body = readBindingRequestBody(request,signal);
    void body.catch(() => undefined);
    const result = await this.#call(async () => this.#request(request.method, request.url, await body, Array.from(request.headers, ([name, value]) => ({ name, value }))), {...options,signal});
    return bindingResponse(result,request.method);
  }
  /**
   * Fork this actor into `child`, in the same namespace, as one server commit:
   * the child starts from this actor's committed state after every call this
   * stub already made, without copying it. `subscriptions` binds the child's
   * own inputs under exactly this actor's subscription ids. The result is a
   * plain reference; obtain a stub for it from the namespace binding.
   */
  async fork(child: NamespaceActorId, subscriptions: readonly Semantic.SubscriptionSpec[], options: ActorForkOptions = {}): Promise<ActorReference> {
    if (this.#broken) throw new Error("actor stub is broken; obtain a new stub");
    const target = namespaceReference(child);
    const childId = target.id;
    const reference = new ActorReference(this.#account, encoder.encode(this.#namespace), childId);
    if (!target.equals(reference)) throw new TypeError("fork child is outside this actor's namespace");
    if (target.equals(namespaceReference(this.#id))) throw new TypeError("an actor cannot fork into itself");
    const selectedSubscriptions = structuredClone(subscriptions);
    const fork = this.#transport.forkActor;
    if (fork === undefined) throw new TypeError("this binding's transport cannot fork actors");
    const childActorId = decoder.decode(childId) as Semantic.ActorId;
    const { idempotencyKey = randomId(), ...callOptions } = options;
    const signal = callOptions.signal ? AbortSignal.any([callOptions.signal, this.#lifetime.signal]) : this.#lifetime.signal;
    await this.#waitPending(signal);
    if (this.#broken) throw new Error("actor stub is broken; obtain a new stub");
    signal.throwIfAborted();
    await fork.call(this.#transport, { actorId: this.#actor, childActorId,
      namespace: this.#namespace, accountId: this.#account.slice(),
      subscriptions: selectedSubscriptions, idempotencyKey }, {...callOptions,signal});
    return reference;
  }
  /** Named RPC payload bytes use the same namespace binding, never a public HTTP credential. */
  async rpc(method: string, payload: Uint8Array, options?: ActorsCallOptions): Promise<Uint8Array> {
    if (typeof method !== "string" || method.length === 0) throw new TypeError("RPC method is required");
    const snapshot = snapshotBytes(payload);
    const result = await this.#call(async () => this.#request("RPC", "https://actor.internal/", snapshot, [], method), options);
    return Uint8Array.from(result.body);
  }
}
