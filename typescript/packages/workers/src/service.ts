import { ResourceIdentity, validateResourceLabel } from "@acyclic-labs/auth/addressing";
import type { ActorsClient, ActorsCallOptions } from "@acyclic-labs/actors";
import type { semantic as Semantic } from "@acyclic-labs/actors";

/** Private Worker-to-Worker binding, authorized by its named deployment binding. */
export class WorkerServiceBinding {
  readonly #identity: ResourceIdentity;
  readonly #transport: Pick<ActorsClient, "invokeWorker">;
  readonly #binding: string;
  constructor(account: Uint8Array, worker: string, transport: Pick<ActorsClient, "invokeWorker">, binding: string) {
    if (typeof worker !== "string") throw new TypeError("worker name must be text");
    this.#identity = new ResourceIdentity(account, new TextEncoder().encode(worker));
    validateResourceLabel(this.#identity.id);
    if (typeof binding !== "string" || binding.length === 0) throw new TypeError("binding name is required");
    this.#transport = transport; this.#binding = binding;
  }
  get identity(): ResourceIdentity { return ResourceIdentity.decode(this.#identity.encode()); }
  #request(method: string, url: string, body: Uint8Array, headers: readonly Semantic.Header[], rpcMethod?: string): Semantic.InvokeWorkerRequest {
    return { workerName: new TextDecoder().decode(this.#identity.id) as Semantic.ActorNamespace,
      accountId: this.#identity.account, method, url, body, headers, bindingName: this.#binding, rpcMethod };
  }
  async fetch(input: RequestInfo | URL, init?: RequestInit, options?: ActorsCallOptions): Promise<Response> {
    const request = new Request(input, init);
    const result = await this.#transport.invokeWorker(this.#request(request.method, request.url, new Uint8Array(await request.arrayBuffer()), Array.from(request.headers, ([name, value]) => ({ name, value }))), options);
    return new Response(Uint8Array.from(result.body), { status: result.status, headers: result.headers.map(h => [h.name, h.value]) });
  }
  async rpc(method: string, payload: Uint8Array, options?: ActorsCallOptions): Promise<Uint8Array> {
    if (typeof method !== "string" || method.length === 0) throw new TypeError("RPC method is required");
    const result = await this.#transport.invokeWorker(this.#request("RPC", "https://worker.internal/", payload.slice(), [], method), options);
    return Uint8Array.from(result.body);
  }
}
