import { bindingRequest, bindingResponse, bindingSignal, readBindingRequestBody } from "@acyclic-labs/actors";
import { ResourceIdentity, validateResourceLabel } from "@acyclic-labs/auth/addressing";
import type { ActorsClient, ActorsCallOptions } from "@acyclic-labs/actors";
import type { semantic as Semantic } from "@acyclic-labs/actors";

const encoder = new TextEncoder();
const noDeploymentEnvelope = new Uint8Array(0);

/** Private Worker binding retaining the original invocation callable and receiver. */
export class WorkerServiceBinding {
  readonly #identity: ResourceIdentity;
  readonly #worker: Semantic.ActorNamespace;
  readonly #invoke: ActorsClient["invokeWorker"];
  readonly #binding: string;
  constructor(account: Uint8Array, worker: string, transport: Pick<ActorsClient, "invokeWorker">, binding: string) {
    if (typeof worker !== "string") throw new TypeError("worker name must be text");
    const id = encoder.encode(worker);
    validateResourceLabel(id);
    this.#identity = new ResourceIdentity(account, id);
    this.#worker = worker as Semantic.ActorNamespace;
    if (typeof binding !== "string" || binding.length === 0) throw new TypeError("binding name is required");
    this.#invoke = transport.invokeWorker.bind(transport); this.#binding = binding;
  }
  get identity(): ResourceIdentity { return ResourceIdentity.decode(this.#identity.encode()); }
  #request(method: string, url: string, body: Uint8Array, headers: readonly Semantic.Header[], rpcMethod?: string): Semantic.InvokeWorkerRequest {
    return { workerName: this.#worker,
      accountId: this.#identity.account, method, url, body, headers, bindingName: this.#binding, deploymentEnvelope: noDeploymentEnvelope,
      ...(rpcMethod === undefined ? {} : {rpcMethod}) };
  }
  async fetch(input: RequestInfo | URL, init?: RequestInit, options?: ActorsCallOptions): Promise<Response> {
    const request = bindingRequest(input, init);
    const signal = bindingSignal(request,options?.signal);
    const result = await this.#invoke(this.#request(request.method, request.url, await readBindingRequestBody(request,signal), Array.from(request.headers, ([name, value]) => ({ name, value }))), {...options,signal});
    return bindingResponse(result,request.method);
  }
  async rpc(method: string, payload: Uint8Array, options?: ActorsCallOptions): Promise<Uint8Array> {
    if (typeof method !== "string" || method.length === 0) throw new TypeError("RPC method is required");
    const result = await this.#invoke(this.#request("RPC", "https://worker.internal/", payload.slice(), [], method), options);
    return Uint8Array.from(result.body);
  }
}
