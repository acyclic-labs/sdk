import { create } from "@bufbuild/protobuf";
import { ActorsClient, type ActorsOptions, type ActorsCallOptions, type semantic } from "@acyclic-labs/actors";
import { PutObjectHeaderSchema } from "@acyclic-labs/objects/proto";
import type { ObjectsV1Provider } from "@acyclic-labs/objects";

/** Named Worker script deployments; no addressable Worker instances. */
export class WorkersDeploymentClient {
  readonly #actors: ActorsClient;
  constructor(
    actors: ActorsOptions,
    readonly objects: Pick<ObjectsV1Provider, "put">,
    readonly codeBucket = "actors-code",
  ) {
    this.#actors = new ActorsClient(actors);
  }

  /** Publish exact module bytes to an existing account-owned code bucket. */
  async publish(code: Uint8Array, idempotencyKey: string): Promise<Uint8Array> {
    const bytes = new Uint8Array(code);
    const digest = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", bytes));
    const hex = Array.from(digest, byte => byte.toString(16).padStart(2, "0")).join("");
    await this.objects.put(create(PutObjectHeaderSchema, {
      bucket: { name: this.codeBucket },
      objectKey: `sha256/${hex}`,
      mutation: { idempotencyKey },
    }), bytes);
    return digest;
  }

  /** The module must export default.fetch and omit the actor export. */
  deploy(request: semantic.DeploymentRequest, options?: ActorsCallOptions): Promise<semantic.DeploymentResponse> {
    return this.#actors.deployWorker(request, options);
  }
  invoke(request: semantic.InvokeWorkerRequest, options?: ActorsCallOptions): Promise<semantic.InvokeActorResponse> {
    return this.#actors.invokeWorker(request, options);
  }
  delete(request: semantic.DeleteWorkerRequest, options?: ActorsCallOptions): Promise<semantic.DeleteWorkerResponse> {
    return this.#actors.deleteWorker(request, options);
  }
}
