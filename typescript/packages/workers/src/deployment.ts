import { ActorsClient, type ActorsOptions, type ActorsCallOptions, type semantic } from "@acyclic-labs/actors";
import { ObjectsV1Error, ErrorCode, verified, type ObjectsV1Provider } from "@acyclic-labs/objects";

/** The retained root that keeps one module's bytes alive, as the Rust client names it. */
function codeRoot(sha256: Uint8Array): Uint8Array {
  return new TextEncoder().encode(`worker-code:sha256:${Array.from(sha256, byte => byte.toString(16).padStart(2, "0")).join("")}`);
}

/** Named Worker script deployments; no addressable Worker instances. */
export class WorkersDeploymentClient {
  readonly #actors: ActorsClient;
  constructor(
    actors: ActorsOptions,
    readonly objects: Pick<ObjectsV1Provider, "put" | "retain" | "release">,
  ) {
    this.#actors = new ActorsClient(actors);
  }

  /**
   * Publish exact module bytes as an account blob, retained under the code's
   * own root until {@link unpublish}, and return the SHA-256 deployments name.
   * Publication is idempotent by content; an unknown outcome throws
   * `OUTCOME_UNKNOWN` and repeating the call is safe.
   */
  async publish(code: Uint8Array): Promise<Uint8Array> {
    const bytes = new Uint8Array(code);
    const sha256 = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", bytes));
    const blob = verified(await this.objects.put(bytes));
    if (blob?.digest === undefined) throw new ObjectsV1Error(ErrorCode.OUTCOME_UNKNOWN);
    await this.objects.retain(codeRoot(sha256), [blob.digest]);
    return sha256;
  }

  /** Release published module bytes once no deployment names them; a later collection reclaims them. */
  async unpublish(sha256: Uint8Array): Promise<boolean> {
    return (await this.objects.release(codeRoot(sha256))).existed;
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
