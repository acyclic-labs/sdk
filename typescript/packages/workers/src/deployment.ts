import { ActorsClient, type ActorsOptions, type ActorsCallOptions, type semantic } from "@acyclic-labs/actors";
import type { ObjectsV1BinaryPort } from "@acyclic-labs/objects";
import { EntryStore, type PublishedDeployment } from "./entry.js";

/** Named Worker deployments over the original Actors and Objects owners. */
export class WorkersDeploymentClient {
  readonly #actors: ActorsClient;
  readonly entries: EntryStore;
  constructor(actors: ActorsOptions, objects: ObjectsV1BinaryPort) {
    this.#actors = new ActorsClient(actors);
    this.entries = new EntryStore(objects);
  }

  /** Submit the original canonical request after its full closure is retained. */
  deploy(published: PublishedDeployment, options?: ActorsCallOptions): Promise<semantic.DeploymentResponse> {
    return this.#actors.callEncoded("deployWorker", published.request, options);
  }
  invoke(request: semantic.InvokeWorkerRequest, options?: ActorsCallOptions): Promise<semantic.InvokeActorResponse> {
    return this.#actors.invokeWorker(request, options);
  }
  delete(request: semantic.DeleteWorkerRequest, options?: ActorsCallOptions): Promise<semantic.DeleteWorkerResponse> {
    return this.#actors.deleteWorker(request, options);
  }
}
