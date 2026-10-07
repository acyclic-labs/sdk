// Generated from the canonical ActorsService descriptor. Do not edit.
import { ActorsService } from "../../generated/proto/actors/v1/actors_pb.js";
import type { ActorsCallOptions, ReadonlySemantic } from "../client.js";
import type * as Semantic from "./semantic/actors/index.js";

export type ActorsMethod = (typeof ActorsService.methods)[number];
export type ActorsOperation = "createActor" | "updateActor" | "inspectActor" | "addSubscription" | "removeSubscription" | "resumeSubscription" | "checkpointActor" | "invokeActor";
export const ACTORS_OPERATION_NAMES = Object.freeze(ActorsService.methods.map(method => method.localName)) as readonly ActorsOperation[];

export interface ActorsClientMethods {
  readonly createActor: (request: ReadonlySemantic<Semantic.CreateActorRequest>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.CreateActorResponse>>;
  readonly updateActor: (request: ReadonlySemantic<Semantic.UpdateActorRequest>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.UpdateActorResponse>>;
  readonly inspectActor: (request: ReadonlySemantic<Semantic.InspectActorRequest>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.InspectActorResponse>>;
  readonly addSubscription: (request: ReadonlySemantic<Semantic.AddSubscriptionRequest>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.AddSubscriptionResponse>>;
  readonly removeSubscription: (request: ReadonlySemantic<Semantic.RemoveSubscriptionRequest>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.RemoveSubscriptionResponse>>;
  readonly resumeSubscription: (request: ReadonlySemantic<Semantic.ResumeSubscriptionRequest>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.ResumeSubscriptionResponse>>;
  readonly checkpointActor: (request: ReadonlySemantic<Semantic.CheckpointActorRequest>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.CheckpointActorResponse>>;
  readonly invokeActor: (request: ReadonlySemantic<Semantic.InvokeActorRequest>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<Semantic.InvokeActorResponse>>;
}

export type ActorsMethodCall = (method: ActorsMethod, request: unknown, options?: ActorsCallOptions) => Promise<unknown>;

/** Installs the typed public methods from the maintained service descriptor. */
export function installActorsMethods(target: object, call: ActorsMethodCall): void {
  for (const method of ActorsService.methods) {
    Object.defineProperty(target, method.localName, {
      configurable: true,
      enumerable: true,
      value: (request: unknown, options?: ActorsCallOptions) => call(method, request, options),
    });
  }
}
