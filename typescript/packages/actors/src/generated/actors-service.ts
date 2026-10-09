// Generated from the canonical ActorsService descriptor. Do not edit.
import { ActorsService } from "../../generated/proto/actors/v1/actors_pb.js";
import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import type { ActorsCallOptions } from "../client.js";
import type { CanonicalCompatible, ReadonlyInputSemantic, ReadonlySemantic } from "./readonly.js";
import type * as Semantic from "./semantic/actors/index.js";

export { canonicalSemantic, requireBigIntCodec } from "./readonly.js";

export type ActorsMethod = (typeof ActorsService.methods)[number];
export type ActorsOperation = keyof typeof ActorsService.method;
export const ACTORS_OPERATION_NAMES = Object.freeze(ActorsService.methods.map(method => method.localName)) as readonly ActorsOperation[];

type SemanticMessages = {
  "acyclic.actors.v1.ActorObservation": Semantic.ActorObservation;
  "acyclic.actors.v1.AddSubscriptionRequest": Semantic.AddSubscriptionRequest;
  "acyclic.actors.v1.AddSubscriptionResponse": Semantic.AddSubscriptionResponse;
  "acyclic.actors.v1.CheckpointActorRequest": Semantic.CheckpointActorRequest;
  "acyclic.actors.v1.CheckpointActorResponse": Semantic.CheckpointActorResponse;
  "acyclic.actors.v1.CreateActorRequest": Semantic.CreateActorRequest;
  "acyclic.actors.v1.CreateActorResponse": Semantic.CreateActorResponse;
  "acyclic.actors.v1.InspectActorRequest": Semantic.InspectActorRequest;
  "acyclic.actors.v1.InspectActorResponse": Semantic.InspectActorResponse;
  "acyclic.actors.v1.InvokeActorRequest": Semantic.InvokeActorRequest;
  "acyclic.actors.v1.InvokeActorResponse": Semantic.InvokeActorResponse;
  "acyclic.actors.v1.RemoveSubscriptionRequest": Semantic.RemoveSubscriptionRequest;
  "acyclic.actors.v1.RemoveSubscriptionResponse": Semantic.RemoveSubscriptionResponse;
  "acyclic.actors.v1.ResumeSubscriptionRequest": Semantic.ResumeSubscriptionRequest;
  "acyclic.actors.v1.ResumeSubscriptionResponse": Semantic.ResumeSubscriptionResponse;
  "acyclic.actors.v1.UpdateActorRequest": Semantic.UpdateActorRequest;
  "acyclic.actors.v1.UpdateActorResponse": Semantic.UpdateActorResponse;
};
type Methods = typeof ActorsService.method;
type SemanticShape<S extends DescMessage> = SemanticMessages[Extract<MessageShape<S>["$typeName"], keyof SemanticMessages>];

type AssertCanonical<T extends true> = T;
/** Every RPC's actual Rust root must recursively match the maintained wire shape. */
export type ActorsCanonicalContract = AssertCanonical<{
  [K in keyof Methods]: CanonicalCompatible<SemanticShape<Methods[K]["input"]>, MessageShape<Methods[K]["input"]>> extends true
    ? CanonicalCompatible<SemanticShape<Methods[K]["output"]>, MessageShape<Methods[K]["output"]>> : false;
}[keyof Methods]>;

export type ActorsClientMethods = {
  readonly [K in keyof Methods]: (request: ReadonlyInputSemantic<SemanticShape<Methods[K]["input"]>>, options?: ActorsCallOptions) => Promise<ReadonlySemantic<SemanticShape<Methods[K]["output"]>>>;
};

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
