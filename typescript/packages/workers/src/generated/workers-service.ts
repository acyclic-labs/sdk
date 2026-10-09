// Generated from the canonical WorkersService descriptor. Do not edit.
import { WorkersService } from "../../generated/proto/workers/v1/workers_pb.js";
import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import type { WorkersCallOptions } from "../client.js";
import type { CanonicalCompatible, ReadonlyInputSemantic, ReadonlySemantic } from "./readonly.js";
import type * as Semantic from "./semantic/workers/index.js";

export { canonicalSemantic, requireBigIntCodec } from "./readonly.js";

export type WorkersMethod = (typeof WorkersService.methods)[number];
export type WorkersOperation = keyof typeof WorkersService.method;
export const WORKERS_OPERATION_NAMES = Object.freeze(WorkersService.methods.map(method => method.localName)) as readonly WorkersOperation[];

type SemanticMessages = {
  "acyclic.workers.v1.CancelJobRequest": Semantic.CancelJobRequest;
  "acyclic.workers.v1.CancelJobResponse": Semantic.CancelJobResponse;
  "acyclic.workers.v1.CodeVersion": Semantic.CodeVersion;
  "acyclic.workers.v1.Deployment": Semantic.Deployment;
  "acyclic.workers.v1.Error": Semantic.Error;
  "acyclic.workers.v1.Header": Semantic.Header;
  "acyclic.workers.v1.InspectJobRequest": Semantic.InspectJobRequest;
  "acyclic.workers.v1.InspectJobResponse": Semantic.InspectJobResponse;
  "acyclic.workers.v1.InvokeDeploymentRequest": Semantic.InvokeDeploymentRequest;
  "acyclic.workers.v1.InvokeResponse": Semantic.InvokeResponse;
  "acyclic.workers.v1.InvokeVersionRequest": Semantic.InvokeVersionRequest;
  "acyclic.workers.v1.JobLimits": Semantic.JobLimits;
  "acyclic.workers.v1.JobObservation": Semantic.JobObservation;
  "acyclic.workers.v1.JobResult": Semantic.JobResult;
  "acyclic.workers.v1.JobTarget": Semantic.JobTarget;
  "acyclic.workers.v1.ObjectRef": Semantic.ObjectRef;
  "acyclic.workers.v1.Payload": Semantic.Payload;
  "acyclic.workers.v1.PublishVersionRequest": Semantic.PublishVersionRequest;
  "acyclic.workers.v1.PublishVersionResponse": Semantic.PublishVersionResponse;
  "acyclic.workers.v1.RetryPolicy": Semantic.RetryPolicy;
  "acyclic.workers.v1.SelectDeploymentRequest": Semantic.SelectDeploymentRequest;
  "acyclic.workers.v1.SelectDeploymentResponse": Semantic.SelectDeploymentResponse;
  "acyclic.workers.v1.SubmitJobRequest": Semantic.SubmitJobRequest;
  "acyclic.workers.v1.SubmitJobResponse": Semantic.SubmitJobResponse;
};
type Methods = typeof WorkersService.method;
type SemanticShape<S extends DescMessage> = SemanticMessages[Extract<MessageShape<S>["$typeName"], keyof SemanticMessages>];

type AssertCanonical<T extends true> = T;
/** Every RPC's actual Rust root must recursively match the maintained wire shape. */
export type WorkersCanonicalContract = AssertCanonical<{
  [K in keyof Methods]: CanonicalCompatible<SemanticShape<Methods[K]["input"]>, MessageShape<Methods[K]["input"]>> extends true
    ? CanonicalCompatible<SemanticShape<Methods[K]["output"]>, MessageShape<Methods[K]["output"]>> : false;
}[keyof Methods]>;

export type WorkersClientMethods = {
  readonly [K in keyof Methods]: (request: ReadonlyInputSemantic<SemanticShape<Methods[K]["input"]>>, options?: WorkersCallOptions) => Promise<ReadonlySemantic<SemanticShape<Methods[K]["output"]>>>;
};

export type WorkersMethodCall = (method: WorkersMethod, request: unknown, options?: WorkersCallOptions) => Promise<unknown>;

/** Installs the typed public methods from the maintained service descriptor. */
export function installWorkersMethods(target: object, call: WorkersMethodCall): void {
  for (const method of WorkersService.methods) {
    Object.defineProperty(target, method.localName, {
      configurable: true,
      enumerable: true,
      value: (request: unknown, options?: WorkersCallOptions) => call(method, request, options),
    });
  }
}
