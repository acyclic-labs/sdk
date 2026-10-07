import { ActorsClient, type ReadonlySemantic } from "@acyclic-labs/actors";
import type { semantic } from "@acyclic-labs/actors";

type ActorId = semantic.ActorId;
type ActorLimits = semantic.ActorLimits;
type AddSubscriptionRequest = semantic.AddSubscriptionRequest;
type AddSubscriptionResponse = ReadonlySemantic<semantic.AddSubscriptionResponse>;
type CheckpointActorRequest = semantic.CheckpointActorRequest;
type CheckpointActorResponse = ReadonlySemantic<semantic.CheckpointActorResponse>;
type CodeSha256 = semantic.CodeSha256;
type CreateActorRequest = semantic.CreateActorRequest;
type CreateActorResponse = ReadonlySemantic<semantic.CreateActorResponse>;
type InspectActorRequest = semantic.InspectActorRequest;
type InspectActorResponse = ReadonlySemantic<semantic.InspectActorResponse>;
type InvokeActorRequest = semantic.InvokeActorRequest;
type InvokeActorResponse = ReadonlySemantic<semantic.InvokeActorResponse>;
type PositiveU64 = semantic.PositiveU64;
type RemoveSubscriptionRequest = semantic.RemoveSubscriptionRequest;
type RemoveSubscriptionResponse = ReadonlySemantic<semantic.RemoveSubscriptionResponse>;
type ResumeSubscriptionRequest = semantic.ResumeSubscriptionRequest;
type ResumeSubscriptionResponse = ReadonlySemantic<semantic.ResumeSubscriptionResponse>;
type SubscriptionSpec = semantic.SubscriptionSpec;
type SubscriptionStart = semantic.SubscriptionStart;
type UpdateActorRequest = semantic.UpdateActorRequest;
type UpdateActorResponse = ReadonlySemantic<semantic.UpdateActorResponse>;

declare const client: ActorsClient;
declare const actorId: ActorId;
declare const codeSha256: CodeSha256;
declare const positive: PositiveU64;

const limits: ActorLimits = {
  handlerTimeoutMillis: positive,
  memoryBytes: positive,
  checkpointBytes: positive,
};
const start: SubscriptionStart = { start: { case: "currentHead", value: true } };
const subscription: SubscriptionSpec = {
  subscriptionId: "subscription-a",
  streamPath: "events/input",
  start,
  placementAnchor: false,
};

const createRequest: CreateActorRequest = {
  codeSha256,
  homeRegion: "eu",
  bindings: [],
  limits,
  subscriptions: [subscription],
  idempotencyKey: "create-a",
};
const updateRequest: UpdateActorRequest = {
  actorId,
  codeSha256,
  bindings: [],
  limits,
  expectedConfigurationRevision: 1n,
  idempotencyKey: "update-a",
};
const inspectRequest: InspectActorRequest = { actorId };
const addRequest: AddSubscriptionRequest = {
  actorId,
  subscription,
  idempotencyKey: "subscribe-a",
};
const removeRequest: RemoveSubscriptionRequest = {
  actorId,
  subscriptionId: "subscription-a",
  idempotencyKey: "remove-a",
};
const resumeRequest: ResumeSubscriptionRequest = {
  actorId,
  subscriptionId: "subscription-a",
  idempotencyKey: "resume-a",
};
const checkpointRequest: CheckpointActorRequest = {
  actorId,
  idempotencyKey: "checkpoint-a",
};
const invokeRequest: InvokeActorRequest = {
  actorId,
  method: "POST",
  url: "/invoke",
  body: new Uint8Array(),
  headers: [],
};

const createResult: Promise<CreateActorResponse> = client.createActor(createRequest);
const updateResult: Promise<UpdateActorResponse> = client.updateActor(updateRequest);
const inspectResult: Promise<InspectActorResponse> = client.inspectActor(inspectRequest);
const addResult: Promise<AddSubscriptionResponse> = client.addSubscription(addRequest);
const removeResult: Promise<RemoveSubscriptionResponse> = client.removeSubscription(removeRequest);
const resumeResult: Promise<ResumeSubscriptionResponse> = client.resumeSubscription(resumeRequest);
const checkpointResult: Promise<CheckpointActorResponse> = client.checkpointActor(checkpointRequest);
const invokeResult: Promise<InvokeActorResponse> = client.invokeActor(invokeRequest);

void [createResult, updateResult, inspectResult, addResult, removeResult, resumeResult, checkpointResult, invokeResult];
