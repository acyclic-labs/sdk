import { ActorsClient } from "@acyclic-labs/actors";
import type {
  ActorId,
  ActorLimits,
  AddSubscriptionRequest,
  AddSubscriptionResponse,
  CheckpointActorRequest,
  CheckpointActorResponse,
  CodeSha256,
  CreateActorRequest,
  CreateActorResponse,
  InspectActorRequest,
  InspectActorResponse,
  InvokeActorRequest,
  InvokeActorResponse,
  PositiveU64,
  RemoveSubscriptionRequest,
  RemoveSubscriptionResponse,
  ResumeSubscriptionRequest,
  ResumeSubscriptionResponse,
  SubscriptionSpec,
  SubscriptionStart,
  UpdateActorRequest,
  UpdateActorResponse,
} from "@acyclic-labs/actors/types";

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
