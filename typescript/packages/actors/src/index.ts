/** Rust-owned Actors v1 contract with generated Protobuf message types. */
export * from "../generated/proto/actors/v1/actors_pb.js";
export * as semantic from "./generated/semantic/actors/readonly.js";
export * from "./client.js";
export * from "./guest.js";
export { GuestActivation, definitionHandler, type DefinitionDeployment } from "./guest-host.js";
export { conformanceDefinition, verifyDefinitionHost, type DefinitionHost } from "./definition-conformance.js";
export { openGuestResponse, type GuestResponseFrame } from "./fetch-response.js";
export { performanceObserver, type AcyclicObserver, type OperationEvent } from "@acyclic-labs/core/observe";
export { credentialProvider, type CredentialProvider, type CredentialOptions } from "@acyclic-labs/auth/credential";

export * from "./namespace.js";
export * from "./durable.js";
export * as liveUpdate from "./live-update.js";

export { bindingRequest, bindingResponse, bindingSignal, readBindingRequestBody, type Fetch } from "./binding-fetch.js";
export { materializeActorState, type ActorStateHost, type ActorStateBinding, type ActorStateTransaction } from "./state-binding.js";

export { AUTHORIZATION, type ActorsMethod } from "./generated/authorization.js";
