package adapter

import uniffi.acyclic_actors.ActorObservation
import uniffi.acyclic_actors.AddSubscriptionRequest
import uniffi.acyclic_actors.CheckpointActorRequest
import uniffi.acyclic_actors.CreateActorRequest
import uniffi.acyclic_actors.InspectActorRequest
import uniffi.acyclic_actors.InvokeActorRequest
import uniffi.acyclic_actors.InvokeActorResponse
import uniffi.acyclic_actors.RemoveSubscriptionRequest
import uniffi.acyclic_actors.ResumeSubscriptionRequest
import uniffi.acyclic_actors.UpdateActorRequest
import uniffi.acyclic_actors_uniffi.ActorsClient
import uniffi.acyclic_actors_uniffi.CancellationHandle
import uniffi.acyclic_actors_uniffi.connectActors as generatedConnectActors
import uniffi.acyclic_actors_uniffi.connectActorsWithCa as generatedConnectActorsWithCa

suspend fun connectActors(endpoint: String, token: String): ActorsClient = generatedConnectActors(endpoint, token, null)

suspend fun connectActorsWithCa(endpoint: String, token: String, caCertificate: ByteArray?): ActorsClient =
    generatedConnectActorsWithCa(endpoint, token, caCertificate, null)

suspend fun ActorsClient.addSubscription(request: AddSubscriptionRequest, handle: CancellationHandle? = null): ActorObservation? =
    addSubscription(request, handle).actor

suspend fun ActorsClient.checkpointActor(request: CheckpointActorRequest, handle: CancellationHandle? = null): ActorObservation? =
    checkpointActor(request, handle).actor

suspend fun ActorsClient.createActor(request: CreateActorRequest, handle: CancellationHandle? = null): ActorObservation? =
    createActor(request, handle).actor

suspend fun ActorsClient.inspectActor(request: InspectActorRequest, handle: CancellationHandle? = null): ActorObservation? =
    inspectActor(request, handle).actor

suspend fun ActorsClient.invokeActor(request: InvokeActorRequest, handle: CancellationHandle? = null): InvokeActorResponse =
    invokeActor(request, handle)

suspend fun ActorsClient.removeSubscription(request: RemoveSubscriptionRequest, handle: CancellationHandle? = null): ActorObservation? =
    removeSubscription(request, handle).actor

suspend fun ActorsClient.resumeSubscription(request: ResumeSubscriptionRequest, handle: CancellationHandle? = null): ActorObservation? =
    resumeSubscription(request, handle).actor

suspend fun ActorsClient.updateActor(request: UpdateActorRequest, handle: CancellationHandle? = null): ActorObservation? =
    updateActor(request, handle).actor