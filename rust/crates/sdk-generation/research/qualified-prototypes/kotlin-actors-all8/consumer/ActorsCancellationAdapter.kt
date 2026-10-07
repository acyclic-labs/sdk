package adapter

import uniffi.acyclic_actors_uniffi.ActorId
import uniffi.acyclic_actors_uniffi.ActorObservation
import uniffi.acyclic_actors_uniffi.ActorsClient
import uniffi.acyclic_actors_uniffi.AddSubscriptionRequest
import uniffi.acyclic_actors_uniffi.CheckpointActorRequest
import uniffi.acyclic_actors_uniffi.CreateActorRequest
import uniffi.acyclic_actors_uniffi.InspectActorRequest
import uniffi.acyclic_actors_uniffi.InvokeActorRequest
import uniffi.acyclic_actors_uniffi.InvokeActorResponse
import uniffi.acyclic_actors_uniffi.RemoveSubscriptionRequest
import uniffi.acyclic_actors_uniffi.ResumeSubscriptionRequest
import uniffi.acyclic_actors_uniffi.UpdateActorRequest
import uniffi.acyclic_actors_uniffi.connectActors as generatedConnectActors
import uniffi.acyclic_actors_uniffi.connectActorsWithCa as generatedConnectActorsWithCa

suspend fun connectActors(endpoint: String, token: String): ActorsClient = generatedConnectActors(endpoint, token, null)

suspend fun connectActorsWithCa(endpoint: String, token: String, caCertificate: ByteArray?): ActorsClient = generatedConnectActorsWithCa(endpoint, token, caCertificate, null)

suspend fun ActorsClient.addSubscription(request: AddSubscriptionRequest): ActorObservation? = addSubscription(request, null)

suspend fun ActorsClient.checkpointActor(request: CheckpointActorRequest): ActorObservation? = checkpointActor(request, null)

suspend fun ActorsClient.createActor(request: CreateActorRequest): ActorObservation? = createActor(request, null)

suspend fun ActorsClient.inspectActor(actorId: ActorId): ActorObservation? = inspectActor(actorId, null)

suspend fun ActorsClient.inspectActor(request: InspectActorRequest): ActorObservation? = inspectActorRequest(request, null)

suspend fun ActorsClient.invokeActor(request: InvokeActorRequest): InvokeActorResponse = invokeActor(request, null)

suspend fun ActorsClient.removeSubscription(request: RemoveSubscriptionRequest): ActorObservation? = removeSubscription(request, null)

suspend fun ActorsClient.resumeSubscription(request: ResumeSubscriptionRequest): ActorObservation? = resumeSubscription(request, null)

suspend fun ActorsClient.updateActor(request: UpdateActorRequest): ActorObservation? = updateActor(request, null)

