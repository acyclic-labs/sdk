#[acyclic_contract_derive::file(
    family = "actors",
    messages(
        BindingProto,
        ActorLimitsProto,
        WorkerLimitsProto,
        SubscriptionStartProto,
        SubscriptionSpecProto,
        SubscriptionObservationProto,
        ActorObservationProto,
        DeploymentObservationProto,
        DeleteWorkerRequestProto,
        DeleteWorkerResponseProto,
        InvokeWorkerRequestProto,
        DeploymentRequestProto,
        DeploymentResponseProto,
        UpdateNamespaceRequestProto,
        UpdateNamespaceResponseProto,
        InspectActorRequestProto,
        InspectActorResponseProto,
        AddSubscriptionRequestProto,
        AddSubscriptionResponseProto,
        RemoveSubscriptionRequestProto,
        RemoveSubscriptionResponseProto,
        ResumeSubscriptionRequestProto,
        ResumeSubscriptionResponseProto,
        CheckpointActorRequestProto,
        CheckpointActorResponseProto,
        DeleteAllRequestProto,
        DeleteAllResponseProto,
        HeaderProto,
        InvokeActorRequestProto,
        InvokeActorResponseProto,
        ServiceErrorProto
    ),
    enums(SubscriptionState, ActorState, ErrorCode),
    services(ActorsService)
)]
pub struct ActorsFile;

#[acyclic_contract_derive::service(file = ActorsFile)]
pub enum ActorsService {
    #[wire(auth(action = "workers/delete", extractor = "workers.delete"))]
    DeleteWorker {
        request: DeleteWorkerRequestProto,
        response: DeleteWorkerResponseProto,
    },
    #[wire(auth(action = "workers/invoke", extractor = "workers.invoke"))]
    InvokeWorker {
        request: InvokeWorkerRequestProto,
        response: InvokeActorResponseProto,
    },
    #[wire(auth(
        action = "actors/deploy-namespace",
        extractor = "actors.deploy-with-bindings"
    ))]
    DeployNamespace {
        request: DeploymentRequestProto,
        response: DeploymentResponseProto,
    },
    #[wire(auth(action = "workers/deploy", extractor = "workers.deploy-with-bindings"))]
    DeployWorker {
        request: DeploymentRequestProto,
        response: DeploymentResponseProto,
    },
    #[wire(auth(
        action = "actors/update-namespace",
        extractor = "actors.update-namespace-with-bindings"
    ))]
    UpdateNamespace {
        request: UpdateNamespaceRequestProto,
        response: UpdateNamespaceResponseProto,
    },
    #[wire(auth(action = "actors/read", extractor = "actors.read"))]
    InspectActor {
        request: InspectActorRequestProto,
        response: InspectActorResponseProto,
    },
    #[wire(auth(action = "actors/subscribe", extractor = "actors.subscribe"))]
    AddSubscription {
        request: AddSubscriptionRequestProto,
        response: AddSubscriptionResponseProto,
    },
    #[wire(auth(action = "actors/unsubscribe", extractor = "actors.unsubscribe"))]
    RemoveSubscription {
        request: RemoveSubscriptionRequestProto,
        response: RemoveSubscriptionResponseProto,
    },
    #[wire(auth(action = "actors/resume", extractor = "actors.resume"))]
    ResumeSubscription {
        request: ResumeSubscriptionRequestProto,
        response: ResumeSubscriptionResponseProto,
    },
    #[wire(auth(action = "actors/checkpoint", extractor = "actors.checkpoint"))]
    CheckpointActor {
        request: CheckpointActorRequestProto,
        response: CheckpointActorResponseProto,
    },
    #[wire(auth(action = "actors/invoke", extractor = "actors.invoke"))]
    InvokeActor {
        request: InvokeActorRequestProto,
        response: InvokeActorResponseProto,
    },
    #[wire(auth(action = "actors/delete-all", extractor = "actors.delete"))]
    DeleteAll {
        request: DeleteAllRequestProto,
        response: DeleteAllResponseProto,
    },
}
