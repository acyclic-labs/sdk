#[acyclic_contract_derive::file(
    family = "actors",
    messages(
        BindingProto,
        ActorLimitsProto,
        SubscriptionStartProto,
        SubscriptionSpecProto,
        SubscriptionObservationProto,
        ActorObservationProto,
        CreateActorRequestProto,
        CreateActorResponseProto,
        UpdateActorRequestProto,
        UpdateActorResponseProto,
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
        DeleteActorRequestProto,
        DeleteActorResponseProto,
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
    #[wire(auth(action = "actors/create", extractor = "actors.create-with-bindings"))]
    CreateActor {
        request: CreateActorRequestProto,
        response: CreateActorResponseProto,
    },
    #[wire(auth(action = "actors/update", extractor = "actors.update-with-bindings"))]
    UpdateActor {
        request: UpdateActorRequestProto,
        response: UpdateActorResponseProto,
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
    #[wire(auth(action = "actors/delete", extractor = "actors.delete"))]
    DeleteActor {
        request: DeleteActorRequestProto,
        response: DeleteActorResponseProto,
    },
}
