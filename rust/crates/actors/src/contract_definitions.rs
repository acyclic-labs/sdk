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
    #[wire(auth(extractor = "actors.create-with-bindings"))]
    CreateActor {
        request: CreateActorRequestProto,
        response: CreateActorResponseProto,
    },
    #[wire(auth(extractor = "actors.update-with-bindings"))]
    UpdateActor {
        request: UpdateActorRequestProto,
        response: UpdateActorResponseProto,
    },
    #[wire(auth(extractor = "actors.read"))]
    InspectActor {
        request: InspectActorRequestProto,
        response: InspectActorResponseProto,
    },
    #[wire(auth(extractor = "actors.subscribe"))]
    AddSubscription {
        request: AddSubscriptionRequestProto,
        response: AddSubscriptionResponseProto,
    },
    #[wire(auth(extractor = "actors.unsubscribe"))]
    RemoveSubscription {
        request: RemoveSubscriptionRequestProto,
        response: RemoveSubscriptionResponseProto,
    },
    #[wire(auth(extractor = "actors.resume"))]
    ResumeSubscription {
        request: ResumeSubscriptionRequestProto,
        response: ResumeSubscriptionResponseProto,
    },
    #[wire(auth(extractor = "actors.checkpoint"))]
    CheckpointActor {
        request: CheckpointActorRequestProto,
        response: CheckpointActorResponseProto,
    },
    #[wire(auth(extractor = "actors.invoke"))]
    InvokeActor {
        request: InvokeActorRequestProto,
        response: InvokeActorResponseProto,
    },
    #[wire(auth(extractor = "actors.delete"))]
    DeleteActor {
        request: DeleteActorRequestProto,
        response: DeleteActorResponseProto,
    },
}
