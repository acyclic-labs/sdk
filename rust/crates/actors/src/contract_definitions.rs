
#[acyclic_contract_derive::file(family = "actors", messages(BindingProto,
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
        HeaderProto,
        InvokeActorRequestProto,
        InvokeActorResponseProto,
        ServiceErrorProto), enums(SubscriptionState, ActorState, ErrorCode), services(ActorsService))]
pub struct ActorsFile;

#[acyclic_contract_derive::service]
pub enum ActorsService {
    CreateActor { request: CreateActorRequestProto, response: CreateActorResponseProto },
    UpdateActor { request: UpdateActorRequestProto, response: UpdateActorResponseProto },
    InspectActor { request: InspectActorRequestProto, response: InspectActorResponseProto },
    AddSubscription { request: AddSubscriptionRequestProto, response: AddSubscriptionResponseProto },
    RemoveSubscription { request: RemoveSubscriptionRequestProto, response: RemoveSubscriptionResponseProto },
    ResumeSubscription { request: ResumeSubscriptionRequestProto, response: ResumeSubscriptionResponseProto },
    CheckpointActor { request: CheckpointActorRequestProto, response: CheckpointActorResponseProto },
    InvokeActor { request: InvokeActorRequestProto, response: InvokeActorResponseProto },
}
