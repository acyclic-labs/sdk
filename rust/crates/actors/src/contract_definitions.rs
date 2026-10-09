proto_package!(
    ACTORS_PACKAGE,
    name = "acyclic.actors.v1",
    files = [ACTORS_FILE]
);
define_proto_file!(
    ACTORS_FILE,
    name = "actors/v1/actors.proto",
    package = ACTORS_PACKAGE,
    options =
        [proto_option!("go_package" => "github.com/acyclic-labs/sdk/go/gen/actors/v1;actorsv1")],
    messages = [
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
        HeaderProto,
        InvokeActorRequestProto,
        InvokeActorResponseProto,
        ServiceErrorProto,
    ],
    enums = [SubscriptionState, ActorState, ErrorCode],
    services = [ActorsService],
);

#[proto_service]
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
