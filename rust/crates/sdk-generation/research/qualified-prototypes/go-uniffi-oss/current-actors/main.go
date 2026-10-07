package main

import (
    "encoding/hex"
    "errors"
    "fmt"
    "math"
    "os"

    actors "example.local/actors-uniffi-current/generated/acyclic_actors_uniffi"
)

func must[T any](v T, err error) T {
    if err != nil { panic(err) }
    return v
}

func main() {
    results := map[string]any{}
    if _, err := actors.NewActorId(""); err == nil {
        panic("empty actor id unexpectedly accepted")
    } else {
        results["empty_actor_id_error"] = errors.Is(err, actors.ErrBindingErrorSemantic)
        if !errors.Is(err, actors.ErrBindingErrorSemantic) { panic(fmt.Sprintf("empty actor id error type: %T %v", err, err)) }
    }
    actor := must(actors.NewActorId("actor-a"))
    if actor.Value() != "actor-a" { panic("actor identity roundtrip") }
    results["actor_id"] = actor.Value()

    if _, err := actors.NewCodeSha256(make([]byte, 31)); err == nil { panic("31-byte sha accepted") } else { results["short_sha_error"] = fmt.Sprintf("%T", err) }
    digest := make([]byte, 32); for i := range digest { digest[i] = 1 }
    sha := must(actors.NewCodeSha256(digest))
    if got := sha.Value(); hex.EncodeToString(got) != hex.EncodeToString(digest) { panic("sha roundtrip") }
    results["sha256"] = hex.EncodeToString(sha.Value())

    if _, err := actors.NewPositiveU64(0); err == nil { panic("zero PositiveU64 accepted") } else { results["zero_positive_error"] = fmt.Sprintf("%T", err) }
    positive := must(actors.NewPositiveU64(math.MaxUint64))
    if positive.Value() != math.MaxUint64 { panic("u64 max did not roundtrip") }
    results["u64_max"] = positive.Value()

    cursor := must(actors.NewSubscriptionSpec("sub-a", "events/input", actors.SubscriptionStartCursor{Cursor: 9007199254740993}, false))
    currentHead := must(actors.NewSubscriptionSpec("sub-head", "events/head", actors.SubscriptionStartCurrentHead{CurrentHead: true}, true))
    if cursor.SubscriptionId() != "sub-a" || cursor.StreamPath() != "events/input" || !currentHead.PlacementAnchor() { panic("subscription presence/nominal roundtrip") }
    results["subscription_cursor"] = uint64(9007199254740993)
    results["subscription_current_head"] = currentHead.SubscriptionId()

    cancel := actors.NewCancellationHandle(); if cancel.IsCancelled() { panic("new cancellation handle cancelled") }; cancel.Cancel(); if !cancel.IsCancelled() { panic("cancel flag did not roundtrip") }; results["cancelled"] = cancel.IsCancelled()

    limits := must(actors.NewActorLimits(1000, 4096, 8192))
    binding := must(actors.NewBinding("primary", "compute", "resource://primary"))
    create := must(actors.NewCreateActorRequest(sha, "eu", []*actors.Binding{binding}, limits, []*actors.SubscriptionSpec{}, "create-a"))
    update := must(actors.NewUpdateActorRequest(actor, sha, []*actors.Binding{binding}, limits, 0, "update-a"))
    add := must(actors.NewAddSubscriptionRequest(actor, cursor, "add-a"))
    remove := actors.NewRemoveSubscriptionRequest(actor, "sub-a", "remove-a")
    resume := actors.NewResumeSubscriptionRequest(actor, "sub-a", "resume-a")
    checkpoint := actors.NewCheckpointActorRequest(actor, "checkpoint-a")
    invoke := actors.NewInvokeActorRequest(actor, "POST", "/result", []byte(`{"input":true}`), []actors.Header{{Name: "content-type", Value: "application/json"}})
    ca := must(os.ReadFile(`Q:\sdk\work\actors-uniffi-go-current-20261007\fixture-ca-live.pem`))
    client := must(actors.ConnectActorsWithCa("https://localhost:62993", "conformance", &ca, nil))
    call := func(name string, obs *actors.ActorObservation, err error) {
        if err != nil { panic(fmt.Sprintf("%s: %v", name, err)) }
        if obs == nil || obs.ActorId == nil { panic(fmt.Sprintf("%s: missing observation", name)) }
        if obs.ActorId.Value() != "actor-a" || obs.CheckpointEpoch != 9 || obs.State != actors.ActorStateActive { panic(fmt.Sprintf("%s: unexpected observation state=%v", name, obs.State)) }
        results[name] = map[string]any{"actor_id": obs.ActorId.Value(), "checkpoint_epoch": obs.CheckpointEpoch, "configuration_revision": obs.ConfigurationRevision, "subscriptions": len(obs.Subscriptions), "state": obs.State}
    }
    checked := func(name string, f func() (*actors.ActorObservation, error)) { obs, err := f(); call(name, obs, err) }
    checked("inspect", func() (*actors.ActorObservation, error) { return client.InspectActor(actor, nil) })
    checked("create", func() (*actors.ActorObservation, error) { return client.CreateActor(create, nil) })
    checked("update", func() (*actors.ActorObservation, error) { return client.UpdateActor(update, nil) })
    checked("add", func() (*actors.ActorObservation, error) { return client.AddSubscription(add, nil) })
    checked("remove", func() (*actors.ActorObservation, error) { return client.RemoveSubscription(remove, nil) })
    checked("resume", func() (*actors.ActorObservation, error) { return client.ResumeSubscription(resume, nil) })
    checked("checkpoint", func() (*actors.ActorObservation, error) { return client.CheckpointActor(checkpoint, nil) })
    response, err := client.InvokeActor(invoke, nil); if err != nil { panic(fmt.Sprintf("invoke: %v", err)) }; results["invoke"] = response

    fmt.Printf("GO_CURRENT_ACTORS_PASS results=%v\n", results)
}

