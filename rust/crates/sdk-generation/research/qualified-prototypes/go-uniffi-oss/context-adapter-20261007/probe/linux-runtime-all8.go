package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"

	actors "example.local/go-uniffi-qual/actors/acyclic_actors_uniffi"
)

type runtimeOptions struct {
	Endpoint     string `json:"endpoint"`
	Token        string `json:"token"`
	CACertificate string `json:"caCertificate"`
	ActorID      string `json:"actorId"`
}

func must[T any](v T, err error) T {
	if err != nil { panic(err) }
	return v
}

func main() {
	raw := must(os.ReadFile(`/mnt/q/sdk/work/actors-uniffi-go-current-20261007/context-options-normal-wsl-20261007.json`))
	var opts runtimeOptions
	must(struct{}{}, json.Unmarshal(raw, &opts))
	actor := must(actors.NewActorId(opts.ActorID))
	digest := make([]byte, 32)
	for i := range digest { digest[i] = 1 }
	sha := must(actors.NewCodeSha256(digest))
	limits := must(actors.NewActorLimits(1000, 4096, 8192))
	binding := must(actors.NewBinding("primary", "compute", "resource://primary"))
	cursor := must(actors.NewSubscriptionSpec("sub-a", "events/input", actors.SubscriptionStartCursor{Cursor: 9007199254740993}, false))
	currentHead := must(actors.NewSubscriptionSpec("sub-head", "events/head", actors.SubscriptionStartCurrentHead{CurrentHead: true}, true))
	create := must(actors.NewCreateActorRequest(sha, "eu", []*actors.Binding{binding}, limits, []*actors.SubscriptionSpec{}, "create-a"))
	update := must(actors.NewUpdateActorRequest(actor, sha, []*actors.Binding{binding}, limits, 0, "update-a"))
	add := must(actors.NewAddSubscriptionRequest(actor, cursor, "add-a"))
	remove := actors.NewRemoveSubscriptionRequest(actor, "sub-a", "remove-a")
	resume := actors.NewResumeSubscriptionRequest(actor, "sub-a", "resume-a")
	checkpoint := actors.NewCheckpointActorRequest(actor, "checkpoint-a")
	invoke := actors.NewInvokeActorRequest(actor, "POST", "/result", []byte(`{"input":true}`), []actors.Header{{Name: "content-type", Value: "application/json"}})
	ca := []byte(opts.CACertificate)
	client := must(actors.ConnectActorsWithCa(opts.Endpoint, opts.Token, &ca, nil, context.Background()))
	ctx := context.Background()
	check := func(name string, obs *actors.ActorObservation, err error) {
		if err != nil || obs == nil || obs.ActorId == nil || obs.ActorId.Value() != "actor-a" || obs.CheckpointEpoch != 9 || obs.State != actors.ActorStateActive {
			panic(fmt.Sprintf("%s failed: obs=%v err=%v", name, obs, err))
		}
	}
	run := func(name string, f func() (*actors.ActorObservation, error)) { obs, err := f(); check(name, obs, err) }
	run("inspect", func() (*actors.ActorObservation, error) { return client.InspectActor(actor, nil, ctx) })
	run("create", func() (*actors.ActorObservation, error) { return client.CreateActor(create, nil, ctx) })
	run("update", func() (*actors.ActorObservation, error) { return client.UpdateActor(update, nil, ctx) })
	run("add", func() (*actors.ActorObservation, error) { return client.AddSubscription(add, nil, ctx) })
	run("remove", func() (*actors.ActorObservation, error) { return client.RemoveSubscription(remove, nil, ctx) })
	run("resume", func() (*actors.ActorObservation, error) { return client.ResumeSubscription(resume, nil, ctx) })
	run("checkpoint", func() (*actors.ActorObservation, error) { return client.CheckpointActor(checkpoint, nil, ctx) })
	response, err := client.InvokeActor(invoke, nil, ctx)
	if err != nil || response.Status != 201 || len(response.Headers) != 1 || response.Headers[0].Value != "/result" { panic(fmt.Sprintf("invoke failed: %+v %v", response, err)) }
	if currentHead.SubscriptionId() != "sub-head" || !currentHead.PlacementAnchor() || cursor.SubscriptionId() != "sub-a" { panic("subscription presence failed") }
	max := must(actors.NewPositiveU64(math.MaxUint64))
	if max.Value() != math.MaxUint64 { panic("u64 max failed") }
	if _, err := actors.NewActorId(""); err == nil || !errors.Is(err, actors.ErrBindingErrorSemantic) { panic(fmt.Sprintf("empty actor id error: %v", err)) }
	if _, err := actors.NewCodeSha256(make([]byte, 31)); err == nil { panic("short sha accepted") }
	if _, err := actors.NewPositiveU64(0); err == nil { panic("zero positive accepted") }
	fmt.Printf("GO_SOURCE_REGENERATED_ALL8_PASS inspect=create=update=add=remove=resume=checkpoint invoke_status=%d cursor=9007199254740993 u64_max=%d semantic_typed=true\n", response.Status, max.Value())
}
