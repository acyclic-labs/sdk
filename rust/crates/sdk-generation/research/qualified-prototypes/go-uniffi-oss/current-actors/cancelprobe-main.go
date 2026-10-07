package main

import (
    "encoding/json"
    "errors"
    "fmt"
    "os"
    "time"

    actors "example.local/actors-uniffi-current/generated/acyclic_actors_uniffi"
)

type fixtureOptions struct { Endpoint string `json:"endpoint"`; Token string `json:"token"`; CACertificate string `json:"caCertificate"`; ActorID string `json:"actorId"` }

func main() {
    raw, err := os.ReadFile(`Q:\sdk\work\actors-uniffi-go-current-20261007\pending-options.json`); if err != nil { panic(err) }
    var opts fixtureOptions; if err := json.Unmarshal(raw, &opts); err != nil { panic(err) }
    actor, err := actors.NewActorId(opts.ActorID); if err != nil { panic(err) }
    ca := []byte(opts.CACertificate)
    client, err := actors.ConnectActorsWithCa(opts.Endpoint, opts.Token, &ca, nil); if err != nil { panic(err) }
    handle := actors.NewCancellationHandle()
    done := make(chan error, 1)
    go func() { _, callErr := client.InspectActor(actor, &handle); done <- callErr }()
    time.Sleep(300 * time.Millisecond)
    handle.Cancel()
    select {
    case callErr := <-done:
        if !errors.Is(callErr, actors.ErrBindingErrorCancelled) { panic(fmt.Sprintf("expected cancellation, got %v", callErr)) }
        if !handle.IsCancelled() { panic("cancel handle lost state") }
        if err := os.WriteFile(`Q:\sdk\work\actors-uniffi-go-current-20261007\release-inspect`, []byte("release"), 0600); err != nil { panic(err) }
        fmt.Println("GO_CURRENT_ACTORS_INFLIGHT_CANCEL_PASS error=ErrBindingErrorCancelled handle=true")
    case <-time.After(5 * time.Second):
        _ = os.WriteFile(`Q:\sdk\work\actors-uniffi-go-current-20261007\release-inspect`, []byte("release"), 0600)
        panic("timed out waiting for cancelled call")
    }
}