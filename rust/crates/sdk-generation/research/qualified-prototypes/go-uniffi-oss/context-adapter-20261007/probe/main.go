package main

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "os"
    "time"

    actors "example.local/actors-uniffi-context/generated/acyclic_actors_uniffi"
)

type fixtureOptions struct {
    Endpoint string `json:"endpoint"`
    Token string `json:"token"`
    CaCertificate string `json:"caCertificate"`
    ActorID string `json:"actorId"`
}

func main() {
    optionsBytes, err := os.ReadFile(`Q:\sdk\work\actors-uniffi-go-current-20261007\context-options-2-20261007.json`)
    if err != nil { panic(err) }
    var options fixtureOptions
    if err := json.Unmarshal(optionsBytes, &options); err != nil { panic(err) }
    ca := []byte(options.CaCertificate)
    client, err := actors.ConnectActorsWithCa(options.Endpoint, options.Token, &ca, nil, context.Background())
    if err != nil { panic(fmt.Errorf("connect: %w", err)) }
    defer client.Destroy()
    actor, err := actors.NewActorId(options.ActorID)
    if err != nil { panic(fmt.Errorf("actor: %w", err)) }
    defer actor.Destroy()

    ctx, cancel := context.WithCancel(context.Background())
    result := make(chan error, 1)
    go func() {
        _, callErr := client.InspectActor(actor, nil, ctx)
        result <- callErr
    }()
    time.Sleep(750 * time.Millisecond)
    cancel()
    select {
    case callErr := <-result:
        if !errors.Is(callErr, actors.ErrBindingErrorCancelled) {
            panic(fmt.Errorf("context cancellation returned %T %v", callErr, callErr))
        }
        if err := os.WriteFile(`Q:\sdk\work\actors-uniffi-go-current-20261007\release-context-2-20261007`, []byte("release\n"), 0o600); err != nil { panic(err) }
        fmt.Printf("GO_CURRENT_ACTORS_CONTEXT_CANCEL_PASS error=ErrBindingErrorCancelled manual_handle=nil endpoint=%s\n", options.Endpoint)
    case <-time.After(5 * time.Second):
        panic("context cancellation timed out")
    }
}

