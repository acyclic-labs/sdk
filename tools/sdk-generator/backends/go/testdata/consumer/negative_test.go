//go:build negative

package consumer

import (
	actors "github.com/acyclic-labs/sdk/go/gen/actors/v1"
	stream "github.com/acyclic-labs/sdk/go/gen/stream/v2"
	workers "github.com/acyclic-labs/sdk/go/gen/workers/v1"
)

var badActor = actors.CreateActorRequest{CodeSha256: "not bytes"}
var badStream = stream.AppendRequest{IfTail: uint64(0)}
var badWorker = workers.PublishVersionRequest{JavascriptModule: "not bytes"}
