package acyclicsdk_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	streamv2 "github.com/acyclic-labs/sdk/go/gen/stream/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func fixtureStreamClient(t *testing.T) (streamv2.StreamServiceClient, *grpc.ClientConn) {
	t.Helper()
	address := os.Getenv("FIXTURE_GRPC_ADDRESS")
	if address == "" {
		t.Skip("FIXTURE_GRPC_ADDRESS is required for Rust fixture transport tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial Rust fixture: %v", err)
	}
	return streamv2.NewStreamServiceClient(connection), connection
}

func fixtureAppendRequest(path, value, key string, tail uint64) *streamv2.AppendRequest {
	return &streamv2.AppendRequest{
		Path: path, Records: [][]byte{[]byte(value)}, IfTail: &tail, IdempotencyKey: []byte(key),
	}
}

func fixtureAppendReceipt(t *testing.T, response *streamv2.AppendResponse) *streamv2.AppendReceipt {
	t.Helper()
	committed, ok := response.GetOutcome().(*streamv2.AppendResponse_Committed)
	if !ok || committed.Committed == nil {
		t.Fatalf("expected committed append outcome, got %v", response.GetOutcome())
	}
	return committed.Committed
}

func TestRustFixtureStreamIdempotencyAndErrors(t *testing.T) {
	client, connection := fixtureStreamClient(t)
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	path := "go-transport/idempotency-" + suffix
	key := "go-idempotency-" + suffix
	first, err := client.Append(ctx, fixtureAppendRequest(path, "one", key, 0))
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	receipt := fixtureAppendReceipt(t, first)
	if receipt.Start != 0 || receipt.End != 1 || receipt.Tail != 1 || len(receipt.CommitId) != 32 {
		t.Fatalf("unexpected first receipt: %v", receipt)
	}

	replay, err := client.Append(ctx, fixtureAppendRequest(path, "one", key, 0))
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	replayReceipt := fixtureAppendReceipt(t, replay)
	if string(replayReceipt.CommitId) != string(receipt.CommitId) || replayReceipt.Start != receipt.Start || replayReceipt.End != receipt.End {
		t.Fatalf("replay changed receipt: first=%v replay=%v", receipt, replayReceipt)
	}

	observationResponse, err := client.InspectIdempotency(ctx, &streamv2.InspectIdempotencyRequest{IdempotencyKey: []byte(key)})
	if err != nil {
		t.Fatalf("inspect idempotency: %v", err)
	}
	observation := observationResponse.GetObservation()
	if observation == nil || string(observation.GetIdempotencyKey()) != key || len(observation.GetRequestDigest()) != 32 || observation.GetAppend() == nil {
		t.Fatalf("unexpected idempotency observation: %v", observation)
	}

	_, err = client.Append(ctx, fixtureAppendRequest(path, "different", key, 0))
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "idempotency_mismatch" {
		t.Fatalf("expected idempotency mismatch, got code=%s message=%q err=%v", status.Code(err), status.Convert(err).Message(), err)
	}

	conflict, err := client.Append(ctx, fixtureAppendRequest(path, "two", "go-tail-conflict-"+suffix, 0))
	if err != nil {
		t.Fatalf("tail conflict append: %v", err)
	}
	if conflict.GetConflict() == nil || conflict.GetConflict().GetActualTail() != 1 {
		t.Fatalf("expected tail conflict at one, got %v", conflict)
	}

	outOfRange, err := client.Read(ctx, &streamv2.ReadRequest{Path: path, From: 100, Limit: 1})
	if err == nil {
		_, err = outOfRange.Recv()
	}
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("expected out-of-range read, got code=%s err=%v", status.Code(err), err)
	}
	missing, err := client.Read(ctx, &streamv2.ReadRequest{Path: path + "/missing", From: 0, Limit: 1})
	if err == nil {
		_, err = missing.Recv()
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected missing-path read, got code=%s err=%v", status.Code(err), err)
	}
}

func TestRustFixtureStreamCancellationAndRecovery(t *testing.T) {
	client, connection := fixtureStreamClient(t)
	defer connection.Close()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	path := "go-transport/recovery-" + suffix
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Append(ctx, fixtureAppendRequest(path, "first", "go-recovery-first-"+suffix, 0)); err != nil {
		t.Fatalf("seed append: %v", err)
	}

	followCtx, cancelFollow := context.WithCancel(context.Background())
	follow, err := client.Follow(followCtx, &streamv2.FollowRequest{Path: path, From: 0})
	if err != nil {
		cancelFollow()
		t.Fatalf("open first follow: %v", err)
	}
	first, err := follow.Recv()
	if err != nil || first.GetRecord().GetSequence() != 0 || string(first.GetRecord().GetValue()) != "first" {
		cancelFollow()
		t.Fatalf("first follow record: record=%v err=%v", first, err)
	}
	cancelFollow()
	if _, err := follow.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("expected canceled follow, got code=%s err=%v", status.Code(err), err)
	}

	recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRecovery()
	recovered, err := client.Follow(recoveryCtx, &streamv2.FollowRequest{Path: path, From: 1})
	if err != nil {
		t.Fatalf("open recovery follow: %v", err)
	}
	appendDone := make(chan error, 1)
	go func() {
		_, appendErr := client.Append(recoveryCtx, fixtureAppendRequest(path, "second", "go-recovery-second-"+suffix, 1))
		appendDone <- appendErr
	}()
	second, err := recovered.Recv()
	if err != nil || second.GetRecord().GetSequence() != 1 || string(second.GetRecord().GetValue()) != "second" {
		t.Fatalf("recovered follow record: record=%v err=%v", second, err)
	}
	if err := <-appendDone; err != nil {
		t.Fatalf("recovery append: %v", err)
	}
}
