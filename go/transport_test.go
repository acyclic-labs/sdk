package acyclicsdk_test

import (
	"context"
	"net"
	"testing"
	"time"

	actorsv1 "github.com/acyclic-labs/sdk/go/gen/actors/v1"
	streamv2 "github.com/acyclic-labs/sdk/go/gen/stream/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type actorsServer struct {
	actorsv1.UnimplementedActorsServiceServer
	metadata metadata.MD
}

func (s *actorsServer) CreateActor(ctx context.Context, request *actorsv1.CreateActorRequest) (*actorsv1.CreateActorResponse, error) {
	s.metadata, _ = metadata.FromIncomingContext(ctx)
	return &actorsv1.CreateActorResponse{Actor: &actorsv1.ActorObservation{
		ActorId:         "actor-1",
		CodeSha256:      request.GetCodeSha256(),
		CheckpointEpoch: request.GetLimits().GetMemoryBytes(),
	}}, nil
}

type streamServer struct {
	streamv2.UnimplementedStreamServiceServer
	cancelled chan struct{}
}

func (s *streamServer) Follow(request *streamv2.FollowRequest, server grpc.ServerStreamingServer[streamv2.ReadResponse]) error {
	if err := server.Send(&streamv2.ReadResponse{Record: &streamv2.Record{
		Sequence: request.GetFrom(),
		Value:    []byte("first"),
	}}); err != nil {
		return err
	}
	<-server.Context().Done()
	close(s.cancelled)
	return server.Context().Err()
}

func startServer(t *testing.T, register func(grpc.ServiceRegistrar)) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	register(server)
	go server.Serve(listener)
	return listener.Addr().String(), func() {
		server.Stop()
		listener.Close()
	}
}

func dial(t *testing.T, address string) (*grpc.ClientConn, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	connection, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	return connection, cancel
}

func TestGeneratedTypesAndUnaryMetadata(t *testing.T) {
	large := uint64(1<<63 + 17)
	request := &actorsv1.CreateActorRequest{
		CodeSha256: []byte{0, 255},
		Limits:     &actorsv1.ActorLimits{MemoryBytes: large},
		Subscriptions: []*actorsv1.SubscriptionSpec{{
			Start: &actorsv1.SubscriptionStart{Start: &actorsv1.SubscriptionStart_Cursor{Cursor: large}},
		}},
	}
	if request.GetSubscriptions()[0].GetStart().GetCursor() != large {
		t.Fatalf("uint64 oneof did not round trip")
	}
	if request.GetCodeSha256()[1] != 255 {
		t.Fatalf("bytes did not round trip")
	}
	server := &actorsServer{}
	address, stop := startServer(t, func(registrar grpc.ServiceRegistrar) {
		actorsv1.RegisterActorsServiceServer(registrar, server)
	})
	defer stop()
	connection, cancel := dial(t, address)
	defer connection.Close()
	defer cancel()
	client := actorsv1.NewActorsServiceClient(connection)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer test-token"))
	response, err := client.CreateActor(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetActor().GetCheckpointEpoch() != large {
		t.Fatalf("uint64 response did not round trip")
	}
	if got := server.metadata.Get("authorization"); len(got) != 1 || got[0] != "Bearer test-token" {
		t.Fatalf("authorization metadata not received: %v", got)
	}
}

func TestGeneratedServerStreamCancellation(t *testing.T) {
	server := &streamServer{cancelled: make(chan struct{})}
	address, stop := startServer(t, func(registrar grpc.ServiceRegistrar) {
		streamv2.RegisterStreamServiceServer(registrar, server)
	})
	defer stop()
	connection, cancel := dial(t, address)
	defer connection.Close()
	defer cancel()
	client := streamv2.NewStreamServiceClient(connection)
	ctx, stopCall := context.WithCancel(context.Background())
	call, err := client.Follow(ctx, &streamv2.FollowRequest{From: 1<<63 + 3})
	if err != nil {
		t.Fatal(err)
	}
	first, err := call.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.GetRecord().GetSequence() != 1<<63+3 || string(first.GetRecord().GetValue()) != "first" {
		t.Fatalf("stream response did not round trip: %v", first)
	}
	stopCall()
	select {
	case <-server.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not observe stream cancellation")
	}
}
