package acyclicsdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	actorsv1 "github.com/acyclic-labs/sdk/go/gen/actors/v1"
	protocolv1 "github.com/acyclic-labs/sdk/go/gen/protocol/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func fixtureHandshakeJSON(t *testing.T, family string) []byte {
	t.Helper()
	identity := Handshake[family]
	body, err := protojson.Marshal(&protocolv1.HandshakeResponse{
		Protocol: &protocolv1.ProtocolIdentity{
			Version:          identity.Version,
			DescriptorDigest: identity.DescriptorDigest,
		},
		Supported: &protocolv1.CapabilitySet{
			Capabilities: []*protocolv1.Capability{{Name: family, Version: identity.Version}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func fixtureClient(endpoint string, httpClient *http.Client) *Client {
	return &Client{
		endpoint:    endpoint,
		bearerToken: "fixture-token",
		httpClient:  httpClient,
		transports:  map[string]string{},
	}
}

func TestHTTPHandshakeFixtureUsesBodylessGetAndRustIdentity(t *testing.T) {
	var method, authorization, accept string
	var body []byte
	handshake := fixtureHandshakeJSON(t, "actors")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method = request.Method
		authorization = request.Header.Get("authorization")
		accept = request.Header.Get("accept")
		body, _ = io.ReadAll(request.Body)
		writer.Header().Set("content-type", "application/json; charset=utf-8")
		_, _ = writer.Write(handshake)
	}))
	defer server.Close()

	client := fixtureClient(server.URL, server.Client())
	if err := client.httpHandshake(context.Background(), "actors"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodGet || len(body) != 0 {
		t.Fatalf("handshake must be a bodyless GET, got method=%q body=%q", method, body)
	}
	if authorization != "Bearer fixture-token" || accept != "application/json" {
		t.Fatalf("handshake headers lost Rust transport policy: authorization=%q accept=%q", authorization, accept)
	}
}

func TestHTTPRequestFixtureUsesDescriptorRouteMethodAndTypedErrors(t *testing.T) {
	var method string
	var body []byte
	var authorization string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method = request.Method
		authorization = request.Header.Get("authorization")
		body, _ = io.ReadAll(request.Body)
		if request.URL.Path == "/typed-error" {
			writer.WriteHeader(http.StatusConflict)
			_, _ = writer.Write([]byte(`{"code":"conflict"}`))
			return
		}
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := fixtureClient(server.URL, server.Client())
	route := HTTPRoutes["actors"]["acyclic.actors.v1.ActorsService/CreateActor"]
	request := &actorsv1.CreateActorRequest{HomeRegion: "fixture"}
	response := new(protocolv1.HandshakeResponse)
	if err := client.invokeHTTP(context.Background(), "actors", route.Method, route.Path, request, response); err != nil {
		t.Fatal(err)
	}
	wantBody, err := protojson.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if method != route.Method || string(body) != string(wantBody) || authorization != "Bearer fixture-token" {
		t.Fatalf("request did not preserve Rust route contract: method=%q body=%q authorization=%q", method, body, authorization)
	}

	err = client.invokeHTTP(context.Background(), "actors", route.Method, "/typed-error", request, response)
	var typed *RustHTTPError
	if !errors.As(err, &typed) || typed.StatusCode != http.StatusConflict || string(typed.Detail) != `{"code":"conflict"}` {
		t.Fatalf("typed Rust HTTP error lost status or canonical payload: %#v", err)
	}
}

func TestHTTPHandshakeFixtureRejectsRedirectAndWrongContentType(t *testing.T) {
	handshake := fixtureHandshakeJSON(t, "actors")
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write(handshake)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	noRedirect := &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	client := fixtureClient(redirect.URL, noRedirect)
	if err := client.httpHandshake(context.Background(), "actors"); err == nil || !strings.Contains(err.Error(), "redirected") {
		t.Fatalf("redirect was accepted by HTTP handshake: %v", err)
	}

	wrongType := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("content-type", "text/plain")
		_, _ = writer.Write(handshake)
	}))
	defer wrongType.Close()
	client = fixtureClient(wrongType.URL, wrongType.Client())
	if err := client.httpHandshake(context.Background(), "actors"); err == nil || !strings.Contains(err.Error(), "content type") {
		t.Fatalf("wrong handshake content type was accepted: %v", err)
	}
}

func TestHTTPStreamFixtureClosesOnCancellation(t *testing.T) {
	serverClosed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("content-type", "application/x-ndjson")
		_, _ = writer.Write([]byte("{}\n"))
		if flusher, ok := writer.(http.Flusher); ok {
			flusher.Flush()
		}
		<-request.Context().Done()
		close(serverClosed)
	}))
	defer server.Close()

	route := HTTPRoutes["stream"]["acyclic.stream.v2.StreamService/Follow"]
	client := fixtureClient(server.URL, server.Client())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.httpStream(ctx, "stream", route.Method, route.Path, &protocolv1.HandshakeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	stream.cancel()
	if err := stream.recv(&protocolv1.HandshakeResponse{}); err != io.EOF {
		t.Fatalf("cancelled HTTP stream returned %v instead of EOF", err)
	}
	select {
	case <-serverClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP fixture did not observe stream cancellation")
	}
}
