package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	i "github.com/acyclic-labs/sdk/go/gen/inference/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type meta struct{ Endpoint, CaCertificate, Certificate, PrivateKey string }
type semantic struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}
type result struct {
	Schema         string          `json:"schema"`
	SourceRevision string          `json:"source_revision"`
	Status         string          `json:"status"`
	Invoked        bool            `json:"invoked"`
	ExecutionMode  string          `json:"execution_mode"`
	ExitCode       int             `json:"exit_code"`
	Family         string          `json:"family"`
	RPC            string          `json:"rpc"`
	Shape          string          `json:"shape"`
	Transport      string          `json:"transport"`
	Checks         []string        `json:"checks"`
	ResponseBytes  int             `json:"response_bytes"`
	ResponseSHA256 string          `json:"response_sha256"`
	DecodedSummary json.RawMessage `json:"decoded_summary"`
	SemanticChecks []semantic      `json:"semantic_checks"`
}
type scenario struct {
	OutputPath   string `json:"output_path"`
	OutputSHA256 string `json:"output_sha256"`
}

func main() {
	var m meta
	b, _ := os.ReadFile(os.Getenv("FIXTURE_META"))
	if err := json.Unmarshal(b, &m); err != nil {
		panic(err)
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM([]byte(m.CaCertificate)) {
		panic("bad CA")
	}
	cert, err := tls.X509KeyPair([]byte(m.Certificate), []byte(m.PrivateKey))
	if err != nil {
		panic(err)
	}
	creds := credentials.NewTLS(&tls.Config{RootCAs: ca, Certificates: []tls.Certificate{cert}, ServerName: "localhost", MinVersion: tls.VersionTLS12})
	target := strings.TrimPrefix(m.Endpoint, "https://")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(creds), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	out := os.Getenv("SCENARIO_OUT")
	_ = os.MkdirAll(out, 0755)
	rev := os.Getenv("SOURCE_REVISION")
	var ss []scenario
	put := func(rpc, shape string, response proto.Message, wire []byte, checks []semantic) {
		summary, _ := protojson.MarshalOptions{UseProtoNames: true}.Marshal(response)
		status := "passed"
		for _, check := range checks {
			if check.Status != "passed" {
				status = "failed"
			}
		}
		obj := result{"acyclic.sdk.rpc-scenario-result.v1", rev, status, true, "remote", 0, "inference", rpc, shape, "grpc+mtls", []string{"invocation", "transport", "serialization", "tls-hostname-verification"}, len(wire), "sha256:" + hash(wire), summary, checks}
		if shape == "server" {
			obj.Checks = append(obj.Checks, "streaming")
		}
		raw, _ := json.MarshalIndent(obj, "", "  ")
		raw = append(raw, '\n')
		name := fmt.Sprintf("go-inference-%02d.json", len(ss)+1)
		p := filepath.Join(out, name)
		_ = os.WriteFile(p, raw, 0644)
		ss = append(ss, scenario{"qualification/consumers/" + name, "sha256:" + hash(raw)})
	}
	actor := i.NewModelsServiceClient(conn)
	models, err := actor.List(ctx, &i.ListModelsRequest{})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.ModelsService/List", "unary", models, mustMarshal(models), inferenceChecks("models", models))
	identity := &i.RequestIdentity{ClientInstance: bytes16(1), RequestId: bytes16(2)}
	contexts := i.NewContextsServiceClient(conn)
	created, err := contexts.Create(ctx, &i.CreateContextRequest{Identity: identity, Model: "fixture-model"})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.ContextsService/Create", "unary", created, mustMarshal(created), inferenceChecks("create-context", created))
	inspected, err := contexts.Inspect(ctx, &i.InspectContextRequest{Revision: created.Revision})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.ContextsService/Inspect", "unary", inspected, mustMarshal(inspected), inferenceChecks("inspect-context", inspected))
	mutated, err := contexts.Mutate(ctx, &i.MutateContextRequest{Identity: identity, Source: created.Revision, Action: &i.MutateContextRequest_Fork{Fork: &i.Empty{}}})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.ContextsService/Mutate", "unary", mutated, mustMarshal(mutated), inferenceChecks("mutate-context", mutated))
	warm := i.NewWarmContextsServiceClient(conn)
	retained, err := warm.Retain(ctx, &i.RetainWarmRequest{Identity: identity, Context: created.Revision, LatencyProfile: bytes32(3), ExpiresAtMs: 4102444800000})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.WarmContextsService/Retain", "unary", retained, mustMarshal(retained), inferenceChecks("warm-active", retained))
	winsp, err := warm.Inspect(ctx, &i.InspectWarmRequest{Commitment: retained.Commitment})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.WarmContextsService/Inspect", "unary", winsp, mustMarshal(winsp), inferenceChecks("warm-active", winsp))
	renewed, err := warm.Renew(ctx, &i.RenewWarmRequest{Identity: identity, Commitment: retained.Commitment, ExpiresAtMs: 4102444800000})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.WarmContextsService/Renew", "unary", renewed, mustMarshal(renewed), inferenceChecks("warm-active", renewed))
	released, err := warm.Release(ctx, &i.ReleaseWarmRequest{Identity: identity, Commitment: retained.Commitment})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.WarmContextsService/Release", "unary", released, mustMarshal(released), inferenceChecks("warm-released", released))
	runs := i.NewRunsServiceClient(conn)
	generated, err := runs.Generate(ctx, &i.GenerateRunRequest{Identity: identity, Context: created.Revision, Input: &i.Item{Kind: i.ItemKind_ITEM_KIND_USER, Payload: []byte("hello")}, MaximumOutput: 8})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.RunsService/Generate", "unary", generated, mustMarshal(generated), inferenceChecks("run-active", generated.Run))
	inspectedRun, err := runs.Inspect(ctx, &i.InspectRunRequest{RunId: generated.Run.RunId})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.RunsService/Inspect", "unary", inspectedRun, mustMarshal(inspectedRun), inferenceChecks("run-active", inspectedRun))
	stream, err := runs.Watch(ctx, &i.WatchRunRequest{RunId: generated.Run.RunId, FromSequence: 0})
	if err != nil {
		panic(err)
	}
	var events []*i.RunEvent
	for {
		ev, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			panic(recvErr)
		}
		events = append(events, ev)
	}
	wire := []byte{}
	for _, ev := range events {
		wire = append(wire, mustMarshal(ev)...)
	}
	var watchSummary proto.Message = &i.RunEvent{}
	if len(events) > 0 {
		watchSummary = events[0]
	}
	put("inference.customer.v1.RunsService/Watch", "server", watchSummary, wire, []semantic{{"watch-count", boolStatus(len(events) == 2), "Rust fixture progress and terminal events"}, {"watch-progress", boolStatus(len(events) > 0 && events[0].Sequence == 0 && events[0].GetProgress().GetKind() == "queued"), "Rust fixture queued progress"}, {"watch-terminal", boolStatus(len(events) > 1 && events[1].Sequence == 1 && events[1].GetTerminal() == i.RunTerminal_RUN_TERMINAL_COMPLETED), "Rust fixture completed terminal"}})
	cancelled, err := runs.Cancel(ctx, &i.InspectRunRequest{RunId: generated.Run.RunId})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.RunsService/Cancel", "unary", cancelled, mustMarshal(cancelled), inferenceChecks("run-cancelled", cancelled))
	evals := i.NewEvaluationsServiceClient(conn)
	spec := &i.EvaluationSpec{Candidates: []*i.EvaluationArtifact{{Digest: bytes32(4), MediaType: "text/plain", LogicalSize: 1}}, Suite: &i.EvaluationSuite{Identity: "fixture-suite", Digest: bytes32(5), Cases: []*i.EvaluationCase{{CaseId: bytes16(6), Input: []byte("x")}}}, Grader: &i.EvaluationGrader{Handle: []byte("fixture"), ArtifactDigest: bytes32(7)}, Metrics: []*i.EvaluationMetric{{Identity: "quality", Aggregation: i.EvaluationAggregation_EVALUATION_AGGREGATION_MEAN}}, MaximumCaseResults: 1, SpecDigest: bytes32(8)}
	evaluation, err := evals.Create(ctx, &i.CreateEvaluationRequest{Identity: identity, Spec: spec})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.EvaluationsService/Create", "unary", evaluation, mustMarshal(evaluation), inferenceChecks("evaluation", evaluation))
	evaluationView, err := evals.Inspect(ctx, &i.InspectEvaluationRequest{EvaluationId: evaluation.EvaluationId})
	if err != nil {
		panic(err)
	}
	put("inference.customer.v1.EvaluationsService/Inspect", "unary", evaluationView, mustMarshal(evaluationView), inferenceChecks("evaluation", evaluationView))
	log := map[string]any{"schema": "acyclic.sdk.rpc-scenario-log.v1", "source_revision": rev, "consumer": map[string]any{"name": "acyclic-go-installed-inference-consumer", "version": "v0.0.1", "artifact_path": os.Getenv("CONSUMER_ARTIFACT"), "artifact_sha256": os.Getenv("CONSUMER_SHA256")}, "scenarios": ss}
	raw, _ := json.MarshalIndent(log, "", "  ")
	_ = os.WriteFile(filepath.Join(out, "scenario-log.json"), append(raw, '\n'), 0644)
	fmt.Printf("wrote %d actual Go Inference scenario results\n", len(ss))
}
func hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func boolStatus(ok bool) string {
	if ok {
		return "passed"
	}
	return "failed"
}
func check(name string, ok bool, detail string) semantic {
	return semantic{name, boolStatus(ok), detail}
}
func inferenceChecks(kind string, value proto.Message) []semantic {
	switch v := value.(type) {
	case *i.ListModelsResponse:
		var model *i.ModelCapability
		if len(v.Models) == 1 {
			model = v.Models[0]
		}
		return []semantic{check("fixture-model", model != nil && model.Model == "fixture-model", "Rust fixture model identity"), check("model-limits", model != nil && model.MaximumContext == 4096 && model.MaximumOutput == 1024, "Rust fixture limits"), check("model-features", model != nil && len(model.Features) == 2 && model.Features[0] == "generate" && model.Features[1] == "stream", "Rust fixture features")}
	case *i.MutationReceipt:
		return []semantic{check("receipt", (kind == "create-context" && string(v.Revision) == string(bytes32(1)) && v.Sequence == 1 && v.Retained) || (kind == "mutate-context" && string(v.Revision) == string(bytes32(6)) && v.Sequence == 2 && !v.Retained), "Rust fixture mutation receipt")}
	case *i.ContextView:
		return []semantic{check("context", v.Model == "fixture-model" && string(v.Lineage) == string(bytes32(3)) && string(v.ExecutionProfile) == string(bytes32(4)) && string(v.ContentDigest) == string(bytes32(5)), "Rust fixture context view"), check("provenance", v.Provenance != nil && v.Provenance.GetCreated() != nil, "Rust fixture created provenance")}
	case *i.WarmView:
		return []semantic{check("warm-identity", string(v.Commitment) == string(bytes32(8)) && string(v.Context) == string(bytes32(1)), "Rust fixture warm identity"), check("warm-state", (kind == "warm-released" && v.State == i.WarmState_WARM_STATE_RELEASED || kind != "warm-released" && v.State == i.WarmState_WARM_STATE_ACTIVE) && v.Sequence == 1, "Rust fixture warm state")}
	case *i.GenerateRunResponse:
		return []semantic{check("run", v.Run != nil && string(v.Run.RunId) == string(bytes16(2)) && v.Run.Model == "model.example.v1" && v.Run.LastSequence == 0 && !v.Run.CancellationRequested, "Rust fixture active run")}
	case *i.RunView:
		return []semantic{check("run", string(v.RunId) == string(bytes16(2)) && v.Model == "model.example.v1", "Rust fixture run view"), check("cancel", kind != "run-cancelled" || v.LastSequence == 1 && v.CancellationRequested && v.Result != nil && v.Result.Terminal == i.RunTerminal_RUN_TERMINAL_CANCELLED, "Rust fixture cancellation")}
	case *i.EvaluationView:
		return []semantic{check("evaluation", string(v.EvaluationId) == string(bytes16(13)) && v.State == i.EvaluationState_EVALUATION_STATE_COMPLETED && v.Sequence == 1, "Rust fixture evaluation")}
	}
	return nil
}
func bytes16(v byte) []byte {
	b := make([]byte, 16)
	for j := range b {
		b[j] = v
	}
	return b
}
func bytes32(v byte) []byte {
	b := make([]byte, 32)
	for j := range b {
		b[j] = v
	}
	return b
}
func mustMarshal(m proto.Message) []byte {
	b, e := proto.Marshal(m)
	if e != nil {
		panic(e)
	}
	return b
}
