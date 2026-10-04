package main

import (
  "context"
  "crypto/sha256"
  "crypto/tls"
  "crypto/x509"
  "encoding/json"
  "encoding/hex"
  "fmt"
  "os"
  "path/filepath"
  "strings"
  "time"

  i "github.com/acyclic-labs/sdk/go/gen/inference/v1"
  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials"
  "google.golang.org/protobuf/proto"
)

type meta struct { Endpoint, CaCertificate, Certificate, PrivateKey string }
type result struct { Schema string `json:"schema"`; SourceRevision string `json:"source_revision"`; Status string `json:"status"`; Invoked bool `json:"invoked"`; ExitCode int `json:"exit_code"`; Family string `json:"family"`; RPC string `json:"rpc"`; Shape string `json:"shape"`; Transport string `json:"transport"`; Checks []string `json:"checks"`; ResponseBytes int `json:"response_bytes"`; ResponseSHA256 string `json:"response_sha256"` }
type scenario struct { OutputPath string `json:"output_path"`; OutputSHA256 string `json:"output_sha256"` }

func main() {
  var m meta; b, _ := os.ReadFile(os.Getenv("FIXTURE_META")); if err := json.Unmarshal(b, &m); err != nil { panic(err) }
  ca := x509.NewCertPool(); if !ca.AppendCertsFromPEM([]byte(m.CaCertificate)) { panic("bad CA") }
  cert, err := tls.X509KeyPair([]byte(m.Certificate), []byte(m.PrivateKey)); if err != nil { panic(err) }
  creds := credentials.NewTLS(&tls.Config{RootCAs: ca, Certificates: []tls.Certificate{cert}, ServerName: "localhost", MinVersion: tls.VersionTLS12})
  target := strings.TrimPrefix(m.Endpoint, "https://")
  ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second); defer cancel()
  conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(creds), grpc.WithBlock()); if err != nil { panic(err) }; defer conn.Close()
  out := os.Getenv("SCENARIO_OUT"); _ = os.MkdirAll(out, 0755); rev := os.Getenv("SOURCE_REVISION"); var ss []scenario
  put := func(rpc, shape string, response []byte) { obj := result{"acyclic.sdk.rpc-scenario-result.v1", rev, "passed", true, 0, "inference", rpc, shape, "grpc+mtls", []string{"invocation", "transport", "serialization", "tls-hostname-verification"}, len(response), "sha256:"+hash(response)}; raw, _ := json.MarshalIndent(obj, "", "  "); raw = append(raw, '\n'); name := fmt.Sprintf("go-inference-%02d.json", len(ss)+1); p := filepath.Join(out, name); _ = os.WriteFile(p, raw, 0644); ss = append(ss, scenario{"qualification/consumers/"+name, "sha256:"+hash(raw)}) }
  actor := i.NewModelsServiceClient(conn); models, err := actor.List(ctx, &i.ListModelsRequest{}); if err != nil { panic(err) }; put("inference.customer.v1.ModelsService/List", "unary", mustMarshal(models))
  identity := &i.RequestIdentity{ClientInstance: bytes16(1), RequestId: bytes16(2)}; contexts := i.NewContextsServiceClient(conn)
  created, err := contexts.Create(ctx, &i.CreateContextRequest{Identity: identity, Model: "fixture-model"}); if err != nil { panic(err) }; put("inference.customer.v1.ContextsService/Create", "unary", mustMarshal(created))
  inspected, err := contexts.Inspect(ctx, &i.InspectContextRequest{Revision: created.Revision}); if err != nil { panic(err) }; put("inference.customer.v1.ContextsService/Inspect", "unary", mustMarshal(inspected))
  mutated, err := contexts.Mutate(ctx, &i.MutateContextRequest{Identity: identity, Source: created.Revision, Action: &i.MutateContextRequest_Fork{Fork: &i.Empty{}}}); if err != nil { panic(err) }; put("inference.customer.v1.ContextsService/Mutate", "unary", mustMarshal(mutated))
  warm := i.NewWarmContextsServiceClient(conn); retained, err := warm.Retain(ctx, &i.RetainWarmRequest{Identity: identity, Context: created.Revision, LatencyProfile: bytes32(3), ExpiresAtMs: 4102444800000}); if err != nil { panic(err) }; put("inference.customer.v1.WarmContextsService/Retain", "unary", mustMarshal(retained))
  winsp, err := warm.Inspect(ctx, &i.InspectWarmRequest{Commitment: retained.Commitment}); if err != nil { panic(err) }; put("inference.customer.v1.WarmContextsService/Inspect", "unary", mustMarshal(winsp))
  renewed, err := warm.Renew(ctx, &i.RenewWarmRequest{Identity: identity, Commitment: retained.Commitment, ExpiresAtMs: 4102444800000}); if err != nil { panic(err) }; put("inference.customer.v1.WarmContextsService/Renew", "unary", mustMarshal(renewed))
  released, err := warm.Release(ctx, &i.ReleaseWarmRequest{Identity: identity, Commitment: retained.Commitment}); if err != nil { panic(err) }; put("inference.customer.v1.WarmContextsService/Release", "unary", mustMarshal(released))
  runs := i.NewRunsServiceClient(conn); generated, err := runs.Generate(ctx, &i.GenerateRunRequest{Identity: identity, Context: created.Revision, Input: &i.Item{Kind: i.ItemKind_ITEM_KIND_USER, Payload: []byte("hello")}, MaximumOutput: 8}); if err != nil { panic(err) }; put("inference.customer.v1.RunsService/Generate", "unary", mustMarshal(generated))
  inspectedRun, err := runs.Inspect(ctx, &i.InspectRunRequest{RunId: generated.Run.RunId}); if err != nil { panic(err) }; put("inference.customer.v1.RunsService/Inspect", "unary", mustMarshal(inspectedRun))
  watchCtx, stop := context.WithCancel(ctx); stream, err := runs.Watch(watchCtx, &i.WatchRunRequest{RunId: generated.Run.RunId, FromSequence: 0}); if err != nil { panic(err) }; ev, err := stream.Recv(); if err != nil { panic(err) }; stop(); put("inference.customer.v1.RunsService/Watch", "server", mustMarshal(ev))
  cancelled, err := runs.Cancel(ctx, &i.InspectRunRequest{RunId: generated.Run.RunId}); if err != nil { panic(err) }; put("inference.customer.v1.RunsService/Cancel", "unary", mustMarshal(cancelled))
  evals := i.NewEvaluationsServiceClient(conn); spec := &i.EvaluationSpec{Candidates: []*i.EvaluationArtifact{{Digest: bytes32(4), MediaType: "text/plain", LogicalSize: 1}}, Suite: &i.EvaluationSuite{Identity: "fixture-suite", Digest: bytes32(5), Cases: []*i.EvaluationCase{{CaseId: bytes16(6), Input: []byte("x")}}}, Grader: &i.EvaluationGrader{Handle: []byte("fixture"), ArtifactDigest: bytes32(7)}, Metrics: []*i.EvaluationMetric{{Identity: "quality", Aggregation: i.EvaluationAggregation_EVALUATION_AGGREGATION_MEAN}}, MaximumCaseResults: 1, SpecDigest: bytes32(8)}
  evaluation, err := evals.Create(ctx, &i.CreateEvaluationRequest{Identity: identity, Spec: spec}); if err != nil { panic(err) }; put("inference.customer.v1.EvaluationsService/Create", "unary", mustMarshal(evaluation)); evaluationView, err := evals.Inspect(ctx, &i.InspectEvaluationRequest{EvaluationId: evaluation.EvaluationId}); if err != nil { panic(err) }; put("inference.customer.v1.EvaluationsService/Inspect", "unary", mustMarshal(evaluationView))
  log := map[string]any{"schema":"acyclic.sdk.rpc-scenario-log.v1", "source_revision":rev, "consumer":map[string]any{"name":"acyclic-go-installed-inference-consumer", "version":"v0.0.1", "artifact_path":os.Getenv("CONSUMER_ARTIFACT"), "artifact_sha256":os.Getenv("CONSUMER_SHA256")}, "scenarios":ss}; raw,_:=json.MarshalIndent(log,"","  "); _=os.WriteFile(filepath.Join(out,"scenario-log.json"),append(raw,'\n'),0644); fmt.Printf("wrote %d actual Go Inference scenario results\n",len(ss))
}
func hash(b []byte) string { h:=sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func bytes16(v byte) []byte { b:=make([]byte,16); for j:=range b { b[j]=v }; return b }
func bytes32(v byte) []byte { b:=make([]byte,32); for j:=range b { b[j]=v }; return b }
func mustMarshal(m proto.Message) []byte { b,e:=proto.Marshal(m); if e!=nil {panic(e)}; return b }



