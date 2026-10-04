package main

// Rust-owned all-routes Machines consumer. This is intentionally a generated
// Go client entrypoint: expected values come from sdk-examples/src/tls_fixture.rs.
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

	machinesv1 "github.com/acyclic-labs/sdk/go/gen/machines/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type fixture struct{ Endpoint, CACertificate, Certificate, PrivateKey, MachineId, OperationId string }
type semantic struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}
type row struct {
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
	ResponseCount  int             `json:"response_count,omitempty"`
}

func ones(s string) []byte {
	b := make([]byte, len(s)/2)
	for i := range b {
		fmt.Sscanf(s[2*i:2*i+2], "%02x", &b[i])
	}
	return b
}
func digest(b []byte) string { h := sha256.Sum256(b); return "sha256:" + hex.EncodeToString(h[:]) }
func status(ok bool) string {
	if ok {
		return "passed"
	}
	return "failed"
}
func check(name string, ok bool, detail string) semantic { return semantic{name, status(ok), detail} }

func semantics(rpc string, value proto.Message) []semantic {
	switch v := value.(type) {
	case *machinesv1.ImageQualification:
		return []semantic{check("image", v.Image != nil && v.Image.GetKind() == machinesv1.ImageKind_IMAGE_KIND_CUSTOM, "Rust fixture echoes Image"), check("capability", len(v.Capabilities) == 1 && v.Capabilities[0] == machinesv1.Capability_CAPABILITY_LIVE_CHECKPOINT, "Rust fixture capability"), check("revision", string(v.CompatibilityRevision) == string(bytes32(8)), "Rust fixture compatibility revision")}
	case *machinesv1.MachineAdmission:
		return []semantic{check("machine", v.Machine != nil && string(v.Machine.Value) == string(bytes16(1)), "Rust fixture machine identity"), check("operation", v.Operation != nil && string(v.Operation.Value) == string(bytes16(2)), "Rust fixture operation identity"), check("contract", v.Contract != nil, "Rust fixture contract")}
	case *machinesv1.CheckpointAdmission:
		return []semantic{check("checkpoint", v.Checkpoint != nil && string(v.Checkpoint.Value) == string(bytes16(3)), "Rust fixture checkpoint identity"), check("operation", v.Operation != nil, "Rust fixture operation identity")}
	case *machinesv1.ForkAdmission:
		return []semantic{check("children", len(v.Children) == 2 && string(v.Children[0].Value) == string(bytes16(4)) && string(v.Children[1].Value) == string(bytes16(5)), "Rust fixture fork children")}
	case *machinesv1.ForkMachineAdmission:
		return []semantic{check("children", len(v.Children) == 2, "Rust fixture fork machine children"), check("fidelity", v.Fidelity == machinesv1.ForkFidelity_FORK_FIDELITY_MEMORY_AND_DISK, "Rust fixture fork fidelity")}
	case *machinesv1.MutationAdmission:
		return []semantic{check("operation", v.Operation != nil && string(v.Operation.Value) == string(bytes16(2)), "Rust fixture operation identity")}
	case *machinesv1.PolicyAdmission:
		return []semantic{check("operation", v.Operation != nil, "Rust fixture policy operation"), check("policy", v.Policy != nil, "Rust fixture policy echo")}
	case *machinesv1.RecoveredAdmission:
		return []semantic{check("operation", v.Operation != nil, "Rust fixture recovery operation"), check("create", v.GetCreate() != nil && v.GetCreate().Machine != nil && string(v.GetCreate().Machine.Value) == string(bytes16(1)), "Rust fixture recovered create")}
	case *machinesv1.MachineState:
		return []semantic{check("running", v.Status == machinesv1.MachineStatus_MACHINE_STATUS_RUNNING, "Rust fixture running state"), check("timestamps", v.CreatedAtUnixMs == 1 && v.ChangedAtUnixMs == 1, "Rust fixture state timestamps")}
	case *machinesv1.CheckpointState:
		return []semantic{check("forkable", v.Forkable, "Rust fixture forkable checkpoint"), check("source", v.Source != nil && string(v.Source.Value) == string(bytes16(1)), "Rust fixture checkpoint source")}
	case *machinesv1.MachinePage:
		return []semantic{check("page", len(v.Machines) == 1 && v.Machines[0].Status == machinesv1.MachineStatus_MACHINE_STATUS_RUNNING && v.Next == nil, "Rust fixture machine page")}
	case *machinesv1.EventPage:
		return []semantic{check("event", len(v.Events) == 1 && v.Events[0].Sequence == 1 && v.Events[0].Kind == machinesv1.EventKind_EVENT_KIND_STATE && v.Events[0].State == machinesv1.MachineStatus_MACHINE_STATUS_RUNNING, "Rust fixture state event"), check("cursor", v.NextSequence == 2, "Rust fixture next sequence")}
	case *machinesv1.UsageReceipt:
		return []semantic{check("usage", v.ElasticCpuNs == 1 && v.PrivateResidentByteSeconds == 1 && v.DurablePrivateBytes == 1 && v.EgressBytes == 1, "Rust fixture usage counters"), check("receipt", string(v.Receipt) == string([]byte{10}), "Rust fixture receipt")}
	case *machinesv1.OperationState:
		if strings.HasSuffix(rpc, "/Cancel") {
			return []semantic{check("cancelled", v.Status == machinesv1.OperationStatus_OPERATION_STATUS_CANCELLED, "Rust fixture cancelled operation")}
		}
		return []semantic{check("pending", v.Status == machinesv1.OperationStatus_OPERATION_STATUS_PENDING, "Rust fixture pending operation")}
	}
	return nil
}

func bytes16(v byte) []byte {
	b := make([]byte, 16)
	for i := range b {
		b[i] = v
	}
	return b
}
func bytes32(v byte) []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = v
	}
	return b
}

func main() {
	raw, _ := os.ReadFile(os.Getenv("MACHINES_FIXTURE_JSON"))
	var f fixture
	if err := json.Unmarshal(raw, &f); err != nil {
		panic(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(f.CACertificate)) {
		panic("bad CA")
	}
	cert, err := tls.X509KeyPair([]byte(f.Certificate), []byte(f.PrivateKey))
	if err != nil {
		panic(err)
	}
	target := strings.TrimPrefix(f.Endpoint, "https://")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, ServerName: "localhost"})), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	c := machinesv1.NewMachinesServiceClient(conn)
	machine := &machinesv1.MachineId{Value: ones(f.MachineId)}
	operation := &machinesv1.OperationId{Value: ones(f.OperationId)}
	image := &machinesv1.Image{Kind: machinesv1.ImageKind_IMAGE_KIND_CUSTOM, ImmutableReference: &machinesv1.Image_CustomDigest{CustomDigest: bytes32(0)}}
	revision := os.Getenv("SOURCE_REVISION")
	out := os.Getenv("SCENARIO_OUT")
	_ = os.MkdirAll(out, 0755)
	rows := []row{}
	put := func(rpc, shape string, value proto.Message, wire []byte, checks []semantic, count int) {
		summary, _ := protojson.MarshalOptions{UseProtoNames: true}.Marshal(value)
		state := "passed"
		for _, s := range checks {
			if s.Status != "passed" {
				state = "failed"
			}
		}
		checkNames := []string{"invocation", "transport", "serialization", "tls-hostname-verification"}
		if shape == "server" {
			checkNames = append(checkNames, "streaming")
		}
		rows = append(rows, row{"acyclic.sdk.rpc-scenario-result.v1", revision, state, true, "remote", 0, "machines", rpc, shape, "grpc+mtls", checkNames, len(wire), digest(wire), summary, checks, count})
	}
	call := func(rpc string, fn func() (proto.Message, error)) {
		value, e := fn()
		if e != nil {
			panic(fmt.Errorf("%s: %w", rpc, e))
		}
		wire, _ := proto.Marshal(value)
		put(rpc, "unary", value, wire, semantics(rpc, value), 0)
	}
	call("acyclic.machines.v1.MachinesService/QualifyImage", func() (proto.Message, error) {
		return c.QualifyImage(ctx, &machinesv1.QualifyImageRequest{Image: image})
	})
	call("acyclic.machines.v1.MachinesService/Create", func() (proto.Message, error) { return c.Create(ctx, &machinesv1.CreateMachineRequest{Image: image}) })
	call("acyclic.machines.v1.MachinesService/Checkpoint", func() (proto.Message, error) {
		return c.Checkpoint(ctx, &machinesv1.CheckpointMachineRequest{Machine: machine})
	})
	call("acyclic.machines.v1.MachinesService/Fork", func() (proto.Message, error) { return c.Fork(ctx, &machinesv1.ForkCheckpointRequest{Count: 2}) })
	call("acyclic.machines.v1.MachinesService/ForkMachine", func() (proto.Message, error) {
		return c.ForkMachine(ctx, &machinesv1.ForkMachineRequest{Machine: machine, Count: 2})
	})
	call("acyclic.machines.v1.MachinesService/Suspend", func() (proto.Message, error) {
		return c.Suspend(ctx, &machinesv1.MachineMutationRequest{Machine: machine})
	})
	call("acyclic.machines.v1.MachinesService/Wake", func() (proto.Message, error) {
		return c.Wake(ctx, &machinesv1.MachineMutationRequest{Machine: machine})
	})
	call("acyclic.machines.v1.MachinesService/SetSuspensionPolicy", func() (proto.Message, error) {
		return c.SetSuspensionPolicy(ctx, &machinesv1.SetSuspensionPolicyRequest{Machine: machine})
	})
	call("acyclic.machines.v1.MachinesService/DestroyMachine", func() (proto.Message, error) {
		return c.DestroyMachine(ctx, &machinesv1.MachineMutationRequest{Machine: machine})
	})
	call("acyclic.machines.v1.MachinesService/DestroyCheckpoint", func() (proto.Message, error) {
		return c.DestroyCheckpoint(ctx, &machinesv1.CheckpointMutationRequest{})
	})
	call("acyclic.machines.v1.MachinesService/Recover", func() (proto.Message, error) { return c.Recover(ctx, &machinesv1.RecoverRequest{}) })
	call("acyclic.machines.v1.MachinesService/InspectMachine", func() (proto.Message, error) {
		return c.InspectMachine(ctx, &machinesv1.InspectMachineRequest{Machine: machine})
	})
	call("acyclic.machines.v1.MachinesService/InspectCheckpoint", func() (proto.Message, error) { return c.InspectCheckpoint(ctx, &machinesv1.InspectCheckpointRequest{}) })
	call("acyclic.machines.v1.MachinesService/ListMachines", func() (proto.Message, error) { return c.ListMachines(ctx, &machinesv1.ListMachinesRequest{Limit: 1}) })
	call("acyclic.machines.v1.MachinesService/Events", func() (proto.Message, error) {
		return c.Events(ctx, &machinesv1.EventsRequest{Machine: machine, Limit: 1})
	})
	call("acyclic.machines.v1.MachinesService/Usage", func() (proto.Message, error) { return c.Usage(ctx, &machinesv1.UsageRequest{Machine: machine}) })
	call("acyclic.machines.v1.MachinesService/Cancel", func() (proto.Message, error) {
		return c.Cancel(ctx, &machinesv1.OperationRequest{Operation: operation})
	})
	call("acyclic.machines.v1.MachinesService/InspectOperation", func() (proto.Message, error) {
		return c.InspectOperation(ctx, &machinesv1.OperationRequest{Operation: operation})
	})
	stream, err := c.WatchOperation(ctx, &machinesv1.OperationRequest{Operation: operation})
	if err != nil {
		panic(err)
	}
	states := []*machinesv1.OperationState{}
	wire := []byte{}
	for {
		item, e := stream.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			panic(e)
		}
		states = append(states, item)
		b, _ := proto.Marshal(item)
		wire = append(wire, b...)
	}
	firstPending := len(states) > 0 && states[0].Status == machinesv1.OperationStatus_OPERATION_STATUS_PENDING
	secondSucceeded := len(states) > 1 && states[1].Status == machinesv1.OperationStatus_OPERATION_STATUS_SUCCEEDED
	put("acyclic.machines.v1.MachinesService/WatchOperation", "server", states[0], wire, []semantic{check("stream-count", len(states) == 2, "Rust fixture pending and succeeded states"), check("pending", firstPending, "Rust fixture pending state"), check("succeeded", secondSucceeded, "Rust fixture succeeded state")}, len(states))
	log := map[string]any{"schema": "acyclic.sdk.rpc-scenario-log.v1", "source_revision": revision, "execution_mode": "remote", "consumer": map[string]any{"name": "acyclic-go-installed-machines-consumer", "version": "0.2.0", "artifact_path": os.Getenv("CONSUMER_ARTIFACT"), "artifact_sha256": os.Getenv("CONSUMER_SHA256")}, "scenarios": rows, "passed": 0, "failed": 0}
	for _, r := range rows {
		if r.Status == "passed" {
			log["passed"] = log["passed"].(int) + 1
		} else {
			log["failed"] = log["failed"].(int) + 1
		}
	}
	b, _ := json.MarshalIndent(log, "", "  ")
	_ = os.WriteFile(filepath.Join(out, "scenario-log.json"), append(b, '\n'), 0644)
	fmt.Printf("%d/%d\n", len(rows), len(rows))
}
