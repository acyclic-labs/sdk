package consumer

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	actors "github.com/acyclic-labs/sdk/go/gen/actors/v1"
	stream "github.com/acyclic-labs/sdk/go/gen/stream/v2"
	workers "github.com/acyclic-labs/sdk/go/gen/workers/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestRustDescriptors(t *testing.T) {
	cases := []struct {
		name string
		file protoreflect.FileDescriptor
	}{
		{"actors/v1/actors.proto", actors.File_actors_v1_actors_proto},
		{"workers/v1/workers.proto", workers.File_workers_v1_workers_proto},
		{"stream/v2/stream.proto", stream.File_stream_v2_stream_proto},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := os.ReadFile(filepath.Join(os.Getenv("SDK_AUTHORITY_DIR"), filepath.FromSlash(c.name+".descriptor.bin")))
			if err != nil {
				t.Fatal(err)
			}
			var set descriptorpb.FileDescriptorSet
			if err := proto.Unmarshal(b, &set); err != nil {
				t.Fatal(err)
			}
			var want *descriptorpb.FileDescriptorProto
			for _, f := range set.File {
				if f.GetName() == c.name {
					want = proto.Clone(f).(*descriptorpb.FileDescriptorProto)
				}
			}
			if want == nil {
				t.Fatal("canonical descriptor missing")
			}
			want.SourceCodeInfo = nil
			filterBufImageMetadata(t, want)
			got := protodesc.ToFileDescriptorProto(c.file)
			got.SourceCodeInfo = nil
			if !proto.Equal(want, got) {
				t.Fatal("generated descriptor differs from Rust export")
			}
		})
	}
}

func TestInstalledWireRoundTrips(t *testing.T) {
	tail := uint64(math.MaxUint64)
	cases := []proto.Message{
		&actors.CreateActorRequest{CodeSha256: make([]byte, 32), HomeRegion: "test", IdempotencyKey: "test"},
		&workers.PublishVersionRequest{JavascriptModule: []byte{0, 255}, ExpectedSha256: make([]byte, 32), IdempotencyKey: "test"},
		&stream.AppendRequest{Path: "test/path", Records: [][]byte{{0, 255}, {}}, IfTail: &tail, IdempotencyKey: []byte{0, 255}},
		&stream.ReadRequest{Path: "test/path", From: math.MaxUint64, Limit: math.MaxUint32},
	}
	for _, original := range cases {
		b, err := proto.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		recovered := original.ProtoReflect().Type().New().Interface()
		if err := proto.Unmarshal(b, recovered); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(original, recovered) {
			t.Fatalf("wire changed %T", original)
		}
	}
	absent := &stream.AppendRequest{}
	explicit := &stream.AppendRequest{IfTail: proto.Uint64(0)}
	absentBytes, _ := proto.Marshal(absent)
	explicitBytes, _ := proto.Marshal(explicit)
	if len(absentBytes) != 0 || len(explicitBytes) == 0 {
		t.Fatal("optional zero presence lost")
	}
}

// Buf's image wrapper adds file-level tag 8042, not a protobuf API field.
// Remove only that known metadata field; retain every other unknown field.
// https://github.com/bufbuild/buf/blob/main/proto/buf/alpha/image/v1/image.proto
func filterBufImageMetadata(t *testing.T, file *descriptorpb.FileDescriptorProto) {
	t.Helper()
	unknown := file.ProtoReflect().GetUnknown()
	var retained []byte
	for len(unknown) > 0 {
		number, kind, n := protowire.ConsumeTag(unknown)
		if n < 0 {
			t.Fatal("malformed descriptor tag")
		}
		valueLength := protowire.ConsumeFieldValue(number, kind, unknown[n:])
		if valueLength < 0 {
			t.Fatal("malformed descriptor field")
		}
		end := n + valueLength
		if number == 8042 && kind != protowire.BytesType {
			t.Fatal("unexpected Buf metadata encoding")
		}
		if number != 8042 {
			retained = append(retained, unknown[:end]...)
		}
		unknown = unknown[end:]
	}
	file.ProtoReflect().SetUnknown(retained)
}
