using Google.Protobuf;
using Google.Protobuf.Reflection;
using Grpc.Core;
using System.Reflection;
using Actors = Acyclic.Actors.V1;
using Workers = Acyclic.Workers.V1;
using Stream = Acyclic.Stream.V1;

static class Program {
  static void Require(bool condition, string detail) {
    if (!condition) throw new Exception(detail);
  }

  static FileDescriptorProto Api(byte[] raw) {
    using var input = new CodedInputStream(raw);
    using var output = new MemoryStream();
    while (!input.IsAtEnd) {
      int start = checked((int)input.Position);
      uint tag = input.ReadTag();
      Require(tag != 0, "invalid descriptor tag");
      input.SkipLastField();
      int end = checked((int)input.Position);
      if (WireFormat.GetTagFieldNumber(tag) == 8042) {
        Require(WireFormat.GetTagWireType(tag) == WireFormat.WireType.LengthDelimited, "invalid Buf image encoding");
      } else output.Write(raw, start, end - start);
    }
    var file = FileDescriptorProto.Parser.ParseFrom(output.ToArray());
    file.SourceCodeInfo = null;
    return file;
  }

  static void Descriptor(FileDescriptor installed, string path) {
    var set = FileDescriptorSet.Parser.ParseFrom(File.ReadAllBytes(path));
    var matches = set.File.Where(file => file.Name == installed.Name).ToArray();
    Require(matches.Length == 1, "missing or duplicate canonical descriptor");
    Require(Api(matches[0].ToByteArray()).Equals(Api(installed.SerializedData.ToByteArray())), "Rust descriptor differs: " + installed.Name);
  }

  static void Roundtrip(IMessage value) {
    var recovered = value.Descriptor.Parser.ParseFrom(value.ToByteArray());
    Require(value.Equals(recovered), "wire changed: " + value.Descriptor.FullName);
  }

  static void Service(Type generated, ServiceDescriptor canonical) {
    var fields = generated.GetFields(BindingFlags.NonPublic | BindingFlags.Static)
      .Where(field => field.Name.StartsWith("__Method_")).ToArray();
    Require(fields.Length == canonical.Methods.Count, "gRPC method count differs");
    foreach (var rpc in canonical.Methods) {
      var field = fields.Single(item => item.Name == "__Method_" + rpc.Name);
      var method = field.GetValue(null)!;
      var actual = (MethodType)field.FieldType.GetProperty("Type")!.GetValue(method)!;
      var expected = rpc.IsClientStreaming
        ? (rpc.IsServerStreaming ? MethodType.DuplexStreaming : MethodType.ClientStreaming)
        : (rpc.IsServerStreaming ? MethodType.ServerStreaming : MethodType.Unary);
      Require(actual == expected, "gRPC shape differs: " + rpc.FullName);
      Require((string)field.FieldType.GetProperty("FullName")!.GetValue(method)! == "/" + canonical.FullName + "/" + rpc.Name, "gRPC method name differs");
    }
  }

  static void Main(string[] args) {
    Require(args.Length == 4, "pass verified Actors/Workers/Stream descriptor paths");
    foreach (var type in new[] { typeof(Actors.ActorsReflection), typeof(Workers.WorkersReflection), typeof(Stream.StreamReflection), typeof(Actors.ActorsService), typeof(Workers.WorkersService), typeof(Stream.StreamService) }) Require(Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(File.ReadAllBytes(type.Assembly.Location))).Equals(args[3], StringComparison.OrdinalIgnoreCase), "installed assembly digest differs");
    Descriptor(Actors.ActorsReflection.Descriptor, args[0]);
    Descriptor(Workers.WorkersReflection.Descriptor, args[1]);
    Descriptor(Stream.StreamReflection.Descriptor, args[2]);
    var bytes = ByteString.CopyFrom(new byte[] {0, 255});
    Roundtrip(new Actors.CreateActorRequest { CodeSha256 = bytes, HomeRegion = "test", IdempotencyKey = "test" });
    Roundtrip(new Workers.PublishVersionRequest { JavascriptModule = bytes, ExpectedSha256 = bytes, IdempotencyKey = "test" });
    var append = new Stream.AppendRequest { Path = "test/path", IfTail = ulong.MaxValue, IdempotencyKey = bytes };
    append.Records.Add(bytes); append.Records.Add(ByteString.Empty);
    Roundtrip(append);
    Roundtrip(new Stream.ReadRequest { Path = "test/path", From = ulong.MaxValue, Limit = uint.MaxValue });
    var target = new Workers.JobTarget { VersionSha256 = bytes };
    Require(target.TargetCase == Workers.JobTarget.TargetOneofCase.VersionSha256 && target.DeploymentAlias == "", "version branch presence");
    var versionTarget = Workers.JobTarget.Parser.ParseFrom(target.ToByteArray());
    Require(versionTarget.TargetCase == Workers.JobTarget.TargetOneofCase.VersionSha256 && versionTarget.VersionSha256.Equals(bytes), "version branch wire");
    target.DeploymentAlias = "test-alias";
    Require(target.TargetCase == Workers.JobTarget.TargetOneofCase.DeploymentAlias && target.VersionSha256.IsEmpty, "alias must clear version branch");
    var aliasTarget = Workers.JobTarget.Parser.ParseFrom(target.ToByteArray());
    Require(aliasTarget.TargetCase == Workers.JobTarget.TargetOneofCase.DeploymentAlias && aliasTarget.DeploymentAlias == "test-alias" && aliasTarget.VersionSha256.IsEmpty, "alias branch wire");
    target.VersionSha256 = bytes;
    Require(target.TargetCase == Workers.JobTarget.TargetOneofCase.VersionSha256 && target.DeploymentAlias == "", "version must clear alias branch");
    Roundtrip(target);
    target.ClearTarget();
    Require(target.TargetCase == Workers.JobTarget.TargetOneofCase.None && target.DeploymentAlias == "" && target.VersionSha256.IsEmpty, "clear oneof target");
    var absent = new Stream.AppendRequest();
    var explicitZero = new Stream.AppendRequest { IfTail = 0 };
    Require(!absent.HasIfTail && explicitZero.HasIfTail, "optional presence lost");
    Require(absent.ToByteArray().Length == 0 && explicitZero.ToByteArray().Length > 0, "optional zero wire lost");
    Service(typeof(Actors.ActorsService), Actors.ActorsService.Descriptor);
    Service(typeof(Workers.WorkersService), Workers.WorkersService.Descriptor);
    Service(typeof(Stream.StreamService), Stream.StreamService.Descriptor);
    Console.WriteLine("PASS: installed .NET descriptors, bytes, unsigned bounds, optional presence, oneof and gRPC shapes");
  }
}
