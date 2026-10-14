import acyclic.actors.v1.Actors;
import acyclic.actors.v1.ActorsServiceGrpc;
import acyclic.stream.v1.Stream;
import acyclic.stream.v1.StreamServiceGrpc;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoServiceDescriptorSupplier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class InstalledConsumer {
  static void require(boolean ok, String message) {
    if (!ok) throw new AssertionError(message);
  }

  static FileDescriptorProto api(FileDescriptorProto value) {
    return value.toBuilder().clearSourceCodeInfo()
      .setUnknownFields(value.getUnknownFields().toBuilder().clearField(8042).build()).build();
  }

  static void descriptor(FileDescriptor installed, String path) throws Exception {
    FileDescriptorSet set = FileDescriptorSet.parseFrom(Files.readAllBytes(Path.of(path)));
    List<FileDescriptorProto> matching = set.getFileList().stream()
      .filter(file -> file.getName().equals(installed.getName())).toList();
    require(matching.size() == 1, "missing or duplicate Rust descriptor");
    require(api(matching.get(0)).equals(api(installed.toProto())), "descriptor changed: " + installed.getName());
  }

  public static void main(String[] args) throws Exception {
    require(args.length == 2, "pass verified Actors/Stream descriptor paths");
    Path installedJar = Path.of(System.getProperty("sdk.qualified.jar"));
    for (Class<?> type : List.of(Actors.class, ActorsServiceGrpc.class, Actors.CreateActorRequest.class,
      Stream.class, StreamServiceGrpc.class, Stream.AppendRequest.class, Stream.ReadRequest.class)) {
      Path origin = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
      require(Files.isSameFile(installedJar, origin), "class did not load from installed JAR: " + type.getName());
    }
    descriptor(Actors.getDescriptor(), args[0]);
    descriptor(Stream.getDescriptor(), args[1]);
    ByteString bytes = ByteString.copyFrom(new byte[] {0, (byte) 255});
    var actor = Actors.CreateActorRequest.newBuilder().setCodeSha256(bytes)
      .setHomeRegion("test").setIdempotencyKey("test").build();
    require(actor.equals(Actors.CreateActorRequest.parseFrom(actor.toByteArray())), "Actors wire");
    var append = Stream.AppendRequest.newBuilder().setPath("test/path").addRecords(bytes)
      .addRecords(ByteString.EMPTY).setIfTail(-1L).setIdempotencyKey(bytes).build();
    require(append.equals(Stream.AppendRequest.parseFrom(append.toByteArray())), "Stream wire uint64 max");
    var read = Stream.ReadRequest.newBuilder().setPath("test/path").setFrom(-1L).setLimit(-1).build();
    require(read.equals(Stream.ReadRequest.parseFrom(read.toByteArray())), "Stream uint64/uint32 bounds");
    var absent = Stream.AppendRequest.newBuilder().build();
    var explicit = Stream.AppendRequest.newBuilder().setIfTail(0).build();
    require(!absent.hasIfTail() && explicit.hasIfTail(), "optional presence");
    require(absent.toByteArray().length == 0 && explicit.toByteArray().length != 0, "optional zero encoding");
    for (var service : List.of(ActorsServiceGrpc.getServiceDescriptor(), StreamServiceGrpc.getServiceDescriptor())) {
      var schema = ((ProtoServiceDescriptorSupplier) service.getSchemaDescriptor()).getServiceDescriptor();
      require(service.getName().equals(schema.getFullName()), "gRPC service name");
      require(service.getMethods().size() == schema.getMethods().size(), "gRPC method count");
      for (MethodDescriptor<?, ?> method : service.getMethods()) {
        var proto = schema.findMethodByName(method.getBareMethodName());
        require(proto != null, "gRPC method missing in Rust schema");
        var expected = proto.isClientStreaming()
          ? (proto.isServerStreaming() ? MethodDescriptor.MethodType.BIDI_STREAMING : MethodDescriptor.MethodType.CLIENT_STREAMING)
          : (proto.isServerStreaming() ? MethodDescriptor.MethodType.SERVER_STREAMING : MethodDescriptor.MethodType.UNARY);
        require(method.getType() == expected, "gRPC shape changed: " + method.getFullMethodName());
      }
    }
    System.out.println("PASS: installed Java descriptors, bytes, unsigned bounds, optional presence, oneof and gRPC shapes");
  }
}
