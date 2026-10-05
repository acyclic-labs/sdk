package dev.acyclic.consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import acyclic.actors.v1.ActorsServiceGrpc;
import acyclic.filesystem.v2.FilesystemServiceGrpc;
import acyclic.harness.v2.HarnessServiceGrpc;
import acyclic.machines.v1.MachinesServiceGrpc;
import acyclic.objects.v2.BucketsServiceGrpc;
import acyclic.objects.v2.MultipartServiceGrpc;
import acyclic.objects.v2.ObjectsServiceGrpc;
import acyclic.stream.v2.StreamServiceGrpc;
import acyclic.workers.v1.WorkersServiceGrpc;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import inference.customer.v1.ContextsServiceGrpc;
import inference.customer.v1.EvaluationsServiceGrpc;
import inference.customer.v1.ModelsServiceGrpc;
import inference.customer.v1.RunsServiceGrpc;
import inference.customer.v1.WarmContextsServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Replays canonical Rust plan bytes through generated JVM descriptors and an
 * in-process gRPC transport. Stateful fixture receipts are qualified separately.
 */
class InstalledCanonicalReplayTest {
  private static final String DEFAULT_MANIFEST = "work/canonical-106-manifest.json";
  private static final String AFTER_COMPLETION = "/after-completion";

  @Test
  void installedArtifactReplaysCanonicalPlanWireShape() throws Exception {
    Path manifest = canonicalManifest();
    assertTrue(Files.isRegularFile(manifest), "canonical manifest is missing: " + manifest);
    JsonObject root = JsonParser.parseString(Files.readString(manifest)).getAsJsonObject();
    assertEquals(111, root.get("execution_plan_count").getAsInt());
    assertEquals(106, root.get("record_count").getAsInt());

    Map<String, Deque<ExpectedCall>> expected = new LinkedHashMap<>();
    int completionMarkers = 0;
    JsonArray plan = root.getAsJsonArray("execution_plan");
    for (JsonElement element : plan) {
      JsonObject entry = element.getAsJsonObject();
      String rpc = entry.get("rpc").getAsString();
      if (rpc.endsWith(AFTER_COMPLETION)) { completionMarkers++; continue; }
      expected.computeIfAbsent(rpc, ignored -> new ArrayDeque<>()).add(ExpectedCall.from(entry));
    }
    assertEquals(106, expected.size());
    assertEquals(2, completionMarkers, "canonical completion markers");

    Map<String, MethodDescriptor<?, ?>> methods = methodDescriptors();
    assertEquals(106, methods.size(), "generated JVM descriptor inventory");
    assertEquals(expected.keySet(), methods.keySet(), "Rust plan and installed descriptors differ");

    FailureSink failures = new FailureSink();
    String serverName = "rust-canonical-replay-" + System.nanoTime();
    Server server = InProcessServerBuilder.forName(serverName).directExecutor()
        .addServices(serviceDefinitions(expected, failures)).build().start();
    ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    try {
      for (JsonElement element : plan) {
        String rpc = element.getAsJsonObject().get("rpc").getAsString();
        if (rpc.endsWith(AFTER_COMPLETION)) continue;
        ExpectedCall call = expected.get(rpc).peek();
        assertNotNull(call, "missing expected call for " + rpc);
        replay(methods.get(rpc), call, channel, failures);
        expected.get(rpc).remove();
        assertTrue(failures.isEmpty(), failures.describe());
      }
    } finally {
      channel.shutdownNow(); server.shutdownNow();
      channel.awaitTermination(10, TimeUnit.SECONDS); server.awaitTermination(10, TimeUnit.SECONDS);
    }
    assertTrue(failures.isEmpty(), failures.describe());
    expected.forEach((rpc, calls) -> assertTrue(calls.isEmpty(), "unreplayed canonical RPC: " + rpc));
  }

  @Test
  void installedArtifactExposesRustOwnedTypedSurfaceForEveryRpc() throws Exception {
    Path manifest = canonicalManifest();
    JsonObject root = JsonParser.parseString(Files.readString(manifest)).getAsJsonObject();
    Set<String> rpcs = new HashSet<>();
    for (JsonElement element : root.getAsJsonArray("execution_plan")) {
      String rpc = element.getAsJsonObject().get("rpc").getAsString();
      if (rpc.endsWith(AFTER_COMPLETION)) continue;
      rpcs.add(rpc);
    }
    assertEquals(106, rpcs.size());
    Class<?> requests = Class.forName("dev.acyclic.transport.RustTypedRequests");
    Class<?> responses = Class.forName("dev.acyclic.transport.RustTypedResponses");
    Class<?> clients = Class.forName("dev.acyclic.transport.RustTypedClients");
    for (String rpc : rpcs) {
      String[] path = rpc.split("/");
      String service = path[0].substring(path[0].lastIndexOf('.') + 1);
      String method = path[1];
      String family = familyFor(rpc);
      String serviceStem = service.endsWith("Service") ? service.substring(0, service.length() - 7) : service;
      String requestName = upperCamel(family) + upperCamel(serviceStem) + upperCamel(method) + "Request";
      String responseName = upperCamel(family) + upperCamel(serviceStem) + upperCamel(method) + "Response";
      String clientName = lowerCamel(family + serviceStem + method);
      assertNotNull(Class.forName(requests.getName() + "$" + requestName), "missing typed request " + rpc);
      assertNotNull(Class.forName(responses.getName() + "$" + responseName), "missing typed response " + rpc);
      assertTrue(java.util.Arrays.stream(clients.getDeclaredMethods()).anyMatch(candidate -> candidate.getName().equals(clientName)),
          "missing typed client " + clientName + " for " + rpc);
    }
  }

  private static String familyFor(String rpc) {
    if (rpc.startsWith("acyclic.actors.")) return "actors";
    if (rpc.startsWith("acyclic.filesystem.")) return "filesystem";
    if (rpc.startsWith("acyclic.harness.")) return "harness";
    if (rpc.startsWith("acyclic.machines.")) return "machines";
    if (rpc.startsWith("acyclic.objects.")) return "objects";
    if (rpc.startsWith("acyclic.stream.")) return "stream";
    if (rpc.startsWith("acyclic.workers.")) return "workers";
    return "inference";
  }

  private static String upperCamel(String value) {
    return Character.toUpperCase(value.charAt(0)) + value.substring(1);
  }

  private static String lowerCamel(String value) {
    return Character.toLowerCase(value.charAt(0)) + value.substring(1);
  }

  private static Path canonicalManifest() {
    String configured = System.getProperty("acyclic.canonical.manifest");
    if (configured != null && !configured.isBlank()) return Path.of(configured);
    Path cwd = Path.of(System.getProperty("user.dir")).toAbsolutePath();
    for (Path root : List.of(cwd, cwd.getParent(), cwd.getParent() == null ? cwd : cwd.getParent().getParent())) {
      if (root != null) {
        Path candidate = root.resolve(DEFAULT_MANIFEST);
        if (Files.isRegularFile(candidate)) return candidate;
      }
    }
    return cwd.resolve(DEFAULT_MANIFEST);
  }

  private static List<ServerServiceDefinition> serviceDefinitions(
      Map<String, Deque<ExpectedCall>> expected, FailureSink failures) {
    List<ServerServiceDefinition> definitions = new ArrayList<>();
    for (ServiceDescriptor descriptor : serviceDescriptors()) {
      ServerServiceDefinition.Builder builder = ServerServiceDefinition.builder(descriptor);
      for (MethodDescriptor<?, ?> method : descriptor.getMethods()) {
        builder.addMethod((MethodDescriptor) method, (ServerCallHandler) handler(method, expected, failures));
      }
      definitions.add(builder.build());
    }
    return definitions;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <Req, Resp> ServerCallHandler<Req, Resp> handler(
      MethodDescriptor<Req, Resp> method, Map<String, Deque<ExpectedCall>> expected, FailureSink failures) {
    return switch (method.getType()) {
      case UNARY -> ServerCalls.asyncUnaryCall((request, observer) -> {
        ExpectedCall call = take(expected, method, failures); if (call == null) return;
        try { checkRequest(method, call, List.of(wire(method.getRequestMarshaller(), request)), failures); emit(method, call, observer); }
        catch (Throwable error) { failures.add(error); observer.onError(Status.INTERNAL.withDescription(error.toString()).asRuntimeException()); }
      });
      case SERVER_STREAMING -> ServerCalls.asyncServerStreamingCall((request, observer) -> {
        ExpectedCall call = take(expected, method, failures); if (call == null) return;
        try { checkRequest(method, call, List.of(wire(method.getRequestMarshaller(), request)), failures); emit(method, call, observer); }
        catch (Throwable error) { failures.add(error); observer.onError(Status.INTERNAL.withDescription(error.toString()).asRuntimeException()); }
      });
      case CLIENT_STREAMING -> ServerCalls.asyncClientStreamingCall(observer -> new StreamObserver<Req>() {
        private final List<byte[]> requests = new ArrayList<>(); private ExpectedCall call;
        @Override public void onNext(Req request) { if (call == null) call = take(expected, method, failures); if (call != null) requests.add(wire(method.getRequestMarshaller(), request)); }
        @Override public void onError(Throwable error) { failures.add(error); }
        @Override public void onCompleted() { if (call == null) call = take(expected, method, failures); if (call == null) return;
          try { checkRequest(method, call, requests, failures); emit(method, call, observer); }
          catch (Throwable error) { failures.add(error); observer.onError(Status.INTERNAL.withDescription(error.toString()).asRuntimeException()); } }
      });
      case BIDI_STREAMING -> ServerCalls.asyncBidiStreamingCall(observer -> new StreamObserver<Req>() {
        private final List<byte[]> requests = new ArrayList<>(); private ExpectedCall call;
        @Override public void onNext(Req request) { if (call == null) call = take(expected, method, failures); if (call != null) requests.add(wire(method.getRequestMarshaller(), request)); }
        @Override public void onError(Throwable error) { failures.add(error); }
        @Override public void onCompleted() { if (call == null) call = take(expected, method, failures); if (call == null) return;
          try { checkRequest(method, call, requests, failures); emit(method, call, observer); }
          catch (Throwable error) { failures.add(error); observer.onError(Status.INTERNAL.withDescription(error.toString()).asRuntimeException()); } }
      });
      default -> throw new IllegalArgumentException("unsupported gRPC method type: " + method.getType());
    };
  }

  private static ExpectedCall take(Map<String, Deque<ExpectedCall>> expected, MethodDescriptor<?, ?> method, FailureSink failures) {
    Deque<ExpectedCall> calls = expected.get(method.getFullMethodName());
    if (calls == null || calls.isEmpty()) { failures.add(new AssertionError("unexpected installed call: " + method.getFullMethodName())); return null; }
    return calls.peek();
  }

  private static <Req, Resp> void emit(MethodDescriptor<Req, Resp> method, ExpectedCall call, StreamObserver<Resp> observer) {
    if (call.responses.isEmpty() && call.expectedStatus.startsWith("observed-status")) {
      observer.onError(Status.UNIMPLEMENTED.withDescription("canonical expected status").asRuntimeException()); return;
    }
    for (byte[] response : call.responses) observer.onNext(parse(method.getResponseMarshaller(), response));
    observer.onCompleted();
  }

  private static void checkRequest(MethodDescriptor<?, ?> method, ExpectedCall call, List<byte[]> actual, FailureSink failures) {
    List<byte[]> expected = call.requests;
    if (expected.isEmpty()
        && (method.getType() == MethodDescriptor.MethodType.UNARY
            || method.getType() == MethodDescriptor.MethodType.SERVER_STREAMING)) {
      return;
    }
    if (!sameFrames(expected, actual)) failures.add(new AssertionError("request wire mismatch for " + method.getFullMethodName()
        + " expected=" + describe(call.requests) + " actual=" + describe(actual)));
  }

  private static boolean sameFrames(List<byte[]> expected, List<byte[]> actual) {
    if (expected.size() != actual.size()) return false;
    for (int i = 0; i < expected.size(); i++) {
      if (!java.util.Arrays.equals(expected.get(i), actual.get(i))) return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private static void replay(MethodDescriptor<?, ?> rawMethod, ExpectedCall call, Channel channel, FailureSink failures) throws Exception {
    if (rawMethod == null) fail("missing method descriptor");
    MethodDescriptor method = rawMethod; List<byte[]> actual = new ArrayList<>(); Throwable error = null;
    try {
      switch (method.getType()) {
        case UNARY -> actual.add(wire(method.getResponseMarshaller(), ClientCalls.blockingUnaryCall(channel, method, CallOptions.DEFAULT,
            parse(method.getRequestMarshaller(), firstRequest(call)))));
        case SERVER_STREAMING -> { Iterator responses = ClientCalls.blockingServerStreamingCall(channel, method, CallOptions.DEFAULT,
            parse(method.getRequestMarshaller(), firstRequest(call))); while (responses.hasNext()) actual.add(wire(method.getResponseMarshaller(), responses.next())); }
        case CLIENT_STREAMING, BIDI_STREAMING -> {
          CountDownLatch done = new CountDownLatch(1); AtomicReference<Throwable> asyncError = new AtomicReference<>();
          StreamObserver responseObserver = new StreamObserver<>() {
            @Override public void onNext(Object response) { actual.add(wire(method.getResponseMarshaller(), response)); }
            @Override public void onError(Throwable t) { asyncError.set(t); done.countDown(); }
            @Override public void onCompleted() { done.countDown(); }
          };
          StreamObserver requestObserver = method.getType() == MethodDescriptor.MethodType.CLIENT_STREAMING
              ? ClientCalls.asyncClientStreamingCall(channel.newCall(method, CallOptions.DEFAULT), responseObserver)
              : ClientCalls.asyncBidiStreamingCall(channel.newCall(method, CallOptions.DEFAULT), responseObserver);
          for (byte[] request : call.requests) requestObserver.onNext(parse(method.getRequestMarshaller(), request));
          requestObserver.onCompleted(); assertTrue(done.await(30, TimeUnit.SECONDS), "stream did not complete: " + method.getFullMethodName());
          error = asyncError.get();
        }
      }
    } catch (Throwable caught) { error = caught; }
    if (call.responses.isEmpty() && call.expectedStatus.startsWith("observed-status")) { assertNotNull(error, "expected canonical status for " + method.getFullMethodName()); return; }
    if (error != null) { failures.add(error); return; }
    assertEquals(call.responses.size(), actual.size(), "response count for " + method.getFullMethodName());
    for (int i = 0; i < actual.size(); i++) assertArrayEquals(call.responses.get(i), actual.get(i), "response wire mismatch for " + method.getFullMethodName() + " frame " + i);
  }

  private static Map<String, MethodDescriptor<?, ?>> methodDescriptors() {
    Map<String, MethodDescriptor<?, ?>> methods = new LinkedHashMap<>();
    for (ServiceDescriptor service : serviceDescriptors()) for (MethodDescriptor<?, ?> method : service.getMethods()) {
      assertTrue(methods.put(method.getFullMethodName(), method) == null, "duplicate generated method descriptor: " + method.getFullMethodName());
    }
    return methods;
  }

  private static List<ServiceDescriptor> serviceDescriptors() {
    return List.of(ActorsServiceGrpc.getServiceDescriptor(), FilesystemServiceGrpc.getServiceDescriptor(), HarnessServiceGrpc.getServiceDescriptor(),
        MachinesServiceGrpc.getServiceDescriptor(), BucketsServiceGrpc.getServiceDescriptor(), MultipartServiceGrpc.getServiceDescriptor(),
        ObjectsServiceGrpc.getServiceDescriptor(), StreamServiceGrpc.getServiceDescriptor(), WorkersServiceGrpc.getServiceDescriptor(),
        ContextsServiceGrpc.getServiceDescriptor(), EvaluationsServiceGrpc.getServiceDescriptor(), ModelsServiceGrpc.getServiceDescriptor(),
        RunsServiceGrpc.getServiceDescriptor(), WarmContextsServiceGrpc.getServiceDescriptor());
  }

  private static <T> byte[] wire(MethodDescriptor.Marshaller<T> marshaller, T value) {
    try (InputStream input = marshaller.stream(value)) { return input.readAllBytes(); }
    catch (IOException error) { throw new IllegalStateException("cannot serialize protobuf message", error); }
  }

  private static <T> T parse(MethodDescriptor.Marshaller<T> marshaller, byte[] bytes) { return marshaller.parse(new ByteArrayInputStream(bytes)); }
  private static byte[] firstRequest(ExpectedCall call) { return call.requests.isEmpty() ? new byte[0] : call.requests.get(0); }
  private static String describe(Collection<byte[]> values) { List<String> hashes = new ArrayList<>(); for (byte[] value : values) hashes.add(Integer.toHexString(java.util.Arrays.hashCode(value))); return hashes.toString(); }

  private static final class FailureSink {
    private final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
    void add(Throwable failure) { failures.add(failure); }
    boolean isEmpty() { return failures.isEmpty(); }
    String describe() { return failures.toString(); }
  }

  private static final class ExpectedCall {
    final String expectedStatus; final List<byte[]> requests; final List<byte[]> responses;
    ExpectedCall(String expectedStatus, List<byte[]> requests, List<byte[]> responses) { this.expectedStatus = expectedStatus; this.requests = List.copyOf(requests); this.responses = List.copyOf(responses); }
    static ExpectedCall from(JsonObject entry) { return new ExpectedCall(entry.get("expected_status").getAsString(), frames(entry, "request_frames", "bytes_base64", "request_base64"), frames(entry, "response_frames", "response_base64", "response_base64")); }
    private static List<byte[]> frames(JsonObject entry, String framesKey, String frameBytesKey, String singleKey) {
      List<byte[]> values = new ArrayList<>(); JsonElement frames = entry.get(framesKey);
      if (frames != null && frames.isJsonArray()) { for (JsonElement frame : frames.getAsJsonArray()) { JsonElement value = frame.getAsJsonObject().get(frameBytesKey); values.add(value == null || value.isJsonNull() ? new byte[0] : Base64.getDecoder().decode(value.getAsString())); } return values; }
      JsonElement single = entry.get(singleKey); values.add(single == null || single.isJsonNull() ? new byte[0] : Base64.getDecoder().decode(single.getAsString())); return values;
    }
  }
}
