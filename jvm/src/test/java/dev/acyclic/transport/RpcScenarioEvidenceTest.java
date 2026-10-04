package dev.acyclic.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

/**
 * Exercises every generated JVM gRPC method through a real in-process gRPC
 * channel and records source-bound qualification evidence. The in-process
 * server uses the generated method marshallers, so a passing scenario proves
 * invocation, transport dispatch, and protobuf serialization for that method.
 */
final class RpcScenarioEvidenceTest {
  private static final List<Class<?>> SERVICES = List.of(
      acyclic.actors.v1.ActorsServiceGrpc.class,
      acyclic.stream.v2.StreamServiceGrpc.class,
      acyclic.objects.v2.ObjectsServiceGrpc.class,
      acyclic.objects.v2.BucketsServiceGrpc.class,
      acyclic.objects.v2.MultipartServiceGrpc.class,
      acyclic.workers.v1.WorkersServiceGrpc.class,
      acyclic.filesystem.v2.FilesystemServiceGrpc.class,
      acyclic.harness.v2.HarnessServiceGrpc.class,
      acyclic.machines.v1.MachinesServiceGrpc.class,
      inference.customer.v1.ContextsServiceGrpc.class,
      inference.customer.v1.EvaluationsServiceGrpc.class,
      inference.customer.v1.ModelsServiceGrpc.class,
      inference.customer.v1.RunsServiceGrpc.class,
      inference.customer.v1.WarmContextsServiceGrpc.class);

  @Test
  void recordsEveryGeneratedRpcScenario() throws Exception {
    String revision = requiredProperty("acyclic.source.revision");
    Path outputRoot = Path.of(requiredProperty("acyclic.scenario.output"));
    String artifact = System.getProperty("acyclic.consumer.artifact",
        "qualification/consumers/jvm-transport.jar");
    Path consumerRoot = outputRoot.resolve("qualification/consumers").normalize();
    Files.createDirectories(consumerRoot);

    List<Scenario> scenarios = new ArrayList<>();
    for (Class<?> serviceClass : SERVICES) {
      Method descriptorMethod = serviceClass.getMethod("getServiceDescriptor");
      ServiceDescriptor descriptor = (ServiceDescriptor) descriptorMethod.invoke(null);
      scenarios.addAll(exerciseService(descriptor, revision, consumerRoot));
    }
    scenarios.sort(Comparator.comparing(Scenario::rpc));
    assertTrue(!scenarios.isEmpty(), "generated JVM services must expose RPCs");
    Map<String, String> observed = new LinkedHashMap<>();
    for (Scenario scenario : scenarios) {
      assertTrue(observed.put(scenario.rpc(), scenario.shape()) == null,
          "duplicate generated RPC " + scenario.rpc());
    }
    assertEquals(authorityMethods(), observed,
        "generated JVM RPC inventory must match the Rust authority manifest");

    List<String> outputs = new ArrayList<>();
    for (int index = 0; index < scenarios.size(); index++) {
      Scenario scenario = scenarios.get(index);
      String filename = String.format("jvm-rpc-%03d.json", index + 1);
      Path path = consumerRoot.resolve(filename);
      Files.writeString(path, scenario.json(), StandardCharsets.UTF_8);
      outputs.add("qualification/consumers/" + filename);
    }

    Path log = consumerRoot.resolve("jvm-rpc-scenario-log.json");
    try (Writer writer = Files.newBufferedWriter(log, StandardCharsets.UTF_8)) {
      writer.write("{\n");
      writer.write("  \"schema\": \"acyclic.sdk.rpc-scenario-log.v1\",\n");
      writer.write("  \"source_revision\": \"" + json(revision) + "\",\n");
      writer.write("  \"consumer\": {\n");
      writer.write("    \"name\": \"jvm\",\n");
      writer.write("    \"version\": \"0.2.0-SNAPSHOT\",\n");
      writer.write("    \"artifact_path\": \"" + json(artifact) + "\",\n");
      writer.write("    \"artifact_sha256\": \"" + json(sha256(outputRoot.resolve(artifact))) + "\"\n");
      writer.write("  },\n  \"scenarios\": [\n");
      for (int index = 0; index < outputs.size(); index++) {
        if (index > 0) writer.write(",\n");
        Path scenarioPath = outputRoot.resolve(outputs.get(index));
        writer.write("    {\"output_path\": \"" + json(outputs.get(index))
            + "\", \"output_sha256\": \"" + json(sha256(scenarioPath)) + "\"}");
      }
      writer.write("\n  ]\n}\n");
    }
    System.out.println("recorded " + scenarios.size() + " generated JVM RPC scenarios");
  }

  private static List<Scenario> exerciseService(ServiceDescriptor descriptor, String revision,
      Path consumerRoot) throws Exception {
    String name = InProcessServerBuilder.generateName();
    ServerServiceDefinition.Builder service = ServerServiceDefinition.builder(descriptor);
    for (MethodDescriptor<?, ?> method : descriptor.getMethods()) {
      addHandler(service, method);
    }
    Server server = InProcessServerBuilder.forName(name).directExecutor().addService(service.build()).build().start();
    Channel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    try {
      List<Scenario> scenarios = new ArrayList<>();
      for (MethodDescriptor<?, ?> method : descriptor.getMethods()) {
        invoke(channel, method);
        String rpc = method.getFullMethodName();
        scenarios.add(new Scenario(revision, familyForRpc(rpc), rpc,
            shape(method.getType())));
      }
      return scenarios;
    } finally {
      ((io.grpc.ManagedChannel) channel).shutdownNow();
      server.shutdownNow();
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void addHandler(ServerServiceDefinition.Builder service, MethodDescriptor method) {
    switch (method.getType()) {
      case UNARY -> service.addMethod(method, ServerCalls.asyncUnaryCall((request, observer) -> {
        observer.onNext(empty(method.getResponseMarshaller()));
        observer.onCompleted();
      }));
      case SERVER_STREAMING -> service.addMethod(method, ServerCalls.asyncServerStreamingCall((request, observer) -> {
        observer.onNext(empty(method.getResponseMarshaller()));
        observer.onCompleted();
      }));
      case CLIENT_STREAMING -> service.addMethod(method, ServerCalls.asyncClientStreamingCall(observer ->
          new StreamObserver() {
            @Override public void onNext(Object value) { }
            @Override public void onError(Throwable error) { observer.onError(error); }
            @Override public void onCompleted() {
              observer.onNext(empty(method.getResponseMarshaller()));
              observer.onCompleted();
            }
          }));
      case BIDI_STREAMING -> service.addMethod(method, ServerCalls.asyncBidiStreamingCall(observer ->
          new StreamObserver() {
            @Override public void onNext(Object value) { observer.onNext(empty(method.getResponseMarshaller())); }
            @Override public void onError(Throwable error) { observer.onError(error); }
            @Override public void onCompleted() { observer.onCompleted(); }
          }));
      default -> throw new IllegalArgumentException("unsupported gRPC method type: " + method.getType());
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void invoke(Channel channel, MethodDescriptor method) throws Exception {
    Object request = empty(method.getRequestMarshaller());
    switch (method.getType()) {
      case UNARY -> ClientCalls.blockingUnaryCall(channel, method, CallOptions.DEFAULT, request);
      case SERVER_STREAMING -> {
        java.util.Iterator<?> responses = ClientCalls.blockingServerStreamingCall(
            channel, method, CallOptions.DEFAULT, request);
        while (responses.hasNext()) responses.next();
      }
      case CLIENT_STREAMING -> streamCall(channel, method, request, false);
      case BIDI_STREAMING -> streamCall(channel, method, request, true);
      default -> throw new IllegalArgumentException("unsupported gRPC method type: " + method.getType());
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void streamCall(Channel channel, MethodDescriptor method, Object request, boolean bidi)
      throws Exception {
    CountDownLatch done = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    StreamObserver response = new StreamObserver() {
      @Override public void onNext(Object value) { }
      @Override public void onError(Throwable error) { failure.set(error); done.countDown(); }
      @Override public void onCompleted() { done.countDown(); }
    };
    ClientCall call = channel.newCall(method, CallOptions.DEFAULT);
    StreamObserver requests = bidi
        ? ClientCalls.asyncBidiStreamingCall(call, response)
        : ClientCalls.asyncClientStreamingCall(call, response);
    requests.onNext(request);
    requests.onCompleted();
    assertTrue(done.await(5, TimeUnit.SECONDS), method.getFullMethodName() + " did not complete");
    if (failure.get() != null) throw new AssertionError(method.getFullMethodName(), failure.get());
  }

  private static Object empty(MethodDescriptor.Marshaller<?> marshaller) {
    return marshaller.parse(new ByteArrayInputStream(new byte[0]));
  }

  private static String shape(MethodDescriptor.MethodType type) {
    return switch (type) {
      case UNARY -> "unary";
      case CLIENT_STREAMING -> "client";
      case SERVER_STREAMING -> "server";
      case BIDI_STREAMING -> "bidi";
      default -> throw new IllegalArgumentException("unsupported gRPC method type: " + type);
    };
  }

  private static String familyForRpc(String rpc) {
    int dot = rpc.indexOf('.');
    if (dot < 0) throw new IllegalArgumentException("RPC has no package: " + rpc);
    String packageName = rpc.substring(0, dot);
    if (packageName.equals("inference")) return "inference";
    int secondDot = rpc.indexOf('.', dot + 1);
    if (secondDot < 0) throw new IllegalArgumentException("RPC has no version: " + rpc);
    return rpc.substring(dot + 1, secondDot);
  }

  private static Map<String, String> authorityMethods() throws IOException {
    InputStream resource = RpcScenarioEvidenceTest.class.getClassLoader()
        .getResourceAsStream("rust-authority.json");
    if (resource == null) throw new IOException("missing Rust authority manifest resource");
    String manifest;
    try (resource) {
      manifest = new String(resource.readAllBytes(), StandardCharsets.UTF_8);
    }
    Pattern method = Pattern.compile(
        "\\\"rpc\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"\\s*,\\s*\\\"shape\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");
    Matcher matcher = method.matcher(manifest);
    Map<String, String> methods = new LinkedHashMap<>();
    while (matcher.find()) {
      String rpc = matcher.group(1);
      String shape = matcher.group(2);
      if (methods.put(rpc, shape) != null) throw new IOException("duplicate Rust authority RPC " + rpc);
    }
    if (methods.isEmpty()) throw new IOException("Rust authority manifest has no RPC methods");
    return methods;
  }

  private record Scenario(String revision, String family, String rpc, String shape) {
    String json() {
      return "{\"schema\":\"acyclic.sdk.rpc-scenario-result.v1\",\"source_revision\":\""
          + RpcScenarioEvidenceTest.json(revision) + "\",\"status\":\"passed\",\"invoked\":true,\"exit_code\":0,"
          + "\"family\":\"" + RpcScenarioEvidenceTest.json(family) + "\",\"rpc\":\"" + RpcScenarioEvidenceTest.json(rpc)
          + "\",\"shape\":\"" + shape + "\",\"transport\":\"grpc\",\"execution_mode\":\"in-process\","
          + "\"checks\":[\"invocation\",\"transport\",\"serialization\"]}\n";
    }
  }

  private static String requiredProperty(String name) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) throw new IllegalArgumentException("missing -D" + name);
    return value;
  }

  private static String json(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static String sha256(Path path) throws Exception {
    if (!Files.isRegularFile(path)) throw new IOException("missing scenario artifact: " + path);
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream input = Files.newInputStream(path)) {
      byte[] buffer = new byte[8192];
      for (int read; (read = input.read(buffer)) >= 0;) if (read > 0) digest.update(buffer, 0, read);
    }
    StringBuilder hex = new StringBuilder("sha256:");
    for (byte value : digest.digest()) hex.append(String.format("%02x", value));
    return hex.toString();
  }
}
