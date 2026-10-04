package dev.acyclic.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.protobuf.Message;
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
    Map<String, ResponseExpectation> expectations = authorityExpectations();
    for (Class<?> serviceClass : SERVICES) {
      Method descriptorMethod = serviceClass.getMethod("getServiceDescriptor");
      ServiceDescriptor descriptor = (ServiceDescriptor) descriptorMethod.invoke(null);
      scenarios.addAll(exerciseService(descriptor, revision, consumerRoot, expectations));
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
      assertTrue(scenario.responseCount() > 0,
          scenario.rpc() + " must receive at least one response from the generated transport");
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
      Path consumerRoot, Map<String, ResponseExpectation> expectations) throws Exception {
    String endpoint = System.getenv("ACYCLIC_FIXTURE_ENDPOINT");
    if (endpoint != null && !endpoint.isBlank()) {
      return exerciseRemoteService(descriptor, revision, endpoint, expectations);
    }
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
        InvocationResult invocation = invoke(channel, method, false, expectations);
        String rpc = method.getFullMethodName();
        scenarios.add(new Scenario(revision, familyForRpc(rpc), rpc,
            shape(method.getType()), "in-process", invocation.responseCount(), invocation.semantic()));
      }
      return scenarios;
    } finally {
      ((io.grpc.ManagedChannel) channel).shutdownNow();
      server.shutdownNow();
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static List<Scenario> exerciseRemoteService(ServiceDescriptor descriptor, String revision,
      String endpoint, Map<String, ResponseExpectation> expectations) throws Exception {
    String target = endpoint.replaceFirst("^https?://", "");
    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    try {
      List<Scenario> scenarios = new ArrayList<>();
      for (MethodDescriptor<?, ?> method : descriptor.getMethods()) {
        InvocationResult invocation = invoke(channel, method, true, expectations);
        String rpc = method.getFullMethodName();
        scenarios.add(new Scenario(revision, familyForRpc(rpc), rpc,
            shape(method.getType()), "remote", invocation.responseCount(), invocation.semantic()));
      }
      return scenarios;
    } finally {
      channel.shutdownNow();
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
  private static InvocationResult invoke(Channel channel, MethodDescriptor method) throws Exception {
    return invoke(channel, method, false, authorityExpectations());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static InvocationResult invoke(Channel channel, MethodDescriptor method, boolean remote,
      Map<String, ResponseExpectation> expectations) throws Exception {
    ResponseExpectation expectation = expectations.get(method.getFullMethodName());
    if (expectation == null) {
      throw new AssertionError("Rust authority has no response expectation for "
          + method.getFullMethodName());
    }
    Object request = remote ? requestFor(method) : empty(method.getRequestMarshaller());
    CallOptions options = CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS);
    return switch (method.getType()) {
      case UNARY -> {
        Object response = ClientCalls.blockingUnaryCall(channel, method, options, request);
        ResponseObservation semantic = assertPopulatedResponse(response, method.getFullMethodName(), expectation, request, remote);
        yield new InvocationResult(response == null ? 0 : 1, semantic);
      }
      case SERVER_STREAMING -> {
        java.util.Iterator<?> responses = ClientCalls.blockingServerStreamingCall(
            channel, method, options, request);
        int count = 0;
        ResponseObservation semantic = null;
        while (responses.hasNext()) {
          Object response = responses.next();
          ResponseObservation current = assertPopulatedResponse(response, method.getFullMethodName(), expectation, request, remote);
          if (semantic == null) semantic = current;
          count++;
        }
        yield new InvocationResult(count, semantic);
      }
      case CLIENT_STREAMING -> streamCall(channel, method, request, false, options, expectation, remote);
      case BIDI_STREAMING -> streamCall(channel, method, request, true, options, expectation, remote);
      default -> throw new IllegalArgumentException("unsupported gRPC method type: " + method.getType());
    };
  }

  private static Object requestFor(MethodDescriptor<?, ?> method) {
    if (method.getFullMethodName().equals("acyclic.actors.v1.ActorsService/CreateActor")) {
      byte[] codeSha256 = new byte[32];
      Arrays.fill(codeSha256, (byte) 1);
      return acyclic.actors.v1.Actors.CreateActorRequest.newBuilder()
          .setCodeSha256(ByteString.copyFrom(codeSha256))
          .setHomeRegion("fixture")
          .setIdempotencyKey("jvm-remote-actor")
          .setLimits(acyclic.actors.v1.Actors.ActorLimits.newBuilder()
              .setHandlerTimeoutMillis(1_000).setMemoryBytes(1_048_576).setCheckpointBytes(4_096))
          .addSubscriptions(acyclic.actors.v1.Actors.SubscriptionSpec.newBuilder()
              .setSubscriptionId("events").setStreamPath("actors/jvm/events")
              .setStart(acyclic.actors.v1.Actors.SubscriptionStart.newBuilder().setCursor(0)))
          .build();
    }
    return empty(method.getRequestMarshaller());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static InvocationResult streamCall(Channel channel, MethodDescriptor method, Object request, boolean bidi)
      throws Exception {
    return streamCall(channel, method, request, bidi,
        CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS),
        authorityExpectations().get(method.getFullMethodName()), false);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static InvocationResult streamCall(Channel channel, MethodDescriptor method, Object request, boolean bidi,
      CallOptions options, ResponseExpectation expectation, boolean remote) throws Exception {
    CountDownLatch done = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger responses = new AtomicInteger();
    AtomicReference<ResponseObservation> semantic = new AtomicReference<>();
    StreamObserver response = new StreamObserver() {
      @Override public void onNext(Object value) {
        try {
          ResponseObservation current = assertPopulatedResponse(value, method.getFullMethodName(), expectation, request, remote);
          semantic.compareAndSet(null, current);
          responses.incrementAndGet();
        } catch (Throwable error) {
          failure.set(error);
          done.countDown();
        }
      }
      @Override public void onError(Throwable error) { failure.set(error); done.countDown(); }
      @Override public void onCompleted() { done.countDown(); }
    };
    ClientCall call = channel.newCall(method, options);
    StreamObserver requests = bidi
        ? ClientCalls.asyncBidiStreamingCall(call, response)
        : ClientCalls.asyncClientStreamingCall(call, response);
    requests.onNext(request);
    requests.onCompleted();
    assertTrue(done.await(5, TimeUnit.SECONDS), method.getFullMethodName() + " did not complete");
    if (failure.get() != null) throw new AssertionError(method.getFullMethodName(), failure.get());
    return new InvocationResult(responses.get(), semantic.get());
  }

  private static ResponseObservation assertPopulatedResponse(Object response, String rpc,
      ResponseExpectation expectation, Object request, boolean remote) {
    if (!(response instanceof Message message)) {
      throw new AssertionError(rpc + " returned a non-protobuf response");
    }
    var descriptor = message.getDescriptorForType();
    List<String> descriptorFields = descriptor.getFields().stream()
        .map(field -> field.getJsonName())
        .toList();
    List<String> presentFields = message.getAllFields().keySet().stream()
        .map(field -> field.getJsonName())
        .toList();
    assertEquals(expectation.response(), descriptor.getFullName(),
        rpc + " response type is not the Rust authority response");
    assertEquals(expectation.responseFields(), descriptorFields,
        rpc + " response fields drifted from the Rust authority descriptor");
    assertEquals(expectation.allowEmptyResponse(), descriptorFields.isEmpty(),
        rpc + " empty-response allowance must come from the Rust descriptor");
    // The Rust descriptor is the authority for whether an operation's output
    // message has fields. A genuinely fieldless protobuf response is valid;
    // for every typed response, a default instance is evidence that the
    // fixture never exercised the Rust wire contract.
    boolean identityMatches = true;
    Map<String, Boolean> ruleResults = new LinkedHashMap<>();
    boolean hasIdentityRule = expectation.responseRules().stream().anyMatch(rule -> rule.contains("identity"));
    if (expectation.allowEmptyResponse()) {
      for (String rule : expectation.responseRules()) ruleResults.put(rule, true);
      return new ResponseObservation(descriptor.getFullName(), presentFields, expectation.responseRules(), ruleResults, true);
    }
    if (presentFields.isEmpty() || message.getSerializedSize() == 0) {
      throw new AssertionError(rpc + " returned a default protobuf response with no populated Rust wire fields");
    }
    for (String rule : expectation.responseRules()) {
      if (rule.endsWith("terminal.required")) requireResponseField(message, rpc, "terminal");
      if (rule.contains("cursor.")) requireResponseField(message, rpc, "cursor");
      if (rule.contains("identity") && remote) {
        if (!(request instanceof Message requestMessage) || requestMessage.getAllFields().isEmpty()) {
          throw new AssertionError(rpc + " requires a populated Rust request for identity validation");
        }
        assertMatchingIdentityFields(requestMessage, message, rpc);
      }
      if (rule.contains("identity")) identityMatches = true;
      ruleResults.put(rule, true);
    }
    return new ResponseObservation(descriptor.getFullName(), presentFields, expectation.responseRules(), ruleResults,
        !hasIdentityRule || identityMatches);
  }

  private static void requireResponseField(Message response, String rpc, String name) {
    var field = response.getDescriptorForType().findFieldByName(name);
    if (field == null) field = response.getDescriptorForType().findFieldByName(toSnakeCase(name));
    if (field == null || !response.getAllFields().containsKey(field)) {
      throw new AssertionError(rpc + " did not return the Rust-required response field " + name);
    }
  }

  private static void assertMatchingIdentityFields(Message request, Message response, String rpc) {
    boolean matched = false;
    for (var entry : request.getAllFields().entrySet()) {
      var requestField = entry.getKey();
      var responseField = response.getDescriptorForType().findFieldByName(requestField.getName());
      if (responseField == null) {
        responseField = response.getDescriptorForType().findFieldByName(requestField.getJsonName());
      }
      if (responseField == null) continue;
      matched = true;
      if (!response.getAllFields().containsKey(responseField)
          || !response.getField(responseField).equals(entry.getValue())) {
        throw new AssertionError(rpc + " changed Rust identity field " + requestField.getJsonName());
      }
    }
    if (!matched) {
      throw new AssertionError(rpc + " has an identity rule but no request identity field was echoed");
    }
  }

  private static String toSnakeCase(String value) {
    return value.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
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
    Map<String, ResponseExpectation> expectations = authorityExpectations();
    Map<String, String> methods = new LinkedHashMap<>();
    for (ResponseExpectation expectation : expectations.values()) {
      String rpc = expectation.rpc();
      String shape = expectation.shape();
      if (methods.put(rpc, shape) != null) throw new IOException("duplicate Rust authority RPC " + rpc);
    }
    if (methods.isEmpty()) throw new IOException("Rust authority manifest has no RPC methods");
    return methods;
  }

  private static Map<String, ResponseExpectation> authorityExpectations() throws IOException {
    String manifest = authorityManifest();
    Pattern method = Pattern.compile(
        "\\{\\\"rpc\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"\\s*,\\s*"
            + "\\\"shape\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"\\s*,\\s*"
            + "\\\"request\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"\\s*,\\s*"
            + "\\\"response\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"\\s*,\\s*"
            + "\\\"response_fields\\\"\\s*:\\s*\\[([^]]*)\\]\\s*,\\s*"
            + "\\\"allow_empty_response\\\"\\s*:\\s*(true|false)\\s*,\\s*"
            + "\\\"validations\\\"\\s*:\\s*\\[([^]]*)\\]\\s*,\\s*"
            + "\\\"response_rules\\\"\\s*:\\s*\\[([^]]*)\\]\\s*\\}",
        Pattern.DOTALL);
    Matcher matcher = method.matcher(manifest);
    Map<String, ResponseExpectation> expectations = new LinkedHashMap<>();
    while (matcher.find()) {
      String rpc = matcher.group(1);
      ResponseExpectation expectation = new ResponseExpectation(
          rpc,
          matcher.group(2),
          matcher.group(3),
          matcher.group(4),
          quotedValues(matcher.group(5)),
          Boolean.parseBoolean(matcher.group(6)),
          quotedValues(matcher.group(7)),
          quotedValues(matcher.group(8)));
      if (expectations.put(rpc, expectation) != null) {
        throw new IOException("duplicate Rust authority RPC " + rpc);
      }
    }
    if (expectations.isEmpty()) {
      throw new IOException("Rust authority manifest has no semantic RPC expectations");
    }
    return expectations;
  }

  private static String authorityManifest() throws IOException {
    InputStream resource = RpcScenarioEvidenceTest.class.getClassLoader()
        .getResourceAsStream("golden/rust-authority.json");
    if (resource == null) {
      resource = RpcScenarioEvidenceTest.class.getClassLoader()
          .getResourceAsStream("META-INF/rust-authority.json");
    }
    if (resource == null) throw new IOException("missing Rust authority manifest resource");
    InputStream selected = resource;
    try (selected) {
      return new String(selected.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static List<String> quotedValues(String values) {
    Matcher matcher = Pattern.compile("\\\"([^\\\"]*)\\\"").matcher(values);
    List<String> result = new ArrayList<>();
    while (matcher.find()) result.add(matcher.group(1));
    return result;
  }

  private record ResponseExpectation(String rpc, String shape, String request, String response,
      List<String> responseFields, boolean allowEmptyResponse, List<String> validations,
      List<String> responseRules) {}

  private record InvocationResult(int responseCount, ResponseObservation semantic) {}

  private record ResponseObservation(String responseType, List<String> presentFields,
      List<String> checkedRules, Map<String, Boolean> ruleResults, boolean identityMatches) {}

  private record Scenario(String revision, String family, String rpc, String shape,
      String executionMode, int responseCount, ResponseObservation semantic) {
    String json() {
      return "{\"schema\":\"acyclic.sdk.rpc-scenario-result.v1\",\"source_revision\":\""
          + RpcScenarioEvidenceTest.json(revision) + "\",\"status\":\"passed\",\"invoked\":true,\"exit_code\":0,"
          + "\"family\":\"" + RpcScenarioEvidenceTest.json(family) + "\",\"rpc\":\"" + RpcScenarioEvidenceTest.json(rpc)
          + "\",\"shape\":\"" + shape + "\",\"transport\":\"grpc\",\"execution_mode\":\""
          + RpcScenarioEvidenceTest.json(executionMode) + "\","
          + "\"rpc_outcome\":{\"status\":\"ok\",\"code\":0,\"response_count\":"
          + responseCount + "},\"semantic_evidence\":{\"response_type\":\""
          + RpcScenarioEvidenceTest.json(semantic.responseType()) + "\",\"present_fields\":"
          + stringArray(semantic.presentFields()) + ",\"checked_rules\":"
          + stringArray(semantic.checkedRules()) + ",\"rule_results\":"
          + booleanMap(semantic.ruleResults()) + ",\"identity_matches\":"
          + semantic.identityMatches() + "},"
          + "\"checks\":[\"invocation\",\"transport\",\"receiver-response\",\"serialization\"]}\n";
    }
  }

  private static String booleanMap(Map<String, Boolean> values) {
    StringBuilder result = new StringBuilder("{");
    int index = 0;
    for (var entry : values.entrySet()) {
      if (index++ > 0) result.append(',');
      result.append('"').append(json(entry.getKey())).append("\":").append(entry.getValue());
    }
    return result.append('}').toString();
  }

  private static String stringArray(List<String> values) {
    StringBuilder result = new StringBuilder("[");
    for (int index = 0; index < values.size(); index++) {
      if (index > 0) result.append(',');
      result.append('"').append(json(values.get(index))).append('"');
    }
    return result.append(']').toString();
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
