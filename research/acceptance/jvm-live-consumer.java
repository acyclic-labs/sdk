/*
 * Live JVM conformance consumer for the Rust authority export.
 *
 * This is deliberately a source-only acceptance tool.  It does not use the
 * in-process test service and it does not turn a transport error into a
 * passing result.  The Rust generation job supplies an RPC inventory whose
 * records contain the serialized request and the response fields expected by
 * the matching fixture.  The program loads the descriptors from the same
 * authority export, calls the configured endpoint over gRPC, decodes the
 * response dynamically, and writes raw evidence.
 *
 * Usage (from the generated JVM package, with its dependency classpath):
 *   java ... JvmLiveConsumer <endpoint> <authority-root> <rpc-inventory> <out>
 *
 * Environment variables are accepted for CI:
 *   ACYCLIC_FIXTURE_ENDPOINT, ACYCLIC_AUTHORITY_ROOT,
 *   ACYCLIC_RPC_INVENTORY, ACYCLIC_JVM_LIVE_EVIDENCE,
 *   ACYCLIC_EXPECTED_SOURCE_REVISION, ACYCLIC_ALLOW_EMPTY_REQUESTS
 */

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Performs live network calls using descriptors emitted by Rust. */
final class JvmLiveConsumer {
  private JvmLiveConsumer() {}

  private static final class InventoryRecord {
    String rpc;
    String requestBase64;
    String requestJsonBase64;
    String requestPath;
    List<String> expectedFields = List.of();
    String scenario = "inventory";
  }

  private static final class Result {
    String rpc;
    String shape;
    String status;
    String grpcStatus;
    String detail;
    long durationMs;
    int responseCount;
    List<String> populatedFields = List.of();
    List<String> expectedFields = List.of();
    String requestSha256;
    String responseSha256;
    String requestBytesBase64;
    List<String> responseFramesBase64 = List.of();
  }

  public static void main(String[] args) throws Exception {
    String endpoint = value(args, 0, "ACYCLIC_FIXTURE_ENDPOINT", null);
    String authorityRootText = value(args, 1, "ACYCLIC_AUTHORITY_ROOT", null);
    String inventoryText = value(args, 2, "ACYCLIC_RPC_INVENTORY", null);
    String outputText = value(args, 3, "ACYCLIC_JVM_LIVE_EVIDENCE", "jvm-live-scenario-log.json");
    if (endpoint == null || authorityRootText == null || inventoryText == null) {
      throw new IllegalArgumentException(
          "endpoint, authority root, and RPC inventory are required; use arguments or "
              + "ACYCLIC_FIXTURE_ENDPOINT/ACYCLIC_AUTHORITY_ROOT/ACYCLIC_RPC_INVENTORY");
    }

    Path authorityRoot = Path.of(authorityRootText).toAbsolutePath().normalize();
    Path manifestPath = authorityRoot.resolve("rust-authority.json");
    Map<String, Object> manifest = asObject(Json.parse(Files.readString(manifestPath)));
    String modelRevision = string(manifest.get("source_revision"));
    String sourceGitRevision = firstString(manifest, "source_git_revision", "git_revision");
    if (sourceGitRevision == null || sourceGitRevision.isBlank())
      sourceGitRevision = System.getenv("ACYCLIC_SOURCE_GIT_REVISION");
    String expectedRevision = System.getenv("ACYCLIC_EXPECTED_SOURCE_REVISION");
    if (expectedRevision != null && !expectedRevision.isBlank()
        && !Objects.equals(expectedRevision, modelRevision)) {
      throw new IllegalStateException("authority model revision mismatch: expected "
          + expectedRevision + ", got " + modelRevision);
    }

    List<Descriptors.FileDescriptor> files = loadDescriptors(authorityRoot, manifest);
    Map<String, Descriptors.MethodDescriptor> methods = new LinkedHashMap<>();
    for (Descriptors.FileDescriptor file : files) {
      for (Descriptors.ServiceDescriptor service : file.getServices()) {
        for (Descriptors.MethodDescriptor method : service.getMethods()) {
          String rpc = service.getFullName() + "/" + method.getName();
          methods.put(rpc, method);
        }
      }
    }

    Map<String, InventoryRecord> inventory = loadInventory(Path.of(inventoryText));
    boolean allowEmpty = Boolean.parseBoolean(System.getenv().getOrDefault(
        "ACYCLIC_ALLOW_EMPTY_REQUESTS", "false"));
    List<Result> results = new ArrayList<>();
    ManagedChannel channel = channel(endpoint);
    try {
      for (Map.Entry<String, InventoryRecord> entry : inventory.entrySet()) {
        Descriptors.MethodDescriptor method = methods.get(entry.getKey());
        if (method == null) {
          Result r = base(entry.getKey(), "unknown", entry.getValue());
          r.status = "failed";
          r.detail = "RPC is absent from the Rust authority descriptor export";
          results.add(r);
          continue;
        }
        results.add(invoke(channel, method, entry.getValue(), allowEmpty));
      }
    } finally {
      channel.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }

    long passed = results.stream().filter(r -> "passed".equals(r.status)).count();
    long failed = results.stream().filter(r -> "failed".equals(r.status)).count();
    long blocked = results.size() - passed - failed;
    Map<String, Object> output = new LinkedHashMap<>();
    output.put("schema", "acyclic.jvm-live-evidence.v1");
    output.put("authority", "rust");
    output.put("endpoint", endpoint);
    output.put("authority_root", authorityRoot.toString());
    output.put("authority_manifest_sha256", sha256(Files.readAllBytes(manifestPath)));
    output.put("rust_model_revision", modelRevision);
    output.put("source_git_revision", sourceGitRevision);
    output.put("inventory", Path.of(inventoryText).toAbsolutePath().normalize().toString());
    output.put("inventory_sha256", sha256(Files.readAllBytes(Path.of(inventoryText))));
    output.put("generated_at", Instant.now().toString());
    output.put("live_network", true);
    output.put("in_process", false);
    output.put("counts", Map.of("total", results.size(), "passed", passed,
        "failed", failed, "blocked", blocked));
    List<Object> encoded = new ArrayList<>();
    for (Result r : results) encoded.add(resultMap(r));
    output.put("results", encoded);
    Path out = Path.of(outputText).toAbsolutePath().normalize();
    if (out.getParent() != null) Files.createDirectories(out.getParent());
    Files.writeString(out, Json.stringify(output) + System.lineSeparator(), StandardCharsets.UTF_8);
    System.out.println(Json.stringify(output));
    if (failed != 0 || blocked != 0 || passed == 0) System.exit(2);
  }

  private static ManagedChannel channel(String endpoint) {
    String target = endpoint;
    if (target.startsWith("http://")) target = target.substring("http://".length());
    if (target.startsWith("https://")) {
      target = target.substring("https://".length());
      String ca = System.getenv("ACYCLIC_FIXTURE_TLS_CA");
      String cert = System.getenv("ACYCLIC_FIXTURE_TLS_CERT");
      String key = System.getenv("ACYCLIC_FIXTURE_TLS_KEY");
      try {
        SslContextBuilder ssl = SslContextBuilder.forClient();
        if (ca != null && !ca.isBlank()) ssl.trustManager(Path.of(ca).toFile());
        if (cert != null && !cert.isBlank() && key != null && !key.isBlank())
          ssl.keyManager(Path.of(cert).toFile(), Path.of(key).toFile());
        return NettyChannelBuilder.forTarget(target).sslContext(ssl.build()).build();
      } catch (Exception e) {
        throw new IllegalStateException("could not configure fixture TLS", e);
      }
    }
    return ManagedChannelBuilder.forTarget(target).usePlaintext().build();
  }

  private static Result invoke(Channel channel, Descriptors.MethodDescriptor method,
      InventoryRecord record, boolean allowEmpty) {
    boolean clientStreaming = method.isClientStreaming();
    boolean serverStreaming = method.isServerStreaming();
    String shape = !clientStreaming && !serverStreaming ? "unary" : serverStreaming && !clientStreaming ? "server"
        : clientStreaming && !serverStreaming ? "client" : "bidi";
    Result result = base(method.getService().getFullName() + "/" + method.getName(), shape, record);
    long started = System.nanoTime();
    try {
      byte[] requestBytes = requestBytes(record, method.getInputType(), allowEmpty);
      result.requestSha256 = sha256(requestBytes);
      result.requestBytesBase64 = Base64.getEncoder().encodeToString(requestBytes);
      DynamicMessage request = DynamicMessage.parseFrom(method.getInputType(), requestBytes);
      MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethod = grpcMethod(method, shape);
      List<DynamicMessage> responses = new ArrayList<>();
      if (!clientStreaming && !serverStreaming) {
        responses.add(ClientCalls.blockingUnaryCall(channel, grpcMethod, CallOptions.DEFAULT, request));
      } else if (serverStreaming && !clientStreaming) {
        java.util.Iterator<DynamicMessage> iterator = ClientCalls.blockingServerStreamingCall(
            channel, grpcMethod, CallOptions.DEFAULT, request);
        while (iterator.hasNext()) responses.add(iterator.next());
      } else if (clientStreaming && !serverStreaming) {
        responses.add(clientStream(channel, grpcMethod, request, method.getOutputType()));
      } else {
        responses.add(bidiStream(channel, grpcMethod, request, method.getOutputType()));
      }
      result.responseCount = responses.size();
      List<String> fields = new ArrayList<>();
      MessageBytes bytes = new MessageBytes();
      List<String> responseFrames = new ArrayList<>();
      for (DynamicMessage response : responses) {
        fields.addAll(populatedFields(response, ""));
        bytes.append(response.toByteArray());
        responseFrames.add(Base64.getEncoder().encodeToString(response.toByteArray()));
      }
      result.populatedFields = sortedDistinct(fields);
      result.responseSha256 = sha256(bytes.bytes());
      result.responseFramesBase64 = List.copyOf(responseFrames);
      List<String> missing = new ArrayList<>();
      for (String expected : record.expectedFields) {
        if (!result.populatedFields.contains(expected)) missing.add(expected);
      }
      if (!record.expectedFields.isEmpty() && !missing.isEmpty()) {
        result.status = "failed";
        result.grpcStatus = "OK";
        result.detail = "response missing expected fields: " + String.join(",", missing);
      } else if (record.expectedFields.isEmpty() && !allowEmpty) {
        result.status = "blocked_missing_expected_fields";
        result.grpcStatus = "OK";
        result.detail = "inventory record has no Rust-owned expected_fields assertion";
      } else {
        result.status = "passed";
        result.grpcStatus = "OK";
        result.detail = "live network invocation and response assertions passed";
      }
    } catch (MissingRequestException e) {
      result.status = "blocked_missing_request";
      result.grpcStatus = "NOT_ATTEMPTED";
      result.detail = e.getMessage();
    } catch (io.grpc.StatusRuntimeException e) {
      result.status = "failed";
      result.grpcStatus = e.getStatus().getCode().name();
      result.detail = e.getStatus().getDescription();
    } catch (Exception e) {
      result.status = "failed";
      result.grpcStatus = "CLIENT_ERROR";
      result.detail = e.getClass().getName() + ": " + String.valueOf(e.getMessage());
    } finally {
      result.durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }
    return result;
  }

  /* Dynamic client-streaming and bidi calls use one inventory request.  The
     fixture may require additional messages; those belong in the inventory as
     a future request_messages extension, while this keeps the first call real
     and observable rather than replacing it with an in-process test. */
  private static DynamicMessage clientStream(Channel channel,
      MethodDescriptor<DynamicMessage, DynamicMessage> method, DynamicMessage request,
      Descriptors.Descriptor outputType)
      throws InterruptedException {
    CountDownLatch done = new CountDownLatch(1);
    List<DynamicMessage> response = new ArrayList<>();
    io.grpc.stub.StreamObserver<DynamicMessage> observer = new io.grpc.stub.StreamObserver<>() {
      public void onNext(DynamicMessage value) { response.add(value); }
      public void onError(Throwable t) { done.countDown(); }
      public void onCompleted() { done.countDown(); }
    };
    io.grpc.stub.StreamObserver<DynamicMessage> requests = ClientCalls.asyncClientStreamingCall(
        channel.newCall(method, CallOptions.DEFAULT), observer);
    requests.onNext(request);
    requests.onCompleted();
    done.await(30, TimeUnit.SECONDS);
    return response.isEmpty() ? DynamicMessage.getDefaultInstance(outputType) : response.get(0);
  }

  private static DynamicMessage bidiStream(Channel channel,
      MethodDescriptor<DynamicMessage, DynamicMessage> method, DynamicMessage request,
      Descriptors.Descriptor outputType)
      throws InterruptedException {
    CountDownLatch done = new CountDownLatch(1);
    List<DynamicMessage> response = new ArrayList<>();
    io.grpc.stub.StreamObserver<DynamicMessage> observer = new io.grpc.stub.StreamObserver<>() {
      public void onNext(DynamicMessage value) { response.add(value); }
      public void onError(Throwable t) { done.countDown(); }
      public void onCompleted() { done.countDown(); }
    };
    io.grpc.stub.StreamObserver<DynamicMessage> requests = ClientCalls.asyncBidiStreamingCall(
        channel.newCall(method, CallOptions.DEFAULT), observer);
    requests.onNext(request);
    requests.onCompleted();
    done.await(30, TimeUnit.SECONDS);
    return response.isEmpty() ? DynamicMessage.getDefaultInstance(outputType) : response.get(0);
  }

  private static MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethod(
      Descriptors.MethodDescriptor method, String shape) {
    MethodDescriptor.MethodType type = switch (shape) {
      case "server" -> MethodDescriptor.MethodType.SERVER_STREAMING;
      case "client" -> MethodDescriptor.MethodType.CLIENT_STREAMING;
      case "bidi" -> MethodDescriptor.MethodType.BIDI_STREAMING;
      default -> MethodDescriptor.MethodType.UNARY;
    };
    return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
        .setType(type)
        .setFullMethodName(MethodDescriptor.generateFullMethodName(
            method.getService().getFullName(), method.getName()))
        .setRequestMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(method.getInputType())))
        .setResponseMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(method.getOutputType())))
        .build();
  }

  private static byte[] requestBytes(InventoryRecord record, Descriptors.Descriptor type,
      boolean allowEmpty) {
    String encoded = record.requestBase64 != null ? record.requestBase64 : record.requestJsonBase64;
    if (encoded == null || encoded.isBlank()) {
      if (!allowEmpty) throw new MissingRequestException("inventory record has no serialized request");
      return DynamicMessage.getDefaultInstance(type).toByteArray();
    }
    return Base64.getDecoder().decode(encoded);
  }

  private static List<Descriptors.FileDescriptor> loadDescriptors(Path root,
      Map<String, Object> manifest) throws Exception {
    Object familyValue = manifest.get("families");
    if (!(familyValue instanceof List<?> families)) throw new IllegalArgumentException("authority families missing");
    List<DescriptorProtos.FileDescriptorProto> protos = new ArrayList<>();
    for (Object familyValueItem : families) {
      Map<String, Object> family = asObject(familyValueItem);
      String descriptor = string(family.get("descriptor"));
      if (descriptor == null) continue;
      Path path = root.resolve(descriptor).normalize();
      if (!path.startsWith(root) || !Files.isRegularFile(path)) {
        throw new IOException("authority descriptor missing: " + path);
      }
      byte[] descriptorBytes = Files.readAllBytes(path);
      String expectedHash = firstString(family, "descriptor_sha256", "schema_descriptor_sha256");
      if (expectedHash != null && !expectedHash.equalsIgnoreCase(sha256(descriptorBytes)))
        throw new IOException("authority descriptor hash mismatch: " + descriptor);
      DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(descriptorBytes);
      protos.addAll(set.getFileList());
    }
    Map<String, Descriptors.FileDescriptor> built = new LinkedHashMap<>();
    // Authority exports intentionally omit protobuf's well-known descriptors;
    // seed the pool from the pinned protobuf runtime instead of introducing a
    // second checked-in copy of those contracts.
    built.put(DescriptorProtos.getDescriptor().getName(), DescriptorProtos.getDescriptor());
    built.put(Timestamp.getDescriptor().getFile().getName(), Timestamp.getDescriptor().getFile());
    boolean progress = true;
    while (!protos.isEmpty() && progress) {
      progress = false;
      for (int i = protos.size() - 1; i >= 0; i--) {
        DescriptorProtos.FileDescriptorProto proto = protos.get(i);
        if (built.containsKey(proto.getName())) { protos.remove(i); continue; }
        List<Descriptors.FileDescriptor> deps = new ArrayList<>();
        boolean ready = true;
        for (String dependency : proto.getDependencyList()) {
          Descriptors.FileDescriptor dep = built.get(dependency);
          if (dep == null) { ready = false; break; }
          deps.add(dep);
        }
        if (ready) {
          built.put(proto.getName(), Descriptors.FileDescriptor.buildFrom(proto,
              deps.toArray(new Descriptors.FileDescriptor[0])));
          protos.remove(i);
          progress = true;
        }
      }
    }
    if (!protos.isEmpty()) throw new IllegalStateException("unresolved descriptor dependencies: " + protos.get(0).getName());
    return new ArrayList<>(built.values());
  }

  private static Map<String, InventoryRecord> loadInventory(Path path) throws IOException {
    Object parsed = Json.parse(Files.readString(path));
    List<?> records;
    if (parsed instanceof List<?> list) records = list;
    else {
      Map<String, Object> object = asObject(parsed);
      Object values = object.get("rpcs");
      if (!(values instanceof List<?>)) values = object.get("scenarios");
      if (!(values instanceof List<?>)) values = object.get("requests");
      if (!(values instanceof List<?>) && object.get("fixtures") instanceof List<?> fixtures) {
        // sdk-examples' Rust-owned transport-fixtures manifest is accepted
        // directly. Request paths are resolved relative to that manifest.
        Map<String, InventoryRecord> fixtureRecords = new LinkedHashMap<>();
        for (Object fixtureValue : fixtures) {
          Map<String, Object> fixture = asObject(fixtureValue);
          String rpc = firstString(fixture, "operation_id", "rpc");
          Object requestsValue = fixture.get("requests");
          if (rpc == null || !(requestsValue instanceof List<?> requests) || requests.isEmpty()) continue;
          Map<String, Object> request = asObject(requests.get(0));
          InventoryRecord record = new InventoryRecord();
          record.rpc = rpc;
          record.requestPath = firstString(request, "path");
          Object expected = fixture.get("response_fields");
          if (!(expected instanceof List<?>)) expected = fixture.get("expected_fields");
          if (expected instanceof List<?> fields) {
            List<String> strings = new ArrayList<>();
            for (Object field : fields) if (field != null) strings.add(String.valueOf(field));
            record.expectedFields = List.copyOf(strings);
          }
          if (record.requestPath != null) {
            Path requestPath = path.getParent().resolve(record.requestPath).normalize();
            if (!requestPath.startsWith(path.getParent()) || !Files.isRegularFile(requestPath))
              throw new IOException("Rust fixture request missing: " + requestPath);
            record.requestBase64 = Base64.getEncoder().encodeToString(Files.readAllBytes(requestPath));
          }
          fixtureRecords.put(rpc, record);
        }
        return fixtureRecords;
      }
      records = values instanceof List<?> list ? list : List.of();
    }
    Map<String, InventoryRecord> result = new LinkedHashMap<>();
    for (Object value : records) {
      Map<String, Object> object = asObject(value);
      String rpc = firstString(object, "rpc", "method", "fullMethodName");
      if (rpc == null) continue;
      InventoryRecord record = new InventoryRecord();
      record.rpc = rpc;
      record.requestBase64 = firstString(object, "requestBase64", "request_bytes_base64", "payloadBase64");
      record.requestJsonBase64 = firstString(object, "requestJsonBase64");
      String scenario = firstString(object, "scenario", "name");
      if (scenario != null) record.scenario = scenario;
      Object expected = object.get("expectedFields");
      if (!(expected instanceof List<?>)) expected = object.get("expected_fields");
      if (expected instanceof List<?> fields) {
        List<String> strings = new ArrayList<>();
        for (Object field : fields) if (field != null) strings.add(String.valueOf(field));
        record.expectedFields = List.copyOf(strings);
      }
      result.put(rpc, record);
    }
    return result;
  }

  private static Result base(String rpc, String shape, InventoryRecord record) {
    Result r = new Result();
    r.rpc = rpc;
    r.shape = shape;
    r.expectedFields = record.expectedFields;
    r.status = "blocked";
    return r;
  }

  private static Map<String, Object> resultMap(Result r) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("rpc", r.rpc); m.put("shape", r.shape); m.put("scenario", "live-network");
    m.put("status", r.status); m.put("grpc_status", r.grpcStatus); m.put("detail", r.detail);
    m.put("duration_ms", r.durationMs); m.put("response_count", r.responseCount);
    m.put("populated_fields", r.populatedFields); m.put("expected_fields", r.expectedFields);
    m.put("request_sha256", r.requestSha256); m.put("response_sha256", r.responseSha256);
    m.put("request_bytes_base64", r.requestBytesBase64);
    m.put("response_frames_base64", r.responseFramesBase64);
    return m;
  }

  private static List<String> populatedFields(DynamicMessage message, String prefix) {
    List<String> fields = new ArrayList<>();
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      String name = prefix.isEmpty() ? entry.getKey().getName() : prefix + "." + entry.getKey().getName();
      fields.add(name);
      if (entry.getValue() instanceof DynamicMessage nested) fields.addAll(populatedFields(nested, name));
      if (entry.getValue() instanceof List<?> list) for (Object item : list)
        if (item instanceof DynamicMessage nested) fields.addAll(populatedFields(nested, name));
    }
    return fields;
  }

  private static List<String> sortedDistinct(List<String> input) {
    return input.stream().distinct().sorted().toList();
  }

  private static String value(String[] args, int index, String env, String fallback) {
    if (args.length > index && args[index] != null && !args[index].isBlank()) return args[index];
    return System.getenv().getOrDefault(env, fallback);
  }

  private static String firstString(Map<String, Object> object, String... keys) {
    for (String key : keys) { String value = string(object.get(key)); if (value != null) return value; }
    return null;
  }

  private static String string(Object value) { return value instanceof String s ? s : null; }
  @SuppressWarnings("unchecked") private static Map<String, Object> asObject(Object value) {
    if (!(value instanceof Map<?, ?>)) throw new IllegalArgumentException("expected JSON object");
    return (Map<String, Object>) value;
  }

  private static String sha256(byte[] bytes) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
      StringBuilder result = new StringBuilder(64);
      for (byte value : digest) result.append(String.format("%02x", value));
      return result.toString();
    } catch (Exception e) { throw new IllegalStateException(e); }
  }

  private static final class MessageBytes {
    private final java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
    void append(byte[] value) { output.writeBytes(value); }
    byte[] bytes() { return output.toByteArray(); }
  }

  private static final class MissingRequestException extends RuntimeException {
    MissingRequestException(String message) { super(message); }
  }

  /* Kept local so this acceptance source has no JSON runtime dependency. */
  private static final class Json {
    static Object parse(String source) { return new Parser(source).parse(); }
    static String stringify(Object value) {
      StringBuilder out = new StringBuilder(); write(out, value); return out.toString();
    }
    private static void write(StringBuilder out, Object value) {
      if (value == null) { out.append("null"); return; }
      if (value instanceof String s) { out.append('"'); for (char c : s.toCharArray()) {
        switch (c) { case '\\' -> out.append("\\\\"); case '"' -> out.append("\\\""); case '\n' -> out.append("\\n"); case '\r' -> out.append("\\r"); case '\t' -> out.append("\\t"); default -> out.append(c); }
      } out.append('"'); return; }
      if (value instanceof Number || value instanceof Boolean) { out.append(value); return; }
      if (value instanceof Map<?, ?> map) { out.append('{'); boolean first = true; for (Map.Entry<?, ?> e : map.entrySet()) {
        if (!first) out.append(','); first = false; write(out, String.valueOf(e.getKey())); out.append(':'); write(out, e.getValue()); }
        out.append('}'); return; }
      if (value instanceof Iterable<?> list) { out.append('['); boolean first = true; for (Object item : list) {
        if (!first) out.append(','); first = false; write(out, item); } out.append(']'); return; }
      write(out, String.valueOf(value));
    }
    private static final class Parser {
      final String s; int p;
      Parser(String s) { this.s = s; }
      Object parse() { skip(); Object v = value(); skip(); if (p != s.length()) throw error("trailing JSON"); return v; }
      Object value() { skip(); if (p >= s.length()) throw error("value expected"); char c = s.charAt(p);
        if (c == '{') return object(); if (c == '[') return array(); if (c == '"') return string();
        if (s.startsWith("true", p)) { p += 4; return true; } if (s.startsWith("false", p)) { p += 5; return false; }
        if (s.startsWith("null", p)) { p += 4; return null; } return number(); }
      Map<String, Object> object() { Map<String, Object> out = new LinkedHashMap<>(); p++; skip(); if (eat('}')) return out;
        while (true) { skip(); String key = string(); skip(); require(':'); out.put(key, value()); skip(); if (eat('}')) return out; require(','); } }
      List<Object> array() { List<Object> out = new ArrayList<>(); p++; skip(); if (eat(']')) return out;
        while (true) { out.add(value()); skip(); if (eat(']')) return out; require(','); } }
      String string() { require('"'); StringBuilder out = new StringBuilder(); while (p < s.length()) { char c = s.charAt(p++);
        if (c == '"') return out.toString(); if (c == '\\') { if (p >= s.length()) throw error("escape"); char e = s.charAt(p++);
          switch (e) { case '"' -> out.append('"'); case '\\' -> out.append('\\'); case '/' -> out.append('/'); case 'b' -> out.append('\b'); case 'f' -> out.append('\f'); case 'n' -> out.append('\n'); case 'r' -> out.append('\r'); case 't' -> out.append('\t'); case 'u' -> out.append((char) Integer.parseInt(s.substring(p, p + 4), 16)); default -> throw error("escape"); } if (e == 'u') p += 4;
        } else out.append(c); } throw error("string"); }
      Number number() { int start = p; while (p < s.length() && "-+0123456789.eE".indexOf(s.charAt(p)) >= 0) p++; String n = s.substring(start, p); try { return n.contains(".") || n.contains("e") || n.contains("E") ? Double.parseDouble(n) : Long.parseLong(n); } catch (NumberFormatException e) { throw error("number"); } }
      void skip() { while (p < s.length() && Character.isWhitespace(s.charAt(p))) p++; }
      boolean eat(char c) { if (p < s.length() && s.charAt(p) == c) { p++; return true; } return false; }
      void require(char c) { skip(); if (!eat(c)) throw error("expected " + c); }
      IllegalArgumentException error(String message) { return new IllegalArgumentException(message + " at JSON offset " + p); }
    }
  }

}
