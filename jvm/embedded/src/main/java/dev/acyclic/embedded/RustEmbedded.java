package dev.acyclic.embedded;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Small installable facade over the Rust-generated C header.
 * The Rust ABI owns stream behavior; this class only maps calls, copies returned bytes,
 * and performs the required explicit releases.
 */
public final class RustEmbedded implements AutoCloseable {
  public static final int ABI_VERSION = 1;
  public static final int OK = 0;
  public static final int END = 1;
  public static final int PENDING = 2;
  public static final int CANCELLED = 3;
  public static final int INVALID_ARGUMENT = 4;
  public static final String[] CANONICAL_OPERATIONS = {
      "inspect_idempotency", "append", "tail", "fork", "read", "follow", "children",
      "children_page", "commit", "read_commit"
  };

  private interface NativeApi extends Library {
    int acyclic_embedded_abi_version();
    long acyclic_embedded_engine_open();
    void acyclic_embedded_engine_close(long engine);
    WireResult.ByValue acyclic_embedded_engine_wire_call(
        long engine, Pointer operation, long operationLength, Pointer request, long requestLength);
    AppendResult.ByValue acyclic_embedded_engine_append(
        long engine, Pointer path, long pathLength, Pointer value, long valueLength);
    OpenResult.ByValue acyclic_embedded_reader_open(
        long engine, Pointer path, long pathLength, long from, int limit, int mode);
    long acyclic_open_result_take_reader(Pointer result);
    NextResult.ByValue acyclic_embedded_reader_next(long reader);
    void acyclic_embedded_reader_cancel(long reader);
    void acyclic_embedded_reader_close(long reader);
    int acyclic_buffer_release(Buffer buffer);
    void acyclic_append_result_release(AppendResult.ByValue result);
    void acyclic_open_result_release(OpenResult.ByValue result);
    void acyclic_next_result_release(NextResult.ByValue result);
    void acyclic_wire_result_release(WireResult.ByValue result);
  }

  @Structure.FieldOrder({"id", "ptr", "len", "capacity"})
  public static class Buffer extends Structure {
    public long id;
    public Pointer ptr;
    public long len;
    public long capacity;
    public Buffer() {}
  }

  @Structure.FieldOrder({"status", "start", "end", "tail", "message"})
  public static class AppendResult extends Structure {
    public int status;
    public long start;
    public long end;
    public long tail;
    public Buffer message = new Buffer();
    public AppendResult() {}
    public static class ByValue extends AppendResult implements Structure.ByValue {}
  }

  @Structure.FieldOrder({"status", "reader", "message"})
  public static class OpenResult extends Structure {
    public int status;
    public long reader;
    public Buffer message = new Buffer();
    public OpenResult() {}
    public static class ByValue extends OpenResult implements Structure.ByValue {}
  }

  @Structure.FieldOrder({"status", "sequence", "value", "message"})
  public static class NextResult extends Structure {
    public int status;
    public long sequence;
    public Buffer value = new Buffer();
    public Buffer message = new Buffer();
    public NextResult() {}
    public static class ByValue extends NextResult implements Structure.ByValue {}
  }

  @Structure.FieldOrder({"status", "response", "message"})
  public static class WireResult extends Structure {
    public int status;
    public Buffer response = new Buffer();
    public Buffer message = new Buffer();
    public WireResult() {}
    public static class ByValue extends WireResult implements Structure.ByValue {}
  }

  public record Append(int status, long start, long end, long tail, byte[] message) {}
  public record Item(int status, long sequence, byte[] value, byte[] message) {}

  private final NativeApi api;
  private final long engine;
  private boolean closed;

  public RustEmbedded() {
    this(loadNative());
  }

  RustEmbedded(NativeApi api) {
    this.api = api;
    if (api.acyclic_embedded_abi_version() != ABI_VERSION) {
      throw new IllegalStateException("unsupported Rust embedded ABI version");
    }
    this.engine = api.acyclic_embedded_engine_open();
    if (engine == 0) throw new IllegalStateException("Rust embedded engine open failed");
  }

  public int abiVersion() {
    return api.acyclic_embedded_abi_version();
  }

  /** Executes one unary generated acyclic.stream.v2 operation through Rust. */
  public byte[] callWire(String operation, byte[] request) {
    ensureOpen();
    try (Memory operationMemory = bytes(operation.getBytes(StandardCharsets.UTF_8));
         Memory requestMemory = bytes(request)) {
      WireResult.ByValue result = api.acyclic_embedded_engine_wire_call(
          engine, operationMemory, operationMemory.size(), requestMemory, requestMemory.size());
      try {
        if (result.status != OK) {
          throw new IllegalStateException("Rust wire operation failed: " + result.status + " "
              + new String(copy(result.message), StandardCharsets.UTF_8));
        }
        return copy(result.response);
      } finally {
        api.acyclic_wire_result_release(result);
      }
    }
  }

  /** Encodes and decodes generated protobuf messages while Rust owns execution and validation. */
  public <T extends Message> T callWire(String operation, Message request, Parser<T> parser) {
    try {
      return parser.parseFrom(callWire(operation, request.toByteArray()));
    } catch (java.io.IOException error) {
      throw new IllegalStateException("Rust wire response could not be decoded", error);
    }
  }

  // Named methods are the generated operation surface. Their bytes are the matching
  // acyclic.stream.v2 protobuf request/response messages; Rust remains the implementation
  // authority and this facade only owns the native byte lifetime.
  public byte[] inspectIdempotency(byte[] request) { return callWire("inspect_idempotency", request); }
  public byte[] appendWire(byte[] request) { return callWire("append", request); }
  public byte[] tail(byte[] request) { return callWire("tail", request); }
  public byte[] fork(byte[] request) { return callWire("fork", request); }
  public byte[] childrenPage(byte[] request) { return callWire("children_page", request); }
  public byte[] commit(byte[] request) { return callWire("commit", request); }
  public byte[] readCommit(byte[] request) { return callWire("read_commit", request); }
  public <T extends Message> T inspectIdempotency(Message request, Parser<T> parser) { return callWire("inspect_idempotency", request, parser); }
  public <T extends Message> T appendWire(Message request, Parser<T> parser) { return callWire("append", request, parser); }
  public <T extends Message> T tail(Message request, Parser<T> parser) { return callWire("tail", request, parser); }
  public <T extends Message> T fork(Message request, Parser<T> parser) { return callWire("fork", request, parser); }
  public <T extends Message> T childrenPage(Message request, Parser<T> parser) { return callWire("children_page", request, parser); }
  public <T extends Message> T commit(Message request, Parser<T> parser) { return callWire("commit", request, parser); }
  public <T extends Message> T readCommit(Message request, Parser<T> parser) { return callWire("read_commit", request, parser); }

  public Append append(String path, byte[] value) {
    ensureOpen();
    try (Memory pathMemory = bytes(path); Memory valueMemory = bytes(value)) {
      AppendResult.ByValue result = api.acyclic_embedded_engine_append(
          engine, pathMemory, pathMemory.size(), valueMemory, valueMemory.size());
      try {
        return new Append(result.status, result.start, result.end, result.tail,
            copy(result.message));
      } finally {
        api.acyclic_append_result_release(result);
      }
    }
  }

  public Reader openReader(String path, long from, int limit, boolean live) {
    ensureOpen();
    try (Memory pathMemory = bytes(path)) {
      OpenResult.ByValue result = api.acyclic_embedded_reader_open(
          engine, pathMemory, pathMemory.size(), from, limit, live ? 1 : 0);
      if (result.status != OK) {
        try {
          throw new IllegalStateException("reader open failed: " + result.status + " "
              + new String(copy(result.message), StandardCharsets.UTF_8));
        } finally {
          api.acyclic_open_result_release(result);
        }
      }
      result.write();
      long reader = api.acyclic_open_result_take_reader(result.getPointer());
      result.read();
      api.acyclic_open_result_release(result);
      if (reader == 0) throw new IllegalStateException("Rust reader handle missing");
      return new Reader(reader);
    }
  }

  public final class Reader implements AutoCloseable {
    private long handle;
    private Reader(long handle) { this.handle = handle; }

    public Item next() {
      if (handle == 0) throw new IllegalStateException("reader closed");
      NextResult.ByValue result = api.acyclic_embedded_reader_next(handle);
      try {
        return new Item(result.status, result.sequence, copy(result.value), copy(result.message));
      } finally {
        api.acyclic_next_result_release(result);
      }
    }

    public void cancel() {
      if (handle != 0) api.acyclic_embedded_reader_cancel(handle);
    }

    @Override
    public void close() {
      if (handle != 0) {
        api.acyclic_embedded_reader_close(handle);
        handle = 0;
      }
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      api.acyclic_embedded_engine_close(engine);
    }
  }

  private void ensureOpen() {
    if (closed) throw new IllegalStateException("engine closed");
  }

  private static Memory bytes(String value) {
    return bytes(value.getBytes(StandardCharsets.UTF_8));
  }

  private static Memory bytes(byte[] value) {
    Memory memory = new Memory(value.length);
    memory.write(0, value, 0, value.length);
    return memory;
  }

  private static byte[] copy(Buffer buffer) {
    if (buffer == null || buffer.ptr == null || buffer.len == 0) return new byte[0];
    if (buffer.len > Integer.MAX_VALUE) throw new IllegalStateException("ABI buffer too large");
    return buffer.ptr.getByteArray(0, (int) buffer.len);
  }

  private static NativeApi loadNative() {
    String configured = System.getProperty("acyclic.embedded.native.path");
    if (configured != null && !configured.isBlank()) {
      return Native.load(configured, NativeApi.class);
    }
    String resource = nativeResource();
    try (InputStream input = RustEmbedded.class.getResourceAsStream(resource)) {
      if (input == null) throw new IllegalStateException("native resource missing: " + resource);
      Path extracted = Files.createTempFile("acyclic-sdk-embedded-", ".dll");
      Files.copy(input, extracted, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      extracted.toFile().deleteOnExit();
      return Native.load(extracted.toAbsolutePath().toString(), NativeApi.class);
    } catch (IOException error) {
      throw new IllegalStateException("native resource extraction failed", error);
    }
  }

  private static String nativeResource() {
    String rid;
    String file;
    boolean arm = Platform.isARM();
    if (Platform.isWindows()) {
      rid = arm ? "win-aarch64" : "win-x86_64";
      file = "acyclic_sdk_embedded_prototype.dll";
    } else if (Platform.isLinux()) {
      rid = arm ? "linux-aarch64-gnu" : "linux-x86_64-gnu";
      file = "libacyclic_sdk_embedded_prototype.so";
    } else if (Platform.isMac()) {
      rid = arm ? "osx-aarch64" : "osx-x86_64";
      file = "libacyclic_sdk_embedded_prototype.dylib";
    } else {
      throw new IllegalStateException("unsupported platform for the Rust embedded ABI");
    }
    return "/native/" + rid + "/" + file;
  }
}





