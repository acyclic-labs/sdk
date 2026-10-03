package dev.acyclic.embedded;

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

  private interface NativeApi extends Library {
    int acyclic_embedded_abi_version();
    long acyclic_embedded_engine_open();
    void acyclic_embedded_engine_close(long engine);
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
    if (!Platform.isWindows() || !Platform.is64Bit()) {
      throw new IllegalStateException("embedded package currently carries only the win-x86_64 Rust library");
    }
    String resource = "/native/win-x86_64/acyclic_sdk_embedded_prototype.dll";
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
}





