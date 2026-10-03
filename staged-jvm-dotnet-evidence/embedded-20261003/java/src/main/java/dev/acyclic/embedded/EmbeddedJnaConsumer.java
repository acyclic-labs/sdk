package dev.acyclic.embedded;

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Bounded JNA consumer of the Rust-owned C ABI. It does not reimplement stream behavior. */
public final class EmbeddedJnaConsumer {
  private static final int OK = 0;
  private static final int END = 1;
  private static final int CANCELLED = 3;
  private static final int INVALID_ARGUMENT = 4;

  interface NativeApi extends Library {
    int acyclic_embedded_abi_version();
    long acyclic_embedded_engine_open();
    void acyclic_embedded_engine_close(long engine);
    AppendResult.ByValue acyclic_embedded_engine_append(
        long engine, Pointer path, long pathLength, Pointer value, long valueLength);
    OpenResult.ByValue acyclic_embedded_reader_open(
        long engine, Pointer path, long pathLength, long from, int limit, int mode);
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
    public Buffer(Pointer memory) { super(memory); read(); }
  }

  @Structure.FieldOrder({"status", "start", "end", "tail", "message"})
  public static class AppendResult extends Structure {
    public int status;
    public long start;
    public long end;
    public long tail;
    public Buffer message;

    public AppendResult() { message = new Buffer(); }
    public static class ByValue extends AppendResult implements Structure.ByValue {}
  }

  @Structure.FieldOrder({"status", "reader", "message"})
  public static class OpenResult extends Structure {
    public int status;
    public long reader;
    public Buffer message;

    public OpenResult() { message = new Buffer(); }
    public static class ByValue extends OpenResult implements Structure.ByValue {}
  }

  @Structure.FieldOrder({"status", "sequence", "value", "message"})
  public static class NextResult extends Structure {
    public int status;
    public long sequence;
    public Buffer value;
    public Buffer message;

    public NextResult() { value = new Buffer(); message = new Buffer(); }
    public static class ByValue extends NextResult implements Structure.ByValue {}
  }

  private static Memory bytes(String value) {
    byte[] data = value.getBytes(StandardCharsets.UTF_8);
    Memory memory = new Memory(data.length);
    memory.write(0, data, 0, data.length);
    return memory;
  }

  private static byte[] read(Buffer buffer) {
    if (buffer.ptr == null || buffer.len == 0) return new byte[0];
    if (buffer.len > Integer.MAX_VALUE) throw new AssertionError("test buffer too large");
    return buffer.ptr.getByteArray(0, (int) buffer.len);
  }

  private static void check(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }

  public static void main(String[] args) {
    NativeApi api = Native.load("acyclic_sdk_embedded_prototype", NativeApi.class);
    check(api.acyclic_embedded_abi_version() == 1, "ABI version");
    long engine = api.acyclic_embedded_engine_open();
    check(engine != 0, "engine open");
    Memory path = bytes("jna/recovery");
    long reader = 0;
    try {
      Memory firstValue = bytes("first");
      AppendResult.ByValue first = api.acyclic_embedded_engine_append(
          engine, path, path.size(), firstValue, firstValue.size());
      check(first.status == OK && first.start == 0 && first.end == 1 && first.tail == 1,
          "first append");
      api.acyclic_append_result_release(first);

      OpenResult.ByValue opened = api.acyclic_embedded_reader_open(
          engine, path, path.size(), 0, 8, 0);
      check(opened.status == OK && opened.reader != 0, "finite reader open");
      reader = opened.reader;
      NextResult.ByValue record = api.acyclic_embedded_reader_next(reader);
      check(record.status == OK && record.sequence == 0
          && Arrays.equals(read(record.value), "first".getBytes(StandardCharsets.UTF_8)),
          "finite read");
      api.acyclic_next_result_release(record);
      NextResult.ByValue end = api.acyclic_embedded_reader_next(reader);
      check(end.status == END, "finite end");
      api.acyclic_next_result_release(end);
      api.acyclic_embedded_reader_close(reader);
      reader = 0;

      Memory secondValue = bytes("second");
      AppendResult.ByValue second = api.acyclic_embedded_engine_append(
          engine, path, path.size(), secondValue, secondValue.size());
      check(second.status == OK && second.start == 1 && second.end == 2 && second.tail == 2,
          "recovery append");
      api.acyclic_append_result_release(second);

      OpenResult.ByValue follow = api.acyclic_embedded_reader_open(
          engine, path, path.size(), 2, 8, 1);
      check(follow.status == OK && follow.reader != 0, "follow reader open");
      reader = follow.reader;
      api.acyclic_embedded_reader_cancel(reader);
      NextResult.ByValue cancelled = api.acyclic_embedded_reader_next(reader);
      check(cancelled.status == CANCELLED, "follow cancellation");
      api.acyclic_next_result_release(cancelled);
      api.acyclic_embedded_reader_close(reader);
      reader = 0;

      Buffer invalid = new Buffer();
      invalid.id = 42;
      check(api.acyclic_buffer_release(invalid) == INVALID_ARGUMENT, "stale buffer rejection");
      System.out.println("JNA Rust embedded ABI checks passed: append/read/release/recovery/cancel");
    } finally {
      if (reader != 0) api.acyclic_embedded_reader_close(reader);
      api.acyclic_embedded_engine_close(engine);
      path.close();
    }
  }
}
