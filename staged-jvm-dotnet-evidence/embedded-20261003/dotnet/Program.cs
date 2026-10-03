using System.Runtime.InteropServices;
using System.Text;

internal static class Native
{
    private const string Library = "acyclic_sdk_embedded_prototype";

    [StructLayout(LayoutKind.Sequential)]
    internal struct Buffer
    {
        internal ulong Id;
        internal IntPtr Ptr;
        internal nuint Len;
        internal nuint Capacity;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct AppendResult
    {
        internal uint Status;
        internal ulong Start;
        internal ulong End;
        internal ulong Tail;
        internal Buffer Message;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct OpenResult
    {
        internal uint Status;
        internal ulong Reader;
        internal Buffer Message;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct NextResult
    {
        internal uint Status;
        internal ulong Sequence;
        internal Buffer Value;
        internal Buffer Message;
    }

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern uint acyclic_embedded_abi_version();
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern ulong acyclic_embedded_engine_open();
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void acyclic_embedded_engine_close(ulong engine);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern AppendResult acyclic_embedded_engine_append(
        ulong engine, IntPtr path, nuint pathLength, IntPtr value, nuint valueLength);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern OpenResult acyclic_embedded_reader_open(
        ulong engine, IntPtr path, nuint pathLength, ulong from, uint limit, uint mode);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NextResult acyclic_embedded_reader_next(ulong reader);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void acyclic_embedded_reader_cancel(ulong reader);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void acyclic_embedded_reader_close(ulong reader);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern uint acyclic_buffer_release(Buffer buffer);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void acyclic_append_result_release(AppendResult result);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void acyclic_next_result_release(NextResult result);

    internal static byte[] Read(Buffer buffer)
    {
        if (buffer.Ptr == IntPtr.Zero || buffer.Len == 0) return [];
        if (buffer.Len > int.MaxValue) throw new InvalidOperationException("test buffer too large");
        var bytes = new byte[(int)buffer.Len];
        Marshal.Copy(buffer.Ptr, bytes, 0, bytes.Length);
        return bytes;
    }
}

internal static class Program
{
    private const uint Ok = 0;
    private const uint End = 1;
    private const uint Cancelled = 3;
    private const uint InvalidArgument = 4;

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }

    private static (IntPtr Pointer, nuint Length) NativeUtf8(string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        IntPtr pointer = Marshal.AllocHGlobal(bytes.Length);
        Marshal.Copy(bytes, 0, pointer, bytes.Length);
        return (pointer, (nuint)bytes.Length);
    }

    public static void Main()
    {
        Check(Native.acyclic_embedded_abi_version() == 1, "ABI version");
        ulong engine = Native.acyclic_embedded_engine_open();
        Check(engine != 0, "engine open");
        var path = NativeUtf8("dotnet/recovery");
        ulong reader = 0;
        try
        {
            var firstValue = NativeUtf8("first");
            var first = Native.acyclic_embedded_engine_append(
                engine, path.Pointer, path.Length, firstValue.Pointer, firstValue.Length);
            Marshal.FreeHGlobal(firstValue.Pointer);
            Check(first.Status == Ok && first.Start == 0 && first.End == 1 && first.Tail == 1,
                "first append");
            Native.acyclic_append_result_release(first);

            var opened = Native.acyclic_embedded_reader_open(
                engine, path.Pointer, path.Length, 0, 8, 0);
            Check(opened.Status == Ok && opened.Reader != 0, "finite reader open");
            reader = opened.Reader;
            var record = Native.acyclic_embedded_reader_next(reader);
            Check(record.Status == Ok && record.Sequence == 0 &&
                  Native.Read(record.Value).AsSpan().SequenceEqual("first"u8), "finite read");
            Native.acyclic_next_result_release(record);
            var end = Native.acyclic_embedded_reader_next(reader);
            Check(end.Status == End, "finite end");
            Native.acyclic_next_result_release(end);
            Native.acyclic_embedded_reader_close(reader);
            reader = 0;

            var secondValue = NativeUtf8("second");
            var second = Native.acyclic_embedded_engine_append(
                engine, path.Pointer, path.Length, secondValue.Pointer, secondValue.Length);
            Marshal.FreeHGlobal(secondValue.Pointer);
            Check(second.Status == Ok && second.Start == 1 && second.End == 2 && second.Tail == 2,
                "recovery append");
            Native.acyclic_append_result_release(second);

            var follow = Native.acyclic_embedded_reader_open(
                engine, path.Pointer, path.Length, 2, 8, 1);
            Check(follow.Status == Ok && follow.Reader != 0, "follow reader open");
            reader = follow.Reader;
            Native.acyclic_embedded_reader_cancel(reader);
            var cancelled = Native.acyclic_embedded_reader_next(reader);
            Check(cancelled.Status == Cancelled, "follow cancellation");
            Native.acyclic_next_result_release(cancelled);
            Native.acyclic_embedded_reader_close(reader);
            reader = 0;

            var invalid = new Native.Buffer { Id = 42 };
            Check(Native.acyclic_buffer_release(invalid) == InvalidArgument, "stale buffer rejection");
            Console.WriteLine(".NET P/Invoke Rust embedded ABI checks passed: append/read/release/recovery/cancel");
        }
        finally
        {
            if (reader != 0) Native.acyclic_embedded_reader_close(reader);
            Native.acyclic_embedded_engine_close(engine);
            Marshal.FreeHGlobal(path.Pointer);
        }
    }
}
