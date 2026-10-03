using System;
using System.Runtime.InteropServices;
using System.Text;

internal static class NativeEmbedded
{
    internal const uint Ok = 0;
    internal const uint End = 1;
    internal const uint Cancelled = 3;
    internal const uint InvalidArgument = 4;

    [StructLayout(LayoutKind.Sequential)]
    internal struct Buffer
    {
        internal ulong Id;
        internal IntPtr Ptr;
        internal UIntPtr Len;
        internal UIntPtr Capacity;
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

    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern uint acyclic_embedded_abi_version();
    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern ulong acyclic_embedded_engine_open();
    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void acyclic_embedded_engine_close(ulong engine);
    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern AppendResult acyclic_embedded_engine_append(
        ulong engine, IntPtr path, UIntPtr pathLength, IntPtr value, UIntPtr valueLength);
    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern OpenResult acyclic_embedded_reader_open(
        ulong engine, IntPtr path, UIntPtr pathLength, ulong from, uint limit, uint mode);
    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NextResult acyclic_embedded_reader_next(ulong reader);
    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void acyclic_embedded_reader_cancel(ulong reader);
    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void acyclic_embedded_reader_close(ulong reader);
    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern uint acyclic_buffer_release(Buffer buffer);
    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void acyclic_append_result_release(AppendResult result);
    [DllImport("acyclic_sdk_embedded_prototype.dll", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void acyclic_next_result_release(NextResult result);

    internal static byte[] Read(Buffer buffer)
    {
        ulong length = buffer.Len.ToUInt64();
        if (buffer.Ptr == IntPtr.Zero || length == 0) return new byte[0];
        if (length > Int32.MaxValue) throw new InvalidOperationException("ABI buffer too large");
        byte[] bytes = new byte[(int)length];
        Marshal.Copy(buffer.Ptr, bytes, 0, bytes.Length);
        return bytes;
    }
}

internal static class Program
{
    private static void Check(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }

    private static byte[] Utf8(string value)
    {
        return Encoding.UTF8.GetBytes(value);
    }

    private static IntPtr Allocate(byte[] bytes)
    {
        IntPtr pointer = Marshal.AllocHGlobal(bytes.Length);
        Marshal.Copy(bytes, 0, pointer, bytes.Length);
        return pointer;
    }

    private static UIntPtr Size(byte[] bytes)
    {
        return new UIntPtr((ulong)bytes.Length);
    }

    private static bool Equal(byte[] left, byte[] right)
    {
        if (left.Length != right.Length) return false;
        for (int i = 0; i < left.Length; i++) if (left[i] != right[i]) return false;
        return true;
    }

    public static void Main()
    {
        Check(NativeEmbedded.acyclic_embedded_abi_version() == 1, "ABI version");
        ulong engine = NativeEmbedded.acyclic_embedded_engine_open();
        Check(engine != 0, "engine open");
        byte[] pathBytes = Utf8("dotnet/embedded");
        IntPtr path = Allocate(pathBytes);
        ulong reader = 0;
        try
        {
            byte[] firstBytes = Utf8("first");
            IntPtr first = Allocate(firstBytes);
            NativeEmbedded.AppendResult append = NativeEmbedded.acyclic_embedded_engine_append(
                engine, path, Size(pathBytes), first, Size(firstBytes));
            Marshal.FreeHGlobal(first);
            Check(append.Status == NativeEmbedded.Ok && append.Start == 0 && append.End == 1
                && append.Tail == 1, "first append");
            NativeEmbedded.acyclic_append_result_release(append);

            NativeEmbedded.OpenResult opened = NativeEmbedded.acyclic_embedded_reader_open(
                engine, path, Size(pathBytes), 0, 8, 0);
            Check(opened.Status == NativeEmbedded.Ok && opened.Reader != 0, "finite reader open");
            reader = opened.Reader;
            NativeEmbedded.NextResult record = NativeEmbedded.acyclic_embedded_reader_next(reader);
            Check(record.Status == NativeEmbedded.Ok && record.Sequence == 0
                && Equal(NativeEmbedded.Read(record.Value), Utf8("first")), "finite read");
            NativeEmbedded.acyclic_next_result_release(record);
            NativeEmbedded.NextResult end = NativeEmbedded.acyclic_embedded_reader_next(reader);
            Check(end.Status == NativeEmbedded.End, "finite end");
            NativeEmbedded.acyclic_next_result_release(end);
            NativeEmbedded.acyclic_embedded_reader_close(reader);
            reader = 0;

            byte[] secondBytes = Utf8("second");
            IntPtr second = Allocate(secondBytes);
            NativeEmbedded.AppendResult recovered = NativeEmbedded.acyclic_embedded_engine_append(
                engine, path, Size(pathBytes), second, Size(secondBytes));
            Marshal.FreeHGlobal(second);
            Check(recovered.Status == NativeEmbedded.Ok && recovered.Start == 1
                && recovered.End == 2 && recovered.Tail == 2, "recovery append");
            NativeEmbedded.acyclic_append_result_release(recovered);

            NativeEmbedded.OpenResult follow = NativeEmbedded.acyclic_embedded_reader_open(
                engine, path, Size(pathBytes), 2, 8, 1);
            Check(follow.Status == NativeEmbedded.Ok && follow.Reader != 0, "follow reader open");
            reader = follow.Reader;
            NativeEmbedded.acyclic_embedded_reader_cancel(reader);
            NativeEmbedded.NextResult cancelled = NativeEmbedded.acyclic_embedded_reader_next(reader);
            Check(cancelled.Status == NativeEmbedded.Cancelled, "follow cancellation");
            NativeEmbedded.acyclic_next_result_release(cancelled);
            NativeEmbedded.acyclic_embedded_reader_close(reader);
            reader = 0;

            NativeEmbedded.Buffer invalid = new NativeEmbedded.Buffer { Id = 42 };
            Check(NativeEmbedded.acyclic_buffer_release(invalid) == NativeEmbedded.InvalidArgument,
                "stale buffer rejection");
            Console.WriteLine(".NET Framework DllImport Rust embedded ABI checks passed: append/read/release/recovery/cancel");
        }
        finally
        {
            if (reader != 0) NativeEmbedded.acyclic_embedded_reader_close(reader);
            NativeEmbedded.acyclic_embedded_engine_close(engine);
            Marshal.FreeHGlobal(path);
        }
    }
}
