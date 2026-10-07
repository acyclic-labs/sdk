using System.Runtime.InteropServices;
using Actors.ArchitectureProbe.Native;
using Microsoft.Win32.SafeHandles;
using NativeOptionalU64 = Actors.ArchitectureProbe.Native.OptionalU64;

NativeLibrary.SetDllImportResolver(
    typeof(NativeMethodsMarker).Assembly,
    static (name, _, _) => name == "dotnet_csbindgen_architecture_probe"
        ? NativeLibrary.Load(Path.Combine(AppContext.BaseDirectory, "dotnet_csbindgen_architecture_probe.dll"))
        : IntPtr.Zero);

unsafe
{
    using var token = ActorToken.Create(ulong.MaxValue);
    if (token.Value != ulong.MaxValue)
        throw new InvalidOperationException("u64 value was narrowed");
    if (!token.Optional.HasValue || token.Optional.Value != ulong.MaxValue)
        throw new InvalidOperationException("optional presence/value was changed");

    var rejected = false;
    try { using var _ = ActorToken.Create(0); }
    catch (ArgumentOutOfRangeException) { rejected = true; }
    if (!rejected) throw new InvalidOperationException("Rust accepted invalid zero");

    Console.WriteLine("generated-declarations=used");
    Console.WriteLine("safehandle-release=forwarded");
    Console.WriteLine("u64-max-preserved=true");
    Console.WriteLine("optional-presence=explicit");
    Console.WriteLine("zero-rejected=true");
}

internal sealed unsafe class ActorTokenHandle : SafeHandle
{
    internal ActorTokenHandle(nint value) : base(IntPtr.Zero, ownsHandle: true) => SetHandle(value);

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        NativeMethods.actor_token_release((ActorTokenOpaque*)handle);
        return true;
    }
}

public sealed unsafe class ActorToken : IDisposable
{
    private readonly ActorTokenHandle handle;

    private ActorToken(ActorTokenHandle handle) => this.handle = handle;

    public static ActorToken Create(ulong value)
    {
        ActorTokenOpaque* raw = null;
        var status = NativeMethods.actor_token_new(value, &raw);
        if (status != 0 || raw is null)
            throw new ArgumentOutOfRangeException(nameof(value), value, $"Rust status {status}");
        return new ActorToken(new ActorTokenHandle((nint)raw));
    }

    public ulong Value
    {
        get
        {
            ObjectDisposedException.ThrowIf(handle.IsClosed, this);
            ulong value = 0;
            var status = NativeMethods.actor_token_value((ActorTokenOpaque*)handle.DangerousGetHandle(), &value);
            if (status != 0) throw new InvalidOperationException($"Rust status {status}");
            return value;
        }
    }

    public OptionalU64 Optional
    {
        get
        {
            ObjectDisposedException.ThrowIf(handle.IsClosed, this);
            NativeOptionalU64 native = default;
            var status = NativeMethods.actor_token_optional_value((ActorTokenOpaque*)handle.DangerousGetHandle(), &native);
            if (status != 0) throw new InvalidOperationException($"Rust status {status}");
            return new OptionalU64(native.present, native.value);
        }
    }

    public void Dispose() => handle.Dispose();
}

public readonly struct OptionalU64
{
    private readonly byte present;
    private readonly ulong value;

    internal OptionalU64(byte present, ulong value)
    {
        this.present = present;
        this.value = value;
    }

    public bool HasValue => present != 0;
    public ulong Value => HasValue ? value : throw new InvalidOperationException("Value absent");
}

internal static class NativeMethodsMarker { }
