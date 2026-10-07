using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Actors.StreamBinding;

static class Program
{
    static async Task Main(string[] args)
    {
        if (args.Length == 0)
            await RunLocal();
        else
            await RunRemote(args);
    }

    static async Task RunLocal()
    {
        using var client = new StreamClientBridge();
        await client.Append("events", new byte[] { 1, 2, 3 });
        await client.Append("events", new byte[] { 4, 5, 6 });
        await client.Append("agents/a/log", new byte[] { 9 });

        var records = await Collect(client.ReadStream("events", 0UL, 10U));
        if (records.Count != 2 || records[0].Sequence != 0UL || records[1].Sequence != 1UL)
            throw new Exception("local ordered records mismatch");
        if (records.Any(r => r.CommittedAtMicros == 0UL || r.CommitId.Length != 32))
            throw new Exception("local typed record metadata mismatch");

        var children = new List<StreamChild>();
        await foreach (var child in client.ChildrenStream(null, 10U))
            children.Add(child);
        if (!children.Select(c => c.Path).SequenceEqual(new[] { "agents", "events" }))
            throw new Exception("local children mismatch");

        // Exercise the complete ulong input boundary through generated FFI.
        await AssertFullWidthBoundary(client.ReadStream("events", ulong.MaxValue, 1U));
        await AssertFullWidthBoundary(client.ReadStream("events", 0x8000000000000000UL, 1U));

        using var cts = new CancellationTokenSource();
        await using var enumerator = client.FollowStream("events", 2UL, cts.Token)
            .GetAsyncEnumerator(cts.Token);
        var pending = enumerator.MoveNextAsync().AsTask();
        for (var i = 0; i < 100 && client.ActiveRecordCursors() == 0UL; i++)
            await Task.Delay(10);
        if (client.ActiveRecordCursors() != 1UL)
            throw new Exception("local cursor did not become active");
        cts.Cancel();
        var cancelled = false;
        try { await pending; }
        catch (OperationCanceledException) { cancelled = true; }
        if (!cancelled)
            throw new Exception("local cancellation did not preserve OperationCanceledException");
        for (var i = 0; i < 100 && client.ActiveRecordCursors() != 0UL; i++)
            await Task.Delay(10);
        if (client.ActiveRecordCursors() != 0UL)
            throw new Exception("local Rust cursor was not closed after cancellation");

        var recovered = await Collect(client.ReadStream("events", 0UL, 10U));
        if (recovered.Count != 2)
            throw new Exception("local recovery read failed");

        Console.WriteLine("CSHARP_STREAM_LOCAL_ORDERED_TYPED_U64_PASS");
        Console.WriteLine("CSHARP_STREAM_LOCAL_CANCELLATION_CLOSE_RECOVERY_PASS");
    }

    static async Task RunRemote(string[] args)
    {
        if (args.Length < 2)
            throw new ArgumentException("remote mode requires endpoint and CA path, optional marker directory");
        var endpoint = args[0];
        var ca = await File.ReadAllBytesAsync(args[1]);
        var markerDir = args.Length >= 3 ? args[2] : null;
        using var client = await RemoteStreamClientBridge.Connect(endpoint, "fixture-token", ca);
        await client.Append("events", new byte[] { 1, 2, 3 });
        await client.Append("events", new byte[] { 4, 5, 6 });
        await client.Append("agents/a/log", new byte[] { 9 });

        var records = await Collect(client.ReadRemoteStream("events", 0UL, 10U));
        if (records.Count != 2 || records[0].Sequence != 0UL || records[1].Sequence != 1UL)
            throw new Exception("remote ordered records mismatch");
        if (records.Any(r => r.CommittedAtMicros == 0UL || r.CommitId.Length != 32))
            throw new Exception("remote typed record metadata mismatch");
        var children = new List<StreamChild>();
        await foreach (var child in client.ChildrenRemoteStream(null, 10U))
            children.Add(child);
        if (!children.Select(c => c.Path).SequenceEqual(new[] { "agents", "events" }))
            throw new Exception("remote children mismatch");

        await AssertFullWidthBoundary(client.ReadRemoteStream("events", ulong.MaxValue, 1U));
        await AssertFullWidthBoundary(client.ReadRemoteStream("events", 0x8000000000000000UL, 1U));

        using var cts = new CancellationTokenSource();
        await using var enumerator = client.FollowRemoteStream("events", 2UL, cts.Token)
            .GetAsyncEnumerator(cts.Token);
        var pending = enumerator.MoveNextAsync().AsTask();
        for (var i = 0; i < 100 && client.ActiveRecordCursors() == 0UL; i++)
            await Task.Delay(10);
        if (client.ActiveRecordCursors() != 1UL)
            throw new Exception("remote cursor did not become active");
        if (markerDir is not null)
            await WaitFor(Path.Combine(markerDir, "follow-open"));
        cts.Cancel();
        var cancelled = false;
        try { await pending; }
        catch (OperationCanceledException) { cancelled = true; }
        if (!cancelled)
            throw new Exception("remote cancellation did not preserve OperationCanceledException");
        for (var i = 0; i < 100 && client.ActiveRecordCursors() != 0UL; i++)
            await Task.Delay(10);
        if (client.ActiveRecordCursors() != 0UL)
            throw new Exception("remote Rust cursor was not closed after cancellation");
        if (markerDir is not null)
            await WaitFor(Path.Combine(markerDir, "follow-closed"));

        var recovered = await Collect(client.ReadRemoteStream("events", 0UL, 10U));
        if (recovered.Count != 2)
            throw new Exception("remote recovery read failed");

        Console.WriteLine("CSHARP_STREAM_REMOTE_TLS_ORDERED_TYPED_U64_PASS");
        Console.WriteLine("CSHARP_STREAM_REMOTE_CANCELLATION_SERVER_CLOSE_RECOVERY_PASS");
    }

    static async Task<List<StreamRecord>> Collect(IAsyncEnumerable<StreamRecord> stream)
    {
        var records = new List<StreamRecord>();
        await foreach (var record in stream)
            records.Add(record);
        return records;
    }

    static async Task AssertFullWidthBoundary(IAsyncEnumerable<StreamRecord> stream)
    {
        try
        {
            var records = await Collect(stream);
            if (records.Count != 0)
                throw new Exception("unexpected record at full-width boundary");
        }
        catch (StreamBridgeException.Stream error) when (error.@code == "out_of_range")
        {
            // The Rust provider saw the complete ulong and rejected it by its
            // range contract; truncation would instead address a low sequence.
        }
    }

    static async Task WaitFor(string path)
    {
        for (var i = 0; i < 200 && !File.Exists(path); i++)
            await Task.Delay(10);
        if (!File.Exists(path))
            throw new Exception($"marker did not appear: {path}");
    }
}
