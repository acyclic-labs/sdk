using Google.Protobuf;
using Acyclic.Sdk.Transport;
using Acyclic.Actors.V1;
using Acyclic.Stream.V2;
using Grpc.Core;
using Grpc.Net.Client;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;

GoldenRoundTrip.Run();
FactoryDefaults.Run();
await Consumer.RunAsync();

sealed class GoldenFixture
{
    [JsonPropertyName("family")]
    public required string Family { get; init; }

    [JsonPropertyName("message")]
    public required string Message { get; init; }

    [JsonPropertyName("bytes_b64")]
    public required string BytesBase64 { get; init; }

    [JsonPropertyName("sha256")]
    public required string Sha256 { get; init; }
}

static class FactoryDefaults
{
    public static void Run()
    {
        var defaults = new GeneratedRemotePolicy.Defaults("http://127.0.0.1:1", "test-token");
        using var client = RemoteClientFactory.Create("actors", streaming: false, defaults, null);
        if (client.Transport != GeneratedRemotePolicy.Transport.Grpc)
            throw new InvalidOperationException($"Expected Rust policy to choose gRPC, got {client.Transport}.");
        if (client.Headers.GetValue("authorization") != "Bearer test-token")
            throw new InvalidOperationException("Factory did not carry the bearer token into call metadata.");
        try
        {
            _ = RemoteClientFactory.Create(
                "actors",
                streaming: false,
                new GeneratedRemotePolicy.Defaults("http://127.0.0.1:1", "bad\ntoken"),
                null);
            throw new InvalidOperationException("Invalid bearer token was accepted.");
        }
        catch (ArgumentException)
        {
            // Rust-emitted bearer validation rejects the call before any network operation.
        }
        Console.WriteLine("Rust-emitted transport policy factory defaults passed.");
    }
}

static class GoldenRoundTrip
{
    public static void Run()
    {
        var fixturePath = Environment.GetEnvironmentVariable("ACYCLIC_GOLDEN_FIXTURE")
            ?? Path.Combine(AppContext.BaseDirectory, "golden", "cross-language-family-fixtures.json");
        if (!File.Exists(fixturePath))
        {
            throw new FileNotFoundException("Rust all-family golden fixture was not staged with the installed consumer.", fixturePath);
        }

        using var document = JsonDocument.Parse(File.ReadAllText(fixturePath));
        var schema = document.RootElement.GetProperty("schema").GetString();
        if (schema != "acyclic.sdk.cross-language-fixtures.v1")
        {
            throw new InvalidOperationException($"Unexpected golden fixture schema: {schema}");
        }

        var fixtures = JsonSerializer.Deserialize<List<GoldenFixture>>(
            document.RootElement.GetProperty("families").GetRawText())
            ?? throw new InvalidOperationException("Golden fixture has no family entries.");
        if (fixtures.Count != 9)
        {
            throw new InvalidOperationException($"Expected 9 all-family golden entries, got {fixtures.Count}.");
        }

        foreach (var fixture in fixtures)
        {
            var bytes = Convert.FromBase64String(fixture.BytesBase64);
            var actualHash = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
            if (!string.Equals(actualHash, fixture.Sha256, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Golden hash mismatch for {fixture.Family}/{fixture.Message}.");
            }

            var roundTripped = fixture.Family switch
            {
                "actors" => Acyclic.Actors.V1.CreateActorRequest.Parser.ParseFrom(bytes).ToByteArray(),
                "stream" => Acyclic.Stream.V2.AppendRequest.Parser.ParseFrom(bytes).ToByteArray(),
                "objects" => Acyclic.Objects.V2.PutObjectHeader.Parser.ParseFrom(bytes).ToByteArray(),
                "workers" => Acyclic.Workers.V1.PublishVersionRequest.Parser.ParseFrom(bytes).ToByteArray(),
                "filesystem" => Acyclic.Filesystem.V2.HandshakeRequest.Parser.ParseFrom(bytes).ToByteArray(),
                "harness" => Acyclic.Harness.V2.CommandEnvelope.Parser.ParseFrom(bytes).ToByteArray(),
                "machines" => Acyclic.Machines.V1.CreateMachineRequest.Parser.ParseFrom(bytes).ToByteArray(),
                "inference" => Inference.Customer.V1.ListModelsRequest.Parser.ParseFrom(bytes).ToByteArray(),
                "protocol" => Acyclic.Protocol.V1.HandshakeRequest.Parser.ParseFrom(bytes).ToByteArray(),
                _ => throw new InvalidOperationException($"Unexpected golden family: {fixture.Family}")
            };
            if (!bytes.AsSpan().SequenceEqual(roundTripped))
            {
                throw new InvalidOperationException($"Golden serialization changed for {fixture.Family}/{fixture.Message}.");
            }
        }

        Console.WriteLine("Rust golden all-family serialization checks passed.");
    }
}

static class Consumer
{
    public static async Task RunAsync()
    {
var request = new CreateActorRequest
{
    CodeSha256 = ByteString.CopyFrom(new byte[32].Select((_, index) => (byte)(index + 1)).ToArray()),
    HomeRegion = "fixture",
    IdempotencyKey = "dotnet-consumer",
    Limits = new ActorLimits { MemoryBytes = ulong.MaxValue },
    Subscriptions =
    {
        new SubscriptionSpec
        {
            Start = new SubscriptionStart { Cursor = ulong.MaxValue }
        }
    }
};

if (request.Limits.MemoryBytes != ulong.MaxValue ||
    request.Subscriptions[0].Start.StartCase != SubscriptionStart.StartOneofCase.Cursor ||
    request.Subscriptions[0].Start.Cursor != ulong.MaxValue)
{
    throw new InvalidOperationException("Generated uint64 or oneof API did not preserve its value.");
}

// Use a fully valid actor request for the source-matched Rust fixture after the wire-shape checks.
request.Limits.HandlerTimeoutMillis = 1_000;
request.Limits.CheckpointBytes = 4_096;
request.Subscriptions[0].SubscriptionId = "fixture";
request.Subscriptions[0].StreamPath = "fixture/events";
request.Subscriptions[0].Start.Cursor = 0;

using var cancellation = new CancellationTokenSource();
cancellation.Cancel();
try
{
    await Task.Delay(Timeout.InfiniteTimeSpan, cancellation.Token);
    throw new InvalidOperationException("Cancellation token was not observed.");
}
catch (OperationCanceledException)
{
    var read = new ReadRequest { From = ulong.MaxValue };
    if (read.From != ulong.MaxValue)
    {
        throw new InvalidOperationException("Generated stream request did not preserve uint64.");
    }
}

var fixtureEndpoint = Environment.GetEnvironmentVariable("ACYCLIC_FIXTURE_ENDPOINT");
if (!string.IsNullOrWhiteSpace(fixtureEndpoint))
{
    using var channel = GrpcChannel.ForAddress(fixtureEndpoint);
    var actors = new ActorsService.ActorsServiceClient(channel);
    var actor = await actors.CreateActorAsync(request);
    if (actor is null)
    {
        throw new InvalidOperationException("Rust fixture did not return an Actors response.");
    }
    var stream = new StreamService.StreamServiceClient(channel);

    var recoveryKey = ByteString.CopyFromUtf8("dotnet-recovery-key-20261003a");
    var firstAppend = new AppendRequest
    {
        Path = "fixture/recovery-dotnet",
        IfTail = 0,
        IdempotencyKey = recoveryKey,
    };
    firstAppend.Records.Add(ByteString.CopyFromUtf8("first"));
    var committed = stream.Append(firstAppend);
    if (committed.OutcomeCase != AppendResponse.OutcomeOneofCase.Committed)
    {
        throw new InvalidOperationException("Rust fixture did not commit the first recovery append.");
    }
    var replay = stream.Append(firstAppend);
    if (!replay.Equals(committed))
    {
        throw new InvalidOperationException("Rust fixture idempotency replay changed the append receipt.");
    }
    var observed = stream.InspectIdempotency(new InspectIdempotencyRequest { IdempotencyKey = recoveryKey });
    if (observed.Observation is null ||
        observed.Observation.OutcomeCase != IdempotencyObservation.OutcomeOneofCase.Append ||
        !observed.Observation.Append.Equals(committed))
    {
        throw new InvalidOperationException("Rust fixture did not expose the committed idempotency observation.");
    }
    var mismatch = firstAppend.Clone();
    mismatch.Records.Clear();
    mismatch.Records.Add(ByteString.CopyFromUtf8("different"));
    try
    {
        _ = stream.Append(mismatch);
        throw new InvalidOperationException("Rust fixture accepted an idempotency-key mismatch.");
    }
    catch (RpcException error) when (error.StatusCode == StatusCode.FailedPrecondition &&
                                     error.Status.Detail == "idempotency_mismatch")
    {
        // The canonical fixture rejected reuse of a key with a different request.
    }

    using var streamCancellation = new CancellationTokenSource();
    using var read = stream.Read(
        new ReadRequest { Path = "fixture/recovery-dotnet", From = 0, Limit = 16 },
        cancellationToken: streamCancellation.Token);
    var received = false;
    try
    {
        await foreach (var response in read.ResponseStream.ReadAllAsync(streamCancellation.Token))
        {
            received = true;
            _ = response.Record?.Sequence;
            // Cancel immediately after the first record so this probe exercises
            // the generated stream cancellation path even on a fast fixture.
            streamCancellation.Cancel();
        }
    }
    catch (RpcException error) when (error.StatusCode == StatusCode.Cancelled)
    {
        if (!received) throw new InvalidOperationException("Rust fixture stream was cancelled before a record arrived.");
    }
    if (!received) throw new InvalidOperationException("Rust fixture did not return a streamed record.");

    using var followCancellation = new CancellationTokenSource();
    using var follow = stream.Follow(
        new FollowRequest { Path = "fixture/recovery-dotnet", From = 0 },
        cancellationToken: followCancellation.Token);
    if (!await follow.ResponseStream.MoveNext(followCancellation.Token) ||
        follow.ResponseStream.Current.Record?.Sequence != 0)
    {
        throw new InvalidOperationException("Rust fixture follow did not replay the first record.");
    }
    followCancellation.Cancel();
    try
    {
        while (await follow.ResponseStream.MoveNext(followCancellation.Token)) { }
    }
    catch (OperationCanceledException) { }
    catch (RpcException error) when (error.StatusCode == StatusCode.Cancelled) { }

    var secondAppend = firstAppend.Clone();
    secondAppend.Records.Clear();
    secondAppend.Records.Add(ByteString.CopyFromUtf8("second"));
    secondAppend.IfTail = 1;
    secondAppend.IdempotencyKey = ByteString.CopyFromUtf8("dotnet-recovery-key-20261003b");
    var second = stream.Append(secondAppend);
    if (second.OutcomeCase != AppendResponse.OutcomeOneofCase.Committed)
    {
        throw new InvalidOperationException("Rust fixture did not commit the resumed append.");
    }
    using var resumed = stream.Read(new ReadRequest { Path = "fixture/recovery-dotnet", From = 1, Limit = 1 });
    if (!await resumed.ResponseStream.MoveNext() || resumed.ResponseStream.Current.Record?.Sequence != 1)
    {
        throw new InvalidOperationException("Rust fixture resume boundary did not return sequence 1.");
    }
    Console.WriteLine("Rust fixture idempotency replay, mismatch, follow cancellation, and resume checks passed.");
    Console.WriteLine("Rust fixture unary, server-stream, and cancellation checks passed.");
}

Console.WriteLine("Generated Actors and Stream consumer checks passed.");
    }
}
