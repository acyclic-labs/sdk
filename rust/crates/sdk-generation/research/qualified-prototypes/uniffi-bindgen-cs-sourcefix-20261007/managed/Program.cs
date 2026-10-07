using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Acyclic.Actors;

internal static class Program
{
    private const string Endpoint = "https://localhost:60191";
    private const string Token = "conformance";
    private const string Ca = "-----BEGIN CERTIFICATE-----\r\nMIIBXjCCAQSgAwIBAgIUKoUSzsmAIWvzaFeOt5MUxrmiI1IwCgYIKoZIzj0EAwIw\r\nITEfMB0GA1UEAwwWcmNnZW4gc2VsZiBzaWduZWQgY2VydDAgFw03NTAxMDEwMDAw\r\nMDBaGA80MDk2MDEwMTAwMDAwMFowITEfMB0GA1UEAwwWcmNnZW4gc2VsZiBzaWdu\r\nZWQgY2VydDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABGunk4tsQi+7eMld4XyR\r\nFTph1dcRlUpaTIayV9xYz5JWvnyR5/3GdwbRIRX3MeGAig4MQKhRoX3xmeqU6ITQ\r\nUqSjGDAWMBQGA1UdEQQNMAuCCWxvY2FsaG9zdDAKBggqhkjOPQQDAgNIADBFAiEA\r\nq3qjS3fHyhOfuWaBex4D17Zwm+yBYI35MpaaTYqJK84CID2RwbdTTTJqgPoQn5vl\r\nd6ZUbvainLcdOKKRzB1cT2LE\r\n-----END CERTIFICATE-----\r\n";

    public static async Task Main()
    {
        using var actorId = new ActorId("actor-a");
        using var digest = new CodeSha256(Enumerable.Repeat((byte)1, 32).ToArray());
        using var limits = new ActorLimits(1, 2, 3);
        using var binding = new Binding("binding-a", "capability-a", "resource-a");
        using var subscription = new SubscriptionSpec(
            "subscription-a", "events/input", new SubscriptionStart.Cursor(9007199254740993UL), true);
        using var create = new CreateActorRequest(digest, "eu", new[] { binding }, limits, new[] { subscription }, "csharp-create-a");
        using var update = new UpdateActorRequest(actorId, digest, new[] { binding }, limits, 0, "csharp-update-a");
        using var inspect = new InspectActorRequest(actorId);
        using var add = new AddSubscriptionRequest(actorId, subscription, "csharp-add-a");
        using var remove = new RemoveSubscriptionRequest(actorId, "subscription-a", "csharp-remove-a");
        using var resume = new ResumeSubscriptionRequest(actorId, "subscription-a", "csharp-resume-a");
        using var checkpoint = new CheckpointActorRequest(actorId, "csharp-checkpoint-a");
        using var invoke = new InvokeActorRequest(actorId, "POST", "/invoke", "request-body"u8.ToArray(), new[] { new Header("content-type", "application/json") });

        var invalidPositiveRejected = false;
        try { using var zero = new PositiveU64(0); }
        catch (BindingException) { invalidPositiveRejected = true; }
        if (!invalidPositiveRejected) throw new Exception("PositiveU64(0) accepted");
        using var maxPositive = new PositiveU64(ulong.MaxValue);
        if (maxPositive.Value() != ulong.MaxValue) throw new Exception("ulong max was not preserved");

        using var client = await WithCancellation(CancellationToken.None, h => AcyclicActorsUniffiMethods.ConnectActorsWithCa(Endpoint, Token, Utf8(Ca), h));
        ExpectActor(await WithCancellation(CancellationToken.None, h => client.CreateActor(create, h)));
        ExpectActor(await WithCancellation(CancellationToken.None, h => client.UpdateActor(update, h)));
        ExpectActor(await WithCancellation(CancellationToken.None, h => client.InspectActorRequest(inspect, h)));
        ExpectActor(await WithCancellation(CancellationToken.None, h => client.AddSubscription(add, h)));
        ExpectActor(await WithCancellation(CancellationToken.None, h => client.RemoveSubscription(remove, h)));
        ExpectActor(await WithCancellation(CancellationToken.None, h => client.ResumeSubscription(resume, h)));
        ExpectActor(await WithCancellation(CancellationToken.None, h => client.CheckpointActor(checkpoint, h)));
        var response = await WithCancellation(CancellationToken.None, h => client.InvokeActor(invoke, h));
        {
            if (response.Status != 201 || response.Body.Length != 0 || response.Headers.Length != 1 || response.Headers[0].Name != "location")
                throw new Exception("invoke response mismatch");
        }

        using var cancelledCts = new CancellationTokenSource();
        cancelledCts.Cancel();
        var cancelledObserved = false;
        try { await WithCancellation(cancelledCts.Token, h => client.InspectActor(actorId, h)); }
        catch (BindingException.Cancelled) { cancelledObserved = true; }
        if (!cancelledObserved) throw new Exception("CancellationToken did not preserve Rust cancellation");

        using var unauthorized = await WithCancellation(CancellationToken.None, h => AcyclicActorsUniffiMethods.ConnectActorsWithCa(Endpoint, "wrong-token", Utf8(Ca), h));
        var serviceErrorObserved = false;
        try { await WithCancellation(CancellationToken.None, h => unauthorized.InspectActor(actorId, h)); }
        catch (BindingException.Service error) { serviceErrorObserved = error.grpcCode == 16 && error.serviceCode is null; }
        if (!serviceErrorObserved) throw new Exception("typed service error mismatch");

        Console.WriteLine("source-fixed UniFFI C# all-eight/default-cancel/CancellationToken/error/u64 probe: PASS");
    }

    private static byte[] Utf8(string value) => System.Text.Encoding.UTF8.GetBytes(value);

    private static async Task<T> WithCancellation<T>(CancellationToken token, Func<CancellationHandle?, Task<T>> operation)
    {
        using var handle = token.CanBeCanceled ? new CancellationHandle() : null;
        using var registration = handle is null ? default : token.Register(handle.Cancel);
        return await operation(handle);
    }

    private static void ExpectActor(ActorObservation? observation)
    {
        if (observation is null) throw new Exception("operation returned null");
        if (observation.ActorId.Value() != "actor-a" || observation.CodeSha256.Value().Length != 32 ||
            observation.State != ActorState.Active || observation.CheckpointUnixMillis is not null ||
            observation.CheckpointEpoch != 9 || observation.ConfigurationRevision != 0)
            throw new Exception("actor observation mismatch");
    }
}



