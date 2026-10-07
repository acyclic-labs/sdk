#nullable enable
using System.Linq;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Acyclic.Actors;
internal static class AllEightProgram
{
    private sealed class Options { public string endpoint { get; set; } = ""; public string token { get; set; } = ""; public string caCertificate { get; set; } = ""; }
    public static async Task Main()
    {
        var options = JsonSerializer.Deserialize<Options>(await File.ReadAllTextAsync(@"Q:\sdk\work\actors-uniffi-csharp-sourcefix-consumer-20261007\fixture-options.json")) ?? throw new System.Exception("fixture options missing");
        using var actorId = new ActorId("actor-a");
        using var digest = new CodeSha256(Enumerable.Repeat((byte)1, 32).ToArray());
        using var limits = new ActorLimits(1, 2, 3);
        using var binding = new Binding("binding-a", "capability-a", "resource-a");
        using var subscription = new SubscriptionSpec("subscription-a", "events/input", new SubscriptionStart.Cursor(9007199254740993UL), true);
        using var create = new CreateActorRequest(digest, "eu", new[] { binding }, limits, new[] { subscription }, "csharp-create-a");
        using var update = new UpdateActorRequest(actorId, digest, new[] { binding }, limits, 0, "csharp-update-a");
        using var inspect = new InspectActorRequest(actorId);
        using var add = new AddSubscriptionRequest(actorId, subscription, "csharp-add-a");
        using var remove = new RemoveSubscriptionRequest(actorId, "subscription-a", "csharp-remove-a");
        using var resume = new ResumeSubscriptionRequest(actorId, "subscription-a", "csharp-resume-a");
        using var checkpoint = new CheckpointActorRequest(actorId, "checkpoint-a");
        using var invoke = new InvokeActorRequest(actorId, "POST", "/invoke", "request-body"u8.ToArray(), new[] { new Header("content-type", "application/json") });
        using var client = await AcyclicActorsUniffiMethods.ConnectActorsWithCa(options.endpoint, options.token, System.Text.Encoding.UTF8.GetBytes(options.caCertificate), null, CancellationToken.None);
        System.Console.WriteLine("connect");
        await client.CreateActor(create, null, CancellationToken.None);
        System.Console.WriteLine("create");
        await client.UpdateActor(update, null, CancellationToken.None);
        System.Console.WriteLine("update");
        await client.InspectActor(actorId, null, CancellationToken.None);
        System.Console.WriteLine("inspect");
        await client.InspectActorRequest(inspect, null, CancellationToken.None);
        System.Console.WriteLine("inspect-request");
        await client.AddSubscription(add, null, CancellationToken.None);
        System.Console.WriteLine("add");
        await client.RemoveSubscription(remove, null, CancellationToken.None);
        System.Console.WriteLine("remove");
        await client.ResumeSubscription(resume, null, CancellationToken.None);
        System.Console.WriteLine("resume");
        await client.CheckpointActor(checkpoint, null, CancellationToken.None);
        System.Console.WriteLine("checkpoint");
        await client.InvokeActor(invoke, null, CancellationToken.None);
        System.Console.WriteLine("DOTNET_GENERATED_ALL8_DECLARATIONS_PASS");
    }
}
