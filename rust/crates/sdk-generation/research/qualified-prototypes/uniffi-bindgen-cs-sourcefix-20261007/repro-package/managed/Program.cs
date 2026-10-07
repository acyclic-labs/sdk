#nullable enable
using System;
using System.Collections;
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Acyclic.Actors;

internal static class Program
{
    private sealed class Options { public string endpoint { get; set; } = ""; public string token { get; set; } = ""; public string caCertificate { get; set; } = ""; public string controlEndpoint { get; set; } = ""; }
    private sealed class FixtureState { public int started { get; set; } public int aborted { get; set; } public int active { get; set; } }

    public static async Task Main()
    {
        var options = JsonSerializer.Deserialize<Options>(await File.ReadAllTextAsync(@"Q:\sdk\work\root-pending-actors-fixture-options.json")) ?? throw new Exception("fixture options missing");
        using var client = await AcyclicActorsUniffiMethods.ConnectActorsWithCa(options.endpoint, options.token, System.Text.Encoding.UTF8.GetBytes(options.caCertificate), null);
        var baseline = ContinuationCount();
        if (baseline != 0) throw new Exception($"continuation baseline {baseline}");
        using var http = new HttpClient();
        var peaks = "";
        for (var i = 0; i < 3; i++)
        {
            using var actor = new ActorId($"pending-csharp-token-{i}");
            using var cts = new CancellationTokenSource();
            var operation = client.InspectActor(actor, null, cts.Token);
            await WaitForState(http, options.controlEndpoint, state => state.started >= i + 1 && state.active == 1);
            var pending = ContinuationCount();
            if (pending < 1) throw new Exception($"continuation map did not grow: {pending}");
            cts.Cancel();
            var canceled = false;
            try { await operation; }
            catch (OperationCanceledException error) { canceled = error.CancellationToken == cts.Token; }
            if (!canceled) throw new Exception("CancellationToken did not map to OperationCanceledException with request token");
            var after = await WaitForState(http, options.controlEndpoint, state => state.aborted >= i + 1 && state.active == 0);
            if (ContinuationCount() != 0) throw new Exception($"continuation map leak after iteration {i}: {ContinuationCount()}");
            peaks = peaks.Length == 0 ? pending.ToString() : peaks + "," + pending;
            Console.WriteLine($"ITERATION {i + 1}: map baseline=0 pending={pending} after=0 state={after}");
        }
        Console.WriteLine($"DOTNET_GENERATED_CANCELLATIONTOKEN_PASS baseline={baseline} peaks={peaks}");
    }

    private static int ContinuationCount()
    {
        var runtime = typeof(ActorsClient).Assembly.GetType("Acyclic.Actors._UniFFIAsync") ?? throw new Exception("generated async runtime missing");
        var mapField = runtime.GetField("_async_handle_map", BindingFlags.Static | BindingFlags.NonPublic) ?? throw new Exception("continuation map missing");
        var map = mapField.GetValue(null) ?? throw new Exception("continuation map null");
        var storage = map.GetType().GetField("_map", BindingFlags.Instance | BindingFlags.NonPublic) ?? throw new Exception("continuation storage missing");
        return ((ICollection)storage.GetValue(map)!).Count;
    }

    private static async Task<FixtureState> WaitForState(HttpClient http, string endpoint, Func<FixtureState, bool> predicate)
    {
        for (var attempt = 0; attempt < 200; attempt++)
        {
            var state = JsonSerializer.Deserialize<FixtureState>(await http.GetStringAsync(endpoint + "/state")) ?? throw new Exception("invalid fixture state");
            if (predicate(state)) return state;
            await Task.Delay(10);
        }
        throw new TimeoutException("fixture state did not reach expected gate");
    }
}
