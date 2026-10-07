#nullable enable
using System; using System.Collections; using System.IO; using System.Net.Http; using System.Text.Json; using System.Threading; using System.Threading.Tasks; using Acyclic.Actors;
internal static class C8Cancellation {
 sealed class O { public string endpoint { get; set; } = ""; public string token { get; set; } = ""; public string caCertificate { get; set; } = ""; public string controlEndpoint { get; set; } = ""; }
 sealed class S { public int started { get; set; } public int aborted { get; set; } public int active { get; set; } }
 public static async Task Main() {
  var o = JsonSerializer.Deserialize<O>(await File.ReadAllTextAsync("/tmp/c8-799-pending-options.json"))!;
  using var client = await AcyclicActorsUniffiMethods.ConnectActorsWithCa(o.endpoint, o.token, System.Text.Encoding.UTF8.GetBytes(o.caCertificate), null, CancellationToken.None);
  using var http = new HttpClient(); var peaks = "";
  for (var i = 0; i < 3; i++) {
   using var cts = new CancellationTokenSource();
   var op = client.InspectActor(new InspectActorRequest($"pending-c8-799-{i}"), null, cts.Token);
   await Wait(http, o.controlEndpoint, s => s.started >= i + 1 && s.active == 1);
   var pending = Count(); if (pending < 1) throw new Exception($"async map did not grow: {pending}");
   cts.Cancel(); var canceled = false;
   try { await op; } catch (OperationCanceledException e) { canceled = e.CancellationToken == cts.Token; }
   if (!canceled) throw new Exception("wrong cancellation exception/token");
   var after = await Wait(http, o.controlEndpoint, s => s.aborted >= i + 1 && s.active == 0);
   var remaining = Count(); if (remaining != 0) throw new Exception($"async map leak: {remaining}");
   peaks = peaks.Length == 0 ? pending.ToString() : peaks + "," + pending;
   Console.WriteLine($"ITERATION {i + 1}: map baseline=0 pending={pending} after=0 state={JsonSerializer.Serialize(after)}");
  }
  Console.WriteLine($"C8_799_CANCELLATIONTOKEN_SERVER_ABORT_PASS baseline=0 peaks={peaks}");
 }
 static int Count() {
  var t = typeof(ActorsClient).Assembly.GetType("Acyclic.Actors._UniFFIAsync")!;
  var f = t.GetField("_async_handle_map", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;
  var m = f.GetValue(null)!; var sf = m.GetType().GetField("_map", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
  return ((ICollection)sf.GetValue(m)!).Count;
 }
 static async Task<S> Wait(HttpClient h, string endpoint, Func<S, bool> predicate) {
  for (var i = 0; i < 300; i++) { var s = JsonSerializer.Deserialize<S>(await h.GetStringAsync(endpoint + "/state"))!; if (predicate(s)) return s; await Task.Delay(10); }
  throw new TimeoutException("fixture state timeout");
 }
}
