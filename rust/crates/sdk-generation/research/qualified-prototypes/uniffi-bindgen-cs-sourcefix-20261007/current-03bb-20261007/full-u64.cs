#nullable enable
using System; using System.IO; using System.Linq; using System.Text.Json; using System.Threading; using System.Threading.Tasks; using Acyclic.Actors;
internal static class FullU64 {
 sealed class O { public string endpoint {get;set;}=""; public string token {get;set;}=""; public string caCertificate {get;set;}=""; }
 public static async Task Main(){
  var o=JsonSerializer.Deserialize<O>(await File.ReadAllTextAsync("/tmp/current-csharp-all8-options.json"))!;
  var max=ulong.MaxValue; var d=Enumerable.Repeat((byte)1,32).ToArray(); var l=new ActorLimits(max,max,max); var b=new Binding("binding-u64","capability-u64","resource-u64");
  var s=new SubscriptionSpec("subscription-u64","events/input",new SubscriptionStart(new Start.Cursor(max)),true);
  var c=new CreateActorRequest(d,"eu",new[]{b},l,new[]{s},"u64-create");
  var u=new UpdateActorRequest("actor-a",d,new[]{b},l,max,"u64-update");
  using var client=await AcyclicActorsUniffiMethods.ConnectActorsWithCa(o.endpoint,o.token,System.Text.Encoding.UTF8.GetBytes(o.caCertificate),null,CancellationToken.None);
  var cr=await client.CreateActor(c,null,CancellationToken.None); var ur=await client.UpdateActor(u,null,CancellationToken.None);
  if(l.HandlerTimeoutMillis!=max || l.MemoryBytes!=max || l.CheckpointBytes!=max || s.Start.Start is not Start.Cursor cursor || cursor.V1!=max) throw new Exception("local u64 mismatch");
  Console.WriteLine($"CURRENT_03BB_FULL_U64_PASS={max} createActor={(cr.Actor is not null)} updateActor={(ur.Actor is not null)} cursor={cursor.V1}");
 }
}
