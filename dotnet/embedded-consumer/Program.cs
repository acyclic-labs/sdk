using System.Text;
using Acyclic.Sdk.Embedded;

using var engine = new EmbeddedStreamEngine();
var receipt = engine.Append("actors/consumer", Encoding.UTF8.GetBytes("one"));
if ((receipt.Start, receipt.End, receipt.Tail) != (0UL, 1UL, 1UL))
    throw new InvalidOperationException("Rust append receipt did not match the canonical stream");

var records = new List<EmbeddedRecord>();
await foreach (var record in engine.ReadAsync("actors/consumer", 0, 1))
    records.Add(record);
if (records.Count != 1 || records[0].Sequence != 0 || Encoding.UTF8.GetString(records[0].Value) != "one")
    throw new InvalidOperationException("Rust read result did not match the canonical stream");

engine.Append("actors/live", Encoding.UTF8.GetBytes("initial"));
using var cancellation = new CancellationTokenSource();
var follow = engine.FollowAsync("actors/live", 1, cancellation.Token).GetAsyncEnumerator(cancellation.Token);
cancellation.Cancel();
try { await follow.MoveNextAsync(); } catch (OperationCanceledException) { }
await follow.DisposeAsync();

var recovered = engine.Append("actors/recovery", Encoding.UTF8.GetBytes("recovered"));
if (recovered.End != 1) throw new InvalidOperationException("Rust provider did not recover after the cancelled follow");
Console.WriteLine("embedded Rust ABI consumer passed: append/read/follow-cancel/recovery");
