using Acyclic.Actors;
public static class Negative {
 public static void Probe(){
  var raw = new ActorsClient(1UL, true);
  var header = new Header("n", "v");
  header.Name = "changed";
  ActorLimits wrong = header;
  var inspect = new InspectActorRequest(header);
 }
}
