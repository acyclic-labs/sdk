using Acyclic.Actors;
public static class NegativePackage {
    public static void Probe(ulong raw) {
        var client = new ActorsClient(raw, true);
        var h = new Header("x", "y");
        ActorLimits limits = h;
        string id = h;
        h.Name = "changed";
    }
}
