using Actors.StreamBinding;

static class Program
{
    static void Main()
    {
        // Raw UniFFI object handles are intentionally not constructible by consumers.
        _ = new RecordCursor(0UL, true);
    }
}