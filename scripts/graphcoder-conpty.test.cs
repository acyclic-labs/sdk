using System;
using System.Runtime.InteropServices;

public static class TerminalContractTest {
    [DllImport("shell32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    static extern IntPtr CommandLineToArgvW(string command, out int count);
    [DllImport("kernel32.dll")] static extern IntPtr LocalFree(IntPtr memory);
    static void Refused(string[] args) {
        try { GraphcoderConPty.TerminalOptions.Parse(args); }
        catch (ArgumentException) { return; }
        throw new InvalidOperationException("invalid terminal option was admitted");
    }
    public static int Main() {
        try {
            string id = "resume \"quoted\" \\";
            var options = GraphcoderConPty.TerminalOptions.Parse(new[] {
                "binary", "workspace", "catalog", "settings", "model", "--rows", "31", "--cols", "101", "--resume", "--operation", id
            });
            if (options.Rows != 31 || options.Columns != 101) throw new InvalidOperationException("terminal size changed");
            int count;
            IntPtr parsed = CommandLineToArgvW("binary" + options.Arguments, out count);
            if (parsed == IntPtr.Zero) throw new InvalidOperationException("OS command parser refused the command");
            try {
                if (count != 4 || Marshal.PtrToStringUni(Marshal.ReadIntPtr(parsed, 3 * IntPtr.Size)) != id)
                    throw new InvalidOperationException("operation changed at OS argument boundary");
            } finally { LocalFree(parsed); }
            foreach (string value in new[] { "0", "-1", "32768", "1.5" })
                Refused(new[] { "B", "W", "C", "S", "M", "--rows", value });
            Refused(new[] { "B", "W", "C", "S", "M", "--rows", "24", "--rows", "25" });
            Refused(new[] { "B", "W", "C", "S", "M", "--operation", "" });
            Refused(new[] { "B", "W", "C", "S", "M", "--deadline", "1" });
            Console.WriteLine("terminal contract: configured dimensions, operation OS quoting and refusal controls passed");
            return 0;
        } catch (Exception error) {
            Console.Error.WriteLine("step=terminal-contract; error=" + error + "; recovery=" +
                System.Reflection.Assembly.GetExecutingAssembly().Location);
            return 1;
        }
    }
}
