using System;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

public static class GraphcoderConPty {
    static string recovery;
    static void Refuse(string step, Exception error) {
        Console.Error.WriteLine("step=conpty-" + step + "; error=" + error + "; recovery=" + recovery);
    }
    // Compiled ahead of qualification; direct launch avoids one PowerShell per PTY.
    public static int Main(string[] args) {
        string host = Environment.GetEnvironmentVariable("E2E_PROCESS_HOST");
        recovery = Quote(String.IsNullOrEmpty(host) ? "acyclic-process-host.exe" : host) + " --inherit-input " +
            Quote(System.Reflection.Assembly.GetExecutingAssembly().Location);
        foreach (string argument in args) recovery += " " + Quote(argument);
        string step = "native-custody";
        try {
            RequireNativeCustody();
            step = "arguments";
            // Typed options only; the caller's owner holds the one deadline.
            const string usage = "usage: conpty BINARY WORKSPACE CATALOG SETTINGS MODEL [--resume] [--operation ID]";
            if (args.Length < 5) throw new ArgumentException(usage);
            var options = new StringBuilder(); bool resume = false, operation = false;
            for (int index = 5; index < args.Length; index++) {
                if (args[index] == "--resume" && !resume) { resume = true; options.Append(" --resume"); }
                else if (args[index] == "--operation" && !operation && index + 1 < args.Length && args[index + 1].Length != 0) {
                    operation = true; options.Append(" --operation " + Quote(args[++index]));
                } else throw new ArgumentException(usage);
            }
            step = "terminal";
            return Run(args[0], args[1], args[2], args[3], args[4], options.ToString());
        } catch (Exception error) { Refuse(step, error); return 125; }
    }
    [StructLayout(LayoutKind.Sequential)] struct Coord { public short X, Y; }
    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)] struct StartupInfo {
        public int cb; public string reserved, desktop, title;
        public int x, y, xSize, ySize, xCountChars, yCountChars, fillAttribute, flags;
        public short showWindow, reservedBytes; public IntPtr reservedPointer, input, output, error;
    }
    [StructLayout(LayoutKind.Sequential)] struct StartupInfoEx { public StartupInfo info; public IntPtr attributes; }
    [StructLayout(LayoutKind.Sequential)] struct ProcessInfo { public IntPtr process, thread; public uint processId, threadId; }
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool CreatePipe(out IntPtr read, out IntPtr write, IntPtr security, uint size);
    [DllImport("kernel32.dll")] static extern int CreatePseudoConsole(Coord size, IntPtr input, IntPtr output, uint flags, out IntPtr console);
    [DllImport("kernel32.dll")] static extern void ClosePseudoConsole(IntPtr console);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool InitializeProcThreadAttributeList(IntPtr list, int count, uint flags, ref IntPtr size);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool UpdateProcThreadAttribute(IntPtr list, uint flags, IntPtr attribute, IntPtr value, IntPtr size, IntPtr previous, IntPtr returned);
    [DllImport("kernel32.dll")] static extern void DeleteProcThreadAttributeList(IntPtr list);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] static extern bool CreateProcessW(string application, StringBuilder command, IntPtr processAttributes, IntPtr threadAttributes, bool inherit, uint flags, IntPtr environment, string directory, ref StartupInfoEx startup, out ProcessInfo process);
    [DllImport("kernel32.dll", SetLastError = true)] static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);
    [DllImport("kernel32.dll", SetLastError = true)] static extern uint ResumeThread(IntPtr thread);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool GetExitCodeProcess(IntPtr process, out uint exit);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool ReadFile(IntPtr handle, byte[] bytes, uint size, out uint read, IntPtr overlapped);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool WriteFile(IntPtr handle, byte[] bytes, uint size, out uint written, IntPtr overlapped);
    [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
    [DllImport("kernel32.dll")] static extern IntPtr GetCurrentProcess();
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] static extern IntPtr OpenJobObjectW(uint access, bool inherit, string name);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool IsProcessInJob(IntPtr process, IntPtr job, out bool member);
    [DllImport("kernel32.dll", SetLastError = true)] static extern IntPtr GetStdHandle(int kind);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool SetStdHandle(int kind, IntPtr handle);
    static void Require(bool ok, string operation) { if (!ok) throw new Win32Exception(Marshal.GetLastWin32Error(), operation); }
    static void RequireNativeCustody() {
        string name = Environment.GetEnvironmentVariable("ACYCLIC_PROCESS_CUSTODY_JOB");
        if (String.IsNullOrEmpty(name)) throw new InvalidOperationException("launch ConPTY through acyclic-process-host --inherit-input");
        IntPtr job = OpenJobObjectW(4, false, name);
        Require(job != IntPtr.Zero, "open native custody for membership query");
        try {
            bool member;
            Require(IsProcessInJob(GetCurrentProcess(), job, out member) && member, "ConPTY requires native process custody");
        } finally { CloseHandle(job); }
    }
    static string Quote(string value) {
        var result = new StringBuilder("\""); int slashes = 0;
        foreach (char ch in value) {
            if (ch == '\\') { slashes++; continue; }
            if (ch == '"') { result.Append('\\', slashes * 2 + 1); result.Append(ch); }
            else { result.Append('\\', slashes); result.Append(ch); }
            slashes = 0;
        }
        result.Append('\\', slashes * 2); return result.Append('"').ToString();
    }
    static void AdmitSuspendedProcess(uint pid) {
        string endpoint = Environment.GetEnvironmentVariable("ACYCLIC_PROCESS_CUSTODY_ADMISSION");
        int separator = endpoint == null ? -1 : endpoint.LastIndexOf(':');
        int port;
        if (separator < 0 || endpoint.Substring(0, separator) != "127.0.0.1" ||
            !Int32.TryParse(endpoint.Substring(separator + 1), out port))
            throw new InvalidOperationException("missing native custody admission endpoint");
        // The native core validates Job membership and owns the stable wait.
        // This adapter retains the original suspended process until its ACK.
        using (var client = new TcpClient()) {
            client.NoDelay = true;
            client.Connect(IPAddress.Loopback, port);
            using (var stream = client.GetStream()) {
                byte[] bytes = { (byte)pid, (byte)(pid >> 8), (byte)(pid >> 16), (byte)(pid >> 24) };
                stream.Write(bytes, 0, bytes.Length);
                if (stream.ReadByte() != 1) throw new IOException("native custodian rejected ConPTY child");
            }
        }
    }
    static int Run(string binary, string workspace, string catalog, string settings, string model, string options) {
        if (!Path.IsPathRooted(binary) || !Path.IsPathRooted(workspace) || !Path.IsPathRooted(catalog) || !Path.IsPathRooted(settings))
            throw new ArgumentException("ConPTY paths must be absolute");
        IntPtr inRead = IntPtr.Zero, inWrite = IntPtr.Zero, outRead = IntPtr.Zero, outWrite = IntPtr.Zero;
        IntPtr console = IntPtr.Zero, attributes = IntPtr.Zero;
        ProcessInfo process = new ProcessInfo(); bool attributeInitialized = false, rootExited = false;
        Thread outputThread = null;
        string step = "terminal-create";
        try {
            Require(CreatePipe(out inRead, out inWrite, IntPtr.Zero, 0), "CreatePipe input");
            Require(CreatePipe(out outRead, out outWrite, IntPtr.Zero, 0), "CreatePipe output");
            int hr = CreatePseudoConsole(new Coord { X = 80, Y = 24 }, inRead, outWrite, 0, out console);
            if (hr != 0) throw new COMException("CreatePseudoConsole unavailable; real Windows ConPTY is mandatory", hr);
            IntPtr size = IntPtr.Zero;
            InitializeProcThreadAttributeList(IntPtr.Zero, 1, 0, ref size);
            if (size == IntPtr.Zero) throw new Win32Exception("ConPTY process attribute size unavailable");
            attributes = Marshal.AllocHGlobal(size);
            Require(InitializeProcThreadAttributeList(attributes, 1, 0, ref size), "InitializeProcThreadAttributeList");
            attributeInitialized = true;
            Require(UpdateProcThreadAttribute(attributes, 0, new IntPtr(0x00020016), console, new IntPtr(IntPtr.Size), IntPtr.Zero, IntPtr.Zero), "PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE");
            var startup = new StartupInfoEx(); startup.info.cb = Marshal.SizeOf(typeof(StartupInfoEx)); startup.attributes = attributes;
            // This dedicated host must not pass its redirected standard handles
            // into the console child. Restore them before starting transport threads.
            IntPtr parentInput = GetStdHandle(-10), parentOutput = GetStdHandle(-11), parentError = GetStdHandle(-12);
            try {
                Require(SetStdHandle(-10, IntPtr.Zero), "clear inherited input");
                Require(SetStdHandle(-11, IntPtr.Zero), "clear inherited output");
                Require(SetStdHandle(-12, IntPtr.Zero), "clear inherited error");
                Require(CreateProcessW(binary, new StringBuilder(Quote(binary) + " --workspace " + Quote(workspace) + " --settings-root " + Quote(settings) + " --catalog " + Quote(catalog) + " --model " + Quote(model) + options),IntPtr.Zero, IntPtr.Zero, false,
                    0x00080004, IntPtr.Zero, workspace, ref startup, out process), "CreateProcessW with real ConPTY");
            } finally {
                bool restoredInput = SetStdHandle(-10, parentInput);
                bool restoredOutput = SetStdHandle(-11, parentOutput);
                bool restoredError = SetStdHandle(-12, parentError);
                Require(restoredInput && restoredOutput && restoredError, "restore host standard handles");
            }
            // Retain the pseudoconsole's channel endpoints through child creation.
            CloseHandle(inRead); inRead = IntPtr.Zero; CloseHandle(outWrite); outWrite = IntPtr.Zero;
            // Native ProcessCustody owns this transport before it executes.
            // ConPTY children inherit that containment atomically at creation;
            // this thin terminal adapter owns no Job or process-tree reaper.
            step = "child-admission";
            AdmitSuspendedProcess(process.processId);
            Require(ResumeThread(process.thread) != UInt32.MaxValue, "resume admitted ConPTY child");
            IntPtr inputHandle = inWrite, outputHandle = outRead;
            outputThread = new Thread(delegate() {
                try {
                    var stdout = Console.OpenStandardOutput(); byte[] bytes = new byte[4096]; uint count; int total = 0;
                    while (true) {
                        if (!ReadFile(outputHandle, bytes, (uint)bytes.Length, out count, IntPtr.Zero)) {
                            int error = Marshal.GetLastWin32Error();
                            if (error != 109) throw new Win32Exception(error, "ConPTY ReadFile");
                            break;
                        }
                        if (count == 0) break;
                        total = checked(total + (int)count);
                        if (total > 1048576) throw new IOException("ConPTY captured output exceeds 1 MiB");
                        stdout.Write(bytes, 0, (int)count); stdout.Flush();
                    }
                } catch (Exception error) { Refuse("output", error); Environment.Exit(125); }
            });
            outputThread.IsBackground = true; outputThread.Start();
            var inputThread = new Thread(delegate() {
                try {
                    var stdin = Console.OpenStandardInput(); byte[] bytes = new byte[4096]; int count; int total = 0;
                    while ((count = stdin.Read(bytes, 0, bytes.Length)) != 0) {
                        total = checked(total + count);
                        if (total > 1048576) throw new IOException("ConPTY input exceeds 1 MiB");
                        uint written; Require(WriteFile(inputHandle, bytes, (uint)count, out written, IntPtr.Zero), "ConPTY WriteFile");
                        if (written != count) throw new IOException("ConPTY partial input write");
                    }
                } catch (Exception error) { Refuse("input", error); Environment.Exit(125); }
            });
            inputThread.IsBackground = true; inputThread.Start();
            step = "child-wait";
            Console.Error.WriteLine("conpty-host primitive=CreatePseudoConsole process-attribute=PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE child-process-id=" + process.processId);
            Require(WaitForSingleObject(process.process, UInt32.MaxValue) == 0, "WaitForSingleObject");
            rootExited = true;
            uint exit; Require(GetExitCodeProcess(process.process, out exit), "GetExitCodeProcess");
            step = "terminal-drain";
            // Drain concurrently while closing; ClosePseudoConsole can wait for its output reader.
            ClosePseudoConsole(console); console = IntPtr.Zero;
            outputThread.Join();
            return unchecked((int)exit);
        } catch (Exception error) {
            Refuse(step, error);
            if (process.process != IntPtr.Zero && !rootExited) Environment.Exit(125);
            return 125;
        } finally {
            // An incomplete transport must not block in ClosePseudoConsole
            // while its child still runs. Exiting transfers terminal cleanup
            // to the native owner, which retains all physical-death barriers.
            if (process.process != IntPtr.Zero && !rootExited) {
                Refuse("incomplete-transport", new IOException("native owner must retire the admitted child"));
                Environment.Exit(125);
            }
            if (console != IntPtr.Zero) ClosePseudoConsole(console);
            if (attributeInitialized) DeleteProcThreadAttributeList(attributes);
            if (attributes != IntPtr.Zero) Marshal.FreeHGlobal(attributes);
            foreach (IntPtr handle in new [] { inRead, inWrite, outRead, outWrite, process.thread, process.process })
                if (handle != IntPtr.Zero) CloseHandle(handle);
        }
    }
}
