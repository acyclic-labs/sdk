# Actual Win32 ConPTY transport for test-graphcoder-pty.mjs; never a redirected-stdin substitute.
param(
    [Parameter(Mandatory = $true)][string]$Binary,
    [Parameter(Mandatory = $true)][string]$Config,
    [int]$DeadlineMilliseconds = 90000
)
$ErrorActionPreference = 'Stop'
if (-not [IO.Path]::IsPathRooted($Binary) -or -not [IO.Path]::IsPathRooted($Config)) {
    throw 'ConPTY requires absolute installed binary and configuration paths'
}
if ($DeadlineMilliseconds -lt 1000 -or $DeadlineMilliseconds -gt 120000) {
    throw 'ConPTY deadline must be finite (1000..120000 ms)'
}
Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

public static class GraphcoderConPty {
    [StructLayout(LayoutKind.Sequential)] struct Coord { public short X, Y; }
    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)] struct StartupInfo {
        public int cb; public string reserved, desktop, title;
        public int x, y, xSize, ySize, xCountChars, yCountChars, fillAttribute, flags;
        public short showWindow, reservedBytes; public IntPtr reservedPointer, input, output, error;
    }
    [StructLayout(LayoutKind.Sequential)] struct StartupInfoEx { public StartupInfo info; public IntPtr attributes; }
    [StructLayout(LayoutKind.Sequential)] struct ProcessInfo { public IntPtr process, thread; public uint processId, threadId; }
    [StructLayout(LayoutKind.Sequential)] struct BasicLimit {
        public long processTime, jobTime; public uint flags; public UIntPtr minimum, maximum;
        public uint activeProcesses; public UIntPtr affinity; public uint priority, scheduling;
    }
    [StructLayout(LayoutKind.Sequential)] struct IoCounters { public ulong a, b, c, d, e, f; }
    [StructLayout(LayoutKind.Sequential)] struct ExtendedLimit {
        public BasicLimit basic; public IoCounters io;
        public UIntPtr processMemory, jobMemory, peakProcessMemory, peakJobMemory;
    }
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool CreatePipe(out IntPtr read, out IntPtr write, IntPtr security, uint size);
    [DllImport("kernel32.dll")] static extern int CreatePseudoConsole(Coord size, IntPtr input, IntPtr output, uint flags, out IntPtr console);
    [DllImport("kernel32.dll")] static extern void ClosePseudoConsole(IntPtr console);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool InitializeProcThreadAttributeList(IntPtr list, int count, uint flags, ref IntPtr size);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool UpdateProcThreadAttribute(IntPtr list, uint flags, IntPtr attribute, IntPtr value, IntPtr size, IntPtr previous, IntPtr returned);
    [DllImport("kernel32.dll")] static extern void DeleteProcThreadAttributeList(IntPtr list);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] static extern bool CreateProcessW(string application, StringBuilder command, IntPtr processAttributes, IntPtr threadAttributes, bool inherit, uint flags, IntPtr environment, string directory, ref StartupInfoEx startup, out ProcessInfo process);
    [DllImport("kernel32.dll", SetLastError = true)] static extern IntPtr CreateJobObjectW(IntPtr security, string name);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool SetInformationJobObject(IntPtr job, int kind, ref ExtendedLimit info, uint size);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool TerminateJobObject(IntPtr job, uint exit);
    [DllImport("kernel32.dll", SetLastError = true)] static extern uint ResumeThread(IntPtr thread);
    [DllImport("kernel32.dll", SetLastError = true)] static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool GetExitCodeProcess(IntPtr process, out uint exit);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool ReadFile(IntPtr handle, byte[] bytes, uint size, out uint read, IntPtr overlapped);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool WriteFile(IntPtr handle, byte[] bytes, uint size, out uint written, IntPtr overlapped);
    [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
    [DllImport("kernel32.dll")] static extern bool TerminateProcess(IntPtr process, uint exit);
    static void Require(bool ok, string operation) { if (!ok) throw new Win32Exception(Marshal.GetLastWin32Error(), operation); }
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
    public static int Run(string binary, string config, int deadline) {
        IntPtr inRead = IntPtr.Zero, inWrite = IntPtr.Zero, outRead = IntPtr.Zero, outWrite = IntPtr.Zero;
        IntPtr console = IntPtr.Zero, attributes = IntPtr.Zero, job = IntPtr.Zero;
        ProcessInfo process = new ProcessInfo(); bool attributeInitialized = false;
        Exception transportError = null; Thread outputThread = null;
        try {
            Require(CreatePipe(out inRead, out inWrite, IntPtr.Zero, 0), "CreatePipe input");
            Require(CreatePipe(out outRead, out outWrite, IntPtr.Zero, 0), "CreatePipe output");
            int hr = CreatePseudoConsole(new Coord { X = 32767, Y = 100 }, inRead, outWrite, 0, out console);
            if (hr != 0) throw new COMException("CreatePseudoConsole unavailable; real Windows ConPTY is mandatory", hr);
            CloseHandle(inRead); inRead = IntPtr.Zero; CloseHandle(outWrite); outWrite = IntPtr.Zero;
            IntPtr size = IntPtr.Zero;
            InitializeProcThreadAttributeList(IntPtr.Zero, 1, 0, ref size);
            if (size == IntPtr.Zero) throw new Win32Exception("ConPTY process attribute size unavailable");
            attributes = Marshal.AllocHGlobal(size);
            Require(InitializeProcThreadAttributeList(attributes, 1, 0, ref size), "InitializeProcThreadAttributeList");
            attributeInitialized = true;
            Require(UpdateProcThreadAttribute(attributes, 0, new IntPtr(0x00020016), console, new IntPtr(IntPtr.Size), IntPtr.Zero, IntPtr.Zero), "PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE");
            job = CreateJobObjectW(IntPtr.Zero, null); Require(job != IntPtr.Zero, "CreateJobObject");
            var limit = new ExtendedLimit(); limit.basic.flags = 0x00002000; // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            Require(SetInformationJobObject(job, 9, ref limit, (uint)Marshal.SizeOf(typeof(ExtendedLimit))), "SetInformationJobObject");
            var startup = new StartupInfoEx(); startup.info.cb = Marshal.SizeOf(typeof(StartupInfoEx)); startup.attributes = attributes;
            Require(CreateProcessW(binary, new StringBuilder(Quote(binary) + " --session " + Quote(config)), IntPtr.Zero, IntPtr.Zero, false,
                0x00080000 | 0x00000004, IntPtr.Zero, Path.GetDirectoryName(config), ref startup, out process), "CreateProcessW with real ConPTY");
            Require(AssignProcessToJobObject(job, process.process), "AssignProcessToJobObject");
            IntPtr inputHandle = inWrite, outputHandle = outRead, jobHandle = job;
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
                } catch (Exception error) { Interlocked.CompareExchange(ref transportError, error, null); TerminateJobObject(jobHandle, 125); }
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
                } catch (Exception error) { Interlocked.CompareExchange(ref transportError, error, null); TerminateJobObject(jobHandle, 125); }
            });
            inputThread.IsBackground = true; inputThread.Start();
            Require(ResumeThread(process.thread) != UInt32.MaxValue, "ResumeThread");
            Console.Error.WriteLine("conpty-host primitive=CreatePseudoConsole process-attribute=PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE child-process-id=" + process.processId);
            uint waited = WaitForSingleObject(process.process, (uint)deadline);
            if (waited == 258) { TerminateJobObject(job, 124); throw new TimeoutException("finite real ConPTY process deadline exceeded"); }
            Require(waited == 0, "WaitForSingleObject");
            uint exit; Require(GetExitCodeProcess(process.process, out exit), "GetExitCodeProcess");
            // Drain concurrently while closing; ClosePseudoConsole can wait for its output reader.
            ClosePseudoConsole(console); console = IntPtr.Zero;
            if (!outputThread.Join(5000)) throw new TimeoutException("finite ConPTY output-drain deadline exceeded");
            if (transportError != null) throw new IOException("real ConPTY transport failed", transportError);
            return unchecked((int)exit);
        } finally {
            if (process.process != IntPtr.Zero) TerminateProcess(process.process, 125);
            if (job != IntPtr.Zero) TerminateJobObject(job, 125);
            if (console != IntPtr.Zero) ClosePseudoConsole(console);
            if (attributeInitialized) DeleteProcThreadAttributeList(attributes);
            if (attributes != IntPtr.Zero) Marshal.FreeHGlobal(attributes);
            foreach (IntPtr handle in new [] { inRead, inWrite, outRead, outWrite, process.thread, process.process, job })
                if (handle != IntPtr.Zero) CloseHandle(handle);
        }
    }
}
'@
try { exit [GraphcoderConPty]::Run($Binary, $Config, $DeadlineMilliseconds) }
catch { [Console]::Error.WriteLine($_.Exception.ToString()); exit 125 }
