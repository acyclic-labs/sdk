using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

// Cross-compile on bb2. Own the acceptance process tree and preserve its output.
public static class OwnedProcess {
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
    [StructLayout(LayoutKind.Sequential)] struct CompletionPort { public IntPtr key, port; }
    [DllImport("kernel32.dll")] static extern IntPtr GetCurrentProcess();
    [DllImport("kernel32.dll")] static extern IntPtr GetStdHandle(int kind);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool DuplicateHandle(IntPtr sourceProcess, IntPtr source, IntPtr targetProcess, out IntPtr target, uint access, bool inherit, uint options);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool InitializeProcThreadAttributeList(IntPtr list, int count, uint flags, ref IntPtr size);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool UpdateProcThreadAttribute(IntPtr list, uint flags, IntPtr attribute, IntPtr value, IntPtr size, IntPtr previous, IntPtr returned);
    [DllImport("kernel32.dll")] static extern void DeleteProcThreadAttributeList(IntPtr list);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] static extern bool CreateProcessW(string application, StringBuilder command, IntPtr processAttributes, IntPtr threadAttributes, bool inherit, uint flags, IntPtr environment, string directory, ref StartupInfoEx startup, out ProcessInfo process);
    [DllImport("kernel32.dll", SetLastError = true)] static extern IntPtr CreateJobObjectW(IntPtr security, string name);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool SetInformationJobObject(IntPtr job, int kind, ref ExtendedLimit info, uint size);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool SetInformationJobObject(IntPtr job, int kind, ref CompletionPort info, uint size);
    [DllImport("kernel32.dll", SetLastError = true)] static extern IntPtr CreateIoCompletionPort(IntPtr file, IntPtr existing, UIntPtr key, uint concurrency);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool GetQueuedCompletionStatus(IntPtr port, out uint message, out UIntPtr key, out IntPtr process, uint milliseconds);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] static extern IntPtr CreateFileW(string name, uint access, uint share, IntPtr security, uint disposition, uint flags, IntPtr template);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool TerminateJobObject(IntPtr job, uint exit);
    [DllImport("kernel32.dll", SetLastError = true)] static extern uint ResumeThread(IntPtr thread);
    [DllImport("kernel32.dll", SetLastError = true)] static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool GetExitCodeProcess(IntPtr process, out uint exit);
    [DllImport("kernel32.dll")] static extern bool TerminateProcess(IntPtr process, uint exit);
    [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
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
    public static int Main(string[] args) {
        IntPtr job = IntPtr.Zero, attributes = IntPtr.Zero, handles = IntPtr.Zero, completion = IntPtr.Zero, emptyInput = IntPtr.Zero;
        var standard = new IntPtr[3]; var child = new ProcessInfo();
        bool initialized = false, exited = false;
        var lifetime = new object(); IntPtr controlledJob = IntPtr.Zero;
        Exception controlError = null;
        try {
            if (args.Length == 0 || !Path.IsPathRooted(args[0]))
                throw new ArgumentException("usage: owned-process ABSOLUTE_BINARY [ARG...]");
            job = CreateJobObjectW(IntPtr.Zero, null); Require(job != IntPtr.Zero, "CreateJobObjectW");
            var limit = new ExtendedLimit(); limit.basic.flags = 0x00002000;
            Require(SetInformationJobObject(job, 9, ref limit, (uint)Marshal.SizeOf(typeof(ExtendedLimit))), "JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE");
            completion = CreateIoCompletionPort(new IntPtr(-1), IntPtr.Zero, UIntPtr.Zero, 1);
            Require(completion != IntPtr.Zero, "CreateIoCompletionPort");
            var port = new CompletionPort { key = new IntPtr(1), port = completion };
            Require(SetInformationJobObject(job, 7, ref port, (uint)Marshal.SizeOf(typeof(CompletionPort))), "associate job completion port");
            var current = GetCurrentProcess();
            // stdin is the lifetime-control channel. External libtest/Bun
            // acceptance commands receive EOF; their output remains unchanged.
            emptyInput = CreateFileW("NUL", 0x80000000, 3, IntPtr.Zero, 3, 0, IntPtr.Zero);
            Require(emptyInput != new IntPtr(-1), "open empty acceptance input");
            Require(DuplicateHandle(current, emptyInput, current, out standard[0], 0, true, 2), "duplicate empty input");
            for (int i = 1; i < 3; i++)
                Require(DuplicateHandle(current, GetStdHandle(-10 - i), current, out standard[i], 0, true, 2), "duplicate standard handle");
            IntPtr size = IntPtr.Zero;
            InitializeProcThreadAttributeList(IntPtr.Zero, 1, 0, ref size);
            Require(size != IntPtr.Zero, "process attribute size");
            attributes = Marshal.AllocHGlobal(size);
            Require(InitializeProcThreadAttributeList(attributes, 1, 0, ref size), "InitializeProcThreadAttributeList");
            initialized = true;
            handles = Marshal.AllocHGlobal(3 * IntPtr.Size);
            Marshal.Copy(standard, 0, handles, 3);
            Require(UpdateProcThreadAttribute(attributes, 0, new IntPtr(0x00020002), handles, new IntPtr(3 * IntPtr.Size), IntPtr.Zero, IntPtr.Zero), "PROC_THREAD_ATTRIBUTE_HANDLE_LIST");
            var startup = new StartupInfoEx(); startup.info.cb = Marshal.SizeOf(typeof(StartupInfoEx));
            startup.info.flags = 0x00000100; startup.info.input = standard[0];
            startup.info.output = standard[1]; startup.info.error = standard[2]; startup.attributes = attributes;
            var command = new StringBuilder(String.Join(" ", Array.ConvertAll(args, Quote)));
            Require(CreateProcessW(args[0], command, IntPtr.Zero, IntPtr.Zero, true,
                0x00080000 | 0x00000004 | 0x08000000, IntPtr.Zero, Environment.CurrentDirectory, ref startup, out child), "CreateProcessW suspended");
            // No instruction executes before assignment; descendants cannot
            // escape by winning a spawn/assignment race. Breakaway is disabled.
            Require(AssignProcessToJobObject(job, child.process), "AssignProcessToJobObject");
            controlledJob = job;
            var cancellation = new Thread(delegate() {
                try { Console.OpenStandardInput().ReadByte(); }
                catch (Exception error) { Interlocked.CompareExchange(ref controlError, error, null); }
                finally {
                    lock (lifetime) {
                        if (controlledJob != IntPtr.Zero) TerminateJobObject(controlledJob, 124);
                    }
                }
            });
            cancellation.IsBackground = true; cancellation.Start();
            Require(ResumeThread(child.thread) != UInt32.MaxValue, "ResumeThread");
            Require(WaitForSingleObject(child.process, UInt32.MaxValue) == 0, "wait for acceptance owner");
            uint code; Require(GetExitCodeProcess(child.process, out code), "GetExitCodeProcess");
            exited = true;
            Require(TerminateJobObject(job, 125), "terminate acceptance descendants");
            bool admitted = false;
            while (true) {
                uint message; UIntPtr key; IntPtr member;
                Require(GetQueuedCompletionStatus(completion, out message, out key, out member, UInt32.MaxValue), "wait for job process completion");
                if (message == 6 && member.ToInt64() == child.processId) admitted = true;
                if (admitted && message == 4) break; // JOB_OBJECT_MSG_ACTIVE_PROCESS_ZERO
            }
            if (controlError != null) throw new IOException("acceptance lifetime control failed", controlError);
            return unchecked((int)code);
        } catch (Exception error) { Console.Error.WriteLine(error); return 125; }
        finally {
            if (!exited && child.process != IntPtr.Zero) TerminateProcess(child.process, 125);
            lock (lifetime) {
                controlledJob = IntPtr.Zero;
                if (job != IntPtr.Zero) CloseHandle(job);
            }
            if (child.thread != IntPtr.Zero) CloseHandle(child.thread);
            if (child.process != IntPtr.Zero) CloseHandle(child.process);
            if (completion != IntPtr.Zero) CloseHandle(completion);
            if (emptyInput != IntPtr.Zero && emptyInput != new IntPtr(-1)) CloseHandle(emptyInput);
            foreach (var handle in standard) if (handle != IntPtr.Zero) CloseHandle(handle);
            if (initialized) DeleteProcThreadAttributeList(attributes);
            if (attributes != IntPtr.Zero) Marshal.FreeHGlobal(attributes);
            if (handles != IntPtr.Zero) Marshal.FreeHGlobal(handles);
        }
    }
}
