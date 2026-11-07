param(
    [Parameter(Mandatory=$true)][string]$Binary,
    [switch]$FrameRegions,
    [ValidateSet('break','close','blocked','input-close')][string]$Scenario
)
$ErrorActionPreference = 'Stop'
if ($FrameRegions -and $Scenario) {
    throw 'scenario selection conflicts with frame regions; recovery: pass either -FrameRegions or -Scenario input-close'
}
$Binary = (Resolve-Path -LiteralPath $Binary).ProviderPath
$artifactRoot = Join-Path (Split-Path -Parent $PSScriptRoot) ('target/terminal-signals-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $artifactRoot | Out-Null
Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.ExceptionServices;

public static class TerminalSignalConPty {
    [StructLayout(LayoutKind.Sequential)] struct Coord { public short x,y; }
    [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)] struct Startup {
        public int cb; public string reserved,desktop,title;
        public int x,y,xSize,ySize,xCount,yCount,attribute,flags;
        public short show,reservedBytes; public IntPtr reservedPointer,input,output,error;
    }
    [StructLayout(LayoutKind.Sequential)] struct StartupEx { public Startup startup; public IntPtr attributes; }
    [StructLayout(LayoutKind.Sequential)] struct ProcessInfo { public IntPtr process,thread; public uint pid,tid; }
    [DllImport("kernel32.dll",SetLastError=true)] static extern bool CreatePipe(out IntPtr read,out IntPtr write,IntPtr security,uint size);
    [DllImport("kernel32.dll")] static extern int CreatePseudoConsole(Coord size,IntPtr input,IntPtr output,uint flags,out IntPtr console);
    [DllImport("kernel32.dll")] static extern void ClosePseudoConsole(IntPtr console);
    [DllImport("kernel32.dll",SetLastError=true)] static extern bool InitializeProcThreadAttributeList(IntPtr list,int count,uint flags,ref IntPtr size);
    [DllImport("kernel32.dll",SetLastError=true)] static extern bool UpdateProcThreadAttribute(IntPtr list,uint flags,IntPtr attribute,IntPtr value,IntPtr size,IntPtr previous,IntPtr returned);
    [DllImport("kernel32.dll")] static extern void DeleteProcThreadAttributeList(IntPtr list);
    [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] static extern bool CreateProcessW(string binary,StringBuilder command,IntPtr pa,IntPtr ta,bool inherit,uint flags,IntPtr environment,string directory,ref StartupEx startup,out ProcessInfo process);
    [DllImport("kernel32.dll")] static extern uint ResumeThread(IntPtr thread);
    [DllImport("kernel32.dll")] static extern uint WaitForSingleObject(IntPtr handle,uint timeout);
    [DllImport("kernel32.dll")] static extern bool GetExitCodeProcess(IntPtr handle,out uint exit);
    [DllImport("kernel32.dll",SetLastError=true)] static extern bool ReadFile(IntPtr handle,byte[] bytes,uint size,out uint read,IntPtr overlapped);
    [DllImport("kernel32.dll",SetLastError=true)] static extern bool WriteFile(IntPtr handle,byte[] bytes,uint size,out uint written,IntPtr overlapped);
    [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
    [DllImport("kernel32.dll")] static extern bool TerminateProcess(IntPtr process,uint exit);
    [DllImport("kernel32.dll")] static extern IntPtr GetStdHandle(int kind);
    [DllImport("kernel32.dll")] static extern bool SetStdHandle(int kind,IntPtr handle);
    [DllImport("kernel32.dll",SetLastError=true)] static extern uint WaitForMultipleObjects(uint count,IntPtr[] handles,bool all,uint milliseconds);
    static void Require(bool ok,string name) { if (!ok) throw new Win32Exception(Marshal.GetLastWin32Error(),name); }

    static void DrainOutput(IntPtr output,string path,string recovery) {
        FileStream log=null;
        Exception failure=null;
        try { log=new FileStream(path,FileMode.Create,FileAccess.Write,FileShare.ReadWrite); }
        catch(Exception error) { failure=error; }
        byte[] bytes=new byte[4096];uint count;
        try {
            for(;;) {
                if(!ReadFile(output,bytes,(uint)bytes.Length,out count,IntPtr.Zero)) {
                    int error=Marshal.GetLastWin32Error();
                    if(error==109) break;
                    throw new Win32Exception(error,"output pipe read failed"+recovery);
                }
                if(count==0) break;
                if(failure==null) {
                    try { log.Write(bytes,0,(int)count); }
                    catch(Exception error) { failure=error; }
                }
            }
        } finally {
            try { log?.Dispose(); }
            catch(Exception error) { failure=failure??error; }
        }
        if(failure!=null) ExceptionDispatchInfo.Capture(failure).Throw();
    }

    static string FailureTail(string report) {
        using var log=new FileStream(Path.ChangeExtension(report,"output"),FileMode.Open,FileAccess.Read,FileShare.ReadWrite);
        log.Seek(Math.Max(0,log.Length-1400),SeekOrigin.Begin);
        using var tail=new StreamReader(log,Encoding.UTF8);
        return tail.ReadToEnd();
    }

    sealed class Publication : IDisposable {
        readonly AutoResetEvent changed=new AutoResetEvent(false);
        readonly FileSystemWatcher watcher;
        readonly object sync=new object();
        Exception failure;
        bool disposed;

        public Publication(string report,string recovery) {
            try {
                watcher=new FileSystemWatcher(Path.GetDirectoryName(report),Path.GetFileNameWithoutExtension(report)+".*");
                watcher.NotifyFilter=NotifyFilters.FileName;
                watcher.Created+=(sender,args)=>Notify(null);
                watcher.Renamed+=(sender,args)=>Notify(null);
                watcher.Error+=(sender,args)=>Notify(args.GetException());
                watcher.EnableRaisingEvents=true;
            } catch(Exception error) {
                watcher?.Dispose(); changed.Dispose();
                throw new IOException("publication watcher initialization failed"+recovery,error);
            }
        }
        void Notify(Exception error) {
            lock(sync) {
                if(disposed) return;
                if(error!=null) failure=error;
                changed.Set();
            }
        }
        public void Await(string path,IntPtr child,Stopwatch timer,string recovery) {
            string step="publication "+Path.GetFileName(path);
            var handles=new[]{child,changed.SafeWaitHandle.DangerousGetHandle()};
            for(;;) {
                lock(sync) { if(failure!=null) throw new IOException(step+" watcher failed"+recovery,failure); }
                if(File.Exists(path)) return;
                long remaining=1500-timer.ElapsedMilliseconds;
                if(remaining<=0) throw new TimeoutException(step+" deadline"+recovery);
                uint result=WaitForMultipleObjects(2,handles,false,(uint)remaining);
                if(result==0) {
                    if(File.Exists(path)) return;
                    throw new EndOfStreamException(step+" producer exited"+recovery);
                }
                if(result==1) continue;
                if(result==258) throw new TimeoutException(step+" deadline"+recovery);
                throw new Win32Exception(Marshal.GetLastWin32Error(),step+" wait failed"+recovery);
            }
        }
        public void Dispose() {
            watcher.Dispose();
            lock(sync) { disposed=true; changed.Dispose(); }
        }
    }

    public static void Run(string binary,string scenario,string report) {
        IntPtr ir=IntPtr.Zero,iw=IntPtr.Zero,orr=IntPtr.Zero,ow=IntPtr.Zero,console=IntPtr.Zero,attributes=IntPtr.Zero;
        ProcessInfo process=new ProcessInfo(); bool initialized=false; Thread drain=null,close=null;
        Exception drainFailure=null,failure=null;
        bool drained=false;
        var draining=new ManualResetEvent(scenario!="blocked");
        bool frameCost=scenario=="regions"||scenario=="frames";
        string recovery="; recovery: pwsh -File scripts/test-terminal-signals.ps1 -Binary \""+binary+"\" "+(frameCost?"-FrameRegions":"-Scenario "+scenario);
        try {
            Require(CreatePipe(out ir,out iw,IntPtr.Zero,0),"input pipe");
            Require(CreatePipe(out orr,out ow,IntPtr.Zero,0),"output pipe");
            Require(CreatePseudoConsole(new Coord{x=(short)(scenario=="regions"?100:80),y=(short)(scenario=="regions"?40:24)},ir,ow,0,out console)==0,"CreatePseudoConsole");
            IntPtr size=IntPtr.Zero;
            InitializeProcThreadAttributeList(IntPtr.Zero,1,0,ref size);
            attributes=Marshal.AllocHGlobal(size);
            Require(InitializeProcThreadAttributeList(attributes,1,0,ref size),"attributes"); initialized=true;
            Require(UpdateProcThreadAttribute(attributes,0,new IntPtr(0x20016),console,new IntPtr(IntPtr.Size),IntPtr.Zero,IntPtr.Zero),"ConPTY attribute");
            var startup=new StartupEx(); startup.startup.cb=Marshal.SizeOf(typeof(StartupEx)); startup.attributes=attributes;
            IntPtr parentInput=GetStdHandle(-10),parentOutput=GetStdHandle(-11),parentError=GetStdHandle(-12);
            try {
                Require(SetStdHandle(-10,IntPtr.Zero)&&SetStdHandle(-11,IntPtr.Zero)&&SetStdHandle(-12,IntPtr.Zero),"clear inherited handles");
                string test=scenario=="regions"?"regions::real_terminal_region_cost":scenario=="frames"?"real_terminal_frame_cost":"term::windows::tests::windows_lifecycle_child";
                Require(CreateProcessW(binary,new StringBuilder("\""+binary+"\" --exact "+test+" --nocapture"),
                    IntPtr.Zero,IntPtr.Zero,false,0x80000|4,IntPtr.Zero,Path.GetDirectoryName(binary),ref startup,out process),"ConPTY child");
            } finally {
                Require(SetStdHandle(-10,parentInput)&&SetStdHandle(-11,parentOutput)&&SetStdHandle(-12,parentError),"restore host handles");
            }
            CloseHandle(ir);ir=IntPtr.Zero;CloseHandle(ow);ow=IntPtr.Zero;
            IntPtr outputHandle=orr;
            drain=new Thread(()=>{
                try {
                    draining.WaitOne();
                    DrainOutput(outputHandle,Path.ChangeExtension(report,"output"),recovery);
                } catch(Exception error) { drainFailure=error; }
            });
            drain.IsBackground=true;drain.Start();
            Require(ResumeThread(process.thread)!=UInt32.MaxValue,"resume child");
            if(frameCost) {
                Require(WaitForSingleObject(process.process,10000)==0,"native frame cost child deadline"+recovery);
                uint exit;Require(GetExitCodeProcess(process.process,out exit)&&exit==0,"native frame cost child exit"+recovery);
                Require(File.Exists(report),"native frame cost assertion report"+recovery);
                Console.Write(File.ReadAllText(report));
                return;
            }
            if(scenario=="input-close") {
                Require(WaitForSingleObject(process.process,1500)==0,"input close child deadline"+recovery);
                uint exit;Require(GetExitCodeProcess(process.process,out exit)&&exit==0,"input close child exit"+recovery);
                Require(File.Exists(report),"native input close acknowledgement"+recovery);
                Console.WriteLine(File.ReadAllText(report));
                return;
            }
            using var publications=new Publication(report,recovery);
            var timer=Stopwatch.StartNew();
            string inputReady=Path.ChangeExtension(report,"input-ready");
            publications.Await(inputReady,process.process,timer,recovery);
            byte[] fixture=Encoding.ASCII.GetBytes(string.Concat(System.Linq.Enumerable.Repeat("0123456789",10)));
            uint written;
            Require(WriteFile(iw,fixture,(uint)fixture.Length,out written,IntPtr.Zero)&&written==fixture.Length,"native input fixture write");
            string probeReady=Path.ChangeExtension(report,"probe-ready");
            publications.Await(probeReady,process.process,timer,recovery);
            byte[] replies=Encoding.ASCII.GetBytes("\x1b[?1004;2$y\x1b[?2004;2$y\x1b[?2027;2$y\x1b[?1049;2$y\x1b[?9;2$y\x1b[?1000;2$y\x1b[?1001;2$y\x1b[?1002;2$y\x1b[?1003;2$y\x1b[?1005;2$y\x1b[?1006;2$y\x1b[?1015;2$y\x1b[?1016;2$y\x1b[?62;1c");
            Require(WriteFile(iw,replies,(uint)replies.Length,out written,IntPtr.Zero)&&written==replies.Length,"negotiation fixture write");
            foreach(int count in new[]{32,64}) {
                string wheelReady=Path.ChangeExtension(report,"wheel"+count+"-ready");
                publications.Await(wheelReady,process.process,timer,recovery);
                var packet=new StringBuilder();
                for(int index=0;index<count;index++) packet.Append(index%3==0?"\x1b[<65;2;1M":"\x1b[<64;2;1M");
                byte[] wheels=Encoding.ASCII.GetBytes(packet.ToString());
                Require(WriteFile(iw,wheels,(uint)wheels.Length,out written,IntPtr.Zero)&&written==wheels.Length,"physical wheel packet write");
                string done=Path.ChangeExtension(report,"wheel"+count+"-done");
                publications.Await(done,process.process,timer,recovery);
                Console.WriteLine("ConPTY "+scenario+": "+File.ReadAllText(done));
            }
            string ready=Path.ChangeExtension(report,"ready");
            publications.Await(ready,process.process,timer,recovery);
            if(scenario=="blocked") {
                draining.Set();
            }
            if(scenario=="close") {
                IntPtr closing=console;console=IntPtr.Zero;
                close=new Thread(()=>ClosePseudoConsole(closing));close.IsBackground=true;close.Start();
            }
            Require(WaitForSingleObject(process.process,1500)==0,"control-event exit deadline");
            if(close!=null) Require(close.Join(500),"ConPTY close deadline");
            Require(File.Exists(report),"synchronous restoration callback");
            string[] fields=File.ReadAllText(report).Split(' ');
            Require(fields.Length==9,"console snapshot fields");
            Require(fields[0]==(scenario=="close"?"2":"1"),"real OS control event type");
            for(int i=1;i<=4;i++) Require(fields[i]==fields[i+4],"exact original console state field "+i);
            Require(fields[3]=="437"&&fields[4]=="437","non-UTF8 original code pages");
            if(console!=IntPtr.Zero) { ClosePseudoConsole(console); console=IntPtr.Zero; }
            drained=drain.Join(500);
            Require(drained,"output drain completion deadline"+recovery);
            if(drainFailure!=null) throw new IOException("output drain failed"+recovery,drainFailure);
            string mouseTrace=File.ReadAllText(Path.ChangeExtension(report,"output"));
            foreach(int mode in new[]{1002,1006}) {
                int enabled=mouseTrace.IndexOf("\x1b[?"+mode+"h",StringComparison.Ordinal);
                int restored=mouseTrace.LastIndexOf("\x1b[?"+mode+"l",StringComparison.Ordinal);
                Require(enabled>=0&&restored>enabled,"physical mouse mode restored "+mode);
            }
            Console.WriteLine("ConPTY "+scenario+": original input/output modes and code pages restored");
        } catch(Exception error) {
            failure=error;
        } finally {
            draining.Set();
            if(process.process!=IntPtr.Zero) TerminateProcess(process.process,125);
            if(console!=IntPtr.Zero) ClosePseudoConsole(console);
            if(initialized) DeleteProcThreadAttributeList(attributes);
            if(attributes!=IntPtr.Zero) Marshal.FreeHGlobal(attributes);
            if(drain!=null&&!drained) drained=drain.Join(500);
            foreach(IntPtr handle in new[]{ir,iw,orr,ow,process.thread,process.process}) if(handle!=IntPtr.Zero) CloseHandle(handle);
        }
        if(failure!=null) {
            if(drainFailure!=null) Console.Error.WriteLine("child output capture failed: "+drainFailure.Message+recovery);
            if(drained) {
                try { Console.Error.WriteLine("ConPTY child output tail:\n"+FailureTail(report)); }
                catch(Exception error) { Console.Error.WriteLine("child output tail read failed: "+error.Message+recovery); }
            } else {
                Console.Error.WriteLine("child output drain did not complete"+recovery);
            }
            ExceptionDispatchInfo.Capture(failure).Throw();
        }
    }
}
'@
$previousReport = $env:ACYCLIC_TERMINAL_WINDOWS_REPORT
$previousEvent = $env:ACYCLIC_TERMINAL_WINDOWS_EVENT
$previousRegionReport = $env:ACYCLIC_TERMINAL_REGION_REPORT
try {
    $scenarios = if ($Scenario) { @($Scenario) } elseif ($FrameRegions) { @('regions','frames') } else { @('break','close','blocked','input-close') }
    foreach ($scenario in $scenarios) {
        $report = Join-Path $artifactRoot ($scenario + '.txt')
        $env:ACYCLIC_TERMINAL_WINDOWS_REPORT = $report
        $env:ACYCLIC_TERMINAL_WINDOWS_EVENT = $scenario
        $env:ACYCLIC_TERMINAL_REGION_REPORT = $report
        [TerminalSignalConPty]::Run($Binary,$scenario,$report)
    }
} finally {
    $env:ACYCLIC_TERMINAL_WINDOWS_REPORT = $previousReport
    $env:ACYCLIC_TERMINAL_WINDOWS_EVENT = $previousEvent
    $env:ACYCLIC_TERMINAL_REGION_REPORT = $previousRegionReport
}
