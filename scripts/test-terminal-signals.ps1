param([Parameter(Mandatory=$true)][string]$Binary)
$ErrorActionPreference = 'Stop'
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
    [DllImport("kernel32.dll")] static extern bool ReadFile(IntPtr handle,byte[] bytes,uint size,out uint read,IntPtr overlapped);
    [DllImport("kernel32.dll",SetLastError=true)] static extern bool WriteFile(IntPtr handle,byte[] bytes,uint size,out uint written,IntPtr overlapped);
    [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
    [DllImport("kernel32.dll")] static extern bool TerminateProcess(IntPtr process,uint exit);
    [DllImport("kernel32.dll")] static extern IntPtr GetStdHandle(int kind);
    [DllImport("kernel32.dll")] static extern bool SetStdHandle(int kind,IntPtr handle);
    static void Require(bool ok,string name) { if (!ok) throw new Win32Exception(Marshal.GetLastWin32Error(),name); }

    public static void Run(string binary,string scenario,string report) {
        IntPtr ir=IntPtr.Zero,iw=IntPtr.Zero,orr=IntPtr.Zero,ow=IntPtr.Zero,console=IntPtr.Zero,attributes=IntPtr.Zero;
        ProcessInfo process=new ProcessInfo(); bool initialized=false; Thread drain=null,close=null;
        var draining=new ManualResetEvent(scenario!="blocked");
        try {
            Require(CreatePipe(out ir,out iw,IntPtr.Zero,0),"input pipe");
            Require(CreatePipe(out orr,out ow,IntPtr.Zero,0),"output pipe");
            Require(CreatePseudoConsole(new Coord{x=80,y=24},ir,ow,0,out console)==0,"CreatePseudoConsole");
            IntPtr size=IntPtr.Zero;
            InitializeProcThreadAttributeList(IntPtr.Zero,1,0,ref size);
            attributes=Marshal.AllocHGlobal(size);
            Require(InitializeProcThreadAttributeList(attributes,1,0,ref size),"attributes"); initialized=true;
            Require(UpdateProcThreadAttribute(attributes,0,new IntPtr(0x20016),console,new IntPtr(IntPtr.Size),IntPtr.Zero,IntPtr.Zero),"ConPTY attribute");
            var startup=new StartupEx(); startup.startup.cb=Marshal.SizeOf(typeof(StartupEx)); startup.attributes=attributes;
            IntPtr parentInput=GetStdHandle(-10),parentOutput=GetStdHandle(-11),parentError=GetStdHandle(-12);
            try {
                Require(SetStdHandle(-10,IntPtr.Zero)&&SetStdHandle(-11,IntPtr.Zero)&&SetStdHandle(-12,IntPtr.Zero),"clear inherited handles");
                Require(CreateProcessW(binary,new StringBuilder("\""+binary+"\" --exact term::windows::tests::windows_lifecycle_child --nocapture"),
                    IntPtr.Zero,IntPtr.Zero,false,0x80000|4,IntPtr.Zero,Path.GetDirectoryName(binary),ref startup,out process),"ConPTY child");
            } finally {
                Require(SetStdHandle(-10,parentInput)&&SetStdHandle(-11,parentOutput)&&SetStdHandle(-12,parentError),"restore host handles");
            }
            CloseHandle(ir);ir=IntPtr.Zero;CloseHandle(ow);ow=IntPtr.Zero;
            IntPtr outputHandle=orr;
            drain=new Thread(()=>{ draining.WaitOne();using(var log=new FileStream(Path.ChangeExtension(report,"output"),FileMode.Create,FileAccess.Write,FileShare.ReadWrite)) { byte[] bytes=new byte[4096];uint count;while(ReadFile(outputHandle,bytes,(uint)bytes.Length,out count,IntPtr.Zero)&&count>0) { log.Write(bytes,0,(int)count);log.Flush(); } } });
            drain.IsBackground=true;drain.Start();
            Require(ResumeThread(process.thread)!=UInt32.MaxValue,"resume child");
            var timer=Stopwatch.StartNew();
            string inputReady=Path.ChangeExtension(report,"input-ready");
            while(!File.Exists(inputReady)&&timer.ElapsedMilliseconds<1500) {
                if(WaitForSingleObject(process.process,0)==0) throw new Exception("child exited before input fixture");
                Thread.Sleep(1);
            }
            Require(File.Exists(inputReady),"native input setup deadline");
            byte[] fixture=Encoding.ASCII.GetBytes(string.Concat(System.Linq.Enumerable.Repeat("0123456789",10)));
            uint written;
            Require(WriteFile(iw,fixture,(uint)fixture.Length,out written,IntPtr.Zero)&&written==fixture.Length,"native input fixture write");
            string probeReady=Path.ChangeExtension(report,"probe-ready");
            while(!File.Exists(probeReady)&&timer.ElapsedMilliseconds<1500) {
                if(WaitForSingleObject(process.process,0)==0) throw new Exception("child exited before negotiation");
                Thread.Sleep(1);
            }
            Require(File.Exists(probeReady),"negotiation setup deadline");
            byte[] replies=Encoding.ASCII.GetBytes("\x1b[?1004;2$y\x1b[?2004;2$y\x1b[?2027;2$y\x1b[?1049;2$y\x1b[?9;2$y\x1b[?1000;2$y\x1b[?1001;2$y\x1b[?1002;2$y\x1b[?1003;2$y\x1b[?1005;2$y\x1b[?1006;2$y\x1b[?1015;2$y\x1b[?1016;2$y\x1b[?62;1c");
            Require(WriteFile(iw,replies,(uint)replies.Length,out written,IntPtr.Zero)&&written==replies.Length,"negotiation fixture write");
            foreach(int count in new[]{32,64}) {
                string wheelReady=Path.ChangeExtension(report,"wheel"+count+"-ready");
                while(!File.Exists(wheelReady)&&timer.ElapsedMilliseconds<1500) {
                    if(WaitForSingleObject(process.process,0)==0) throw new Exception("child exited before wheel fixture");
                    Thread.Sleep(1);
                }
                Require(File.Exists(wheelReady),"physical wheel fixture ready");
                var packet=new StringBuilder();
                for(int index=0;index<count;index++) packet.Append(index%3==0?"\x1b[<65;2;1M":"\x1b[<64;2;1M");
                byte[] wheels=Encoding.ASCII.GetBytes(packet.ToString());
                Require(WriteFile(iw,wheels,(uint)wheels.Length,out written,IntPtr.Zero)&&written==wheels.Length,"physical wheel packet write");
                string done=Path.ChangeExtension(report,"wheel"+count+"-done");
                while(!File.Exists(done)&&timer.ElapsedMilliseconds<1500) {
                    if(WaitForSingleObject(process.process,0)==0) throw new Exception("child exited during wheel fixture");
                    Thread.Sleep(1);
                }
                Require(File.Exists(done),"physical wheel count acknowledgement");
                Console.WriteLine("ConPTY "+scenario+": "+File.ReadAllText(done));
            }
            string ready=Path.ChangeExtension(report,"ready");
            while(!File.Exists(ready)&&timer.ElapsedMilliseconds<1500) {
                if(WaitForSingleObject(process.process,0)==0) throw new Exception("child exited before terminal setup");
                Thread.Sleep(1);
            }
            if(!File.Exists(ready)) {
                draining.Set();
                WaitForSingleObject(process.process,500);
                string logPath=Path.ChangeExtension(report,"output");
                string log="no output";
                if(File.Exists(logPath)) using(var read=new StreamReader(new FileStream(logPath,FileMode.Open,FileAccess.Read,FileShare.ReadWrite))) log=read.ReadToEnd();
                throw new Exception("terminal setup deadline: "+log.Substring(Math.Max(0,log.Length-1400)));
            }
            if(scenario=="blocked") {
                draining.Set();
                File.WriteAllText(Path.ChangeExtension(report,"draining"),"draining");
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
            string mouseTrace="", outputLog=Path.ChangeExtension(report,"output");
            do {
                if(File.Exists(outputLog)) using(var read=new StreamReader(new FileStream(outputLog,FileMode.Open,FileAccess.Read,FileShare.ReadWrite))) mouseTrace=read.ReadToEnd();
                if(mouseTrace.Contains("\x1b[?1002l")&&mouseTrace.Contains("\x1b[?1006l")) break;
                Thread.Sleep(1);
            } while(timer.ElapsedMilliseconds<1500);
            foreach(int mode in new[]{1002,1006}) {
                int enabled=mouseTrace.IndexOf("\x1b[?"+mode+"h",StringComparison.Ordinal);
                int restored=mouseTrace.LastIndexOf("\x1b[?"+mode+"l",StringComparison.Ordinal);
                Require(enabled>=0&&restored>enabled,"physical mouse mode restored "+mode);
            }
            Console.WriteLine("ConPTY "+scenario+": original input/output modes and code pages restored");
        } finally {
            draining.Set();
            if(process.process!=IntPtr.Zero) TerminateProcess(process.process,125);
            if(console!=IntPtr.Zero) ClosePseudoConsole(console);
            if(initialized) DeleteProcThreadAttributeList(attributes);
            if(attributes!=IntPtr.Zero) Marshal.FreeHGlobal(attributes);
            foreach(IntPtr handle in new[]{ir,iw,orr,ow,process.thread,process.process}) if(handle!=IntPtr.Zero) CloseHandle(handle);
            if(drain!=null) drain.Join(500);
        }
    }
}
'@
$previousReport = $env:ACYCLIC_TERMINAL_WINDOWS_REPORT
$previousEvent = $env:ACYCLIC_TERMINAL_WINDOWS_EVENT
try {
    foreach ($scenario in @('break','close','blocked')) {
        $report = Join-Path $artifactRoot ($scenario + '.txt')
        $env:ACYCLIC_TERMINAL_WINDOWS_REPORT = $report
        $env:ACYCLIC_TERMINAL_WINDOWS_EVENT = $scenario
        [TerminalSignalConPty]::Run($Binary,$scenario,$report)
    }
} finally {
    $env:ACYCLIC_TERMINAL_WINDOWS_REPORT = $previousReport
    $env:ACYCLIC_TERMINAL_WINDOWS_EVENT = $previousEvent
}
