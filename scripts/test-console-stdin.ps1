param([Parameter(Mandatory=$true)][string]$Binary, [switch]$Cancellation)
$ErrorActionPreference = 'Stop'
$Binary = (Resolve-Path -LiteralPath $Binary).ProviderPath
$artifactRoot = Join-Path (Split-Path -Parent $PSScriptRoot) ('target/console-stdin-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $artifactRoot | Out-Null
Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.ComponentModel;
using System.Runtime.InteropServices;

public static class ConsoleStdinConPty {
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
    static void WaitFor(string path,IntPtr process,string name) {
        var timer=Stopwatch.StartNew();
        while(!File.Exists(path)&&timer.ElapsedMilliseconds<3000) {
            if(WaitForSingleObject(process,0)==0) throw new Exception("child exited before "+name);
            Thread.Sleep(1);
        }
        Require(File.Exists(path),name+" deadline");
    }

    // Runs the harness console child in a pseudoconsole, so its stdin is a
    // real console input buffer. The child holds std's stdin lock while it
    // drops the console reader, then reads typed text through it.
    public static void Run(string binary,string report,bool cancellation) {
        IntPtr ir=IntPtr.Zero,iw=IntPtr.Zero,orr=IntPtr.Zero,ow=IntPtr.Zero,console=IntPtr.Zero,attributes=IntPtr.Zero;
        ProcessInfo process=new ProcessInfo(); bool initialized=false; Thread drain=null;
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
                Require(CreateProcessW(binary,new StringBuilder("\""+binary+"\" --exact "+(cancellation?"term::windows::tests::stolen_console_read_child":"client::native_input::windows_tests::console_stdin_child")+" --nocapture"),
                    IntPtr.Zero,IntPtr.Zero,false,0x80000|4,IntPtr.Zero,Path.GetDirectoryName(binary),ref startup,out process),"ConPTY child");
            } finally {
                Require(SetStdHandle(-10,parentInput)&&SetStdHandle(-11,parentOutput)&&SetStdHandle(-12,parentError),"restore host handles");
            }
            CloseHandle(ir);ir=IntPtr.Zero;CloseHandle(ow);ow=IntPtr.Zero;
            IntPtr outputHandle=orr;
            drain=new Thread(()=>{ using(var log=new FileStream(Path.ChangeExtension(report,"output"),FileMode.Create,FileAccess.Write,FileShare.ReadWrite)) { byte[] bytes=new byte[4096];uint count;while(ReadFile(outputHandle,bytes,(uint)bytes.Length,out count,IntPtr.Zero)&&count>0) { log.Write(bytes,0,(int)count);log.Flush(); } } });
            drain.IsBackground=true;drain.Start();
            Require(ResumeThread(process.thread)!=UInt32.MaxValue,"resume child");
            WaitFor(Path.ChangeExtension(report,"ready"),process.process,"console reader");
            if(cancellation) {
                uint count; byte[] key=new byte[]{0x78};
                Require(WriteFile(iw,key,1,out count,IntPtr.Zero)&&count==1,"racing key");
                Require(WaitForSingleObject(process.process,3000)==0,"cancellation child exit");
                Require(File.ReadAllText(report)=="stolen key read cancelled and acknowledged","cancellation acknowledgement");
                Console.WriteLine("ConPTY: stolen key read cancelled and acknowledged");
                return;
            }
            // UTF-8 into the pseudoconsole arrives as UTF-16 key events; the
            // emoji is a surrogate pair. Ctrl+Z on its own line ends input.
            byte[] typed=Encoding.UTF8.GetBytes("h\u00e9llo \U0001F600\r");
            uint written;
            Require(WriteFile(iw,typed,(uint)typed.Length,out written,IntPtr.Zero)&&written==typed.Length,"typed input write");
            WaitFor(Path.ChangeExtension(report,"line"),process.process,"decoded line");
            byte[] eof=new byte[]{0x1a};
            Require(WriteFile(iw,eof,1,out written,IntPtr.Zero)&&written==1,"console EOF write");
            Require(WaitForSingleObject(process.process,3000)==0,"console child exit deadline");
            string[] lines=File.ReadAllText(report,Encoding.UTF8).Split((char)10);
            Require(lines.Length==3,"console child report: "+String.Join("|",lines));
            Console.WriteLine("ConPTY console stdin: "+lines[0]);
            Console.WriteLine("ConPTY console stdin: "+lines[1]);
            Require(lines[2]=="h\u00e9llo \U0001F600 then end of input","decoded console text: "+lines[2]);
            Console.WriteLine("ConPTY console stdin: decoded the typed line with its surrogate pair, then Ctrl+Z ended input");
        } finally {
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
$previousReport = $env:ACYCLIC_CONSOLE_STDIN_CHILD
try {
    $report = Join-Path $artifactRoot 'console.txt'
    $env:ACYCLIC_CONSOLE_STDIN_CHILD = $report
    [ConsoleStdinConPty]::Run($Binary,$report,$Cancellation.IsPresent)
} finally {
    $env:ACYCLIC_CONSOLE_STDIN_CHILD = $previousReport
}
