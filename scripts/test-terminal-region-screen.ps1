param([Parameter(Mandatory=$true)][string]$Binary)
$ErrorActionPreference = 'Stop'
$Binary = (Resolve-Path -LiteralPath $Binary).ProviderPath
$fixtures = Join-Path ([IO.Path]::GetTempPath()) ('acyclic-region-screen-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $fixtures | Out-Null
$previous = $env:ACYCLIC_REGION_SCREEN_FIXTURES
try {
    $env:ACYCLIC_REGION_SCREEN_FIXTURES = $fixtures
    & $Binary --exact native_profiles::export_windows_screen_fixtures --ignored --nocapture
    if ($LASTEXITCODE -ne 0) { throw 'Production fixture export failed' }
    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
public static class RegionScreenOracle {
    [StructLayout(LayoutKind.Sequential)] struct Coord { public short x,y; }
    [StructLayout(LayoutKind.Sequential)] struct Rect { public short left,top,right,bottom; }
    [StructLayout(LayoutKind.Explicit)] struct Cell {
        [FieldOffset(0)] public char character;
        [FieldOffset(2)] public ushort attributes;
    }
    [DllImport("kernel32.dll",SetLastError=true)] static extern IntPtr CreateConsoleScreenBuffer(uint access,uint sharing,IntPtr security,uint flags,IntPtr data);
    [DllImport("kernel32.dll",SetLastError=true)] static extern bool SetConsoleMode(IntPtr output,uint mode);
    [DllImport("kernel32.dll",SetLastError=true)] static extern bool SetConsoleScreenBufferSize(IntPtr output,Coord size);
    [DllImport("kernel32.dll",SetLastError=true)] static extern bool WriteFile(IntPtr output,byte[] bytes,uint size,out uint written,IntPtr overlapped);
    [DllImport("kernel32.dll",SetLastError=true)] static extern bool ReadConsoleOutputW(IntPtr output,[Out] Cell[] cells,Coord size,Coord origin,ref Rect region);
    [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
    static void Require(bool ok,string name) { if(!ok) throw new Win32Exception(Marshal.GetLastWin32Error(),name); }
    public static string[] Capture(byte[] bytes) {
        // Independent inactive buffer: the user's screen remains untouched.
        // Run attached to conhost or ConPTY; no new console is opened.
        IntPtr handle=CreateConsoleScreenBuffer(0xC0000000,3,IntPtr.Zero,1,IntPtr.Zero);
        Require(handle!=new IntPtr(-1),"CreateConsoleScreenBuffer");
        try {
            Require(SetConsoleScreenBufferSize(handle,new Coord{x=80,y=25}),"buffer size");
            Require(SetConsoleMode(handle,7),"enable VT processing");
            uint written;
            Require(WriteFile(handle,bytes,(uint)bytes.Length,out written,IntPtr.Zero)&&written==bytes.Length,"production frame write");
            var cells=new Cell[24*8]; var region=new Rect{left=0,top=0,right=23,bottom=7};
            Require(ReadConsoleOutputW(handle,cells,new Coord{x=24,y=8},new Coord(),ref region),"ReadConsoleOutputW");
            var rows=new string[8];
            for(int row=0;row<8;row++) {
                var text=new char[24];
                for(int col=0;col<24;col++) text[col]=cells[row*24+col].character;
                rows[row]=new string(text);
            }
            return rows;
        } finally { CloseHandle(handle); }
    }
}
'@
    $clock = [Diagnostics.Stopwatch]::StartNew()
    for ($index=0; $index -lt 4; $index++) {
        $expected = Get-Content -Raw -LiteralPath (Join-Path $fixtures "$index.json") | ConvertFrom-Json
        foreach ($control in @($false,$true)) {
            $name = if($control) { "$index-control.bin" } else { "$index.bin" }
            $actual = [RegionScreenOracle]::Capture([IO.File]::ReadAllBytes((Join-Path $fixtures $name)))
            $same = ($actual -join "`n") -ceq ($expected -join "`n")
            if($same -eq $control) { throw "ReadConsoleOutput screen mismatch: fixture=$index control=$control actual=$($actual -join '|')" }
        }
    }
    if($clock.Elapsed.TotalSeconds -ge 10) { throw 'Screen oracle exceeded 10 seconds' }
    Write-Output ("ReadConsoleOutput: 4 production screens and 4 missing-scroll controls passed in {0:F3}s" -f $clock.Elapsed.TotalSeconds)
} finally {
    $env:ACYCLIC_REGION_SCREEN_FIXTURES = $previous
    $resolved = [IO.Path]::GetFullPath($fixtures)
    $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    if (!$resolved.StartsWith($tempRoot,[StringComparison]::OrdinalIgnoreCase) -or
        !(Split-Path -Leaf $resolved).StartsWith('acyclic-region-screen-')) { throw 'Unexpected fixture path' }
    Remove-Item -LiteralPath $resolved -Recurse -Force
}
