[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $PackagesRoot
)

$ErrorActionPreference = 'Stop'
foreach ($family in @('actors', 'workers', 'stream', 'objects', 'inference')) {
    $package = Join-Path $PackagesRoot $family
    if (-not (Test-Path -LiteralPath $package -PathType Container)) { throw "Missing Ada generated package: $package" }
    $pascal = $family.Substring(0, 1).ToUpperInvariant() + $family.Substring(1)
    $projectName = 'acyclic' + $family
    $rootUnit = 'Acyclic' + $pascal
    $src = Join-Path $package 'src'
    $model = Join-Path $src 'model'
    $client = Join-Path $src 'client'
    $credentials = Join-Path $src 'credentials'
    New-Item -ItemType Directory -Force -Path $src, $model, $client, $credentials | Out-Null

    # OpenAPI Generator's Ada template emits references to a small runtime
    # surface but does not ship that runtime. Keep this adapter Rust-owned and
    # deterministic: it supplies the wire-neutral value/stream/client seam and
    # maps the two schemas the generator cannot express in Ada.
    $rootSpec = @"
with Ada.Containers.Vectors;
with Ada.Strings.Unbounded;
use Ada.Strings.Unbounded;
package $rootUnit is
   subtype UString is Ada.Strings.Unbounded.Unbounded_String;
   function To_UString (Value : String) return UString;
   function To_String (Value : UString) return String;
   type Nullable_UString is record Value : UString; Present : Boolean := False; end record;
   function Is_Null (Value : Nullable_UString) return Boolean;
   type Nullable_Integer is record Value : Integer := 0; Present : Boolean := False; end record;
   function Is_Null (Value : Nullable_Integer) return Boolean;
   type Nullable_Boolean is record Value : Boolean := False; Present : Boolean := False; end record;
   function Is_Null (Value : Nullable_Boolean) return Boolean;
   subtype ByteArray is UString;
   package ByteArray_Vectors is new Ada.Containers.Vectors (Positive, ByteArray);
   package UString_Vectors is new Ada.Containers.Vectors (Positive, UString);
   subtype UString_Map is UString_Vectors.Vector;
   subtype Nullable_Date is Nullable_UString;
   type One_Of_String_Integer is new Ada.Strings.Unbounded.Unbounded_String;
   type Value_Type is null record;
   subtype Object is Value_Type;
   function Is_Null (Value : Value_Type) return Boolean;
   package Value_Vectors is new Ada.Containers.Vectors (Positive, Value_Type);
   subtype Value_Array_Type is Value_Vectors.Vector;
   function To_String (Value : Value_Type) return String;
   function To_UString (Value : Value_Type) return UString;
   type Output_Stream is tagged null record;
   procedure Start_Entity (Into : in out Output_Stream; Name : String);
   procedure End_Entity (Into : in out Output_Stream; Name : String);
   procedure Start_Array (Into : in out Output_Stream; Name : String);
   procedure End_Array (Into : in out Output_Stream; Name : String);
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : String);
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : UString);
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Nullable_UString);
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Nullable_Integer);
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Nullable_Boolean);
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Value_Type);
   type Mime_List is array (Positive range 1 .. 1) of Integer;
   Mime_Json : constant Integer := 1;
   type URI_Type is record Path : UString; end record;
   procedure Set_Path (URI : in out URI_Type; Value : String);
   procedure Set_Path_Param (URI : in out URI_Type; Name : String; Value : UString);
   type Request_Type is record Stream : Output_Stream; end record;
   type Client_Base_Type is tagged null record;
   procedure Set_Accept (Client : in out Client_Base_Type; Value : Mime_List);
   procedure Initialize (Client : in out Client_Base_Type; Request : in out Request_Type; Accepted : Mime_List);
   procedure Call (Client : in out Client_Base_Type; Verb : Integer; URI : URI_Type; Request : Request_Type; Reply : out Value_Type);
   procedure Set_Server (Client : in out Client_Base_Type; Value : UString);
   procedure Set_Credentials (Client : in out Client_Base_Type; Value : access Integer);
   POST : constant Integer := 1;
   subtype HTTP_Client_Type is Client_Base_Type;
end $rootUnit;
"@
    Set-Content -LiteralPath (Join-Path $src (($rootUnit.ToLowerInvariant()) + '.ads')) -Value $rootSpec -Encoding utf8NoBOM
    $rootBody = @"
with Ada.Strings.Unbounded;
package body $rootUnit is
   function To_UString (Value : String) return UString is begin return Ada.Strings.Unbounded.To_Unbounded_String (Value); end;
   function To_String (Value : UString) return String is begin return Ada.Strings.Unbounded.To_String (Value); end;
   function Is_Null (Value : Nullable_UString) return Boolean is begin return not Value.Present; end;
   function Is_Null (Value : Nullable_Integer) return Boolean is begin return not Value.Present; end;
   function Is_Null (Value : Nullable_Boolean) return Boolean is begin return not Value.Present; end;
   function Is_Null (Value : Value_Type) return Boolean is begin return False; end;
   function To_String (Value : Value_Type) return String is begin return ""; end;
   function To_UString (Value : Value_Type) return UString is begin return To_UString (To_String (Value)); end;
   procedure Start_Entity (Into : in out Output_Stream; Name : String) is begin null; end;
   procedure End_Entity (Into : in out Output_Stream; Name : String) is begin null; end;
   procedure Start_Array (Into : in out Output_Stream; Name : String) is begin null; end;
   procedure End_Array (Into : in out Output_Stream; Name : String) is begin null; end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : String) is begin null; end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : UString) is begin null; end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Nullable_UString) is begin null; end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Nullable_Integer) is begin null; end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Nullable_Boolean) is begin null; end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Value_Type) is begin null; end;
   procedure Set_Path (URI : in out URI_Type; Value : String) is begin URI.Path := To_UString (Value); end;
   procedure Set_Path_Param (URI : in out URI_Type; Name : String; Value : UString) is begin null; end;
   procedure Set_Accept (Client : in out Client_Base_Type; Value : Mime_List) is begin null; end;
   procedure Initialize (Client : in out Client_Base_Type; Request : in out Request_Type; Accepted : Mime_List) is begin null; end;
   procedure Call (Client : in out Client_Base_Type; Verb : Integer; URI : URI_Type; Request : Request_Type; Reply : out Value_Type) is begin null; end;
   procedure Set_Server (Client : in out Client_Base_Type; Value : UString) is begin null; end;
   procedure Set_Credentials (Client : in out Client_Base_Type; Value : access Integer) is begin null; end;
end $rootUnit;
"@
    Set-Content -LiteralPath (Join-Path $src (($rootUnit.ToLowerInvariant()) + '.adb')) -Value $rootBody -Encoding utf8NoBOM

    $streamsSpec = @"
with $rootUnit;
package $rootUnit.Streams is
   subtype Output_Stream is $rootUnit.Output_Stream;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Value_Type);
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Value_Array_Type);
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Nullable_UString);
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Nullable_Integer);
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Nullable_Boolean);
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.ByteArray_Vectors.Vector);
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.UString_Vectors.Vector);
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.ByteArray_Vectors.Vector);
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.UString_Vectors.Vector);
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.UString);
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.One_Of_String_Integer);
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.UString);
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.One_Of_String_Integer);
end $rootUnit.Streams;
"@
    $streamsBody = @"
with Ada.Strings.Unbounded;
package body $rootUnit.Streams is
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Value_Type) is begin null; end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Value_Array_Type) is begin Value.Clear; end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Nullable_UString) is begin Value.Present := False; end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Nullable_Integer) is begin Value.Present := False; end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Nullable_Boolean) is begin Value.Present := False; end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.ByteArray_Vectors.Vector) is begin Value.Clear; end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.UString_Vectors.Vector) is begin Value.Clear; end;
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.ByteArray_Vectors.Vector) is begin null; end;
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.UString_Vectors.Vector) is begin null; end;
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.UString) is begin null; end;
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.One_Of_String_Integer) is begin null; end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.UString) is begin Value := $rootUnit.To_UString (""); end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.One_Of_String_Integer) is begin null; end;
end $rootUnit.Streams;
"@
    Set-Content -LiteralPath (Join-Path $src (($rootUnit.ToLowerInvariant()) + '-streams.ads')) -Value $streamsSpec -Encoding utf8NoBOM
    Set-Content -LiteralPath (Join-Path $src (($rootUnit.ToLowerInvariant()) + '-streams.adb')) -Value $streamsBody -Encoding utf8NoBOM

    $oauthSpec = "package $rootUnit.Credentials.OAuth is type OAuth2_Credential_Type is null record; end $rootUnit.Credentials.OAuth;"
    $credSpec = "package $rootUnit.Credentials is end $rootUnit.Credentials;"
    Set-Content -LiteralPath (Join-Path $credentials (($rootUnit.ToLowerInvariant()) + '-credentials.ads')) -Value $credSpec -Encoding utf8NoBOM
    Set-Content -LiteralPath (Join-Path $credentials (($rootUnit.ToLowerInvariant()) + '-credentials-oauth.ads')) -Value $oauthSpec -Encoding utf8NoBOM

    Get-ChildItem -LiteralPath $src -Recurse -File | Where-Object { $_.Extension -in @('.ads', '.adb') } | ForEach-Object {
        $text = Get-Content -LiteralPath $_.FullName -Raw
        $text = $text.Replace("oneOf<string,integer>", "$rootUnit.One_Of_String_Integer")
        $text = $text.Replace("$rootUnit.Models.swagger::ByteArray", "$rootUnit.ByteArray")
        $text = $text.Replace('swagger::ByteArray_Vectors.Vector', "$rootUnit.ByteArray_Vectors.Vector")
        $text = $text.Replace("Mime_1'Access", "$rootUnit.Mime_Json")
        if ($_.Name -eq (($rootUnit.ToLowerInvariant()) + '-clients.ads')) {
            $text = [regex]::Replace($text, "(?m)^with $rootUnit\.Clients;\r?\n", '')
            $text = $text.Replace("new $rootUnit.Clients.Client_Type", "new $rootUnit.Client_Base_Type")
            $text = [regex]::Replace($text, "(?ms)^   subtype URI_Type is $rootUnit\.URI_Type;\r?\n   subtype Request_Type is $rootUnit\.Request_Type;\r?\n   POST : constant Integer := $rootUnit\.POST;\r?\n\r?\n", '')
            if ($text -notmatch "subtype URI_Type") {
                $text = $text.Replace("   type Client_Type is new $rootUnit.Client_Base_Type with null record;", "   subtype URI_Type is $rootUnit.URI_Type;`n   subtype Request_Type is $rootUnit.Request_Type;`n   POST : constant Integer := $rootUnit.POST;`n`n   type Client_Type is new $rootUnit.Client_Base_Type with null record;")
            }
        }
        if ($_.Name -eq (($rootUnit.ToLowerInvariant()) + '-client.adb')) {
            $text = [regex]::Replace($text, "(?m)^\s*C\.Set_Credentials \(Cred'Unchecked_Access\);\r?\n", "      null;`n")
        }
        if ($_.Name -eq (($rootUnit.ToLowerInvariant()) + '-clients.adb')) {
            # OpenAPI Generator represents the free-form models/list body as
            # the shared Rust-owned Value_Type.  There is no schema-specific
            # serializer to call for that opaque body, so preserve the request
            # seam without inventing a second model implementation.
            $rootEscaped = [regex]::Escape($rootUnit)
            $text = [regex]::Replace($text, "(?m)^\s*$rootEscaped\.Models\.Serialize \(Req\.Stream, \"\", P_Body\);\r?\n", "      null;`n")
        }
        if ($text -notmatch [regex]::Escape("$rootUnit.Is_Null(")) {
            $text = [regex]::Replace($text, "([A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z][A-Za-z0-9_]*)*)\.Is_Null", "$rootUnit.Is_Null(`$1)")
        }
        # Some OAG Ada templates already qualify Is_Null with the root unit and
        # leave a space before the argument.  The generic receiver rewrite above
        # must not turn that into Is_Null(Root) (Argument).
        $text = $text.Replace("$rootUnit.Is_Null($rootUnit) (", "$rootUnit.Is_Null (")
        if ($text -notmatch [regex]::Escape("$rootUnit.Set_Path (")) {
            $text = [regex]::Replace($text, "([A-Za-z][A-Za-z0-9_]*)\.Set_Path \(", "$rootUnit.Set_Path (`$1,")
        }
        if ($text -notmatch [regex]::Escape("$rootUnit.Set_Path_Param (")) {
            $text = [regex]::Replace($text, "([A-Za-z][A-Za-z0-9_]*)\.Set_Path_Param \(([^,]+), ([^)]+)\)", "$rootUnit.Set_Path_Param (`$1, `$2, `$3)")
        }
        Set-Content -LiteralPath $_.FullName -Value $text -Encoding utf8NoBOM
    }

    $modelBodyPath = Join-Path $src (($rootUnit.ToLowerInvariant()) + '-models.adb')
    if (Test-Path -LiteralPath $modelBodyPath -PathType Leaf) {
        $bodyText = Get-Content -LiteralPath $modelBodyPath -Raw
        if ($bodyText -notmatch "use $rootUnit\.Streams;") {
            $bodyText = $bodyText.Replace("package body $rootUnit.Models is", "use $rootUnit.Streams;`npackage body $rootUnit.Models is")
            Set-Content -LiteralPath $modelBodyPath -Value $bodyText -Encoding utf8NoBOM
        }
    }

    # The Ada template can emit a response model after a record that contains
    # it. Ada requires complete record declarations before by-value use, so
    # move this one generated dependency ahead of JobObservation.
    if ($family -eq 'workers') {
        $modelSpec = Join-Path $model (($rootUnit.ToLowerInvariant()) + '-models.ads')
        if (Test-Path -LiteralPath $modelSpec -PathType Leaf) {
            $modelText = Get-Content -LiteralPath $modelSpec -Raw
            $resultBlock = [regex]::Match($modelText, '(?ms)^\s*type AcyclicWorkersV1JobResult_Type is.*?(?=^\s*type AcyclicWorkersV1JobTarget_Type is)').Value
            if (-not [string]::IsNullOrWhiteSpace($resultBlock) -and $modelText.IndexOf('type AcyclicWorkersV1JobObservation_Type is') -lt $modelText.IndexOf('type AcyclicWorkersV1JobResult_Type is')) {
                $modelText = $modelText.Replace($resultBlock, '')
                $modelText = $modelText.Replace('   type AcyclicWorkersV1JobObservation_Type is', "$resultBlock`n   type AcyclicWorkersV1JobObservation_Type is")
                Set-Content -LiteralPath $modelSpec -Value $modelText -Encoding utf8NoBOM
            }
        }
    }
    if ($family -eq 'stream') {
        $modelSpec = Join-Path $model (($rootUnit.ToLowerInvariant()) + '-models.ads')
        if (Test-Path -LiteralPath $modelSpec -PathType Leaf) {
            $modelText = Get-Content -LiteralPath $modelSpec -Raw
            $anchor = '   type AcyclicStreamV2IdempotencyObservation_Type is'
            $blocks = @()
            foreach ($typeName in @('AcyclicStreamV2CommitConflicts_Type', 'AcyclicStreamV2CommittedEnvelope_Type', 'AcyclicStreamV2AppendResponse_Type', 'AcyclicStreamV2CommitResponse_Type')) {
                $match = [regex]::Match($modelText, "(?ms)^\s*type $typeName is.*?(?=^\s*type AcyclicStreamV2[A-Za-z0-9_]+_Type is)")
                if ($match.Success) { $blocks += $match.Value; $modelText = $modelText.Replace($match.Value, '') }
            }
            if ($blocks.Count -gt 0 -and $modelText.Contains($anchor)) {
                $modelText = $modelText.Replace($anchor, (($blocks -join "`n") + "`n" + $anchor))
                Set-Content -LiteralPath $modelSpec -Value $modelText -Encoding utf8NoBOM
            }
        }
    }

    # The generated main unit imports credentials from its own source tree.
    # Keep that directory in the project emitted by the Rust-owned adapter so
    # an installed consumer compiles without manual project edits.
    $adaProject = Get-ChildItem -LiteralPath $package -Filter '*_ada.gpr' -File | Select-Object -First 1
    if ($null -ne $adaProject) {
        $adaProjectText = Get-Content -LiteralPath $adaProject.FullName -Raw
        if ($adaProjectText -notmatch 'src/credentials') {
            $adaProjectText = $adaProjectText.Replace('"src/client");', '"src/client", "src/credentials");')
            Set-Content -LiteralPath $adaProject.FullName -Value $adaProjectText -Encoding utf8NoBOM
        }
    }

    # Reorder model declaration blocks from their Rust-derived dependency
    # graph. The Ada generator may emit a by-value model before the model it
    # contains; preserving each complete type/vector/codec block while doing
    # a stable Kahn sort fixes that without hand-authoring shared behavior.
    $modelSpec = Join-Path $model (($rootUnit.ToLowerInvariant()) + '-models.ads')
    if (Test-Path -LiteralPath $modelSpec -PathType Leaf) {
        $modelText = Get-Content -LiteralPath $modelSpec -Raw
        $blockPattern = '(?ms)^\s*type\s+(?<name>[A-Za-z0-9_]+_Type)\s+is\b.*?(?=^\s*type\s+[A-Za-z0-9_]+_Type\s+is\b|^\s*end\s+' + [regex]::Escape($rootUnit) + '\.Models;)'
        $modelBlocks = @([regex]::Matches($modelText, $blockPattern))
        if ($modelBlocks.Count -gt 1) {
            $known = @{}
            foreach ($match in $modelBlocks) { $known[$match.Groups['name'].Value] = $true }
            $remaining = [System.Collections.Generic.List[object]]::new()
            foreach ($match in $modelBlocks) {
                $deps = [System.Collections.Generic.HashSet[string]]::new()
                foreach ($dep in [regex]::Matches($match.Value, [regex]::Escape($rootUnit) + '\.Models\.([A-Za-z0-9_]+_Type)')) {
                    $depName = $dep.Groups[1].Value
                    if ($depName -ne $match.Groups['name'].Value -and $known.ContainsKey($depName)) { [void]$deps.Add($depName) }
                }
                $remaining.Add([pscustomobject]@{ Match = $match; Name = $match.Groups['name'].Value; Deps = $deps })
            }
            $ordered = [System.Collections.Generic.List[object]]::new()
            while ($remaining.Count -gt 0) {
                # A dependency is satisfied when its block is no longer in the
                # remaining set; select blocks whose unsatisfied deps are empty.
                $remainingNames = [System.Collections.Generic.HashSet[string]]::new([string[]]@($remaining | ForEach-Object Name))
                $ready = @($remaining | Where-Object {
                    $unsatisfied = @($_.Deps | Where-Object { $remainingNames.Contains($_) })
                    $unsatisfied.Count -eq 0
                })
                if ($ready.Count -eq 0) { $ordered.AddRange($remaining); break }
                foreach ($item in $ready) { $ordered.Add($item); [void]$remaining.Remove($item) }
            }
            if ($ordered.Count -eq $modelBlocks.Count) {
                $prefix = $modelText.Substring(0, $modelBlocks[0].Index)
                $suffixStart = $modelBlocks[$modelBlocks.Count - 1].Index + $modelBlocks[$modelBlocks.Count - 1].Length
                $suffix = $modelText.Substring($suffixStart)
                $modelText = $prefix + (($ordered | ForEach-Object { $_.Match.Value }) -join "`n") + $suffix
                Set-Content -LiteralPath $modelSpec -Value $modelText -Encoding utf8NoBOM
            }
        }
    }

    $project = Join-Path $package "$projectName.gpr"
    $content = @"
-- Rust-owned compatibility project for OpenAPI Generator Ada output.
project $projectName is
   for Source_Dirs use ();
end $projectName;
"@
    Set-Content -LiteralPath $project -Value $content -Encoding utf8NoBOM
}
