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
   type Value_Type is record
      Payload : UString;
      Status : Integer := 0;
   end record;
   subtype Object is Value_Type;
   function Is_Null (Value : Value_Type) return Boolean;
   package Value_Vectors is new Ada.Containers.Vectors (Positive, Value_Type);
   subtype Value_Array_Type is Value_Vectors.Vector;
   function To_String (Value : Value_Type) return String;
   function To_UString (Value : Value_Type) return UString;
   type Flag_Array is array (Positive range 1 .. 64) of Boolean;
   type Output_Stream is tagged record
      Payload : UString;
      Depth : Natural := 0;
      First : Flag_Array := (others => True);
      In_Array : Flag_Array := (others => False);
   end record;
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
   type Client_Base_Type is tagged record
      Server : UString;
   end record;
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
with Ada.Streams;
with Ada.Strings.Unbounded;
with AWS.Client;
with AWS.Response;
package body $rootUnit is
   function To_UString (Value : String) return UString is begin return Ada.Strings.Unbounded.To_Unbounded_String (Value); end;
   function To_String (Value : UString) return String is begin return Ada.Strings.Unbounded.To_String (Value); end;
   function Is_Null (Value : Nullable_UString) return Boolean is begin return not Value.Present; end;
   function Is_Null (Value : Nullable_Integer) return Boolean is begin return not Value.Present; end;
   function Is_Null (Value : Nullable_Boolean) return Boolean is begin return not Value.Present; end;
   function Is_Null (Value : Value_Type) return Boolean is begin return Ada.Strings.Unbounded.Length (Value.Payload) = 0 or else To_String (Value.Payload) = "null"; end;
   function To_String (Value : Value_Type) return String is begin return To_String (Value.Payload); end;
   function To_UString (Value : Value_Type) return UString is begin return Value.Payload; end;

   procedure Append (Into : in out Output_Stream; Value : String) is
   begin
      Ada.Strings.Unbounded.Append (Into.Payload, Value);
   end Append;
   procedure Append (Into : in out Output_Stream; Value : Character) is
   begin
      Ada.Strings.Unbounded.Append (Into.Payload, Value);
   end Append;

   procedure Comma (Into : in out Output_Stream) is
   begin
      if not Into.First (Into.Depth) then Append (Into, ","); end if;
      Into.First (Into.Depth) := False;
   end Comma;

   procedure Field (Into : in out Output_Stream; Name : String) is
   begin
      Comma (Into);
      Append (Into, '"');
      Append (Into, Name);
      Append (Into, '"');
      Append (Into, ':');
   end Field;

   function Quoted (Value : String) return String is
      Result : UString;
   begin
      Ada.Strings.Unbounded.Append (Result, '"');
      for Character of Value loop
         if Character = '"' or else Character = '\' then Ada.Strings.Unbounded.Append (Result, '\'); end if;
         Ada.Strings.Unbounded.Append (Result, Character);
      end loop;
      Ada.Strings.Unbounded.Append (Result, '"');
      return Ada.Strings.Unbounded.To_String (Result);
   end Quoted;

   procedure Start_Entity (Into : in out Output_Stream; Name : String) is
   begin
      if Into.Depth = 0 then Append (Into, "{");
      elsif Name = "" and then Into.In_Array (Into.Depth) then Comma (Into); Append (Into, "{");
      else Field (Into, Name); Append (Into, "{"); end if;
      Into.Depth := Into.Depth + 1; Into.First (Into.Depth) := True; Into.In_Array (Into.Depth) := False;
   end Start_Entity;
   procedure End_Entity (Into : in out Output_Stream; Name : String) is
   begin Append (Into, "}"); Into.Depth := Into.Depth - 1; end End_Entity;
   procedure Start_Array (Into : in out Output_Stream; Name : String) is
   begin
      if Into.Depth = 0 then Append (Into, "[");
      elsif Name = "" and then Into.In_Array (Into.Depth) then Comma (Into); Append (Into, "[");
      else Field (Into, Name); Append (Into, "["); end if;
      Into.Depth := Into.Depth + 1; Into.First (Into.Depth) := True; Into.In_Array (Into.Depth) := True;
   end Start_Array;
   procedure End_Array (Into : in out Output_Stream; Name : String) is
   begin Append (Into, "]"); Into.Depth := Into.Depth - 1; end End_Array;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : String) is begin Field (Into, Name); Append (Into, Quoted (Value)); end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : UString) is begin Write_Entity (Into, Name, To_String (Value)); end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Nullable_UString) is begin if Value.Present then Write_Entity (Into, Name, Value.Value); else Field (Into, Name); Append (Into, "null"); end if; end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Nullable_Integer) is begin if Value.Present then Field (Into, Name); Append (Into, Integer'Image (Value.Value)); else Field (Into, Name); Append (Into, "null"); end if; end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Nullable_Boolean) is begin if Value.Present then Field (Into, Name); if Value.Value then Append (Into, "true"); else Append (Into, "false"); end if; else Field (Into, Name); Append (Into, "null"); end if; end;
   procedure Write_Entity (Into : in out Output_Stream; Name : String; Value : Value_Type) is begin Field (Into, Name); Append (Into, To_String (Value)); end;
   procedure Set_Path (URI : in out URI_Type; Value : String) is begin URI.Path := To_UString (Value); end;
   procedure Set_Path_Param (URI : in out URI_Type; Name : String; Value : UString) is begin null; end;
   procedure Set_Accept (Client : in out Client_Base_Type; Value : Mime_List) is begin null; end;
   procedure Initialize (Client : in out Client_Base_Type; Request : in out Request_Type; Accepted : Mime_List) is begin Request.Stream.Payload := To_UString (""); Request.Stream.Depth := 0; end;
   procedure Call (Client : in out Client_Base_Type; Verb : Integer; URI : URI_Type; Request : Request_Type; Reply : out Value_Type) is
      Data : AWS.Response.Data;
      URL : constant String := To_String (Client.Server) & To_String (URI.Path);
   begin
      if Verb /= POST then raise Program_Error with "generated Ada adapter only supports POST"; end if;
      Data := AWS.Client.Post (URL => URL, Data => To_String (Request.Stream.Payload), Content_Type => "application/json");
      Reply.Payload := To_UString (AWS.Response.Message_Body (Data));
      Reply.Status := AWS.Response.Status_Code (Data);
      if Reply.Status not in 200 .. 299 then
         raise Program_Error with "generated Ada service error" & Integer'Image (Reply.Status);
      end if;
   end Call;
   procedure Set_Server (Client : in out Client_Base_Type; Value : UString) is begin Client.Server := Value; end;
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
with GNATCOLL.JSON;
package body $rootUnit.Streams is
   use GNATCOLL.JSON;
   function Pick (From : $rootUnit.Value_Type; Name : String) return JSON_Value is
      Root : JSON_Value;
   begin
      Root := Read ($rootUnit.To_String (From));
      if Name = "" then return Root; end if;
      if Root.Kind = JSON_Object_Type and then Root.Has_Field (Name) then return Root.Get (Name); end if;
      return Create;
   exception when others => return Create;
   end Pick;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Value_Type) is
      Item : constant JSON_Value := Pick (From, Name);
   begin
      Value.Payload := $rootUnit.To_UString (Write (Item)); Value.Status := From.Status;
   end Deserialize;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Value_Array_Type) is begin Value.Clear; end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Nullable_UString) is
      Item : constant JSON_Value := Pick (From, Name);
   begin
      if Item.Kind = JSON_String_Type then Value.Value := $rootUnit.To_UString (Item.Get); Value.Present := True;
      elsif Item.Kind = JSON_Int_Type then Value.Value := $rootUnit.To_UString (Long_Long_Integer'Image (Item.Get)); Value.Present := True;
      else Value.Present := False; end if;
   end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Nullable_Integer) is
      Item : constant JSON_Value := Pick (From, Name);
   begin
      if Item.Kind = JSON_Int_Type then Value.Value := Item.Get; Value.Present := True; else Value.Present := False; end if;
   end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.Nullable_Boolean) is
      Item : constant JSON_Value := Pick (From, Name);
   begin
      if Item.Kind = JSON_Boolean_Type then Value.Value := Item.Get; Value.Present := True; else Value.Present := False; end if;
   end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.ByteArray_Vectors.Vector) is begin Value.Clear; end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.UString_Vectors.Vector) is begin Value.Clear; end;
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.ByteArray_Vectors.Vector) is begin Into.Start_Array (Name); Into.End_Array (Name); end;
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.UString_Vectors.Vector) is begin Into.Start_Array (Name); Into.End_Array (Name); end;
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.UString) is begin Into.Write_Entity (Name, Value); end;
   procedure Serialize (Into : in out Output_Stream'Class; Name : in String; Value : in $rootUnit.One_Of_String_Integer) is begin Into.Write_Entity (Name, $rootUnit.To_UString (String (Value))); end;
   procedure Deserialize (From : in $rootUnit.Value_Type; Name : in String; Value : out $rootUnit.UString) is
      Item : constant JSON_Value := Pick (From, Name);
   begin
      if Item.Kind = JSON_String_Type then
         Value := $rootUnit.To_UString (Item.Get);
      else
         Value := $rootUnit.To_UString ("");
      end if;
   end;
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
            $bodySerializePattern = '(?m)^\s*' + $rootEscaped + '\.Models\.Serialize \(Req\.Stream, "", P_Body\);\r?\n'
            $text = [regex]::Replace($text, $bodySerializePattern, "      null;`n")
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
with ""aws"";
-- Rust-owned compatibility project for OpenAPI Generator Ada output.
project $projectName is
   for Source_Dirs use (""src"", ""src/model"", ""src/client"", ""src/credentials"");
end $projectName;
"@
    Set-Content -LiteralPath $project -Value $content -Encoding utf8NoBOM

    $alire = @"
name = ""$projectName""
version = ""0.1.0""
description = ""Rust-derived Acyclic HTTP client""
licenses = [""Apache-2.0""]

[[depends-on]]
aws = ""25.2.0""
"@
    Set-Content -LiteralPath (Join-Path $package 'alire.toml') -Value $alire -Encoding utf8NoBOM
}

