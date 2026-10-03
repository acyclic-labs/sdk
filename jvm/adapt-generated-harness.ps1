param(
  [Parameter(Mandatory = $true)][string]$GeneratedRoot
)

$ErrorActionPreference = 'Stop'
$path = Join-Path $GeneratedRoot 'acyclic\harness\v2\Harness.java'
if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
  throw "Generated harness Java source is missing: $path"
}
$text = [System.IO.File]::ReadAllText($path)
if ($text.Contains('getFileDescriptor()')) {
  Write-Host 'Harness Java accessor adaptation already applied.'
  exit 0
}

function Edit-Section([string]$whole, [string]$startMarker, [string]$endMarker, [scriptblock]$edit) {
  $start = $whole.IndexOf($startMarker, [System.StringComparison]::Ordinal)
  if ($start -lt 0) { throw "Generated harness marker not found: $startMarker" }
  $end = $whole.IndexOf($endMarker, $start, [System.StringComparison]::Ordinal)
  if ($end -lt 0) { throw "Generated harness end marker not found: $endMarker" }
  $head = $whole.Substring(0, $start)
  $body = $whole.Substring($start, $end - $start)
  $tail = $whole.Substring($end)
  return $head + (& $edit $body) + $tail
}

$text = Edit-Section $text 'public interface FileRefOrBuilder' '   * Protobuf type {@code acyclic.harness.v2.FileRef}' {
  param($body)
  # The first following doc block starts FileRef; this section has only the
  # field accessors, so changing these names cannot touch descriptor metadata.
  $body.Replace('hasDescriptor', 'hasFileDescriptor').Replace('getDescriptorOrBuilder', 'getFileDescriptorOrBuilder').Replace('getDescriptor()', 'getFileDescriptor()')
}
$text = Edit-Section $text 'public static final class FileRef extends' '   * Protobuf type {@code acyclic.harness.v2.TaskOutcome}' {
  param($body)
  $body = $body.Replace('hasDescriptor', 'hasFileDescriptor')
  $body = $body.Replace('getDescriptorOrBuilder', 'getFileDescriptorOrBuilder')
  $body = $body.Replace('getDescriptorBuilder', 'getFileDescriptorBuilder')
  $body = $body.Replace('internalGetDescriptorFieldBuilder', 'internalGetFileDescriptorFieldBuilder')
  $body = $body.Replace('setDescriptor', 'setFileDescriptor')
  $body = $body.Replace('mergeDescriptor', 'mergeFileDescriptor')
  $body = $body.Replace('clearDescriptor', 'clearFileDescriptor')
  $body = $body.Replace('getDescriptor()', 'getFileDescriptor()')
  # Keep the class-descriptor accessors required by protobuf reflection; only
  # the field accessors are renamed.
  $descriptorDecl = 'public static final com.google.protobuf.Descriptors.Descriptor'
  # Restore the static class-descriptor methods after the field accessor rename.
  $body = [regex]::Replace($body, [regex]::Escape($descriptorDecl) + '(\s+)getFileDescriptor\(\)', {
    param($match)
    $descriptorDecl + $match.Groups[1].Value + 'getDescriptor()'
  })
  $body
}
$oldNames = 'new java.lang.String[] { "Volume", "NormalizedPath", "ImmutableVersion", "Descriptor", "DisplayName", }'
$newNames = 'new java.lang.String[] { "Volume", "NormalizedPath", "ImmutableVersion", "FileDescriptor", "DisplayName", }'
if (-not $text.Contains($oldNames)) { throw 'Harness FileRef FieldAccessorTable names were not found.' }
$text = $text.Replace($oldNames, $newNames)
[System.IO.File]::WriteAllText($path, $text, [System.Text.UTF8Encoding]::new($false))
Write-Host 'Applied generated harness FileRef accessor adaptation (descriptor wire name unchanged).'
