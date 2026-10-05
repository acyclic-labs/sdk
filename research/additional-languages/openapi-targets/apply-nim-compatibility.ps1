[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $PackagesRoot
)

$ErrorActionPreference = 'Stop'
$maps = @(
    @{ Family = 'actors'; Prefix = 'acyclic_actors_v1_'; TypePrefix = 'AcyclicActorsV1'; ModelPrefix = 'model_AcyclicActorsV1'; ModelNamePrefix = 'model_acyclic_actors_v1_' },
    @{ Family = 'workers'; Prefix = 'acyclic_workers_v1_'; TypePrefix = 'AcyclicWorkersV1'; ModelPrefix = 'model_AcyclicWorkersV1'; ModelNamePrefix = 'model_acyclic_workers_v1_' },
    @{ Family = 'stream'; Prefix = 'acyclic_stream_v2_'; TypePrefix = 'AcyclicStreamV2'; ModelPrefix = 'model_AcyclicStreamV2'; ModelNamePrefix = 'model_acyclic_stream_v2_' },
    @{ Family = 'objects'; Prefix = 'acyclic_objects_v2_'; TypePrefix = 'AcyclicObjectsV2'; ModelPrefix = 'model_AcyclicObjectsV2'; ModelNamePrefix = 'model_acyclic_objects_v2_' },
    @{ Family = 'inference'; Prefix = 'inference_customer_v1_'; TypePrefix = 'InferenceCustomerV1'; ModelPrefix = 'model_InferenceCustomerV1'; ModelNamePrefix = 'model_inference_customer_v1_' }
)

foreach ($map in $maps) {
    $package = Join-Path (Join-Path $PackagesRoot $map.Family) ("acyclic_{0}_nim" -f $map.Family)
    if (-not (Test-Path -LiteralPath $package -PathType Container)) { throw "Missing generated Nim package: $package" }
    foreach ($file in Get-ChildItem -LiteralPath $package -Recurse -Filter '*.nim' -File) {
        $source = Get-Content -LiteralPath $file.FullName -Raw
        # OpenAPI Generator 7.25.0 emits lower-case references for model types,
        # while its declarations are PascalCase. This preserves the Rust-derived
        # model identity without changing the generated schema or wire names.
        $source = $source -replace [regex]::Escape($map.Prefix), $map.TypePrefix
        # Import paths remain Nim module filenames and must stay snake_case.
        $source = $source -replace [regex]::Escape($map.ModelPrefix), $map.ModelNamePrefix
        if ($file.Name -eq 'api_default.nim') {
            $source = $source -replace '(?m)^import httpclient\r?\n', "import httpclient`r`nimport os`r`n"
            $source = $source -replace '(?m)^const basepath = "http://localhost"', 'let basepath = getEnv("ACYCLIC_BASE_URL", "http://localhost")'
        }
        Set-Content -LiteralPath $file.FullName -Value $source -Encoding utf8NoBOM
    }
    $byteArray = Join-Path $package 'models/model_byte_array.nim'
    Set-Content -LiteralPath $byteArray -Value ((@(
        '## Rust-owned wire identity: OpenAPI format byte maps to a base64 JSON string.',
        'type ByteArray* = string'
    ) -join [Environment]::NewLine) + [Environment]::NewLine) -Encoding utf8NoBOM
}

Write-Output "Applied Rust-owned Nim compatibility adapter to $PackagesRoot"
