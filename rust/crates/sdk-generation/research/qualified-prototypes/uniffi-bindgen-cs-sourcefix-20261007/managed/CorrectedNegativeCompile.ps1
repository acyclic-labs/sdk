param([Parameter(Mandatory = $true)][string]$AssemblyPath)

$ErrorActionPreference = 'Stop'
$rawSource = 'using Acyclic.Actors; public static class CorrectedRawNegative { public static PositiveU64 Make() => new PositiveU64(1UL, true); }'
$strongSource = 'using System.Threading; using Acyclic.Actors; public static class CorrectedStrongNegative { public static void Call(ActorsClient client) { client.InspectActor("actor-a", null, CancellationToken.None); } }'

foreach ($case in @(
    @{ Marker = 'NUPKG_CORRECTED_RAW_HANDLE_NEGATIVE'; Source = $rawSource; Pattern = 'CS1729' },
    @{ Marker = 'NUPKG_CORRECTED_STRONG_TYPE_NEGATIVE'; Source = $strongSource; Pattern = 'CS1503' }
)) {
    try {
        Add-Type -TypeDefinition $case.Source -ReferencedAssemblies $AssemblyPath -ErrorAction Stop
        Write-Output ("{0}=UNEXPECTED_COMPILE" -f $case.Marker)
        exit 1
    } catch {
        $text = $_.Exception.ToString()
        if ($text -notmatch $case.Pattern) { throw }
        Write-Output ("{0}=EXPECTED_FAIL" -f $case.Marker)
        ($text -split "`r?`n") | Where-Object { $_ -match $case.Pattern -or $_ -match 'PositiveU64|InspectActor|ActorId' } | ForEach-Object { Write-Output $_ }
    }
}
