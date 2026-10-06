[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Repository,
    [Parameter(Mandatory)][string[]]$Files,
    [Parameter(Mandatory)][string]$Message,
    [ValidateRange(1, 20)][int]$Attempts = 8
)

$ErrorActionPreference = 'Stop'
$previousIndex = $env:GIT_INDEX_FILE
$privateIndex = $null
$pathspecFile = $null

function Invoke-CheckedGit([string[]]$GitArguments) {
    $result = @(& git -C $script:repositoryRoot @GitArguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "git $($GitArguments[0]) failed: $($result -join [Environment]::NewLine)"
    }
    return ($result -join "`n").Trim()
}

try {
    if ($env:GIT_OBJECT_DIRECTORY) {
        throw 'Use the repository object database; GIT_OBJECT_DIRECTORY must be unset.'
    }
    $script:repositoryRoot = (Resolve-Path -LiteralPath $Repository).Path
    $top = Invoke-CheckedGit @('rev-parse', '--show-toplevel')
    if ([IO.Path]::GetFullPath($top) -ne [IO.Path]::GetFullPath($repositoryRoot)) {
        throw 'Repository must name the exact checkout root.'
    }
    $branch = Invoke-CheckedGit @('symbolic-ref', '--quiet', 'HEAD')
    if ($branch -ne 'refs/heads/codex/rust-sdk-docs-source') {
        throw "Expected the isolated SDK branch; found $branch."
    }
    # Match the effective author and committer to the repository's existing
    # signature policy before producing any Git objects or updating a ref.
    $signersPath = Join-Path $repositoryRoot '.github/allowed_signers'
    if (-not (Test-Path -LiteralPath $signersPath -PathType Leaf)) {
        throw 'Repository signature policy is missing.'
    }
    $principals = @(Get-Content -LiteralPath $signersPath | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith('#')) {
            ($line -split '\s+', 2)[0] -split ','
        }
    } | Sort-Object -Unique)
    foreach ($identityKind in @('GIT_AUTHOR_IDENT', 'GIT_COMMITTER_IDENT')) {
        $identity = Invoke-CheckedGit @('var', $identityKind)
        if ($identity -notmatch '<([^<>]+)>\s+\d+\s+[+-]\d{4}$') {
            throw "Cannot parse the effective $identityKind identity."
        }
        $identityEmail = $Matches[1]
        if ($identityEmail -notin $principals) {
            throw "Effective $identityKind email $identityEmail is absent from .github/allowed_signers. Repair the accidental Git identity override before committing."
        }
    }
    $prefix = [IO.Path]::GetFullPath($repositoryRoot).TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
    $owned = @()
    foreach ($file in $Files) {
        $absolute = [IO.Path]::GetFullPath((Join-Path $repositoryRoot $file))
        if (-not $absolute.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Owned file escapes the repository: $file"
        }
        $relative = [IO.Path]::GetRelativePath($repositoryRoot, $absolute).Replace('\', '/')
        if ($relative -match '^\.(git|agents|codex|aws)(/|$)') {
            throw "Repository metadata is not an owned source file: $relative"
        }
        if (Test-Path -LiteralPath $absolute -PathType Container) {
            throw "List individual owned files, not directories: $relative"
        }
        $owned += $relative
    }
    $owned = @($owned | Sort-Object -Unique)
    if ($owned.Count -eq 0) { throw 'At least one owned file is required.' }

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        # Rebuild from the latest parent after a failed CAS. Reusing the old
        # tree with a new parent silently removes another agent's edits.
        $privateIndex = Join-Path ([IO.Path]::GetTempPath()) ("sdk-owned-" + [Guid]::NewGuid().ToString('N') + '.index')
        if (Test-Path -LiteralPath $privateIndex) { throw 'Private index already exists.' }
        $env:GIT_INDEX_FILE = $privateIndex
        $parent = Invoke-CheckedGit @('rev-parse', $branch)
        $null = Invoke-CheckedGit @('read-tree', $parent)
        # File-based literal pathspecs avoid Windows process argument limits.
        $pathspecFile = $privateIndex + '.paths'
        [IO.File]::WriteAllBytes($pathspecFile,
            [Text.Encoding]::UTF8.GetBytes(($owned -join [char]0) + [char]0))
        $null = Invoke-CheckedGit @('--literal-pathspecs', 'add', '-A',
            '--pathspec-from-file', $pathspecFile, '--pathspec-file-nul')
        Remove-Item -LiteralPath $pathspecFile
        $pathspecFile = $null
        $tree = Invoke-CheckedGit @('write-tree')
        $changed = Invoke-CheckedGit @('diff-tree', '--no-commit-id', '--name-only', '-r', $parent, $tree)
        if (-not $changed) {
            [PSCustomObject]@{ status = 'unchanged'; revision = $parent; files = $owned }
            return
        }
        $unexpected = @($changed -split "`n" | Where-Object { $_ -notin $owned })
        if ($unexpected.Count) { throw "Unowned paths entered the tree: $($unexpected -join ', ')" }
        $commit = Invoke-CheckedGit @('commit-tree', '-S', $tree, '-p', $parent, '-m', $Message)
        $cas = @(& git -C $repositoryRoot update-ref $branch $commit $parent 2>&1)
        if ($LASTEXITCODE -eq 0) {
            [PSCustomObject]@{ status = 'committed'; revision = $commit; parent = $parent; files = @($changed -split "`n"); attempts = $attempt }
            return
        }
        $observed = Invoke-CheckedGit @('rev-parse', $branch)
        if ($observed -eq $parent) { throw "Branch update failed: $($cas -join '`n')" }
        Remove-Item -LiteralPath $privateIndex
        $privateIndex = $null
$pathspecFile = $null
    }
    throw "Branch kept advancing across $Attempts attempts; owned files remain intact."
}
finally {
    if ($pathspecFile -and (Test-Path -LiteralPath $pathspecFile)) {
        Remove-Item -LiteralPath $pathspecFile
    }
    $env:GIT_INDEX_FILE = $previousIndex
    if ($privateIndex -and (Test-Path -LiteralPath $privateIndex)) {
        Remove-Item -LiteralPath $privateIndex
    }
}
