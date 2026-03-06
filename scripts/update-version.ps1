<#
.SYNOPSIS
    Updates the <Version> property in all .csproj files in the repository.

.DESCRIPTION
    Replaces the <Version> tag in each project's .csproj file with the
    specified version string. This is the equivalent of what the old
    PackageManager WinForms app did via its UpdateVersion / ReplaceVersion
    methods, now available as a standalone script.

.PARAMETER Version
    The version string to set (e.g. "8.4.0", "9.0.0-beta1").

.PARAMETER DryRun
    Show what would be changed without writing any files.

.EXAMPLE
    ./scripts/update-version.ps1 -Version 8.4.0
    ./scripts/update-version.ps1 -Version 9.0.0-beta1 -DryRun
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidatePattern('^\d+\.\d+\.\d+')]
    [string]$Version,

    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

$projects = @(
    'AerospikeClient',
    'AerospikeTest',
    'AerospikeBenchmarks',
    'AerospikeDemo',
    'AerospikeAdmin'
)

$pattern = '<Version>[^<]+</Version>'
$replacement = "<Version>$Version</Version>"
$changed = 0

foreach ($project in $projects) {
    $csprojPath = Join-Path $repoRoot "$project/$project.csproj"

    if (-not (Test-Path $csprojPath)) {
        Write-Warning "Not found: $csprojPath"
        continue
    }

    $bytes = [System.IO.File]::ReadAllBytes($csprojPath)
    $hasBom = $bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF
    $encoding = if ($hasBom) {
        New-Object System.Text.UTF8Encoding $true
    } else {
        New-Object System.Text.UTF8Encoding $false
    }
    $content = $encoding.GetString($bytes, $(if ($hasBom) { 3 } else { 0 }), $bytes.Length - $(if ($hasBom) { 3 } else { 0 }))

    if ($content -match '<Version>([^<]+)</Version>') {
        $oldVersion = $Matches[1]

        if ($oldVersion -eq $Version) {
            Write-Host "  $project.csproj — already at $Version"
            continue
        }

        $newContent = $content -replace $pattern, $replacement

        if ($DryRun) {
            Write-Host "  $project.csproj — $oldVersion -> $Version (dry run)"
        }
        else {
            $outBytes = $encoding.GetPreamble() + $encoding.GetBytes($newContent)
            [System.IO.File]::WriteAllBytes($csprojPath, $outBytes)
            Write-Host "  $project.csproj — $oldVersion -> $Version"
        }
        $changed++
    }
    else {
        Write-Warning "  $project.csproj — no <Version> tag found"
    }
}

if ($DryRun) {
    Write-Host "`n$changed file(s) would be updated (dry run)."
}
else {
    Write-Host "`n$changed file(s) updated to version $Version."
}
