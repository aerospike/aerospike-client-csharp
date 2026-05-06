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
    [ValidatePattern('^\d+\.\d+\.\d+(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?(?:\+[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?$')]
    [string]$Version,

    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

$csprojFiles = Get-ChildItem -Path $repoRoot -Recurse -Filter '*.csproj' -File |
    Where-Object { $_.FullName -notmatch '[\\/](bin|obj)[\\/]' } |
    Sort-Object FullName

if ($csprojFiles.Count -eq 0) {
    Write-Warning "No .csproj files found under $repoRoot"
    return
}

$pattern = '<Version>[^<]+</Version>'
$replacement = "<Version>$Version</Version>"
$changed = 0

foreach ($csproj in $csprojFiles) {
    $csprojPath = $csproj.FullName
    $relativePath = [System.IO.Path]::GetRelativePath($repoRoot, $csprojPath)

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
            Write-Host "  $relativePath — already at $Version"
            continue
        }

        $newContent = $content -replace $pattern, $replacement

        if ($DryRun) {
            Write-Host "  $relativePath — $oldVersion -> $Version (dry run)"
        }
        else {
            $outBytes = $encoding.GetPreamble() + $encoding.GetBytes($newContent)
            [System.IO.File]::WriteAllBytes($csprojPath, $outBytes)
            Write-Host "  $relativePath — $oldVersion -> $Version"
        }
        $changed++
    }
    else {
        Write-Verbose "  $relativePath — no <Version> tag found, skipping"
    }
}

if ($DryRun) {
    Write-Host "`n$changed file(s) would be updated (dry run)."
}
else {
    Write-Host "`n$changed file(s) updated to version $Version."
}
