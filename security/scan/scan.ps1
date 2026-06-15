#requires -Version 5
<#
.SYNOPSIS
  Security/leak scanner. Scans a repo's git-TRACKED files against the leak + PII pattern catalogs.
  Read-only. Exit 0 = clean (below -FailLevel); 1 = findings >= -FailLevel; 2 = setup error.
.DESCRIPTION
  $0, dependency-free (PowerShell + git). Catalogs are public-safe generic shapes; tenant-specific
  literals live in a gitignored config.json (loaded as extra high-precision patterns when present).
.EXAMPLE
  .\scan.ps1 -RepoPath D:\PERDITIO_PLATFORM\reporium-api -FailLevel high
#>
[CmdletBinding()]
param(
  [string]$RepoPath = (Get-Location).Path,
  [string]$PolicyDir,
  [ValidateSet('critical','high','medium','low')][string]$FailLevel = 'high',
  [string]$ReportPath
)
$ErrorActionPreference = 'Continue'
$rank = @{ critical = 4; high = 3; medium = 2; low = 1 }
if (-not $PolicyDir) { $sd = Split-Path -Parent $MyInvocation.MyCommand.Path; $PolicyDir = Join-Path $sd '..\policy' }

function Load-Patterns($file) {
  if (-not (Test-Path $file)) { return @() }
  try { $j = Get-Content $file -Raw | ConvertFrom-Json } catch { Write-Warning "invalid JSON: $file"; return @() }
  $groups = $null
  foreach ($k in 'pattern_groups', 'patterns', 'pii_patterns', 'rules') { if ($j.$k) { $groups = $j.$k; break } }
  if (-not $groups) { if ($j -is [array]) { $groups = $j } else { $groups = @() } }
  foreach ($g in $groups) {
    $rx = if ($g.regex) { $g.regex } elseif ($g.pattern) { $g.pattern } else { $null }
    $sev = if ($g.severity) { [string]$g.severity } else { 'medium' }
    if ($rx) { [pscustomobject]@{ name = $g.name; regex = $rx; severity = $sev } }
  }
}

if (-not $PolicyDir) { Write-Error "policy dir not found"; exit 2 }
$pats = @()
$pats += Load-Patterns (Join-Path $PolicyDir 'leak-patterns.json')
$pats += Load-Patterns (Join-Path $PolicyDir 'pii-patterns.json')
# optional private high-precision literals (gitignored)
$cfg = Join-Path $RepoPath 'config.json'
if (Test-Path $cfg) { $pats += Load-Patterns $cfg }
if (-not $pats) { Write-Error "no patterns loaded from $PolicyDir"; exit 2 }

Push-Location $RepoPath
$files = @(git ls-files 2>$null)
Pop-Location
if (-not $files) { Write-Warning "no git-tracked files at $RepoPath"; }

$skipExt = @('.png','.jpg','.jpeg','.gif','.bmp','.ico','.pdf','.zip','.gz','.tgz','.7z','.woff','.woff2','.ttf','.eot','.mp4','.mov','.mp3','.lock','.min.js','.map')
$findings = New-Object System.Collections.ArrayList
foreach ($rel in $files) {
  $r = $rel.ToLower()
  if ($r -like 'policy/*' -or $r -like '*/policy/leak-patterns.json' -or $r -like '*/policy/pii-patterns.json') { continue } # catalogs describe patterns; don't self-flag
  $ext = [IO.Path]::GetExtension($r)
  if ($skipExt -contains $ext) { continue }
  $full = Join-Path $RepoPath $rel
  if (-not (Test-Path $full)) { continue }
  if ((Get-Item $full).Length -gt 2MB) { continue }
  $content = Get-Content $full -Raw -EA SilentlyContinue
  if (-not $content) { continue }
  foreach ($p in $pats) {
    try { $m = [regex]::Matches($content, $p.regex) } catch { continue }
    if ($m.Count -gt 0) {
      $idx = $m[0].Index; $line = ($content.Substring(0, $idx) -split "`n").Count
      [void]$findings.Add([pscustomobject]@{ severity = $p.severity; pattern = $p.name; file = $rel; line = $line; count = $m.Count })
    }
  }
}
$findings = $findings | Sort-Object @{ e = { $rank[$_.severity] }; Descending = $true }, file
$findings | Format-Table severity, pattern, @{ N = 'location'; E = { "$($_.file):$($_.line)" } }, count -AutoSize | Out-String | Write-Output
$bySev = ($findings | Group-Object severity | ForEach-Object { "$($_.Name)=$($_.Count)" }) -join ' '
Write-Output ("SCANNED $($files.Count) tracked files at $RepoPath | FINDINGS: $bySev | total $($findings.Count)")
if ($ReportPath) { $findings | Export-Csv $ReportPath -NoTypeInformation -Encoding UTF8; Write-Output "report -> $ReportPath" }
$failN = @($findings | Where-Object { $rank[$_.severity] -ge $rank[$FailLevel] }).Count
if ($failN -gt 0) { Write-Output "RESULT: FAIL ($failN findings >= $FailLevel)"; exit 1 }
Write-Output "RESULT: PASS (no findings >= $FailLevel)"; exit 0
