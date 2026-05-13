# Batch-convert all pitch deliverables (4 .docx reports + 2 .pptx one-pagers)
# to PDF using LibreOffice headless mode. Overwrites any existing PDFs.
#
# Run from anywhere:
#   powershell -ExecutionPolicy Bypass -File <path to this script>

$ErrorActionPreference = 'Stop'

# ── Find soffice.exe ──
$candidates = @(
    'C:\Program Files\LibreOffice\program\soffice.exe',
    'C:\Program Files (x86)\LibreOffice\program\soffice.exe'
)
$soffice = $null
foreach ($c in $candidates) {
    if (Test-Path $c) { $soffice = $c; break }
}
if (-not $soffice) {
    Write-Error "soffice.exe not found. Install LibreOffice or edit this script to point at it."
    exit 1
}
Write-Host "Using LibreOffice at: $soffice"

# ── Build the file list ──
$pitchRoot = $PSScriptRoot
$reportDir = Join-Path $pitchRoot 'report'
$onepagersDir = Join-Path $pitchRoot 'onepagers'

$files = @(
    (Join-Path $reportDir 'trusight_report_ip_licensing.docx'),
    (Join-Path $reportDir 'trusight_report_part2_personas.docx'),
    (Join-Path $reportDir 'trusight_report_part2_proofs_personaA.docx'),
    (Join-Path $reportDir 'trusight_report_A1_LIVE_audit.docx'),
    (Join-Path $onepagersDir 'trusight_onepager_ip.pptx'),
    (Join-Path $onepagersDir 'trusight_onepager_capabilities.pptx')
)

# ── Convert each ──
$success = 0
$failed  = 0
foreach ($f in $files) {
    $name = Split-Path -Leaf $f
    if (-not (Test-Path $f)) {
        Write-Warning "SKIP (not found): $name"
        $failed++
        continue
    }
    $dir = Split-Path -Parent $f
    Write-Host ""
    Write-Host "Converting: $name ..."
    & $soffice --headless --convert-to pdf $f --outdir $dir 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $pdfName = [System.IO.Path]::ChangeExtension($name, '.pdf')
        $pdfPath = Join-Path $dir $pdfName
        if (Test-Path $pdfPath) {
            $sizeKB = [math]::Round((Get-Item $pdfPath).Length / 1KB)
            Write-Host "  -> $pdfName ($sizeKB KB)"
            $success++
        } else {
            Write-Warning "  -> exit 0 but PDF not found (?)"
            $failed++
        }
    } else {
        Write-Warning "  -> FAILED (exit $LASTEXITCODE)"
        $failed++
    }
}

Write-Host ""
Write-Host "Done. Converted $success / $($files.Count). Failed: $failed."
if ($failed -gt 0) {
    Write-Host ""
    Write-Host "If LibreOffice was open at the time of running, close it and rerun." -ForegroundColor Yellow
}
