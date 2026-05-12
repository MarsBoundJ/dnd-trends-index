# Run from this folder in a fresh PowerShell window.
# Produces trusight_breakdowns.html (self-contained, ready to print-to-PDF in browser).

$ErrorActionPreference = 'Stop'

# Try pandoc on PATH first; fall back to the default install location if needed
$pandoc = (Get-Command pandoc -ErrorAction SilentlyContinue).Source
if (-not $pandoc) {
    $candidate = 'C:\Program Files\Pandoc\pandoc.exe'
    if (Test-Path $candidate) {
        $pandoc = $candidate
    } else {
        Write-Error "pandoc not found on PATH and not at $candidate. Open a fresh PowerShell window (so PATH refreshes after the winget install) and try again."
        exit 1
    }
}

Write-Host "Using pandoc at: $pandoc"
Write-Host "Converting trusight_breakdowns_scratch.md -> trusight_breakdowns.html ..."

& $pandoc `
    'trusight_breakdowns_scratch.md' `
    -o 'trusight_breakdowns.html' `
    --standalone `
    --embed-resources `
    --css 'pdf_style.css' `
    --metadata 'pagetitle=IP Deep Dive: 19 Licensing Candidates'

if ($LASTEXITCODE -eq 0) {
    $size = (Get-Item 'trusight_breakdowns.html').Length / 1MB
    Write-Host ("Done. trusight_breakdowns.html created ({0:N1} MB)." -f $size)
    Write-Host "Next: double-click the HTML to open in your browser, then Ctrl+P -> Save as PDF."
} else {
    Write-Error "pandoc exited with code $LASTEXITCODE"
    exit $LASTEXITCODE
}
