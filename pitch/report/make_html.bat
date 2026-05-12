@echo off
REM Run from this folder by double-clicking or 'make_html' in cmd/PS.
REM Produces trusight_breakdowns.html (self-contained, ready to print-to-PDF).

set "PANDOC=pandoc"
where %PANDOC% >nul 2>nul
if errorlevel 1 (
    set "PANDOC=C:\Program Files\Pandoc\pandoc.exe"
    if not exist "%PANDOC%" (
        echo ERROR: pandoc not found on PATH or at default location.
        echo Open a fresh shell window so PATH refreshes after the winget install, then retry.
        pause
        exit /b 1
    )
)

echo Using pandoc: %PANDOC%
echo Converting trusight_breakdowns_scratch.md -^> trusight_breakdowns.html ...

"%PANDOC%" trusight_breakdowns_scratch.md -o trusight_breakdowns.html --standalone --embed-resources --css pdf_style.css --metadata "pagetitle=IP Deep Dive: 19 Licensing Candidates"

if errorlevel 1 (
    echo ERROR: pandoc exited with code %errorlevel%.
    pause
    exit /b %errorlevel%
)

echo.
echo Done. trusight_breakdowns.html created.
echo Next: double-click the HTML to open in your browser, then Ctrl+P -^> Save as PDF.
pause
