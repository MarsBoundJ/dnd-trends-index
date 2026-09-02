@echo off
REM Run from this folder by double-clicking or 'make_html' in cmd/PS.
REM
REM This is a THIN WRAPPER around make_html.ps1 — it deliberately duplicates no
REM logic. It used to carry its own copy of the pandoc invocation, which meant
REM two implementations of one build that could drift apart. That is the same
REM failure this whole report-tooling pass exists to remove, so: one build, one
REM place, two entry points.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0make_html.ps1"
if errorlevel 1 (
    echo.
    echo Build failed. See the error above.
    pause
    exit /b 1
)
pause
