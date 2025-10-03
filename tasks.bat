@echo off
REM === Luna AI Task Runner (Windows) ===

if "%1"=="run" (
    echo Starting Luna session server...
    .\.venv\Scripts\python.exe server.py
    goto end
)

if "%1"=="demo" (
    echo Running client demo...
    .\.venv\Scripts\python.exe client_demo.py
    goto end
)

if "%1"=="dev" (
    echo Installing dev dependencies...
    pip install -r requirements_dev.txt
    goto end
)

if "%1"=="test" (
    echo Running tests...
    .\.venv\Scripts\pytest.exe -v --cov=.
    goto end
)

if "%1"=="lint" (
    echo Linting with flake8...
    .\.venv\Scripts\flake8.exe .
    goto end
)

if "%1"=="format" (
    echo Formatting with black...
    .\.venv\Scripts\black.exe .
    goto end
)

if "%1"=="clean" (
    echo Cleaning up caches...
    for /d /r %%i in (__pycache__) do rmdir /s /q "%%i"
    goto end
)

if "%1"=="voice" (
    echo Testing ElevenLabs voice output...
    .\.venv\Scripts\python.exe - <<END
from voice_adapter import tts_generate
msg = "Hello, this is Luna. Your ElevenLabs voice is working."
path = tts_generate(msg, "test", "voice")
print("Voice test saved:", path)
END
    goto end
)

echo Unknown task. Use: run, demo, dev, test, lint, format, clean, voice

:end
