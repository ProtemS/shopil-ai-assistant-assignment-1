$ErrorActionPreference = 'Stop'
$projectPath = $PSScriptRoot
$pythonPath = Join-Path $projectPath '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonPath)) { throw 'Create .venv and install requirements first; see README.md.' }
& $pythonPath -m pip install 'llama-cpp-python==0.3.36' --only-binary=:all: --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/cpu
if ($LASTEXITCODE -ne 0) { throw 'Local inference package installation failed.' }
$modelPath = Join-Path $projectPath 'models'
New-Item -ItemType Directory -Force -Path $modelPath | Out-Null
$files = @(
    @('qwen2.5-7b-instruct-q4_k_m-00001-of-00002.gguf', 'DFCE12E3862A5283CCFB88221B48480E58745165DE856439950D0F22590580DB'),
    @('qwen2.5-7b-instruct-q4_k_m-00002-of-00002.gguf', '539CF93F78E887EDEA1C04E2D7D8CDACA9D01DAE9C9025BCB8ACCBE29DF3D72A')
)
foreach ($file in $files) {
    $destination = Join-Path $modelPath $file[0]
    if (-not (Test-Path -LiteralPath $destination)) {
        & curl.exe -L --fail ('https://huggingface.co/Qwen/Qwen2.5-7B-Instruct-GGUF/resolve/main/' + $file[0] + '?download=true') -o $destination
        if ($LASTEXITCODE -ne 0) { throw 'Download failed; check connectivity.' }
    }
    if ((Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash -ne $file[1]) { throw ('Model checksum mismatch: ' + $destination) }
}
$env:LOCAL_MODEL_PATH = Join-Path $modelPath $files[0][0]
$env:PYTHONUTF8 = '1'
$envFile = Join-Path $projectPath '.env'
if (-not (Test-Path -LiteralPath $envFile)) {
    [IO.File]::WriteAllText($envFile, 'LOCAL_MODEL_PATH=' + $env:LOCAL_MODEL_PATH.Replace('\','/') + [Environment]::NewLine)
}
Write-Output 'Model files verified. Run .\.venv\Scripts\python.exe app.py --mode live'
