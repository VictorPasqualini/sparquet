# Sparquet Studio — local runner launcher for Windows.
#
# Sets a correct Spark environment (JAVA_HOME -> JDK 17, HADOOP_HOME -> a folder
# with winutils.exe) and starts the runner from the project venv. Run it from a
# PowerShell in the sparquet-studio directory:
#
#     .\run-runner.ps1
#
# Everything it exports lives only in the process it launches, so it never
# pollutes your machine's environment. See README.md -> Local runner.

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

# 1. JAVA_HOME -> newest JDK 17 (Spark 4 requires Java 17+). Re-detect when it is
#    missing, broken, or still pointing at an old 8/11 JDK.
$needsJava = (-not $env:JAVA_HOME) -or
             (-not (Test-Path "$env:JAVA_HOME\bin\java.exe")) -or
             ($env:JAVA_HOME -match "jdk1\.8|jdk-8|jdk-11|jre")
if ($needsJava) {
    $roots = @("C:\Program Files\Eclipse Adoptium", "C:\Program Files\Java")
    $jdk = Get-ChildItem $roots -Directory -ErrorAction SilentlyContinue |
           Where-Object { $_.Name -match "17" } |
           Sort-Object Name -Descending |
           Select-Object -First 1
    if ($jdk) { $env:JAVA_HOME = $jdk.FullName }
}
if ((-not $env:JAVA_HOME) -or (-not (Test-Path "$env:JAVA_HOME\bin\java.exe"))) {
    Write-Warning "No JDK 17 found. Install Temurin 17 and set JAVA_HOME, or Spark will not start."
} else {
    Write-Host "JAVA_HOME  = $env:JAVA_HOME"
}

# 2. HADOOP_HOME -> a folder whose bin\ holds winutils.exe (+ hadoop.dll).
$candidates = @($env:HADOOP_HOME, "C:\hadoop", "C:\Program Files\Apache\Hadoop") |
              Where-Object { $_ }
$hadoop = $candidates | Where-Object { Test-Path "$_\bin\winutils.exe" } | Select-Object -First 1
if ($hadoop) {
    $env:HADOOP_HOME = $hadoop
    $env:PATH = "$hadoop\bin;$env:PATH"
    Write-Host "HADOOP_HOME = $env:HADOOP_HOME"
} else {
    Write-Warning "winutils.exe not found. Put winutils.exe + hadoop.dll in C:\hadoop\bin (writes will fail without it)."
}

# 3. Launch the runner straight from the venv (no activation needed).
$py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
    Write-Warning "venv not found at .\.venv - falling back to 'python' on PATH."
    $py = "python"
}
Write-Host "Starting runner on http://127.0.0.1:8787 ..."
Write-Host ""
& $py -m uvicorn server.main:app --port 8787
