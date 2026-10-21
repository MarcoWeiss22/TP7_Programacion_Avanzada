$ErrorActionPreference = "Stop"
$raiz = $PSScriptRoot
Set-Location $raiz

Write-Host "Verificando Docker..."
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    throw "No se pudo iniciar Docker Compose."
}

$servicios = @(
    "proyector",
    "notificaciones",
    "reprogramacion",
    "monitor-sla",
    "analitica"
)

$procesos = Get-CimInstance Win32_Process

foreach ($servicio in $servicios) {
    $patron = "servicios\\$servicio\\index\.js"
    $activo = $procesos | Where-Object {
        $_.Name -eq "node.exe" -and $_.CommandLine -match $patron
    }

    if ($activo) {
        Write-Host "Ya activo: $servicio"
        continue
    }

    $archivo = Join-Path $raiz "servicios\$servicio\index.js"
    Start-Process -FilePath "node" `
        -ArgumentList "`"$archivo`"" `
        -WorkingDirectory $raiz

    Write-Host "Iniciado: $servicio"
}

$frontendActivo = $procesos | Where-Object {
    $_.CommandLine -match 'next dev.*--port 3001'
}

if (-not $frontendActivo) {
    Start-Process -FilePath "npm.cmd" `
        -ArgumentList "run", "dev", "--", "--port", "3001" `
        -WorkingDirectory (Join-Path $raiz "frontend")
    Write-Host "Iniciado: frontend"
} else {
    Write-Host "Ya activo: frontend"
}

Write-Host ""
Write-Host "RutaSur iniciado."
Write-Host "Interfaz: http://localhost:3001"
Write-Host "Para simular un envio: node simulador/index.js feliz"
