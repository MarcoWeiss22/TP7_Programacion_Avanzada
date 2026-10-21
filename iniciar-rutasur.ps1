$ErrorActionPreference = "Stop"
$raiz = $PSScriptRoot
Set-Location $raiz

Write-Host "=== RUTASUR: ARRANQUE ==="

docker compose up -d
if ($LASTEXITCODE -ne 0) {
    throw "No se pudo iniciar Kafka y MongoDB."
}

# Esperar a que Kafka acepte conexiones.
$kafkaListo = $false
for ($i = 0; $i -lt 30; $i++) {
    docker exec rutasur-kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 --list *> $null
    if ($LASTEXITCODE -eq 0) {
        $kafkaListo = $true
        break
    }
    Start-Sleep -Seconds 2
}
if (-not $kafkaListo) {
    throw "Kafka no estuvo disponible después de 60 segundos."
}

# Verificar que MongoDB responda.
$mongoListo = $false
for ($i = 0; $i -lt 30; $i++) {
    docker exec rutasur-mongodb mongosh --quiet --eval `
        "db.adminCommand('ping').ok" *> $null
    if ($LASTEXITCODE -eq 0) {
        $mongoListo = $true
        break
    }
    Start-Sleep -Seconds 2
}
if (-not $mongoListo) {
    throw "MongoDB no estuvo disponible después de 60 segundos."
}

# No inicializamos automáticamente el replica set: si es una
# instalación nueva, se debe inicializar siguiendo el README.
$replicaSet = docker exec rutasur-mongodb mongosh --quiet --eval `
    "try { rs.status().ok } catch(e) { 0 }"
if (($LASTEXITCODE -ne 0) -or ($replicaSet -notmatch '1')) {
    throw "MongoDB responde, pero el replica set rs0 no está inicializado o no está listo. Revisá el README."
}

$servicios = @(
    "proyector",
    "notificaciones",
    "reprogramacion",
    "monitor-sla",
    "analitica"
)

foreach ($servicio in $servicios) {
    $procesos = Get-CimInstance Win32_Process
    $patron = "servicios[\\/]+$servicio[\\/]+index\.js"

    $activo = $procesos | Where-Object {
        $_.Name -eq "node.exe" -and $_.CommandLine -match $patron
    }

    if ($activo) {
        Write-Host "Ya activo: $servicio"
        continue
    }

    $archivo = Join-Path $raiz "servicios\$servicio\index.js"
    Start-Process -FilePath "node.exe" `
        -ArgumentList "`"$archivo`"" `
        -WorkingDirectory $raiz

    Write-Host "Iniciado: $servicio"
}

# No abrir otro frontend si el puerto 3001 ya está ocupado.
$listener = Get-NetTCPConnection -LocalPort 3001 -State Listen `
    -ErrorAction SilentlyContinue

if ($listener) {
    Write-Host "El puerto 3001 ya está ocupado; no se abre otro frontend."
} else {
    Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c npm run dev -- --port 3001" `
        -WorkingDirectory (Join-Path $raiz "frontend")
    Write-Host "Iniciado: frontend"
}

Write-Host ""
Write-Host "Arranque solicitado."
Write-Host "Interfaz: http://localhost:3001"
Write-Host "Simulador manual: node simulador/index.js feliz"
