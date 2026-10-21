# RutaSur - Sistema de seguimiento de envíos

RutaSur es un sistema de seguimiento y gestión de envíos basado en una arquitectura de eventos. Utiliza Kafka para la comunicación entre servicios, MongoDB para persistencia y Next.js para la interfaz web.

## Tecnologías

- Node.js y JavaScript
- Apache Kafka
- MongoDB con replica set `rs0`
- Next.js, React y TypeScript
- Docker Compose

## Requisitos previos

- Node.js y npm
- Docker Desktop en ejecución
- Git

## Instalación

Desde la raíz del proyecto:

```powershell
npm install
Set-Location .\frontend
npm install
Set-Location ..
```

## Iniciar la infraestructura

Desde la raíz:

```powershell
docker compose up -d
docker compose ps
```

Kafka se publica en el puerto `9092` y MongoDB en el puerto `27017`.

La configuración de MongoDB utiliza el replica set `rs0`. Si es la primera ejecución y todavía no está inicializado, hay que comprobar su estado antes de inicializarlo.

## Iniciar los servicios

Abrí una terminal de PowerShell por servicio. En cada terminal, ubicáte en la raíz del proyecto y ejecutá el comando correspondiente:

```powershell
node servicios/proyector/index.js
```

```powershell
node servicios/notificaciones/index.js
```

```powershell
node servicios/reprogramacion/index.js
```

```powershell
node servicios/monitor-sla/index.js
```

```powershell
node servicios/analitica/index.js
```

En otra terminal, iniciá el simulador:

```powershell
node simulador/index.js
```

## Iniciar la interfaz web

Desde la raíz del proyecto:

```powershell
Set-Location .\frontend
npm run dev -- --port 3001
```

Abrí `http://localhost:3001`.

## Funcionalidades principales

- Panel de envíos y torre de control.
- Consulta de alertas SLA.
- Indicadores de analítica.
- Simulación de eventos de envíos.
- Seguimiento público mediante `/tracking/[shipmentId]`.
- API de seguimiento mediante `/api/tracking/[shipmentId]`.

El seguimiento público expone información básica del envío y su historial, sin mostrar información interna del repartidor.

## Verificación

Para compilar el frontend:

```powershell
Set-Location .\frontend
npm run build
```

Para revisar el estado de Git:

```powershell
Set-Location ..
git status
git diff --check
```

Con el servidor web en ejecución, se puede verificar el seguimiento con un identificador de envío existente:

```powershell
Invoke-WebRequest "http://localhost:3001/tracking/ENVIO-0627ed46" -UseBasicParsing
Invoke-RestMethod "http://localhost:3001/api/tracking/ENVIO-0627ed46" |
    ConvertTo-Json -Depth 5
```

El identificador es un ejemplo probado anteriormente; puede cambiar según los datos presentes en MongoDB.

## Detener la infraestructura

```powershell
docker compose down
```

Este comando detiene los contenedores, pero conserva el volumen persistente de MongoDB. No uses `docker compose down -v` si querés conservar los datos.

## Estado de validación

La compilación de producción del frontend y las rutas de seguimiento fueron verificadas anteriormente. El resto de los requisitos funcionales debe comprobarse con pruebas específicas antes de considerar completa la entrega.