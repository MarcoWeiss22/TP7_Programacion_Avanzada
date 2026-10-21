# Informe técnico - RutaSur

## 1. Introducción

RutaSur es una aplicación de gestión y seguimiento de envíos que utiliza una arquitectura orientada a eventos. El objetivo es separar responsabilidades en servicios independientes y mantener información consultable desde una interfaz web.

## 2. Arquitectura

Los componentes principales son:

1. **Simulador:** genera eventos del ciclo de vida de los envíos.
2. **Kafka:** transporta eventos entre los distintos servicios.
3. **Proyector:** procesa eventos y actualiza la información persistida.
4. **Notificaciones:** procesa eventos relacionados con envíos y genera notificaciones.
5. **Reprogramación:** gestiona situaciones que requieren reprogramar una entrega.
6. **Monitor SLA:** evalúa situaciones relacionadas con los compromisos de entrega.
7. **Analítica:** calcula indicadores sobre la operación.
8. **MongoDB:** almacena envíos, eventos procesados, alertas y resultados.
9. **Frontend Next.js:** presenta la información y permite consultar el seguimiento.

## 3. Tecnologías

- Node.js para ejecutar los servicios.
- KafkaJS para la integración con Kafka.
- Apache Kafka como plataforma de eventos.
- MongoDB y su controlador para Node.js como almacenamiento.
- Next.js, React y TypeScript para la aplicación web.
- Docker Compose para levantar Kafka y MongoDB.

## 4. Infraestructura

La configuración Docker Compose define dos servicios:

- Kafka, publicado en el puerto 9092.
- MongoDB, publicado en el puerto 27017 y configurado para utilizar el replica set `rs0`.

El volumen `mongodb_data` permite conservar los datos de MongoDB al detener los contenedores sin eliminar el volumen.

Comandos habituales:

```powershell
docker compose up -d
docker compose ps
docker compose logs --tail 100
docker compose down
```

## 5. Eventos y estados

Entre los estados utilizados por el sistema se encuentran:

- `CREADO`
- `INGRESADO_HUB`
- `CLASIFICADO`
- `EN_REPARTO`
- `ENTREGADO`
- `ENTREGA_FALLIDA`

Los eventos permiten comunicar cambios entre servicios sin exigir que todos los componentes se ejecuten dentro de un único proceso.

Entre los tópicos contemplados por la implementación se encuentran `envios.estados`, `envios.promesas`, `envios.reprogramaciones`, `alertas.sla`, `notificaciones.enviadas` y `envios.estados.dlq`. La existencia, configuración y utilización efectiva de cada tópico debe confirmarse en la configuración y en la ejecución de Kafka.

## 6. Servicios

### 6.1. Proyector

Ubicación: `servicios/proyector/index.js`.

Procesa eventos y actualiza las vistas persistidas de los envíos. La implementación incluye mecanismos de control de eventos procesados y manejo de errores.

### 6.2. Notificaciones

Ubicación: `servicios/notificaciones/index.js`.

Consume eventos relacionados con envíos y publica eventos de notificación. La idempotencia, el tratamiento de mensajes inválidos y el envío a una cola de errores deben validarse mediante pruebas específicas.

### 6.3. Reprogramación

Ubicación: `servicios/reprogramacion/index.js`.

Contempla motivos de entrega fallida como ausencia del destinatario, dirección incorrecta, rechazo y zona inaccesible. Deben verificarse mediante pruebas las reglas, los contadores y el comportamiento tras reiniciar el servicio.

### 6.4. Monitor SLA

Ubicación: `servicios/monitor-sla/index.js`.

Evalúa eventos y compromisos de entrega para detectar posibles incumplimientos. Es necesario comprobar que no genere alertas duplicadas y que recupere correctamente su estado tras reinicios.

### 6.5. Analítica

Ubicación: `servicios/analitica/index.js`.

Calcula indicadores operativos, como la tasa de entrega en el primer intento, los fallos por zona o motivo, el cumplimiento de SLA y los tiempos entre etapas logísticas. Los filtros, la reproducción histórica y la actualización de indicadores requieren validación independiente.

### 6.6. Simulador

Ubicación: `simulador/index.js`.

Genera eventos para probar el flujo de los envíos. Debe comprobarse que la configuración temporal controle realmente los intervalos entre eventos y que existan escenarios reproducibles.

## 7. Persistencia

La implementación utiliza colecciones de MongoDB para almacenar información de envíos, alertas, métricas, eventos procesados y datos de SLA y analítica.

Los nombres y esquemas exactos deben contrastarse con las operaciones de lectura y escritura de cada servicio; no se asume que todas las colecciones estén activas por el solo hecho de mencionarlas.

## 8. Interfaz y seguimiento público

La aplicación web se encuentra en `frontend`.

Las rutas de seguimiento implementadas son:

- Página: `/tracking/[shipmentId]`.
- API: `/api/tracking/[shipmentId]`.

La API consulta el envío por identificador, devuelve el estado y el historial disponible y responde con un error HTTP 404 cuando no encuentra el envío. La página muestra la información pública y actualiza la consulta periódicamente.

Se verificó previamente una compilación de producción y una respuesta HTTP 200 de la página de seguimiento para un envío de prueba. También se comprobó una respuesta JSON con estado e historial mediante la API.

## 9. Instalación y ejecución

Instalar dependencias:

```powershell
npm install
Set-Location .\frontend
npm install
Set-Location ..
```

Levantar la infraestructura:

```powershell
docker compose up -d
```

Ejecutar los servicios y el simulador en terminales separadas:

```powershell
node servicios/proyector/index.js
node servicios/notificaciones/index.js
node servicios/reprogramacion/index.js
node servicios/monitor-sla/index.js
node servicios/analitica/index.js
node simulador/index.js
```

Iniciar el frontend:

```powershell
Set-Location .\frontend
npm run dev -- --port 3001
```

## 10. Pruebas y validación

Compilar el frontend:

```powershell
Set-Location .\frontend
npm run build
```

Probar el seguimiento:

```powershell
Invoke-WebRequest "http://localhost:3001/tracking/ENVIO-0627ed46" -UseBasicParsing
Invoke-RestMethod "http://localhost:3001/api/tracking/ENVIO-0627ed46" |
    ConvertTo-Json -Depth 5
```

Revisar Git:

```powershell
Set-Location ..
git diff --check
git status
```

La compilación y las rutas de seguimiento se verificaron anteriormente. No se considera validado un requisito funcional únicamente porque exista código relacionado con él.

## 11. Pendientes de validación

Antes de dar por terminada la entrega, corresponde revisar:

- Arranque coordinado de todos los componentes.
- Retención y configuración de los tópicos Kafka.
- Manejo de mensajes inválidos y DLQ en cada consumidor.
- Idempotencia de notificaciones y reprogramaciones.
- Recuperación de estado y ausencia de alertas SLA duplicadas.
- Exactitud de los indicadores analíticos.
- Filtros de analítica, si forman parte de los requisitos.
- Actualización en tiempo real mediante SSE, si es obligatoria.
- Control de los tiempos y escenarios del simulador.
- Recuperación ante reinicios y pruebas de integración.
- Configuración segura y documentación de variables de entorno.

Estos puntos se presentan como verificaciones pendientes y no como funcionalidades ya completadas.

## 12. Publicación

El repositorio remoto configurado es:

`https://github.com/MarcoWeiss22/TP7_Programacion_Avanzada.git`

Para publicar cambios documentales:

```powershell
git add README.md INFORME_TECNICO_RUTASUR.md
git commit -m "Agregar documentacion del proyecto"
git push origin master
```

## 13. Conclusión

RutaSur integra una infraestructura de eventos, servicios especializados, persistencia y una interfaz de seguimiento. La compilación y el seguimiento público fueron probados previamente. La aceptación final requiere ejecutar y documentar las pruebas correspondientes a cada requisito restante.