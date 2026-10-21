
import { Kafka } from "kafkajs";
import { MongoClient } from "mongodb";
import { randomUUID } from "node:crypto";

const kafka = new Kafka({
    clientId: "rutasur-monitor-sla",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "monitor-sla" });
const producer = kafka.producer();

const mongo = new MongoClient(
    process.env.MONGO_URL || "mongodb://localhost:27017/?replicaSet=rs0"
);

const DB_NAME = "rutasur";
const INTERVALO_REVISION_MS = 60_000;

const DURACION_HORA_MS = Number(
    process.env.HORAS_SIMULADAS_MS || 30_000
);

const FACTOR_TIEMPO = (60 * 60 * 1000) / DURACION_HORA_MS;

let enviosSla;
let alertasSla;
let eventosProcesados;
let revisionEnCurso = false;

const estadosOrdenados = [
    "CREADO",
    "INGRESADO_HUB",
    "CLASIFICADO",
    "EN_REPARTO",
    "ENTREGADO",
];

function estadoYaAlcanzo(estadoActual, estadoObjetivo) {
    const actual = estadosOrdenados.indexOf(estadoActual);
    const objetivo = estadosOrdenados.indexOf(estadoObjetivo);

    return actual !== -1 && objetivo !== -1 && actual >= objetivo;
}

function mismoDia(fechaA, fechaB) {
    const a = new Date(fechaA);
    const b = new Date(fechaB);

    if (
        Number.isNaN(a.getTime()) ||
        Number.isNaN(b.getTime())
    ) {
        return false;
    }

    return a.toISOString().slice(0, 10) ===
        b.toISOString().slice(0, 10);
}

function obtenerFechaValida(...fechas) {
    for (const fecha of fechas) {
        if (!fecha) continue;

        const resultado = new Date(fecha);

        if (!Number.isNaN(resultado.getTime())) {
            return resultado;
        }
    }

    return null;
}

function ahoraSimulado(envio) {
    const base = obtenerFechaValida(envio.relojSimuladoEn);

    if (!base) {
        return obtenerFechaValida(
            envio.estadoActualizadoEn,
            envio.promesaActualizadaEn
        ) || new Date();
    }

    const anclaReal =
        obtenerFechaValida(envio.relojSimuladoActualizadoEn) ||
        new Date();

    const transcurridoReal = Math.max(
        0,
        Date.now() - anclaReal.getTime()
    );

    return new Date(
        base.getTime() + transcurridoReal * FACTOR_TIEMPO
    );
}

function calcularReglas(envio, ahora) {
    const reglas = [];

    if (!envio.fechaPrometida || envio.estadoActual === "ENTREGADO") {
        return reglas;
    }

    const fechaPrometida = new Date(envio.fechaPrometida);

    if (Number.isNaN(fechaPrometida.getTime())) {
        return reglas;
    }

    const horasRestantes =
        (fechaPrometida.getTime() - ahora.getTime()) /
        (60 * 60 * 1000);

    const ingresoHub = estadoYaAlcanzo(
        envio.estadoActual,
        "INGRESADO_HUB"
    );

    const salidaReparto = estadoYaAlcanzo(
        envio.estadoActual,
        "EN_REPARTO"
    );

    if (!ingresoHub && horasRestantes >= 0 && horasRestantes < 10) {
        reglas.push({
            regla: "SIN_INGRESO",
            prioridad: "Media",
            descripcion:
                "El envío todavía no ingresó al hub y quedan menos de 10 horas para la promesa.",
        });
    }

    if (!salidaReparto && horasRestantes >= 0 && horasRestantes < 4) {
        reglas.push({
            regla: "SIN_SALIDA",
            prioridad: "Alta",
            descripcion:
                "El envío todavía no salió a reparto y quedan menos de 4 horas para la promesa.",
        });
    }

    const fechaUltimoFallo = obtenerFechaValida(
        envio.ultimoFalloEn
    );

    if (
        fechaUltimoFallo &&
        mismoDia(fechaPrometida, fechaUltimoFallo)
    ) {
        reglas.push({
            regla: "FALLO_DIA_LIMITE",
            prioridad: "Alta",
            descripcion:
                `Hubo un intento fallido el día de la promesa. Motivo: ${envio.ultimoMotivoFallo || "NO_ESPECIFICADO"
                }.`,
        });
    }
    if (ahora.getTime() > fechaPrometida.getTime()) {
        reglas.push({
            regla: "VENCIDO",
            prioridad: "Crítica",
            descripcion:
                "La promesa de entrega venció y el envío todavía no fue entregado.",
        });
    }

    return reglas;
}

async function publicarAlerta(alerta) {
    await producer.send({
        topic: "alertas.sla",
        messages: [{
            key: `${alerta.shipmentId}:${alerta.regla}`,
            value: JSON.stringify(alerta),
        }],
    });
}

async function cerrarAlerta(alerta, motivo) {
    const fechaCierre = new Date().toISOString();

    const resultado = await alertasSla.updateOne(
        { _id: alerta._id, estado: "ABIERTA" },
        {
            $set: {
                estado: "CERRADA",
                motivoCierre: motivo,
                cerradaEn: fechaCierre,
                actualizadaEn: fechaCierre,
            },
        }
    );

    if (resultado.modifiedCount === 0) return;

    await publicarAlerta({
        ...alerta,
        estado: "CERRADA",
        motivoCierre: motivo,
        cerradaEn: fechaCierre,
        actualizadaEn: fechaCierre,
    });

    console.log(
        `Alerta cerrada: ${alerta.regla} | Envío: ${alerta.shipmentId}`
    );
}

async function cerrarAlertasAbiertas(shipmentId, motivo) {
    const abiertas = await alertasSla.find({
        shipmentId,
        estado: "ABIERTA",
    }).toArray();

    for (const alerta of abiertas) {
        await cerrarAlerta(alerta, motivo);
    }
}

async function crearAlerta(envio, regla) {
    const fecha = new Date().toISOString();

    const alerta = {
        alertaId: randomUUID(),
        shipmentId: envio.shipmentId,
        regla: regla.regla,
        prioridad: regla.prioridad,
        descripcion: regla.descripcion,
        estado: "ABIERTA",
        fechaPrometida: envio.fechaPrometida,
        hubId: envio.hubId || null,
        zonaId: envio.zonaId || null,
        creadaEn: fecha,
        actualizadaEn: fecha,
    };

    try {
        // El índice único parcial impide crear dos alertas abiertas
        // para el mismo envío y la misma regla.
        await alertasSla.insertOne(alerta);
    } catch (error) {
        if (error.code === 11000) return;
        throw error;
    }

    await publicarAlerta(alerta);

    console.log(
        `Nueva alerta: ${regla.regla} (${regla.prioridad}) | Envío: ${envio.shipmentId}`
    );
}

async function evaluarEnvio(shipmentId) {
    let envio = await enviosSla.findOne({ shipmentId });

    if (!envio) return;

    if (envio.estadoActual === "ENTREGADO") {
        await cerrarAlertasAbiertas(shipmentId, "ENVIO_ENTREGADO");
        return;
    }

    // Recuperar el reloj de envíos antiguos que aún no lo tengan.
    if (!envio.relojSimuladoEn) {
        const referencia = obtenerFechaValida(
            envio.estadoActualizadoEn,
            envio.promesaActualizadaEn
        ) || new Date();

        await enviosSla.updateOne(
            {
                _id: envio._id,
                relojSimuladoEn: { $exists: false },
            },
            {
                $set: {
                    relojSimuladoEn: referencia.toISOString(),
                    relojSimuladoActualizadoEn: new Date().toISOString(),
                },
            }
        );

        envio = await enviosSla.findOne({ shipmentId });

        if (!envio) return;
    }

    const ahora = ahoraSimulado(envio);
    const reglasActivas = calcularReglas(envio, ahora);

    const idsActivos = new Set(
        reglasActivas.map((regla) => regla.regla)
    );

    const abiertas = await alertasSla.find({
        shipmentId,
        estado: "ABIERTA",
    }).toArray();

    for (const alerta of abiertas) {
        if (!idsActivos.has(alerta.regla)) {
            await cerrarAlerta(alerta, "REGLA_YA_NO_APLICA");
        }
    }

    for (const regla of reglasActivas) {
        const existente = await alertasSla.findOne({
            shipmentId,
            regla: regla.regla,
            estado: "ABIERTA",
        });

        if (!existente) {
            await crearAlerta(envio, regla);
        }
    }
}

async function actualizarReloj(envioActual, ocurridoEn) {
    if (!envioActual) return;

    const fechaEvento = obtenerFechaValida(ocurridoEn);

    if (!fechaEvento) return;

    const relojGuardado = obtenerFechaValida(
        envioActual.relojSimuladoEn
    );

    const anclaReal = obtenerFechaValida(
        envioActual.relojSimuladoActualizadoEn
    );

    let relojAvanzado = relojGuardado;

    if (relojGuardado && anclaReal) {
        const transcurridoReal = Math.max(
            0,
            Date.now() - anclaReal.getTime()
        );

        relojAvanzado = new Date(
            relojGuardado.getTime() +
            transcurridoReal * FACTOR_TIEMPO
        );
    }

    // Nunca retroceder el reloj por un evento atrasado.
    const nuevaBase =
        relojAvanzado && relojAvanzado > fechaEvento
            ? relojAvanzado
            : fechaEvento;

    await enviosSla.updateOne(
        { shipmentId: envioActual.shipmentId },
        {
            $set: {
                relojSimuladoEn: nuevaBase.toISOString(),
                relojSimuladoActualizadoEn: new Date().toISOString(),
            },
        }
    );
}

async function registrarProcesado(clave) {
    await eventosProcesados.updateOne(
        { _id: clave },
        {
            $setOnInsert: {
                procesadoEn: new Date().toISOString(),
            },
        },
        { upsert: true }
    );
}

async function enviarDLQ(topic, contenido, motivo) {
    await producer.send({
        topic: "envios.estados.dlq",
        messages: [{
            key: `${topic}:${randomUUID()}`,
            value: JSON.stringify({
                topicOriginal: topic,
                motivo,
                contenido,
                ocurridoEn: new Date().toISOString(),
            }),
        }],
    });
}

async function procesarMensaje(topic, mensaje) {
    if (!mensaje.value) {
        console.warn(`Mensaje vacío en ${topic}; se ignora.`);
        return;
    }

    let evento;

    try {
        evento = JSON.parse(mensaje.value.toString());
    } catch {
        await enviarDLQ(
            topic,
            mensaje.value.toString(),
            "JSON_INVALIDO"
        );
        return;
    }

    if (!evento.shipmentId || !evento.eventId) {
        await enviarDLQ(
            topic,
            mensaje.value.toString(),
            "EVENTO_SIN_IDENTIFICADOR"
        );
        return;
    }

    const clave = `${topic}:${evento.eventId}`;

    if (await eventosProcesados.findOne({ _id: clave })) {
        return;
    }

    const shipmentId = evento.shipmentId;


    if (topic === "envios.promesas") {
        const fechaPrometida = obtenerFechaValida(
            evento.fechaPrometida
        );

        if (!fechaPrometida) {
            await enviarDLQ(
                topic,
                mensaje.value.toString(),
                "FECHA_PROMETIDA_INVALIDA"
            );
            return;
        }

        const envioActual = await enviosSla.findOne({ shipmentId });

        await enviosSla.updateOne(
            { shipmentId },
            {
                $set: {
                    shipmentId,
                    fechaPrometida: fechaPrometida.toISOString(),
                    promesaActualizadaEn: new Date().toISOString(),
                    hubId: evento.hubId || envioActual?.hubId || null,
                    zonaId: evento.zonaId || envioActual?.zonaId || null,
                },
                $setOnInsert: {
                    estadoActual: null,
                    ultimaSecuencia: 0,
                    relojSimuladoEn: new Date().toISOString(),
                    relojSimuladoActualizadoEn: new Date().toISOString(),
                },
            },
            { upsert: true }
        );

        const envio = await enviosSla.findOne({ shipmentId });
        await actualizarReloj(envio, evento.ocurridoEn);

    } else if (topic === "envios.estados") {
        if (
            !evento.tipo ||
            !Number.isInteger(evento.secuencia) ||
            evento.secuencia < 1
        ) {
            await enviarDLQ(
                topic,
                mensaje.value.toString(),
                "ESTADO_INVALIDO"
            );
            return;
        }

        const envioActual = await enviosSla.findOne({ shipmentId });

        if (
            envioActual?.ultimaSecuencia != null &&
            evento.secuencia <= envioActual.ultimaSecuencia
        ) {
            await registrarProcesado(clave);
            console.log(
                `Evento repetido o atrasado ignorado: ${shipmentId} | Secuencia ${evento.secuencia}`
            );
            return;
        }

        await enviosSla.updateOne(
            { shipmentId },
            {
                $set: {
                    shipmentId,
                    estadoActual: evento.tipo,
                    ultimaSecuencia: evento.secuencia,
                    estadoActualizadoEn:
                        evento.ocurridoEn || new Date().toISOString(),
                    hubId: evento.hubId || envioActual?.hubId || null,
                    zonaId: evento.zonaId || envioActual?.zonaId || null,
                },
                $setOnInsert: {
                    relojSimuladoEn:
                        evento.ocurridoEn || new Date().toISOString(),
                    relojSimuladoActualizadoEn: new Date().toISOString(),
                },
            },
            { upsert: true }
        );

        const envio = await enviosSla.findOne({ shipmentId });
        await actualizarReloj(envio, evento.ocurridoEn);
    }

    await registrarProcesado(clave);
    await evaluarEnvio(shipmentId);
}

async function inicializarRelojesAntiguos() {
    const cursor = enviosSla.find({
        relojSimuladoEn: { $exists: false },
    });

    let inicializados = 0;

    for await (const envio of cursor) {
        const fechasConocidas = [
            envio.estadoActualizadoEn,
            envio.promesaActualizadaEn,
        ]
            .filter(Boolean)
            .map((fecha) => new Date(fecha))
            .filter((fecha) => !Number.isNaN(fecha.getTime()));

        const fechaBase = fechasConocidas.length
            ? new Date(
                Math.max(
                    ...fechasConocidas.map((fecha) => fecha.getTime())
                )
            )
            : new Date();

        const resultado = await enviosSla.updateOne(
            {
                _id: envio._id,
                relojSimuladoEn: { $exists: false },
            },
            {
                $set: {
                    relojSimuladoEn: fechaBase.toISOString(),
                    relojSimuladoActualizadoEn: new Date().toISOString(),
                },
            }
        );

        if (resultado.modifiedCount > 0) {
            inicializados++;
        }
    }

    console.log(
        `Relojes simulados inicializados para ${inicializados} envíos antiguos.`
    );
}

async function revisarTodosLosEnvios() {
    if (revisionEnCurso) return;

    revisionEnCurso = true;

    try {
        const cursor = enviosSla.find({
            estadoActual: { $ne: "ENTREGADO" },
            fechaPrometida: { $exists: true },
        });

        for await (const envio of cursor) {
            try {
                await evaluarEnvio(envio.shipmentId);
            } catch (error) {
                console.error(
                    `Error al revisar ${envio.shipmentId}:`,
                    error.message
                );
            }
        }
    } finally {
        revisionEnCurso = false;
    }
}

async function iniciar() {
    if (
        !Number.isFinite(DURACION_HORA_MS) ||
        DURACION_HORA_MS <= 0
    ) {
        throw new Error("HORAS_SIMULADAS_MS debe ser mayor que cero.");
    }

    await mongo.connect();

    const db = mongo.db(DB_NAME);

    enviosSla = db.collection("envios_sla");
    alertasSla = db.collection("alertas_sla");
    eventosProcesados = db.collection("eventos_sla_procesados");

    await alertasSla.createIndex(
        { shipmentId: 1, regla: 1 },
        {
            unique: true,
            partialFilterExpression: { estado: "ABIERTA" },
            name: "una_alerta_abierta_por_regla",
        }
    );

    await eventosProcesados.createIndex(
        { procesadoEn: 1 },
        { expireAfterSeconds: 60 * 60 * 24 * 30 }
    );

    await inicializarRelojesAntiguos();

    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
        topics: ["envios.estados", "envios.promesas"],
        fromBeginning: true,
    });

    console.log("Monitor SLA iniciado.");
    console.log(
        `Reloj simulado: 1 hora = ${DURACION_HORA_MS / 1000} segundos reales.`
    );
    console.log("Revisión periódica cada 60 segundos.");

    await revisarTodosLosEnvios();

    setInterval(() => {
        revisarTodosLosEnvios().catch((error) => {
            console.error(
                "Error en la revisión periódica:",
                error.message
            );
        });
    }, INTERVALO_REVISION_MS);

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            try {
                await procesarMensaje(topic, message);
            } catch (error) {
                console.error(
                    `Error procesando ${topic}:`,
                    error.message
                );
            }
        },
    });
}

async function cerrar() {
    console.log("Cerrando monitor SLA...");

    try {
        await consumer.disconnect();
        await producer.disconnect();
        await mongo.close();
    } catch (error) {
        console.error("Error durante el cierre:", error.message);
    }
}

process.on("SIGINT", async () => {
    await cerrar();
    process.exit(0);
});

process.on("SIGTERM", async () => {
    await cerrar();
    process.exit(0);
});

iniciar().catch((error) => {
    console.error("No se pudo iniciar el monitor SLA:", error);
    process.exitCode = 1;
});
