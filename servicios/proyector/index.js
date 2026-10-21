
import { Kafka } from "kafkajs";
import { MongoClient } from "mongodb";

const MONGO_URL = "mongodb://localhost:27017/?replicaSet=rs0";

const kafka = new Kafka({
    clientId: "rutasur-proyector",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "proyector",
});

const producer = kafka.producer();
const mongo = new MongoClient(MONGO_URL);

const TOPIC_ESTADOS = "envios.estados";
const TOPIC_REPROGRAMACIONES = "envios.reprogramaciones";
const TOPIC_NOTIFICACIONES = "notificaciones.enviadas";
const TOPIC_DLQ = "envios.estados.dlq";

// Comprueba los campos comunes de los eventos.
function validarEvento(evento) {
    return (
        evento !== null &&
        typeof evento === "object" &&
        !Array.isArray(evento) &&
        typeof evento.eventId === "string" &&
        evento.eventId.length > 0 &&
        typeof evento.shipmentId === "string" &&
        evento.shipmentId.length > 0 &&
        typeof evento.tipo === "string" &&
        evento.tipo.length > 0 &&
        typeof evento.version === "number" &&
        Number.isFinite(evento.version)
    );
}

// Envía a la DLQ el mensaje que no se puede procesar.
async function enviarADLQ(
    mensajeOriginal,
    motivo,
    topicOrigen,
    partition,
    offset
) {
    const mensajeDLQ = {
        eventoOriginal: mensajeOriginal,
        motivo,
        topicOrigen,
        partition,
        offset,
        enviadoEn: new Date().toISOString(),
    };

    const shipmentId =
        mensajeOriginal &&
            typeof mensajeOriginal === "object"
            ? mensajeOriginal.shipmentId
            : null;

    await producer.send({
        topic: TOPIC_DLQ,
        messages: [
            {
                key:
                    typeof shipmentId === "string"
                        ? shipmentId
                        : "evento-invalido",
                value: JSON.stringify(mensajeDLQ),
            },
        ],
    });

    console.log(
        `Evento enviado a DLQ: motivo=${motivo}, topic=${topicOrigen}, partición=${partition}, offset=${offset}`
    );
}

// Registra que un evento ya produjo su efecto.
async function registrarEventoProcesado(
    procesados,
    eventId,
    topic
) {
    await procesados.insertOne({
        consumer: "proyector",
        eventId,
        topic,
        procesadoEn: new Date(),
    });
}

// Procesa los cambios de estado de un envío.
async function procesarEstado(evento, envios, procesados) {
    const existente = await procesados.findOne({
        eventId: evento.eventId,
    });

    if (existente) {
        console.log(
            `Evento duplicado ignorado: ${evento.eventId}`
        );
        return;
    }

    const envioActual = await envios.findOne({
        shipmentId: evento.shipmentId,
    });

    if (
        envioActual &&
        evento.secuencia <= envioActual.ultimaSecuencia
    ) {
        await registrarEventoProcesado(
            procesados,
            evento.eventId,
            TOPIC_ESTADOS
        );

        console.log(
            `Secuencia antigua ignorada: ${evento.shipmentId}`
        );
        return;
    }

    const entrada = {
        eventId: evento.eventId,
        tipo: evento.tipo,
        ocurridoEn: evento.ocurridoEn,
        secuencia: evento.secuencia,
    };

    if (evento.motivo) {
        entrada.motivo = evento.motivo;
    }

    await envios.updateOne(
        { shipmentId: evento.shipmentId },
        {
            $set: {
                estadoActual: evento.tipo,
                ultimaSecuencia: evento.secuencia,
                hubId: evento.hubId ?? null,
                zonaId: evento.zonaId ?? null,
                repartidorId: evento.repartidorId ?? null,
                actualizadoEn: new Date(),
            },
            $setOnInsert: {
                shipmentId: evento.shipmentId,
                creadoEn: new Date(),
                intentos: 1,
            },
            $push: {
                historial: entrada,
            },
        },
        { upsert: true }
    );

    await registrarEventoProcesado(
        procesados,
        evento.eventId,
        TOPIC_ESTADOS
    );

    console.log(
        `Estado actualizado: ${evento.shipmentId} -> ${evento.tipo}`
    );
}

// Guarda la decisión del servicio de reprogramación.
async function procesarReprogramacion(
    evento,
    envios,
    procesados
) {
    const existente = await procesados.findOne({
        eventId: evento.eventId,
    });

    if (existente) {
        console.log("Reprogramación duplicada ignorada.");
        return;
    }

    const detalle = {
        eventId: evento.eventId,
        decision: evento.decision,
        motivoDecision: evento.motivoDecision,
        numeroIntento: evento.numeroIntento,
        actualizadoEn: new Date(),
    };

    if (evento.nuevaFecha) {
        detalle.nuevaFecha = evento.nuevaFecha;
    }

    if (evento.nuevaFranja) {
        detalle.nuevaFranja = evento.nuevaFranja;
    }

    const resultado = await envios.updateOne(
        { shipmentId: evento.shipmentId },
        {
            $set: {
                reprogramacion: detalle,
                actualizadoEn: new Date(),
            },
        }
    );

    if (resultado.matchedCount === 0) {
        throw new Error(
            `No existe el envío ${evento.shipmentId} para la reprogramación`
        );
    }

    await registrarEventoProcesado(
        procesados,
        evento.eventId,
        TOPIC_REPROGRAMACIONES
    );

    console.log(
        `Reprogramación guardada: ${evento.shipmentId} -> ${evento.decision}`
    );
}

// Guarda la notificación simulada.
async function procesarNotificacion(
    evento,
    envios,
    procesados
) {
    const existente = await procesados.findOne({
        eventId: evento.eventId,
    });

    if (existente) {
        console.log("Notificación duplicada ignorada.");
        return;
    }

    const notificacion = {
        eventId: evento.eventId,
        eventoOrigenId: evento.eventoOrigenId,
        tipo: evento.tipo,
        motivo: evento.motivo,
        mensaje: evento.mensaje,
        ocurridoEn: evento.ocurridoEn,
    };

    const resultado = await envios.updateOne(
        { shipmentId: evento.shipmentId },
        {
            $push: {
                notificaciones: notificacion,
            },
            $set: {
                actualizadoEn: new Date(),
            },
        }
    );

    if (resultado.matchedCount === 0) {
        throw new Error(
            `No existe el envío ${evento.shipmentId} para la notificación`
        );
    }

    await registrarEventoProcesado(
        procesados,
        evento.eventId,
        TOPIC_NOTIFICACIONES
    );

    console.log(
        `Notificación guardada: ${evento.shipmentId}`
    );
}

// Comprueba que cada tipo de evento tenga sus campos necesarios.
function validarContenidoEvento(evento, topic) {
    if (topic === TOPIC_ESTADOS) {
        const estadosPermitidos = [
            "CREADO",
            "INGRESADO_HUB",
            "CLASIFICADO",
            "EN_REPARTO",
            "ENTREGADO",
            "ENTREGA_FALLIDA",
        ];

        if (!estadosPermitidos.includes(evento.tipo)) {
            return "TIPO_ESTADO_INVALIDO";
        }

        if (
            !Number.isInteger(evento.secuencia) ||
            evento.secuencia < 1
        ) {
            return "SECUENCIA_INVALIDA";
        }

        if (
            typeof evento.ocurridoEn !== "string" ||
            Number.isNaN(Date.parse(evento.ocurridoEn))
        ) {
            return "FECHA_INVALIDA";
        }

        if (
            evento.tipo === "ENTREGA_FALLIDA" &&
            ![
                "AUSENTE",
                "DIRECCION_INCORRECTA",
                "RECHAZADO",
                "ZONA_INACCESIBLE",
            ].includes(evento.motivo)
        ) {
            return "MOTIVO_FALLO_INVALIDO";
        }
    }

    if (topic === TOPIC_REPROGRAMACIONES) {
        if (
            typeof evento.decision !== "string" ||
            typeof evento.motivoDecision !== "string" ||
            !Number.isInteger(evento.numeroIntento) ||
            evento.numeroIntento < 1
        ) {
            return "REPROGRAMACION_INVALIDA";
        }
    }

    if (topic === TOPIC_NOTIFICACIONES) {
        if (
            typeof evento.mensaje !== "string" ||
            typeof evento.eventoOrigenId !== "string"
        ) {
            return "NOTIFICACION_INVALIDA";
        }
    }

    return null;
}

async function iniciarProyector() {
    await mongo.connect();

    const db = mongo.db("rutasur");
    const envios = db.collection("envios");
    const procesados = db.collection("eventos_procesados");

    await envios.createIndex(
        { shipmentId: 1 },
        { unique: true }
    );

    await procesados.createIndex(
        { consumer: 1, eventId: 1 },
        { unique: true }
    );

    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
        topics: [
            TOPIC_ESTADOS,
            TOPIC_REPROGRAMACIONES,
            TOPIC_NOTIFICACIONES,
        ],
        fromBeginning: true,
    });

    console.log("Proyector integrado iniciado.");

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({
            topic,
            partition,
            message,
            heartbeat,
        }) => {
            const offset = message.offset;
            const textoOriginal = message.value?.toString() ?? "";

            let evento;

            // Paso 1: comprobar que el mensaje sea JSON válido.
            try {
                evento = JSON.parse(textoOriginal);
            } catch {
                await enviarADLQ(
                    textoOriginal,
                    "JSON_MALFORMADO",
                    topic,
                    partition,
                    offset
                );

                await consumer.commitOffsets([
                    {
                        topic,
                        partition,
                        offset: (BigInt(offset) + 1n).toString(),
                    },
                ]);

                await heartbeat();
                return;
            }

            // Paso 2: comprobar los campos comunes.
            if (!validarEvento(evento)) {
                await enviarADLQ(
                    evento,
                    "ESQUEMA_INVALIDO",
                    topic,
                    partition,
                    offset
                );

                await consumer.commitOffsets([
                    {
                        topic,
                        partition,
                        offset: (BigInt(offset) + 1n).toString(),
                    },
                ]);

                await heartbeat();
                return;
            }

            // Paso 3: validar los campos propios de cada evento.
            const motivoInvalidez = validarContenidoEvento(
                evento,
                topic
            );

            if (motivoInvalidez) {
                await enviarADLQ(
                    evento,
                    motivoInvalidez,
                    topic,
                    partition,
                    offset
                );

                await consumer.commitOffsets([
                    {
                        topic,
                        partition,
                        offset: (BigInt(offset) + 1n).toString(),
                    },
                ]);

                await heartbeat();
                return;
            }

            // Paso 4: procesar el evento válido.
            if (topic === TOPIC_ESTADOS) {
                await procesarEstado(
                    evento,
                    envios,
                    procesados
                );
            } else if (topic === TOPIC_REPROGRAMACIONES) {
                await procesarReprogramacion(
                    evento,
                    envios,
                    procesados
                );
            } else if (topic === TOPIC_NOTIFICACIONES) {
                await procesarNotificacion(
                    evento,
                    envios,
                    procesados
                );
            } else {
                await enviarADLQ(
                    evento,
                    "TOPIC_NO_RECONOCIDO",
                    topic,
                    partition,
                    offset
                );
            }

            // Confirmar solamente después de completar el trabajo.
            await consumer.commitOffsets([
                {
                    topic,
                    partition,
                    offset: (BigInt(offset) + 1n).toString(),
                },
            ]);

            await heartbeat();
        },
    });
}

iniciarProyector().catch(async (error) => {
    console.error(
        "No se pudo iniciar el proyector:",
        error
    );

    try {
        await consumer.disconnect();
    } catch { }

    try {
        await producer.disconnect();
    } catch { }

    try {
        await mongo.close();
    } catch { }

    process.exitCode = 1;
});
