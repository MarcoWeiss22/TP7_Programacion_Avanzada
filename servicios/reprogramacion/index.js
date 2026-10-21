
import { Kafka } from "kafkajs";
import { MongoClient } from "mongodb";
import { randomUUID } from "node:crypto";

const MONGO_URL = "mongodb://localhost:27017/?replicaSet=rs0";

const TOPIC_ESTADOS = "envios.estados";
const TOPIC_REPROGRAMACIONES = "envios.reprogramaciones";
const TOPIC_DLQ = "envios.estados.dlq";

const kafka = new Kafka({
    clientId: "rutasur-reprogramacion",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "reprogramacion",
});

const producer = kafka.producer();
const mongo = new MongoClient(MONGO_URL);

function siguienteDiaHabil(fecha) {
    const resultado = new Date(fecha);

    resultado.setDate(resultado.getDate() + 1);

    // 0 = domingo; 6 = sábado.
    while (
        resultado.getDay() === 0 ||
        resultado.getDay() === 6
    ) {
        resultado.setDate(resultado.getDate() + 1);
    }

    resultado.setHours(9, 0, 0, 0);

    return resultado;
}

function franjaOpuesta(fecha) {
    const hora = fecha.getHours();

    return hora < 12 ? "VESPERTINA" : "MATUTINA";
}

function decidirReprogramacion(evento, numeroIntento) {
    // El tercer fallo siempre implica devolución.
    if (numeroIntento >= 3) {
        return {
            decision: "DEVOLUCION_COMERCIO",
            motivoDecision: "TERCER_INTENTO_FALLIDO",
        };
    }

    if (evento.motivo === "RECHAZADO") {
        return {
            decision: "DEVOLUCION_COMERCIO",
            motivoDecision: "DESTINATARIO_RECHAZO",
        };
    }

    if (evento.motivo === "DIRECCION_INCORRECTA") {
        return {
            decision: "RETENIDO",
            motivoDecision: "CORREGIR_DIRECCION",
        };
    }

    if (evento.motivo === "ZONA_INACCESIBLE") {
        return {
            decision: "RETENIDO",
            motivoDecision: "ZONA_INACCESIBLE",
        };
    }

    if (evento.motivo === "AUSENTE") {
        const fechaFallo = new Date(evento.ocurridoEn);

        if (Number.isNaN(fechaFallo.getTime())) {
            throw new Error("FECHA_DE_FALLO_INVALIDA");
        }

        const proximoDia = siguienteDiaHabil(fechaFallo);

        return {
            decision: "REPROGRAMADO",
            motivoDecision: "DESTINATARIO_AUSENTE",
            nuevaFecha: proximoDia.toISOString(),
            nuevaFranja: franjaOpuesta(fechaFallo),
        };
    }

    return {
        decision: "RETENIDO",
        motivoDecision: "MOTIVO_NO_RECONOCIDO",
    };
}

function validarEventoFallo(evento) {
    if (
        !evento ||
        typeof evento !== "object" ||
        Array.isArray(evento)
    ) {
        return "EVENTO_NO_ES_OBJETO";
    }

    if (
        typeof evento.eventId !== "string" ||
        evento.eventId.length === 0 ||
        typeof evento.shipmentId !== "string" ||
        evento.shipmentId.length === 0
    ) {
        return "IDENTIFICADORES_INVALIDOS";
    }

    if (
        typeof evento.ocurridoEn !== "string" ||
        Number.isNaN(Date.parse(evento.ocurridoEn))
    ) {
        return "FECHA_INVALIDA";
    }

    if (
        ![
            "AUSENTE",
            "DIRECCION_INCORRECTA",
            "RECHAZADO",
            "ZONA_INACCESIBLE",
        ].includes(evento.motivo)
    ) {
        return "MOTIVO_INVALIDO";
    }

    if (
        typeof evento.version !== "number" ||
        !Number.isFinite(evento.version)
    ) {
        return "VERSION_INVALIDA";
    }

    return null;
}

async function enviarADLQ(
    mensajeOriginal,
    motivo,
    partition,
    offset
) {
    const registro = {
        eventoOriginal: mensajeOriginal,
        motivo,
        topicOrigen: TOPIC_ESTADOS,
        partition,
        offset,
        enviadoEn: new Date().toISOString(),
        servicio: "reprogramacion",
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
                value: JSON.stringify(registro),
            },
        ],
    });

    console.log(`Evento inválido enviado a DLQ: ${motivo}`);
}

async function confirmarOffset(topic, partition, offset) {
    await consumer.commitOffsets([
        {
            topic,
            partition,
            offset: (BigInt(offset) + 1n).toString(),
        },
    ]);
}

// Registra una decisión una sola vez en MongoDB.
// La transacción guarda el contador y el resultado juntos.
async function obtenerOCrearDecision(
    evento,
    intentos,
    procesados
) {
    const existente = await procesados.findOne({
        eventId: evento.eventId,
    });

    if (existente) {
        return existente;
    }

    const session = mongo.startSession();
    let registroFinal;

    try {
        await session.withTransaction(async () => {
            const repetido = await procesados.findOne(
                { eventId: evento.eventId },
                { session }
            );

            if (repetido) {
                registroFinal = repetido;
                return;
            }

            await intentos.updateOne(
                { shipmentId: evento.shipmentId },
                {
                    $inc: { cantidadFallos: 1 },
                    $set: { actualizadoEn: new Date() },
                    $setOnInsert: {
                        shipmentId: evento.shipmentId,
                        creadoEn: new Date(),
                    },
                },
                {
                    upsert: true,
                    session,
                }
            );

            const contador = await intentos.findOne(
                { shipmentId: evento.shipmentId },
                { session }
            );

            const numeroIntento = contador.cantidadFallos;
            const decision = decidirReprogramacion(
                evento,
                numeroIntento
            );

            const resultado = {
                eventId: randomUUID(),
                eventoOrigenId: evento.eventId,
                shipmentId: evento.shipmentId,
                tipo: "REPROGRAMACION_DECIDIDA",
                numeroIntento,
                ...decision,
                ocurridoEn: new Date().toISOString(),
                version: 1,
            };

            const registro = {
                eventId: evento.eventId,
                shipmentId: evento.shipmentId,
                resultado,
                publicado: false,
                creadoEn: new Date(),
            };

            await procesados.insertOne(registro, { session });

            registroFinal = registro;
        });
    } finally {
        await session.endSession();
    }

    return registroFinal;
}

// Publica la decisión guardada.
// Si el proceso se reinicia, puede volver a publicar el mismo
// resultado sin volver a incrementar el contador de fallos.
async function publicarDecisionPendiente(
    registro,
    procesados
) {
    if (registro.publicado) {
        console.log(`Fallo ya procesado: ${registro.eventId}`);
        return;
    }

    await producer.send({
        topic: TOPIC_REPROGRAMACIONES,
        messages: [
            {
                key: registro.shipmentId,
                value: JSON.stringify(registro.resultado),
            },
        ],
    });

    await procesados.updateOne(
        { eventId: registro.eventId },
        {
            $set: {
                publicado: true,
                publicadoEn: new Date(),
            },
        }
    );

    console.log(
        `Reprogramación: ${registro.shipmentId} -> ${registro.resultado.decision}`
    );

    console.log(JSON.stringify(registro.resultado, null, 2));
}

async function iniciarServicio() {
    await mongo.connect();

    const db = mongo.db("rutasur");
    const intentos = db.collection("intentos_reprogramacion");
    const procesados = db.collection("reprogramaciones_procesadas");

    await procesados.createIndex(
        { eventId: 1 },
        { unique: true }
    );

    await intentos.createIndex(
        { shipmentId: 1 },
        { unique: true }
    );

    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
        topic: TOPIC_ESTADOS,
        fromBeginning: true,
    });

    console.log("Servicio de reprogramación iniciado.");

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({
            topic,
            partition,
            message,
            heartbeat,
        }) => {
            const offset = message.offset;
            const texto = message.value?.toString() ?? "";

            let evento;

            try {
                evento = JSON.parse(texto);
            } catch {
                await enviarADLQ(
                    texto,
                    "JSON_MALFORMADO",
                    partition,
                    offset
                );

                await confirmarOffset(topic, partition, offset);
                await heartbeat();
                return;
            }

            if (
                !evento ||
                typeof evento !== "object" ||
                Array.isArray(evento)
            ) {
                await enviarADLQ(
                    evento,
                    "ESQUEMA_INVALIDO",
                    partition,
                    offset
                );

                await confirmarOffset(topic, partition, offset);
                await heartbeat();
                return;
            }

            // Solo se reacciona a fallos de entrega.
            if (evento.tipo !== "ENTREGA_FALLIDA") {
                await confirmarOffset(topic, partition, offset);
                await heartbeat();
                return;
            }

            const motivoInvalidez = validarEventoFallo(evento);

            if (motivoInvalidez) {
                await enviarADLQ(
                    evento,
                    motivoInvalidez,
                    partition,
                    offset
                );

                await confirmarOffset(topic, partition, offset);
                await heartbeat();
                return;
            }

            const registro = await obtenerOCrearDecision(
                evento,
                intentos,
                procesados
            );

            await publicarDecisionPendiente(
                registro,
                procesados
            );

            await confirmarOffset(topic, partition, offset);
            await heartbeat();
        },
    });
}

iniciarServicio().catch(async (error) => {
    console.error(
        "No se pudo iniciar reprogramación:",
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
