import { Kafka } from "kafkajs";
import { MongoClient } from "mongodb";
import { randomUUID } from "node:crypto";
import { obtenerNotificacion } from "./lib/deduplicacion.js";

const MONGO_URL = "mongodb://localhost:27017/?replicaSet=rs0";

const kafka = new Kafka({
    clientId: "rutasur-notificaciones",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "notificaciones",
    autoCommit: false,
});

const producer = kafka.producer();
const mongo = new MongoClient(MONGO_URL);

let notificaciones;

async function confirmarOffset(topic, partition, offset) {
    await consumer.commitOffsets([
        {
            topic,
            partition,
            offset: (BigInt(offset) + 1n).toString(),
        },
    ]);
}

async function iniciarNotificaciones() {
    await mongo.connect();

    notificaciones = mongo
        .db("rutasur")
        .collection("notificaciones_procesadas");

    await notificaciones.createIndex(
        { eventoOrigenId: 1 },
        { unique: true, name: "evento_origen_unico" }
    );

    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
        topic: "envios.estados",
        fromBeginning: true,
    });

    console.log("Servicio de notificaciones iniciado.");

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({
            topic,
            partition,
            message,
            heartbeat,
        }) => {
            const offset = message.offset;
            let evento;

            try {
                evento = JSON.parse(message.value.toString());
            } catch {
                console.error("Se recibió un mensaje JSON inválido.");
                await confirmarOffset(topic, partition, offset);
                await heartbeat();
                return;
            }

            if (evento.tipo !== "ENTREGA_FALLIDA") {
                await confirmarOffset(topic, partition, offset);
                await heartbeat();
                return;
            }

            if (
                typeof evento.eventId !== "string" ||
                !evento.eventId.trim() ||
                typeof evento.shipmentId !== "string" ||
                !evento.shipmentId.trim()
            ) {
                console.error("Evento de fallo sin identificadores válidos.");
                await confirmarOffset(topic, partition, offset);
                await heartbeat();
                return;
            }

            let notificacion;

            try {
                notificacion = await obtenerNotificacion({
                    notificaciones,
                    evento,
                    generarId: randomUUID,
                    obtenerFecha: () => new Date().toISOString(),
                });

                notificacion = notificacion?.value ?? notificacion;

                if (!notificacion.publicado) {
                    await producer.send({
                        topic: "notificaciones.enviadas",
                        messages: [
                            {
                                key: evento.shipmentId,
                                value: JSON.stringify({
                                    eventId: notificacion.eventId,
                                    eventoOrigenId: notificacion.eventoOrigenId,
                                    shipmentId: notificacion.shipmentId,
                                    tipo: notificacion.tipo,
                                    motivo: notificacion.motivo,
                                    mensaje: notificacion.mensaje,
                                    ocurridoEn: notificacion.ocurridoEn,
                                    version: notificacion.version,
                                }),
                            },
                        ],
                    });

                    await notificaciones.updateOne(
                        {
                            eventoOrigenId: evento.eventId,
                            publicado: false,
                        },
                        { $set: { publicado: true } }
                    );

                    console.log(
                        `Notificación publicada para ${evento.shipmentId}`
                    );
                } else {
                    console.log(
                        `Evento duplicado ignorado: ${evento.eventId}`
                    );
                }
            } catch (error) {
                // Un fallo no se confirma: Kafka podrá volver a entregarlo.
                console.error(
                    `Error procesando notificación ${evento.eventId}:`,
                    error
                );
                throw error;
            }

            await confirmarOffset(topic, partition, offset);
            await heartbeat();
        },
    });
}

iniciarNotificaciones().catch(async (error) => {
    console.error("Error en notificaciones:", error);

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