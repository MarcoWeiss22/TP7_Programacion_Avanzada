
import { Kafka } from "kafkajs";
import { randomUUID } from "node:crypto";

const kafka = new Kafka({
    clientId: "rutasur-notificaciones",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "notificaciones",
});

const producer = kafka.producer();

async function iniciarNotificaciones() {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
        topic: "envios.estados",
        fromBeginning: true,
    });

    console.log("Servicio de notificaciones iniciado.");

    await consumer.run({
        eachMessage: async ({ message }) => {
            let evento;

            try {
                evento = JSON.parse(message.value.toString());
            } catch {
                console.error("Se recibió un mensaje JSON inválido.");
                return;
            }

            if (evento.tipo !== "ENTREGA_FALLIDA") {
                return;
            }

            if (
                typeof evento.eventId !== "string" ||
                typeof evento.shipmentId !== "string"
            ) {
                console.error("Evento de fallo sin identificadores válidos.");
                return;
            }

            const notificacion = {
                eventId: randomUUID(),
                eventoOrigenId: evento.eventId,
                shipmentId: evento.shipmentId,
                tipo: "NOTIFICACION_ENTREGA_FALLIDA",
                motivo: evento.motivo ?? "NO_ESPECIFICADO",
                mensaje:
                    `El envío ${evento.shipmentId} no pudo entregarse. ` +
                    "Contacte al operador para conocer las opciones disponibles.",
                ocurridoEn: new Date().toISOString(),
                version: 1,
            };

            await producer.send({
                topic: "notificaciones.enviadas",
                messages: [
                    {
                        key: evento.shipmentId,
                        value: JSON.stringify(notificacion),
                    },
                ],
            });

            console.log(
                `Notificación publicada para ${evento.shipmentId}`
            );
        },
    });
}

iniciarNotificaciones().catch((error) => {
    console.error("Error en notificaciones:", error);
    process.exitCode = 1;
});
