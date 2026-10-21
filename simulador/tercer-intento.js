
import { Kafka } from "kafkajs";
import { randomUUID } from "node:crypto";

const kafka = new Kafka({
    clientId: "rutasur-prueba-tercer-intento",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const shipmentId = "ENVIO-1165e4f9";

async function main() {
    await producer.connect();

    const evento = {
        eventId: randomUUID(),
        shipmentId,
        tipo: "ENTREGA_FALLIDA",
        ocurridoEn: new Date().toISOString(),
        secuencia: 7,
        hubId: "HUB-01",
        zonaId: "ZONA-01",
        repartidorId: "REP-001",
        version: 1,
        motivo: "AUSENTE",
    };

    await producer.send({
        topic: "envios.estados",
        messages: [
            {
                key: shipmentId,
                value: JSON.stringify(evento),
            },
        ],
    });

    console.log("Tercer evento fallido publicado:");
    console.log(JSON.stringify(evento, null, 2));
}

main()
    .catch((error) => {
        console.error("Error:", error);
        process.exitCode = 1;
    })
    .finally(async () => {
        await producer.disconnect();
    });
