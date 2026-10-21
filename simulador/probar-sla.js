
import { Kafka } from "kafkajs";
import { randomUUID } from "node:crypto";

const kafka = new Kafka({
    clientId: "rutasur-prueba-sla",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function probarSla() {
    const shipmentId = `ENVIO-SLA-${randomUUID().slice(0, 8)}`;
    const ahora = new Date();
    const fechaPrometida = new Date(ahora.getTime() - 60 * 60 * 1000);

    const promesa = {
        eventId: randomUUID(),
        shipmentId,
        fechaPrometida: fechaPrometida.toISOString(),
        zonaId: "ZONA-01",
        hubId: "HUB-01",
        ocurridoEn: ahora.toISOString(),
        version: 1,
    };

    const estado = {
        eventId: randomUUID(),
        shipmentId,
        tipo: "INGRESADO_HUB",
        ocurridoEn: ahora.toISOString(),
        secuencia: 2,
        hubId: "HUB-01",
        zonaId: "ZONA-01",
        repartidorId: null,
        version: 1,
    };

    try {
        await producer.connect();

        // Publicamos primero la promesa vencida.
        await producer.send({
            topic: "envios.promesas",
            messages: [
                {
                    key: shipmentId,
                    value: JSON.stringify(promesa),
                },
            ],
        });

        // Luego publicamos el estado del envío.
        await producer.send({
            topic: "envios.estados",
            messages: [
                {
                    key: shipmentId,
                    value: JSON.stringify(estado),
                },
            ],
        });

        console.log("Prueba SLA publicada correctamente.");
        console.log(`Envío de prueba: ${shipmentId}`);
        console.log(`Estado: ${estado.tipo}`);
        console.log(`Promesa vencida: ${promesa.fechaPrometida}`);
        console.log("Esperá unos segundos y revisá la terminal del monitor.");
    } catch (error) {
        console.error("Error en la prueba SLA:", error.message);
        process.exitCode = 1;
    } finally {
        await producer.disconnect();
    }
}

probarSla();
