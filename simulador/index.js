
import { Kafka } from "kafkajs";
import { randomUUID } from "node:crypto";

const kafka = new Kafka({
    clientId: "rutasur-simulador",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();

const escenario = process.argv[2] || "feliz";

// Una hora simulada equivale a 30 segundos reales.
// Podés cambiarlo con la variable HORAS_SIMULADAS_MS.
const DURACION_HORA_MS = Number(
    process.env.HORAS_SIMULADAS_MS || 30000
);

const escenariosFallidos = {
    ausente: {
        motivo: "AUSENTE",
        intento: 1,
    },
    direccion: {
        motivo: "DIRECCION_INCORRECTA",
        intento: 1,
    },
    rechazado: {
        motivo: "RECHAZADO",
        intento: 1,
    },
    zona: {
        motivo: "ZONA_INACCESIBLE",
        intento: 1,
    },
    ausente2: {
        motivo: "AUSENTE",
        intento: 2,
    },
    ausente3: {
        motivo: "AUSENTE",
        intento: 3,
    },
};

function crearEstados() {
    if (escenario === "feliz") {
        return [
            "CREADO",
            "INGRESADO_HUB",
            "CLASIFICADO",
            "EN_REPARTO",
            "ENTREGADO",
        ];
    }

    if (escenariosFallidos[escenario]) {
        return [
            "CREADO",
            "INGRESADO_HUB",
            "CLASIFICADO",
            "EN_REPARTO",
            "ENTREGA_FALLIDA",
        ];
    }

    throw new Error(
        "Escenario inválido. Usá: feliz, ausente, direccion, rechazado, zona, ausente2 o ausente3."
    );
}

function crearEvento(tipo, shipmentId, secuencia, ocurridoEn) {
    const evento = {
        eventId: randomUUID(),
        shipmentId,
        tipo,
        ocurridoEn: ocurridoEn.toISOString(),
        secuencia,
        hubId: "HUB-01",
        zonaId: "ZONA-01",
        repartidorId:
            tipo === "CREADO" || tipo === "INGRESADO_HUB"
                ? null
                : "REP-001",
        version: 1,
    };

    if (tipo === "ENTREGA_FALLIDA") {
        evento.motivo = escenariosFallidos[escenario].motivo;
    }

    return evento;
}

function crearPromesa(shipmentId, fechaBase) {
    // La promesa queda dos horas después del inicio simulado.
    const fechaPrometida = new Date(
        fechaBase.getTime() + 2 * 60 * 60 * 1000
    );

    return {
        eventId: randomUUID(),
        shipmentId,
        fechaPrometida: fechaPrometida.toISOString(),
        zonaId: "ZONA-01",
        hubId: "HUB-01",
        ocurridoEn: fechaBase.toISOString(),
        version: 1,
    };
}

function esperar(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function iniciarSimulador() {
    try {
        const estados = crearEstados();

        if (
            !Number.isFinite(DURACION_HORA_MS) ||
            DURACION_HORA_MS <= 0
        ) {
            throw new Error(
                "HORAS_SIMULADAS_MS debe ser un número mayor que cero."
            );
        }

        await producer.connect();

        const shipmentId = `ENVIO-${randomUUID().slice(0, 8)}`;

        // El tiempo del envío comienza ahora y avanza de forma simulada.
        const fechaBase = new Date();

        console.log(`Escenario: ${escenario}`);
        console.log(`Envío: ${shipmentId}`);
        console.log(
            `Tiempo simulado: 1 hora = ${DURACION_HORA_MS / 1000} segundos reales`
        );

        const promesa = crearPromesa(shipmentId, fechaBase);

        await producer.send({
            topic: "envios.promesas",
            messages: [
                {
                    key: shipmentId,
                    value: JSON.stringify(promesa),
                },
            ],
        });

        console.log(
            `Promesa publicada: ${promesa.fechaPrometida}`
        );

        if (escenariosFallidos[escenario]) {
            console.log(
                `Motivo del fallo: ${escenariosFallidos[escenario].motivo}`
            );
            console.log(
                `Intento solicitado: ${escenariosFallidos[escenario].intento}`
            );
            console.log(
                "Nota: el contador real de intentos lo determina el servicio de reprogramación."
            );
        }

        // Cada estado representa un momento distinto de la operación.
        // Los tiempos se expresan en minutos desde el inicio.
        const minutosPorEstado = {
            CREADO: 0,
            INGRESADO_HUB: 15,
            CLASIFICADO: 45,
            EN_REPARTO: 90,
            ENTREGADO: 110,
            ENTREGA_FALLIDA: 110,
        };

        for (let i = 0; i < estados.length; i++) {
            const tipo = estados[i];
            const minutosSimulados = minutosPorEstado[tipo] ?? i * 15;

            const ocurridoEn = new Date(
                fechaBase.getTime() + minutosSimulados * 60 * 1000
            );

            const evento = crearEvento(
                tipo,
                shipmentId,
                i + 1,
                ocurridoEn
            );

            await producer.send({
                topic: "envios.estados",
                messages: [
                    {
                        key: shipmentId,
                        value: JSON.stringify(evento),
                    },
                ],
            });

            console.log(
                `Publicado: ${evento.tipo} | Secuencia: ${evento.secuencia} | Fecha simulada: ${evento.ocurridoEn}`
            );

            // Pausa breve para que los consumidores procesen los eventos.
            await esperar(500);
        }

        console.log("Simulación finalizada correctamente.");
    } catch (error) {
        console.error("Error en el simulador:", error.message);
        process.exitCode = 1;
    } finally {
        await producer.disconnect();
    }
}

iniciarSimulador();
