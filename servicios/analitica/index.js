
import { Kafka } from "kafkajs";
import { MongoClient } from "mongodb";

const kafka = new Kafka({
    clientId: "rutasur-analitica",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "analitica-v2",
});

const mongo = new MongoClient(
    process.env.MONGO_URL || "mongodb://localhost:27017/?replicaSet=rs0"
);

const DB_NAME = "rutasur";

let eventosAnalitica;
let promesasAnalitica;
let reprogramacionesAnalitica;
let metricasAnalitica;

function fechaValida(valor) {
    if (!valor) return null;

    const fecha = new Date(valor);
    return Number.isNaN(fecha.getTime()) ? null : fecha;
}

function diaDe(fecha) {
    if (!fecha) return "FECHA_DESCONOCIDA";

    return fecha.toISOString().slice(0, 10);
}

async function guardarEstado(evento) {
    const fecha = fechaValida(evento.ocurridoEn);

    await eventosAnalitica.updateOne(
        { eventId: evento.eventId },
        {
            $setOnInsert: {
                eventId: evento.eventId,
                shipmentId: evento.shipmentId,
                tipo: evento.tipo,
                motivo: evento.motivo || null,
                hubId: evento.hubId || null,
                zonaId: evento.zonaId || null,
                ocurridoEn: fecha ? fecha.toISOString() : null,
                secuencia: evento.secuencia ?? null,
                recibidoEn: new Date().toISOString(),
            },
        },
        { upsert: true }
    );
}

async function guardarPromesa(evento) {
    const fecha = fechaValida(evento.fechaPrometida);

    if (!fecha) {
        console.warn(
            `Promesa inválida para ${evento.shipmentId}; se ignora.`
        );
        return;
    }

    await promesasAnalitica.updateOne(
        { shipmentId: evento.shipmentId },
        {
            $set: {
                shipmentId: evento.shipmentId,
                fechaPrometida: fecha.toISOString(),
                hubId: evento.hubId || null,
                zonaId: evento.zonaId || null,
                actualizadaEn: new Date().toISOString(),
            },
        },
        { upsert: true }
    );
}

async function guardarReprogramacion(evento) {
    const eventId =
        evento.eventId ||
        `${evento.shipmentId}:${evento.numeroIntento ?? "sin-intento"}`;

    await reprogramacionesAnalitica.updateOne(
        { eventId },
        {
            $setOnInsert: {
                eventId,
                shipmentId: evento.shipmentId,
                numeroIntento: evento.numeroIntento ?? null,
                motivo: evento.motivo || null,
                motivoDecision: evento.motivoDecision || null,
                decision: evento.decision || null,
                nuevaFecha: evento.nuevaFecha || null,
                ocurridoEn:
                    evento.ocurridoEn || new Date().toISOString(),
            },
        },
        { upsert: true }
    );
}

async function procesarMensaje(topic, message) {
    if (!message.value) {
        console.warn(`Mensaje vacío en ${topic}; se ignora.`);
        return;
    }

    let evento;

    try {
        evento = JSON.parse(message.value.toString());
    } catch {
        console.error(`JSON inválido en ${topic}; se ignora.`);
        return;
    }

    if (!evento.shipmentId) {
        console.warn(`Evento sin shipmentId en ${topic}; se ignora.`);
        return;
    }

    if (topic === "envios.estados") {
        if (!evento.eventId || !evento.tipo) {
            console.warn("Evento de estado incompleto; se ignora.");
            return;
        }

        await guardarEstado(evento);
    } else if (topic === "envios.promesas") {
        await guardarPromesa(evento);
    } else if (topic === "envios.reprogramaciones") {
        await guardarReprogramacion(evento);
    }

    await recalcularMetricas();

    console.log(
        `Analítica registrada: ${evento.shipmentId} | ${topic}`
    );
}

async function recalcularMetricas() {
    const estados = await eventosAnalitica.find({}).toArray();
    const promesas = await promesasAnalitica.find({}).toArray();
    const reprogramaciones = await reprogramacionesAnalitica.find({}).toArray();

    const porEnvio = new Map();

    for (const evento of estados) {
        if (!porEnvio.has(evento.shipmentId)) {
            porEnvio.set(evento.shipmentId, []);
        }

        porEnvio.get(evento.shipmentId).push(evento);
    }

    for (const lista of porEnvio.values()) {
        lista.sort((a, b) => {
            const secuenciaA = a.secuencia ?? 0;
            const secuenciaB = b.secuencia ?? 0;

            if (secuenciaA !== secuenciaB) {
                return secuenciaA - secuenciaB;
            }

            return (a.ocurridoEn || "").localeCompare(
                b.ocurridoEn || ""
            );
        });
    }

    const promesasPorEnvio = new Map(
        promesas.map((promesa) => [promesa.shipmentId, promesa])
    );

    const reprogramacionesPorEnvio = new Map();

    for (const reprogramacion of reprogramaciones) {
        const lista =
            reprogramacionesPorEnvio.get(reprogramacion.shipmentId) || [];

        lista.push(reprogramacion);
        reprogramacionesPorEnvio.set(reprogramacion.shipmentId, lista);
    }

    // Indicador 1: entrega al primer intento.
    let enviosEntregados = 0;
    let entregadosPrimerIntento = 0;

    for (const [shipmentId, lista] of porEnvio.entries()) {
        const eventoEntrega = lista.find(
            (evento) => evento.tipo === "ENTREGADO"
        );

        // Si el envío no fue entregado, no cuenta para este indicador.
        if (!eventoEntrega) {
            continue;
        }

        enviosEntregados++;

        // Verificamos si tuvo una entrega fallida antes de ser entregado.
        const tuvoFalloAntesDeEntrega = lista.some(
            (evento) =>
                evento.tipo === "ENTREGA_FALLIDA" &&
                new Date(evento.ocurridoEn) <=
                new Date(eventoEntrega.ocurridoEn)
        );

        if (!tuvoFalloAntesDeEntrega) {
            entregadosPrimerIntento++;
        }
    }

    const tasaPrimerIntento =
        enviosEntregados === 0
            ? 0
            : Number(
                (
                    (entregadosPrimerIntento / enviosEntregados) *
                    100
                ).toFixed(2)
            );

    // Indicador 2: fallos agrupados por zona y motivo.
    const fallosPorZonaMotivo = new Map();

    for (const evento of estados) {
        if (evento.tipo !== "ENTREGA_FALLIDA") continue;

        const clave = `${evento.zonaId || "SIN_ZONA"}|${evento.motivo || "SIN_MOTIVO"}`;

        fallosPorZonaMotivo.set(
            clave,
            (fallosPorZonaMotivo.get(clave) || 0) + 1
        );
    }

    // Indicador 3: cumplimiento de promesa por hub y día.
    const cumplimientoPorHubDia = new Map();

    for (const [shipmentId, lista] of porEnvio.entries()) {
        const entrega = [...lista]
            .reverse()
            .find((evento) => evento.tipo === "ENTREGADO");

        if (!entrega) continue;

        const promesa = promesasPorEnvio.get(shipmentId);
        const fechaEntrega = fechaValida(entrega.ocurridoEn);
        const fechaPrometida = fechaValida(promesa?.fechaPrometida);

        if (!fechaEntrega || !fechaPrometida) continue;

        const hubId = entrega.hubId || promesa?.hubId || "SIN_HUB";
        const dia = diaDe(fechaEntrega);
        const clave = `${hubId}|${dia}`;

        const dato = cumplimientoPorHubDia.get(clave) || {
            hubId,
            dia,
            totalEntregados: 0,
            entregadosATiempo: 0,
        };

        dato.totalEntregados++;

        if (fechaEntrega <= fechaPrometida) {
            dato.entregadosATiempo++;
        }

        cumplimientoPorHubDia.set(clave, dato);
    }

    const cumplimientoSla = [...cumplimientoPorHubDia.values()].map(
        (dato) => ({
            ...dato,
            porcentajeCumplimiento:
                dato.totalEntregados === 0
                    ? 0
                    : Number(
                        (
                            (dato.entregadosATiempo /
                                dato.totalEntregados) *
                            100
                        ).toFixed(2)
                    ),
        })
    );

    // Indicador 4: tiempo entre ingreso al hub y salida a reparto.
    const tiemposPorHub = new Map();

    for (const lista of porEnvio.values()) {
        const ingreso = lista.find(
            (evento) => evento.tipo === "INGRESADO_HUB"
        );

        const salida = lista.find(
            (evento) => evento.tipo === "EN_REPARTO"
        );

        if (!ingreso || !salida) continue;

        const fechaIngreso = fechaValida(ingreso.ocurridoEn);
        const fechaSalida = fechaValida(salida.ocurridoEn);

        if (
            !fechaIngreso ||
            !fechaSalida ||
            fechaSalida < fechaIngreso
        ) {
            continue;
        }

        const hubId = ingreso.hubId || salida.hubId || "SIN_HUB";
        const minutos =
            (fechaSalida.getTime() - fechaIngreso.getTime()) / 60000;

        const dato = tiemposPorHub.get(hubId) || {
            hubId,
            cantidadEnvios: 0,
            totalMinutos: 0,
        };

        dato.cantidadEnvios++;
        dato.totalMinutos += minutos;

        tiemposPorHub.set(hubId, dato);
    }

    const tiempoPromedioHub = [...tiemposPorHub.values()].map((dato) => ({
        hubId: dato.hubId,
        cantidadEnvios: dato.cantidadEnvios,
        promedioMinutos:
            dato.cantidadEnvios === 0
                ? 0
                : Number(
                    (dato.totalMinutos / dato.cantidadEnvios).toFixed(2)
                ),
    }));

    const fechaCalculo = new Date().toISOString();

    await metricasAnalitica.updateOne(
        { _id: "indicadores_actuales" },
        {
            $set: {
                calculadoEn: fechaCalculo,
                entregaPrimerIntento: {
                    enviosEntregados,
                    entregadosPrimerIntento,
                    porcentaje: tasaPrimerIntento,
                },
                fallosPorZonaMotivo: [...fallosPorZonaMotivo.entries()].map(
                    ([clave, cantidad]) => {
                        const [zonaId, motivo] = clave.split("|");

                        return { zonaId, motivo, cantidad };
                    }
                ),
                cumplimientoSla,
                tiempoPromedioHub,
            },
        },
        { upsert: true }
    );
}

async function iniciar() {
    try {
        await mongo.connect();

        const db = mongo.db(DB_NAME);

        eventosAnalitica = db.collection("eventos_analitica");
        promesasAnalitica = db.collection("promesas_analitica");
        reprogramacionesAnalitica = db.collection("reprogramaciones_analitica");
        metricasAnalitica = db.collection("metricas_analitica");

        await eventosAnalitica.createIndex(
            { eventId: 1 },
            { unique: true }
        );

        await eventosAnalitica.createIndex({
            shipmentId: 1,
            tipo: 1,
            ocurridoEn: 1,
        });

        await promesasAnalitica.createIndex(
            { shipmentId: 1 },
            { unique: true }
        );

        await reprogramacionesAnalitica.createIndex(
            { eventId: 1 },
            { unique: true }
        );

        await consumer.connect();

        await consumer.subscribe({
            topics: [
                "envios.estados",
                "envios.promesas",
                "envios.reprogramaciones",
            ],
            fromBeginning: true,
        });

        console.log("Servicio de analítica iniciado.");
        console.log(
            "Escuchando estados, promesas y reprogramaciones."
        );

        await recalcularMetricas();

        await consumer.run({
            eachMessage: async ({ topic, message }) => {
                try {
                    await procesarMensaje(topic, message);
                } catch (error) {
                    console.error(
                        "Error procesando analítica:",
                        error.message
                    );
                }
            },
        });
    } catch (error) {
        console.error("No se pudo iniciar analítica:", error);
        process.exitCode = 1;
    }
}

async function cerrar() {
    console.log("Cerrando servicio de analítica...");

    try {
        await consumer.disconnect();
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

iniciar();
